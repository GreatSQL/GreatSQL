/* Copyright (c) 2013, 2022, Oracle and/or its affiliates.
   Copyright (c) 2024, GreatDB Software Co., Ltd.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include "sql/local_binlog_reader.h"

#include <stdio.h>

#include "my_dbug.h"
#include "my_sys.h"
#include "mysql.h"
#include "scope_guard.h"
#include "sql/binlog_base.h"
#include "sql/binlog_reader.h"
#include "sql/log_event.h"  // MAX_MAX_ALLOWED_PACKET
#include "sql/mysqld.h"     // global_system_variables ...
#include "sql/rpl_gtid.h"
#include "sql/rpl_source.h"
#include "sql/sql_class.h"  // THD
#include "sql_string.h"

const uint32 Local_binlog_reader::PACKET_MIN_SIZE = 4096;
const uint32 Local_binlog_reader::PACKET_MAX_SIZE = UINT_MAX32;
const ushort Local_binlog_reader::PACKET_SHRINK_COUNTER_THRESHOLD = 100;
const float Local_binlog_reader::PACKET_GROW_FACTOR = 2.0;
const float Local_binlog_reader::PACKET_SHRINK_FACTOR = 0.5;

using binary_log::checksum_crc32;

/**
  Binlog_sender reads events one by one. It uses the preallocated memory
  (A String object) to store all event_data instead of allocating memory when
  reading each event_data. So event should not free the memory at destructor.
*/
class Local_binlog_reader::Event_allocator {
 public:
  enum { DELEGATE_MEMORY_TO_EVENT_OBJECT = false };

  void set_reader(Local_binlog_reader *reader) { m_reader = reader; }
  unsigned char *allocate(size_t size) {
    my_off_t event_offset = m_reader->m_packet.length();
    if (m_reader->grow_packet(size)) return nullptr;

    m_reader->m_packet.length(event_offset + size);
    return pointer_cast<unsigned char *>(m_reader->m_packet.ptr() +
                                         event_offset);
  }

  void deallocate(unsigned char *ptr [[maybe_unused]]) {}

 private:
  Local_binlog_reader *m_reader = nullptr;
};

Local_binlog_reader::Local_binlog_reader(THD *thd, const char *start_file,
                                         my_off_t start_pos,
                                         Gtid_set *exclude_gtids, uint32 flag)
    : m_thd(thd),
      m_start_pos(start_pos),
      m_exclude_gtid(exclude_gtids),
      m_using_gtid_protocol(exclude_gtids != nullptr),
      m_gtid_clear_fd_created_flag(exclude_gtids == nullptr),
      m_diag_area(false),
      m_errmsg(nullptr),
      m_errno(0),
      m_last_file(nullptr),
      m_last_pos(0),
      m_flag(flag),
      m_filter(nullptr),
      m_applier(nullptr),
      m_stop(false) {
  if (start_file == nullptr)
    m_start_file = "";
  else
    m_start_file = start_file;
}

extern TYPELIB binlog_checksum_typelib;

void Local_binlog_reader::init() {
  DBUG_TRACE;
  THD *thd = m_thd;

  mysql_mutex_lock(&thd->LOCK_thd_data);
  thd->current_linfo = &m_linfo;
  mysql_mutex_unlock(&thd->LOCK_thd_data);

  if (!mysql_bin_log.is_open()) {
    set_fatal_error("Binary log is not open");
    return;
  }

  if (check_start_file()) return;

  m_heartbeat_period = std::chrono::nanoseconds(1 * 1000000000UL);
  m_event_checksum_alg =
      static_cast<enum_binlog_checksum_alg>(binlog_checksum_options);
  m_fde.reset(new Format_description_log_event());
  m_fde->common_footer->checksum_alg = m_event_checksum_alg;
  m_wait_new_events = true;
}

void Local_binlog_reader::cleanup() {
  DBUG_TRACE;
  THD *thd = m_thd;

  mysql_mutex_lock(&thd->LOCK_thd_data);
  thd->current_linfo = nullptr;
  mysql_mutex_unlock(&thd->LOCK_thd_data);
}

void Local_binlog_reader::run() {
  DBUG_TRACE;
  init();

  File_reader reader(opt_source_verify_checksum);
  reader.allocator()->set_reader(this);
  my_off_t start_pos = m_start_pos;
  const char *log_file = m_linfo.log_file_name;
  bool is_index_file_reopened_on_binlog_disable = false;

  while (!has_error() && !m_stop) {
    if (reader.open(log_file)) {
      set_fatal_error(log_read_error_msg(reader.get_error_type()));
      break;
    }

    if (send_binlog(reader, start_pos)) {
      break;
    }

    /* Will go to next file, need to copy log file name */
    set_last_file(log_file);

    mysql_bin_log.lock_index();
    if (!mysql_bin_log.is_open()) {
      if (mysql_bin_log.open_index_file(mysql_bin_log.get_index_fname(),
                                        log_file, false)) {
        set_fatal_error(
            "Binary log is not open and failed to open index file "
            "to retrieve next file.");
        mysql_bin_log.unlock_index();
        break;
      }
      is_index_file_reopened_on_binlog_disable = true;
    }
    int error = mysql_bin_log.find_next_log(&m_linfo, false);
    mysql_bin_log.unlock_index();
    if (unlikely(error)) {
      if (is_index_file_reopened_on_binlog_disable)
        mysql_bin_log.close(LOG_CLOSE_INDEX, true /*need_lock_log=true*/,
                            true /*need_lock_index=true*/);
      set_fatal_error("could not find next log");
      break;
    }

    start_pos = BIN_LOG_HEADER_SIZE;
    reader.close();
  }

  m_applier->cleanup();

  char error_text[1024];
  if (reader.is_open()) {
    if (is_fatal_error()) {
      /* output events range to error message */
      my_snprintf_8bit(nullptr, error_text, sizeof(error_text),
                       "%s; the first event '%s' at %lld, "
                       "the last event read from '%s' at %lld, "
                       "the last byte read from '%s' at %lld.",
                       m_errmsg, m_start_file, m_start_pos, m_last_file,
                       m_last_pos, log_file, reader.position());
      set_fatal_error(error_text);
    }

    reader.close();
  }

  cleanup();
}

int Local_binlog_reader::send_binlog(File_reader &reader, my_off_t start_pos) {
  if (unlikely(send_format_description_event(reader, start_pos))) return 1;
  if (start_pos == BIN_LOG_HEADER_SIZE) start_pos = reader.position();
  if (reader.position() != start_pos && reader.seek(start_pos)) return 1;

  while (!m_stop) {
    auto [end_pos, code] = get_binlog_end_pos(reader);

    if (code) return 1;
    if (send_events(reader, end_pos)) return 1;
    /*
      It is not active binlog, send_events should not return unless
      it reads all events.
    */
    if (end_pos == 0) return 0;
  }
  return 1;
}

std::pair<my_off_t, int> Local_binlog_reader::get_binlog_end_pos(
    File_reader &reader) {
  DBUG_TRACE;
  my_off_t read_pos = reader.position();

  std::pair<my_off_t, int> result = std::make_pair(read_pos, 1);

  if (m_wait_new_events) {
    if (unlikely(wait_new_events(read_pos))) return result;
  }

  result.first = mysql_bin_log.get_binlog_end_pos();

  /* If this is a cold binlog file, we are done getting the end pos */
  if (unlikely(!mysql_bin_log.is_active(m_linfo.log_file_name))) {
    return std::make_pair(0, 0);
  }
  if (read_pos < result.first) {
    result.second = 0;
    return result;
  }
  return result;
}

int Local_binlog_reader::send_events(File_reader &reader, my_off_t end_pos) {
  DBUG_TRACE;

  my_off_t log_pos = reader.position();
  bool in_exclude_group = false;

  while (likely(log_pos < end_pos) || end_pos == 0) {
    if (DBUG_EVALUATE_IF("test_secondary_sync_task_hang", 1, 0)) {
      sleep(1);
      continue;
    }
    uchar *event_ptr = nullptr;
    uint32 event_len = 0;

    if (unlikely(m_stop)) return 1;
    if (unlikely(read_event(reader, &event_ptr, &event_len))) return 1;
    if (event_ptr == nullptr) {
      if (end_pos == 0) return 0;  // Arrive the end of inactive file

      /*
        It is reading events before end_pos of active binlog file. In theory,
        it should never return nullptr. But RESET MASTER doesn't check if there
        is any dump thread working. So it is possible that the active binlog
        file is reopened and truncated to 0 after RESET MASTER.
      */
      set_fatal_error(log_read_error_msg(Binlog_read_error::SYSTEM_IO));
      return 1;
    }
    log_pos = reader.position();

    in_exclude_group = skip_event(event_ptr, in_exclude_group);
    if (!in_exclude_group) {
      if (unlikely(handle_packet())) {
        return 1;
      }
    }
  }

  return 0;
}

inline bool Local_binlog_reader::skip_event(const uchar *event_ptr,
                                            bool in_exclude_group) {
  DBUG_TRACE;
  if (m_exclude_gtid == nullptr) return false;

  uint8 event_type = (Log_event_type)event_ptr[LOG_EVENT_OFFSET];
  switch (event_type) {
    case binary_log::GTID_LOG_EVENT: {
      Format_description_log_event fd_ev;
      fd_ev.common_footer->checksum_alg = m_event_checksum_alg;
      Gtid_log_event gtid_ev(reinterpret_cast<const char *>(event_ptr), &fd_ev);
      Gtid gtid;
      auto *sid_lock = m_exclude_gtid->get_sid_map()->get_sid_lock();
      if (sid_lock != nullptr) sid_lock->wrlock();
      gtid.sidno = gtid_ev.get_sidno(m_exclude_gtid->get_sid_map());
      gtid.gno = gtid_ev.get_gno();
      auto ret = m_exclude_gtid->contains_gtid(gtid);
      if (sid_lock != nullptr) sid_lock->unlock();
      return ret;
    }
    case binary_log::ROTATE_EVENT:
      return false;
  }
  return in_exclude_group;
}

int Local_binlog_reader::wait_new_events(my_off_t log_pos) {
  int ret = 0;
  PSI_stage_info old_stage;

  /*
    MYSQL_BIN_LOG::binlog_end_pos is atomic. We should only acquire the
    LOCK_binlog_end_pos if we reached the end of the hot log and are going
    to wait for updates on the binary log (Binlog_sender::wait_new_event()).
  */
  if (stop_waiting_for_update(log_pos)) {
    return 0;
  }

  mysql_bin_log.lock_binlog_end_pos();

  m_thd->ENTER_COND(mysql_bin_log.get_log_cond(),
                    mysql_bin_log.get_binlog_end_pos_lock(),
                    &stage_source_has_sent_all_binlog_to_replica, &old_stage);

  if (m_heartbeat_period.count() > 0) {
    while (!stop_waiting_for_update(log_pos)) {
      mysql_bin_log.wait_for_update(m_heartbeat_period);

      if (stop_waiting_for_update(log_pos)) {
        break;
      }
      mysql_bin_log.unlock_binlog_end_pos();
      Scope_guard lock([]() { mysql_bin_log.lock_binlog_end_pos(); });
      ret = send_heartbeat_event(log_pos);
      if (ret) break;
    }
  }

  mysql_bin_log.unlock_binlog_end_pos();
  m_thd->EXIT_COND(&old_stage);

  return ret;
}

int Local_binlog_reader::send_heartbeat_event(my_off_t log_pos) {
  DBUG_TRACE;
  const char *filename = m_linfo.log_file_name;
  const char *p = filename + dirname_length(filename);
  size_t ident_len = strlen(p);
  size_t event_len = ident_len + LOG_EVENT_HEADER_LEN +
                     (event_checksum_on() ? BINLOG_CHECKSUM_LEN : 0);

  m_packet.length(event_len);
  uchar *header = pointer_cast<uchar *>(m_packet.ptr());

  /* Timestamp field */
  int4store(header, 0);
  header[EVENT_TYPE_OFFSET] = binary_log::HEARTBEAT_LOG_EVENT;
  int4store(header + SERVER_ID_OFFSET, server_id);
  int4store(header + EVENT_LEN_OFFSET, event_len);
  int4store(header + LOG_POS_OFFSET, static_cast<uint32>(log_pos));
  int2store(header + FLAGS_OFFSET, 0);
  memcpy(header + LOG_EVENT_HEADER_LEN, p, ident_len);
  if (event_checksum_on()) calc_event_checksum(header, event_len);
  return handle_packet(false);
}

bool Local_binlog_reader::stop_waiting_for_update(my_off_t log_pos) const {
  if (mysql_bin_log.get_binlog_end_pos() > log_pos ||
      !mysql_bin_log.is_active(m_linfo.log_file_name) || m_stop) {
    return true;
  }
  return false;
}

int Local_binlog_reader::check_start_file() {
  char index_entry_name[FN_REFLEN];
  char *name_ptr = nullptr;
  std::string errmsg;

  if (m_start_file[0] != '\0') {
    mysql_bin_log.make_log_name(index_entry_name, m_start_file);
    name_ptr = index_entry_name;
  } else if (m_using_gtid_protocol) {
    // 1. check request gtids
    auto *sid_lock = m_exclude_gtid->get_sid_map()->get_sid_lock();
    if (sid_lock != nullptr) sid_lock->wrlock();
    global_sid_lock->wrlock();
    Gtid_set gtid_executed_and_owned(
        gtid_state->get_executed_gtids()->get_sid_map());
    auto status =
        gtid_executed_and_owned.add_gtid_set(gtid_state->get_executed_gtids());
    assert(status == RETURN_STATUS_OK);
    gtid_state->get_owned_gtids()->get_gtids(gtid_executed_and_owned);

    if (!m_exclude_gtid->is_subset(&gtid_executed_and_owned)) {
      global_sid_lock->unlock();
      if (sid_lock != nullptr) sid_lock->unlock();
      set_fatal_error("The server has less GTIDs than request");
      return 1;
    }
    if (!gtid_state->get_lost_gtids()->is_subset(m_exclude_gtid)) {
      set_fatal_error("The server has purged GTIDs for request");
      global_sid_lock->unlock();
      if (sid_lock != nullptr) sid_lock->unlock();
      return 1;
    }
    global_sid_lock->unlock();

    // 2. find start file and pos
    Gtid first_gtid = {0, 0};
    if (mysql_bin_log.find_first_log_not_in_gtid_set(
            index_entry_name, m_exclude_gtid, &first_gtid, errmsg)) {
      set_fatal_error(errmsg.c_str());
      if (sid_lock != nullptr) sid_lock->unlock();
      return 1;
    }
    name_ptr = index_entry_name;
    /*
      If we are skipping at least the first transaction of the binlog,
      we must clear the "created" field of the FD event (set it to 0)
      to avoid cleaning up temp tables on slave.
    */
    m_gtid_clear_fd_created_flag =
        (first_gtid.sidno >= 1 && first_gtid.gno >= 1 &&
         m_exclude_gtid->contains_gtid(first_gtid));
    if (sid_lock != nullptr) sid_lock->unlock();
  }

  /*
    Index entry name is saved into m_linfo. If name_ptr is NULL,
    then starts from the first file in index file.
  */
  if (mysql_bin_log.find_log_pos(&m_linfo, name_ptr, true)) {
    set_fatal_error(
        "Could not find first log file name in binary log "
        "index file");
    return 1;
  }

  Binlog_read_error binlog_read_error;
  Binlog_ifile binlog_ifile(&binlog_read_error);
  if (binlog_ifile.open(m_linfo.log_file_name)) {
    set_fatal_error(binlog_read_error.get_str());
    return 1;
  }

  if (m_start_pos == 0) m_start_pos = BIN_LOG_HEADER_SIZE;
  assert(m_start_pos >= BIN_LOG_HEADER_SIZE);
  if (m_start_pos > binlog_ifile.length()) {
    set_fatal_error("Request position larger than file size");
    return 1;
  }
  return 0;
}

inline void Local_binlog_reader::calc_event_checksum(uchar *event_ptr,
                                                     size_t event_len) {
  ha_checksum crc = checksum_crc32(0L, nullptr, 0);
  crc = checksum_crc32(crc, event_ptr, event_len - BINLOG_CHECKSUM_LEN);
  int4store(event_ptr + event_len - BINLOG_CHECKSUM_LEN, crc);
}

int Local_binlog_reader::send_format_description_event(File_reader &reader,
                                                       my_off_t start_pos) {
  DBUG_TRACE;
  uchar *event_ptr = nullptr;
  uint32 event_len = 0;

  if (read_event(reader, &event_ptr, &event_len)) return 1;

  if (event_ptr == nullptr ||
      event_ptr[EVENT_TYPE_OFFSET] != binary_log::FORMAT_DESCRIPTION_EVENT) {
    set_fatal_error("Could not find format_description_event in binlog file");
    return 1;
  }

  Log_event *ev = nullptr;
  Binlog_read_error binlog_read_error = binlog_event_deserialize(
      event_ptr, event_len, reader.format_description_event(), false, &ev);
  if (binlog_read_error.has_error()) {
    set_fatal_error(binlog_read_error.get_str());
    return 1;
  }
  reader.set_format_description_event(
      dynamic_cast<Format_description_log_event &>(*ev));
  delete ev;

  assert(event_ptr[LOG_POS_OFFSET] > 0);
  m_event_checksum_alg =
      Log_event_footer::get_checksum_alg((const char *)event_ptr, event_len);
  assert(m_event_checksum_alg < binary_log::BINLOG_CHECKSUM_ALG_ENUM_END ||
         m_event_checksum_alg == binary_log::BINLOG_CHECKSUM_ALG_UNDEF);
  event_ptr[FLAGS_OFFSET] &= ~LOG_EVENT_BINLOG_IN_USE_F;

  bool event_updated = false;
  if (m_using_gtid_protocol) {
    if (m_gtid_clear_fd_created_flag) {
      /*
        As we are skipping at least the first transaction of the binlog,
        we must clear the "created" field of the FD event (set it to 0)
        to avoid destroying temp tables on slave.
      */
      int4store(event_ptr + LOG_EVENT_MINIMAL_HEADER_LEN + ST_CREATED_OFFSET,
                0);
      event_updated = true;
    }
  } else if (start_pos > BIN_LOG_HEADER_SIZE) {
    /*
      If we are skipping the beginning of the binlog file based on the position
      asked by the slave, we must clear the log_pos and the created flag of the
      Format_description_log_event to be sent. Mark that this event with
      "log_pos=0", so the slave should not increment master's binlog position
      (rli->group_master_log_pos)
    */
    int4store(event_ptr + LOG_POS_OFFSET, 0);
    /*
      Set the 'created' field to 0 to avoid destroying
      temp tables on slave.
    */
    int4store(event_ptr + LOG_EVENT_MINIMAL_HEADER_LEN + ST_CREATED_OFFSET, 0);
    event_updated = true;
  }

  /* fix the checksum due to latest changes in header */
  if (event_checksum_on() && event_updated)
    calc_event_checksum(event_ptr, event_len);

  if (handle_packet()) return 1;

  // TODO: support START_5_7_ENCRYPTION_EVENT
  return 0;
}

const char *Local_binlog_reader::log_read_error_msg(
    Binlog_read_error::Error_type error) {
  switch (error) {
    case Binlog_read_error::BOGUS:
      return "bogus data in log event";
    case Binlog_read_error::MEM_ALLOCATE:
      return "memory allocation failed reading log event";
    case Binlog_read_error::TRUNC_EVENT:
      return "binlog truncated in the middle of event; consider out of disk "
             "space on master";
    case Binlog_read_error::CHECKSUM_FAILURE:
      return "event read from binlog did not pass crc check";
    default:
      return Binlog_read_error(error).get_str();
  }
}

inline int Local_binlog_reader::read_event(File_reader &reader,
                                           uchar **event_ptr,
                                           uint32 *event_len) {
  DBUG_TRACE;

  m_packet.length(0);
  if (reader.read_event_data(event_ptr, event_len)) {
    if (reader.get_error_type() == Binlog_read_error::READ_EOF) {
      *event_ptr = nullptr;
      *event_len = 0;
      return 0;
    }
    set_fatal_error(log_read_error_msg(reader.get_error_type()));
    return 1;
  }

  set_last_pos(reader.position());
  return 0;
}

inline int Local_binlog_reader::handle_packet(bool need_shrink) {
  DBUG_TRACE;
  auto buffer = m_packet.ptr();
  auto buf_len = m_packet.length();
  assert(buf_len > BIN_LOG_HEADER_SIZE);
  uchar event_type = (uchar)buffer[EVENT_TYPE_OFFSET];

  int ret = 0;
  do {
    Log_event *ev = nullptr;
    if (event_type == binary_log::HEARTBEAT_LOG_EVENT) {
      ev = new Heartbeat_log_event(buffer, m_fde.get());
    } else if (binlog_event_deserialize(pointer_cast<const uchar *>(buffer),
                                        buf_len, m_fde.get(), false,
                                        &ev) != Binlog_read_error::SUCCESS) {
      set_fatal_error("wrong binlog event format");
      ret = 1;
      break;
    }

    std::shared_ptr<Log_event> event(ev);
    if (ev->get_type_code() == binary_log::FORMAT_DESCRIPTION_EVENT) {
      m_fde = std::dynamic_pointer_cast<Format_description_log_event>(event);
    }
    bool discard_event = false;
    Query_event_info event_info;
    if (m_filter && !m_filter->filter_event(event, event_info)) {
      discard_event = true;
      if (m_filter->has_error()) {
        set_error(ER_BINLOG_FATAL_ERROR, m_filter->error().c_str());
        return 1;
      }
    }
    assert(m_applier != nullptr);
    ret = m_applier->apply_event(event, discard_event, event_info);
    if (ret) {
      set_error(ER_BINLOG_FATAL_ERROR, m_applier->error().c_str());
    } else if (event->get_type_code() == binary_log::GTID_LOG_EVENT) {
      m_last_gtid_log_event = std::dynamic_pointer_cast<Gtid_log_event>(event);
    }
  } while (false);

  if (need_shrink && ret == 0) ret = shrink_packet() ? 1 : 0;
  return ret;
}

inline bool Local_binlog_reader::grow_packet(size_t extra_size) {
  DBUG_TRACE;

  size_t cur_buffer_size = m_packet.alloced_length();
  size_t cur_buffer_used = m_packet.length();
  size_t needed_buffer_size = cur_buffer_used + extra_size;

  if (extra_size > (PACKET_MAX_SIZE - cur_buffer_used))
    /*
       Not enough memory: requesting packet to be bigger than the max
       allowed - PACKET_MAX_SIZE.
    */
    return true;

  /* Grow the buffer if needed. */
  if (needed_buffer_size > cur_buffer_size) {
    size_t new_buffer_size;
    new_buffer_size =
        calc_grow_buffer_size(cur_buffer_size, needed_buffer_size);

    if (!new_buffer_size) return true;

    if (m_packet.mem_realloc(new_buffer_size)) return true;

    /*
     Calculates the new, smaller buffer, size to use the next time
     one wants to shrink the buffer.
    */
    calc_shrink_buffer_size(new_buffer_size);
  }

  return false;
}

inline size_t Local_binlog_reader::calc_grow_buffer_size(size_t current_size,
                                                         size_t min_size) {
  /* Check that a sane minimum buffer size was requested.  */
  if (min_size > PACKET_MAX_SIZE) return 0;

  /*
     Even if this overflows (PACKET_MAX_SIZE == UINT_MAX32) and
     new_size wraps around, the min_size will always be returned,
     i.e., it is a safety net.

     Also, cap new_size to PACKET_MAX_SIZE (in case
     PACKET_MAX_SIZE < UINT_MAX32).
   */
  size_t new_size = static_cast<size_t>(
      std::min(static_cast<double>(PACKET_MAX_SIZE),
               static_cast<double>(current_size * PACKET_GROW_FACTOR)));

  new_size = ALIGN_SIZE(std::max(new_size, min_size));

  return new_size;
}

void Local_binlog_reader::calc_shrink_buffer_size(size_t current_size) {
  size_t new_size = static_cast<size_t>(
      std::max(static_cast<double>(PACKET_MIN_SIZE),
               static_cast<double>(current_size * PACKET_SHRINK_FACTOR)));

  m_new_shrink_size = ALIGN_SIZE(new_size);
}

inline bool Local_binlog_reader::shrink_packet() {
  DBUG_TRACE;
  bool res = false;
  size_t cur_buffer_size = m_packet.alloced_length();
  size_t buffer_used = m_packet.length();

  /*
     If the packet is already at the minimum size, just
     do nothing. Otherwise, check if we should shrink.
   */
  if (cur_buffer_size > PACKET_MIN_SIZE) {
    /* increment the counter if we used less than the new shrink size. */
    if (buffer_used < m_new_shrink_size) {
      m_half_buffer_size_req_counter++;

      /* Check if we should shrink the buffer. */
      if (m_half_buffer_size_req_counter == PACKET_SHRINK_COUNTER_THRESHOLD) {
        /*
         The last PACKET_SHRINK_COUNTER_THRESHOLD consecutive packets
         required less than half of the current buffer size. Lets shrink
         it to not hold more memory than we potentially need.
        */
        m_packet.shrink(m_new_shrink_size);

        /*
           Calculates the new, smaller buffer, size to use the next time
           one wants to shrink the buffer.
         */
        calc_shrink_buffer_size(m_new_shrink_size);

        /* Reset the counter. */
        m_half_buffer_size_req_counter = 0;
      }
    } else
      m_half_buffer_size_req_counter = 0;
  }
  return res;
}

int Read_binlog_meta_applier::apply_event(std::shared_ptr<Log_event> &ev,
                                          bool discard_event,
                                          const Query_event_info &event_info) {
  DBUG_TRACE;
  auto type_code = ev->get_type_code();
  switch (type_code) {
    case binary_log::FORMAT_DESCRIPTION_EVENT: {
      m_fde = std::dynamic_pointer_cast<Format_description_log_event>(ev);
    } break;
    case binary_log::ANONYMOUS_GTID_LOG_EVENT: {
      // not support yet
      set_error("not support ANONYMOUS_GTID_LOG_EVENT");
      return 1;
    } break;
    case binary_log::GTID_LOG_EVENT: {
      assert(!discard_event);
      if (m_last_gtid_log_event != nullptr) {
        set_error("transaction not committed correctly for last gtid");
        return 1;
      }
      m_last_gtid_log_event = std::dynamic_pointer_cast<Gtid_log_event>(ev);
      if (m_executor_cbk) {
        if (m_executor_cbk->execute_gtid(m_last_gtid_log_event)) {
          set_error("not support savepoint/rollback trx in binlog");
          return 1;
        }
      }
    } break;
    case binary_log::QUERY_EVENT: {
      std::shared_ptr<Query_log_event> query_event =
          std::dynamic_pointer_cast<Query_log_event>(ev);
      const char *query = query_event->query;
      if (!native_strncasecmp(query, STRING_WITH_LEN("XA START")) ||
          !native_strncasecmp(query, STRING_WITH_LEN("XA END")) ||
          !native_strncasecmp(query, STRING_WITH_LEN("XA PREPARE")) ||
          !native_strncasecmp(query, STRING_WITH_LEN("XA COMMIT")) ||
          !native_strncasecmp(query, STRING_WITH_LEN("XA ROLLBACK"))) {
        set_error("unsupport 2pc xa transaction yet");
        return 1;
      } else if (!native_strncasecmp(query, "SAVEPOINT", 9) ||
                 !native_strncasecmp(query, "ROLLBACK", 8)) {
        set_error("not support savepoint/rollback trx in binlog");
        return 1;
      } else if (!strncmp(query, "COMMIT", query_event->q_len)) {
        assert(!discard_event);
        if (commit_trx()) return 1;
      } else if (!strncmp(query, "BEGIN", query_event->q_len)) {
        assert(!discard_event);
        if (m_executor_cbk) {
          if (m_executor_cbk->start_trx()) {
            set_error(m_executor_cbk->error());
            return 1;
          }
        }
      } else {
        if (!discard_event) {
          if (m_executor_cbk) {
            if (m_executor_cbk->execute_ddl(query_event)) {
              set_error(m_executor_cbk->error());
            }
          }
          if (!event_info.transaction_ddl && commit_trx()) return 1;
        }
      }
    } break;
    case binary_log::XID_EVENT: {
      assert(!discard_event);
      if (commit_trx()) return 1;
    } break;
    case binary_log::XA_PREPARE_LOG_EVENT: {
      // TODO: it should save binlog, and wait next XA COMMIT/ROLLBACK
      // how to save current event/trx
      set_error("unsupport 2pc xa transaction yet");
      return 1;
    } break;
    case binary_log::TABLE_MAP_EVENT: {
      if (discard_event) break;
      std::shared_ptr<Table_map_log_event> tbl_log_event =
          std::dynamic_pointer_cast<Table_map_log_event>(ev);
      auto table_id = tbl_log_event->get_table_id().id();
      m_tbl_map_events[table_id] = tbl_log_event;
    } break;
    case binary_log::PARTIAL_UPDATE_ROWS_EVENT: {
      if (discard_event) break;
      // TODO: support partial update row for JSON
      // check what unpack_row does
      set_error(
          "unsupport PARTIAL_UPDATE_ROWS_EVENT yet, it should set "
          "binlog_row_value_options to empty");
      return 1;
    } break;
    case binary_log::WRITE_ROWS_EVENT:
    case binary_log::UPDATE_ROWS_EVENT:
    case binary_log::DELETE_ROWS_EVENT: {
      if (discard_event) break;
      // TODO: support gpik which introduce from 8.0.32
      std::shared_ptr<Rows_log_event> rows_log_event =
          std::dynamic_pointer_cast<Rows_log_event>(ev);
      auto table_id = rows_log_event->get_table_id().id();
      assert(m_tbl_map_events.count(table_id));
      auto &tbl_map_event = m_tbl_map_events[table_id];

      TABLE *table =
          open_table_for_task(current_thd, tbl_map_event->get_db_name(),
                              tbl_map_event->get_table_name());
      if (table == nullptr) {
        set_error("cannot open table");
        return 1;
      }

      enum_row_image_type row_image_type;
      if (type_code == binary_log::WRITE_ROWS_EVENT) {
        row_image_type = enum_row_image_type::WRITE_AI;
      } else if (type_code == binary_log::DELETE_ROWS_EVENT) {
        row_image_type = enum_row_image_type::DELETE_BI;
      } else {
        row_image_type = enum_row_image_type::UPDATE_BI;
      }

      for (const uchar *value = rows_log_event->get_rows_buf();
           value < rows_log_event->get_rows_end();) {
        /* sweep the first image */
        auto length = m_row_sweeper.sweep_one_row(
            table, tbl_map_event->get_db_name(),
            tbl_map_event->get_table_name(), tbl_map_event->m_colcnt,
            rows_log_event->get_cols_for_update(), value,
            rows_log_event->get_rows_end(), row_image_type,
            tbl_map_event->m_coltype, tbl_map_event->m_field_metadata);
        if (length == 0 || length == UINT32_MAX) {
          set_error(m_executor_cbk->error());
          close_table_for_task(current_thd);
          return 1;
        }
        value += length;

        if (type_code == binary_log::WRITE_ROWS_EVENT ||
            type_code == binary_log::DELETE_ROWS_EVENT) {
          continue;
        }
        // TODO: provide func to handle row
        /* sweep second image (for UPDATE only) */
        length = m_row_sweeper.sweep_one_row(
            table, tbl_map_event->get_db_name(),
            tbl_map_event->get_table_name(), tbl_map_event->m_colcnt,
            rows_log_event->get_cols_ai_for_update(), value,
            rows_log_event->get_rows_end(), enum_row_image_type::UPDATE_AI,
            tbl_map_event->m_coltype, tbl_map_event->m_field_metadata);
        if (length == 0 || length == UINT32_MAX) {
          set_error(m_executor_cbk->error());
          close_table_for_task(current_thd);
          return 1;
        }
        value += length;
      }

      bool stmt_end =
          rows_log_event->get_flags(binary_log::Rows_event::STMT_END_F);
      if (m_executor_cbk) {
        int dml_type = 0;
        if (type_code == binary_log::WRITE_ROWS_EVENT)
          dml_type = 0;
        else if (type_code == binary_log::UPDATE_ROWS_EVENT)
          dml_type = 1;
        else if (type_code == binary_log::DELETE_ROWS_EVENT)
          dml_type = 2;
        if (m_executor_cbk->execute_row(tbl_map_event->get_db_name(),
                                        tbl_map_event->get_table_name(),
                                        dml_type, stmt_end)) {
          close_table_for_task(current_thd);
          set_error(m_executor_cbk->error());
          return 1;
        }
      }
      if (stmt_end) {
        m_tbl_map_events.clear();
      }
      close_table_for_task(current_thd);
    } break;
    case binary_log::WRITE_ROWS_EVENT_V1:
    case binary_log::UPDATE_ROWS_EVENT_V1:
    case binary_log::DELETE_ROWS_EVENT_V1: {
      // should not happen
      assert(0);
      set_error("unexpect old version row events");
      return 1;
    } break;
    case binary_log::ROTATE_EVENT: {
      assert(!discard_event);
      Rotate_log_event *rotate = (Rotate_log_event *)(ev.get());
      m_last_file.assign(rotate->new_log_ident, rotate->ident_len);
      m_last_pos = rotate->pos;
    } break;
    case binary_log::HEARTBEAT_LOG_EVENT: {
      std::shared_ptr<Heartbeat_log_event> heartbeat_event =
          std::dynamic_pointer_cast<Heartbeat_log_event>(ev);
      m_executor_cbk->execute_heartbeat_event(heartbeat_event);
    } break;
    default: {
      break;
    }
  }

  if (type_code != binary_log::ROTATE_EVENT &&
      type_code != binary_log::HEARTBEAT_LOG_EVENT) {
    m_last_pos = ev->common_header->log_pos;
  }
  return 0;
}

int Read_binlog_meta_applier::commit_trx() {
  assert(m_last_gtid_log_event != nullptr);
  int ret = 0;
  if (m_executor_cbk) {
    ret = m_executor_cbk->commit_trx();
    if (ret) {
      auto error = m_executor_cbk->error();
      set_error(error.empty() ? "commit trx error" : error);
    }
  }

  m_last_gtid_log_event = nullptr;
  return ret;
}

void Read_binlog_meta_applier::cleanup() {
  if (m_executor_cbk) {
    (void)m_executor_cbk->cleanup();
  }
}

void Read_binlog_meta_applier::set_executor_callback(
    Binlog_data_executor *executor) {
  m_executor_cbk = executor;
  m_row_sweeper.set_executor_callback(executor);
}

void Read_binlog_meta_applier::set_error(const std::string &err_msg) {
  m_err_msg = err_msg;
}
