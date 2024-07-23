/* Copyright (c) 2023, 2024, GreatDB Software Co., Ltd.

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

#include "sql/secondary_engine_inc_load_task.h"

#include "my_dbug.h"
#include "mysql/components/services/bits/psi_thread_bits.h"
#include "mysql/components/services/log_builtins.h"
#include "mysql/plugin.h"
#include "mysqld_error.h"
#include "sql/binlog_base.h"
#include "sql/local_binlog_reader.h"
#include "sql/mysqld.h"
#include "sql/sql_base.h"
#include "sql/sql_lex.h"
#include "sql/sql_plugin.h"

static PSI_thread_key key_thread_secondary_engine_load_inc_task;

static std::string get_format_date_string() {
  struct tm tm_tmp;

  time_t skr = std::time(nullptr);
  localtime_r(&skr, &tm_tmp);

  char to[64];
  std::sprintf(to, "%4d-%02d-%02d %02d:%02d:%02d", tm_tmp.tm_year + 1900,
               tm_tmp.tm_mon + 1, tm_tmp.tm_mday, tm_tmp.tm_hour, tm_tmp.tm_min,
               tm_tmp.tm_sec);
  return std::string(to);
}

static bool delay_over_time(time_t check_time, time_t sync_time,
                            uint64_t threshold) {
  if (threshold == UINT64_MAX) return false;

  if (sync_time == 0) return false;  // no time delay

  if (check_time > sync_time &&
      (uint64_t)(check_time - sync_time) > threshold) {
    return true;
  }
  return false;
}

static bool delay_over_gtid(Gtid_set *check_gtid_set, Gtid_set *sync_gtid_set,
                            uint64_t threshold) {
  if (threshold == UINT64_MAX) return false;

  auto *check_sid_lock = check_gtid_set->get_sid_map()->get_sid_lock();
  assert(check_sid_lock == nullptr);
  auto *sync_sid_lock = sync_gtid_set->get_sid_map()->get_sid_lock();
  if (sync_sid_lock != nullptr) sync_sid_lock->wrlock();
  check_gtid_set->remove_gtid_set(sync_gtid_set);
  if (sync_sid_lock != nullptr) sync_sid_lock->unlock();
  return check_gtid_set->is_size_greater_than_or_equal(threshold + 1);
}

static int read_secondary_load_gtid(TABLE *table, const char *schema_name,
                                    const char *table_name,
                                    std::string &gtid_value, time_t &time,
                                    std::string &err);

void secondary_engine_task_shutdown() {
  auto &mngr = Secondary_engine_increment_load_task_manager::instance();
  mngr.shutdown_and_clear_all_inc_load_task();
}

enum_secondary_read_delay_cost_level can_read_from_secondary_engine(
    TABLE *table, const char *db_name, const char *table_name,
    uint64_t time_threshold, uint64_t gtid_threshold, uint64_t wait_timeout,
    ulong wait_mode, ulong read_delay_level) {
  auto &manager = Secondary_engine_increment_load_task_manager::instance();
  return manager.can_read_from_secondary_engine(
      table, db_name, table_name, time_threshold, gtid_threshold, wait_timeout,
      wait_mode, read_delay_level);
}

enum_secondary_read_delay_cost_level
Secondary_engine_increment_load_task_manager::can_read_from_secondary_engine(
    TABLE *table, const char *db_name, const char *table_name,
    uint64_t time_threshold, uint64_t gtid_threshold, uint64_t wait_timeout,
    ulong wait_mode, ulong read_delay_level) {
  bool find_inc_task = false;
  std::pair<std::string, std::string> full_table_name =
      std::make_pair(db_name, table_name);
  m_mutex.lock();
  auto it = m_inc_load_tasks.find(full_table_name);
  if (it != m_inc_load_tasks.end()) {
    find_inc_task = true;
  }
  m_mutex.unlock();
  bool task_running = true;
  if (!find_inc_task) {
    task_running = false;
  }

  bool first_check = true;

  time_t begin = std::time(nullptr);
  time_t check_timestamp = begin;
  time_t now = check_timestamp;
  global_sid_lock->wrlock();
  Sid_map sid_map(nullptr);
  Gtid_set check_gtid_set(&sid_map);
  auto status = check_gtid_set.add_gtid_set(gtid_state->get_executed_gtids());
  assert(status == RETURN_STATUS_OK);
  global_sid_lock->unlock();

  auto ret = SECONDARY_ENGINE_OK_TO_READ;
  while (true) {
    if (task_running && it->second->task_stopped()) {
      task_running = false;
    }
    if (!task_running) {
      break;
    }

    if (!first_check) {
      now = std::time(nullptr);
    }

    if (wait_mode == SECONDARY_ENGINE_WAIT_FOR_DB && !first_check) {
      check_timestamp = now;
      global_sid_lock->wrlock();
      status = check_gtid_set.add_gtid_set(gtid_state->get_executed_gtids());
      assert(status == RETURN_STATUS_OK);
      global_sid_lock->unlock();
    } else {
      first_check = false;
    }

    bool delay =
        delay_over_time(check_timestamp, it->second->get_sync_timestamp(),
                        time_threshold) ||
        delay_over_gtid(&check_gtid_set, it->second->get_sync_gtid_set(),
                        gtid_threshold);
    if (delay) {
      if (wait_timeout == 0 ||
          (now > begin && (uint64_t)(now - begin) >= wait_timeout)) {
        ret = SECONDARY_ENGINE_DELAY_TOO_MUCH;
        break;
      } else {
        usleep(10000);  // 10ms, 0.01s
        continue;
      }
    } else {
      ret = SECONDARY_ENGINE_OK_TO_READ;
      break;
    }
  }

  if (!task_running &&
      read_delay_level == SECONDARY_ENGINE_READ_DELAY_FOR_ALL_TABLE) {
    std::string gtid_value;
    time_t sync_time = 0;
    std::string err;
    if (read_secondary_load_gtid(table, db_name, table_name, gtid_value,
                                 sync_time, err)) {
      return SECONDARY_ENGINE_READ_DELAY_COST_ERROR;
    }

    Sid_map sync_sid_map(nullptr);
    Gtid_set sync_set(&sync_sid_map);
    status = sync_set.add_gtid_text(gtid_value.c_str());
    if (status != RETURN_STATUS_OK) {
      return SECONDARY_ENGINE_READ_DELAY_COST_ERROR;
    }

    bool delay = delay_over_time(check_timestamp, sync_time, time_threshold) ||
                 delay_over_gtid(&check_gtid_set, &sync_set, gtid_threshold);
    if (delay) {
      ret = SECONDARY_ENGINE_DELAY_TOO_MUCH;
    } else {
      ret = SECONDARY_ENGINE_OK_TO_READ;
    }
  }

  return ret;
}

int Secondary_engine_increment_load_task_manager::start_inc_load_task(
    const char *schema_name, const char *table_name, const char *gtid_value,
    String *str, bool auto_position) {
  DBUG_TRACE;
  int lower_case = lower_case_table_names;
#ifndef NDEBUG
  if (DBUG_EVALUATE_IF("test_ignore_lower_case_check", true, false)) {
    lower_case = 1;
  }
#endif
  if (!lower_case) {
    str->append(
        STRING_WITH_LEN("not support when lower_case_table_names == 0"));
    return 1;
  }
  std::string db_name(schema_name);
  std::string tbl_name(table_name);
  std::transform(db_name.begin(), db_name.end(), db_name.begin(), ::tolower);
  std::transform(tbl_name.begin(), tbl_name.end(), tbl_name.begin(), ::tolower);
  auto table = std::make_pair(db_name, tbl_name);
  m_mutex.lock();
  auto it = m_inc_load_tasks.find(table);
  if (it != m_inc_load_tasks.end() && !it->second->task_stopped()) {
    str->append("table task already running");
    m_mutex.unlock();
    return 1;
  }

  std::unique_ptr<Secondary_engine_inc_load_task> task;
  if (!auto_position) {
    assert(gtid_value != nullptr);
    task.reset(
        new Secondary_engine_inc_load_task(db_name, tbl_name, gtid_value));
  } else {
    assert(gtid_value == nullptr);
    task.reset(new Secondary_engine_inc_load_task(db_name, tbl_name));
  }
  int ret = task->start_task();
  if (!ret) {
    m_inc_load_tasks[table] = std::move(task);
    str->append(STRING_WITH_LEN("success"));
    std::string info_msg("Start econdary_engine binlog load task for table ");
    info_msg += schema_name;
    info_msg += ".";
    info_msg += table_name;
    LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, info_msg.c_str());
  } else {
    str->append(STRING_WITH_LEN("start task error"));
  }
  m_mutex.unlock();
  return ret;
}

int Secondary_engine_increment_load_task_manager::stop_inc_load_task(
    const char *schema_name, const char *table_name, String *str) {
  DBUG_TRACE;
  int lower_case = lower_case_table_names;
#ifndef NDEBUG
  if (DBUG_EVALUATE_IF("test_ignore_lower_case_check", true, false)) {
    lower_case = 1;
  }
#endif
  if (!lower_case) {
    str->append(
        STRING_WITH_LEN("not support when lower_case_table_names == 0"));
    return 1;
  }
  std::string db_name(schema_name);
  std::string tbl_name(table_name);
  std::transform(db_name.begin(), db_name.end(), db_name.begin(), ::tolower);
  std::transform(tbl_name.begin(), tbl_name.end(), tbl_name.begin(), ::tolower);
  auto table = std::make_pair(db_name, tbl_name);
  m_mutex.lock();
  auto it = m_inc_load_tasks.find(table);
  if (it == m_inc_load_tasks.end()) {
    m_mutex.unlock();
    str->append(STRING_WITH_LEN("table task not exists"));
    return 1;
  } else if (it->second->task_stopped()) {
    m_mutex.unlock();
    str->append(STRING_WITH_LEN("table task already stopped"));
    return 1;
  }
  auto task = it->second.get();
  m_mutex.unlock();
  task->stop_task();
  str->append(STRING_WITH_LEN("success"));
#ifndef NDEBUG
  if (DBUG_EVALUATE_IF("test_stop_load_task_rm_task", true, false)) {
    m_mutex.lock();
    m_inc_load_tasks.erase(it);
    m_mutex.unlock();
  }
#endif
  return 0;
}

void Secondary_engine_increment_load_task_manager::
    shutdown_and_clear_all_inc_load_task() {
  m_mutex.lock();
  m_inc_load_tasks.clear();
  m_mutex.unlock();
}

int Secondary_engine_increment_load_task_manager::read_load_gtid(
    const char *schema_name, const char *table_name, String *str) {
  DBUG_TRACE;
  int lower_case = lower_case_table_names;
#ifndef NDEBUG
  if (DBUG_EVALUATE_IF("test_ignore_lower_case_check", true, false)) {
    lower_case = 1;
  }
#endif
  if (!lower_case) {
    str->append(
        STRING_WITH_LEN("not support when lower_case_table_names == 0"));
    return 1;
  }
  std::string db_name(schema_name);
  std::string tbl_name(table_name);
  std::transform(db_name.begin(), db_name.end(), db_name.begin(), ::tolower);
  std::transform(tbl_name.begin(), tbl_name.end(), tbl_name.begin(), ::tolower);
  auto table = std::make_pair(db_name, tbl_name);
  m_mutex.lock();
  auto it = m_inc_load_tasks.find(table);
  if (it == m_inc_load_tasks.end()) {
    m_mutex.unlock();
    std::string gtid_value;
    time_t time;
    std::string err;
    int ret = read_secondary_load_gtid(nullptr, db_name.c_str(),
                                       tbl_name.c_str(), gtid_value, time, err);
    if (ret) {
      str->append(err.c_str(), err.size());
    } else {
      str->append(gtid_value.c_str(), gtid_value.size());
    }
  } else {
    auto task = it->second.get();
    m_mutex.unlock();
    auto *gtid_value = task->get_sync_gtid_set_str();
    str->append(gtid_value, strlen(gtid_value));
    my_free(gtid_value);
  }
  return 0;
}

Secondary_engine_inc_load_task::Secondary_engine_inc_load_task(
    const std::string &schema_name, const std::string &table_name,
    const char *gtid_value)
    : m_run_once(false),
      m_stop(false),
      m_schema_name(schema_name),
      m_table_name(table_name),
      m_start_gtid(gtid_value),
      m_auto_position(false),
      m_sid_map(&m_gtid_lock),
      m_gtid_set(&m_sid_map, &m_gtid_lock),
      m_sync_timestamp(0) {
  m_start_time = get_format_date_string();
}

Secondary_engine_inc_load_task::Secondary_engine_inc_load_task(
    const std::string &schema_name, const std::string &table_name)
    : m_run_once(false),
      m_stop(false),
      m_schema_name(schema_name),
      m_table_name(table_name),
      m_auto_position(true),
      m_sid_map(&m_gtid_lock),
      m_gtid_set(&m_sid_map, &m_gtid_lock),
      m_sync_timestamp(0) {
  m_start_time = get_format_date_string();
}

int Secondary_engine_inc_load_task::start_task() {
  DBUG_TRACE;
  {
    // check table
    THD *thd = current_thd;
    TABLE *table =
        open_table_for_task(thd, m_schema_name.c_str(), m_table_name.c_str());
    if (table == nullptr) {
      return 1;
    }
    close_table_for_task(thd);
  }
  if (m_auto_position) {
    assert(m_start_gtid.empty());
    if (read_auto_gtid()) {
      char errbuf[1024];
      sprintf(errbuf,
              "fail read auto gtid in secondary_engine_load_inc_task for table "
              "%s.%s",
              m_schema_name.c_str(), m_table_name.c_str());
      LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG, errbuf);
      return 1;
    }
  }
  m_gtid_lock.wrlock();
  auto status = m_gtid_set.add_gtid_text(m_start_gtid.c_str());
  m_gtid_lock.unlock();
  if (status != RETURN_STATUS_OK) {
    char errbuf[1024];
    sprintf(
        errbuf,
        "wrong gtid value %s in secondary_engine_load_inc_task for table %s.%s",
        m_start_gtid.c_str(), m_schema_name.c_str(), m_table_name.c_str());
    LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG, errbuf);
    return 1;
  }

  int err = mysql_thread_create(key_thread_secondary_engine_load_inc_task,
                                &m_handle, nullptr, thread_func, this);
  return err;
}

int Secondary_engine_inc_load_task::stop_task() {
  DBUG_TRACE;
  if (m_reader) {
    m_reader->stop();
  }
  my_thread_join(&m_handle, nullptr);
  return 0;
}

void *Secondary_engine_inc_load_task::thread_func(void *const thread_ptr) {
  DBUG_TRACE;
  assert(thread_ptr != nullptr);
  Secondary_engine_inc_load_task *const thread =
      static_cast<Secondary_engine_inc_load_task *>(thread_ptr);
  if (!thread->m_run_once.exchange(true)) {
    thread->run();
  }
  return nullptr;
}

int Secondary_engine_inc_load_task::run() {
  DBUG_TRACE;
  my_thread_init();
  THD *thd = new THD;
  if (thd == nullptr) {
    LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
           "memory error in Secondary_engine_inc_load_task");
    return 1;
  }
  thd->thread_stack = (char *)&thd;
  thd->store_globals();

  int ret = 0;
  Read_binlog_meta_applier *applier = nullptr;
  Local_tables_filter *filter = nullptr;
  Binlog_data_executor *executor = nullptr;

  // 1. init executor callback
#ifndef NDEBUG
  if (DBUG_EVALUATE_IF("test_local_binlog_reader_with_output", true, false)) {
    executor = new Output_test_data_executor();
  } else
#endif
  {
    TABLE *table =
        open_table_for_task(thd, m_schema_name.c_str(), m_table_name.c_str());
    if (table == nullptr) {
      ret = 1;
      goto end;
    }
    executor = register_engine_cbk(thd, table, &m_gtid_set, &m_sync_timestamp);
    if (executor == nullptr) {
      ret = 1;
      goto end;
    }
    close_table_for_task(thd);
  }

  // 2. init binlog applier
  applier = new Read_binlog_meta_applier(nullptr, 4);
  applier->set_executor_callback(executor);
  // 3. init table filter
  filter = new Local_tables_filter();
  filter->add_filter_table(m_schema_name, m_table_name);
  // 4. init reader
  m_reader.reset(new Local_binlog_reader(thd, nullptr, 4, &m_gtid_set, 0));
  m_reader->set_binlog_filter(filter);
  m_reader->set_binlog_applier(applier);
  m_reader->run();

end:
  if (ret) close_table_for_task(thd);
  if (filter != nullptr) delete filter;
  if (applier != nullptr) delete applier;
  if (executor != nullptr) delete executor;

  delete thd;
  my_thread_end();
  m_end_time = get_format_date_string();
  m_stop = true;

  if (has_error()) {
    std::string err_msg("Secondary_engine binlog load task for table ");
    err_msg += m_schema_name;
    err_msg += ".";
    err_msg += m_table_name;
    err_msg += " exit by error: ";
    err_msg += error();
    err_msg += ", error binlog gtid is: ";
    err_msg += get_last_gtid();
    err_msg += ", binlog file is: ";
    err_msg += get_last_file();
    err_msg += ", binlog pos is: ";
    err_msg += std::to_string(get_last_pos());
    LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG, err_msg.c_str());
  } else {
    std::string info_msg(
        "Secondary_engine binlog load task stopped for table ");
    info_msg += m_schema_name;
    info_msg += ".";
    info_msg += m_table_name;
    LogErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG, info_msg.c_str());
  }
  return ret;
}

Binlog_data_executor *Secondary_engine_inc_load_task::register_engine_cbk(
    THD *thd, TABLE *table, Gtid_set *gtid_set, time_t *time) {
  plugin_ref plugin =
      ha_resolve_by_name(thd, &table->s->secondary_engine, false);
  if ((plugin == nullptr) || !plugin_is_ready(table->s->secondary_engine,
                                              MYSQL_STORAGE_ENGINE_PLUGIN)) {
    LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG, "Not find second storage engine");
    return nullptr;
  }

  // The engine must support being used as a secondary engine.
  handlerton *hton = plugin_data<handlerton *>(plugin);
  if (!(hton->flags & HTON_IS_SECONDARY_ENGINE)) {
    LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
           "Unsupport secondary storage engine");
    return nullptr;
  }
  void *cbk = hton->secondary_engine_register_binlog_inc_executor_cbk(
      table, m_schema_name, m_table_name, gtid_set, time);
  if (cbk == nullptr) {
    LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
           "cannot register binlog_executor_cbk for secondary storage engine");
    return nullptr;
  }
  return (Binlog_data_executor *)cbk;
}

int Secondary_engine_inc_load_task::read_auto_gtid() {
  std::string err;
  int ret = read_secondary_load_gtid(nullptr, m_schema_name.c_str(),
                                     m_table_name.c_str(), m_start_gtid,
                                     m_sync_timestamp, err);
  if (ret) {
    LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG, err.c_str());
  }
  return ret;
}

int read_secondary_load_gtid(TABLE *table, const char *schema_name,
                             const char *table_name, std::string &gtid_value,
                             time_t &time, std::string &err) {
  bool open = false;
  if (table != nullptr) open = true;

  std::string full_table_name;
  full_table_name += schema_name;
  full_table_name += ".";
  full_table_name += table_name;
  THD *thd = current_thd;
  if (!open) {
    table = open_table_for_task(thd, schema_name, table_name);
    if (table == nullptr) return 1;
  }

  int ret = 0;
  do {
    assert(table != nullptr);
    plugin_ref plugin =
        ha_resolve_by_name(thd, &table->s->secondary_engine, false);
    if ((plugin == nullptr) || !plugin_is_ready(table->s->secondary_engine,
                                                MYSQL_STORAGE_ENGINE_PLUGIN)) {
      err = "Not find secondary engine for table: ";
      err += full_table_name;
      ret = 1;
      break;
    }

    // The engine must support being used as a secondary engine.
    handlerton *hton = plugin_data<handlerton *>(plugin);
    if (!(hton->flags & HTON_IS_SECONDARY_ENGINE)) {
      err = "Not support secondary engine for table: ";
      err += full_table_name;
      break;
    }

    ret = hton->secondary_engine_read_binlog_inc_gtid(
        table, schema_name, table_name, gtid_value, time);
    if (ret) {
      err += "Cannot read persist gtid of secondary engine for table: ";
      err += full_table_name;
      err += ". Please see more details on error log.";
    }
  } while (false);

  if (!open) close_table_for_task(thd);
  return ret;
}
