/* Copyright (c) 2023, 2025, GreatDB Software Co., Ltd.

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

#ifndef BINLOG_READER_TASK
#define BINLOG_READER_TASK

#include "sql/local_binlog_reader.h"
#include "sql/rpl_gtid.h"
#include "sql/sql_class.h"

#include <map>
#include <memory>
#include <string>

enum enum_secondary_read_delay_cost_level {
  SECONDARY_ENGINE_OK_TO_READ = 0,
  SECONDARY_ENGINE_DELAY_TOO_MUCH,
  SECONDARY_ENGINE_READ_DELAY_COST_ERROR,
};

enum enum_secondary_engine_wait_mode_level {
  SECONDARY_ENGINE_WAIT_FOR_DB = 0,
  SECONDARY_ENGINE_WAIT_FOR_TRX,
};

enum enum_secondary_read_delay_level {
  SECONDARY_ENGINE_READ_DELAY_FOR_ALL_TABLE,
  SECONDARY_ENGINE_READ_DELAY_FOR_TABLE_START_INC_TASK,
};

void secondary_engine_task_shutdown();

enum_secondary_read_delay_cost_level can_read_from_secondary_engine(
    TABLE *table, const char *db_name, const char *table_name,
    uint64_t time_threshold, uint64_t gtid_threshold, uint64_t wait_timeout,
    ulong wait_mode, ulong read_delay_level);

class Secondary_engine_inc_load_task;
class Secondary_engine_increment_load_task_manager {
 public:
  Secondary_engine_increment_load_task_manager(
      const Secondary_engine_increment_load_task_manager &) = delete;
  Secondary_engine_increment_load_task_manager(
      Secondary_engine_increment_load_task_manager &&) = delete;
  Secondary_engine_increment_load_task_manager &operator=(
      const Secondary_engine_increment_load_task_manager &) = delete;
  Secondary_engine_increment_load_task_manager &operator=(
      Secondary_engine_increment_load_task_manager &&) = delete;
  Secondary_engine_increment_load_task_manager() = default;

  static Secondary_engine_increment_load_task_manager &instance() {
    static Secondary_engine_increment_load_task_manager task_manager;
    return task_manager;
  }

  int start_inc_load_task(const char *schema_name, const char *table_name,
                          const char *gtid, String *str, bool auto_position,
                          bool strict_mode);
  int stop_inc_load_task(const char *schema_name, const char *table_name,
                         String *str);
  void shutdown_and_clear_all_inc_load_task();
  int read_load_gtid(const char *schema_name, const char *table_name,
                     String *str);
  enum_secondary_read_delay_cost_level can_read_from_secondary_engine(
      TABLE *table, const char *db_name, const char *table_name,
      uint64_t time_threshold, uint64_t gtid_threshold, uint64_t wait_timeout,
      ulong wait_mode, ulong read_delay_level);

  void lock() { m_mutex.lock(); }
  void unlock() { m_mutex.unlock(); }

  std::map<std::pair<std::string, std::string>,
           std::unique_ptr<Secondary_engine_inc_load_task>>
      *get_all_tasks() {
    return &m_inc_load_tasks;
  }

 private:
  std::mutex m_mutex;
  std::map<std::pair<std::string, std::string>,
           std::unique_ptr<Secondary_engine_inc_load_task>>
      m_inc_load_tasks;
};

class Secondary_engine_inc_load_task {
 public:
  Secondary_engine_inc_load_task(const std::string &schema_name,
                                 const std::string &table_name,
                                 const char *gtid);
  Secondary_engine_inc_load_task(const std::string &schema_name,
                                 const std::string &table_name);
  ~Secondary_engine_inc_load_task() = default;

  int start_task(bool strict_mode);
  int stop_task();

  bool task_stopped() { return m_stop; }
  const std::string &start_gtid() { return m_start_gtid; }
  const std::string &start_time() { return m_start_time; }
  const std::string &end_time() { return m_end_time; }

  std::string get_last_file() {
    return (m_reader == nullptr) ? "" : m_reader->get_last_file();
  }
  uint64_t get_last_pos() {
    return (m_reader == nullptr) ? 0 : m_reader->get_last_pos();
  }
  std::string get_last_gtid() {
    return (m_reader == nullptr) ? "" : m_reader->get_last_gtid();
  }
  bool has_error() {
    if (m_reader) return m_reader->has_error();
    if (!m_errmsg.empty()) return true;
    return false;
  }
  const char *error() {
    if (m_reader) return m_reader->error();
    if (!m_errmsg.empty()) return m_errmsg.c_str();
    return "";
  }

  char *get_sync_gtid_set_str() {
    char *buf = nullptr;
    m_gtid_set.to_string(&buf, true);
    return buf;
  }
  uint32_t get_sync_timestamp() { return m_sync_timestamp; }
  Gtid_set *get_sync_gtid_set() { return &m_gtid_set; }

 private:
  int run();
  static void *thread_func(void *const thread_ptr);

  Binlog_data_executor *register_engine_cbk(THD *thd, TABLE *table,
                                            Gtid_set *gtid_set, time_t *time);
  int read_auto_gtid();

 private:
  std::unique_ptr<Local_binlog_reader> m_reader;
  std::atomic_bool m_run_once;
  my_thread_handle m_handle;
  bool m_stop;
  std::string m_errmsg;
  std::string m_warning;

  std::string m_schema_name;
  std::string m_table_name;
  std::string m_start_gtid;
  bool m_auto_position;
  std::mutex m_gtid_mutex;
  Checkable_rwlock m_gtid_lock;
  Sid_map m_sid_map;
  Gtid_set m_gtid_set;
  time_t m_sync_timestamp;

  std::string m_start_time;
  std::string m_end_time;

  friend class Secondary_engine_increment_load_task_manager;
};

#endif  // BINLOG_READER_TASK
