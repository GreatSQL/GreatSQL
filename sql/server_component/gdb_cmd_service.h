/*
   Copyright (c) 2023, GreatDB Software Co., Ltd. All rights
   reserved.

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
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

#ifndef GDB_CMD_SERVICE_INCLUDED
#define GDB_CMD_SERVICE_INCLUDED

/* C++ standard header files */
#include <list>
#include <map>
#include <vector>

/* MySQL header files */
#include <mysql/components/my_service.h>
#include <mysql/components/services/mysql_admin_session.h>
#include <mysql/plugin.h>
#include <mysql/service_srv_session_info.h>
#include <mysqld_error.h>
#include "m_ctype.h"
#include "my_dbug.h"
#include "my_inttypes.h"
#include "mysql_time.h"

class Gdb_cmd_cb_data {
 public:
  Gdb_cmd_cb_data();
  ~Gdb_cmd_cb_data() {}

  void reset() {
    m_num_cols = 0;
    m_cur_row = 0;
    m_resultcs = nullptr;
    m_result.clear();

    m_server_status = 0;
    m_warn_count = 0;
    m_affected_rows = 0;
    m_last_insert_id = 0;
    m_message = "";

    // err
    m_errno = 0;
    m_is_error = false;
    m_errmsg = "";
    m_sqlstate = "";

    m_is_shutdown = false;
  }

  struct Field_meta {
    Field_meta(struct st_send_field *field) {
      db_name = field->db_name;
      table_name = field->table_name;
      col_name = field->col_name;
      length = field->length;
      charsetnr = field->charsetnr;
      flags = field->flags;
      decimals = field->decimals;
      type = field->type;
    }
    std::string db_name;
    std::string table_name;
    std::string col_name;
    unsigned long length;
    unsigned int charsetnr;
    unsigned int flags;
    unsigned int decimals;
    enum_field_types type;
  };
  void start_result_metadata(uint num_cols, const CHARSET_INFO *resultcs);
  void set_field_metadata(struct st_send_field *field);
  void start_row();
  void end_row();

  void store(const char *const value, size_t length);

  void handle_ok(uint server_status, uint statement_warn_count,
                 ulonglong affected_rows, ulonglong last_insert_id,
                 const char *const message);

  void handle_error(uint sql_errno, const char *const err_msg,
                    const char *const sqlstate);

  void handle_shutdown(int shutdown_server);

  bool is_error() { return m_is_error; }
  uint error_no() { return m_errno; }
  std::string &error_msg() { return m_errmsg; }
  uint rows() { return m_cur_row; }
  std::string &get_value(uint row, uint field) { return m_result[row][field]; }
  ulonglong affected_rows() { return m_affected_rows; }

 private:
  uint m_num_cols;
  uint m_cur_row;
  const CHARSET_INFO *m_resultcs;
  std::vector<Field_meta> m_fields_meta;
  std::map<uint, std::vector<std::string>> m_result;

  // ok
  uint m_server_status;
  uint m_warn_count;
  ulonglong m_affected_rows;
  ulonglong m_last_insert_id;
  std::string m_message;

  // err
  bool m_is_error;
  uint m_errno;
  std::string m_errmsg;
  std::string m_sqlstate;

  // shutdown
  bool m_is_shutdown;
  bool m_shutdown;

  friend class Gdb_query_schedule_job;
  friend class Gdb_mgr_seeds_job;
};

class Gdb_cmd_service {
 public:
  Gdb_cmd_service(bool keep_thd_rpl_channel = false)
      : m_keep_thd_rpl_channel(keep_thd_rpl_channel), m_st_session(nullptr) {}
  ~Gdb_cmd_service() { close_session(); }
  int execute_sql(const char *sql, size_t len);
  int execute_sql(std::string sql) {
    return execute_sql(sql.c_str(), sql.length());
  }
  int execute_sqls(std::vector<std::string> &sqls);

  Gdb_cmd_cb_data &get_cb_data() { return cb_data; }

  int open_session();
  void close_session();

  inline void add_init_sql(const std::string &sql) {
    m_init_sqls.push_back(sql);
  }
  void set_db(const char *db, size_t len);
  void set_parallel_load_executor();
  std::string err_msg();

 private:
  int execute_query(const char *sql, size_t len);

 private:
  bool m_keep_thd_rpl_channel;
  MYSQL_SESSION m_st_session;
  Gdb_cmd_cb_data cb_data;
  std::string m_err_msg;

  std::vector<std::string> m_init_sqls;
};

class Gdb_admin_session_factory {
 public:
  Gdb_admin_session_factory()
      : m_registry{mysql_plugin_registry_acquire()},
        m_admin_session{"mysql_admin_session", m_registry} {}

  ~Gdb_admin_session_factory() { mysql_plugin_registry_release(m_registry); }

  MYSQL_SESSION create(srv_session_error_cb error_cb, void *context) {
    if (!m_admin_session.is_valid()) return nullptr;

    return m_admin_session->open(error_cb, context);
  }

 private:
  SERVICE_TYPE(registry) * m_registry;
  my_service<SERVICE_TYPE(mysql_admin_session)> m_admin_session;
};

#endif
