/* Copyright (c) 2026, GreatDB Software Co., Ltd.

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

#include <string>
#ifndef MYSQL_SERVER
#define MYSQL_SERVER 1
#endif

#include "plan_baseline_cmd_service.h"

/* MySQL header files */
#include <mysql/components/services/mysql_admin_session.h>
#include "mysql/components/services/log_builtins.h"
#include "mysql/thread_pool_priv.h"
#include "plan_baseline.h"
#include "sql/srv_session.h"
#include "sql/thd_raii.h"
#include "udf_descriptor.h"

namespace greatdb_plan_baseline {

static SERVICE_TYPE_NO_CONST(mysql_admin_session) * admin_session_factory;

static int sql_start_result_metadata(void *ctx, uint num_cols, uint,
                                     const CHARSET_INFO *resultcs) {
  DBUG_ENTER("sql_start_result_metadata");
  auto *cb_data = static_cast<plan_baseline_cmd_cb_data *>(ctx);
  cb_data->start_result_metadata(num_cols, resultcs);
  DBUG_RETURN(false);
}

static int sql_field_metadata(void *ctx, struct st_send_field *field,
                              const CHARSET_INFO *) {
  DBUG_ENTER("sql_field_metadata");
  auto *cb_data = static_cast<plan_baseline_cmd_cb_data *>(ctx);
  cb_data->set_field_metadata(field);
  DBUG_RETURN(false);
}

static int sql_end_result_metadata(void *, uint, uint) {
  DBUG_ENTER("sql_end_result_metadata");
  DBUG_RETURN(false);
}

static int sql_start_row(void *ctx) {
  DBUG_ENTER("sql_start_row");
  auto *cb_data = static_cast<plan_baseline_cmd_cb_data *>(ctx);
  cb_data->start_row();
  DBUG_RETURN(false);
}

static int sql_end_row(void *ctx) {
  DBUG_ENTER("sql_end_row");
  auto *cb_data = static_cast<plan_baseline_cmd_cb_data *>(ctx);
  cb_data->end_row();
  DBUG_RETURN(false);
}

static void sql_abort_row(void *) {
  DBUG_ENTER("sql_abort_row");
  DBUG_VOID_RETURN;
}

static ulong sql_get_client_capabilities(void *) {
  DBUG_ENTER("sql_get_client_capabilities");
  DBUG_RETURN(0);
}

static int sql_get_null(void *ctx) {
  DBUG_ENTER("sql_get_null");
  auto *cb_data = static_cast<plan_baseline_cmd_cb_data *>(ctx);
  cb_data->store_null();
  DBUG_RETURN(false);
}

static int sql_get_integer(void *ctx, longlong value) {
  DBUG_ENTER("sql_get_integer");
  auto *cb_data = static_cast<plan_baseline_cmd_cb_data *>(ctx);
  auto s = std::to_string(value);
  cb_data->store(s.c_str(), s.length());
  DBUG_RETURN(false);
}

static int sql_get_longlong(void *ctx, longlong value, uint is_unsigned) {
  DBUG_ENTER("sql_get_longlong");
  auto *cb_data = static_cast<plan_baseline_cmd_cb_data *>(ctx);
  std::string s;
  if (is_unsigned) {
    s = std::to_string((ulonglong)value);
  } else {
    s = std::to_string(value);
  }
  cb_data->store(s.c_str(), s.length());
  DBUG_RETURN(false);
}

static int sql_get_decimal(void *ctx, const decimal_t *value) {
  DBUG_ENTER("sql_get_decimal");
  auto *cb_data = static_cast<plan_baseline_cmd_cb_data *>(ctx);
  if (!value) {
    cb_data->store_null();
  } else {
    int len = 256;
    char buff[256] = {0};
    decimal2string(value, buff, &len);
    cb_data->store(buff, len);
  }
  DBUG_RETURN(false);
}

static int sql_get_double(void *ctx, double value, uint32) {
  DBUG_ENTER("sql_get_double");

  auto *cb_data = static_cast<plan_baseline_cmd_cb_data *>(ctx);
  char buffer[256] = {0};
  size_t len = snprintf(buffer, sizeof(buffer), "%3.7g", value);
  cb_data->store(buffer, len);
  DBUG_RETURN(false);
}

static int sql_get_date(void *ctx, const MYSQL_TIME *value) {
  DBUG_ENTER("sql_get_date");

  auto *cb_data = static_cast<plan_baseline_cmd_cb_data *>(ctx);
  char buffer[31] = {0};

  size_t len =
      snprintf(buffer, sizeof(buffer), "%s%4d-%02d-%02d", value->neg ? "-" : "",
               value->year, value->month, value->day);

  cb_data->store(buffer, len);
  DBUG_RETURN(false);
}

static int sql_get_time(void *ctx, const MYSQL_TIME *value, uint) {
  DBUG_ENTER("sql_get_time");
  auto *cb_data = static_cast<plan_baseline_cmd_cb_data *>(ctx);
  char buffer[31] = {0};

  size_t len = snprintf(
      buffer, sizeof(buffer), "%s%02d:%02d:%02d", value->neg ? "-" : "",
      value->day ? (value->day * 24 + value->hour) : value->hour, value->minute,
      value->second);
  cb_data->store(buffer, len);
  DBUG_RETURN(false);
}

static int sql_get_datetime(void *ctx, const MYSQL_TIME *value, uint) {
  DBUG_ENTER("sql_get_datetime");
  auto *cb_data = static_cast<plan_baseline_cmd_cb_data *>(ctx);
  char buffer[31] = {0};

  size_t len =
      snprintf(buffer, sizeof(buffer), "%s%4d-%02d-%02d %02d:%02d:%02d",
               value->neg ? "-" : "", value->year, value->month, value->day,
               value->hour, value->minute, value->second);
  cb_data->store(buffer, len);
  DBUG_RETURN(false);
}

static int sql_get_string(void *ctx, const char *const value, size_t length,
                          const CHARSET_INFO *const) {
  DBUG_ENTER("sql_get_string");
  auto *cb_data = static_cast<plan_baseline_cmd_cb_data *>(ctx);
  cb_data->store(value, length);
  DBUG_RETURN(false);
}

static void sql_handle_ok(void *ctx, uint server_status,
                          uint statement_warn_count, ulonglong affected_rows,
                          ulonglong last_insert_id, const char *const message) {
  DBUG_ENTER("sql_handle_ok");

  auto *cb_data = static_cast<plan_baseline_cmd_cb_data *>(ctx);
  cb_data->handle_ok(server_status, statement_warn_count, affected_rows,
                     last_insert_id, message);
  DBUG_VOID_RETURN;
}

static void sql_handle_error(void *ctx, uint sql_errno,
                             const char *const err_msg,
                             const char *const sqlstate) {
  DBUG_ENTER("sql_handle_error");
  auto *cb_data = static_cast<plan_baseline_cmd_cb_data *>(ctx);
  cb_data->handle_error(sql_errno, err_msg, sqlstate);
  DBUG_VOID_RETURN;
}

static void sql_shutdown(void *ctx, int shutdown_server) {
  DBUG_ENTER("sql_shutdown");
  auto *cb_data = static_cast<plan_baseline_cmd_cb_data *>(ctx);
  cb_data->handle_shutdown(shutdown_server);
  DBUG_VOID_RETURN;
}

const struct st_command_service_cbs sql_cbs = {
    sql_start_result_metadata,
    sql_field_metadata,
    sql_end_result_metadata,
    sql_start_row,
    sql_end_row,
    sql_abort_row,
    sql_get_client_capabilities,
    sql_get_null,
    sql_get_integer,
    sql_get_longlong,
    sql_get_decimal,
    sql_get_double,
    sql_get_date,
    sql_get_time,
    sql_get_datetime,
    sql_get_string,
    sql_handle_ok,
    sql_handle_error,
    sql_shutdown,
    nullptr,
};

/*************************plan_baseline_cmd_cb_data
 * Start***********************/
plan_baseline_cmd_cb_data::plan_baseline_cmd_cb_data() { reset(); }

void plan_baseline_cmd_cb_data::reset() {
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
  m_shutdown = 0;
}

void plan_baseline_cmd_cb_data::start_result_metadata(
    uint num_cols, const CHARSET_INFO *resultcs) {
  DBUG_TRACE;
  m_cur_row = 0;
  m_num_cols = num_cols;
  m_resultcs = resultcs;
  m_fields_meta.clear();
  m_result.clear();
}

void plan_baseline_cmd_cb_data::set_field_metadata(
    struct st_send_field *field) {
  DBUG_TRACE;
  m_fields_meta.emplace_back(Field_meta(field));
}
void plan_baseline_cmd_cb_data::start_row() {}
void plan_baseline_cmd_cb_data::end_row() { m_cur_row++; }

void plan_baseline_cmd_cb_data::store_null() {
  m_result[m_cur_row].push_back(nullptr);
}

void plan_baseline_cmd_cb_data::store(const char *const value, size_t length) {
  m_result[m_cur_row].emplace_back(
      std::make_shared<std::string>(value, length));
}

void plan_baseline_cmd_cb_data::handle_ok(uint server_status,
                                          uint statement_warn_count,
                                          ulonglong affected_rows,
                                          ulonglong last_insert_id,
                                          const char *const message) {
  DBUG_TRACE;
  m_server_status = server_status;
  m_warn_count = statement_warn_count;
  m_affected_rows = affected_rows;
  m_last_insert_id = last_insert_id;
  if (message) {
    m_message = message;
  }
}

void plan_baseline_cmd_cb_data::handle_error(uint sql_errno,
                                             const char *const err_msg,
                                             const char *const sqlstate) {
  DBUG_TRACE;
  m_is_error = true;
  m_errno = sql_errno;
  m_errmsg = err_msg;
  m_sqlstate = sqlstate;
}

void plan_baseline_cmd_cb_data::handle_shutdown(int shutdown_server) {
  DBUG_TRACE;
  m_is_shutdown = true;
  m_shutdown = shutdown_server;
}
/*************************Gdb_cmd_cb_data End***********************/

/*************************Gdb_cmd_service Start***********************/
static const char user_localhost[] = "localhost";
static const char user_local[] = "127.0.0.1";
static const char user_privileged[] = "greatdb.sys";

static void switch_user(MYSQL_SESSION session) {
  DBUG_TRACE;
  MYSQL_SECURITY_CONTEXT sc;
  thd_get_security_context(srv_session_info_get_thd(session), &sc);
  security_context_lookup(sc, user_privileged, user_localhost, user_local, "");
}

static void srv_session_error_handler(void *, unsigned int sql_errno,
                                      const char *err_msg) {
  LogPluginErrMsg(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
                  "srv_session_error_handler : %d:%s", sql_errno, err_msg);
}

int plan_baseline_cmd_service::open_session() {
  DBUG_TRACE;
  if (!admin_session_factory) {
    LogPluginErrMsg(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
                    "admin_session_factory not init.");
    return 1;
  }

  m_st_session =
      admin_session_factory->open(srv_session_error_handler, nullptr);
  if (!m_st_session) {
#ifndef NDEBUG
    LogPluginErrMsg(ERROR_LEVEL, ER_LOG_PRINTF_MSG, "srv_session_open failed.");
#endif
    return 1;
  } else {
    switch_user(m_st_session);
  }

  /* should use sql_log_bin=0 */
  if (execute_query(STRING_WITH_LEN("set session sql_log_bin=0"))) {
    close_session();
    return 1;
  }

  /* should use auto_commit=on */
  if (execute_query(STRING_WITH_LEN("SET SESSION AUTOCOMMIT=ON"))) {
    close_session();
    return 1;
  }

  /* should use sql_mode='' */
  if (execute_query(STRING_WITH_LEN("SET SESSION SQL_MODE=''"))) {
    close_session();
    return 1;
  }

  for (auto &sql : m_init_sqls) {
    if (execute_query(sql.c_str(), sql.size())) {
      close_session();
      return 1;
    }
  }

  return 0;
}

void plan_baseline_cmd_service::close_session() {
  DBUG_TRACE;
  if (m_st_session) {
    srv_session_close(m_st_session);
    m_st_session = nullptr;
  }
}

std::string plan_baseline_cmd_service::err_msg() { return m_err_msg; }

void plan_baseline_cmd_service::set_db(const char *db, size_t len) {
  if (len == 0 || db == nullptr) {
    m_st_session->get_thd()->set_db(NULL_CSTR);
  } else {
    LEX_CSTRING new_db;
    new_db.length = len;
    new_db.str = db;
    m_st_session->get_thd()->set_db(new_db);
  }
}

THD *plan_baseline_cmd_service::get_thd() { return m_st_session->get_thd(); }

int plan_baseline_cmd_service::execute_sql(const char *sql, size_t len) {
  DBUG_TRACE;
  if (m_st_session == nullptr) {
    if (open_session() != 0) return 1;
  }

  return execute_query(sql, len);
}

int plan_baseline_cmd_service::execute_sqls(std::vector<std::string> &sqls) {
  DBUG_TRACE;
  int ret = 0;
  for (auto &sql : sqls) {
    ret = execute_sql(sql);
    if (ret) break;
  }

  return ret;
}

int plan_baseline_cmd_service::execute_query(const char *sql, size_t len) {
  DBUG_TRACE;
  assert(m_st_session != nullptr);

  COM_DATA cmd;
  memset(&cmd, 0, sizeof(cmd));
  DBUG_PRINT("info", ("command service execute sql %s", sql));

  cmd.com_query.query = sql;
  cmd.com_query.length = len;
  cb_data.reset();
  auto fail = command_service_run_command(
      m_st_session, COM_QUERY, &cmd, &my_charset_utf8mb3_general_ci, &sql_cbs,
      CS_TEXT_REPRESENTATION, &cb_data);

  if (fail || cb_data.is_error()) {
    DBUG_PRINT("info", ("command service execute error msg %s",
                        (fail ? "-1" : cb_data.error_msg().c_str())));
    std::string err("Gdb_cmd_service execute_sql error sql [");
    err += sql;
    err += "] error[";
    err += std::to_string(fail ? fail : cb_data.error_no());
    err += " ";
    err += cb_data.error_msg().c_str();
    err += ", thread_id is [";
    err += std::to_string(m_st_session->get_thd()->thread_id());
    err += "]";
    if (cb_data.m_is_shutdown)
      err += ",session is killed " + std::to_string(cb_data.m_shutdown);
    LogErr(WARNING_LEVEL, ER_LOG_PRINTF_MSG, err.c_str());
    return 1;
  }
  return 0;
}

/*************************Gdb_cmd_service End***********************/

bool sql_service_interface_init() {
  do {
    ACQUIRE_SERVICE_BY_NAME(reg_srv, admin_session_factory,
                            mysql_admin_session);
    return false;
  } while (0);
  return true;
}

bool sql_service_interface_deinit() {
  RELEASE_SERVIE(reg_srv, admin_session_factory);
  return false;
}

}  // namespace greatdb_plan_baseline
