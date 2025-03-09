/*
   Copyright (c) 2023, 2025, GreatDB Software Co., Ltd. All rights
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
#define MYSQL_SERVER 1
#include "gdb_cmd_service.h"

/* MySQL header files */
#include "mysql/components/services/log_builtins.h"
#include "mysql/thread_pool_priv.h"
#include "sql/log.h"
#include "sql/sql_error.h"
#include "sql/srv_session.h"
#include "sql/thd_raii.h"

static int sql_start_result_metadata(void *ctx, uint num_cols, uint,
                                     const CHARSET_INFO *resultcs) {
  DBUG_ENTER("sql_start_result_metadata");
  auto *cb_data = static_cast<Gdb_cmd_cb_data *>(ctx);
  cb_data->start_result_metadata(num_cols, resultcs);
  DBUG_RETURN(false);
}

static int sql_field_metadata(void *ctx, struct st_send_field *field,
                              const CHARSET_INFO *) {
  DBUG_ENTER("sql_field_metadata");
  auto *cb_data = static_cast<Gdb_cmd_cb_data *>(ctx);
  cb_data->set_field_metadata(field);
  DBUG_RETURN(false);
}

static int sql_end_result_metadata(void *, uint, uint) {
  DBUG_ENTER("sql_end_result_metadata");
  DBUG_RETURN(false);
}

static int sql_start_row(void *ctx) {
  DBUG_ENTER("sql_start_row");
  auto *cb_data = static_cast<Gdb_cmd_cb_data *>(ctx);
  cb_data->start_row();
  DBUG_RETURN(false);
}

static int sql_end_row(void *ctx) {
  DBUG_ENTER("sql_end_row");
  auto *cb_data = static_cast<Gdb_cmd_cb_data *>(ctx);
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
  auto *cb_data = static_cast<Gdb_cmd_cb_data *>(ctx);
  cb_data->store(STRING_WITH_LEN("NULL"));
  DBUG_RETURN(false);
}

static int sql_get_integer(void *ctx, longlong value) {
  DBUG_ENTER("sql_get_integer");
  auto *cb_data = static_cast<Gdb_cmd_cb_data *>(ctx);
  auto s = std::to_string(value);
  cb_data->store(s.c_str(), s.length());
  DBUG_RETURN(false);
}

static int sql_get_longlong(void *ctx, longlong value, uint is_unsigned) {
  DBUG_ENTER("sql_get_longlong");
  auto *cb_data = static_cast<Gdb_cmd_cb_data *>(ctx);
  std::string s;
  if (is_unsigned) {
    s = std::to_string((ulonglong)value);
  } else {
    s = std::to_string(value);
  }
  cb_data->store(s.c_str(), s.length());
  DBUG_RETURN(false);
}

static int sql_get_decimal(void *, const decimal_t *) {
  DBUG_ENTER("sql_get_decimal");
  assert(0);
  DBUG_RETURN(false);
}

static int sql_get_double(void *, double, uint32) {
  DBUG_ENTER("sql_get_double");
  assert(0);
  DBUG_RETURN(false);
}

static int sql_get_date(void *, const MYSQL_TIME *) {
  DBUG_ENTER("sql_get_date");
  assert(0);
  DBUG_RETURN(false);
}

static int sql_get_time(void *, const MYSQL_TIME *, uint) {
  DBUG_ENTER("sql_get_time");
  assert(0);
  DBUG_RETURN(false);
}

static int sql_get_datetime(void *, const MYSQL_TIME *, uint) {
  DBUG_ENTER("sql_get_datetime");
  assert(0);
  DBUG_RETURN(false);
}

static int sql_get_string(void *ctx, const char *const value, size_t length,
                          const CHARSET_INFO *const) {
  DBUG_ENTER("sql_get_string");
  auto *cb_data = static_cast<Gdb_cmd_cb_data *>(ctx);
  cb_data->store(value, length);
  DBUG_RETURN(false);
}

static void sql_handle_ok(void *ctx, uint server_status,
                          uint statement_warn_count, ulonglong affected_rows,
                          ulonglong last_insert_id, const char *const message) {
  DBUG_ENTER("sql_handle_ok");

  auto *cb_data = static_cast<Gdb_cmd_cb_data *>(ctx);
  cb_data->handle_ok(server_status, statement_warn_count, affected_rows,
                     last_insert_id, message);
  DBUG_VOID_RETURN;
}

static void sql_handle_error(void *ctx, uint sql_errno,
                             const char *const err_msg,
                             const char *const sqlstate) {
  DBUG_ENTER("sql_handle_error");
  auto *cb_data = static_cast<Gdb_cmd_cb_data *>(ctx);
  cb_data->handle_error(sql_errno, err_msg, sqlstate);
  DBUG_VOID_RETURN;
}

static void sql_shutdown(void *ctx, int shutdown_server) {
  DBUG_ENTER("sql_shutdown");
  auto *cb_data = static_cast<Gdb_cmd_cb_data *>(ctx);
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

/*************************Gdb_cmd_cb_data Start***********************/
Gdb_cmd_cb_data::Gdb_cmd_cb_data()
    : m_num_cols(0),
      m_cur_row(0),
      m_resultcs(nullptr),
      m_is_error(false),
      m_is_shutdown(false) {}

void Gdb_cmd_cb_data::start_result_metadata(uint num_cols,
                                            const CHARSET_INFO *resultcs) {
  DBUG_TRACE;
  m_cur_row = 0;
  m_num_cols = num_cols;
  m_resultcs = resultcs;
  m_fields_meta.clear();
  m_result.clear();
}

void Gdb_cmd_cb_data::set_field_metadata(struct st_send_field *field) {
  DBUG_TRACE;
  m_fields_meta.emplace_back(Field_meta(field));
}
void Gdb_cmd_cb_data::start_row() {}
void Gdb_cmd_cb_data::end_row() { m_cur_row++; }

void Gdb_cmd_cb_data::store(const char *const value, size_t length) {
  std::string col_value(value, length);
  m_result[m_cur_row].push_back(col_value);
}

void Gdb_cmd_cb_data::handle_ok(uint server_status, uint statement_warn_count,
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

void Gdb_cmd_cb_data::handle_error(uint sql_errno, const char *const err_msg,
                                   const char *const sqlstate) {
  DBUG_TRACE;
  m_is_error = true;
  m_errno = sql_errno;
  m_errmsg = err_msg;
  m_sqlstate = sqlstate;
}

void Gdb_cmd_cb_data::handle_shutdown(int shutdown_server) {
  DBUG_TRACE;
  m_is_shutdown = true;
  m_shutdown = shutdown_server;
}
/*************************Gdb_cmd_cb_data End***********************/

/*************************Gdb_cmd_service Start***********************/
static const char *user_localhost = "localhost";
static const char *user_local = "127.0.0.1";
static const char *user_db = "";
static const char *user_privileged = "greatdb.sys";

static void switch_user(MYSQL_SESSION session, const char *user) {
  DBUG_TRACE;
  MYSQL_SECURITY_CONTEXT sc;
  thd_get_security_context(srv_session_info_get_thd(session), &sc);
  security_context_lookup(sc, user, user_localhost, user_local, user_db);
}

int Gdb_cmd_service::open_session() {
  DBUG_TRACE;
  {
    Gdb_admin_session_factory factory;
    m_st_session = factory.create(nullptr, nullptr);
  }
  if (!m_st_session) {
    LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG, "mysql_admin_session failed.");
    return true;
  } else {
    switch_user(m_st_session, user_privileged);
  }
  m_st_session->get_thd()->set_skip_audit_srv(true);

  if (m_keep_thd_rpl_channel) {
    THD *thd = thd_get_current_thd();
    if (thd) {
      m_st_session->get_thd()->rpl_thd_ctx.set_rpl_channel_type(
          thd->rpl_thd_ctx.get_rpl_channel_type());
    }
  }
  return false;
}

void Gdb_cmd_service::close_session() {
  DBUG_TRACE;
  if (m_st_session) {
    srv_session_close(m_st_session);
    m_st_session = nullptr;
  }
}

std::string Gdb_cmd_service::err_msg() { return m_err_msg; }

void Gdb_cmd_service::set_db(const char *db, size_t len) {
  if (len == 0 || db == nullptr) {
    m_st_session->get_thd()->set_db(NULL_CSTR);
  } else {
    LEX_CSTRING new_db;
    new_db.length = len;
    new_db.str = db;
    m_st_session->get_thd()->set_db(new_db);
  }
}

void Gdb_cmd_service::set_parallel_load_executor() {
  m_st_session->get_thd()->set_parallel_load_executor();
}

int Gdb_cmd_service::execute_sql(const char *sql, size_t len) {
  DBUG_TRACE;
  if (m_st_session == nullptr) {
    if (open_session() != false) return true;
  }

  COM_DATA cmd;
  DBUG_PRINT("info", ("command service execute sql %s", sql));
  cmd.com_query.query = sql;
  cmd.com_query.length = len;
  cmd.com_query.parameters = nullptr;
  cmd.com_query.parameter_count = 0;
  cb_data.reset();
  auto fail = command_service_run_command(
      m_st_session, COM_QUERY, &cmd, &my_charset_utf8mb3_general_ci, &sql_cbs,
      CS_TEXT_REPRESENTATION, &cb_data);

  if (fail || cb_data.is_error()) {
#ifndef DBUG_OFF
    DBUG_PRINT("info", ("command service execute error msg %s",
                        (fail ? "-1" : cb_data.error_msg().c_str())));
    char buf[2048];
    snprintf(buf, sizeof(buf),
             "Gdb_cmd_service execute_sql error sql[%s] error[%d %s], "
             "thread_id is [%d]",
             sql, fail ? fail : cb_data.error_no(), cb_data.error_msg().c_str(),
             m_st_session->get_thd()->thread_id());
    LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG, buf);
#endif
    return true;
  }
  return false;
}

int Gdb_cmd_service::execute_sqls(std::vector<std::string> &sqls) {
  DBUG_TRACE;
  int ret = false;
  for (auto &sql : sqls) {
    ret = execute_sql(sql);
    if (ret) break;
  }

  return ret;
}
