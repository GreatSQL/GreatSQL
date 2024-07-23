/*
   Copyright (c) 2024, GreatDB Software Co., Ltd.
   All rights reserved.

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

#include "audit_log.h"
#include "mysql/udf_registration_types.h"
#include "mysqlpp/udf_wrappers.hpp"
#include "sql/current_thd.h"
#include "sql/server_component/gdb_cmd_service.h"
#include "sql/sql_class.h"

class audit_login_messages_impl {
 public:
  audit_login_messages_impl(mysqlpp::udf_context &ctx) {
    if (ctx.get_number_of_args() != 1)
      throw std::invalid_argument(
          "Function requires one argument for max rows");
    ctx.mark_arg_nullable(0, false);
    ctx.set_arg_type(0, INT_RESULT);

    uint max_rows = ctx.get_arg<INT_RESULT>(0).get();
    if (max_rows < 1 || max_rows > 10000)
      throw std::invalid_argument("Argument max rows should be in [1, 10000].");
  }
  ~audit_login_messages_impl() {}

  mysqlpp::udf_result_t<STRING_RESULT> calculate(
      const mysqlpp::udf_context &args);
};

mysqlpp::udf_result_t<STRING_RESULT> audit_login_messages_impl::calculate(
    const mysqlpp::udf_context &ctx MY_ATTRIBUTE((unused))) {
  THD *thd = current_thd;
  if (!thd) return {std::string{"Function error with non thread"}};

  Gdb_cmd_service cmd_service;
  std::string user_name(thd->security_context()->priv_user().str,
                        thd->security_context()->priv_user().length);
  user_name.append("@");
  user_name.append(thd->security_context()->priv_host().str,
                   thd->security_context()->priv_host().length);
  ulonglong login_time = thd->conn_start_time;
  uint max_rows = ctx.get_arg<INT_RESULT>(0).get();
  uint real_rows = 0;

  std::string sql, sql1, msg;
  bool error = false;
  sql =
      "SELECT MAX(timegmt) FROM sys_audit.audit_log WHERE name = 'Connect' "
      "AND timegmt < " +
      std::to_string(login_time) + " AND priv_user = '" + user_name +
      "' AND status = '0'";
  {
    if (cmd_service.execute_sql(sql)) {
      error = true;
      goto end;
    }
    auto &cb_data = cmd_service.get_cb_data();
    if (cb_data.is_error() || cb_data.rows() != 1) {
      error = true;
      goto end;
    }
    if (!strcmp(cb_data.get_value(0, 0).c_str(), "NULL")) {
      sql =
          "SELECT name, timestamp, connection_id, status, user, host, "
          "ip, server_id FROM sys_audit.audit_log WHERE name = 'Connect' AND "
          "priv_user = '" +
          user_name + "' AND timegmt < " + std::to_string(login_time) +
          " ORDER BY timegmt DESC LIMIT " + std::to_string(max_rows);
      sql1 =
          "SELECT COUNT(*) FROM sys_audit.audit_log WHERE name = 'Connect' AND "
          "priv_user = '" +
          user_name + "' AND timegmt < " + std::to_string(login_time);
    } else {
      sql =
          "SELECT name, timestamp, connection_id, status, user, host, "
          "ip, server_id FROM sys_audit.audit_log WHERE name = 'Connect' AND "
          "priv_user = '" +
          user_name + "' AND timegmt < " + std::to_string(login_time) +
          " AND timegmt >= " + cb_data.get_value(0, 0) +
          " ORDER BY timegmt DESC LIMIT " + std::to_string(max_rows);
      sql1 =
          "SELECT COUNT(*) FROM sys_audit.audit_log WHERE name = 'Connect' AND "
          "priv_user = '" +
          user_name + "' AND timegmt < " + std::to_string(login_time) +
          " AND timegmt >= " + cb_data.get_value(0, 0);
    }
  }
  {
    if (cmd_service.execute_sql(sql)) {
      error = true;
      goto end;
    }
    auto &cb_data = cmd_service.get_cb_data();
    if (cb_data.is_error()) {
      error = true;
      goto end;
    }
    if (cb_data.rows() == 0) {
      msg = "First login";
      goto end;
    }
    msg +=
        "| name | time | connection_id | status | user | host | ip "
        "| server_id |";
    for (uint i = 0; i < cb_data.rows(); i++) {
      msg = msg + "\n|";
      for (uint j = 0; j < cb_data.columns(); j++)
        msg = msg + " " + cb_data.get_value(i, j) + " |";
    }
    real_rows = cb_data.rows();
  }
  {
    if (real_rows == max_rows) {
      if (cmd_service.execute_sql(sql1)) {
        error = true;
        goto end;
      }
      auto &cb_data = cmd_service.get_cb_data();
      if (cb_data.is_error() || cb_data.rows() != 1) {
        error = true;
        goto end;
      }
      real_rows = std::stoul(cb_data.get_value(0, 0));
      if (real_rows > max_rows) {
        msg =
            msg +
            "\n| ...... |\n| Not showing all failed login attempts, plz check "
            "audit_log for detail. |";
      }
    }
  }

end:
  if (error) return {std::string{"Function failed with sql error"}};

  msg = msg + "\n| Total " + std::to_string(real_rows) + " rows |";
  return {msg};
}

DECLARE_STRING_UDF(audit_login_messages_impl, audit_login_messages)
