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

#include <mysqld_error.h>
#include <vector>

#include "mysql/components/my_service.h"
#include "mysql/components/services/log_builtins.h"
#include "mysql/components/services/pfs_plugin_table_service.h"
#include "mysql/service_plugin_registry.h"
#include "storage/perfschema/pfs_instr_class.h"

#include "pfs.h"
#include "pfs_digest_sql_info.h"
#include "pfs_hist_sql_plan.h"
#include "pfs_sql_plan_baselines.h"
#include "plan_baseline.h"
#include "psi.h"

namespace greatdb_plan_baseline {

SERVICE_TYPE_NO_CONST(pfs_plugin_table_v1) *mysql_pfs_table = nullptr;
SERVICE_TYPE_NO_CONST(pfs_plugin_column_integer_v1) *mysql_pfscol_int = nullptr;
SERVICE_TYPE_NO_CONST(pfs_plugin_column_bigint_v1) *mysql_pfscol_bigint =
    nullptr;
SERVICE_TYPE_NO_CONST(pfs_plugin_column_timestamp_v2) *mysql_pfscol_timestamp =
    nullptr;
SERVICE_TYPE_NO_CONST(pfs_plugin_column_decimal_v1) *mysql_pfscol_decimal =
    nullptr;
SERVICE_TYPE_NO_CONST(pfs_plugin_column_string_v2) *mysql_pfscol_string =
    nullptr;
SERVICE_TYPE_NO_CONST(pfs_plugin_column_blob_v1) *mysql_pfscol_blob = nullptr;

std::vector<PFS_engine_table_share_proxy *> pfs_proxy_tables = {};

bool Init_pfs() {
  do {
    ACQUIRE_SERVICE_BY_NAME(reg_srv, mysql_pfs_table, pfs_plugin_table_v1);
    ACQUIRE_SERVICE_BY_NAME(reg_srv, mysql_pfscol_int,
                            pfs_plugin_column_integer_v1);
    ACQUIRE_SERVICE_BY_NAME(reg_srv, mysql_pfscol_bigint,
                            pfs_plugin_column_bigint_v1);
    ACQUIRE_SERVICE_BY_NAME(reg_srv, mysql_pfscol_timestamp,
                            pfs_plugin_column_timestamp_v2);
    ACQUIRE_SERVICE_BY_NAME(reg_srv, mysql_pfscol_decimal,
                            pfs_plugin_column_decimal_v1);
    ACQUIRE_SERVICE_BY_NAME(reg_srv, mysql_pfscol_string,
                            pfs_plugin_column_string_v2);
    ACQUIRE_SERVICE_BY_NAME(reg_srv, mysql_pfscol_blob,
                            pfs_plugin_column_blob_v1);

    if (!pfs_enabled) {
      LogPluginErrMsg(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
                      "greatdb_plan_baseline plugin init failed : "
                      "performance_schema config should be ON");
      break;
    }

    pfs_proxy_tables.clear();
    sql_plan_baselines::get_instance()->init();

    hist_sql_plan::get_instance()->init();

    digest_sql_info::get_instance()->init();

    pfs_proxy_tables.push_back(
        sql_plan_baselines::get_instance()->get_proxy_share());
    pfs_proxy_tables.push_back(
        hist_sql_plan::get_instance()->get_proxy_share());
    pfs_proxy_tables.push_back(
        digest_sql_info::get_instance()->get_proxy_share());

    auto err = mysql_pfs_table->add_tables(pfs_proxy_tables.data(),
                                           pfs_proxy_tables.size());
    if (err != 0) {
      LogPluginErrMsg(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
                      "create_pfs_tables failed :%d", err);
      break;
    }
    return false;
  } while (0);  // once
  Deinit_pfs();
  return true;
}

void Deinit_pfs() {
  sql_plan_baselines::get_instance()->deinit();
  hist_sql_plan::get_instance()->deinit();
  digest_sql_info::get_instance()->deinit();

  if (mysql_pfs_table)
    mysql_pfs_table->delete_tables(pfs_proxy_tables.data(),
                                   pfs_proxy_tables.size());
  pfs_proxy_tables.clear();
  RELEASE_SERVIE(reg_srv, mysql_pfs_table);
  RELEASE_SERVIE(reg_srv, mysql_pfscol_timestamp);
  RELEASE_SERVIE(reg_srv, mysql_pfscol_bigint);
  RELEASE_SERVIE(reg_srv, mysql_pfscol_decimal);
  RELEASE_SERVIE(reg_srv, mysql_pfscol_int);
  RELEASE_SERVIE(reg_srv, mysql_pfscol_string);
  RELEASE_SERVIE(reg_srv, mysql_pfscol_blob);
}

static int cbk_rnd_init(PSI_table_handle *handle, bool) {
  auto table = reinterpret_cast<Gdb_plan_baseline_pfs_table *>(handle);
  return (table->rnd_init());
}

static int cbk_rnd_next(PSI_table_handle *handle) {
  auto table = reinterpret_cast<Gdb_plan_baseline_pfs_table *>(handle);
  return (table->rnd_next());
}

static int cbk_rnd_pos(PSI_table_handle *handle) {
  auto table = reinterpret_cast<Gdb_plan_baseline_pfs_table *>(handle);
  return (table->rnd_pos());
}

static void cbk_reset_pos(PSI_table_handle *handle) {
  auto table = reinterpret_cast<Gdb_plan_baseline_pfs_table *>(handle);
  table->reset_pos();
}
static int cbk_read_column(PSI_table_handle *handle, PSI_field *field,
                           uint32_t index) {
  auto table = reinterpret_cast<Gdb_plan_baseline_pfs_table *>(handle);
  return (table->read_column_value(field, index));
}

static void cbk_close_table(PSI_table_handle *handle) {
  auto table = reinterpret_cast<Gdb_plan_baseline_pfs_table *>(handle);
  table->close();
  delete table;
}

Gdb_plan_baseline_pfs_base::Gdb_plan_baseline_pfs_base() {
  /* Must set for each table separately in derived classes. */
  m_table_def.m_table_name = "";
  m_table_def.m_table_name_length = 0;
  m_table_def.m_table_definition = "";

  /* Table information common for all. */
  m_table_def.m_ref_length = sizeof(uint32_t);
  m_table_def.m_acl = READONLY;
  m_table_def.delete_all_rows = nullptr;

  /* Initialize proxy table access methods. */
  auto &proxy_table = m_table_def.m_proxy_engine_table;

  /* Table open and close method. Open method must be set
  separately in each derived class. */
  proxy_table.open_table = nullptr;
  proxy_table.close_table = cbk_close_table;

  /* Table scan methods. */
  proxy_table.rnd_init = cbk_rnd_init;
  proxy_table.rnd_next = cbk_rnd_next;
  proxy_table.rnd_pos = cbk_rnd_pos;

  /* Read operation. */
  proxy_table.read_column_value = cbk_read_column;
  proxy_table.reset_position = cbk_reset_pos;

  /* No index scan. */
  proxy_table.index_init = nullptr;
  proxy_table.index_read = nullptr;
  proxy_table.index_next = nullptr;

  /* No write operation. */
  proxy_table.write_column_value = nullptr;
  proxy_table.write_row_values = nullptr;
  proxy_table.update_column_value = nullptr;
  proxy_table.update_row_values = nullptr;
  proxy_table.delete_row_values = nullptr;
}

}  // namespace greatdb_plan_baseline
