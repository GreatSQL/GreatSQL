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

#include <mysql_version.h>

#include "my_stacktrace.h"
#include "my_sys.h"
#include "mysql/plugin.h"

#include "mysql/components/my_service.h"
#include "mysql/components/services/log_builtins.h"

//#include "map_helpers.h"
#include "plan_baseline.h"
#include "psi.h"

#include "explain_query_serializer.h"
#include "udf_registration.h"
#include "workMgr.h"

using std::unique_ptr;
const char *plan_baseline_database = "greatdb_plan_baseline";
ulonglong explain_count = 0;

// using namespace greatdb;
const int plan_baseline_version = 0x0001;
SERVICE_TYPE(registry) *reg_srv = nullptr;
SERVICE_TYPE(log_builtins) *log_bi = nullptr;
SERVICE_TYPE(log_builtins_string) *log_bs = nullptr;

plan_baseline_variables global_plan_baseline_var;

// key : digest_hash+plan_name
gdb_collation_unordered_map<std::shared_ptr<std::string>,
                            std::unique_ptr<explain_query_result>>
    plan_hash_map(system_charset_info);
// key : query_sql
gdb_collation_unordered_map<std::shared_ptr<std::string>,
                            std::unique_ptr<explain_query_sql>>
    plan_sql_map(system_charset_info);
// store map's old data to delete old data
std::deque<ulonglong> map_id_list;

mysql_mutex_t lock_plan_hash_map;
mysql_mutex_t lock_plan_sql_map;
mysql_mutex_t lock_map_id_list;

MYSQL_SYSVAR_ULONG(
    refresh_interval, global_plan_baseline_var.refresh_interval,
    PLUGIN_VAR_OPCMDARG,
    "interval setting for store data to disk (in units of minutes)", nullptr,
    greatdb_plan_baseline::update_refresh_interval, 60 * 60, 1800, 3600, 0);

MYSQL_SYSVAR_BOOL(enable_summary, global_plan_baseline_var.enable_summary,
                  PLUGIN_VAR_OPCMDARG, "enable plan baseline data collection",
                  nullptr, update_plan_baseline_enable_summary, false);

MYSQL_SYSVAR_BOOL(enable_persistent, global_plan_baseline_var.enable_persistent,
                  PLUGIN_VAR_OPCMDARG,
                  "enable plan baseline data store to disk", nullptr,
                  greatdb_plan_baseline::update_plan_baseline_enable_persistent,
                  false);

MYSQL_SYSVAR_ULONG(max_rows_count, global_plan_baseline_var.max_rows_count,
                   PLUGIN_VAR_OPCMDARG, "max count of plan baseline data rows",
                   nullptr, update_max_rows_count, 3000, 1000, 20000, 1);

MYSQL_SYSVAR_ULONG(max_tables_count, global_plan_baseline_var.max_tables_count,
                   PLUGIN_VAR_OPCMDARG,
                   "max count of plan baseline persistent tables", nullptr,
                   greatdb_plan_baseline::update_max_tables_count, 24, 1, 50,
                   1);

SYS_VAR *plan_baseline_system_variables[] = {
    MYSQL_SYSVAR(refresh_interval),
    MYSQL_SYSVAR(enable_summary),
    MYSQL_SYSVAR(enable_persistent),
    MYSQL_SYSVAR(max_rows_count),
    MYSQL_SYSVAR(max_tables_count),
    nullptr  // END
};

static volatile bool inited;
static int show_status(THD *, SHOW_VAR *var, char *) {
  inited =
      greatdb_plan_baseline::WorkMgr::get_instance()->tasks_initialized.load();
  var->type = SHOW_BOOL;
  var->value = const_cast<char *>(reinterpret_cast<volatile char *>(&inited));
  var->scope = SHOW_SCOPE_GLOBAL;
  return 0;
}

static SHOW_VAR plan_baseline_status_vars[] = {
    {"plan_baseline_work", (char *)&show_status, SHOW_FUNC, SHOW_SCOPE_GLOBAL},
    {nullptr, nullptr, SHOW_UNDEF, SHOW_SCOPE_UNDEF}};
/*
  Plugin library descriptor
*/

static struct st_mysql_daemon plan_baseline_plugin = {
    MYSQL_DAEMON_INTERFACE_VERSION};

std::vector<std::string> split(const std::string &str, char delimiter,
                               int count) {
  std::vector<std::string> tokens;
  std::stringstream ss(str);
  std::string token;
  int i = 0;
  while (std::getline(ss, token, delimiter)) {
    if (i == count) break;
    tokens.push_back(token);
    i++;
  }
  return tokens;
}

void update_plan_baseline_enable_summary(MYSQL_THD, SYS_VAR *, void *var_ptr,
                                         const void *save) {
  bool enable = *static_cast<const bool *>(save);
  if (!enable) {
    Mutex_guard_plan guard(lock_plan_hash_map, lock_plan_sql_map,
                           lock_map_id_list);
    plan_hash_map.clear();
    plan_sql_map.clear();
    map_id_list.clear();
    explain_count = 0;
  }
  *static_cast<bool *>(var_ptr) = enable;

  global_plan_baseline_var.enable_summary = enable;
}

void update_max_rows_count(MYSQL_THD, SYS_VAR *, void *var_ptr,
                           const void *save) {
  ulong count = *static_cast<const ulong *>(save);
  *static_cast<ulong *>(var_ptr) = count;
  global_plan_baseline_var.max_rows_count = count;
}

// serialize explian access path
static bool gdb_plan_baseline_collect_explain(THD *thd,
                                              Query_expression *unit) {
  if (!global_plan_baseline_var.enable_summary) return false;

  if (check_if_all_db_is_system_schema(thd)) return false;

  // e.g: call sp and cursor doesn't have thd->m_digest
  if (!thd->m_digest) return false;

  return gdb_plan_baseline_collect_explain_impl(thd, unit);
}

static int plan_baseline_plugin_init(MYSQL_PLUGIN p MY_ATTRIBUTE((unused))) {
  DBUG_ENTER("plan_baseline_plugin_init");
  do {
    mysql_mutex_init(
        greatdb_plan_baseline::gdb_plan_baseline_plan_hash_map_mutex_key,
        &lock_plan_hash_map, MY_MUTEX_INIT_FAST);
    mysql_mutex_init(
        greatdb_plan_baseline::gdb_plan_baseline_plan_sql_map_mutex_key,
        &lock_plan_sql_map, MY_MUTEX_INIT_FAST);
    mysql_mutex_init(
        greatdb_plan_baseline::gdb_plan_baseline_plan_map_id_list_mutex_key,
        &lock_map_id_list, MY_MUTEX_INIT_FAST);
    if (init_logging_service_for_plugin(&reg_srv, &log_bi, &log_bs)) break;
    LogPluginErrMsg(INFORMATION_LEVEL, 0, "plan_baseline start Installation.");
    if (greatdb_plan_baseline::sql_service_interface_init()) break;
    LogPluginErrMsg(INFORMATION_LEVEL, 0, "plan_baseline Installation.SERVICE");
    greatdb_plan_baseline::init_greatdb_plan_baseline_psi_keys();
    LogPluginErrMsg(INFORMATION_LEVEL, 0, "plan_baseline Installation.PSI");
    if (greatdb_plan_baseline::register_udfs()) break;
    LogPluginErrMsg(INFORMATION_LEVEL, 0, "plan_baseline Installation.UDF");

    if (greatdb_plan_baseline::Init_pfs()) break;
    LogPluginErrMsg(INFORMATION_LEVEL, 0, "plan_baseline Installation.PFS");

    if (greatdb_plan_baseline::WorkMgr::get_instance()->InitWork()) break;

    greatdb_plan_baseline::gdb_plan_baseline_processor =
        gdb_plan_baseline_collect_explain;

    LogPluginErrMsg(INFORMATION_LEVEL, 0,
                    "plan_baseline Installation init task");
    LogPluginErrMsg(INFORMATION_LEVEL, 0,
                    "plan_baseline plugin Installation success!");
    DBUG_RETURN(0);
  } while (false);  // once

  DBUG_RETURN(1);
}

static int plan_baseline_plugin_check(MYSQL_PLUGIN p MY_ATTRIBUTE((unused))) {
  LogPluginErrMsg(INFORMATION_LEVEL, 0, "plan_baseline plugin check.");
  //  wait init over to uninstall

  uint i = 0;
  while (!greatdb_plan_baseline::WorkMgr::get_instance()->tasks_initialized) {
    my_sleep(1000 * 1000);
    if (i > 30) {
      LogPluginErrMsg(WARNING_LEVEL, 0, "plan_baseline plugin wait for init ");
      return 1;
    }
    i++;
  }
  return 0;
}

static int plan_baseline_plugin_deinit(MYSQL_PLUGIN p MY_ATTRIBUTE((unused))) {
  DBUG_ENTER("plan_baseline_plugin_deinit");
  greatdb_plan_baseline::gdb_plan_baseline_processor = nullptr;
  plan_hash_map.clear();
  plan_sql_map.clear();
  mysql_mutex_destroy(&lock_plan_hash_map);
  mysql_mutex_destroy(&lock_plan_sql_map);
  mysql_mutex_destroy(&lock_map_id_list);
  greatdb_plan_baseline::WorkMgr::get_instance()->DeinitWork();
  // greatdb_close_backend_thread();
  greatdb_plan_baseline::Deinit_pfs();
  greatdb_plan_baseline::unregister_udfs();
  greatdb_plan_baseline::sql_service_interface_deinit();
  LogPluginErrMsg(INFORMATION_LEVEL, 0, "plan_baseline plugin deinit ");
  deinit_logging_service_for_plugin(&reg_srv, &log_bi, &log_bs);
  DBUG_RETURN(0);
}

/* clang-format off */
mysql_declare_plugin(greatdb_plan_baseline_plugin)
{
  MYSQL_DAEMON_PLUGIN,
  &plan_baseline_plugin,
  "plan_baseline",                             /* plugin name */
  "Greatdb",                                      /* plugin author (for I_S.PLUGINS) */
  "Greatdb plan baseline collection",  /* general descriptive text (for I_S.PLUGINS) */
  PLUGIN_LICENSE_GPL,                   /* the plugin license (PLUGIN_LICENSE_XXX) */
  plan_baseline_plugin_init,               /* Plugin Init      */
  plan_baseline_plugin_check,           /* Plugin Check uninstall */
  plan_baseline_plugin_deinit,           /* Plugin Deinit    */
  plan_baseline_version,                   /* version */
  plan_baseline_status_vars,             /* status variables */
  plan_baseline_system_variables,    /* system variables */
  nullptr,                                          /* config options   */
  PLUGIN_OPT_ALLOW_EARLY,          /* flags            */
}
mysql_declare_plugin_end;
