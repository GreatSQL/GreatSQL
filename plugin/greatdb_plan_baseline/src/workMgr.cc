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

#include <time.h>
#include <algorithm>
#include <array>
#include <boost/algorithm/string.hpp>
#include <exception>

#include <regex>

#include "mysql/components/services/log_builtins.h"
#include "plan_baseline_cmd_service.h"
#include "psi.h"
#include "workMgr.h"

extern time_t server_start_time;
#define PLAN_BASELINE_TABLES_INFO_FIELDS 4
namespace greatdb_plan_baseline {
const char *plan_baseline_tables[PLAN_BASELINE_TABLE_COUNT] = {
    "gdb_hist_sql_plan", "gdb_sql_plan_baselines", "gdb_digest_sql_info"};
const char *plan_baseline_tables_info = "gdb_sql_plan_baseline_table_info";
const char *plan_compare_result = "plan_compare_result";

bool WorkJob::Execute(plan_baseline_cmd_service &cmd_service, std::string time,
                      ulong max_tables_count, ulong tables_count) {
  // first: drop old tables
  if (tables_count >= max_tables_count) {
    int diff = tables_count - max_tables_count + 1;
    std::string name = m_name.substr(4);
    char buff[2048] = {0};
    memset(buff, 0, sizeof(buff));
    auto len = snprintf(buff, sizeof(buff), "select id,%s from %s.%s limit %d",
                        name.c_str(), plan_baseline_database,
                        plan_baseline_tables_info, diff);
    DBUG_PRINT("plan_baseline",
               ("select * from table %s", plan_baseline_tables_info));

    auto &cb_data = cmd_service.get_cb_data();
    if (cmd_service.execute_sql(buff, len) || cb_data.is_error()) {
      LogPluginErrMsg(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
                      "plan_baseline select id,%s from table %s failed: %d:%s",
                      name.c_str(), plan_baseline_tables_info,
                      cb_data.error_no(), cb_data.error_msg().c_str());
      return true;
    }
    if (!cb_data.empty()) {
      std::vector<ulong> table_rows_list;
      std::vector<std::string> table_info_list;
      for (uint i = 0; i < cb_data.rows(); i++) {
        ulong info_id = std::atoi(cb_data.get_value(i, 0)->c_str());
        table_rows_list.push_back(info_id);
        auto ptr = cb_data.get_value(i, 1);
        table_info_list.push_back(std::string(ptr->c_str()));
      }
      for (uint i = 0; i < table_rows_list.size(); i++) {
        ulong info_id = table_rows_list[i];
        auto ptr = table_info_list[i];
        // drop table
        memset(buff, 0, sizeof(buff));
        len = snprintf(buff, sizeof(buff), "drop table if exists %s.%s",
                       plan_baseline_database, ptr.c_str());
        DBUG_PRINT("plan_baseline", ("drop table %s", ptr.c_str()));

        cb_data = cmd_service.get_cb_data();
        if (cmd_service.execute_sql(buff, len) || cb_data.is_error()) {
          LogPluginErrMsg(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
                          "plan_baseline drop table %s failed: %d:%s",
                          ptr.c_str(), cb_data.error_no(),
                          cb_data.error_msg().c_str());
          return true;
        }
        // update gdb_sql_plan_baseline_table_info
        memset(buff, 0, sizeof(buff));
        len = snprintf(buff, sizeof(buff),
                       "update %s.%s set %s=null where id=%ld",
                       plan_baseline_database, plan_baseline_tables_info,
                       name.c_str(), info_id);
        DBUG_PRINT("plan_baseline",
                   ("update table %s", plan_baseline_tables_info));

        cb_data = cmd_service.get_cb_data();
        if (cmd_service.execute_sql(buff, len) || cb_data.is_error()) {
          LogPluginErrMsg(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
                          "plan_baseline update table %s failed: %d:%s",
                          plan_baseline_tables_info, cb_data.error_no(),
                          cb_data.error_msg().c_str());
          return true;
        }
      }
    }
  }

  // second:create table
  char sqlstr[4000] = {0};
  memset(sqlstr, 0, sizeof(sqlstr));
  auto len = snprintf(sqlstr, sizeof(sqlstr),
                      "create table if not exists %s.%s_%s as select * from "
                      "performance_schema.%s",
                      plan_baseline_database, m_name.c_str(), time.c_str(),
                      m_name.c_str());

  auto &cb_data = cmd_service.get_cb_data();
  if (cmd_service.execute_sql(sqlstr, len) || cb_data.is_error()) {
    LogPluginErrMsg(ERROR_LEVEL, ER_LOG_PRINTF_MSG, "exec failed {%s}",
                    m_name.c_str());
    return true;
  }

  return false;
}

void WorkMgr::initWorkJob() {
  // restart server run init wait server is ready
  while (!srv_session_server_is_available()) my_sleep(500);
  // not need check
  if (WorkMgr::get_instance()->InitCollect()) {
    return;
  }
  tasks_initialized.store(true);
  LogPluginErrMsg(SYSTEM_LEVEL, ER_LOG_PRINTF_MSG,
                  "plan_baseline init success");
}

int WorkMgr::get_tables_info_count() {
  plan_baseline_cmd_service cmd_service;
  char buff[2048] = {0};
  memset(buff, 0, sizeof(buff));
  auto len = snprintf(buff, sizeof(buff), "select count(*) from %s.%s",
                      plan_baseline_database, plan_baseline_tables_info);
  DBUG_PRINT("plan_baseline",
             ("select count(*) from %s.%s", plan_baseline_database,
              plan_baseline_tables_info));

  auto &cb_data = cmd_service.get_cb_data();
  if (cmd_service.execute_sql(buff, len) || cb_data.is_error()) {
    LogPluginErrMsg(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
                    "plan_baseline insert into table %s failed: %d:%s",
                    plan_baseline_tables_info, cb_data.error_no(),
                    cb_data.error_msg().c_str());
    return true;
  }
  auto ptr = cb_data.get_value(0, 0);
  if (!ptr) {
    LogPluginErrMsg(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
                    "plan_baseline get value failed!");
    return true;
  }
  m_tables_count.store(std::atoi(ptr->c_str()));
  return false;
}

// called from interval task
void WorkMgr::nextWorkJob() {
  if (WorkMgr::get_instance()->tasks_initialized.load()) {
    WorkMgr::get_instance()->nextTask();
  }
}

bool WorkMgr::InitWork() {
  if (!initialized.load()) {
    m_interval.store(global_plan_baseline_var.refresh_interval);
    m_enable_persistent.store(global_plan_baseline_var.enable_persistent);
    m_max_tables_count.store(global_plan_baseline_var.max_tables_count);

    m_task_pool = std::make_unique<ThreadPool>(
        gdb_plan_baseline_queue_mutex_key,
        gdb_plan_baseline_in_flight_mutex_key,
        gdb_plan_baseline_task_producers_psi_cond_key,
        gdb_plan_baseline_task_consumers_psi_cond_key,
        gdb_plan_baseline_task_in_flight_psi_cond_key);
    if (!m_task_pool) {
      return true;
    }

    m_thread = std::make_unique<Gdb_create_plan_tables>(
        m_interval, std::bind(&WorkMgr::initWorkJob, this),
        std::bind(&WorkMgr::nextWorkJob, this));
    if (m_thread) {
      m_thread->init(gdb_sql_plan_baselines_task_psi_mutex_key,
                     gdb_sql_plan_baselines_task_cond_key);
      if (m_thread->create_thread(gdb_sql_plan_baselines_task_psi_thread_key) !=
          0) {
        return true;
      }

      // success
    } else {
      return true;
    }

    initialized.store(true);
  }
  return false;
}

void WorkMgr::DeinitWork() {
  // wait work stop
  std::lock_guard lock(data_mutex);
  if (m_thread) {
    m_thread->signal(true);
    m_thread->join();
    m_thread->uninit();
    m_thread.reset(nullptr);
  }
  if (m_task_pool) {
    m_task_pool.reset(nullptr);
  }
  tasks_initialized.store(false);
  initialized.store(false);
}

bool WorkMgr::InitCollect() {
  m_tasks.clear();
  for (uint i = 0; i < PLAN_BASELINE_TABLE_COUNT; i++) {
    m_tasks.push_back(std::make_unique<WorkJob>(
        std::make_shared<std::string>(std::string(plan_baseline_tables[i]))));
  }

  LogPluginErrMsg(INFORMATION_LEVEL, 0, "init check successs");
  return false;
}

// called from create_plan_baseline_tables()
bool WorkMgr::NewTask() {
  if (!tasks_initialized.load()) {
    if (error_handler_hook != nullptr)
      (*error_handler_hook)(ER_PLAN_BASELINE_GENERATOR, "has not init", MYF(0));
    return true;
  }
  if (nextTask()) {
    if (error_handler_hook != nullptr)
      (*error_handler_hook)(ER_PLAN_BASELINE_GENERATOR,
                            "exec failed,please check", MYF(0));
    return true;
  }
  return false;
}

bool WorkMgr::nextTask() {
  if (createTask()) {
    return true;
  }

  return false;
}

static std::string get_string_addPrefixzero(int number) {
  auto result = std::to_string(number);
  if (result.length() < 2) result = "0" + result;
  return result;
}

static std::string get_time() {
  // get current time
  std::time_t current_time = std::time(nullptr);
  std::tm *time_info = std::localtime(&current_time);
  auto year = get_string_addPrefixzero(time_info->tm_year + 1900);
  auto month = get_string_addPrefixzero(time_info->tm_mon + 1);
  auto day = get_string_addPrefixzero(time_info->tm_mday);
  auto hour = get_string_addPrefixzero(time_info->tm_hour);
  auto minute = get_string_addPrefixzero(time_info->tm_min);
  auto second = get_string_addPrefixzero(time_info->tm_sec);
  std::string time_str = year + month + day + hour + minute + second;
  return time_str;
}

bool WorkMgr::createTask() {
  plan_baseline_cmd_service cmd_service;

  // get current time
  std::string time_str = get_time();

  // run exec all collect
  auto error_count = createTableTask(time_str);
  LogPluginErrMsg(SYSTEM_LEVEL, 0,
                  "plan_baseline create table error_count:%d compile",
                  error_count);

  // delete old tables from gdb_sql_plan_baseline_table_info
  error_count = dropTableTask(cmd_service);
  LogPluginErrMsg(SYSTEM_LEVEL, 0,
                  "plan_baseline delete from plan_baseline_tables_info "
                  "error_count:%d compile",
                  error_count);

  // insert data into gdb_sql_plan_baseline_table_info
  error_count = insertInfoTableTask(cmd_service, time_str);
  LogPluginErrMsg(SYSTEM_LEVEL, 0,
                  "plan_baseline insert into plan_baseline_tables_info "
                  "error_count:%d compile",
                  error_count);

  DBUG_PRINT("plan_baseline", ("end create table"));

  return false;
}

// insert into plan_baseline_tables_info
int WorkMgr::insertInfoTableTask(plan_baseline_cmd_service &cmd_service,
                                 std::string time_str) {
  char buff[2048] = {0};
  memset(buff, 0, sizeof(buff));
  auto len = snprintf(buff, sizeof(buff),
                      "insert into %s.%s "
                      "values(null, \"%s_%s\", \"%s_%s\", \"%s_%s\")",
                      plan_baseline_database, plan_baseline_tables_info,
                      plan_baseline_tables[0], time_str.c_str(),
                      plan_baseline_tables[1], time_str.c_str(),
                      plan_baseline_tables[2], time_str.c_str());
  DBUG_PRINT("plan_baseline",
             ("insert into table %s", plan_baseline_tables_info));

  auto &cb_data = cmd_service.get_cb_data();
  if (cmd_service.execute_sql(buff, len) || cb_data.is_error()) {
    LogPluginErrMsg(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
                    "plan_baseline insert into table %s failed: %d:%s",
                    plan_baseline_tables_info, cb_data.error_no(),
                    cb_data.error_msg().c_str());
    return true;
  }
  return false;
}

// It should keep m_max_tables_count for persistent tables.
int WorkMgr::dropTableTask(plan_baseline_cmd_service &cmd_service) {
  char buff[2048] = {0};
  memset(buff, 0, sizeof(buff));
  std::string hist_sql_plan = std::string(plan_baseline_tables[0]).substr(4);
  std::string sql_plan_baselines =
      std::string(plan_baseline_tables[1]).substr(4);
  std::string digest_sql_info = std::string(plan_baseline_tables[2]).substr(4);
  // delete from plan_baseline_tables_info
  auto len = snprintf(
      buff, sizeof(buff),
      "delete from %s.%s where %s is null and %s is null and %s is null",
      plan_baseline_database, plan_baseline_tables_info, hist_sql_plan.c_str(),
      sql_plan_baselines.c_str(), digest_sql_info.c_str());
  DBUG_PRINT("plan_baseline",
             ("delete from table %s", plan_baseline_tables_info));

  auto &cb_data = cmd_service.get_cb_data();
  if (cmd_service.execute_sql(buff, len) || cb_data.is_error()) {
    LogPluginErrMsg(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
                    "plan_baseline delete from table %s failed: %d:%s",
                    plan_baseline_tables_info, cb_data.error_no(),
                    cb_data.error_msg().c_str());
    return cb_data.error_no();
  }

  return false;
}

int WorkMgr::createTableTask(std::string time) {
  std::vector<std::future<bool>> results;
  if (get_tables_info_count()) return true;
  for (auto &task : m_tasks) {
    results.emplace_back(m_task_pool->enqueue([&](size_t) {
      plan_baseline_cmd_service local_cmd;
      return task->Execute(local_cmd, time, m_max_tables_count.load(),
                           m_tables_count.load());
    }));
  }
  m_task_pool->wait_until_empty();
  m_task_pool->wait_until_nothing_in_flight();
  int err_count = 0;
  try {
    for (auto &result : results) {
      if (result.get()) {
        err_count++;
      }
    }
  } catch (std::exception &e) {
    LogPluginErrMsg(ERROR_LEVEL, 0, "Task exception: %s", e.what());
    err_count++;
  } catch (...) {
    LogPluginErrMsg(ERROR_LEVEL, 0, "Task exception");
    err_count++;
  }

  return err_count;
}

/**
 * @brief Create plan_baseline tables init
 *
 * @param initid
 * @param args
 * @param message
 * @return true
 * @return false
 */
static bool create_plan_baseline_tables_init(UDF_INIT *initid, UDF_ARGS *,
                                             char *) {
  DBUG_ENTER("create_plan_baseline_tables_init");
  initid->maybe_null = false;
  initid->const_item = 0;
  initid->ptr = nullptr;

  if (WorkMgr::get_instance()->InitCollect()) {
    DBUG_RETURN(true);
  }

  DBUG_RETURN(false);
}

static void create_plan_baseline_tables_deinit(UDF_INIT *) {
  DBUG_ENTER("create_plan_baseline_tables_deinit");
  DBUG_VOID_RETURN;
}

static long long create_plan_baseline_tables(UDF_INIT *, UDF_ARGS *,
                                             unsigned char *is_null,
                                             unsigned char *error) {
  DBUG_ENTER("create_plan_baseline_tables");
  *error = 0;
  *is_null = 0;
  if (WorkMgr::get_instance()->NewTask()) {
    *error = 1;

    DBUG_RETURN(1);
  }

  DBUG_RETURN(0);
}

udf_descriptor gdb_create_plan_baseline_tables() {
  return {"gdb_create_plan_baseline_tables", Item_result::INT_RESULT,
          reinterpret_cast<Udf_func_any>(create_plan_baseline_tables),
          create_plan_baseline_tables_init, create_plan_baseline_tables_deinit};
}

void update_refresh_interval(MYSQL_THD, SYS_VAR *, void *var_ptr,
                             const void *save) {
  ulong interval = *static_cast<const ulong *>(save);
  *static_cast<ulong *>(var_ptr) = interval;
  global_plan_baseline_var.refresh_interval = interval;
  WorkMgr::get_instance()->UpdateInterval(interval);
}

void update_max_tables_count(MYSQL_THD, SYS_VAR *, void *var_ptr,
                             const void *save) {
  ulong count = *static_cast<const ulong *>(save);
  *static_cast<ulong *>(var_ptr) = count;
  global_plan_baseline_var.max_tables_count = count;
  WorkMgr::get_instance()->Update_max_tables_count(count);
}

void update_plan_baseline_enable_persistent(MYSQL_THD, SYS_VAR *, void *var_ptr,
                                            const void *save) {
  bool enable = *static_cast<const bool *>(save);
  // if it set from true to false,should store data first
  WorkMgr::get_instance()->Update_enable_persistent(
      enable, global_plan_baseline_var.enable_summary);
  *static_cast<bool *>(var_ptr) = enable;
  global_plan_baseline_var.enable_persistent = enable;
}
}  // namespace greatdb_plan_baseline
