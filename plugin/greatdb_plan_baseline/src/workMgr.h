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

#ifndef PLUGIN_GDB_PLAN_BASELINE_WORK_MGR_H
#define PLUGIN_GDB_PLAN_BASELINE_WORK_MGR_H
#include <boost/algorithm/string.hpp>
#include <functional>
#include <mutex>
#include <regex>
#include <thread>
#include "plan_baseline.h"
#include "plan_baseline_cmd_service.h"
#include "plan_baseline_thread.h"

extern "C" MYSQL_PLUGIN_IMPORT ulong server_id;

namespace greatdb_plan_baseline {
class WorkJob {
  std::string m_name;

 public:
  WorkJob(std::shared_ptr<std::string> tbname) : m_name(*tbname) {}

  bool Execute(plan_baseline_cmd_service &cmd_service, std::string time,
               ulong max_tables_count, ulong tables_count);
};

class WorkMgr {
 public:
  static WorkMgr *get_instance() {
    static WorkMgr instance;
    return &instance;
  }
  std::atomic<bool> initialized;
  std::atomic<bool> tasks_initialized;

  bool InitWork();
  void DeinitWork();
  void UpdateInterval(ulong val) {
    m_interval.store(val);
    m_thread->UpdateTc(m_interval);
  }
  void Update_max_tables_count(ulong val) { m_max_tables_count.store(val); }
  void Update_enable_persistent(bool val, bool enable_summary) {
    if (enable_summary && m_enable_persistent.load() && !val) {
      m_thread->signal();
    }
    m_enable_persistent.store(val && enable_summary);
    m_thread->Update_enable_persistent(val && enable_summary);
  }
  bool InitCollect();
  void initWorkJob();
  void nextWorkJob();

  /**
   * @brief create new snaphot
   *
   * @return bool false exec success  ,true exec failed
   */
  bool NewTask();
  bool nextTask();

 protected:
  bool createTask();

  /**
   * @brief batch get collect data
   *
   * @return int  err counts
   */
  int createTableTask(std::string time);

  int dropTableTask(plan_baseline_cmd_service &cmd_service);

  int insertInfoTableTask(plan_baseline_cmd_service &cmd_service,
                          std::string time_str);

 private:
  std::mutex data_mutex;

  std::unique_ptr<Gdb_create_plan_tables> m_thread;
  std::vector<std::unique_ptr<WorkJob>> m_tasks;

  std::atomic<ulong> m_interval;
  std::atomic<ulong> m_enable_persistent;
  std::atomic<ulong> m_max_tables_count;
  // worker thread pool
  std::unique_ptr<ThreadPool> m_task_pool;
  std::atomic<ulong> m_tables_count{0};

  WorkMgr() : initialized(false), tasks_initialized(false) {}
  WorkMgr(const WorkMgr &) = delete;
  WorkMgr(WorkMgr &&) = delete;
  WorkMgr &operator=(const WorkMgr &) = delete;
  WorkMgr &operator=(WorkMgr &&) = delete;
  int get_tables_info_count();
};

udf_descriptor gdb_create_plan_baseline_tables();
void update_refresh_interval(MYSQL_THD thd, SYS_VAR *var, void *var_ptr,
                             const void *save);
void update_max_tables_count(MYSQL_THD thd, SYS_VAR *var, void *var_ptr,
                             const void *save);
void update_plan_baseline_enable_persistent(MYSQL_THD thd, SYS_VAR *var,
                                            void *var_ptr, const void *save);

}  // namespace greatdb_plan_baseline

#endif
