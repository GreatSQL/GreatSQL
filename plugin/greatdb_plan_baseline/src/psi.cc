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

#define MYSQL_SERVER 1

#include "psi.h"
#include "template_utils.h"

namespace greatdb_plan_baseline {
PSI_mutex_key gdb_sql_plan_baselines_task_psi_mutex_key = 0;
PSI_mutex_key gdb_plan_baseline_queue_mutex_key = 0;
PSI_mutex_key gdb_plan_baseline_work_psi_mutex_key = 0;
PSI_mutex_key gdb_plan_baseline_in_flight_mutex_key = 0;
PSI_mutex_key gdb_plan_baseline_plan_hash_map_mutex_key = 0;
PSI_mutex_key gdb_plan_baseline_plan_sql_map_mutex_key = 0;
PSI_mutex_key gdb_plan_baseline_plan_map_id_list_mutex_key;

PSI_mutex_info all_greatdb_plan_baseline_mutexes[] = {
    {&gdb_sql_plan_baselines_task_psi_mutex_key,
     "sql_plan_baselines_task_mutex", PSI_FLAG_SINGLETON, 0, PSI_DOCUMENT_ME},
    {&gdb_plan_baseline_in_flight_mutex_key, "plan_baseline_in_flight_mutex",
     PSI_FLAG_SINGLETON, 0, PSI_DOCUMENT_ME},
    {&gdb_plan_baseline_plan_hash_map_mutex_key,
     "plan_baseline_plan_hash_map_mutex", PSI_FLAG_SINGLETON, 0,
     PSI_DOCUMENT_ME},
    {&gdb_plan_baseline_plan_sql_map_mutex_key,
     "plan_baseline_plan_sql_map_mutex", PSI_FLAG_SINGLETON, 0,
     PSI_DOCUMENT_ME},
    {&gdb_plan_baseline_plan_map_id_list_mutex_key,
     "plan_baseline_map_id_list_mutex_key", PSI_FLAG_SINGLETON, 0,
     PSI_DOCUMENT_ME},
};

PSI_cond_key gdb_sql_plan_baselines_task_cond_key = 0;
PSI_cond_key gdb_plan_baseline_queue_cond_key = 0;
PSI_cond_key gdb_plan_baseline_work_psi_cond_key = 0;
PSI_cond_key gdb_plan_baseline_task_producers_psi_cond_key = 0;
PSI_cond_key gdb_plan_baseline_task_consumers_psi_cond_key = 0;
PSI_cond_key gdb_plan_baseline_task_in_flight_psi_cond_key = 0;

PSI_cond_info all_greatdb_plan_baseline_conds[] = {
    {&gdb_sql_plan_baselines_task_cond_key, "sql_plan_baselines_task cond ",
     PSI_FLAG_SINGLETON, 0, PSI_DOCUMENT_ME},
    {&gdb_plan_baseline_queue_cond_key, "plan_baseline_queue cond ",
     PSI_FLAG_SINGLETON, 0, PSI_DOCUMENT_ME},
    {&gdb_plan_baseline_work_psi_cond_key, "plan_baseline_work_psi cond ",
     PSI_FLAG_SINGLETON, 0, PSI_DOCUMENT_ME},
    {&gdb_plan_baseline_task_producers_psi_cond_key,
     "plan_baseline_task_producers_psi cond ", PSI_FLAG_SINGLETON, 0,
     PSI_DOCUMENT_ME},
    {&gdb_plan_baseline_task_consumers_psi_cond_key,
     "plan_baseline_task_consumers_psi cond ", PSI_FLAG_SINGLETON, 0,
     PSI_DOCUMENT_ME},
    {&gdb_plan_baseline_task_in_flight_psi_cond_key,
     "plan_baseline_task_in_flight_psi_cond cond ", PSI_FLAG_SINGLETON, 0,
     PSI_DOCUMENT_ME},
};

PSI_thread_key gdb_sql_plan_baselines_task_psi_thread_key = 0;

PSI_thread_info all_greatdb_plan_baseline_threads[] = {
    {&gdb_sql_plan_baselines_task_psi_thread_key,
     "sql_plan_baselines_task_manager", "bls_tk_mgr", PSI_FLAG_SINGLETON, 0,
     PSI_DOCUMENT_ME}};

#ifdef HAVE_PSI_INTERFACE
void init_greatdb_plan_baseline_psi_keys() {
  const char *const category = "greatdb_plan_baseline";
  int count;
  count = array_elements(all_greatdb_plan_baseline_mutexes);
  mysql_mutex_register(category, all_greatdb_plan_baseline_mutexes, count);

  count = array_elements(all_greatdb_plan_baseline_conds);
  mysql_cond_register(category, all_greatdb_plan_baseline_conds, count);

  count = array_elements(all_greatdb_plan_baseline_threads);
  mysql_thread_register(category, all_greatdb_plan_baseline_threads, count);
}

#else   // HAVE_PSI_INTERFACE
void init_greatdb_plan_baseline_psi_keys() {}
#endif  // HAVE+PSI_INTERFACE

}  // namespace greatdb_plan_baseline
