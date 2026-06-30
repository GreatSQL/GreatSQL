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

#ifndef PLUGIN_GDB_PLAN_BASELINE_PSI_H
#define PLUGIN_GDB_PLAN_BASELINE_PSI_H

#include "mysql/psi/mysql_cond.h"
#include "mysql/psi/mysql_file.h"
#include "mysql/psi/mysql_memory.h"
#include "mysql/psi/mysql_mutex.h"
#include "mysql/psi/mysql_stage.h"
#include "mysql/psi/mysql_thread.h"

namespace greatdb_plan_baseline {
#ifdef HAVE_PSI_INTERFACE
extern PSI_mutex_key gdb_sql_plan_baselines_task_psi_mutex_key;
extern PSI_mutex_key gdb_plan_baseline_queue_mutex_key;
extern PSI_mutex_key gdb_plan_baseline_work_psi_mutex_key;
extern PSI_mutex_key gdb_plan_baseline_in_flight_mutex_key;
extern PSI_mutex_key gdb_plan_baseline_plan_hash_map_mutex_key;
extern PSI_mutex_key gdb_plan_baseline_plan_sql_map_mutex_key;
extern PSI_mutex_key gdb_plan_baseline_plan_map_id_list_mutex_key;

extern PSI_cond_key gdb_sql_plan_baselines_task_cond_key;
extern PSI_cond_key gdb_plan_baseline_queue_cond_key;
extern PSI_cond_key gdb_plan_baseline_work_psi_cond_key;
extern PSI_cond_key gdb_plan_baseline_task_producers_psi_cond_key;
extern PSI_cond_key gdb_plan_baseline_task_consumers_psi_cond_key;
extern PSI_cond_key gdb_plan_baseline_task_in_flight_psi_cond_key;

extern PSI_thread_key gdb_sql_plan_baselines_task_psi_thread_key;

#endif

void init_greatdb_plan_baseline_psi_keys();

}  // namespace greatdb_plan_baseline

#endif
