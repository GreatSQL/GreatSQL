/* Copyright (c) 2013, 2020, Oracle and/or its affiliates. All rights reserved.
   Copyright (c) 2022, Huawei Technologies Co., Ltd.
   Copyright (c) 2023, 2025, GreatDB Software Co., Ltd.

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

#include "include/my_dbug.h"
#include "include/mysql/psi/mysql_thread.h"
#include "sql/join_optimizer/access_path.h"
#include "sql/mysqld.h"
#include "sql/range_optimizer/range_optimizer.h"
#include "sql/sql_base.h"
#include "sql/sql_lex.h"
#include "sql/sql_opt_exec_shared.h"
#include "sql/sql_optimizer.h"
#include "sql/sql_resolver.h"
#include "sql/system_variables.h"

bool System_variables::pq_copy_from(struct System_variables orig) {
  pseudo_thread_id = orig.pseudo_thread_id;
  sql_mode = orig.sql_mode;
  collation_connection = orig.collation_connection;
  div_precincrement = orig.div_precincrement;
  time_zone = orig.time_zone;
  big_tables = orig.big_tables;
  lc_time_names = orig.lc_time_names;
  my_aes_mode = orig.my_aes_mode;
  transaction_isolation = orig.transaction_isolation;
  option_bits = orig.option_bits;
  explicit_defaults_for_timestamp = orig.explicit_defaults_for_timestamp;
  sortbuff_size = orig.sortbuff_size;
  join_buff_size = orig.join_buff_size;
  return false;
}

bool THD::pq_copy_from(THD *thd) {
  variables.pq_copy_from(thd->variables);
  start_time = thd->start_time;
  user_time = thd->user_time;
  m_query_string = thd->m_query_string;
  tx_isolation = thd->tx_isolation;
  tx_read_only = thd->tx_read_only;
  arg_of_last_insert_id_function = thd->arg_of_last_insert_id_function;
  first_successful_insert_id_in_prev_stmt =
      thd->first_successful_insert_id_in_prev_stmt;
  first_successful_insert_id_in_prev_stmt_for_binlog =
      thd->first_successful_insert_id_in_prev_stmt_for_binlog;
  first_successful_insert_id_in_cur_stmt =
      thd->first_successful_insert_id_in_cur_stmt;
  stmt_depends_on_first_successful_insert_id_in_prev_stmt =
      thd->stmt_depends_on_first_successful_insert_id_in_prev_stmt;
  return false;
}
