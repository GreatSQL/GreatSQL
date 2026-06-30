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
#include "mysql/plugin.h"
#include "sql-common/json_dom.h"  // Json_wrapper
#include "sql/join_optimizer/access_path.h"
#include "sql/sql_optimizer.h"
#include "sql/sql_plugin.h"

std::string print_explain_query(THD *thd, Query_expression *unit,
                                double *last_cost, ulonglong *rows);
bool check_if_all_db_is_system_schema(THD *thd);
bool gdb_plan_baseline_collect_explain_impl(THD *thd, Query_expression *unit);
