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

#ifndef PLUGIN_PLAN_BASELINE
#define PLUGIN_PLAN_BASELINE

#include <functional>

class THD;
class Query_expression;
class Query_block;
class Table_ref;

namespace greatdb_plan_baseline {

extern std::function<bool(THD *, Query_expression *)>
    gdb_plan_baseline_processor;

bool GDB_plan_baseline_get_explain(THD *thd, Query_expression *unit);
}  // namespace greatdb_plan_baseline

#endif  // PLUGIN_PLAN_BASELINE
