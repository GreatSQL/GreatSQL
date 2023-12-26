/* Copyright (c) 2023, GreatDB Software Co., Ltd. All rights reserved.

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

#ifndef SQL_PARSE_TREE_PIVOT_INCLUDED
#define SQL_PARSE_TREE_PIVOT_INCLUDED

#include "sql/parse_tree_node_base.h"
#include "sql/pivot.h"

class PT_pivot : public Parse_tree_node, public Pivot {
 public:
  PT_pivot(MEM_ROOT *mem_root, PT_item_list *funcs, Item *var,
           PT_item_list *values, const LEX_CSTRING &table_alias)
      : Pivot(mem_root, funcs, var, values), m_table_alias(table_alias.str) {}

  bool contextualize(Parse_context *pc) override;
  const char *table_alias() const { return m_table_alias; }

 private:
  const char *const m_table_alias{nullptr};
};

#endif /* SQL_PARSE_TREE_PIVOT_INCLUDED */
