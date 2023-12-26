#ifndef OUTERJOIN_REMOVER_INCLUDED
#define OUTERJOIN_REMOVER_INCLUDED
/* Copyright (c) 2021, Oracle and/or its affiliates.
   Copyright (c) 2023, GreatDB Software Co., Ltd.

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

/**
  @file

  @brief
  Implementation of transform (+) to outer join stage

*/

class Item;
class Query_block;
class THD;

#include <map>
#include <set>
#include <vector>
#include "sql/table.h"

class Outerjoin_remover {
 public:
  Outerjoin_remover(Query_block *select_lex, THD *thd)
      : m_select_lex(select_lex), m_thd(thd) {}
  ~Outerjoin_remover() {}
  void do_remove(mem_root_deque<Table_ref *> *join_list);

 private:
  bool collect_all_columns(mem_root_deque<Item_field *> &result);
  bool can_do_remove(Table_ref *table, Item *join_cond);
  void remove_table_from_list(Table_ref *object);

 private:
  Query_block *m_select_lex;
  THD *m_thd;
};

#endif /* OUTERJOIN_REMOVER_INCLUDED */
