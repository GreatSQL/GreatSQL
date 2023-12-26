#ifndef OUTERJOIN_TRANSFORMER_INCLUDED
#define OUTERJOIN_TRANSFORMER_INCLUDED
/* Copyright (c) 2023, GreatDB Software Co., Ltd.

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

typedef struct outer_join_info {
  // key is inner table, value is outer table;
  std::map<Table_ref *, Table_ref *> outer_joined_map;
  // key is inner table, value is join_cond;
  std::map<Table_ref *, List<Item>> join_cond_map;
  // key is table no, value is pointer for the table_list.
  std::map<uint, Table_ref *> table_no_map;
  std::vector<uint> inner_tables;
  std::vector<uint> outer_tables;
  // key is inner table table_no, value is outer tables occur in all conds.
  std::map<uint, std::set<uint>> inner_to_outer_map;
  // key is outer table, value is inner table;
  std::map<uint, std::set<uint>> outer_to_inner_map;
  List<Item> where_conds;
} OUTER_INFO;

class Outerjoin_transformer {
 public:
  Outerjoin_transformer(Query_block *select_lex) : m_select_lex(select_lex) {}
  ~Outerjoin_transformer() {}
  bool setup_outer_join(THD *thd, Item *&item);

 private:
  bool check_single_outer_join_cond(Item *item);
  bool collect_outer_join_info(THD *thd, Item *item);
  bool check_outer_join(THD *thd, Item *item);
  void set_nullable(THD *thd);

 private:
  Query_block *m_select_lex;
  OUTER_INFO m_outer_info;
};

#endif /* OUTERJOIN_TRANSFORMER_INCLUDED */
