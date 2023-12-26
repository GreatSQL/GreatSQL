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
  Implementation of removing useless outer join.

*/

#include "outerjoin_remover.h"

#include "my_dbug.h"
#include "sql/item.h"
#include "sql/item_sum.h"
#include "sql/nested_join.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"
#include "sql/sql_parse.h"
#include "sql/sql_resolver.h"

bool is_sigle_column_unique_key(Field *field) {
  TABLE *table = field->table;
  for (uint i = 0; i < table->s->keys; i++) {
    KEY *cur_key = table->key_info + i;
    if ((cur_key->flags & HA_NOSAME) && (field->part_of_key.is_set(i))) {
      if (cur_key->user_defined_key_parts == 1) {
        return true;
      }
    }
  }
  return false;
}

bool collect_all_columns_from_join_cond(
    Table_ref *table, mem_root_deque<Item_field *> &referenced_columns) {
  NESTED_JOIN *nested_join = nullptr;
  if ((nested_join = table->nested_join)) {
    for (auto ref : nested_join->m_tables) {
      if (collect_all_columns_from_join_cond(ref, referenced_columns))
        return true;
    }
  }
  Item *join_cond = table->join_cond();
  if (join_cond) {
    if (join_cond->walk(&Item::collect_all_item_fields_processor,
                        enum_walk::POSTFIX, (uchar *)&referenced_columns))
      return true;
  }
  return false;
}

bool belongs_to_table(Item_field *i_field, Table_ref *table) {
  bool is_merge_derived = (table->is_view_or_derived() && table->is_merged());
  if (is_merge_derived) {
    for (Table_ref *tbl = table->merge_underlying_list; tbl;
         tbl = tbl->next_local) {
      if (i_field->table_ref == tbl) {
        return true;
      }
    }
  } else {
    if (i_field->table_ref == table) return true;
  }
  return false;
}

void Outerjoin_remover::remove_table_from_list(Table_ref *object) {
  Table_ref *cur_tbl = m_select_lex->leaf_tables;
  while (cur_tbl && cur_tbl->next_leaf) {
    if (cur_tbl->next_leaf == object) {
      cur_tbl->next_leaf = cur_tbl->next_leaf->next_leaf;
      m_select_lex->leaf_table_count--;
      break;
    } else {
      cur_tbl = cur_tbl->next_leaf;
    }
  }
}

bool Outerjoin_remover::collect_all_columns(
    mem_root_deque<Item_field *> &referenced_columns) {
  mem_root_deque<Item *> &fields = m_select_lex->fields;
  /**
   *  Here <fields> must includes all columns referenced by group, order and
   * having expression.
   */
  for (auto &item : fields) {
    if (item->walk(&Item::collect_all_item_fields_processor,
                   enum_walk::SUBQUERY_PREFIX | enum_walk::POSTFIX,
                   (uchar *)&referenced_columns))
      return true;
  }
  for (Table_ref *table : m_select_lex->m_table_nest) {
    if (collect_all_columns_from_join_cond(table, referenced_columns))
      return true;
  }
  Item *where = m_select_lex->where_cond();
  if (where && where->walk(&Item::collect_all_item_fields_processor,
                           enum_walk::SUBQUERY_PREFIX | enum_walk::POSTFIX,
                           (uchar *)&referenced_columns))
    return true;

  Item *connect_by_cond = m_select_lex->connect_by_cond();
  if (connect_by_cond) {
    Item *start_cond = m_select_lex->start_with_cond();
    if (start_cond &&
        start_cond->walk(&Item::collect_all_item_fields_processor,
                         enum_walk::SUBQUERY_PREFIX | enum_walk::POSTFIX,
                         (uchar *)&referenced_columns))
      return true;

    if (connect_by_cond->walk(&Item::collect_all_item_fields_processor,
                              enum_walk::SUBQUERY_PREFIX | enum_walk::POSTFIX,
                              (uchar *)&referenced_columns))
      return true;
  }

  return false;
}

bool Outerjoin_remover::can_do_remove(Table_ref *table, Item *join_cond) {
  if (table->is_sj_or_aj_nest() || table->foj_inner || table->foj_outer)
    return false;
  if (!(table->outer_join && join_cond)) return false;
  bool is_equal_cond =
      join_cond->type() == Item::FUNC_ITEM &&
      (((const Item_func *)join_cond)->functype() == Item_func::EQ_FUNC ||
       ((const Item_func *)join_cond)->functype() == Item_func::EQUAL_FUNC);
  if (!is_equal_cond) return false;
  uint arg_count = ((const Item_func *)join_cond)->argument_count();
  if (arg_count != 2) return false;
  Item **args = ((const Item_func *)join_cond)->arguments();

  Item_field *first_field = nullptr;
  Item_field *second_field = nullptr;
  for (uint i = 0; i < arg_count; i++) {
    if (args[i]->real_item()->type() != Item::FIELD_ITEM) return false;
    if (!first_field) {
      first_field = ((Item_field *)args[i]->real_item());
    } else {
      second_field = ((Item_field *)args[i]->real_item());
    }
  }

  Table_ref *f_table = first_field->table_ref;
  Table_ref *s_table = second_field->table_ref;
  if (f_table == s_table) return false;
  /**
   * select * from (t1 inner join t2 on ...) left join t3 on t1.x = t2.x;
   * for table t3, <t1.x=t2.x> outer_join is all false.
   * select t2.b from t1 left join t2 on t1.a = t2.a;
   * select t1.* from t1 left join t2 on t2.b = t2.c;
   */
  if (!(belongs_to_table(first_field, table) ||
        belongs_to_table(second_field, table)))
    return false;
  // Confirm whether the referenced columns of inner table only appear in the
  // current join condition.
  mem_root_deque<Item_field *> referenced_columns(m_thd->mem_root);
  if (collect_all_columns(referenced_columns)) return false;
  List<Item_field> inner_table_fields;
  for (auto i_field : referenced_columns) {
    if (belongs_to_table(i_field, table)) {
      inner_table_fields.push_back(i_field);
    }
  }
  if (inner_table_fields.size() != 1) return false;

  // check uniqueness for inner_field.
  bool is_single_column_unique = false;
  Item_field *inner_field = inner_table_fields[0];
  if (table->is_view_or_derived() && (!table->is_merged())) {
    Query_expression *unit = table->derived_query_expression();
    if (unit->is_set_operation()) return false;

    uint index_no = inner_field->field_index;
    if (NO_FIELD_INDEX == index_no) return false;
    Query_block *qb = unit->first_query_block();
    if (qb->group_list.size() != 1) return false;
    mem_root_deque<Item *> *derived_fields = unit->get_unit_column_types();
    Item *ref_item = (*derived_fields)[index_no];

    ORDER *group_order = qb->group_list.first;
    if (group_order->in_field_list) {
      Item *group_item = *group_order->item;
      if (group_item == ref_item) {
        is_single_column_unique = true;
      }
    }
  } else {  // base table.
    if (is_sigle_column_unique_key(inner_field->field)) {
      is_single_column_unique = true;
    }
  }
  if (!is_single_column_unique) return false;
  return true;
}

void Outerjoin_remover::do_remove(mem_root_deque<Table_ref *> *join_list) {
  if (m_thd->lex->sql_command == SQLCOM_DELETE_MULTI ||
      m_thd->lex->sql_command == SQLCOM_UPDATE_MULTI)
    return;
  NESTED_JOIN *nested_join = nullptr;
  for (auto li = join_list->begin(); li != join_list->end();) {
    Table_ref *table = *li;
    if (table->schema_table ||
        (table->referencing_view && table->referencing_view->is_system_view)) {
      li++;
      continue;
    }
    Item *join_cond = table->join_cond();
    bool is_a_join = false;
    if ((nested_join = table->nested_join)) {
      is_a_join = true;
      do_remove(&nested_join->m_tables);
    }

    if (is_a_join && table->outerjoin_removed &&
        nested_join->m_tables.size() == 1) {
      Table_ref *real_table = nested_join->m_tables[0];
      if (join_cond) {
        real_table->set_join_cond(join_cond);
      }
      real_table->outer_join = table->outer_join;
      real_table->embedding = table->embedding;
      *li = real_table;
      table = real_table;
    }

    if (!can_do_remove(table, join_cond)) {
      li++;
      continue;
    }

    table->dep_tables = 0;
    if (table->embedding) {
      table->embedding->outerjoin_removed = true;
    }
    // delete table from leaf tables list. The head node must not be deleted.
    if (table->is_view_or_derived()) {
      if (table->is_merged()) {
        for (Table_ref *tbl = table->merge_underlying_list; tbl;
             tbl = tbl->next_local) {
          remove_table_from_list(tbl);
        }
      } else {
        remove_table_from_list(table);
        Query_expression *unit = table->derived_query_expression();
        unit->exclude_level();
      }
      m_select_lex->derived_table_count--;
    } else {
      remove_table_from_list(table);
    }

    m_select_lex->need_to_remap = true;
    li = join_list->erase(li);
    continue;
  }
}
