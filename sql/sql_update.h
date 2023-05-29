/* Copyright (c) 2006, 2022, Oracle and/or its affiliates. All rights reserved.
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

#ifndef SQL_UPDATE_INCLUDED
#define SQL_UPDATE_INCLUDED

#include <sys/types.h>

#include "mem_root_deque.h"
#include "my_alloc.h"
#include "my_sqlcommand.h"
#include "my_table_map.h"
#include "sql/query_result.h"  // Query_result_interceptor
#include "sql/sql_cmd_dml.h"   // Sql_cmd_dml
#include "sql/sql_data_change.h"  // enum_duplicates
#include "sql/sql_list.h"
#include "sql/thr_malloc.h"

class Copy_field;
class Item;
class JOIN;
class Query_block;
class Query_expression;
class RowIterator;
class Select_lex_visitor;
class THD;
class Temp_table_param;
struct TABLE;
class Table_ref;

using List_item = mem_root_deque<Item *>;

bool records_are_comparable(const TABLE *table);
bool compare_records(const TABLE *table);
bool should_switch_to_multi_table_if_subqueries(const THD *thd,
                                                const Query_block *select,
                                                const Table_ref *table_list);

class Query_result_update final : public Query_result_interceptor {
  /// Number of tables being updated
  uint update_table_count{0};
  /// Pointer to list of updated tables, linked via 'next_local'
  Table_ref *update_tables{nullptr};
  /// Array of references to temporary tables used to store cached updates
  TABLE **tmp_tables{nullptr};
  /// Array of parameter structs for creation of temporary tables
  Temp_table_param *tmp_table_param{nullptr};
  /// The first table in the join operation
  TABLE *main_table{nullptr};
  /**
    In a multi-table update, this is equal to the first table in the join
    operation (#main_table) if that table can be updated on the fly while
    scanning it. It is `nullptr` otherwise.

    @see safe_update_on_fly
  */
  TABLE *table_to_update{nullptr};
  /// List of pointers to fields to update, in order from statement
  const mem_root_deque<Item *> *fields;
  /// List of pointers to values to update with, in order from statement
  const mem_root_deque<Item *> *values;
  /// The fields list decomposed into separate lists per table
  mem_root_deque<Item *> **fields_for_table;
  /// The values list decomposed into separate lists per table
  mem_root_deque<Item *> **values_for_table;
  /**
   List of tables referenced in the CHECK OPTION condition of
   the updated view excluding the updated table.
  */
  List<TABLE> unupdated_check_opt_tables;
  /// ???
  Copy_field *copy_field{nullptr};

  /**
     Array of update operations, arranged per _updated_ table. For each
     _updated_ table in the multiple table update statement, a COPY_INFO
     pointer is present at the table's position in this array.

     The array is allocated and populated during Query_result_update::prepare().
     The position that each table is assigned is also given here and is stored
     in the member TABLE::pos_in_table_list::shared. However, this is a publicly
     available field, so nothing can be trusted about its integrity.

     This member is NULL when the Query_result_update is created.

     @see Query_result_update::prepare
  */
  COPY_INFO **update_operations{nullptr};

  TABLE *tmp_insert_table{nullptr};
  Temp_table_param *tmp_insert_table_param{nullptr};
  /// True if the full merge-into insert operation is complete
  bool insert_completed{false};
  MY_BITMAP *merge_insert_bitmap{nullptr};
  MY_BITMAP *merge_delete_read_set{nullptr};
  MY_BITMAP *merge_delete_write_set{nullptr};

 public:
  bool merge_into_stmt{false};
  bool merge_when_insert{false};
  bool merge_when_update_delete{false};
  /// The table used for insertion of rows
  Table_ref *insert_table_list{nullptr};
  TABLE *insert_table{nullptr};

  /// Insert field list. for MERGE-INTO.
  mem_root_deque<Item *> *insert_field_list{nullptr};
  /// Insert values lists. It should be one List_item. for MERGE-INTO.
  mem_root_deque<List_item *> *insert_many_values{nullptr};
  COPY_INFO insert;
  ulonglong autoinc_value_of_last_inserted_row{0};
  Item *opt_merge_update_where{nullptr};
  Item *opt_merge_insert_where{nullptr};
  Item *opt_merge_update_delete{nullptr};
  /// merge into, right outer join cond
  bool const_join_on{false};
  bool const_join_on_value{false};

 public:
  Query_result_update(mem_root_deque<Item *> *field_list,
                      mem_root_deque<Item *> *value_list,
                      Table_ref *insert_table_list_arg,
                      mem_root_deque<Item *> *insert_field_list_arg,
                      mem_root_deque<List_item *> *insert_many_values_arg)
      : Query_result_interceptor(),
        fields(field_list),
        values(value_list),
        insert_table_list(insert_table_list_arg),
        insert_field_list(insert_field_list_arg),
        insert_many_values(insert_many_values_arg),
        insert(COPY_INFO::INSERT_OPERATION, insert_field_list,
               (insert_field_list == nullptr || !insert_field_list->empty()),
               DUP_ERROR) {}
  bool need_explain_interceptor() const override { return true; }
  bool prepare(THD *thd, const mem_root_deque<Item *> &list,
               Query_expression *u) override;
  bool optimize();
  bool start_execution(THD *thd) override;
  bool send_data(THD *thd, const mem_root_deque<Item *> &items) override;
  bool send_eof(THD *thd) override;
  void cleanup() override;
  unique_ptr_destroy_only<RowIterator> create_iterator(
      THD *thd, MEM_ROOT *mem_root,
      unique_ptr_destroy_only<RowIterator> source);

  bool merge_insert_into_tmp(THD *thd);
  void set_merge_params(bool merge_when_insert_arg,
                        Item *opt_merge_update_where_arg,
                        Item *opt_merge_update_delete_arg,
                        Item *opt_merge_insert_where_arg,
                        bool const_join_on_arg, bool const_join_on_value_arg,
                        MY_BITMAP *bitmap_arg) {
    merge_into_stmt = true;
    merge_when_insert = merge_when_insert_arg;
    opt_merge_update_where = opt_merge_update_where_arg;
    opt_merge_insert_where = opt_merge_insert_where_arg;
    opt_merge_update_delete = opt_merge_update_delete_arg;
    merge_when_update_delete = opt_merge_update_delete != nullptr;
    const_join_on = const_join_on_arg;
    const_join_on_value = const_join_on_value_arg;
    merge_insert_bitmap = bitmap_arg;
  }
};

class Sql_cmd_update final : public Sql_cmd_dml {
 public:
  Sql_cmd_update(bool multitable_arg, mem_root_deque<Item *> *update_values)
      : multitable(multitable_arg),
        original_fields(*THR_MALLOC),
        update_value_list(update_values),
        insert_field_list(*THR_MALLOC),
        insert_many_values(*THR_MALLOC) {}

  enum_sql_command sql_command_code() const override {
    return multitable ? SQLCOM_UPDATE_MULTI : SQLCOM_UPDATE;
  }

  bool is_single_table_plan() const override { return !multitable; }

 protected:
  bool precheck(THD *thd) override;
  bool check_privileges(THD *thd) override;

  bool prepare_inner(THD *thd) override;

  bool execute_inner(THD *thd) override;

 private:
  bool update_single_table(THD *thd, bool is_ora_update_set = false);

  bool multitable;

  /// Bitmap of all tables which are to be updated
  table_map tables_for_update{0};

  bool accept(THD *thd, Select_lex_visitor *visitor) override;

  /// Convert list of fields to update to base table fields
  bool make_base_table_fields(THD *thd, mem_root_deque<Item *> *items);

 public:
  /// The original list of fields to update, used for privilege checking
  mem_root_deque<Item *> original_fields;
  /// The values used to update fields
  mem_root_deque<Item *> *update_value_list;

  bool merge_into_stmt{false};
  bool merge_when_insert{false};
  mem_root_deque<Item *> insert_field_list;
  mem_root_deque<List_item *> insert_many_values;
  Item *opt_merge_update_where{nullptr};
  Item *opt_merge_insert_where{nullptr};
  Item *opt_merge_update_delete{nullptr};

  /// True if VALUES clause contain column references that need privilege check
  bool values_need_privilege_check{false};

  /// Number of columns in original insert column list
  uint column_count;

  /// Number of values per row in insert_many_values, available after resolving
  uint value_count;

  /// merge into, right outer join cond
  bool const_join_on{false};
  bool const_join_on_value{false};

  /// Bitmap for insertion. To differentiate from the one in update.
  MY_BITMAP merge_insert_bitmap;
  /// Bitmap for delete write_set. To differentiate from the one in update.
  MY_BITMAP merge_delete_write_set;
  MY_BITMAP merge_delete_read_set;
  bool is_ora_update_set{false};
};

/// Find out which of the target tables can be updated immediately while
/// scanning. This is used by the old optimizer *after* the plan has been
/// created. The hypergraph optimizer does not use this function, as it makes
/// the decision about immediate update *during* planning, not after planning.
///
/// @param join The top-level JOIN object of the UPDATE statement.
/// @param single_target True if the UPDATE statement has exactly
///                      one target table.
/// @return Map of tables to update while scanning.
table_map GetImmediateUpdateTable(const JOIN *join, bool single_target);

/// Makes the TABLE and handler objects ready for being used in an UPDATE
/// statement. Called at the beginning of each execution.
///
/// @param join  The top-level JOIN object of the UPDATE operation.
/// @return true on error.
bool FinalizeOptimizationForUpdate(JOIN *join);

/// Creates an UpdateRowsIterator which updates the rows returned by the given
/// "source" iterator.
unique_ptr_destroy_only<RowIterator> CreateUpdateRowsIterator(
    THD *thd, MEM_ROOT *mem_root, JOIN *join,
    unique_ptr_destroy_only<RowIterator> source);

#endif /* SQL_UPDATE_INCLUDED */
