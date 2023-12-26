/* Copyright (c) 2017, 2022, Oracle and/or its affiliates. All rights reserved.
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

#include "sql/table_function.h"

#include <string.h>
#include <memory>
#include <new>
#include <utility>

#include "field_types.h"
#include "m_ctype.h"
#include "m_string.h"
#include "my_sys.h"
#include "mysql/components/services/bits/psi_bits.h"
#include "mysql_com.h"
#include "mysqld_error.h"
#include "prealloced_array.h"
#include "scope_guard.h"
#include "sql-common/json_dom.h"
#include "sql-common/json_path.h"
#include "sql/error_handler.h"
#include "sql/field.h"
#include "sql/handler.h"
#include "sql/item.h"
#include "sql/item_json_func.h"
#include "sql/psi_memory_key.h"
#include "sql/sp.h"
#include "sql/sp_pcontext.h"
#include "sql/sp_rcontext.h"
#include "sql/sql_class.h"  // THD
#include "sql/sql_exception_handler.h"
#include "sql/sql_list.h"
#include "sql/sql_select.h"
#include "sql/sql_show.h"
#include "sql/sql_table.h"      // create_typelib
#include "sql/sql_tmp_table.h"  // create_tmp_table_from_fields
#include "sql/table.h"
#include "sql/thd_raii.h"
#include "sql_string.h"

/******************************************************************************
  Implementation of Table_function
******************************************************************************/

bool Table_function::create_result_table(THD *thd, ulonglong options,
                                         const char *table_alias) {
  assert(table == nullptr);

  table = create_tmp_table_from_fields(thd, *get_field_list(), false, options,
                                       table_alias);
  return table == nullptr;
}

bool Table_function::write_row() {
  int error;

  if ((error = table->file->ha_write_row(table->record[0]))) {
    if (!table->file->is_ignorable_error(error) &&
        create_ondisk_from_heap(
            current_thd, table, error, /*insert_last_record=*/true,
            /*ignore_last_dup=*/true, /*is_duplicate=*/nullptr))
      return true;  // Not a table_is_full error
  }
  return false;
}

void Table_function::empty_table() {
  assert(table->is_created());
  (void)table->empty_result_table();
}

bool Table_function::init_args() {
  assert(!inited);
  if (do_init_args()) return true;
  table->pos_in_table_list->dep_tables |= used_tables();
  inited = true;
  return false;
}

/******************************************************************************
  Implementation of JSON_TABLE function
******************************************************************************/
Table_function_json::Table_function_json(const char *alias, Item *a,
                                         List<Json_table_column> *cols)
    : Table_function(),
      m_columns(cols),
      m_all_columns(current_thd->mem_root),
      m_table_alias(alias),
      is_source_parsed(false),
      source(a) {}

bool Table_function_json::walk(Item_processor processor, enum_walk walk,
                               uchar *arg) {
  // Only 'source' may reference columns of other tables; rest is literals.
  return source->walk(processor, walk, arg);
}

List<Create_field> *Table_function_json::get_field_list() {
  // It's safe as Json_table_column is derived from Create_field
  return reinterpret_cast<List<Create_field> *>(&m_vt_list);
}

bool Table_function_json::init_json_table_col_lists(uint *nest_idx,
                                                    Json_table_column *parent) {
  List_iterator<Json_table_column> li(*parent->m_nested_columns);
  Json_table_column *col;
  const uint current_nest_idx = *nest_idx;
  // Used to set fast track between sibling NESTED PATH nodes
  Json_table_column *nested = nullptr;
  /*
    This need to be set up once per statement, as it doesn't change between
    EXECUTE calls.
  */
  Prepared_stmt_arena_holder ps_arena_holder(current_thd);

  while ((col = li++)) {
    String buffer;
    col->is_unsigned = (col->flags & UNSIGNED_FLAG);
    col->m_jds_elt = &m_jds[current_nest_idx];
    if (col->m_jtc_type != enum_jt_column::JTC_NESTED_PATH) {
      col->m_field_idx = m_vt_list.elements;
      m_vt_list.push_back(col);
      if (check_column_name(col->field_name)) {
        my_error(ER_WRONG_COLUMN_NAME, MYF(0), col->field_name);
        return true;
      }
      if ((col->sql_type == MYSQL_TYPE_ENUM ||
           col->sql_type == MYSQL_TYPE_SET) &&
          !col->interval)
        col->interval = create_typelib(current_thd->mem_root, col);
    }
    m_all_columns.push_back(col);

    switch (col->m_jtc_type) {
      case enum_jt_column::JTC_ORDINALITY: {
        // No special handling is needed
        break;
      }
      case enum_jt_column::JTC_PATH: {
        const String *path = col->m_path_string->val_str(&buffer);
        assert(path != nullptr);
        if (parse_path(*path, false, &col->m_path_json)) return true;
        if (col->m_on_empty == Json_on_response_type::DEFAULT) {
          const String *default_string =
              col->m_default_empty_string->val_str(&buffer);
          assert(default_string != nullptr);
          Json_dom_ptr dom;  //@< we'll receive a DOM here
          JsonParseDefaultErrorHandler parse_handler("JSON_TABLE", 0);
          if (parse_json(*default_string, &dom, true, parse_handler,
                         JsonDocumentDefaultDepthHandler) ||
              (col->sql_type != MYSQL_TYPE_JSON && !dom->is_scalar())) {
            my_error(ER_INVALID_DEFAULT, MYF(0), col->field_name);
            return true;
          }
          col->m_default_empty_json = Json_wrapper(std::move(dom));
        }
        if (col->m_on_error == Json_on_response_type::DEFAULT) {
          const String *default_string =
              col->m_default_error_string->val_str(&buffer);
          assert(default_string != nullptr);
          Json_dom_ptr dom;  //@< we'll receive a DOM here
          JsonParseDefaultErrorHandler parse_handler("JSON_TABLE", 0);
          if (parse_json(*default_string, &dom, true, parse_handler,
                         JsonDocumentDefaultDepthHandler) ||
              (col->sql_type != MYSQL_TYPE_JSON && !dom->is_scalar())) {
            my_error(ER_INVALID_DEFAULT, MYF(0), col->field_name);
            return true;
          }
          col->m_default_error_json = Json_wrapper(std::move(dom));
        }
        break;
      }
      case enum_jt_column::JTC_EXISTS: {
        const String *path = col->m_path_string->val_str(&buffer);
        assert(path != nullptr);
        if (parse_path(*path, false, &col->m_path_json)) return true;
        break;
      }
      case enum_jt_column::JTC_NESTED_PATH: {
        (*nest_idx)++;
        if (*nest_idx >= MAX_NESTED_PATH) {
          my_error(ER_JT_MAX_NESTED_PATH, MYF(0), MAX_NESTED_PATH,
                   m_table_alias);
          return true;
        }
        col->m_child_jds_elt = &m_jds[*nest_idx];

        const String *path = col->m_path_string->val_str(&buffer);
        assert(path != nullptr);
        if (nested) {
          nested->m_next_nested = col;
          col->m_prev_nested = nested;
        }
        nested = col;

        if (parse_path(*path, false, &col->m_path_json) ||
            init_json_table_col_lists(nest_idx, col))
          return true;
        break;
      }
      default:
        assert(0);
    }
  }
  return false;
}

/**
  Check whether given default values can be saved to fields

  @returns
    true    a conversion error occurred
    false   defaults can be saved or aren't specified
*/

bool Table_function_json::do_init_args() {
  assert(!is_source_parsed);

  Item *dummy = source;
  if (source->fix_fields(current_thd, &dummy)) return true;

  /*
    For the default type of '?', two choices make sense: VARCHAR and JSON. The
    latter would lead to a call to Item_param::val_json() which isn't
    implemented. So we use the former.
  */
  if (source->propagate_type(current_thd)) return true;
  assert(source->data_type() != MYSQL_TYPE_VAR_STRING);
  if (source->has_aggregation() || source->has_subquery() || source != dummy) {
    my_error(ER_WRONG_ARGUMENTS, MYF(0), "JSON_TABLE");
    return true;
  }

  if (source->check_cols(1)) {
    return true;
  }

  try {
    /*
      Check whether given JSON source is a const and it's valid, see also
      Table_function_json::fill_result_table().
    */
    if (source->const_item()) {
      String buf;
      Item *args[] = {source};
      if (get_json_wrapper(args, 0, &buf, func_name(), &m_jds[0].jdata))
        return true;  // Error is already thrown
      is_source_parsed = true;
    }
  } catch (...) {
    /* purecov: begin inspected */
    handle_std_exception(func_name());
    return true;
    /* purecov: end */
  }

  // Validate that all the DEFAULT values are convertible to the target type.
  for (const Json_table_column *col : m_all_columns) {
    if (col->m_jtc_type != enum_jt_column::JTC_PATH) continue;
    assert(col->m_field_idx >= 0);
    if (col->m_on_empty == Json_on_response_type::DEFAULT) {
      if (save_json_to_field(current_thd, get_field(col->m_field_idx),
                             &col->m_default_empty_json, false)) {
        return true;
      }
    }
    if (col->m_on_error == Json_on_response_type::DEFAULT) {
      if (save_json_to_field(current_thd, get_field(col->m_field_idx),
                             &col->m_default_error_json, false)) {
        return true;
      }
    }
  }
  return false;
}

bool Table_function_json::init() {
  Json_table_column top(nullptr, m_columns);
  if (m_vt_list.elements == 0) {
    uint nest_idx = 0;
    if (init_json_table_col_lists(&nest_idx, &top)) return true;

    // Check for duplicate field names.
    for (Create_field &outer : m_vt_list) {
      Name_string outer_name(to_lex_cstring(outer.field_name));
      for (Create_field &inner : m_vt_list) {
        if (&outer == &inner) break;
        if (outer_name.eq(inner.field_name)) {
          my_error(ER_DUP_FIELDNAME, MYF(0), inner.field_name);
          return true;
        }
      }
    }
  }
  return false;
}

void Table_function_json::set_subtree_to_null(Json_table_column *root,
                                              Json_table_column **last) {
  List_iterator<Json_table_column> li(*root->m_nested_columns);
  Json_table_column *col;
  while ((col = li++)) {
    *last = col;
    switch (col->m_jtc_type) {
      case enum_jt_column::JTC_NESTED_PATH:
        set_subtree_to_null(col, last);
        break;
      default:
        get_field(col->m_field_idx)->set_null();
        break;
    }
  }
}

bool Json_table_column::fill_column(Table_function_json *table_function,
                                    jt_skip_reason *skip) {
  *skip = JTS_NONE;

  Field *const fld = m_jtc_type == enum_jt_column::JTC_NESTED_PATH
                         ? nullptr
                         : table_function->get_field(m_field_idx);
  assert(m_jtc_type == enum_jt_column::JTC_NESTED_PATH ||
         (fld != nullptr && fld->field_index() == m_field_idx));

  switch (m_jtc_type) {
    case enum_jt_column::JTC_ORDINALITY: {
      if (fld->store(m_jds_elt->m_rowid, true)) return true;
      fld->set_notnull();
      break;
    }
    case enum_jt_column::JTC_PATH: {
      THD *thd = current_thd;
      // Vector of matches
      Json_wrapper_vector data_v(key_memory_JSON);
      m_jds_elt->jdata.seek(m_path_json, m_path_json.leg_count(), &data_v, true,
                            false);
      if (data_v.size() > 0) {
        Json_wrapper buf;
        bool is_error = false;
        if (data_v.size() > 1) {
          // Make result array
          if (fld->type() == MYSQL_TYPE_JSON) {
            Json_array *a = new (std::nothrow) Json_array();
            if (!a) return true;
            for (Json_wrapper &w : data_v) {
              if (a->append_alias(w.clone_dom())) {
                delete a; /* purecov: inspected */
                return true;
              }
            }
            buf = Json_wrapper(a);
          } else {
            is_error = true;
            // Thrown an error when save_json_to_field() isn't called
            if (m_on_error == Json_on_response_type::ERROR)
              my_error(ER_WRONG_JSON_TABLE_VALUE, MYF(0), field_name);
          }
        } else
          buf = std::move(data_v[0]);
        if (!is_error) {
          // Save the extracted value to the field in JSON_TABLE. Make sure an
          // error is raised for conversion errors if ERROR ON ERROR is
          // specified. Don't raise any warnings when DEFAULT/NULL ON ERROR is
          // specified, as they may be promoted to errors by
          // Strict_error_handler and prevent the ON ERROR clause from being
          // respected.
          Ignore_warnings_error_handler ignore_warnings;
          const bool no_error = m_on_error != Json_on_response_type::ERROR;
          if (no_error) thd->push_internal_handler(&ignore_warnings);
          if (buf.type() == enum_json_type::J_NULL)  // see JSON_VALUE
            fld->set_null();
          else
            is_error = save_json_to_field(thd, fld, &buf, no_error);
          if (no_error) thd->pop_internal_handler();
        }
        if (is_error) {
          switch (m_on_error) {
            case Json_on_response_type::ERROR: {
              return true;
              break;
            }
            case Json_on_response_type::DEFAULT: {
              save_json_to_field(thd, fld, &m_default_error_json, true);
              break;
            }
            case Json_on_response_type::NULL_VALUE:
            default: {
              fld->set_null();
              break;
            }
          }
        }
      } else {
        switch (m_on_empty) {
          case Json_on_response_type::ERROR: {
            my_error(ER_MISSING_JSON_TABLE_VALUE, MYF(0), field_name);
            return true;
          }
          case Json_on_response_type::DEFAULT: {
            save_json_to_field(thd, fld, &m_default_empty_json, true);
            break;
          }
          case Json_on_response_type::NULL_VALUE:
          default: {
            fld->set_null();
            break;
          }
        }
      }
      break;
    }
    case enum_jt_column::JTC_EXISTS: {
      // Vector of matches
      Json_wrapper_vector data_v(key_memory_JSON);
      m_jds_elt->jdata.seek(m_path_json, m_path_json.leg_count(), &data_v, true,
                            true);
      if (data_v.size() >= 1)
        fld->store(1, true);
      else
        fld->store(0, true);
      if (current_thd->is_error()) return true;
      fld->set_notnull();
      break;
    }
    case enum_jt_column::JTC_NESTED_PATH: {
      // If this node sends data, advance ts iterator
      if (m_child_jds_elt->producing_records) {
        ++m_child_jds_elt->it;
        m_child_jds_elt->m_rowid++;

        if ((m_child_jds_elt->it != m_child_jds_elt->v.end()))
          m_child_jds_elt->jdata = std::move(*m_child_jds_elt->it);
        else {
          m_child_jds_elt->producing_records = false;
          *skip = JTS_EOD;
        }
        return false;
      }
      // Run only one sibling nested path at a time
      for (Json_table_column *tc = m_prev_nested; tc; tc = tc->m_prev_nested) {
        assert(tc->m_jtc_type == enum_jt_column::JTC_NESTED_PATH);
        if (tc->m_child_jds_elt->producing_records) {
          *skip = JTS_SIBLING;
          return false;
        }
      }
      m_child_jds_elt->v.clear();
      if (m_jds_elt->jdata.seek(m_path_json, m_path_json.leg_count(),
                                &m_child_jds_elt->v, true, false))
        return true;
      if (m_child_jds_elt->v.size() == 0) {
        *skip = JTS_EOD;
        return false;
      }
      m_child_jds_elt->it = m_child_jds_elt->v.begin();
      m_child_jds_elt->producing_records = true;
      m_child_jds_elt->m_rowid = 1;
      m_child_jds_elt->jdata = std::move(*m_child_jds_elt->it);
      break;
    }
    default: {
      assert(0);
      break;
    }
  }
  return false;
}

Json_table_column::~Json_table_column() {
  // Reset paths and wrappers to free allocated memory.
  m_path_json = Json_path(key_memory_JSON);
  if (m_on_empty == Json_on_response_type::DEFAULT)
    m_default_empty_json = Json_wrapper();
  if (m_on_error == Json_on_response_type::DEFAULT)
    m_default_error_json = Json_wrapper();
}

/**
  Fill json table

  @details This function goes along the flattened list of columns and
  updates them by calling fill_column(). As it goes, it pushes all nested
  path nodes to 'nested' list, using it as a stack. After writing a row, it
  checks whether there's more data in the right-most nested path (top in the
  stack). If there is, it advances path's iterator, if no - pops the path
  from stack and goes to the next nested path (i.e more to left). When stack
  is empty, then the loop is over and all data (if any) was stored in the table,
  and function exits. Otherwise, the list of columns is positioned to the top
  nested path in the stack and incremented to the column after the nested
  path, then the loop of updating columns is executed again. So, whole
  execution could look as follows:

      columns (                      <-- npr
        cr1,
        cr2,
        nested path .. columns (     <-- np1
          c11,
          nested path .. columns (   <-- np2
            c21
          )
        )
      )

      iteration | columns updated in the loop
      1           npr cr1 cr2 np1 c11 np2 c21
      2                                   c21
      3                                   c21
      4                           c11 np2 c21
      5                                   c21
      6                           c11 np2 c21
      7                                   c21
      8           npr cr1 cr2 np1 c11 np2 c21
      9                                   c21
     10                           c11 np2 c21
  Note that result table's row isn't automatically reset and if a column
  isn't updated, its data is written multiple times. E.g. cr1 in the
  example above is updated 2 times, but is written 10 times. This allows to
  save cycles on updating fields that for sure haven't been changed.

  When there's sibling nested paths, i.e two or more nested paths in the
  same columns clause, then they're processed one at a time. Started with
  first, and the rest are set to null with help f set_subtree_to_null().
  When the first sibling nested path runs out of rows, it's set to null and
  processing moves on to the next one.

  @returns
    false table filled
    true  error occurred
*/

bool Table_function_json::fill_json_table() {
  // 'Stack' of nested NESTED PATH clauses
  Prealloced_array<uint, MAX_NESTED_PATH> nested(PSI_NOT_INSTRUMENTED);

  // The column being processed
  uint col_idx = 0;
  jt_skip_reason skip_subtree;
  const enum_check_fields check_save = current_thd->check_for_truncated_fields;

  do {
    skip_subtree = JTS_NONE;
    /*
      When a NESTED PATH runs out of matches, we set it to null, and
      continue filling the row, so next sibling NESTED PATH could start
      sending rows. But if there's no such NESTED PATH, then this row must be
      skipped as it's not a result of a match.
    */
    bool skip_row = true;
    for (; col_idx < m_all_columns.size(); col_idx++) {
      /*
        When NESTED PATH doesn't have a match for any reason, set its
        columns to nullptr.
      */
      Json_table_column *col = m_all_columns[col_idx];
      if (col->fill_column(this, &skip_subtree)) return true;
      if (skip_subtree) {
        set_subtree_to_null(col, &col);
        // Position iterator to the last element of subtree
        while (m_all_columns[col_idx] != col) col_idx++;
      } else if (col->m_jtc_type == enum_jt_column::JTC_NESTED_PATH) {
        nested.push_back(col_idx);
        // Found a NESTED PATH which produced a record
        skip_row = false;
      }
    }
    if (!skip_row) write_row();
    // Find next nested path and advance its iterator.
    if (nested.size() > 0) {
      uint j = nested.back();
      nested.pop_back();
      Json_table_column *col = m_all_columns[j];

      /*
        When there're sibling NESTED PATHs and the first one is producing
        records, second one will skip_subtree and we need to reset it here,
        as it's not relevant.
      */
      if (col->m_child_jds_elt->producing_records) skip_subtree = JTS_NONE;
      col_idx = j;
    }
  } while (nested.size() != 0 || skip_subtree != JTS_EOD);

  current_thd->check_for_truncated_fields = check_save;
  return false;
}

bool Table_function_json::fill_result_table() {
  String buf;
  assert(!table->materialized);
  // reset table
  empty_table();

  try {
    Item *args[] = {source};
    /*
      There are 3 possible cases of data source expression const-ness:

      1. Always const, e.g. a plain string, source will be parsed once at
         Table_function_json::init()
      2. Non-const during init(), but become const after it, e.g a field from a
         const table: source will be parsed here ONCE
      3. Non-const, e.g. a table field: source will be parsed here EVERY TIME
         fill_result_table() is called
    */
    if (!source->const_item() || !is_source_parsed) {
      if (get_json_wrapper(args, 0, &buf, func_name(), &m_jds[0].jdata)) {
        return true;
      }
    }
    if (args[0]->null_value) {
      // No need to set null_value as it's not used by table functions
      return false;
    }
    is_source_parsed = true;
    return fill_json_table();
  } catch (...) {
    /* purecov: begin inspected */
    handle_std_exception(func_name());
    return true;
    /* purecov: end */
  }
  return false;
}

void print_on_empty_or_error(const THD *thd, String *str,
                             enum_query_type query_type, bool on_empty,
                             Json_on_response_type response_type,
                             const Item *default_string) {
  switch (response_type) {
    case Json_on_response_type::ERROR:
      str->append(STRING_WITH_LEN(" error"));
      break;
    case Json_on_response_type::NULL_VALUE:
      str->append(STRING_WITH_LEN(" null"));
      break;
    case Json_on_response_type::DEFAULT:
      str->append(STRING_WITH_LEN(" default "));
      default_string->print(thd, str, query_type);
      break;
    case Json_on_response_type::IMPLICIT:
      // Nothing to print when the clause was implicit.
      return;
  };

  if (on_empty)
    str->append(STRING_WITH_LEN(" on empty"));
  else
    str->append(STRING_WITH_LEN(" on error"));
}

/// Prints the type of a column in a JSON_TABLE expression.
static bool print_json_table_column_type(const Field *field, String *str) {
  StringBuffer<STRING_BUFFER_USUAL_SIZE> type;
  field->sql_type(type);
  if (str->append(type)) return true;
  if (field->has_charset()) {
    // Append the character set.
    if (str->append(STRING_WITH_LEN(" character set ")) ||
        str->append(field->charset()->csname))
      return true;
    // Append the collation, if it is not the primary collation of the
    // character set.
    if ((field->charset()->state & MY_CS_PRIMARY) == 0 &&
        (str->append(STRING_WITH_LEN(" collate ")) ||
         str->append(field->charset()->m_coll_name)))
      return true;
  }
  return false;
}

/**
  Helper function to print a single NESTED PATH column.

  @param thd        the current session
  @param table      the TABLE object representing the JSON_TABLE expression
  @param col        the column to print
  @param query_type the type of the query
  @param str        the string to print to

  @returns true on error, false on success
*/
static bool print_nested_path(const THD *thd, const TABLE *table,
                              const Json_table_column *col,
                              enum_query_type query_type, String *str) {
  col->m_path_string->print(thd, str, query_type);
  if (str->append(STRING_WITH_LEN(" columns ("))) return true;
  bool first = true;
  for (const Json_table_column &jtc : *col->m_nested_columns) {
    if (!first && str->append(STRING_WITH_LEN(", "))) return true;
    first = false;

    switch (jtc.m_jtc_type) {
      case enum_jt_column::JTC_ORDINALITY: {
        append_identifier(thd, str, jtc.field_name, strlen(jtc.field_name));
        if (str->append(STRING_WITH_LEN(" for ordinality"))) return true;
        break;
      }
      case enum_jt_column::JTC_EXISTS:
      case enum_jt_column::JTC_PATH: {
        append_identifier(thd, str, jtc.field_name, strlen(jtc.field_name));
        if (str->append(' ')) return true;
        if (table == nullptr) {
          if (str->append(STRING_WITH_LEN("<column type not resolved yet>"))) {
            return true;
          }
        } else if (print_json_table_column_type(table->field[jtc.m_field_idx],
                                                str)) {
          return true;
        }
        if (jtc.m_jtc_type == enum_jt_column::JTC_EXISTS) {
          if (str->append(STRING_WITH_LEN(" exists"))) return true;
        }
        if (str->append(STRING_WITH_LEN(" path "))) return true;
        jtc.m_path_string->print(thd, str, query_type);
        if (jtc.m_jtc_type == enum_jt_column::JTC_EXISTS) break;
        // ON EMPTY
        print_on_empty_or_error(thd, str, query_type, /*on_empty=*/true,
                                jtc.m_on_empty, jtc.m_default_empty_string);
        // ON ERROR
        print_on_empty_or_error(thd, str, query_type, /*on_empty=*/false,
                                jtc.m_on_error, jtc.m_default_error_string);
        break;
      }
      case enum_jt_column::JTC_NESTED_PATH: {
        if (str->append(STRING_WITH_LEN("nested path ")) ||
            print_nested_path(thd, table, &jtc, query_type, str))
          return true;
        break;
      }
    };
  }
  return str->append(')');
}

bool Table_function_json::print(const THD *thd, String *str,
                                enum_query_type query_type) const {
  if (str->append(STRING_WITH_LEN("json_table("))) return true;
  source->print(thd, str, query_type);
  return (str->append(STRING_WITH_LEN(", ")) ||
          print_nested_path(thd, table, m_columns->head(), query_type, str) ||
          str->append(')'));
}

table_map Table_function_json::used_tables() { return source->used_tables(); }

void Table_function_json::do_cleanup() {
  source->cleanup();
  is_source_parsed = false;
  for (uint i = 0; i < MAX_NESTED_PATH; i++) m_jds[i].cleanup();
  for (uint i = 0; i < m_all_columns.size(); i++) m_all_columns[i]->cleanup();
}

void JT_data_source::cleanup() {
  v.clear();
  producing_records = false;
}

Table_function_sequence::Table_function_sequence(const char *alias, Item *a)
    : Table_function(),
      m_table_alias(alias),
      m_source(a),
      m_vt_list(),
      m_upper_bound_precalculated(false),
      m_precalculated_upper_bound(0) {
  m_value_field.init_for_tmp_table(MYSQL_TYPE_LONGLONG,  // sql_type_arg
                                   20,                   // length_arg
                                   0,                    // decimals_arg
                                   false,                // maybe_null_arg
                                   true,                 // is_unsigned_arg
                                   8,  // pack_length_override_arg
                                   value_field_name);  // fld_name
}

bool Table_function_sequence::init() {
  return m_vt_list.push_back(&m_value_field);
}

List<Create_field> *Table_function_sequence::get_field_list() {
  return &m_vt_list;
}

bool Table_function_sequence::fill_result_table() {
  assert(!table->materialized);
  // reset table
  empty_table();

  ulonglong upper_bound;
  if (m_upper_bound_precalculated) {
    upper_bound = m_precalculated_upper_bound;
  } else {
    upper_bound = calculate_upper_bound();
    if (m_source->const_item()) m_upper_bound_precalculated = true;
  }

  if (upper_bound > tf_sequence_table_max_upper_bound) {
    my_error(ER_SEQUENCE_TABLE_SIZE_LIMIT, MYF(0),
             tf_sequence_table_max_upper_bound, upper_bound);
    return true;
  }

  for (ulonglong u = 0; u < upper_bound; ++u) {
    if (get_field(0)->store(u, true)) return true;
    if (write_row()) return true;
  }
  return false;
}

table_map Table_function_sequence::used_tables() {
  return m_source->used_tables();
}

bool Table_function_sequence::print(const THD *thd, String *str,
                                    enum_query_type query_type) const {
  if (str->append(STRING_WITH_LEN("sequence_table("))) return true;
  m_source->print(thd, str, query_type);
  if (thd->is_error()) return true;
  return str->append(')');
}

bool Table_function_sequence::walk(Item_processor processor, enum_walk walk,
                                   uchar *arg) {
  // Only 'source' may reference columns of other tables; rest is literals.
  return m_source->walk(processor, walk, arg);
}

bool Table_function_sequence::do_init_args() {
  assert(!m_upper_bound_precalculated);

  Item *dummy = m_source;
  if (m_source->fix_fields(current_thd, &dummy)) return true;

  // Set the default type of '?'
  if (m_source->propagate_type(current_thd, MYSQL_TYPE_LONGLONG)) return true;

  assert(m_source->data_type() != MYSQL_TYPE_VAR_STRING);
  if (m_source->has_aggregation() || m_source->has_subquery() ||
      m_source != dummy) {
    my_error(ER_WRONG_ARGUMENTS, MYF(0), "SEQUENCE_TABLE");
    return true;
  }

  if (m_source->const_item()) {
    m_precalculated_upper_bound = calculate_upper_bound();
    m_upper_bound_precalculated = true;
  }

  return false;
}

void Table_function_sequence::do_cleanup() {
  m_source->cleanup();
  m_vt_list.clear();
  m_upper_bound_precalculated = false;
  m_precalculated_upper_bound = 0;
}

ulonglong Table_function_sequence::calculate_upper_bound() const {
  ulonglong res = 0;
  if (!m_source->is_null()) {
    if (m_source->unsigned_flag) {
      res = m_source->val_uint();
    } else {
      auto signed_res = m_source->val_int();
      if (signed_res > 0) res = static_cast<ulonglong>(signed_res);
    }
  }
  return res;
}

bool Table_function_record::init() {
  Item_field_row_table *record =
      (Item_field_row_table *)current_thd->sp_runtime_ctx->get_item(
          m_spv_offset);
  if (!record) return true;
  sp_variable *spv = current_thd->sp_runtime_ctx->search_variable(
      record->item_name.ptr(), record->item_name.length());
  if (!spv || !spv->field_def.ora_record.row_field_table_definitions()) {
    my_error(ER_SP_MISMATCH_RECORD_TABLE_VAR, MYF(0), m_table_alias);
    return true;
  }
  List<Row_definition_list> *sd = dynamic_cast<List<Row_definition_list> *>(
      spv->field_def.ora_record.row_field_table_definitions());
  if (!sd) return true;

  List_iterator_fast<Row_definition_list> it_table(*sd);
  Row_definition_list *def_list;
  for (uint offset = 0; (def_list = it_table++); offset++) {
    List<Create_field> *sd_list = dynamic_cast<List<Create_field> *>(def_list);
    List_iterator_fast<Create_field> it(*sd_list);
    Create_field *def;
    uint i;
    for (i = 0; (def = it++); i++) {
      m_vt_list.push_back(def);
    }
  }
  if (!m_vt_list.size()) return true;
  return false;
}

List<Create_field> *Table_function_record::get_field_list() {
  return &m_vt_list;
}
// copy data from table_from to this.
bool Table_function_record::fill_result_table() {
  Item_field_row_table *record =
      (Item_field_row_table *)current_thd->sp_runtime_ctx->get_item(
          m_spv_offset);
  Field_row_table *field_table = (Field_row_table *)record->field;
  Table_function_record *table_from = field_table->table_record;
  Item *item_i = current_thd->sp_runtime_ctx->get_item(m_forall_i_offset);
  if (!item_i->fixed) {
    Item *dummy = item_i;
    if (item_i->fix_fields(current_thd, &dummy)) return true;
  }
  init_table();
  mem_root_deque<Item *> items(current_thd->mem_root);
  if (table_from->select_result_table_one_row(item_i, &items)) return true;
  for (uint col = 0; col < table->s->fields; col++) {
    Item *item = items.at(col);
    if (sp_eval_expr(current_thd, get_field(col), &item)) return true;
  }
  if (write_row_and_cmp_index(items.at(0))) return true;
  return false;
}

bool Table_function_record::fill_result_table_all_col(
    THD *thd, const mem_root_deque<Item *> &items, Item *item_index) {
  auto item_iter = VisibleFields(items).begin();
  int count = 0;
  int size = CountVisibleFields(items);
  while (count < size) {
    Item *item = *item_iter++;
    item = item->this_item();
    /* e.g: stu_record_val(1) := stu_record_val1;
       stu_record_val(1) := stu_record_val(2)
    */
    if (item->type() == Item::ORACLE_ROWTYPE_ITEM) {
      bool is_field_table = item->get_arg_count() == table->s->fields;
      for (uint i = 0; i < item->get_arg_count(); i++) {
        if (sp_eval_expr(thd, get_field(i + 1 - is_field_table), item->addr(i)))
          return true;
      }
    } else if (item->result_type() == ROW_RESULT &&
               item->type() == Item::FUNC_ITEM) {
      // e.g: stu_record_val(1) := stu_record('a','aa10');
      Item_func *udt = dynamic_cast<Item_func *>(item);
      for (uint i = 0; i < udt->argument_count(); i++) {
        if (sp_eval_expr(thd, get_field(i + 1), udt->arguments() + i))
          return true;
      }
    } else if (item->type() == Item::ORACLE_ROWTYPE_TABLE_ITEM) {
      my_error(ER_NOT_SUPPORTED_YET, MYF(0),
               "table object saves to non-table object.");
      return true;
    } else if (item->type() == Item::NULL_ITEM && size == 1 &&
               (int)table->s->fields > size) {
      // for stu_record_val(3) := null;
      for (uint i = 1; i < table->s->fields; i++)
        if (sp_eval_expr(thd, get_field(i), &item)) return true;
    } else if (sp_eval_expr(thd, get_field(count + 1), &item))
      return true;
    count++;
  }
  if (sp_eval_expr(thd, get_field(0), &item_index)) return true;
  if (write_row_and_cmp_index(item_index)) return true;
  return false;
}

bool Table_function_record::table_index_read_map(Item *item_index,
                                                 uint record_offset) {
  Index_lookup ref;
  if (prepare_index_lookup_part(item_index, &ref)) return true;
  int error = 0;
  if (!table->file->inited) error = table->file->ha_index_init(ref.key, false);
  if (!error) {
    error = table->file->ha_index_read_map(
        table->record[record_offset], ref.key_buff,
        make_prev_keypart_map(ref.key_parts), HA_READ_KEY_EXACT);
  }
  if (error) {
    if (table->file->inited) table->file->ha_index_end();
    return true;
  }
  return false;
}

bool Table_function_record::update_result_table_one_col(Item **value,
                                                        uint field_idx,
                                                        Item *item_index,
                                                        Field *field_udt) {
  bool rc = true;
  if (table_index_read_map(item_index, 1)) {
    my_error(ER_SP_NOT_EXIST_OF_RECORD_TABLE, MYF(0), table->alias);
    return true;
  }

  if (!field_udt) {
    if (sp_eval_expr(current_thd, get_field(field_idx), value)) goto error;
  } else {
    TABLE *table_udt = field_udt->virtual_tmp_table_addr()[0];
    get_field(field_idx)->set_notnull();
    if (get_field(field_idx)->store((const char *)table_udt->record[0],
                                    table_udt->s->reclength,
                                    &my_charset_bin) != TYPE_OK)
      goto error;
  }
  /*  e.g: table(3).id := table(2).id;
      sp_eval_expr() did table->file->ha_rnd_end(),make table->file->inited ==
     handler::NONE.
  */
  if (table->file->inited == handler::NONE) {
    if (table_index_read_map(item_index, 1)) {
      my_error(ER_SP_NOT_EXIST_OF_RECORD_TABLE, MYF(0), table->alias);
      return true;
    }
  }
  if (sp_eval_expr(current_thd, get_field(0), &item_index)) goto error;
  if (table->file->ha_update_row(table->record[1], table->record[0])) {
    my_error(ER_UPDATING_DD_TABLE, MYF(0), table->alias);
    goto error;
  }
  rc = false;

error:
  if (table->file->inited) table->file->ha_index_end();
  return rc;
}

bool Table_function_record::update_result_table_all_cols(Item **value,
                                                         Item *item_index) {
  bool rc = true;
  Item_func_udt_constructor *item_func =
      dynamic_cast<Item_func_udt_constructor *>((*value)->this_item());
  Item_func_sp *item_func_sp = dynamic_cast<Item_func_sp *>((*value));
  Item **item_value = nullptr;
  if (table_index_read_map(item_index, 1)) {
    my_error(ER_SP_NOT_EXIST_OF_RECORD_TABLE, MYF(0), table->alias);
    return true;
  }

  /*The get_field(0) is index column,so do sp_eval_expr from i=1.*/
  if ((*value)->type() == Item::NULL_ITEM) {
    for (uint i = 1; i < table->s->fields; i++)
      if (sp_eval_expr(current_thd, get_field(i), value)) goto error;
  } else if (item_func_sp) {
    Query_arena backup_arena;
    current_thd->swap_query_arena(*current_thd->stmt_arena, &backup_arena);
    String tmp;
    item_func_sp->val_str(&tmp);
    current_thd->swap_query_arena(backup_arena, current_thd->stmt_arena);

    if (item_func_sp->null_value) {
      for (uint i = 1; i < table->s->fields; i++) {
        type_conversion_status ret =
            set_field_to_null_with_conversions(get_field(i), false);
        if (ret != TYPE_OK) goto error;
      }

    } else {
      Field_udt_type *field_udt_type =
          dynamic_cast<Field_udt_type *>(item_func_sp->get_sp_result_field());

      if (field_udt_type) {
        TABLE *from_table = field_udt_type->virtual_tmp_table_addr()[0];
        for (uint i = 1; i < table->s->fields; i++) {
          Item *item_filed =
              new (current_thd->mem_root) Item_field(from_table->field[i - 1]);
          if (sp_eval_expr(current_thd, get_field(i), &item_filed)) goto error;
        }
      } else {
        Item *item_filed = new (current_thd->mem_root)
            Item_field(item_func_sp->get_sp_result_field());
        if (sp_eval_expr(current_thd, get_field(1), &item_filed)) goto error;
      }
    }
  } else {
    Item_field_row *item_tmp =
        dynamic_cast<Item_field_row *>((*value)->this_item());
    int include_index_column = 0;
    if (item_tmp) {
      include_index_column =
          item_tmp->get_udt_table()->s->fields == table->s->fields ? 1 : 0;
    }
    for (uint i = 1; i < table->s->fields; i++) {
      item_value =
          item_func ? item_func->arguments() + i - 1
                    : (*value)->this_item()->addr(i - 1 + include_index_column);
      // item_value = nullptr, single type case
      if (item_value == nullptr) item_value = value;
      if (sp_eval_expr(current_thd, get_field(i), item_value)) goto error;
    }
  }
  if (sp_eval_expr(current_thd, get_field(0), &item_index)) goto error;
  /*  e.g: table(3) := table(2);
      sp_eval_expr() did table->file->ha_rnd_end(),make table->file->inited ==
     handler::NONE.
  */
  if (!table->file->inited) {
    if (table_index_read_map(item_index, 1)) {
      my_error(ER_SP_NOT_EXIST_OF_RECORD_TABLE, MYF(0), table->alias);
      return true;
    }
  }
  if (table->file->ha_update_row(table->record[1], table->record[0])) {
    my_error(ER_UPDATING_DD_TABLE, MYF(0), table->alias);
    goto error;
  }
  rc = false;

error:
  if (table->file->inited) table->file->ha_index_end();
  return rc;
}

table_map Table_function_record::used_tables() { return 0; }

bool Table_function_record::print(const THD *thd, String *str,
                                  enum_query_type) const {
  if (str->append(STRING_WITH_LEN("record_table("))) return true;
  if (thd->is_error()) return true;
  return str->append(')');
}

bool Table_function_record::walk(Item_processor, enum_walk, uchar *) {
  return false;
}

static bool cmp_index_str_key(const String *key1, const String *key2) {
  if ((key1 == nullptr && key2) || (key2 == nullptr && key1)) return false;
  if (my_strnncoll(&my_charset_bin, pointer_cast<const uchar *>(key1->ptr()),
                   key1->length(), pointer_cast<const uchar *>(key2->ptr()),
                   key2->length()) < 0)
    return true;
  return false;
}

static bool cmp_index_int_key(String *key1, String *key2) {
  if ((key1 == nullptr && key2) || (key2 == nullptr && key1)) return false;
  if (std::atoi(key1->c_ptr_safe()) < std::atoi(key2->c_ptr_safe()))
    return true;
  return false;
}

bool Table_function_record::write_row_and_cmp_index(Item *item_index) {
  m_row_count++;
  // sort index asc
  if (item_index) {
    String tmp;
    String *result = item_index->this_item()->val_str(&tmp);
    THD *thd = current_thd;
    Query_arena backup_arena;
    if (thd->sp_runtime_ctx)
      thd->swap_query_arena(*thd->sp_runtime_ctx->callers_arena, &backup_arena);
    const auto x_guard = create_scope_guard([&]() {
      if (thd->sp_runtime_ctx)
        thd->swap_query_arena(backup_arena, thd->sp_runtime_ctx->callers_arena);
    });
    char *chr = thd->strmake(result->c_ptr_safe(), result->length());
    String *value_result =
        new (thd->mem_root) String(chr, result->length(), &my_charset_bin);
    m_first_last_str_list.push_back(value_result);
    if (m_first_last_str_list.size() < 2) {
      return Table_function::write_row();
    }
    if (m_is_str_index)
      m_first_last_str_list.sort(cmp_index_str_key);
    else
      m_first_last_str_list.sort(cmp_index_int_key);

    if (m_first_last_str_list.size() == 2) {
      String *str_last = m_first_last_str_list.pop();
      if ((m_is_str_index &&
           !cmp_index_str_key(m_first_last_str_list.head(), str_last)) ||
          (!m_is_str_index &&
           !cmp_index_int_key(m_first_last_str_list.head(), str_last)))
        m_first_last_str_list.push_front(str_last);
      else
        m_first_last_str_list.push_back(str_last);
    }
    if (m_first_last_str_list.size() > 2) {
      String *str_last = m_first_last_str_list.pop();
      m_first_last_str_list.pop()->mem_free();
      m_first_last_str_list.push_back(str_last);
    }
  }
  return Table_function::write_row();
}

bool Table_function_record::do_init_args() { return false; }

void Table_function_record::do_cleanup() {
  m_vt_list.clear();
  clear_first_last_str_list();
}

bool Table_function_record::prepare_index_lookup_part(Item *item_index,
                                                      Index_lookup *ref) {
  /* set up fieldref */
  if (init_ref(current_thd, 1, table->key_info->key_part[0].store_length, 0,
               ref)) {
    return true;
  }
  uchar *key_buff = ref->key_buff;
  if (init_ref_part(current_thd, 0, item_index, nullptr,
                    /*null_rejecting*/ true, /*join->const_table_map*/ 1,
                    /*used_tables*/ 0, /*nullable*/ true,
                    table->key_info->key_part, key_buff, ref)) {
    return true;
  }
  return false;
}

Item *Table_function_record::select_result_table_one_row_col(Item *item_index,
                                                             int col) {
  if ((col != -1 && !table->field[col])) return nullptr;
  if (table_index_read_map(item_index, 0)) return nullptr;
  store_record(table, record[1]);
  if (table->file->inited) table->file->ha_index_end();

  Item *item_result = nullptr;
  if (col != -1) {
    item_result = m_item_field_row->element_index(col);
  } else
    item_result = m_item_field_row;
  return item_result;
}

bool Table_function_record::select_result_table_one_row(
    Item *item_index, mem_root_deque<Item *> *items) {
  Item *item_result = select_result_table_one_row_col(item_index, -1);
  if (!item_result) {
    my_error(ER_SP_NOT_EXIST_OF_RECORD_TABLE, MYF(0), table->alias);
    return true;
  }
  for (uint col = 0; col < table->s->fields; col++) {
    Item_field *item_tmp =
        new (current_thd->mem_root) Item_field(table->field[col]);
    item_tmp->table_name = table->s->table_name.str;
    items->push_back(item_tmp);
  }
  return false;
}

bool Table_function_record::create_result_table(THD *thd, ulonglong options,
                                                const char *table_alias) {
  if (table) return false;

  table = create_tmp_table_from_fields(thd, *get_field_list(), false, options,
                                       table_alias);
  if (!table) return true;
  if (create_tmp_table_unique_index_info(thd, table, table_alias,
                                         get_field_list()))
    return true;
  return table == nullptr;
}

bool Table_function_record::copy_to_other_table(
    Table_function_record *dst_table) {
  dst_table->init_table();
  if (table->file->ha_rnd_init(true)) return true;
  bool rc = true;
  do {
    int res = table->file->ha_rnd_next(table->record[0]);
    if (res == 0) {
      memcpy(dst_table->table->record[0], table->record[0],
             table->s->reclength);
      Item *item_index = nullptr;
      Query_arena backup_arena;
      if (current_thd->sp_runtime_ctx)
        current_thd->swap_query_arena(
            *current_thd->sp_runtime_ctx->callers_arena, &backup_arena);
      const auto x_guard = create_scope_guard([&]() {
        if (current_thd->sp_runtime_ctx)
          current_thd->swap_query_arena(
              backup_arena, current_thd->sp_runtime_ctx->callers_arena);
      });
      item_index = new (current_thd->mem_root)
          Item_field(table->field[0]->clone(current_thd->mem_root));
      if (dst_table->write_row_and_cmp_index(item_index)) {
        break;
      }
    } else if (res == HA_ERR_END_OF_FILE) {
      rc = false;
      break;
    } else {
      table->file->print_error(res, MYF(0));
      break;
    }
  } while (true);

  table->file->ha_rnd_end();

  return rc;
}

bool Table_function_udt::init() {
  Item *dummy = m_source;
  if (m_source->fix_fields(current_thd, &dummy)) return true;
  if (!m_source->this_item()->is_ora_table() &&
      !m_source->this_item()->result_type_table()) {
    my_error(ER_UDT_NONTESTED_TABLE_ITEM, myf(0));
    return true;
  }
  m_vt_list = *m_source->this_item()->get_field_create_field_list();
  return false;
}

List<Create_field> *Table_function_udt::get_field_list() { return &m_vt_list; }
// copy data from table_from to this.
bool Table_function_udt::fill_result_table() {
  // reset table
  empty_table();
  ulonglong upper_bound;
  if (m_upper_bound_precalculated) {
    upper_bound = m_precalculated_upper_bound;
  } else {
    upper_bound = calculate_upper_bound();
    if (m_source->const_item()) m_upper_bound_precalculated = true;
  }

  if (upper_bound > tf_udt_table_max_rows) {
    my_error(ER_UDT_TABLE_SIZE_LIMIT, MYF(0), tf_sequence_table_max_upper_bound,
             upper_bound);
    return true;
  }

  Item_func_udt_table *item_udt = dynamic_cast<Item_func_udt_table *>(m_source);
  if (item_udt) {
    for (uint i = 0; i < item_udt->arg_count; i++) {
      if (item_udt->arguments()[i]->this_item()->udt_table_store_to_table(
              table))
        return true;
      if (write_row()) return true;
    }
  }
  Item_func_udt_single_type_table *udt_single =
      dynamic_cast<Item_func_udt_single_type_table *>(m_source);
  if (udt_single) {
    for (uint i = 0; i < udt_single->arg_count; i++) {
      udt_single->arguments()[i]->save_in_field(get_field(0), false);
      if (write_row()) return true;
    }
  }

  Item_splocal *item_splocal = dynamic_cast<Item_splocal *>(m_source);
  if (item_splocal) {
    Item_field_row_table *item_field_row_table =
        dynamic_cast<Item_field_row_table *>(item_splocal->this_item());
    if (item_field_row_table == nullptr) return true;
    Field_row_table *field = item_field_row_table->get_row_table_field();
    if (field == nullptr) return true;
    TABLE *table_record = field->table_record->table;
    if (table_record->file->ha_rnd_init(true)) return true;

    do {
      int res = table_record->file->ha_rnd_next(table->record[0]);
      if (res == 0) {
        if (write_row()) return true;
      } else if (res == HA_ERR_END_OF_FILE) {
        break;
      } else {
        table_record->file->print_error(res, MYF(0));
        return true;
      }
    } while (true);

    table_record->file->ha_rnd_end();
  }
  return false;
}

ulonglong Table_function_udt::calculate_upper_bound() const {
  ulonglong res = 0;
  if (m_source->is_ora_table()) {
    Item_func *item_udt = dynamic_cast<Item_func *>(m_source);
    res = item_udt->argument_count();
  }
  return res;
}

table_map Table_function_udt::used_tables() { return m_source->used_tables(); }

bool Table_function_udt::print(const THD *thd, String *str,
                               enum_query_type query_type) const {
  if (str->append(STRING_WITH_LEN("table("))) return true;
  m_source->print(thd, str, query_type);
  if (thd->is_error()) return true;
  return str->append(')');
}

bool Table_function_udt::walk(Item_processor processor, enum_walk walk,
                              uchar *arg) {
  // Only 'source' may reference columns of other tables; rest is literals.
  return m_source->walk(processor, walk, arg);
}

bool Table_function_udt::do_init_args() {
  if (m_source->const_item()) {
    m_precalculated_upper_bound = calculate_upper_bound();
    m_upper_bound_precalculated = true;
  }
  return false;
}

void Table_function_udt::do_cleanup() {
  m_source->cleanup();
  m_vt_list.clear();
  m_upper_bound_precalculated = false;
  m_precalculated_upper_bound = 0;
}
