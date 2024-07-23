/* Copyright (c) 2002, 2022, Oracle and/or its affiliates. All rights reserved.
   Copyright (c) 2023, 2024, GreatDB Software Co., Ltd.

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

#include "sql/sp_rcontext.h"

#include <atomic>
#include <new>

#include <scope_guard.h>
#include "my_alloc.h"
#include "my_dbug.h"
#include "my_sys.h"
#include "mysql/components/services/bits/psi_bits.h"
#include "mysqld_error.h"
#include "sql/current_thd.h"
#include "sql/dd/dd_routine.h"
#include "sql/derror.h"  // ER_THD
#include "sql/field.h"
#include "sql/protocol.h"
#include "sql/sp.h"           // sp_eval_instr
#include "sql/sp_instr.h"     // sp_instr
#include "sql/sp_pcontext.h"  // sp_pcontext
#include "sql/sql_class.h"    // THD
#include "sql/sql_cursor.h"   // mysql_open_cursor
#include "sql/sql_list.h"
#include "sql/sql_tmp_table.h"  // create_tmp_table_from_fields
#include "sql/table_function.h"
#include "sql/thd_raii.h"    // Prepared_stmt_arena_holder
#include "template_utils.h"  // delete_container_pointers

class Query_expression;

extern "C" void sql_alloc_error_handler(void);

Sp_rcontext_handler_local sp_rcontext_handler_local;
Sp_rcontext_handler_package_body sp_rcontext_handler_package_body;

sp_rcontext *Sp_rcontext_handler_local::get_rcontext(sp_rcontext *ctx) const {
  return ctx;
}

sp_rcontext *Sp_rcontext_handler_package_body::get_rcontext(
    sp_rcontext *ctx) const {
  assert(ctx->sp->m_parent && ctx->sp->m_parent->m_rcontext);
  return ctx->sp->m_parent->m_rcontext;
}

const LEX_CSTRING *Sp_rcontext_handler_local::get_name_prefix() const {
  return &EMPTY_CSTR;
}

const LEX_CSTRING *Sp_rcontext_handler_package_body::get_name_prefix() const {
  static const LEX_CSTRING sp_package_body_variable_prefix_clex_str = {
      STRING_WITH_LEN("PACKAGE_BODY.")};
  return &sp_package_body_variable_prefix_clex_str;
}

///////////////////////////////////////////////////////////////////////////
// sp_rcontext implementation.
///////////////////////////////////////////////////////////////////////////

sp_rcontext::sp_rcontext(const sp_pcontext *root_parsing_ctx,
                         Field *return_value_fld, bool in_sub_stmt)
    : end_partial_result_set(false),
      m_root_parsing_ctx(root_parsing_ctx),
      m_var_table(nullptr),
      m_return_value_fld(return_value_fld),
      m_return_value_set(false),
      m_in_sub_stmt(in_sub_stmt),
      m_visible_handlers(PSI_INSTRUMENT_ME),
      m_activated_handlers(PSI_INSTRUMENT_ME),
      m_ccount(0) {}

sp_rcontext::~sp_rcontext() {
  if (m_var_table) {
    free_blobs(m_var_table);
    destroy(m_var_table);
  }

  delete_container_pointers(m_activated_handlers);
  delete_container_pointers(m_visible_handlers);
  assert(m_ccount == 0);

  // Leave m_var_items and m_case_expr_holders untouched.
  // They are allocated in mem roots and will be freed accordingly.
}

sp_rcontext *sp_rcontext::create(THD *thd, const sp_pcontext *root_parsing_ctx,
                                 Field *return_value_fld) {
  sp_rcontext *ctx = new (thd->mem_root)
      sp_rcontext(root_parsing_ctx, return_value_fld, thd->in_sub_stmt);

  if (!ctx) return nullptr;

  if (ctx->alloc_arrays(thd) || ctx->init_var_table(thd) ||
      ctx->init_var_items(thd)) {
    destroy(ctx);
    return nullptr;
  }
  return ctx;
}

bool sp_rcontext::find_row_field_by_name_or_error(
    uint *field_idx, uint var_idx, const Name_string &field_name) {
  TABLE *vtable = virtual_tmp_table_for_row(var_idx);
  if (!vtable) return true;
  Field *row = m_var_table->field[var_idx];
  return vtable->sp_find_field_by_name_or_error(
      field_idx, to_lex_cstring(row->field_name),
      to_lex_cstring(field_name.ptr()));
}

bool sp_rcontext::record_field_expand_table(THD *, Field *field,
                                            Item *row_index) {
  StringBuffer<STRING_BUFFER_USUAL_SIZE> tmp_str;
  String *result = row_index->val_str(&tmp_str);
  if (!field->udt_value_initialized()) {
    my_error(ER_UDT_REF_UNINIT_COMPOSITE, MYF(0));
    return true;
  }
  Field_row_table *row_field = dynamic_cast<Field_row_table *>(field);
  if (!row_field) {
    my_error(ER_NOT_SUPPORTED_YET, MYF(0), "non record table to expand rows");
    return true;
  }
  // varchar type.
  if (row_field->get_index_length() > 0) {
    if ((int)result->length() > row_field->get_index_length()) {
      my_error(ER_WRONG_PARAMCOUNT_TO_TYPE_TABLE, MYF(0), result->ptr());
      return true;
    }
  } else {  // int type.
    if (!row_field->m_is_index_by) {
      if (row_field->check_if_row_not_exist(row_index)) {
        my_error(ER_RECORD_TABLE_OUTSIZE_LIMIT, MYF(0));
        return true;
      }
    }
    if (row_index->this_item()->result_type() != INT_RESULT) {
      char *row_str = result->ptr();
      char *str = row_str;
      char *end = row_str + result->length();
      while (str != end) {
        if (*str == '-') str++;
        if (!my_isdigit(&my_charset_bin, *str++)) {
          my_error(ER_WRONG_PARAMCOUNT_TO_TYPE_TABLE, MYF(0), result->ptr());
          return true;
        }
      }
    }
  }
  return false;
}

bool sp_rcontext::set_variable_row_field(THD *thd, uint var_idx, uint field_idx,
                                         Item **value) {
  DBUG_ENTER("sp_rcontext::set_variable_row_field");
  assert(value);
  TABLE *vtable = virtual_tmp_table_for_row(var_idx);
  if (!vtable) return true;
  m_var_table->field[var_idx]->set_notnull();
  DBUG_RETURN(sp_eval_expr(thd, vtable->field[field_idx], value));
}

bool sp_rcontext::set_variable_row_field_by_name(THD *thd, uint var_idx,
                                                 const Name_string &field_name,
                                                 Item **value) {
  DBUG_ENTER("sp_rcontext::set_variable_row_field_by_name");
  uint field_idx;
  if (find_row_field_by_name_or_error(&field_idx, var_idx, field_name))
    DBUG_RETURN(1);
  Item_field_row *item_row = dynamic_cast<Item_field_row *>(get_item(var_idx));
  if (!item_row->field->udt_value_initialized()) {
    my_error(ER_UDT_REF_UNINIT_COMPOSITE, MYF(0));
    DBUG_RETURN(true);
  }
  DBUG_RETURN(set_variable_row_field(thd, var_idx, field_idx, value));
}

bool sp_rcontext::set_variable_row_table_by_name(
    THD *thd, sp_pcontext *m_pcont, uint var_idx, const Name_string &field_name,
    List<ora_simple_ident_def> *arg_list, Item **value) {
  List_iterator_fast<ora_simple_ident_def> it(*arg_list);
  ora_simple_ident_def *def = nullptr;
  sp_variable *spv = nullptr;
  ora_simple_ident_def *def_before = nullptr;
  Item *def_number_before = nullptr;
  LEX_STRING def_name_before{nullptr, 0};
  Field *field = m_var_table->field[var_idx];
  uint m_field_idx = 0;
  String tmp_str;
  Item_field_row_table *item_table = nullptr;
  uint table_field_idx = 0;
  Create_field *cdf_before = nullptr, *cdf_before_last;
  Item *item = get_item(var_idx);
  if (!sp_prepare_func_item(thd, value)) return true;

  for (uint offset = 0; (def = it++); offset++) {
    // offset[0] is var_idx
    if (offset == 0) {
      spv = m_pcont->find_variable(def->ident.str, def->ident.length, false);
      if (!spv) {
        my_error(ER_SP_UNDECLARED_VAR, MYF(0), def->ident.str);
        return true;
      }
      def_name_before = spv->field_def.udt_name.str
                            ? spv->field_def.udt_name
                            : to_lex_string(spv->field_def.nested_table_udt);
      def_before = def;
      def_number_before = def->number ? def->number : nullptr;
      continue;
    }

    Item *item_number = nullptr;
    if (offset > 0) {
      // It must find the offset of def_before by name.Now it's def_before.def
      spv = m_pcont->find_variable(def_name_before.str, def_name_before.length,
                                   false);
      if (!spv) {
        my_error(ER_SP_UNDECLARED_VAR, MYF(0), def_name_before.str);
        return true;
      }
      if (def_before->number == nullptr) {  // a.b
        if (!spv->field_def.ora_record.row_field_definitions()) {
          my_error(ER_SP_MISMATCH_RECORD_VAR, MYF(0), def_before->ident.str);
          return true;
        }
        cdf_before = spv->field_def.ora_record.row_field_definitions()
                         ->find_row_field_by_name(def->ident, &m_field_idx);
        if (!cdf_before) {
          my_error(ER_ROW_VARIABLE_DOES_NOT_HAVE_FIELD, MYF(0),
                   def_before->ident.str, def->ident.str);
          return true;
        }
        item = item->element_index(m_field_idx);
        Item_field *item_field = dynamic_cast<Item_field *>(item);
        field = item_field->field;
      } else {  // a(1).b
        if (!spv->field_def.ora_record.row_field_table_definitions()) {
          my_error(ER_SP_MISMATCH_RECORD_TABLE_VAR, MYF(0),
                   def_before->ident.str);
          return true;
        }
        item_number = sp_prepare_func_item(thd, &def_before->number);
        if (item_number == nullptr) return true;
        if (!item_number->is_null() &&
            item_number->result_type() != ROW_RESULT) {
          if (record_field_expand_table(thd, field, item_number)) return true;
        } else {
          my_error(ER_WRONG_PARAMCOUNT_TO_TYPE_TABLE, MYF(0),
                   item_number->item_name.ptr());
          return true;
        }

        cdf_before = spv->field_def.ora_record.row_field_table_definitions()
                         ->find_row_field_by_name(def->ident, &m_field_idx);
        if (!cdf_before) {
          my_error(ER_ROW_VARIABLE_DOES_NOT_HAVE_FIELD, MYF(0),
                   def_before->ident.str, def->ident.str);
          return true;
        }
        item_table = dynamic_cast<Item_field_row_table *>(item);
        Item *item_tmp =
            item_table->element_row_index(item_number, m_field_idx);
        if (!item_tmp) {
          if (!(item = insert_into_table_default_and_set_default(
                    thd, item, m_field_idx, item_number)))
            return true;
        } else
          item = item_tmp;
        table_field_idx = m_field_idx;
        Item_field *item_field = dynamic_cast<Item_field *>(item);
        field = item_field->field;
      }
    }
    def_name_before = cdf_before->udt_name.str
                          ? cdf_before->udt_name
                          : to_lex_string(cdf_before->nested_table_udt);
    if (!def_name_before.length) {
      LEX_STRING def_cdf_str{const_cast<char *>(cdf_before->field_name),
                             strlen(cdf_before->field_name)};
      def_name_before = def_cdf_str;
    }
    def_number_before = item_number ? item_number : nullptr;
    def_before = def;
  }  // end of for
  spv = m_pcont->find_variable(def_name_before.str, def_name_before.length,
                               false);
  if (!spv) {
    spv = search_udt_variable(def_name_before.str, def_name_before.length,
                              nullptr, 0);
    if (!spv) {
      my_error(ER_SP_UNDECLARED_VAR, MYF(0), def_name_before.str);
      return true;
    }
  }
  if (def_before->number == nullptr) {  // a.b
    if (!spv->field_def.ora_record.row_field_definitions()) {
      my_error(ER_SP_MISMATCH_RECORD_VAR, MYF(0), def_before->ident.str);
      return true;
    }
    cdf_before_last =
        spv->field_def.ora_record.row_field_definitions()
            ->find_row_field_by_name(
                to_lex_string(to_lex_cstring(field_name.ptr())), &m_field_idx);
    if (!cdf_before_last) {
      my_error(ER_ROW_VARIABLE_DOES_NOT_HAVE_FIELD, MYF(0),
               def_before->ident.str, field_name.ptr());
      return true;
    }
    if (!field->udt_value_initialized()) {
      my_error(ER_UDT_REF_UNINIT_COMPOSITE, MYF(0));
      return true;
    }
    Field_udt_type *udt_field = dynamic_cast<Field_udt_type *>(field);
    if (udt_field) {
      if (udt_field->update_field_of_table(thd, m_field_idx, value))
        return true;
      return item_table->update_table_value(thd, nullptr, table_field_idx,
                                            def_number_before, udt_field);
    } else {
      field = field->val_udt()->field[m_field_idx];
      return sp_eval_expr(thd, field, value);
    }
  } else {  // a(1).b
    if (!spv->field_def.ora_record.row_field_table_definitions()) {
      my_error(ER_SP_MISMATCH_RECORD_TABLE_VAR, MYF(0), def_before->ident.str);
      return true;
    }
    Item *item_number = sp_prepare_func_item(thd, &def_before->number);
    if (item_number == nullptr) return true;
    if (item_number->is_null() || item_number->result_type() == ROW_RESULT) {
      my_error(ER_WRONG_PARAMCOUNT_TO_TYPE_TABLE, MYF(0),
               item_number->item_name.ptr());
      return true;
    }
    Item *item_str_index = make_item_index(thd, item_number);
    if (!item_str_index ||
        record_field_expand_table(thd, field, item_str_index))
      return true;
    cdf_before_last =
        spv->field_def.ora_record.row_field_table_definitions()
            ->find_row_field_by_name(
                to_lex_string(to_lex_cstring(field_name.ptr())), &m_field_idx);
    if (!cdf_before_last) {
      my_error(ER_ROW_VARIABLE_DOES_NOT_HAVE_FIELD, MYF(0),
               def_before->ident.str, field_name.ptr());
      return true;
    }
    item_table = dynamic_cast<Item_field_row_table *>(item);
    Item *item_tmp = item_table->element_row_index(item_str_index, m_field_idx);
    if (!item_tmp)
      if (!(item = insert_into_table_default_and_set_default(
                thd, item, m_field_idx, item_str_index)))
        return true;
    return item_table->update_table_value(thd, value, m_field_idx,
                                          item_str_index);
  }
}

Item *sp_rcontext::insert_into_table_default_and_set_default(THD *thd,
                                                             Item *item,
                                                             uint m_field_idx,
                                                             Item *index_item) {
  // For example:v_t_rec3.v_t_rec2(0).v_t_rec1(0).col1,there is no data
  // in v_t_rec3.v_t_rec2(0),so it must insert new row of all null value.
  Item_field_row_table *item_table = dynamic_cast<Item_field_row_table *>(item);
  if (!item_table) return nullptr;

  Item *index_item_tmp = sp_prepare_func_item(thd, &index_item);
  if (item_table->create_item_for_store_default_value_and_insert_into_table(
          thd, this, index_item_tmp))
    return nullptr;

  Item *item_result =
      item_table->element_row_index(index_item_tmp, m_field_idx);
  if (!item_result) {
    my_error(ER_UDT_REF_UNINIT_COMPOSITE, MYF(0));
  }
  return item_result;
}

bool sp_rcontext::set_variable_row_table_by_index(THD *thd, uint var_idx,
                                                  Item *index, Item **value) {
  Item *item = get_item(var_idx);
  Item_field_row_table *item_table = dynamic_cast<Item_field_row_table *>(item);
  if (!item_table) {
    my_error(ER_SP_MISMATCH_RECORD_TABLE_VAR, MYF(0), item->item_name.m_str);
    return true;
  }
  Field *field = item_table->field;
  if (!field->udt_value_initialized()) {
    my_error(ER_UDT_REF_UNINIT_COMPOSITE, MYF(0));
    return true;
  }

  Item *item_index = sp_prepare_func_item(thd, &index);
  if (item_index == nullptr) return true;
  if (item_index->is_null()) {
    my_error(ER_NOT_SUPPORTED_YET, MYF(0), "record table with null index");
    return true;
  }
  Item *item_str_index = make_item_index(thd, item_index);
  if (!item_str_index) return true;
  // Avoid stu_record_val1(3) := stu_record_val1(3) when stu_record_val1(3) is
  // empty.
  if (!sp_prepare_func_item(thd, value)) return true;
  Field_row_table *row_field = dynamic_cast<Field_row_table *>(field);

  if (search_udt_variable(
          field->udt_name().str, field->udt_name().length,
          field->get_udt_db_name(),
          field->get_udt_db_name() ? strlen(field->get_udt_db_name()) : 0)) {
    if (row_field->check_if_row_not_exist(item_str_index)) {
      my_error(ER_UDT_REF_UNINIT_COMPOSITE, MYF(0));
      return true;
    }
  }

  if (record_field_expand_table(thd, field, item_str_index)) return true;

  Item *item_tmp = item_table->element_row_index(item_str_index, 1);
  if (!item_tmp) {
    mem_root_deque<Item *> items(thd->mem_root);
    items.push_back(*value);
    return item_table->insert_into_table_all_value(thd, items, item_str_index);
  }

  return item_table->update_table_value(thd, value, -1, item_str_index);
}

bool sp_rcontext::set_variable_row(THD *thd, uint var_idx,
                                   const mem_root_deque<Item *> &items) {
  Field *field_first = m_var_table->field[var_idx];
  Field_row *field_row = dynamic_cast<Field_row *>(field_first);
  field_row->set_notnull();
  TABLE *vtable = virtual_tmp_table_for_row(var_idx);
  if (!vtable) return true;
  m_var_table->field[var_idx]->set_notnull();
  auto item_iter = VisibleFields(items).begin();
  int count = 0;
  int size = CountVisibleFields(items);
  while (count < size) {
    Item *item = *item_iter++;
    if (vtable->field[count]->sp_prepare_and_store_item(thd, &item)) {
      return true;
    }
    count++;
  }
  return false;
}

/*It must be inited when it's called from select bulk collect into.*/
bool sp_rcontext::set_variable_row_table(THD *thd, uint var_idx,
                                         const mem_root_deque<Item *> &items,
                                         ha_rows row_count, bool is_bulk_into) {
  Item *item_var = get_item(var_idx);
  Field *field_first = m_var_table->field[var_idx];
  Field_row_table *row_table = dynamic_cast<Field_row_table *>(field_first);
  auto strict_guard =
      create_scope_guard([row_table, saved_index = row_table->m_is_index_by]() {
        row_table->m_is_index_by = saved_index;
      });
  if (is_bulk_into) {
    field_first->set_udt_value_initialized(true);
    row_table->m_is_index_by = true;
  }
  Item_field_row_table *item_table =
      dynamic_cast<Item_field_row_table *>(item_var);
  Item *item_index = new (thd->mem_root) Item_uint(row_count);
  if (record_field_expand_table(thd, field_first, item_index)) return true;
  return item_table->insert_into_table_all_value(thd, items, item_index,
                                                 is_bulk_into);
}

void sp_rcontext::cleanup_record_variable_row_table(uint var_idx) {
  Item *item = get_item(var_idx);
  item->clear_args_table();
}

void sp_rcontext::cleanup_record_variable_tmp_row_table(uint var_idx) {
  Item *item = get_item(var_idx);
  Item_field_row_table *item_table = dynamic_cast<Item_field_row_table *>(item);
  item_table->get_row_table_field()->tmp_table_record->init_table();
}

bool sp_rcontext::fill_bulk_table_with_tmp_table(uint var_idx) {
  Item *item = get_item(var_idx);
  Item_field_row_table *item_table = dynamic_cast<Item_field_row_table *>(item);
  return item_table->get_row_table_field()->fill_bulk_table_with_tmp_table();
}

TABLE *sp_rcontext::virtual_tmp_table_for_row(uint var_idx) {
  if (get_item(var_idx)->type() != Item::ORACLE_ROWTYPE_ITEM ||
      get_item(var_idx)->result_type() != ROW_RESULT) {
    my_error(ER_SP_MISMATCH_RECORD_VAR, MYF(0),
             get_item(var_idx)->item_name.ptr());
    return nullptr;
  }
  Field *field = m_var_table->field[var_idx];
  return field->val_udt();
}

bool sp_rcontext::alloc_arrays(THD *thd) {
  {
    size_t n = m_root_parsing_ctx->max_cursor_index();
    m_cstack.reset(
        static_cast<sp_cursor **>(thd->alloc(n * sizeof(sp_cursor *))), n);
  }

  {
    size_t n = m_root_parsing_ctx->get_num_case_exprs();
    m_case_expr_holders.reset(
        static_cast<Item_cache **>(thd->mem_calloc(n * sizeof(Item_cache *))),
        n);
  }

  return !m_cstack.array() || !m_case_expr_holders.array();
}

bool sp_rcontext::init_var_table(THD *thd) {
  List<Create_field> field_def_lst;

  if (!m_root_parsing_ctx->max_var_index()) return false;

  m_vars_list.clear();
  m_udt_vars_list.clear();
  uint udt_var_count_diff = 0;
  m_root_parsing_ctx->retrieve_field_definitions(
      &field_def_lst, &m_vars_list, &m_udt_vars_list, &udt_var_count_diff);
  m_root_parsing_ctx->retrieve_udt_field_definitions(thd, &m_udt_vars_list);

  if (field_def_lst.elements != m_root_parsing_ctx->max_var_index())
    return true;

  if (m_udt_vars_list.elements !=
      m_root_parsing_ctx->max_udt_var_index() + udt_var_count_diff)
    return true;

  if (!(m_var_table = create_tmp_table_from_fields(thd, field_def_lst)))
    return true;

  m_var_table->copy_blobs = true;
  m_var_table->alias = "";

  return false;
}

bool sp_rcontext::init_var_items(THD *thd) {
  uint num_vars = m_root_parsing_ctx->max_var_index();

  List<sp_variable> m_vars = m_vars_list;
  List_iterator_fast<sp_variable> li(m_vars);
  m_var_items.reset(static_cast<Item **>(thd->alloc(num_vars * sizeof(Item *))),
                    num_vars);

  if (!m_var_items.array()) return true;

  uint idx = 0;
  sp_variable *vars;
  while ((vars = li++)) {
    if (vars->field_def.udt_name.str) {
      m_var_table->field[idx]->set_udt_name(
          to_lex_cstring(vars->field_def.udt_name.str));
    }
    if (vars->field_def.udt_db_name.str) {
      m_var_table->field[idx]->set_udt_db_name(vars->field_def.udt_db_name.str);
    }

    if (vars->field_def.ora_record.is_record_define ||
        vars->field_def.ora_record.is_record_table_define ||
        vars->field_def.ora_record.is_index_by ||
        (vars->field_def.ora_record.row_field_definitions() &&
         // rec3 rec%type;rec3 must set initialize.
         !search_udt_variable(vars->field_def.udt_name.str,
                              vars->field_def.udt_name.length,
                              vars->field_def.udt_db_name.str,
                              vars->field_def.udt_db_name.length)) ||
        vars->field_def.ora_record.is_cursor_rowtype_ref() ||
        (vars->field_def.ora_record.table_rowtype_ref() &&
         !vars->field_def.ora_record.row_field_table_definitions())) {
      m_var_table->field[idx]->set_udt_value_initialized(true);
      m_var_table->field[idx]->set_notnull();
    }
    if (vars->field_def.ora_record.is_ref_cursor) {
      m_var_items[idx] =
          new (thd->mem_root) Item_field_refcursor(m_var_table->field[idx]);
      if (!m_var_items[idx]) return true;
      if (vars->field_def.ora_record.row_field_definitions()) {
        if (m_var_items[idx]->row_create_items(
                thd, vars->field_def.ora_record.row_field_definitions()))
          return true;
      }
    } else if (!vars->field_def.ora_record.is_ref_cursor &&
               (vars->field_def.ora_record.is_cursor_rowtype_ref() ||
                vars->field_def.ora_record.row_field_definitions()) &&
               !vars->field_def.ora_record.row_field_table_definitions()) {
      m_var_items[idx] =
          new (thd->mem_root) Item_field_row(m_var_table->field[idx]);
      if (!m_var_items[idx]) return true;
      // it's udt column like tt_air.udt%type
      if (vars->field_def.ora_record.column_type_ref() ||
          vars->field_def.ora_record.m_is_sp_param) {
        if (m_var_items[idx]->row_create_items(
                thd, vars->field_def.ora_record.row_field_definitions()))
          return true;
      }
    } else if (vars->field_def.ora_record.row_field_table_definitions()) {
      Field_row_table *row_field =
          dynamic_cast<Field_row_table *>(m_var_table->field[idx]);
      row_field->m_is_record_table_type_define =
          vars->field_def.ora_record.is_record_table_type_define;
      row_field->m_is_index_by = vars->field_def.ora_record.is_index_by;
      if (vars->field_def.nested_table_udt.str) {
        row_field->set_nested_table_udt(vars->field_def.nested_table_udt);
      }
      row_field->m_varray_size_limit =
          vars->field_def.table_type == 0 /*VARRAY*/ ? vars->field_def
                                                           .varray_limit
                                                     : -1;
      Item_field_row_table *item =
          new (thd->mem_root) Item_field_row_table(m_var_table->field[idx]);
      m_var_items[idx] = item;
      item->set_item_table_name(vars->name.str);
      if (vars->field_def.ora_record.m_is_sp_param) {
        if (item->row_table_create_items(
                thd, vars->field_def.ora_record.row_field_table_definitions()))
          return true;
      }
    } else {
      if (!(m_var_items[idx] =
                new (thd->mem_root) Item_field(m_var_table->field[idx])))
        return true;
    }
    ++idx;
  }
  return false;
}

/* ident t1%rowtype and ident t1.udt%type,if there is Field_udt_type,
  must add it to m_udt_vars_list.
*/
bool sp_rcontext::add_udt_to_sp_var_list(THD *thd,
                                         List<Create_field> field_def_lst) {
  List_iterator_fast<Create_field> it_cdf(field_def_lst);
  Create_field *cdf;
  for (uint i = 0; (cdf = it_cdf++); i++) {
    if (cdf->udt_name.str) {
      LEX_STRING udt_db_name =
          cdf->udt_db_name.str ? cdf->udt_db_name : to_lex_string(thd->db());
      sp_variable *spvar_udt = m_root_parsing_ctx->find_udt_variable(
          cdf->udt_name.str, cdf->udt_name.length, udt_db_name.str,
          udt_db_name.length, false);
      if (!spvar_udt) {
        spvar_udt = new (thd->mem_root)
            sp_variable(cdf->udt_name, cdf->sql_type, sp_variable::MODE_IN,
                        m_udt_vars_list.size() + 1);
        spvar_udt->field_def = *cdf;
        m_udt_vars_list.push_back(spvar_udt);
      }
    }
  }
  return false;
}

sp_variable *sp_rcontext::search_udt_variable(const char *name, size_t name_len,
                                              const char *db_name,
                                              size_t db_name_len) {
  if (!db_name) {
    db_name = current_thd->db().str;
    db_name_len = current_thd->db().length;
  }
  List_iterator_fast<sp_variable> it(m_udt_vars_list);
  sp_variable *p;
  for (uint i = 0; (p = it++); i++) {
    LEX_STRING p_db_name = p->field_def.udt_db_name.str
                               ? p->field_def.udt_db_name
                               : to_lex_string(current_thd->db());
    if (my_strnncoll(system_charset_info, pointer_cast<const uchar *>(name),
                     name_len, pointer_cast<const uchar *>(p->name.str),
                     p->name.length) == 0 &&
        my_strnncoll(system_charset_info, pointer_cast<const uchar *>(db_name),
                     db_name_len, pointer_cast<const uchar *>(p_db_name.str),
                     p_db_name.length) == 0) {
      return p;
    }
  }

  return p;
}

sp_variable *sp_rcontext::search_variable(const char *name, size_t name_len) {
  List_iterator_fast<sp_variable> it(m_vars_list);
  sp_variable *p;
  for (uint i = 0; (p = it++); i++) {
    if (my_strnncoll(system_charset_info, pointer_cast<const uchar *>(name),
                     name_len, pointer_cast<const uchar *>(p->name.str),
                     p->name.length) == 0) {
      return p;
    }
  }

  return p;
}

bool sp_rcontext::set_return_value(THD *thd, Item **return_value_item) {
  assert(m_return_value_fld);

  m_return_value_set = true;

  return sp_eval_expr(thd, m_return_value_fld, return_value_item);
}

bool sp_rcontext::push_cursor(sp_instr_cpush *i, uint cursor_offset) {
  /*
    We should create cursors on the system heap because:
     - they could be (and usually are) used in several instructions,
       thus they can not be stored on an execution mem-root;
     - a cursor can be pushed/popped many times in a loop, having these objects
       on callers' mem-root would lead to a memory leak in every iteration.
  */
  sp_cursor *c = new (std::nothrow) sp_cursor(i, cursor_offset);

  if (!c) {
    sql_alloc_error_handler();
    return true;
  }
  m_cstack[m_ccount++] = c;
  return false;
}

sp_cursor *sp_rcontext::get_cursor(uint i) const {
  for (uint j = 0; j < m_ccount; j++) {
    sp_cursor *c = m_cstack[j];
    if (c->m_cursor_offset == i) return c;
  }
  return nullptr;
}

void sp_rcontext::pop_cursors(uint count) {
  assert(m_ccount >= count);

  while (count--) {
    m_ccount--;
    if (m_cstack[m_ccount]->is_open()) m_cstack[m_ccount]->close();
    delete m_cstack[m_ccount];
  }
}

bool sp_rcontext::push_handler(sp_handler *handler, uint first_ip) {
  /*
    We should create handler entries on the system heap because:
     - they could be (and usually are) used in several instructions,
       thus they can not be stored on an execution mem-root;
     - a handler can be pushed/popped many times in a loop, having these
       objects on callers' mem-root would lead to a memory leak in every
       iteration.
  */
  sp_handler_entry *he = new (std::nothrow) sp_handler_entry(handler, first_ip);

  if (!he) {
    sql_alloc_error_handler();
    return true;
  }

  return m_visible_handlers.push_back(he);
}

void sp_rcontext::pop_handlers(sp_pcontext *current_scope, uint count) {
  if (count == 0) count = m_visible_handlers.size();
  for (int i = count - 1; i >= 0; --i) {
    int handler_level = m_visible_handlers.at(i)->handler->scope->get_level();

    if (handler_level >= current_scope->get_level()) {
      delete m_visible_handlers.back();
      m_visible_handlers.pop_back();
    }
  }
}

void sp_rcontext::pop_handler_frame(THD *thd) {
  Handler_call_frame *frame = m_activated_handlers.back();
  m_activated_handlers.pop_back();

  // Also pop matching DA and copy new conditions.
  assert(thd->get_stmt_da() == &frame->handler_da);
  thd->pop_diagnostics_area();
  // Out with the old, in with the new!
  thd->get_stmt_da()->reset_condition_info(thd);
  thd->get_stmt_da()->copy_new_sql_conditions(thd, &frame->handler_da);

  delete frame;
}

void sp_rcontext::exit_handler(THD *thd, sp_pcontext *target_scope) {
  /*
    The handler has successfully completed. We should now pop the current
    handler frame and pop the diagnostics area which was pushed when the
    handler was activated.

    The diagnostics area of the caller should then contain only any
    conditions pushed during handler execution. The conditions present
    when the handler was activated should be removed since they have
    been successfully handled.
  */
  pop_handler_frame(thd);

  // Pop frames below the target scope level.
  for (int i = static_cast<int>(m_activated_handlers.size()) - 1; i >= 0; --i) {
    int handler_level = m_activated_handlers.at(i)->handler->scope->get_level();

    if (handler_level <= target_scope->get_level()) break;

    pop_handler_frame(thd);
  }

  /*
    Condition was successfully handled, reset condition count
    so we don't trigger the handler again for the same warning.
  */
  thd->get_stmt_da()->reset_statement_cond_count();
}

bool sp_rcontext::handle_sql_condition(THD *thd, uint *ip,
                                       const sp_instr *cur_spi) {
  DBUG_TRACE;

  /*
    If this is a fatal sub-statement error, and this runtime
    context corresponds to a sub-statement, no CONTINUE/EXIT
    handlers from this context are applicable: try to locate one
    in the outer scope.
  */
  if (thd->is_fatal_sub_stmt_error && m_in_sub_stmt) return false;

  Diagnostics_area *da = thd->get_stmt_da();
  const sp_handler *found_handler = nullptr;
  Sql_condition *found_condition = nullptr;

  if (thd->is_error()) {
    sp_pcontext *cur_pctx = cur_spi->get_parsing_ctx();

    if (thd->variables.sql_mode & MODE_ORACLE) {
      found_handler =
          cur_pctx->find_handler(da->returned_sqlstate(), da->mysql_errno(),
                                 Sql_condition::SL_ERROR, da->user_defined());
    } else {
      found_handler = cur_pctx->find_handler(
          da->returned_sqlstate(), da->mysql_errno(), Sql_condition::SL_ERROR);
    }

    if (found_handler) {
      found_condition = da->error_condition();

      /*
        error_condition can be NULL if the Diagnostics Area was full
        when the error was raised. It can also be NULL if
        Diagnostics_area::set_error_status(uint sql_error) was used.
        In these cases, make a temporary Sql_condition here so the
        error can be handled.
      */
      if (!found_condition) {
        found_condition = new (callers_arena->mem_root) Sql_condition(
            callers_arena->mem_root, da->mysql_errno(), da->returned_sqlstate(),
            Sql_condition::SL_ERROR, da->message_text());
      }
    }
  } else if (da->current_statement_cond_count()) {
    Diagnostics_area::Sql_condition_iterator it = da->sql_conditions();
    const Sql_condition *c;

    /*
      Here we need to find the last warning/note from the stack.
      In MySQL most substantial warning is the last one.
      (We could have used a reverse iterator here if one existed.)
      We ignore preexisting conditions so we don't throw for them
      again if the next statement isn't one that pre-clears the
      DA. (Critically, that includes hpush_jump, i.e. any handlers
      declared within the one we're calling. At that point, the
      catcher for our throw would become very hard to predict!)
      One benefit of not simply clearing the DA as we enter a handler
      (instead of resetting the condition count further down in this
      exact function as we do now) and forcing the user to utilize
      GET STACKED DIAGNOSTICS is that this way, we can make
      SHOW WARNINGS|ERRORS work.
    */

    while ((c = it++)) {
      if (c->severity() == Sql_condition::SL_WARNING ||
          c->severity() == Sql_condition::SL_NOTE) {
        sp_pcontext *cur_pctx = cur_spi->get_parsing_ctx();

        if (thd->variables.sql_mode & MODE_ORACLE) {
          const sp_handler *ora_handler =
              cur_pctx->find_handler(c->returned_sqlstate(), c->mysql_errno(),
                                     c->severity(), c->get_user_defined());
          if (ora_handler) {
            found_handler = ora_handler;
            found_condition = const_cast<Sql_condition *>(c);
          }
        } else {
          const sp_handler *handler = cur_pctx->find_handler(
              c->returned_sqlstate(), c->mysql_errno(), c->severity());
          if (handler) {
            found_handler = handler;
            found_condition = const_cast<Sql_condition *>(c);
          }
        }
      }
    }
  }

  if (!found_handler) return false;

  // At this point, we know that:
  //  - there is a pending SQL-condition (error or warning);
  //  - there is an SQL-handler for it.

  assert(found_condition);

  sp_handler_entry *handler_entry = nullptr;
  for (size_t i = 0; i < m_visible_handlers.size(); ++i) {
    sp_handler_entry *h = m_visible_handlers.at(i);

    if (h->handler == found_handler) {
      handler_entry = h;
      break;
    }
  }

  /*
    handler_entry usually should not be NULL here, as that indicates
    that the parser context thinks a HANDLER should be activated,
    but the runtime context cannot find it.

    However, this can happen (and this is in line with the Standard)
    if SQL-condition has been raised before DECLARE HANDLER instruction
    is processed.

    For example:
    CREATE PROCEDURE p()
    BEGIN
      DECLARE v INT DEFAULT 'get'; -- raises SQL-warning here
      DECLARE EXIT HANDLER ...     -- this handler does not catch the warning
    END
  */
  if (!handler_entry) return false;

  uint continue_ip = handler_entry->handler->type == sp_handler::CONTINUE
                         ? cur_spi->get_cont_dest()
                         : 0;

  /* Add a frame to handler-call-stack. */
  Handler_call_frame *frame = new (std::nothrow)
      Handler_call_frame(found_handler, found_condition, continue_ip);
  if (!frame) {
    sql_alloc_error_handler();
    return false;
  }

  m_activated_handlers.push_back(frame);

  /* End aborted result set. */
  if (end_partial_result_set) thd->get_protocol()->end_partial_result_set();

  /* Reset error state. */
  thd->clear_error();
  thd->killed = THD::NOT_KILLED;  // Some errors set thd->killed
                                  // (e.g. "bad data").

  thd->push_diagnostics_area(&frame->handler_da);

  /*
    Mark current conditions so we later will know which conditions
    were added during handler execution (if any).
  */
  frame->handler_da.mark_preexisting_sql_conditions();

  frame->handler_da.reset_statement_cond_count();

  *ip = handler_entry->first_ip;

  return true;
}

bool sp_rcontext::set_variable(THD *thd, Field *field, Item **value) {
  if (!value) {
    field->set_null();
    return false;
  }
    Item_splocal_row_field_table_by_index *item_index =
        dynamic_cast<Item_splocal_row_field_table_by_index *>(*value);
    if (item_index) {
      Item *expr_item;
      if (!(expr_item = sp_prepare_func_item(thd, value))) return true;
      Item_field_row *item_field_row =
          dynamic_cast<Item_field_row *>(expr_item);
      if (item_field_row)  // stu_record_val1 :=  stu_record_val(2);
        return item_field_row->index_item_save_in_field_no_index(field);
      else  // select stu_record_val(3) into id;
        return sp_eval_expr(thd, field, value);
    }

    return sp_eval_expr(thd, field, value);
}

Item_cache *sp_rcontext::create_case_expr_holder(THD *thd,
                                                 const Item *item) const {
  Item_cache *holder;
  Query_arena backup_arena;

  thd->swap_query_arena(*thd->sp_runtime_ctx->callers_arena, &backup_arena);

  holder = Item_cache::get_cache(item);

  thd->swap_query_arena(backup_arena, thd->sp_runtime_ctx->callers_arena);

  return holder;
}

bool sp_rcontext::set_case_expr(THD *thd, int case_expr_id,
                                Item **case_expr_item_ptr) {
  Item *case_expr_item = sp_prepare_func_item(thd, case_expr_item_ptr);
  if (!case_expr_item) return true;

  if (!m_case_expr_holders[case_expr_id] ||
      m_case_expr_holders[case_expr_id]->result_type() !=
          case_expr_item->result_type()) {
    m_case_expr_holders[case_expr_id] =
        create_case_expr_holder(thd, case_expr_item);
  }

  m_case_expr_holders[case_expr_id]->store(case_expr_item);
  m_case_expr_holders[case_expr_id]->cache_value();
  return false;
}

/*for close c and c:=null,it must reset all the cursor about c.*/
void sp_rcontext::reset_refcursor(const Field_refcursor *field_from) {
  for (uint i = 0; i < m_var_table->visible_field_count(); i++) {
    Field_refcursor *field =
        dynamic_cast<Field_refcursor *>(m_var_table->visible_field_ptr()[i]);
    if (field && field->m_cursor.get() &&
        field->m_cursor.get() == field_from->m_cursor.get() &&
        field != field_from)
      field->m_cursor.reset();
  }
}
///////////////////////////////////////////////////////////////////////////
// sp_cursor implementation.
///////////////////////////////////////////////////////////////////////////
/**
  Open an SP cursor

  @param thd  Thread context

  @return Error status
*/

bool sp_cursor::open(THD *thd) {
  if (m_server_side_cursor != nullptr) {
    my_error(ER_SP_CURSOR_ALREADY_OPEN, MYF(0));
    return true;
  }
  bool rc = mysql_open_cursor(thd, &m_result, &m_server_side_cursor);

  // If execution failed, ensure that the cursor is closed.
  if (rc && m_server_side_cursor != nullptr) {
    m_server_side_cursor->close();
    m_server_side_cursor = nullptr;
  }
  if (!rc && check_cursor_return_type_count()) rc = true;
  return rc;
}

bool sp_cursor::close() {
  if (!is_open()) {
    my_error(ER_SP_CURSOR_NOT_OPEN, MYF(0));
    return true;
  }
  m_server_side_cursor->close();
  m_server_side_cursor = nullptr;
  return false;
}

void sp_cursor::destroy() {
  assert(!m_push_instr || m_server_side_cursor == nullptr);
}

bool sp_cursor::fetch(List<sp_variable> *vars) {
  if (!is_open()) {
    my_error(ER_SP_CURSOR_NOT_OPEN, MYF(0));
    return true;
  }

  bool oracle_mode_flag = false;
  if (current_thd->variables.sql_mode & MODE_ORACLE) oracle_mode_flag = true;
  if (!oracle_mode_flag) {
    if (vars->elements != m_result.get_field_count()) {
      my_error(ER_SP_WRONG_NO_OF_FETCH_ARGS, MYF(0));
      return true;
    }
  } else {
    if (vars->elements != m_result.get_field_count() &&
        (vars->elements != 1 ||
         m_result.get_field_count() !=
             current_thd->sp_runtime_ctx->get_item(vars->head()->offset)
                 ->cols())) {
      my_message(ER_SP_WRONG_NO_OF_FETCH_ARGS,
                 ER_THD(current_thd, ER_SP_WRONG_NO_OF_FETCH_ARGS), MYF(0));
      return -1;
    }
  }

  DBUG_EXECUTE_IF(
      "bug23032_emit_warning",
      push_warning(current_thd, Sql_condition::SL_WARNING, ER_UNKNOWN_ERROR,
                   ER_THD(current_thd, ER_UNKNOWN_ERROR)););

  m_result.set_spvar_list(vars);

  /* Attempt to fetch one row */
  if (m_server_side_cursor->is_open()) {
    if (m_server_side_cursor->fetch(1)) return true;
  }

  /*
    If the cursor was pointing after the last row, the fetch will
    close it instead of sending any rows.
  */
  if (!oracle_mode_flag) {
    if (!m_server_side_cursor->is_open()) {
      my_error(ER_SP_FETCH_NO_DATA, MYF(0));
      return true;
    }
  } else {
    if (!m_server_side_cursor->is_open()) {
      return false;
    }
  }

  return false;
}

bool sp_cursor::fetch_bulk(List<sp_variable> *vars, ha_rows row_count) {
  if (!is_open()) {
    my_error(ER_SP_CURSOR_NOT_OPEN, MYF(0));
    return true;
  }

  if (vars->elements != m_result.get_field_count() &&
      (vars->elements != 1 ||
       m_result.get_field_count() !=
           current_thd->sp_runtime_ctx->get_item(vars->head()->offset)->cols() -
               1)) {
    my_message(ER_SP_WRONG_NO_OF_FETCH_ARGS,
               ER_THD(current_thd, ER_SP_WRONG_NO_OF_FETCH_ARGS), MYF(0));
    return -1;
  }

  DBUG_EXECUTE_IF(
      "bug23032_emit_warning",
      push_warning(current_thd, Sql_condition::SL_WARNING, ER_UNKNOWN_ERROR,
                   ER_THD(current_thd, ER_UNKNOWN_ERROR)););

  m_result.set_spvar_list(vars);
  m_result.set_row_batch(row_count);

  /* Attempt to fetch row_count rows */
  if (m_server_side_cursor->is_open()) {
    if (m_server_side_cursor->fetch(row_count)) return true;
  }

  if (!m_server_side_cursor->is_open()) {
    return false;
  }

  return false;
}

bool sp_cursor::export_structure(THD *thd, List<Create_field> *list) {
  if (!is_open()) {
    my_error(ER_SP_CURSOR_NOT_OPEN, MYF(0));
    return true;
  }
  return m_server_side_cursor->export_structure(thd, list);
}

bool sp_cursor::found() {
  return m_server_side_cursor && m_server_side_cursor->is_open();
}

ulonglong sp_cursor::fetch_count() const {
  return m_server_side_cursor ? m_server_side_cursor->get_fetch_count() : 0;
}

bool sp_cursor::is_cursor_fetched() {
  return m_server_side_cursor && m_server_side_cursor->is_cursor_fetched();
}

// check cursor return type
bool sp_cursor::check_cursor_return_type_count() {
  if (!m_server_side_cursor) return false;
  if (m_result.get_return_table()) {
    if (m_server_side_cursor->get_field_count() !=
        m_result.get_return_table_count()) {
      m_server_side_cursor->close();
      m_server_side_cursor = nullptr;
      my_error(ER_WRONG_NUMBER_OF_COLUMNS_IN_SELECT, MYF(0));
      return true;
    }
  }
  return false;
}

static bool change_list_field_name(List<Create_field> *list,
                                   List<Create_field> *list_open) {
  if (list == list_open) return false;
  if (list->size() != list_open->size()) {
    my_error(ER_WRONG_NUMBER_OF_COLUMNS_IN_SELECT, MYF(0));
    return true;
  }
  List_iterator_fast<Create_field> iter(*list);
  List_iterator_fast<Create_field> iter_open(*list_open);
  Create_field *def = nullptr;
  Create_field *def_open = nullptr;

  while ((def = iter++)) {
    def_open = iter_open++;
    if (!def_open) return true;
    def->field_name = def_open->field_name;
  }
  return false;
}

/**
  for static cursor,it needs to create a result table to check.
  Use the list_open's column name instead of list's column name.

  @param thd       THD
  @param list      the structure of cursor return %rowtype
*/
bool sp_cursor::make_return_table(THD *thd, List<Create_field> *list,
                                  List<Create_field> *list_c) {
  if (change_list_field_name(list, list_c)) return true;

  return m_result.m_return_table->make_return_table(thd, list);
}

// for static cursor or ref cursor,it's result table is field's return table.
bool sp_cursor::set_return_table_from_cursor(sp_cursor *from_cursor,
                                             List<Create_field> *list_c) {
  List<Create_field> list;
  if (from_cursor->m_result.m_return_table->get_table()->export_structure(
          current_thd, &list))
    return true;
  return make_return_table(current_thd, &list, list_c);
}

// for type is ref cursor,it's result table is field's return table.
bool sp_cursor::set_return_table_from_pcursor(THD *thd,
                                              LEX_CSTRING *from_cursor) {
  sp_variable *spv_define = thd->sp_runtime_ctx->search_variable(
      from_cursor->str, from_cursor->length);
  Item *item = thd->sp_runtime_ctx->get_item(spv_define->offset);
  Item_field_refcursor *item_field = dynamic_cast<Item_field_refcursor *>(item);
  m_result.m_return_table = item_field->get_refcursor_field()->m_return_table;
  return false;
}
///////////////////////////////////////////////////////////////////////////
// sp_cursor::Query_fetch_into_spvars implementation.
///////////////////////////////////////////////////////////////////////////

bool sp_cursor::Query_fetch_into_spvars::prepare(
    THD *thd, const mem_root_deque<Item *> &fields, Query_expression *u) {
  /*
    Cache the number of columns in the result set in order to easily
    return an error if column count does not match value count.
  */
  field_count = CountVisibleFields(fields);
  return Query_result_interceptor::prepare(thd, fields, u);
}

bool sp_cursor::Query_fetch_into_spvars::send_data(
    THD *thd, const mem_root_deque<Item *> &items) {
  assert(items.size() == CountVisibleFields(items));
  mem_root_deque<Item *> items_result(thd->mem_root);
  // check cursor return type
  if (get_return_table()) {
    if (m_return_table->check_return_data(thd, items) ||
        get_return_table()->fill_item_list(&items_result))
      return true;
  } else
    items_result = items;
  return send_data_after_check(thd, items_result);
}

bool sp_cursor::Query_fetch_into_spvars::send_data_after_check(
    THD *thd, const mem_root_deque<Item *> &items) {
  List_iterator_fast<sp_variable> spvar_iter(*spvar_list);
  auto item_iter = VisibleFields(items).begin();
  sp_variable *spvar;

  assert(items.size() == CountVisibleFields(items));

  /*
    Assign the row fetched from a server side cursor to stored
    procedure variables.
  */
  Item *item_tmp = nullptr;

  if (spvar_list->elements == 1 &&
      (item_tmp = thd->sp_runtime_ctx->get_item(spvar_list->head()->offset)) &&
      item_tmp->data_type() == MYSQL_TYPE_NULL &&
      ((item_tmp->result_type_table() &&
        item_tmp->cols() - 1 == items.size()) ||
       (!item_tmp->result_type_table() && item_tmp->cols() == items.size()))) {
    if (item_tmp->result_type() == ROW_RESULT && !item_tmp->is_ora_table() &&
        !item_tmp->result_type_table()) {
      if (thd->sp_runtime_ctx->set_variable_row(thd, spvar_list->head()->offset,
                                                items))
        return true;
    } else if (item_tmp->result_type_table()) {  // select bulk collect into
      uint count = fetch_bulk_count % m_row_batch;
      if (count >= thd->variables.select_bulk_into_batch) {
        my_error(ER_WRONG_BATCH_FOR_BULK_INTO, MYF(0));
        return true;
      }
      if (thd->sp_runtime_ctx->set_variable_row_table(
              thd, spvar_list->head()->offset, items, count + 1))
        return true;
      fetch_bulk_count++;
    }
  } else {
    assert(spvar_list->size() == items.size());
    /*
      Assign the row fetched from a server side cursor to stored
      procedure variables.
    */
    while ((spvar = spvar_iter++)) {
      item_tmp = *item_iter++;
      if (thd->sp_runtime_ctx->set_variable(thd, spvar->offset, &item_tmp))
        return true;
    }
  }
  return false;
}

///////////////////////////////////////////////////////////////////////////
// sp_refcursor implementation.
///////////////////////////////////////////////////////////////////////////
bool sp_refcursor::open_for_sql(THD *thd) {
  Prepared_stmt_arena_holder ps_arena_holder(thd);
  thd->stmt_arena = &m_arena;
  bool rc = mysql_open_cursor(thd, &m_result, &m_server_side_cursor);
  // thd->lex->result must free in this class,not free after this lex.
  m_query_result = thd->lex->result;
  thd->lex->result = nullptr;
  if (rc && m_server_side_cursor != nullptr) {
    m_server_side_cursor->close();
    m_server_side_cursor = nullptr;
  }
  if (!rc && check_cursor_return_type_count()) {
    destroy();
    rc = true;
  }
  return rc;
}

bool sp_refcursor::open_for_ident(THD *thd, uint m_sql_spv_idx) {
  Item *item_query = thd->sp_runtime_ctx->get_item(m_sql_spv_idx);
  String tmp;
  String *result = item_query->val_str(&tmp);
  if (item_query->null_value) {
    my_error(ER_EMPTY_QUERY, MYF(0));
    return true;
  }
  /*
    In a stored function thd->locked_tables_mode == LTM_PRELOCKED,
    it won't open table in open_table().So set thd->locked_tables_mode =
    LTM_NONE first to avoid the error.
  */
  uint in_sub_stmt = thd->in_sub_stmt;
  thd->in_sub_stmt = 0;

  Diagnostics_area tmp_da(false);
  Open_tables_backup open_tables_state_backup;
  Query_tables_list table_list_backup;
  thd->reset_n_backup_open_tables_state(&open_tables_state_backup, 0);
  thd->lex->reset_n_backup_query_tables_list(&table_list_backup);
  thd->temporary_tables = open_tables_state_backup.temporary_tables;

  auto backup_guard2 = create_scope_guard([&]() {
    thd->use_in_dyn_sql = false;
    thd->in_sub_stmt = in_sub_stmt;
    open_tables_state_backup.temporary_tables = thd->temporary_tables;
    thd->temporary_tables = nullptr;
    thd->lex->restore_backup_query_tables_list(&table_list_backup);
    thd->restore_backup_open_tables_state(&open_tables_state_backup);
  });

  bool rc = false;

  stmt = new (m_arena.mem_root) Prepared_statement(thd);
  m_query_result = new_cursor_result(stmt->m_arena.mem_root, &m_result);
  if (m_query_result == nullptr) {
    return true;
  }
  stmt->set_sql_prepare();
  stmt->set_state_for_prepare(thd);
  stmt->set_query_result(m_query_result);

  rc = stmt->prepare(thd, result->c_ptr_safe(), result->length(), nullptr);
  if (rc) return true;
  if (stmt->m_lex->sql_command != SQLCOM_SELECT) {
    my_error(ER_SP_BAD_CURSOR_QUERY, MYF(0));
    return true;
  }
  if (stmt->m_lex->result) {
    my_error(ER_SP_BAD_CURSOR_SELECT, MYF(0));
    return true;
  }

  /*It must set back the in_sub_stmt for not do close_thread_table in
   * sp_lex_instr::reset_lex_and_exec_core.*/
  thd->in_sub_stmt = in_sub_stmt;
  thd->use_in_dyn_sql = true;
  thd->push_diagnostics_area(&tmp_da);
  rc = stmt->execute_loop(thd, result, true);

  thd->merge_backup_open_tables_state(&open_tables_state_backup);

  stmt->m_lex->lock_tables_state = Query_tables_list::LTS_NOT_LOCKED;
  thd->pop_diagnostics_area();
  if (tmp_da.is_error()) {
    // Copy the exception condition information.
    thd->get_stmt_da()->set_error_status(tmp_da.mysql_errno(),
                                         tmp_da.message_text(),
                                         tmp_da.returned_sqlstate());
  }

  if (!rc) {
    m_server_side_cursor = m_query_result->cursor();
    if (check_cursor_return_type_count()) {
      destroy();
      rc = true;
    }
  }
  return rc;
}

void sp_refcursor::destroy() {
  if (is_open()) {
    if (m_server_side_cursor->is_open()) m_server_side_cursor->close();
  }
  if (stmt)
    ::destroy(stmt);
  else if (m_query_result)
    ::destroy(m_query_result);
  stmt = nullptr;
  m_query_result = nullptr;
  m_server_side_cursor = nullptr;
  m_arena.free_items();
  m_mem_root.ClearForReuse();
}

Item *make_item_index(THD *thd, Item *item_index) {
  StringBuffer<STRING_BUFFER_USUAL_SIZE> buf;
  String *res = item_index->val_str(&buf);
  char *str = thd->strmake(res->c_ptr_safe(), res->length());
  Item *item_str_index =
      new Item_string(str, res->length(), item_index->charset_for_protocol());
  item_str_index->m_is_udt_table_index = true;
  return item_str_index;
}
