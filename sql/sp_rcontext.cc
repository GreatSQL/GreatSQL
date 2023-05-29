/* Copyright (c) 2002, 2022, Oracle and/or its affiliates. All rights reserved.
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

#include "sql/sp_rcontext.h"

#include <atomic>
#include <new>

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
      m_ccount(0),
      is_udt_resolved(false) {}

sp_rcontext::~sp_rcontext() {
  if (m_var_table) {
    free_blobs(m_var_table);
    if (has_been_destroyed == 0) {
      destroy_record_table();
      has_been_destroyed++;
    }
    destroy(m_var_table);
  }

  delete_container_pointers(m_activated_handlers);
  delete_container_pointers(m_visible_handlers);
  assert(m_ccount == 0);

  // Leave m_var_items and m_case_expr_holders untouched.
  // They are allocated in mem roots and will be freed accordingly.
}

void sp_rcontext::destroy_record_table() {
  uint num_vars = m_root_parsing_ctx->max_var_index();
  for (uint i = 0; i < num_vars; i++) {
    Item_field *item_field = dynamic_cast<Item_field *>(m_var_items[i]);
    if (item_field) item_field->field->clear_table_all_field();
  }
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
  Field *row = m_var_table->field[var_idx];
  return vtable->sp_find_field_by_name_or_error(
      field_idx, to_lex_cstring(row->field_name),
      to_lex_cstring(field_name.ptr()));
}

void sp_rcontext::record_field_expand_table(THD *, Field *field, int row_number,
                                            Item *) {
  if (field->virtual_record_count_map_key_count(row_number) == 0) {
    if (field->is_offset_table_fixed()) {
      field->set_row_count_map(row_number);
    } else {
      // If it's first time to create row,must modify the key to real number.
      field->set_row_count_with_fix(row_number);
    }
  }
}

bool sp_rcontext::set_variable_row_field(THD *thd, uint var_idx, uint field_idx,
                                         Item **value) {
  DBUG_ENTER("sp_rcontext::set_variable_row_field");
  assert(value);
  TABLE *vtable = virtual_tmp_table_for_row(var_idx);
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
  LEX_STRING def_name_before{nullptr, 0};
  Field *field = m_var_table->field[var_idx];
  uint m_field_idx = 0;
  int irow_number = -1;
  int offset_table = -1;
  Item_field_row_table *item_table = nullptr;
  uint table_field_idx = 0;
  Create_field *cdf_before = nullptr, *cdf_before_last;
  Item *item = get_item(var_idx);
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
      continue;
    }

    if (offset > 0) {
      // It must find the offset of def_before by name.Now it's def_before.def
      spv = m_pcont->find_variable(def_name_before.str, def_name_before.length,
                                   false);
      if (!spv) {
        my_error(ER_SP_UNDECLARED_VAR, MYF(0), def_name_before.str);
        return true;
      }
      if (def_before->number == nullptr) {  // a.b
        if (!spv->field_def.row_field_definitions()) {
          my_error(ER_SP_MISMATCH_RECORD_VAR, MYF(0), def_before->ident.str);
          return true;
        }
        cdf_before =
            spv->field_def.row_field_definitions()->find_row_field_by_name(
                to_lex_cstring(def->ident.str), &m_field_idx);
        if (!cdf_before) {
          my_error(ER_ROW_VARIABLE_DOES_NOT_HAVE_FIELD, MYF(0),
                   def_before->ident.str, def->ident.str);
          return true;
        }
        item = item->element_index(m_field_idx);
        Item_field *item_field = dynamic_cast<Item_field *>(item);
        field = item_field->field;
      } else {  // a(1).b
        if (!spv->field_def.row_field_table_definitions()) {
          my_error(ER_SP_MISMATCH_RECORD_TABLE_VAR, MYF(0),
                   def_before->ident.str);
          return true;
        }
        if (!def_before->number->fixed) {
          if (def_before->number->fix_fields(thd, &def_before->number)) {
            return true;
          }
        }
        if (!def_before->number->this_item()->is_null() &&
            def_before->number->this_item()->result_type() != ROW_RESULT &&
            def_before->number->this_item()->result_type() != STRING_RESULT) {
          irow_number = def_before->number->val_int();
        } else {
          my_error(ER_WRONG_PARAMCOUNT_TO_TYPE_TABLE, MYF(0),
                   def_before->number->this_item()->item_name.ptr());
          return true;
        }
        record_field_expand_table(thd, field, irow_number, item);
        cdf_before = spv->field_def.row_field_table_definitions()
                         ->find_row_field_by_name(
                             to_lex_cstring(def->ident.str), &m_field_idx);
        if (!cdf_before) {
          my_error(ER_ROW_VARIABLE_DOES_NOT_HAVE_FIELD, MYF(0),
                   def_before->ident.str, def->ident.str);
          return true;
        }
        if (field->virtual_record_count_map_key_count(irow_number) > 0)
          offset_table = field->virtual_record_count_map(irow_number);
        else {
          my_error(ER_SP_NOT_EXIST_OF_RECORD_TABLE, MYF(0),
                   def_name_before.str);
          return true;
        }
        item_table = dynamic_cast<Item_field_row_table *>(item);
        Item *item_tmp =
            item_table->element_row_index(offset_table, m_field_idx);
        if (!item_tmp) {
          if (!(item = insert_into_table_default_and_set_default(
                    thd, item, offset_table, m_field_idx)))
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
    def_before = def;
  }  // end of for
  spv = m_pcont->find_variable(def_name_before.str, def_name_before.length,
                               false);
  if (!spv) {
    spv = search_udt_variable(def_name_before.str, def_name_before.length);
    if (!spv) {
      my_error(ER_SP_UNDECLARED_VAR, MYF(0), def_name_before.str);
      return true;
    }
  }
  if (def_before->number == nullptr) {  // a.b
    if (!spv->field_def.row_field_definitions()) {
      my_error(ER_SP_MISMATCH_RECORD_VAR, MYF(0), def_before->ident.str);
      return true;
    }
    cdf_before_last =
        spv->field_def.row_field_definitions()->find_row_field_by_name(
            to_lex_cstring(field_name.ptr()), &m_field_idx);
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
                                            offset_table, udt_field);
    } else {
      field = field->virtual_tmp_table_addr()[0]->field[m_field_idx];
      return sp_eval_expr(thd, field, value);
    }
  } else {  // a(1).b
    if (!spv->field_def.row_field_table_definitions()) {
      my_error(ER_SP_MISMATCH_RECORD_TABLE_VAR, MYF(0), def_before->ident.str);
      return true;
    }
    if (!def_before->number->fixed) {
      if (def_before->number->fix_fields(thd, &def_before->number)) {
        return true;
      }
    }
    if (!def_before->number->this_item()->is_null() &&
        def_before->number->this_item()->result_type() != ROW_RESULT &&
        def_before->number->this_item()->result_type() != STRING_RESULT) {
      irow_number = def_before->number->val_int();
    } else {
      my_error(ER_WRONG_PARAMCOUNT_TO_TYPE_TABLE, MYF(0),
               def_before->number->this_item()->item_name.ptr());
      return true;
    }
    record_field_expand_table(thd, field, irow_number, item);
    cdf_before_last =
        spv->field_def.row_field_table_definitions()->find_row_field_by_name(
            to_lex_cstring(field_name.ptr()), &m_field_idx);
    if (!cdf_before_last) {
      my_error(ER_ROW_VARIABLE_DOES_NOT_HAVE_FIELD, MYF(0),
               def_before->ident.str, field_name.ptr());
      return true;
    }
    if (field->virtual_record_count_map_key_count(irow_number) > 0)
      offset_table = field->virtual_record_count_map(irow_number);
    else {
      my_error(ER_SP_NOT_EXIST_OF_RECORD_TABLE, MYF(0), def_name_before.str);
      return true;
    }
    item_table = dynamic_cast<Item_field_row_table *>(item);
    Item *item_tmp = item_table->element_row_index(offset_table, m_field_idx);
    if (!item_tmp)
      if (!(item = insert_into_table_default_and_set_default(
                thd, item, offset_table, m_field_idx)))
        return true;
    return item_table->update_table_value(thd, value, m_field_idx,
                                          offset_table);
  }
}

Item *sp_rcontext::insert_into_table_default_and_set_default(THD *thd,
                                                             Item *item,
                                                             int offset_table,
                                                             uint m_field_idx) {
  // For example:v_t_rec3.v_t_rec2(0).v_t_rec1(0).col1,there is no data
  // in v_t_rec3.v_t_rec2(0),so it must insert new row of all null value.
  Item_field_row_table *item_table = dynamic_cast<Item_field_row_table *>(item);
  if (!item_table) return nullptr;
  Field_row_table *field_table =
      dynamic_cast<Field_row_table *>(item_table->field);
  /*case 1:
      type stu_record is record(
      id int := 1,
      name_d  t_air_table,
      score float := 0 }
      type dr_type is table of stu_record index by binary_integer;
      dr_table dr_type; dr_table->get_udt_name() = dr_type,
      dr_table->get_nested_table_udt() = stu_record,
      If name_d is not inited,it gets error.
    case 2:
      type dr_type is table of tt_air%ROWTYPE index by binary_integer;
      dr_table dr_type; dr_table->get_udt_name() = dr_type,
      dr_table->get_nested_table_udt() = dr_type,
      so search_udt_variable = nullptr,and it will go through.
  */
  if (field_table->get_nested_table_udt().str &&
      search_udt_variable(field_table->get_nested_table_udt().str,
                          field_table->get_nested_table_udt().length) &&
      !field_table->udt_value_initialized()) {
    my_error(ER_UDT_REF_UNINIT_COMPOSITE, MYF(0));
    return nullptr;
  }
  /*type stu_record is record(
      id int := 1,
      name_d  t_air_table,
      score float := 0
    );
    type stu_record1 is record(
      ii int := 1,
      stu_record_val1 stu_record:= stu_record(10,t_air_table(t_air(3,'cc')),1.1)
    ); stu_record_val.stu_record_val1.name_d(2) doesn't init,
  */
  if (search_udt_variable(field_table->udt_name().str,
                          field_table->udt_name().length)) {
    my_error(ER_UDT_REF_UNINIT_COMPOSITE, MYF(0));
    return nullptr;
  }

  if (item_table->create_item_for_store_default_value_and_insert_into_table(
          thd, this))
    return nullptr;

  Item *item_result = item_table->element_row_index(offset_table, m_field_idx);
  return item_result;
}

bool sp_rcontext::set_variable_row(THD *thd, uint var_idx,
                                   const mem_root_deque<Item *> &items) {
  TABLE *vtable = virtual_tmp_table_for_row(var_idx);

  auto item_iter = VisibleFields(items).begin();
  int count = 0;
  int size = items.size();
  while (count < size) {
    Item *item = *item_iter++;
    if (vtable->field[count]->sp_prepare_and_store_item(thd, &item)) {
      return true;
    }
    count++;
  }
  return false;
}

bool sp_rcontext::set_variable_row_table(THD *thd, uint var_idx,
                                         const mem_root_deque<Item *> &items,
                                         ha_rows row_count) {
  Item *item_var = get_item(var_idx);
  Field *field_first = m_var_table->field[var_idx];
  Item_field_row_table *item_table =
      dynamic_cast<Item_field_row_table *>(item_var);
  record_field_expand_table(thd, field_first, row_count, item_var);
  return item_table->insert_into_table_all_value(thd, items);
}

void sp_rcontext::cleanup_record_variable_row_table(uint var_idx) {
  Field *field_first = m_var_table->field[var_idx];
  Item *item = get_item(var_idx);
  field_first->clear_args_map();
  item->clear_args_table();
}

TABLE **sp_rcontext::virtual_tmp_table_for_row_table(uint var_idx) {
  assert(get_item(var_idx)->type() == Item::ROW_ITEM);
  assert(get_item(var_idx)->result_type_table());
  Field *field = m_var_table->field[var_idx];
  TABLE **ptable = field->virtual_tmp_table_addr();
  assert(ptable);
  return ptable;
}

TABLE *sp_rcontext::virtual_tmp_table_for_row(uint var_idx) {
  assert(get_item(var_idx)->type() == Item::ROW_ITEM);
  assert(get_item(var_idx)->result_type() == ROW_RESULT);
  Field *field = m_var_table->field[var_idx];
  TABLE **ptable = field->virtual_tmp_table_addr();
  assert(ptable[0]);
  return ptable[0];
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
  m_root_parsing_ctx->retrieve_field_definitions(&field_def_lst, &m_vars_list);
  if (!is_udt_resolved) {
    m_root_parsing_ctx->retrieve_udt_field_definitions(thd, &m_udt_vars_list);
    is_udt_resolved = true;
  }

  if (field_def_lst.elements != m_root_parsing_ctx->max_var_index())
    return true;

  if (m_udt_vars_list.elements != m_root_parsing_ctx->max_udt_var_index())
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
    if (vars->field_def.udt_db_name) {
      m_var_table->field[idx]->set_udt_db_name(
          const_cast<char *>(vars->field_def.udt_db_name));
    }
    if (vars->field_def.is_record_define ||
        vars->field_def.is_record_table_define ||
        // rec3 rec%type;rec3 must set initialize.
        !search_udt_variable(vars->field_def.udt_name.str,
                             vars->field_def.udt_name.length)) {
      m_var_table->field[idx]->set_udt_value_initialized();
      m_var_table->field[idx]->set_notnull();
    }
    if (vars->field_def.is_cursor_rowtype_ref()) {
      Item_field_row *item =
          new (thd->mem_root) Item_field_row(m_var_table->field[idx]);
      if (!(m_var_items[idx] = item)) return true;
    } else if (vars->field_def.is_table_rowtype_ref()) {
      List<Create_field> defs;
      Item_field_row *item =
          new (thd->mem_root) Item_field_row(m_var_table->field[idx]);
      if (!(m_var_items[idx] = item) ||
          vars->field_def.table_rowtype_ref()->resolve_table_rowtype_ref(
              thd, defs) ||
          add_udt_to_sp_var_list(thd, defs) ||
          item->row_create_items(thd, &defs))
        return true;
    } else if (vars->field_def.is_row()) {
      Item_field_row *item =
          new (thd->mem_root) Item_field_row(m_var_table->field[idx]);
      if (!(m_var_items[idx] = item) ||
          item->row_create_items(thd, vars->field_def.row_field_definitions()))
        return true;
    } else if (vars->field_def.is_row_table()) {
      Field_row_table *row_field =
          dynamic_cast<Field_row_table *>(m_var_table->field[idx]);
      if (vars->field_def.nested_table_udt.str) {
        row_field->set_nested_table_udt(vars->field_def.nested_table_udt);
      }
      row_field->m_varray_size_limit =
          vars->field_def.table_type == 0 /*VARRAY*/ ? vars->field_def
                                                           .varray_limit
                                                     : -1;
      Item_field_row_table *item =
          new (thd->mem_root) Item_field_row_table(m_var_table->field[idx]);
      // for type dr_type is table of stu_record,it doesn't have
      // table_rowtype_ref.
      if (vars->field_def.is_record_table_ref) {
        List<Create_field> defs;
        if (!(m_var_items[idx] = item) ||
            vars->field_def.table_rowtype_ref()->resolve_table_rowtype_ref(
                thd, defs) ||
            add_udt_to_sp_var_list(thd, defs)) {
          return true;
        } else {
          Row_definition_list *rdl = nullptr;
          List_iterator_fast<Create_field> it(defs);
          Create_field *def;
          for (uint i = 0; (def = it++); i++) {
            if (i == 0)
              rdl = Row_definition_list::make(thd->mem_root, def);
            else
              rdl->append_uniq(thd->mem_root, def);
          }
          Row_definition_table_list *row_definition_table_list =
              Row_definition_table_list::make(thd->mem_root, rdl);
          vars->field_def.set_row_field_table_definitions(
              row_definition_table_list);
          item->set_item_table_name(vars->name.str);
          if (item->row_create_items(thd, row_definition_table_list))
            return true;
        }
      } else {
        m_var_items[idx] = item;
        item->set_item_table_name(vars->name.str);
        if (item->row_create_items(
                thd, vars->field_def.row_field_table_definitions()))
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
      sp_variable *spvar_udt = m_root_parsing_ctx->find_udt_variable(
          cdf->udt_name.str, cdf->udt_name.length, false);
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

sp_variable *sp_rcontext::search_udt_variable(const char *name,
                                              size_t name_len) {
  List_iterator_fast<sp_variable> it(m_udt_vars_list);
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

bool sp_rcontext::push_cursor(sp_instr_cpush *i) {
  /*
    We should create cursors on the system heap because:
     - they could be (and usually are) used in several instructions,
       thus they can not be stored on an execution mem-root;
     - a cursor can be pushed/popped many times in a loop, having these objects
       on callers' mem-root would lead to a memory leak in every iteration.
  */
  sp_cursor *c = new (std::nothrow) sp_cursor(i);

  if (!c) {
    sql_alloc_error_handler();
    return true;
  }

  m_cstack[m_ccount++] = c;
  return false;
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

void sp_rcontext::pop_handlers(sp_pcontext *current_scope) {
  for (int i = static_cast<int>(m_visible_handlers.size()) - 1; i >= 0; --i) {
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

  Query_block *sl = thd->lex->query_block;
  if (sl && sl->rownum_func && sl->need_to_reset_flag) {
    sl->reset_rownum_read_flag();
  }

  // If execution failed, ensure that the cursor is closed.
  if (rc && m_server_side_cursor != nullptr) {
    m_server_side_cursor->close();
    m_server_side_cursor = nullptr;
  }
  return rc;
}

bool sp_cursor::close() {
  if (m_server_side_cursor == nullptr) {
    my_error(ER_SP_CURSOR_NOT_OPEN, MYF(0));
    return true;
  }

  m_server_side_cursor->close();
  m_server_side_cursor = nullptr;
  return false;
}

void sp_cursor::destroy() { assert(m_server_side_cursor == nullptr); }

bool sp_cursor::fetch(List<sp_variable> *vars) {
  if (m_server_side_cursor == nullptr) {
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
      m_found = false;
      return false;
    }

    m_found = true;
    m_row_count++;
  }

  return false;
}

bool sp_cursor::fetch_bulk(List<sp_variable> *vars, ha_rows row_count) {
  if (m_server_side_cursor == nullptr) {
    my_error(ER_SP_CURSOR_NOT_OPEN, MYF(0));
    return true;
  }

  if (vars->elements != m_result.get_field_count() &&
      (vars->elements != 1 ||
       m_result.get_field_count() !=
           current_thd->sp_runtime_ctx->get_item(vars->head()->offset)
               ->cols())) {
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

  /* Attempt to fetch one row */
  if (m_server_side_cursor->is_open()) {
    if (m_server_side_cursor->fetch(row_count)) return true;
  }

  if (!m_server_side_cursor->is_open()) {
    m_found = false;
    return false;
  }

  m_found = true;
  m_row_count++;

  return false;
}

bool sp_cursor::export_structure(THD *thd, List<Create_field> *list) {
  bool rc = m_server_side_cursor->export_structure(thd, list);
  return rc;
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
  List_iterator_fast<sp_variable> spvar_iter(*spvar_list);
  auto item_iter = VisibleFields(items).begin();
  sp_variable *spvar;

  assert(items.size() == CountVisibleFields(items));
  assert(spvar_list->size() == items.size());

  /*
    Assign the row fetched from a server side cursor to stored
    procedure variables.
  */
  while ((spvar = spvar_iter++)) {
    Item *item = *item_iter++;
    if (thd->sp_runtime_ctx->set_variable(thd, spvar->offset, &item))
      return true;
  }
  return false;
}
