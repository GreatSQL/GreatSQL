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

#include "sql/sp_pcontext.h"

#include <assert.h>
#include "m_ctype.h"
#include "m_string.h"
#include "my_alloc.h"

#include "my_inttypes.h"
#include "sp_instr.h"
#include "sql/sp.h"
#include "sql/sql_class.h"
#include "sql/sql_table.h"
#include "sql_string.h"

bool sp_condition_value::equals(const sp_condition_value *cv) const {
  assert(cv);

  if (this == cv) return true;

  if (type != cv->type) return false;

  switch (type) {
    case sp_condition_value::ERROR_CODE:
      return (mysqlerr == cv->mysqlerr);

    case sp_condition_value::SQLSTATE:
      if (strcmp(sql_state, cv->sql_state) == 0) {
        if (m_is_user_defined) return this == cv;
        return true;
      }
      return false;
    default:
      return true;
  }
}

void sp_condition_value::print(String *str) const {
  switch (type) {
    case sp_condition_value::ERROR_CODE:
      str->append(STRING_WITH_LEN(" "));
      str->append_ulonglong(static_cast<ulonglong>(mysqlerr));
      break;
    case sp_condition_value::SQLSTATE:
      if (m_is_user_defined) {
        str->append(STRING_WITH_LEN(" USER DEFINED EXCEPTION"));
      } else {
        str->append(STRING_WITH_LEN(" SQLSTATE '"));
        str->append(static_cast<const char *>(sql_state), 5);
        str->append(STRING_WITH_LEN("'"));
      }
      break;
    case sp_condition_value::WARNING:
      str->append(STRING_WITH_LEN(" SQLWARNING"));
      break;
    case sp_condition_value::NOT_FOUND:
      str->append(STRING_WITH_LEN(" NOT FOUND"));
      break;
    case sp_condition_value::EXCEPTION:
      str->append(STRING_WITH_LEN(" SQLEXCEPTION"));
      break;
    default:
      break;
  }
}

const char *sp_oracle_condition_value::UER_DEFINED_EXCEPTION_SQLSTATE = {
    "45000"};

void sp_oracle_condition_value::set_redirect_err_to_user_defined(
    uint _mysqlerr) {
  assert(m_is_user_defined);
  mysqlerr = _mysqlerr;
  memcpy(sql_state, mysql_errno_to_sqlstate(_mysqlerr), SQLSTATE_LENGTH);
  sql_state[SQLSTATE_LENGTH] = 0;
  type = ERROR_CODE;
}

void sp_handler::print_conditions(String *str) const {
  bool first = true;
  for (const sp_condition_value &cv : condition_values) {
    if (first) {
      first = false;
      str->append(STRING_WITH_LEN(" HANDLER FOR"));
    } else
      str->append(STRING_WITH_LEN(","));

    cv.print(str);
  }
}

void sp_handler::print(String *str) const {
  switch (type) {
    case sp_handler::EXIT:
      str->append(STRING_WITH_LEN(" EXIT"));
      break;
    case sp_handler::CONTINUE:
      str->append(STRING_WITH_LEN(" CONTINUE"));
      break;
    default:
      // The handler type must be either CONTINUE or EXIT.
      assert(0);
  }

  print_conditions(str);
}

void sp_pcontext::init(uint var_offset, uint cursor_offset, uint udt_var_offset,
                       int num_case_expressions) {
  m_var_offset = var_offset;
  m_cursor_offset = cursor_offset;
  m_udt_var_offset = udt_var_offset;
  m_num_case_exprs = num_case_expressions;

  m_labels.clear();
}

sp_pcontext::sp_pcontext(THD *thd)
    : m_level(0),
      m_max_var_index(0),
      m_max_cursor_index(0),
      m_max_udt_var_index(0),
      m_parent(nullptr),
      m_pboundary(0),
      m_vars(thd->mem_root),
      m_udt_vars(thd->mem_root),
      m_case_expr_ids(thd->mem_root),
      m_conditions(thd->mem_root),
      m_cursors(thd->mem_root),
      m_cursor_vars(thd->mem_root),
      m_handlers(thd->mem_root),
      m_children(thd->mem_root),
      m_scope(REGULAR_SCOPE) {
  init(0, 0, 0, 0);
}

sp_pcontext::sp_pcontext(THD *thd, sp_pcontext *prev,
                         sp_pcontext::enum_scope scope)
    : m_level(prev->m_level + 1),
      m_max_var_index(0),
      m_max_cursor_index(0),
      m_max_udt_var_index(0),
      m_parent(prev),
      m_pboundary(0),
      m_vars(thd->mem_root),
      m_udt_vars(thd->mem_root),
      m_case_expr_ids(thd->mem_root),
      m_conditions(thd->mem_root),
      m_cursors(thd->mem_root),
      m_cursor_vars(thd->mem_root),
      m_handlers(thd->mem_root),
      m_children(thd->mem_root),
      m_scope(scope) {
  init(prev->m_var_offset + prev->m_max_var_index, prev->current_cursor_count(),
       prev->m_udt_var_offset + prev->m_max_udt_var_index,
       prev->get_num_case_exprs());
}

sp_pcontext::~sp_pcontext() {
  for (size_t i = 0; i < m_children.size(); ++i) destroy(m_children.at(i));
}

sp_pcontext *sp_pcontext::push_context(THD *thd,
                                       sp_pcontext::enum_scope scope) {
  sp_pcontext *child = new (thd->mem_root) sp_pcontext(thd, this, scope);

  if (child) m_children.push_back(child);
  return child;
}

sp_pcontext *sp_pcontext::pop_context() {
  m_parent->m_max_var_index += m_max_var_index;
  m_parent->m_max_udt_var_index += m_max_udt_var_index;

  uint submax = max_cursor_index();
  if (submax > m_parent->m_max_cursor_index)
    m_parent->m_max_cursor_index = submax;

  if (m_num_case_exprs > m_parent->m_num_case_exprs)
    m_parent->m_num_case_exprs = m_num_case_exprs;

  return m_parent;
}

size_t sp_pcontext::diff_handlers(const sp_pcontext *ctx,
                                  bool exclusive) const {
  size_t n = 0;
  const sp_pcontext *pctx = this;
  const sp_pcontext *last_ctx = nullptr;

  while (pctx && pctx != ctx) {
    n += pctx->m_handlers.size();
    last_ctx = pctx;
    pctx = pctx->parent_context();
  }
  if (pctx)
    return (exclusive && last_ctx ? n - last_ctx->m_handlers.size() : n);
  return 0;  // Didn't find ctx
}

size_t sp_pcontext::diff_cursors(const sp_pcontext *ctx, bool exclusive) const {
  size_t n = 0;
  const sp_pcontext *pctx = this;
  const sp_pcontext *last_ctx = nullptr;

  while (pctx && pctx != ctx) {
    n += pctx->m_cursors.size();
    last_ctx = pctx;
    pctx = pctx->parent_context();
  }
  if (pctx) return (exclusive && last_ctx ? n - last_ctx->m_cursors.size() : n);
  return 0;  // Didn't find ctx
}

sp_variable *sp_pcontext::find_variable(const char *name, size_t name_len,
                                        bool current_scope_only) const {
  size_t i = m_vars.size() - m_pboundary;

  while (i--) {
    sp_variable *p = m_vars.at(i);

    if (my_strnncoll(system_charset_info, pointer_cast<const uchar *>(name),
                     name_len, pointer_cast<const uchar *>(p->name.str),
                     p->name.length) == 0) {
      return p;
    }
  }

  return (!current_scope_only && m_parent)
             ? m_parent->find_variable(name, name_len, false)
             : nullptr;
}

sp_variable *sp_pcontext::find_udt_variable(const char *name, size_t name_len,
                                            bool current_scope_only) const {
  size_t i = m_udt_vars.size();

  while (i--) {
    sp_variable *p = m_udt_vars.at(i);

    if (my_strnncoll(system_charset_info, pointer_cast<const uchar *>(name),
                     name_len, pointer_cast<const uchar *>(p->name.str),
                     p->name.length) == 0) {
      return p;
    }
  }

  return (!current_scope_only && m_parent)
             ? m_parent->find_variable(name, name_len, false)
             : nullptr;
}

sp_variable *sp_pcontext::add_udt_variable(THD *thd, Row_definition_list *rdl,
                                           LEX_STRING name, uint table_type,
                                           enum enum_field_types type,
                                           ulonglong varray_limit,
                                           LEX_CSTRING nested_table_udt) {
  sp_variable *spvar_udt = new (thd->mem_root) sp_variable(
      name, type, sp_variable::MODE_IN, m_udt_var_offset + m_max_udt_var_index);

  if (!spvar_udt) return nullptr;
  if (spvar_udt->field_def.init(
          thd, "", MYSQL_TYPE_NULL, nullptr, nullptr, 0, NULL, NULL, &NULL_CSTR,
          0, nullptr, thd->variables.collation_database, false, 0, nullptr,
          nullptr, nullptr, {}, dd::Column::enum_hidden_type::HT_VISIBLE)) {
    return nullptr;
  }
  if (!nested_table_udt.str) {
    if (table_type == 0 || table_type == 1) {
      spvar_udt->field_def.set_row_field_table_definitions(
          Row_definition_table_list::make(thd->mem_root, rdl));
      spvar_udt->field_def.varray_limit = varray_limit;
      spvar_udt->field_def.is_record_table_define = true;
      spvar_udt->field_def.nested_table_udt = nested_table_udt;
    } else {
      spvar_udt->field_def.set_row_field_definitions(rdl);
      spvar_udt->field_def.is_record_define = true;
    }
  } else {
    spvar_udt->field_def.set_row_field_table_definitions(
        Row_definition_table_list::make(thd->mem_root, rdl));
    spvar_udt->field_def.nested_table_udt = nested_table_udt;
    spvar_udt->field_def.varray_limit = varray_limit;
    spvar_udt->field_def.is_record_table_define = true;
    sp_variable *spvar_udt_table =
        find_udt_variable(nested_table_udt.str, nested_table_udt.length, false);
    if (!spvar_udt_table) {
      spvar_udt_table = add_udt_variable(
          thd, rdl, to_lex_string(nested_table_udt), 255, type, 0, NULL_CSTR);
      if (!spvar_udt_table) {
        my_error(ER_SP_UNDECLARED_VAR, MYF(0), nested_table_udt.str);
        return nullptr;
      }
    }
  }
  spvar_udt->field_def.table_type = table_type;
  spvar_udt->field_def.udt_name = name;
  spvar_udt->field_def.udt_db_name = thd->db().str;
  spvar_udt->field_def.field_name = name.str;
  spvar_udt->field_def.is_nullable = true;
  if (prepare_sp_create_field(thd, &spvar_udt->field_def)) {
    return nullptr;
  }
  ++m_max_udt_var_index;

  return m_udt_vars.push_back(spvar_udt) ? nullptr : spvar_udt;
}

/*
  Find a variable by its run-time offset.
  If the variable with a desired run-time offset is not found in this
  context frame, it's recursively searched on parent context frames.

  Note, context frames can have holes:
    CREATE PROCEDURE p1() AS
      x0 INT:=100;
      CURSOR cur(p0 INT, p1 INT) IS SELECT p0, p1;
      x1 INT:=101;
    BEGIN
      ...
    END;
  The variables (x0 and x1) and the cursor parameters (p0 and p1)
  reside in separate parse context frames.

  The variables reside on the top level parse context frame:
  - x0 has frame offset 0 and run-time offset 0
  - x1 has frame offset 1 and run-time offset 3

  The cursor parameters reside on the second level parse context frame:
  - p0 has frame offset 0 and run-time offset 1
  - p1 has frame offset 1 and run-time offset 2

  Run-time offsets on a frame can have holes, but offsets monotonocally grow,
  so run-time offsets of all variables are not greater than the run-time offset
  of the very last variable in this frame.
*/
sp_variable *sp_pcontext::find_variable(uint offset) const {
  if (m_var_offset <= offset && m_vars.size() &&
      offset <= get_last_context_variable()->offset) {
    for (uint i = 0; i < m_vars.size(); i++) {
      if (m_vars.at(i)->offset == offset) return m_vars.at(i);  // This frame
    }
  }

  return m_parent ? m_parent->find_variable(offset) :  // Some previous frame
             nullptr;                                  // Index out of bounds
}

sp_variable *sp_pcontext::add_variable(THD *thd, LEX_STRING name,
                                       enum enum_field_types type,
                                       sp_variable::enum_mode mode) {
  // for cursor(para0,para1)
  sp_variable *p = new (thd->mem_root)
      sp_variable(name, type, mode, m_var_offset + m_max_var_index);

  if (!p) return nullptr;

  ++m_max_var_index;

  return m_vars.push_back(p) ? nullptr : p;
}

bool sp_pcontext::copy_default_params(mem_root_deque<Item *> *args) {
  assert(args != nullptr);

  // copy all default values, even nullptr
  for (auto var : m_vars) {
    Item *d = nullptr;

    if (var->default_value != nullptr)
      // d = var->default_value;
      d = var->default_value->this_item();

    args->push_back(d);
  }

  assert(m_vars.size() == args->size());
  return false;
}

sp_label *sp_pcontext::push_label(THD *thd, LEX_CSTRING name, uint ip) {
  sp_label *label =
      new (thd->mem_root) sp_label(name, ip, sp_label::IMPLICIT, this);

  if (!label) return nullptr;

  m_labels.push_front(label);

  return label;
}

sp_label *sp_pcontext::find_label(LEX_CSTRING name) {
  List_iterator_fast<sp_label> li(m_labels);
  sp_label *lab;

  while ((lab = li++)) {
    if (my_strcasecmp(system_charset_info, name.str, lab->name.str) == 0)
      return lab;
  }

  /*
    Note about exception handlers.
    See SQL:2003 SQL/PSM (ISO/IEC 9075-4:2003),
    section 13.1 <compound statement>,
    syntax rule 4.
    In short, a DECLARE HANDLER block can not refer
    to labels from the parent context, as they are out of scope.
  */
  return (m_parent && (m_scope == REGULAR_SCOPE)) ? m_parent->find_label(name)
                                                  : nullptr;
}

sp_label *sp_pcontext::find_label_current_loop_start() {
  List_iterator_fast<sp_label> li(m_labels);
  sp_label *lab;

  while ((lab = li++)) {
    if (lab->type == sp_label::ITERATION) return lab;
  }

  // See a comment in sp_pcontext::find_label()
  return (m_parent && (m_scope == REGULAR_SCOPE))
             ? m_parent->find_label_current_loop_start()
             : nullptr;
}

bool sp_pcontext::add_condition(THD *thd, LEX_STRING name,
                                sp_condition_value *value) {
  sp_condition *p = new (thd->mem_root) sp_condition(name, value);

  if (p == nullptr) return true;

  return m_conditions.push_back(p);
}

sp_condition_value *sp_pcontext::find_condition(LEX_STRING name,
                                                bool current_scope_only) const {
  size_t i = m_conditions.size();

  while (i--) {
    sp_condition *p = m_conditions.at(i);

    if (my_strnncoll(system_charset_info, (const uchar *)name.str, name.length,
                     (const uchar *)p->name.str, p->name.length) == 0) {
      return p->value;
    }
  }

  return (!current_scope_only && m_parent)
             ? m_parent->find_condition(name, false)
             : nullptr;
}

/*
 oracle compatible Predefined Exceptions

  unspport define
    ACCESS_INTO_NULL
    CASE_NOT_FOUND
    COLLECTION_IS_NULL|-6531
    LOGIN_DENIED|-1017
    NO_DATA_NEEDED|-6548
    NOT_LOGGED_ON|-1012
    PROGRAM_ERROR|-6501
    ROWTYPE_MISMATCH|-6504
    SELF_IS_NULL|-30625
    SUBSCRIPT_BEYOND_COUNT|-6533
    SUBSCRIPT_OUTSIDE_LIMIT|-6532
    SYS_INVALID_ROWID|-1410
    TIMEOUT_ON_RESOURCE|-51

*/

#define PREDEFINE_CONDITION(str, errno)                    \
  sp_condition(                                            \
      LEX_STRING{const_cast<char *> STRING_WITH_LEN(str)}, \
      new sp_oracle_condition_value(errno, mysql_errno_to_sqlstate(errno)))

static sp_condition sp_predefined_conditions[] = {
    PREDEFINE_CONDITION("NO_DATA_FOUND", ER_SP_FETCH_NO_DATA),
    PREDEFINE_CONDITION("NO_DATA_FOUND", ER_SP_NOT_EXIST_OF_RECORD_TABLE),
    PREDEFINE_CONDITION("INVALID_CURSOR", ER_SP_CURSOR_NOT_OPEN),
    PREDEFINE_CONDITION("DUP_VAL_ON_INDEX", ER_DUP_ENTRY),
    PREDEFINE_CONDITION("DUP_VAL_ON_INDEX", ER_DUP_ENTRY_WITH_KEY_NAME),
    PREDEFINE_CONDITION("TOO_MANY_ROWS", ER_TOO_MANY_ROWS),
    PREDEFINE_CONDITION("INVALID_NUMBER", ER_DATA_OUT_OF_RANGE),
    PREDEFINE_CONDITION("CURSOR_ALREADY_OPEN", ER_SP_CURSOR_ALREADY_OPEN),
    PREDEFINE_CONDITION("VALUE_ERROR", ER_WRONG_VALUE),
    PREDEFINE_CONDITION("STORAGE_ERROR", ER_GET_ERRNO),
    PREDEFINE_CONDITION("ZERO_DIVIDE", ER_DIVISION_BY_ZERO)  // 1365
};

bool sp_pcontext::declared_or_predefined_condition(LEX_STRING name,
                                                   sp_instr *i) {
  sp_instr_hpush_jump *jump = dynamic_cast<sp_instr_hpush_jump *>(i);
  auto cond = find_condition(name, false);
  if (cond) {
    if (parent_context()->check_duplicate_handler(cond)) {
      my_error(ER_SP_DUP_HANDLER, MYF(0));
      return true;
    }
    jump->add_condition(cond);
  } else {
    bool flag = true;
    for (auto ptr : sp_predefined_conditions) {
      if (my_strnncoll(system_charset_info, (const uchar *)name.str,
                       name.length, (const uchar *)ptr.name.str,
                       ptr.name.length) == 0) {
        if (parent_context()->check_duplicate_handler(ptr.value)) {
          my_error(ER_SP_DUP_HANDLER, MYF(0));
          return true;
        }
        jump->add_condition(ptr.value);
        flag = false;
      }
    }
    if (flag) {
      my_error(ER_SP_COND_MISMATCH, MYF(0), name.str);
      return true;
    }
  }
  return false;
}

sp_condition_value *sp_pcontext::find_declared_or_predefined_condition(
    const LEX_STRING name) const {
  auto p = find_condition(name, false);
  if (p) return p;

  for (auto ptr : sp_predefined_conditions) {
    if (my_strnncoll(system_charset_info, (const uchar *)name.str, name.length,
                     (const uchar *)ptr.name.str, ptr.name.length) == 0) {
      return ptr.value;
    }
  }
  return nullptr;
}

sp_handler *sp_pcontext::add_handler(THD *thd, sp_handler::enum_type type) {
  sp_handler *h = new (thd->mem_root) sp_handler(type, this);

  if (!h) return nullptr;

  return m_handlers.push_back(h) ? nullptr : h;
}

bool sp_pcontext::check_duplicate_handler(
    const sp_condition_value *cond_value) const {
  for (size_t i = 0; i < m_handlers.size(); ++i) {
    sp_handler *h = m_handlers.at(i);

    List_iterator_fast<const sp_condition_value> li(h->condition_values);
    const sp_condition_value *cv;

    while ((cv = li++)) {
      if (cond_value->equals(cv)) return true;
    }
  }

  return false;
}

sp_handler *sp_pcontext::find_handler(
    const char *sql_state, uint sql_errno,
    Sql_condition::enum_severity_level severity,
    sp_condition_value *user_defined) const {
  sp_handler *found_handler = nullptr;
  const sp_condition_value *found_cv = nullptr;

  for (size_t i = 0; i < m_handlers.size(); ++i) {
    sp_handler *h = m_handlers.at(i);

    List_iterator_fast<const sp_condition_value> li(h->condition_values);
    const sp_condition_value *cv;

    while ((cv = li++)) {
      switch (cv->type) {
        case sp_condition_value::ERROR_CODE:
          if (sql_errno == cv->mysqlerr &&
              (!found_cv || found_cv->type > sp_condition_value::ERROR_CODE)) {
            found_cv = cv;
            found_handler = h;
          }
          break;

        case sp_condition_value::SQLSTATE:

          if (strcmp(sql_state, cv->sql_state) == 0 &&
              (!found_cv || found_cv->type > sp_condition_value::SQLSTATE)) {
            if (current_thd->variables.sql_mode & MODE_ORACLE) {
              DBUG_PRINT("info",
                         ("check handler  SQLSTATE %s,errno:%d cond: %p, cv:%p",
                          cv->sql_state, cv->mysqlerr, user_defined, cv));

              if (cv->m_is_user_defined) {
                if (user_defined != nullptr && user_defined == cv) {
                  found_cv = cv;
                  found_handler = h;
                }
              } else {
                found_cv = cv;
                found_handler = h;
              }
            } else {
              found_cv = cv;
              found_handler = h;
            }
          }
          break;

        case sp_condition_value::WARNING:
          if ((is_sqlstate_warning(sql_state) ||
               severity == Sql_condition::SL_WARNING) &&
              !found_cv) {
            found_cv = cv;
            found_handler = h;
          }
          break;

        case sp_condition_value::NOT_FOUND:
          if (is_sqlstate_not_found(sql_state) && !found_cv) {
            found_cv = cv;
            found_handler = h;
          }
          break;

        case sp_condition_value::EXCEPTION:
          if (is_sqlstate_exception(sql_state) &&
              severity == Sql_condition::SL_ERROR && !found_cv) {
            found_cv = cv;
            found_handler = h;
          }
          break;
      }
    }
  }

  if (found_handler) return found_handler;

  // There is no appropriate handler in this parsing context. We need to look up
  // in parent contexts. There might be two cases here:
  //
  // 1. The current context has REGULAR_SCOPE. That means, it's a simple
  // BEGIN..END block:
  //     ...
  //     BEGIN
  //       ... # We're here.
  //     END
  //     ...
  // In this case we simply call find_handler() on parent's context recursively.
  //
  // 2. The current context has HANDLER_SCOPE. That means, we're inside an
  // SQL-handler block:
  //   ...
  //   DECLARE ... HANDLER FOR ...
  //   BEGIN
  //     ... # We're here.
  //   END
  //   ...
  // In this case we can not just call parent's find_handler(), because
  // parent's handler don't catch conditions from this scope. Instead, we should
  // try to find first parent context (we might have nested handler
  // declarations), which has REGULAR_SCOPE (i.e. which is regular BEGIN..END
  // block).

  const sp_pcontext *p = this;

  while (p && p->m_scope == HANDLER_SCOPE) p = p->m_parent;

  if (!p || !p->m_parent) return nullptr;

  return p->m_parent->find_handler(sql_state, sql_errno, severity,
                                   user_defined);
}

bool sp_pcontext::add_cursor(LEX_STRING name) {
  if (m_cursors.size() == m_max_cursor_index) ++m_max_cursor_index;

  return m_cursors.push_back(name);
}

bool sp_pcontext::add_cursor_parameters(THD *thd, const LEX_STRING name,
                                        sp_pcontext *param_ctx) {
  if (m_cursors.size() == m_max_cursor_index) ++m_max_cursor_index;

  m_cursors.push_back(name);
  uint offset;
  if (!find_cursor(name, &offset, false)) {
    my_error(ER_SP_DUP_CURS, MYF(0), name.str);
    return true;
  }
  sp_pcursor *spcur =
      new (thd->mem_root) sp_pcursor(to_lex_cstring(name), param_ctx, offset);
  return m_cursor_vars.push_back(spcur);
}

sp_pcursor *sp_pcontext::find_cursor_parameters(uint offset) const {
  sp_pcursor *pcur;
  if (m_cursor_offset <= offset &&
      offset < m_cursor_offset + m_cursor_vars.size()) {
    pcur = m_cursor_vars.at(offset - m_cursor_offset);  // This frame

    return pcur;
  }

  return m_parent ? m_parent->find_cursor_parameters(offset)
                  :    // Some previous frame
             nullptr;  // Index out of bounds
}

bool sp_pcontext::find_cursor(LEX_STRING name, uint *poff,
                              bool current_scope_only) const {
  size_t i = m_cursors.size();

  while (i--) {
    LEX_STRING n = m_cursors.at(i);

    if (my_strnncoll(system_charset_info, (const uchar *)name.str, name.length,
                     (const uchar *)n.str, n.length) == 0) {
      *poff = m_cursor_offset + i;
      return true;
    }
  }

  return (!current_scope_only && m_parent)
             ? m_parent->find_cursor(name, poff, false)
             : false;
}

void sp_pcontext::retrieve_field_definitions(
    List<Create_field> *field_def_lst, List<sp_variable> *vars_list) const {
  /* Put local/context fields in the result list. */
  size_t next_child = 0;

  for (size_t i = 0; i < m_vars.size(); ++i) {
    sp_variable *var_def = m_vars.at(i);
    var_def->reset_ref_actual_param();
    if (var_def->field_def.column_type_ref()) {
      Qualified_column_ident *ref_back = var_def->field_def.column_type_ref();
      Create_field *def_tmp = new (current_thd->mem_root) Create_field();
      if (var_def->field_def.column_type_ref()->resolve_type_ref(current_thd,
                                                                 def_tmp))
        return;
      if (def_tmp->udt_name.str) {
        List<Create_field> def_tmp_list;
        def_tmp_list.push_back(def_tmp);
        if (resolve_udt_create_field_list(current_thd, &def_tmp_list)) return;
      }
      var_def->type = def_tmp->sql_type;
      var_def->field_def = *def_tmp;
      var_def->field_def.set_column_type_ref(ref_back);
      var_def->field_def.field_name = var_def->name.str;
    }
    /*
      The context can have holes in run-time offsets,
      the missing offsets reside on the children contexts in such cases.
      Example:
        CREATE PROCEDURE p1() AS
          x0 INT:=100;        -- context 0, position 0, run-time 0
          CURSOR cur(
            p0 INT,           -- context 1, position 0, run-time 1
            p1 INT            -- context 1, position 1, run-time 2
          ) IS SELECT p0, p1;
          x1 INT:=101;        -- context 0, position 1, run-time 3
        BEGIN
          ...
        END;
      See more comments in sp_pcontext::find_variable().
      We must retrieve the definitions in the order of their run-time offsets.
      Check that there are children that should go before the current variable.
    */
    for (; next_child < m_children.size(); next_child++) {
      sp_pcontext *child = m_children.at(next_child);
      if (!child->context_var_count() ||
          child->get_context_variable(0)->offset > var_def->offset)
        break;
      /*
        All variables on the embedded context (that fills holes of the parent)
        should have the run-time offset strictly less than var_def.
      */
      assert(child->get_context_variable(0)->offset < var_def->offset);
      assert(child->get_last_context_variable(0)->offset < var_def->offset);
      child->retrieve_field_definitions(field_def_lst, vars_list);
    }
    field_def_lst->push_back(&var_def->field_def);
    vars_list->push_back(var_def);
  }

  /* Put the fields of the enclosed contexts in the result list. */

  for (size_t i = next_child; i < m_children.size(); ++i)
    m_children.at(i)->retrieve_field_definitions(field_def_lst, vars_list);
}

void sp_pcontext::retrieve_udt_field_definitions(
    THD *thd, List<sp_variable> *vars_list) const {
  /* Put local/context fields in the result list. */
  size_t next_child = 0;

  for (size_t i = 0; i < m_udt_vars.size(); ++i) {
    sp_variable *var_def = m_udt_vars.at(i);
    for (; next_child < m_children.size(); next_child++) {
      sp_pcontext *child = m_children.at(next_child);
      if (!child->context_udt_var_count() ||
          child->get_context_udt_variable(0)->offset > var_def->offset)
        break;
      /*
        All variables on the embedded context (that fills holes of the parent)
        should have the run-time offset strictly less than var_def.
      */
      assert(child->get_context_udt_variable(0)->offset < var_def->offset);
      assert(child->get_last_context_variable(0)->offset < var_def->offset);
      child->retrieve_udt_field_definitions(thd, vars_list);
    }
    vars_list->push_back(var_def);
  }
  /* Put the fields of the enclosed contexts in the result list. */

  for (size_t i = next_child; i < m_children.size(); ++i)
    m_children.at(i)->retrieve_udt_field_definitions(thd, vars_list);
}

bool sp_pcontext::resolve_udt_create_field_list(
    THD *thd, List<Create_field> *field_def_lst) const {
  List_iterator_fast<Create_field> it_cdf(*field_def_lst);
  Create_field *cdf;
  for (uint i = 0; (cdf = it_cdf++); i++) {
    if (cdf->udt_name.str) {
      sp_variable *spvar_udt =
          find_udt_variable(cdf->udt_name.str, cdf->udt_name.length, false);
      if (!spvar_udt) {
        List<Create_field> *field_def_list = nullptr;
        ulong reclength = 0;
        sql_mode_t sql_mode;
        MDL_savepoint mdl_savepoint = thd->mdl_context.mdl_savepoint();
        LEX_CSTRING nested_table_udt = NULL_CSTR;
        uint table_type = 255;
        ulonglong varray_limit = 0;
        if (!sp_find_ora_type_create_fields(
                thd, to_lex_string(thd->db()), cdf->udt_name, false,
                &field_def_list, &reclength, &sql_mode, &nested_table_udt,
                &table_type, &varray_limit)) {
          Row_definition_list *rdl = nullptr;
          List_iterator_fast<Create_field> it(*field_def_list);
          Create_field *def_tmp_udt;
          for (uint j = 0; (def_tmp_udt = it++); j++) {
            if (j == 0)
              rdl = Row_definition_list::make(thd->mem_root, def_tmp_udt);
            else
              rdl->append_uniq(thd->mem_root, def_tmp_udt);
          }
          cdf->set_row_field_definitions(rdl);
        } else {
          thd->mdl_context.rollback_to_savepoint(mdl_savepoint);
          return true;
        }
      } else {
        cdf->set_row_field_definitions(
            spvar_udt->field_def.row_field_definitions());
      }
    }
  }
  return false;
}

const LEX_STRING *sp_pcontext::find_cursor(uint offset) const {
  if (m_cursor_offset <= offset &&
      offset < m_cursor_offset + m_cursors.size()) {
    return &m_cursors.at(offset - m_cursor_offset);  // This frame
  }

  return m_parent ? m_parent->find_cursor(offset) :  // Some previous frame
             nullptr;                                // Index out of bounds
}

Item *sp_pcontext::make_item_plsql_cursor_attr(THD *thd, LEX_CSTRING name,
                                               uint offset) {
  Item *expr = new (thd->mem_root) Item_func_cursor_found(name, offset);

  return expr;
}

Item *sp_pcontext::make_item_plsql_sp_for_loop_attr(THD *thd, int m_direction,
                                                    Item *var, Item *value_from,
                                                    Item *value_to) {
  Item *expr = m_direction > 0
                   ? (Item *)new (thd->mem_root) Item_func_le(var, value_to)
                   : (Item *)new (thd->mem_root) Item_func_ge(var, value_from);

  return expr;
}

// for i in var.first .. var.last,it means compare between i and var.last.
Item *sp_pcontext::make_item_plsql_sp_for_record_table_loop_attr(THD *thd,
                                                                 int m_var_idx,
                                                                 Item *var) {
  sp_variable *spvar_ident = find_variable(m_var_idx);
  Item *expr = new (thd->mem_root) Item_func_record_table_found_bool(
      var, spvar_ident->name, spvar_ident->offset);

  return expr;
}

// forall i in var.first .. var.last,it means compare between i and var.last.
Item *sp_pcontext::make_item_plsql_sp_forall_record_table_loop_attr(
    THD *thd, int m_var_idx, Item *var) {
  sp_variable *spvar_ident = find_variable(m_var_idx);
  Item *expr = new (thd->mem_root) Item_func_record_table_forall_bool(
      var, spvar_ident->name, spvar_ident->offset);

  return expr;
}

Item *sp_pcontext::make_item_plsql_plus_one(THD *thd, POS &pos, int direction,
                                            Item_splocal *item_splocal) {
  int32 one = 1;

  Item_int *inc = new (thd->mem_root) Item_int(pos, one);
  LEX_CSTRING name = to_lex_cstring("1");
  Item_name_string *item_name = new (thd->mem_root) Item_name_string(name);
  inc->item_name = *item_name;
  Item *expr;
  if (direction > 0)
    expr = new (thd->mem_root) Item_func_plus(item_splocal, inc);
  else
    expr = new (thd->mem_root) Item_func_minus(item_splocal, inc);

  return expr;
}

bool sp_pcursor::check_param_count_with_error(uint param_count) const {
  if (param_count !=
      (m_param_context ? m_param_context->context_var_count() : 0)) {
    my_error(ER_WRONG_PARAMCOUNT_TO_CURSOR, MYF(0), LEX_CSTRING::str);
    return true;
  }
  return false;
}
