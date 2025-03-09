/* Copyright (c) 2016, 2022, Oracle and/or its affiliates. All rights reserved.
   Copyright (c) 2023, 2025, GreatDB Software Co., Ltd.

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

#ifndef SQL_CALL_INCLUDED
#define SQL_CALL_INCLUDED

#include "my_sqlcommand.h"
#include "sp.h"
#include "sql/sql_cmd_dml.h"  // Sql_cmd_dml

class Item;
class THD;
class sp_name;
template <class T>
class mem_root_deque;
class sp_head;
enum class enum_sp_type;

class Sql_cmd_call final : public Sql_cmd_dml {
 public:
  explicit Sql_cmd_call(sp_name *proc_name_arg,
                        mem_root_deque<Item *> *prog_args_arg,
                        enum_sp_type sp_type, sp_name *pkg_name,
                        sp_signature *sig, LEX_CSTRING db_arg,
                        LEX_STRING fn_arg, bool explicit_arg)
      : Sql_cmd_dml(),
        proc_name(proc_name_arg),
        proc_args(prog_args_arg),
        m_type(sp_type),
        m_pkg_name(pkg_name),
        m_sig(sig),
        m_save_db(db_arg),
        m_save_fn_name(fn_arg),
        m_save_explicit_name(explicit_arg) {}

  enum_sql_command sql_command_code() const override { return SQLCOM_CALL; }

  bool is_data_change_stmt() const override { return false; }

 protected:
  bool precheck(THD *thd) override;
  bool check_privileges(THD *thd) override;

  bool prepare_inner(THD *thd) override;

  bool execute_inner(THD *thd) override;

 private:
  sp_name *proc_name;
  mem_root_deque<Item *> *proc_args;
  friend bool reorder_named_parameters(THD *thd, const char *type,
                                       Item **params, uint params_count,
                                       sp_head *sp, bool *is_ordered);

 public:
  enum_sp_type m_type;
  sp_name *m_pkg_name;
  sp_signature *m_sig;
  LEX_CSTRING m_save_db;
  LEX_STRING m_save_fn_name;
  bool m_save_explicit_name;
};

class Sql_cmd_compound : public Sql_cmd_dml {
  sp_head *sp;

 public:
  explicit Sql_cmd_compound(sp_head *sp_arg) : sp(sp_arg) {}

  bool is_data_change_stmt() const override { return false; }

  enum_sql_command sql_command_code() const override { return SQLCOM_COMPOUND; }

 protected:
  bool precheck(THD *thd) override;
  bool check_privileges(THD *thd) override;

  bool prepare_inner(THD *thd) override;
  bool execute_inner(THD *thd) override;
};

class Sql_cmd_execute : public Sql_cmd_dml {
  Item *m_query;
  int mode;
  mem_root_deque<Item *> m_param;
  // Prepared_statement m_stmt;

 public:
  Sql_cmd_execute(Item *query_arg, int mode_arg,
                  mem_root_deque<Item *> *param_arg)
      : m_query(query_arg), mode(mode_arg), m_param(*THR_MALLOC) {
    if (param_arg) {
      m_param = std::move(*param_arg);
    }
  }
  enum_sql_command sql_command_code() const override {
    return SQLCOM_EXECUTE_IMMEDIATE;
  }
  bool is_data_change_stmt() const override { return false; }

 protected:
  bool precheck(THD *thd) override;
  bool check_privileges(THD *thd) override;

  bool prepare_inner(THD *thd) override;

  bool execute_inner(THD *thd) override;
};

#endif /* SQL_CALL_INCLUDED */
