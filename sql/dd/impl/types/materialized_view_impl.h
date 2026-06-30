/* Copyright (c) 2026, GreatDB Software Co., Ltd.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have either included with
   the program or referenced in the documentation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#ifndef DD__MATERIALIZED_VIEW_IMPL_INCLUDED
#define DD__MATERIALIZED_VIEW_IMPL_INCLUDED

#include "my_config.h"

#include "my_inttypes.h"
#include "sql/dd/string_type.h"

#ifdef HAVE_SYS_TIME_H
#include <sys/time.h>
#endif
#include <sys/types.h>
#include <new>

#include "sql/dd/impl/properties_impl.h"  // Properties_impl
#include "sql/dd/impl/raw/raw_record.h"
#include "sql/dd/impl/types/entity_object_impl.h"  // dd::Entity_object_impl
#include "sql/dd/impl/types/table_impl.h"          // dd::Table_impl
#include "sql/dd/impl/types/weak_object_impl.h"
#include "sql/dd/types/materialized_view.h"

#include "sql/mysqld_cs.h"  // system_charset_info

namespace dd {

class Materialized_view_impl : public Entity_object_impl,
                               public Materialized_view {
 public:
  Materialized_view_impl();
  Materialized_view_impl(Table_impl *table);

  Materialized_view_impl(const Materialized_view_impl &src, Table_impl *parent);

  static void register_tables(Open_dictionary_tables_ctx *otx);

  const Object_table &object_table() const override;

  bool validate() const override;

  bool restore_attributes(const Raw_record &r) override;

  bool store_attributes(Raw_record *r) override;

  void debug_print(String_type &outb) const override;

  void set_ordinal_position(uint) {}

  uint ordinal_position() const { return -1; }
  /////////////////////////////////////////////////////////////////////////
  // parent table.
  /////////////////////////////////////////////////////////////////////////

  const Table &table() const override;

  Table &table() override;

  /* non-virtual */ const Table_impl &table_impl() const { return *m_table; }

  /* non-virtual */ Table_impl &table_impl() { return *m_table; }

  Object_id schema_id() const override {
    return (m_table != nullptr ? m_table->schema_id() : INVALID_OBJECT_ID);
  }

  /////////////////////////////////////////////////////////////////////////
  // table.
  /////////////////////////////////////////////////////////////////////////

  Object_id table_id() const override {
    return (m_table != nullptr ? m_table->id() : INVALID_OBJECT_ID);
  }
  /////////////////////////////////////////////////////////////////////////

  void serialize(Sdi_wcontext *wctx, Sdi_writer *w) const override;

  bool deserialize(Sdi_rcontext *rctx, const RJ_Value &val) override;

  static Materialized_view_impl *restore_item(Table_impl *table) {
    return new (std::nothrow) Materialized_view_impl(table);
  }

  static Materialized_view_impl *clone(const Materialized_view_impl &other,
                                       Table_impl *parent) {
    return new Materialized_view_impl(other, parent);
  }
  // Fix "inherits ... via dominance" warnings
  Entity_object_impl *impl() override { return Entity_object_impl::impl(); }
  const Entity_object_impl *impl() const override {
    return Entity_object_impl::impl();
  }
  Object_id id() const override { return Entity_object_impl::id(); }
  bool is_persistent() const override {
    return Entity_object_impl::is_persistent();
  }
  const String_type &name() const override {
    return Entity_object_impl::name();
  }
  void set_name(const String_type &name) override {
    Entity_object_impl::set_name(name);
  }

  int status() override { return m_status; }

  void set_status(int statue_arg) override { m_status = statue_arg; }

  my_timeval last_updated() const override { return m_updated_time; }

  void set_last_updated(my_timeval update_time) override {
    m_updated_time = update_time;
  }

  int build_clause() const override { return m_build_clause; }

  void set_build_clause(int build_clause) override {
    m_build_clause = build_clause;
  }

  int flush_mode() const override { return m_flush_mode; }
  void set_flush_mode(int flush_mode) override { m_flush_mode = flush_mode; }

  String_type error_msg() override { return m_error_msg; }
  void set_error_msg(String_type error_msg) override {
    m_error_msg = error_msg;
  }

  const Properties &options() const override { return m_options; }

  Properties &options() override { return m_options; }

  bool set_options(const String_type &options_raw) override {
    return m_options.insert_values(options_raw);
  }
  // source define query
  const String_type &definition() const override;
  void set_definition(const String_type &definition) override;

  // has transform query
  const String_type &definition_view() const override {
    return m_definition_view;
  }
  void set_definition_view(const String_type &definition_view) override {
    m_definition_view = definition_view;
  }

  // only use for sql read

  const String_type &table_ref() const override { return m_table_ref; }

  void set_table_ref(const String_type &table_ref) override {
    m_table_ref = table_ref;
  }

  const String_type &columns() const override { return m_columns; }

  String_type &columns() override { return m_columns; }

  void set_columns(const String_type &columns) override { m_columns = columns; }

 private:
  Table_impl *m_table = nullptr;

  int m_build_clause;
  int m_flush_mode;

  my_timeval m_updated_time;

  int m_status;
  String_type m_error_msg;

  String_type m_definition;

  String_type m_definition_view;

  String_type m_table_ref;

  String_type m_columns;
  Properties_impl m_options;
};
}  // namespace dd

#endif
