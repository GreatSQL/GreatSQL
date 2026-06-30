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

#ifndef DD__MATERIALIZED_VIEW_LOG_IMPL_INCLUDED
#define DD__MATERIALIZED_VIEW_LOG_IMPL_INCLUDED

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
#include "sql/dd/types/mlog.h"

#include "sql/mysqld_cs.h"  // system_charset_info
namespace dd {

class Materialized_view_log_impl : public Entity_object_impl,
                                   public Materialized_view_log {
 public:
  Materialized_view_log_impl();

  Materialized_view_log_impl(Table_impl *table);

  Materialized_view_log_impl(const Materialized_view_log_impl &src,
                             Table_impl *parent);

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

  /////////////////////////////////////////////////////////////////////////

  static Materialized_view_log_impl *restore_item(Table_impl *table) {
    return new (std::nothrow) Materialized_view_log_impl(table);
  }

  static Materialized_view_log_impl *clone(
      const Materialized_view_log_impl &other, Table_impl *parent) {
    return new Materialized_view_log_impl(other, parent);
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

  /////////////////////////////////////////////////////////////////////////
  // the schema name of the referenced table.
  /////////////////////////////////////////////////////////////////////////

  const String_type &referenced_table_catalog_name() const override {
    return m_referenced_table_catalog_name;
  }

  void set_referenced_table_catalog_name(const String_type &name) override {
    m_referenced_table_catalog_name = name;
  }

  const String_type &referenced_table_schema_name() const override {
    return m_referenced_table_schema_name;
  }

  void set_referenced_table_schema_name(const String_type &name) override {
    m_referenced_table_schema_name = name;
  }

  const String_type &referenced_table_name() const override {
    return m_referenced_table_name;
  }

  void set_referenced_table_name(const String_type &name) override {
    m_referenced_table_name = name;
  }

  void set_referenced_trigger_name(const String_type &name) override {
    m_trigger_name = name;
  }

  const String_type &referenced_trigger_name() const override {
    return m_trigger_name;
  }

  int status() override { return m_status; }

  void set_status(int statue_arg) override { m_status = statue_arg; }

  my_timeval last_purged() override { return m_purged_time; }

  void set_last_purged(my_timeval update_time) override {
    m_purged_time = update_time;
  }

  /////////////////////////////////////////////////////////////////////

  /**
   * bit map
   *
   *  define all mv has read bitmap value
   *  each row compare with this value
   *  until purge
   */
  uint64_t ref() override { return m_ref; }
  void set_ref(uint64_t ref_arg) override { m_ref = ref_arg; }

  /////////////////////////////////////////////////////////////////////////
  // Options.
  /////////////////////////////////////////////////////////////////////////

  const Properties &options() const override { return m_options; }

  Properties &options() override { return m_options; }

  bool set_options(const String_type &options_raw) override {
    return m_options.insert_values(options_raw);
  }

 private:
  Table_impl *m_table = nullptr;
  String_type m_referenced_table_catalog_name;
  String_type m_referenced_table_schema_name;
  String_type m_referenced_table_name;

  String_type m_trigger_name;

  int m_status;
  my_timeval m_purged_time;
  ulonglong m_ref;
  Properties_impl m_options;
};

}  // namespace dd

#endif
