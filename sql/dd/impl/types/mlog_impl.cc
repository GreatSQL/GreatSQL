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

#include "sql/dd/impl/types/mlog_impl.h"

#include <sstream>
#include <string>
#include "sql/dd/impl/sdi_impl.h"     // sdi read/write functions
#include "sql/dd/impl/tables/mlog.h"  // dd::tables::Materialized_view_logs
#include "sql/dd/impl/transaction_impl.h"
#include "sql/dd/impl/utils.h"   // is_string_in_lowercase
#include "sql/dd/string_type.h"  // dd::String_type
#include "sql/dd/types/object_table.h"
#include "sql/dd/types/weak_object.h"

namespace dd {

Materialized_view_log_impl::Materialized_view_log_impl()
    : m_status(0), m_purged_time(0, 0) {}

Materialized_view_log_impl::Materialized_view_log_impl(Table_impl *table)
    : m_table(table), m_status(0), m_purged_time(0, 0) {}

Materialized_view_log_impl::Materialized_view_log_impl(
    const Materialized_view_log_impl &src, Table_impl *parent)
    : Entity_object_impl(src),
      m_table(parent),
      m_referenced_table_catalog_name(src.m_referenced_table_catalog_name),
      m_referenced_table_schema_name(src.m_referenced_table_schema_name),
      m_referenced_table_name(src.m_referenced_table_name),
      m_trigger_name(src.m_trigger_name),
      m_status(src.m_status),
      m_purged_time(src.m_purged_time),
      m_ref(src.m_ref),
      m_options(src.m_options) {}

const Table &Materialized_view_log_impl::table() const { return *m_table; }
Table &Materialized_view_log_impl::table() { return *m_table; }

const Object_table &Materialized_view_log_impl::object_table() const {
  return DD_table::instance();
}
bool Materialized_view_log_impl::validate() const {
  if (m_table == nullptr) {
    my_error(
        ER_INVALID_DD_OBJECT, MYF(0), DD_table::instance().name().c_str(),
        "No table object associated with this  Materialized View Log object.");
    return true;
  }
  return false;
}

bool Materialized_view_log_impl::restore_attributes(const Raw_record &r) {
  if (check_parent_consistency(m_table,
                               r.read_ref_id(DD_table::FIELD_TABLE_ID))) {
    return true;
  }
  restore_id(r, DD_table::FIELD_ID);
  restore_name(r, DD_table::FIELD_NAME);

  m_referenced_table_catalog_name =
      r.read_str(DD_table::FIELD_REFERENCED_TABLE_CATALOG);
  m_referenced_table_schema_name =
      r.read_str(DD_table::FIELD_REFERENCED_TABLE_SCHEMA);
  m_referenced_table_name = r.read_str(DD_table::FIELD_REFERENCED_TABLE_NAME);
  m_trigger_name = r.read_str(DD_table::FIELD_TRIGGER_NAME);
  m_status = r.read_int(DD_table::FIELD_STATUS);
  m_purged_time = r.read_timestamp(DD_table::FIELD_LAST_PURGED);
  m_ref = r.read_uint(DD_table::FIELD_REF);

  String_type options_raw = r.read_str(DD_table::FIELD_OPTIONS);
  if (!options_raw.empty() && m_options.insert_values(options_raw)) return true;

  return false;
}

bool Materialized_view_log_impl::store_attributes(Raw_record *r) {
  if (m_table != nullptr)
    set_name(m_table->name());  // use for find_all_mlog easy find mlog_name

  return store_id(r, DD_table::FIELD_ID) ||
         store_name(r, DD_table::FIELD_NAME) ||
         r->store_ref_id(DD_table::FIELD_TABLE_ID, m_table->id()) ||
         r->store_ref_id(DD_table::FIELD_SCHEMA_ID, m_table->schema_id()) ||
         r->store(DD_table::FIELD_REFERENCED_TABLE_CATALOG,
                  m_referenced_table_catalog_name) ||
         r->store(DD_table::FIELD_REFERENCED_TABLE_SCHEMA,
                  m_referenced_table_schema_name) ||
         r->store(DD_table::FIELD_REFERENCED_TABLE_NAME,
                  m_referenced_table_name) ||
         r->store(DD_table::FIELD_TRIGGER_NAME, m_trigger_name) ||

         r->store(DD_table::FIELD_STATUS, m_status) ||
         r->store_timestamp(DD_table::FIELD_LAST_PURGED, m_purged_time) ||
         r->store(DD_table::FIELD_REF, m_ref) ||
         r->store(DD_table::FIELD_OPTIONS, m_options);
}

void Materialized_view_log_impl::debug_print(String_type &outb) const {
  std::ostringstream ss;
  ss << "Materialized View Log: {"
     << "id: " << id() << ", name: " << name() << ", table_id: "
     << (m_table != nullptr ? m_table->id() : INVALID_OBJECT_ID)
     << ", referenced_table_catalog: " << m_referenced_table_catalog_name
     << ", referenced_table_schema: " << m_referenced_table_schema_name
     << ", referenced_table_name: " << m_referenced_table_name
     << ", referenced_trigger_name: " << m_trigger_name
     << ", statue: " << m_status << ", last_purged: " << m_purged_time.m_tv_sec
     << ", ref: " << m_ref << "}";
  outb.append(ss.str().c_str());
}

void Materialized_view_log_impl::register_tables(
    Open_dictionary_tables_ctx *otx) {
  otx->add_table<dd::tables::Materialized_view_logs>();
}

static_assert(
    Materialized_view_log_impl::DD_table::FIELD_OPTIONS == 11,
    "Materialized_view_log_impl definition has changed, check if serialize() "
    "and deserialize() needs to be updated!");

void Materialized_view_log_impl::serialize(Sdi_wcontext *wctx,
                                           Sdi_writer *w) const {
  w->StartObject();
  Entity_object_impl::serialize(wctx, w);

  write(w, m_ref, STRING_WITH_LEN("ref"));
  write(w, m_status, STRING_WITH_LEN("status"));
  write(w, m_referenced_table_catalog_name,
        STRING_WITH_LEN("referenced_table_catalog_name"));
  write(w, m_referenced_table_schema_name,
        STRING_WITH_LEN("referenced_table_schema_name"));
  write(w, m_referenced_table_name, STRING_WITH_LEN("referenced_table_name"));
  write(w, m_trigger_name, STRING_WITH_LEN("referenced_trigger_name"));

  w->EndObject();
}

///////////////////////////////////////////////////////////////////////////

bool Materialized_view_log_impl::deserialize(Sdi_rcontext *rctx,
                                             const RJ_Value &val) {
  if (Entity_object_impl::deserialize(rctx, val)) return true;
  read(&m_ref, val, "ref");
  read(&m_status, val, "status");
  read(&m_referenced_table_catalog_name, val, "referenced_table_catalog_name");
  read(&m_referenced_table_schema_name, val, "referenced_table_schema_name");
  read(&m_referenced_table_name, val, "referenced_table_name");
  read(&m_trigger_name, val, "referenced_trigger_name");
  return false;
}

}  // namespace dd
