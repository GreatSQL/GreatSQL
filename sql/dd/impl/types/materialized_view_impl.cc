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

#include "sql/dd/impl/types/materialized_view_impl.h"

#include <sstream>
#include <string>
#include "sql/dd/impl/sdi_impl.h"                  // sdi read/write functions
#include "sql/dd/impl/tables/materialized_view.h"  // dd::tables::Materialized_views
#include "sql/dd/impl/transaction_impl.h"
#include "sql/dd/impl/utils.h"   // is_string_in_lowercase
#include "sql/dd/string_type.h"  // dd::String_type
#include "sql/dd/types/object_table.h"
#include "sql/dd/types/weak_object.h"

namespace dd {
Materialized_view_impl::Materialized_view_impl()
    : m_table(nullptr),
      m_build_clause(0),
      m_flush_mode(0),
      m_updated_time(0, 0),
      m_status(0) {}

Materialized_view_impl::Materialized_view_impl(Table_impl *table)
    : m_table(table),
      m_build_clause(0),
      m_flush_mode(0),
      m_updated_time(0, 0),
      m_status(0) {}

Materialized_view_impl::Materialized_view_impl(
    const Materialized_view_impl &src, Table_impl *parent)
    : Entity_object_impl(src),
      m_table(parent),
      m_build_clause(src.m_build_clause),
      m_flush_mode(src.m_flush_mode),
      m_updated_time(src.m_updated_time),
      m_status(src.m_status),
      m_error_msg(src.m_error_msg),
      m_definition(src.m_definition),
      m_definition_view(src.m_definition_view),
      m_table_ref(src.m_table_ref),
      m_columns(src.m_columns),
      m_options(src.m_options) {}

void Materialized_view_impl::register_tables(Open_dictionary_tables_ctx *otx) {
  otx->add_table<dd::tables::Materialized_views>();
}

const Object_table &Materialized_view_impl::object_table() const {
  return DD_table::instance();
}

const Table &Materialized_view_impl::table() const { return *m_table; }
Table &Materialized_view_impl::table() { return *m_table; }

bool Materialized_view_impl::validate() const {
  if (m_table == nullptr) {
    my_error(ER_INVALID_DD_OBJECT, MYF(0), DD_table::instance().name().c_str(),
             "No table object associated with this  Materialized View object.");
    return true;
  }
  return false;
}

bool Materialized_view_impl::restore_attributes(const Raw_record &r) {
  if (check_parent_consistency(m_table,
                               r.read_ref_id(DD_table::FIELD_TABLE_ID))) {
    return true;
  }
  restore_id(r, DD_table::FIELD_ID);

  m_status = r.read_int(DD_table::FIELD_STATUS);

  m_build_clause = r.read_int(DD_table::FIELD_BUILD_CLAUSE);

  m_flush_mode = r.read_int(DD_table::FIELD_FLUSH_MODE);

  m_updated_time = r.read_timestamp(DD_table::FIELD_LAST_UPDATED);
  m_error_msg = r.read_str(DD_table::FIELD_ERROR);

  m_definition = r.read_str(DD_table::FIELD_DEFINITION);

  m_definition_view = r.read_str(DD_table::FIELD_VIEW_DEFINITION);
  // NOTE: don't read json
  m_table_ref = r.read_str(DD_table::FIELD_TABLE_REF);
  // NOTE: don't read json
  if (!r.is_null(DD_table::FIELD_COLUMNS))
    m_columns = r.read_str(DD_table::FIELD_COLUMNS);

  String_type options_raw = r.read_str(DD_table::FIELD_OPTIONS);
  if (!options_raw.empty() && m_options.insert_values(options_raw)) return true;

  return false;
}

bool Materialized_view_impl::store_attributes(Raw_record *r) {
  return store_id(r, DD_table::FIELD_ID) ||
         r->store_ref_id(DD_table::FIELD_TABLE_ID, m_table->id()) ||
         r->store(DD_table::FIELD_STATUS, m_status) ||
         r->store(DD_table::FIELD_BUILD_CLAUSE, m_build_clause) ||

         r->store(DD_table::FIELD_FLUSH_MODE, m_flush_mode) ||
         r->store(DD_table::FIELD_ERROR, m_error_msg) ||
         r->store_timestamp(DD_table::FIELD_LAST_UPDATED, m_updated_time) ||
         r->store(DD_table::FIELD_COLUMNS, m_columns, m_columns.empty()) ||
         r->store(DD_table::FIELD_DEFINITION, m_definition) ||
         r->store(DD_table::FIELD_VIEW_DEFINITION, m_definition_view) ||
         r->store(DD_table::FIELD_TABLE_REF, m_table_ref,
                  m_table_ref.empty()) ||
         r->store(DD_table::FIELD_OPTIONS, m_options);
}

static_assert(
    Materialized_view_impl::DD_table::FIELD_OPTIONS == 11,
    "Materialized_view_impl definition has changed, check if serialize() "
    "and deserialize() needs to be updated!");

void Materialized_view_impl::serialize(Sdi_wcontext *wctx,
                                       Sdi_writer *w) const {
  w->StartObject();
  Entity_object_impl::serialize(wctx, w);
  write(w, m_status, STRING_WITH_LEN("status"));
  write(w, m_flush_mode, STRING_WITH_LEN("flush_mode"));
  write(w, m_build_clause, STRING_WITH_LEN("build_clause"));
  write(w, m_definition, STRING_WITH_LEN("definition"));
  write(w, m_definition_view, STRING_WITH_LEN("definition_view"));
  write(w, m_table_ref, STRING_WITH_LEN("table_ref"));
  write(w, m_columns, STRING_WITH_LEN("columns_names"));

  w->EndObject();
}

bool Materialized_view_impl::deserialize(Sdi_rcontext *rctx,
                                         const RJ_Value &val) {
  if (Entity_object_impl::deserialize(rctx, val)) return true;

  read(&m_status, val, "status");
  read(&m_flush_mode, val, "flush_mode");
  read(&m_build_clause, val, "build_clause");
  read(&m_definition, val, "definition");
  read(&m_definition_view, val, "definition_view");
  read(&m_table_ref, val, "table_ref");
  read(&m_columns, val, "columns_names");

  return false;
}

void Materialized_view_impl::debug_print(String_type &outb) const {
  std::ostringstream ss;
  ss << "Materialized View Log: {"
     << "id: " << id() << ", name: " << name() << ", table_id: "
     << (m_table != nullptr ? m_table->id() : INVALID_OBJECT_ID)
     << ", build_clause" << m_build_clause << ", flush_mode" << m_flush_mode
     << ", statue: " << m_status
     << ", last_updated: " << m_updated_time.m_tv_sec << ", define "
     << m_definition << "}";
  outb.append(ss.str());
}

const String_type &Materialized_view_impl::definition() const {
  return m_definition;
}

void Materialized_view_impl::set_definition(const String_type &definition) {
  m_definition = definition;
}

}  // namespace dd
