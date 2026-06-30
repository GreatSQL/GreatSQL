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

#include "sql/dd/impl/tables/mlog.h"

namespace dd {
namespace tables {

const CHARSET_INFO *Materialized_view_logs::name_collation() {
  return &my_charset_utf8mb3_general_ci;
}
const Materialized_view_logs &Materialized_view_logs::instance() {
  static Materialized_view_logs *s_instance = new Materialized_view_logs();
  return *s_instance;
}

Materialized_view_logs::Materialized_view_logs() {
  m_target_def.set_table_name("materialized_view_logs");
  m_target_def.add_field(FIELD_ID, "FIELD_ID",
                         "id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT");
  m_target_def.add_field(FIELD_NAME, "FIELD_NAME",
                         "name VARCHAR(64) NOT NULL COLLATE " +
                             String_type(name_collation()->m_coll_name));
  m_target_def.add_field(FIELD_TABLE_ID, "FIELD_TABLE_ID",
                         "table_id BIGINT UNSIGNED NOT NULL");

  m_target_def.add_field(FIELD_SCHEMA_ID, "FIELD_SCHEMA_ID",
                         "schema_id BIGINT UNSIGNED NOT NULL");
  m_target_def.add_field(FIELD_REFERENCED_TABLE_CATALOG,
                         "FIELD_REFERENCED_TABLE_CATALOG",
                         "referenced_table_catalog VARCHAR(512) NOT NULL");
  m_target_def.add_field(FIELD_REFERENCED_TABLE_SCHEMA,
                         "FIELD_REFERENCED_TABLE_SCHEMA",
                         "referenced_table_schema VARCHAR(64) NOT NULL");
  m_target_def.add_field(FIELD_REFERENCED_TABLE_NAME,
                         "FIELD_REFERENCED_TABLE_NAME",
                         "referenced_table_name VARCHAR(64) NOT NULL");

  m_target_def.add_field(FIELD_TRIGGER_NAME, "FIELD_TRIGGER_NAME",
                         "trigger_name VARCHAR(64) NOT NULL ");

  m_target_def.add_field(FIELD_STATUS, "FIELD_STATUS",
                         "status int NOT NULL DEFAULT 0");

  m_target_def.add_field(FIELD_REF, "FIELD_REF",
                         "ref BIGINT UNSIGNED NOT NULL DEFAULT 0");

  m_target_def.add_field(FIELD_LAST_PURGED, "FIELD_LAST_PURGED",
                         "last_purged TIMESTAMP NOT NULL");
  m_target_def.add_field(FIELD_OPTIONS, "FIELD_OPTIONS", "options MEDIUMTEXT");

  m_target_def.add_index(INDEX_PK_ID, "INDEX_PK_ID", "PRIMARY KEY(id)");
  m_target_def.add_index(INDEX_UK_TABLE_ID, "INDEX_UK_TABLE_ID",
                         "UNIQUE KEY  (table_id)");

  m_target_def.add_index(INDEX_UK_NAME, "INDEX_UK_NAME",
                         "UNIQUE KEY  (referenced_table_catalog, "
                         "referenced_table_schema, referenced_table_name)");
}

Object_key *Materialized_view_logs::create_key_by_table_id(Object_id table_id) {
  return new (std::nothrow)
      Parent_id_range_key(INDEX_UK_TABLE_ID, FIELD_TABLE_ID, table_id);
}

Object_key *Materialized_view_logs::create_key_by_name(
    const String_type &table_catalog, const String_type &table_schema,
    const String_type &table_name) {
  return new (std::nothrow) Table_reference_range_key(
      INDEX_UK_NAME, FIELD_REFERENCED_TABLE_CATALOG, table_catalog,
      FIELD_REFERENCED_TABLE_SCHEMA, table_schema, FIELD_REFERENCED_TABLE_NAME,
      table_name);
}

}  // namespace tables
}  // namespace dd
