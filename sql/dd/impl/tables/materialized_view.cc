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

#include "sql/dd/impl/tables/materialized_view.h"

namespace dd {
namespace tables {

const CHARSET_INFO *Materialized_views::name_collation() {
  return &my_charset_utf8mb3_general_ci;
}

const Materialized_views &Materialized_views::instance() {
  static Materialized_views *s_instance = new Materialized_views();
  return *s_instance;
}

Materialized_views::Materialized_views() {
  m_target_def.set_table_name("materialized_views");

  m_target_def.add_field(FIELD_ID, "FIELD_ID",
                         "id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT");

  m_target_def.add_field(FIELD_TABLE_ID, "FIELD_TABLE_ID",
                         "table_id BIGINT UNSIGNED NOT NULL");

  m_target_def.add_field(FIELD_BUILD_CLAUSE, "FIELD_BUILD_CLAUSE",
                         "build_clause int NOT NULL DEFAULT 0");

  m_target_def.add_field(FIELD_FLUSH_MODE, "FIELD_FLUSH_MODE",
                         "flush_mode int NOT NULL DEFAULT 0");
  m_target_def.add_field(FIELD_STATUS, "FIELD_STATUS",
                         "status int NOT NULL DEFAULT 0");

  m_target_def.add_field(FIELD_LAST_UPDATED, "FIELD_LAST_UPDATED",
                         "last_updated TIMESTAMP NOT NULL");
  m_target_def.add_field(FIELD_ERROR, "FIELD_ERROR", "error_msg TEXT");

  m_target_def.add_field(FIELD_COLUMNS, "FIELD_COLUMNS",
                         "view_column_names JSON");

  m_target_def.add_field(FIELD_DEFINITION, "FIELD_DEFINITION",
                         "view_definition LONGTEXT");
  m_target_def.add_field(FIELD_VIEW_DEFINITION, "FIELD_VIEW_DEFINITION",
                         "view_definition_utf8 LONGTEXT");
  m_target_def.add_field(FIELD_TABLE_REF, "FIELD_TABLE_REF", "table_ref JSON"),
      m_target_def.add_field(FIELD_OPTIONS, "FIELD_OPTIONS",
                             "options MEDIUMTEXT");
  m_target_def.add_index(INDEX_PK_ID, "INDEX_PK_ID", "PRIMARY KEY(id)");
  m_target_def.add_index(INDEX_UK_TABLE_ID, "INDEX_UK_TABLE_ID",
                         "UNIQUE KEY  (table_id)");
}

Object_key *Materialized_views::create_key_by_table_id(Object_id table_id) {
  return new (std::nothrow)
      Parent_id_range_key(INDEX_UK_TABLE_ID, FIELD_TABLE_ID, table_id);
}

}  // namespace tables
}  // namespace dd
