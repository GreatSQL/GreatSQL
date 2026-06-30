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

#include "sql/dd/impl/system_views/materialized_view.h"

#include <algorithm>
#include <string>

#include "sql/keyword_list.h"
#include "sql/stateless_allocator.h"

namespace dd {
namespace system_views {

const Materialized_views &Materialized_views::instance() {
  static Materialized_views *s_instance = new Materialized_views();
  return *s_instance;
}
Materialized_views::Materialized_views() {
  m_target_def.set_view_name(view_name());

  m_target_def.add_field(FIELD_TABLE_SCHEMA, "VIEW_SCHEMA",
                         "sch.name" + m_target_def.fs_name_collation());

  m_target_def.add_field(FIELD_TABLE_NAME, "VIEW_NAME",
                         "vw.name" + m_target_def.fs_name_collation());

  m_target_def.add_field(FIELD_BUILD_CLAUSE, "BUILD_CLAUSE", "ml.build_clause");

  m_target_def.add_field(FIELD_FLUSH_MODE, "FLUSH_MODE", "ml.flush_mode");

  m_target_def.add_field(FIELD_LAST_UPDATED, "LAST_UPDATED", "ml.last_updated");
  m_target_def.add_field(FIELD_STATUS, "MATERIALIZED_VIEW_STATUS", "ml.status");

  m_target_def.add_field(FIELD_VIEW_DEFINITION, "VIEW_DEFINITION",
                         "ml.view_definition");
  m_target_def.add_field(FIELD_VIEW_DEFINITION_UTF8, "VIEW_DEFINITION_UTF8",
                         "ml.view_definition_utf8");
  m_target_def.add_field(FIELD_VIEW_USAGE, "VIEW_USEAGE", "ml.table_ref");
  m_target_def.add_field(FIELD_ERROR, "VIEW_ERROR", "ml.error_msg");

  m_target_def.add_from("mysql.tables vw");
  m_target_def.add_from("JOIN mysql.schemata sch ON vw.schema_id=sch.id");
  m_target_def.add_from(
      "JOIN mysql.materialized_views ml ON ml.table_id=vw.id");
}

const Materialized_view_usages &Materialized_view_usages::instance() {
  static Materialized_view_usages *s_instance = new Materialized_view_usages();
  return *s_instance;
}
Materialized_view_usages::Materialized_view_usages() {
  m_target_def.set_view_name(view_name());

  m_target_def.add_field(FIELD_TABLE_SCHEMA, "VIEW_SCHEMA",
                         "sch.name" + m_target_def.fs_name_collation());

  m_target_def.add_field(FIELD_TABLE_NAME, "VIEW_NAME",
                         "vw.name" + m_target_def.fs_name_collation());

  m_target_def.add_field(FIELD_REF_SCHEMA, "REF_SCHEMA",
                         "jt.db" + m_target_def.fs_name_collation());

  m_target_def.add_field(FIELD_REF_NAME, "REF_NAME",
                         "jt.table_name" + m_target_def.fs_name_collation());

  m_target_def.add_field(FIELD_REF, "REF", "jt.ref");

  m_target_def.add_from("mysql.tables vw");

  m_target_def.add_from("JOIN mysql.schemata sch ON vw.schema_id=sch.id");
  m_target_def.add_from(
      "JOIN mysql.materialized_views mv ON mv.table_id=vw.id");

  m_target_def.add_from(
      "JOIN JSON_TABLE( mv.table_ref,   '$[*]' COLUMNS( db VARCHAR(64) PATH '$.db', \
       ref INT PATH '$.ref',  table_name VARCHAR(64) PATH '$.table')) AS jt");
}

}  // namespace system_views
}  // namespace dd
