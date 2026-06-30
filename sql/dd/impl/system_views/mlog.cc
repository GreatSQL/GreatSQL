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

#include "sql/dd/impl/system_views/mlog.h"

#include <algorithm>
#include <string>

#include "sql/keyword_list.h"
#include "sql/stateless_allocator.h"

namespace dd {
namespace system_views {

const Materialized_view_logs &Materialized_view_logs::instance() {
  static Materialized_view_logs *s_instance = new Materialized_view_logs();
  return *s_instance;
}
Materialized_view_logs::Materialized_view_logs() {
  m_target_def.set_view_name(view_name());

  m_target_def.add_field(FIELD_MLOG_DB, "MLOG_DB",
                         "sch.name" + m_target_def.fs_name_collation());

  m_target_def.add_field(FIELD_MLOG_NAME, "MLOG_NAME",
                         "vw.name" + m_target_def.fs_name_collation());

  m_target_def.add_field(
      FIELD_TABLE_SCHEMA, "TABLE_SCHEMA",
      "ml.referenced_table_schema" + m_target_def.fs_name_collation());
  m_target_def.add_field(
      FIELD_TABLE_NAME, "TABLE_NAME",
      "ml.referenced_table_name" + m_target_def.fs_name_collation());

  m_target_def.add_field(FIELD_REF, "REF", "ml.ref");

  m_target_def.add_field(FIELD_STATUS, "STATUS", "ml.status");

  m_target_def.add_field(FIELD_LAST_PURGED, "LAST_PURGED", "ml.last_purged");

  m_target_def.add_from("mysql.tables vw");
  m_target_def.add_from("JOIN mysql.schemata sch ON vw.schema_id=sch.id");
  m_target_def.add_from(
      "JOIN mysql.materialized_view_logs ml ON ml.table_id=vw.id");
}

}  // namespace system_views
}  // namespace dd
