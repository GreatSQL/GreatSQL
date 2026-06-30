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

#ifndef DD_TABLES__MATERIALIZED_VIEWS_INCLUDED
#define DD_TABLES__MATERIALIZED_VIEWS_INCLUDED

#include "sql/dd/impl/raw/object_keys.h"
#include "sql/dd/impl/types/object_table_impl.h"  // dd::Object_table_impl
#include "sql/dd/object_id.h"                     // dd::Object_id
#include "sql/dd/string_type.h"

struct CHARSET_INFO;

namespace dd {
class Object_key;

namespace tables {

///////////////////////////////////////////////////////////////////////////

class Materialized_views : public Object_table_impl {
 public:
  static const CHARSET_INFO *name_collation();
  static const Materialized_views &instance();

  enum enum_fields {
    FIELD_ID,
    FIELD_TABLE_ID,
    FIELD_BUILD_CLAUSE,
    FIELD_FLUSH_MODE,
    FIELD_LAST_UPDATED,
    FIELD_STATUS,
    FIELD_ERROR,
    FIELD_COLUMNS,
    FIELD_DEFINITION,
    FIELD_VIEW_DEFINITION,
    FIELD_TABLE_REF,
    FIELD_OPTIONS,
  };

  enum enum_indexes {
    INDEX_PK_ID = static_cast<uint>(Common_index::PK_ID),
    INDEX_UK_TABLE_ID,
  };

  Materialized_views();

  static Object_key *create_key_by_table_id(Object_id table_id);
};

}  // namespace tables

}  // namespace dd

#endif
