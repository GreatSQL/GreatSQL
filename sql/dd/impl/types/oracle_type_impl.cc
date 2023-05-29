/* Copyright (c) 2023, GreatDB Software Co., Ltd.

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

#include "sql/dd/impl/types/oracle_type_impl.h"

#include <sstream>
#include <string>

#include "sql/dd/impl/tables/routines.h"   // Routines
#include "sql/dd/impl/transaction_impl.h"  // Open_dictionary_tables_ctx
#include "sql/dd/string_type.h"            // dd::String_type
#include "sql/dd/types/parameter.h"        // Parameter
#include "sql/dd/types/weak_object.h"

using dd::tables::Routines;

namespace dd {

///////////////////////////////////////////////////////////////////////////
// Oracle_type_impl implementation.
///////////////////////////////////////////////////////////////////////////

Oracle_type_impl::Oracle_type_impl()
    : m_result_data_type(enum_column_types::TYPE_NULL),
      m_result_data_type_null(false),
      m_result_is_zerofill(false),
      m_result_is_unsigned(false),
      m_result_numeric_precision_null(true),
      m_result_numeric_scale_null(true),
      m_result_datetime_precision_null(true),
      m_result_numeric_precision(0),
      m_result_numeric_scale(0),
      m_result_datetime_precision(0),
      m_result_char_length(0),
      m_result_collation_id(INVALID_OBJECT_ID) {
  set_type(RT_TYPE);
}

///////////////////////////////////////////////////////////////////////////
// Oracle_type_impl implementation.
///////////////////////////////////////////////////////////////////////////

bool Oracle_type_impl::update_routine_name_key(Name_key *key,
                                               Object_id schema_id,
                                               const String_type &name) const {
  return Oracle_type::update_name_key(key, schema_id, name);
}

///////////////////////////////////////////////////////////////////////////

bool Oracle_type::update_name_key(Name_key *key, Object_id schema_id,
                                  const String_type &name) {
  return Routines::update_object_key(key, schema_id, Routine::RT_TYPE, name);
}

///////////////////////////////////////////////////////////////////////////

void Oracle_type_impl::debug_print(String_type &outb) const {
  dd::Stringstream_type ss;

  String_type s;
  Routine_impl::debug_print(s);

  ss << "TYPE OBJECT: { " << s
     << "m_result_data_type: " << static_cast<int>(m_result_data_type) << "; "
     << "m_result_data_type_utf8: " << m_result_data_type_utf8 << "; "
     << "m_result_data_type_null: " << m_result_data_type_null << "; "
     << "m_result_is_zerofill: " << m_result_is_zerofill << "; "
     << "m_result_is_unsigned: " << m_result_is_unsigned << "; "
     << "m_result_numeric_precision: " << m_result_numeric_precision << "; "
     << "m_result_numeric_precision_null: " << m_result_numeric_precision_null
     << "; "
     << "m_result_numeric_scale: " << m_result_numeric_scale << "; "
     << "m_result_numeric_scale_null: " << m_result_numeric_scale_null << "; "
     << "m_result_datetime_precision: " << m_result_datetime_precision << "; "
     << "m_result_datetime_precision_null: " << m_result_datetime_precision_null
     << "; "
     << "m_result_char_length: " << m_result_char_length << "; "
     << "m_result_collation_id: " << m_result_collation_id << "; "
     << "} ";

  outb = ss.str();
}

///////////////////////////////////////////////////////////////////////////

Oracle_type_impl::Oracle_type_impl(const Oracle_type_impl &src)
    : Weak_object(src),
      Routine_impl(src),
      m_result_data_type(src.m_result_data_type),
      m_result_data_type_utf8(src.m_result_data_type_utf8),
      m_result_data_type_null(src.m_result_data_type_null),
      m_result_is_zerofill(src.m_result_is_zerofill),
      m_result_is_unsigned(src.m_result_is_unsigned),
      m_result_numeric_precision_null(src.m_result_numeric_precision_null),
      m_result_numeric_scale_null(src.m_result_numeric_scale_null),
      m_result_datetime_precision_null(src.m_result_datetime_precision_null),
      m_result_numeric_precision(src.m_result_numeric_precision),
      m_result_numeric_scale(src.m_result_numeric_scale),
      m_result_datetime_precision(src.m_result_datetime_precision),
      m_result_char_length(src.m_result_char_length),
      m_result_collation_id(src.m_result_collation_id) {}

///////////////////////////////////////////////////////////////////////////

}  // namespace dd
