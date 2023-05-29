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

#include "sql/dd/impl/types/package_spec_impl.h"

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
// Package_spec_impl implementation.
///////////////////////////////////////////////////////////////////////////

bool Package_spec_impl::update_routine_name_key(Name_key *key,
                                                Object_id schema_id,
                                                const String_type &name) const {
  return Package_spec::update_name_key(key, schema_id, name);
}

///////////////////////////////////////////////////////////////////////////

bool Package_spec::update_name_key(Name_key *key, Object_id schema_id,
                                   const String_type &name) {
  return Routines::update_object_key(key, schema_id, Routine::RT_PACKAGE_SPEC,
                                     name);
}

///////////////////////////////////////////////////////////////////////////

void Package_spec_impl::debug_print(String_type &outb) const {
  dd::Stringstream_type ss;

  String_type s;
  Routine_impl::debug_print(s);

  ss << "PACKAGE OBJECT: { " << s << "} ";

  outb = ss.str();
}

///////////////////////////////////////////////////////////////////////////

Package_spec_impl::Package_spec_impl(const Package_spec_impl &src)
    : Weak_object(src), Routine_impl(src) {}

///////////////////////////////////////////////////////////////////////////

}  // namespace dd
