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

#ifndef DD__PACKAGE_BODY_INCLUDED
#define DD__PACKAGE_BODY_INCLUDED

#include "sql/dd/types/routine.h"  // Routine

struct MDL_key;

namespace dd {

class Package_body_impl;

///////////////////////////////////////////////////////////////////////////

class Package_body : virtual public Routine {
 public:
  typedef Package_body_impl Impl;

  bool update_name_key(Name_key *key) const override {
    return update_routine_name_key(key, schema_id(), name());
  }

  static bool update_name_key(Name_key *key, Object_id schema_id,
                              const String_type &name);

 public:
  ~Package_body() override {}

 public:
  /**
    Allocate a new object graph and invoke the copy contructor for
    each object. Only used in unit testing.

    @return pointer to dynamically allocated copy
  */
  Package_body *clone() const override = 0;
  Package_body *clone_dropped_object_placeholder() const override = 0;

  static void create_mdl_key(const String_type &schema_name,
                             const String_type &name, MDL_key *key) {
    Routine::create_mdl_key(RT_PACKAGE_BODY, schema_name, name, key);
  }
};

///////////////////////////////////////////////////////////////////////////

}  // namespace dd

#endif  // DD__PACKAGE_BODY_INCLUDED
