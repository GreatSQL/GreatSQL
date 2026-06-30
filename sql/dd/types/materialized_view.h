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

#ifndef DD__MATERIALIZED_VIEW_INCLUDED
#define DD__MATERIALIZED_VIEW_INCLUDED

#include "my_time_t.h"
#include "sql/dd/impl/tables/materialized_view.h"  // dd::Materialized_views
#include "sql/dd/types/entity_object.h"            // dd::Entity_object
namespace dd {

class Materialized_view_impl;

class Materialized_view : virtual public Entity_object {
 public:
  typedef tables::Materialized_views DD_table;
  typedef Materialized_view_impl Impl;  // register_tables
  ~Materialized_view() override = default;

  /////////////////////////////////////////////////////////////////////////
  // parent table.
  /////////////////////////////////////////////////////////////////////////

  virtual const Table &table() const = 0;

  virtual Table &table() = 0;

  /////////////////////////////////////////////////////////////////////////
  // schema.
  /////////////////////////////////////////////////////////////////////////

  virtual Object_id schema_id() const = 0;

  /////////////////////////////////////////////////////////////////////////
  // table.
  /////////////////////////////////////////////////////////////////////////

  virtual Object_id table_id() const = 0;

  //////////////////////////////////////////////////////////////////////////

  /////////////////////////////////////////////////////////////////////////

  virtual int build_clause() const = 0;
  virtual void set_build_clause(int build_clause) = 0;

  virtual int flush_mode() const = 0;

  virtual void set_flush_mode(int flush_mode) = 0;

  virtual String_type error_msg() = 0;
  virtual void set_error_msg(String_type error_msg) = 0;

  virtual int status() = 0;

  virtual void set_status(int statue_arg) = 0;

  virtual my_timeval last_updated() const = 0;

  virtual void set_last_updated(my_timeval update_time) = 0;

  /////////////////////////////////////////////////////////////////////////
  // definition/utf8.
  /////////////////////////////////////////////////////////////////////////
  virtual const String_type &definition() const = 0;
  virtual void set_definition(const String_type &definition) = 0;

  virtual const String_type &definition_view() const = 0;
  virtual void set_definition_view(const String_type &definition_view) = 0;

  virtual const String_type &table_ref() const = 0;
  virtual void set_table_ref(const String_type &table_ref) = 0;

  /////////////////////////////////////////////////////////////////////////
  // Options.
  /////////////////////////////////////////////////////////////////////////

  virtual const Properties &options() const = 0;

  virtual Properties &options() = 0;

  virtual bool set_options(const String_type &) = 0;

  /// columns map
  virtual const String_type &columns() const = 0;

  virtual String_type &columns() = 0;

  virtual void set_columns(const String_type &) = 0;

  /**
    Converts *this into json.

    Converts all member variables that are to be included in the sdi
    into json by transforming them appropriately and passing them to
    the rapidjson writer provided.

    @param wctx opaque context for data needed by serialization
    @param w rapidjson writer which will perform conversion to json

  */

  virtual void serialize(Sdi_wcontext *wctx, Sdi_writer *w) const = 0;

  /**
    Re-establishes the state of *this by reading sdi information from
    the rapidjson DOM subobject provided.

    Cross-references encountered within this object are tracked in
    sdictx, so that they can be updated when the entire object graph
    has been established.

    @param rctx stores book-keeping information for the
    deserialization process
    @param val subobject of rapidjson DOM containing json
    representation of this object
    @retval false success
    @retval true  failure
  */

  virtual bool deserialize(Sdi_rcontext *rctx, const RJ_Value &val) = 0;

};  // MATERIALIZED VIEW
}  // namespace dd
#endif
