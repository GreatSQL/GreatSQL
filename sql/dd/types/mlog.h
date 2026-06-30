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

#ifndef DD__MATERIALIZED_VIEW_LOG_INCLUDED
#define DD__MATERIALIZED_VIEW_LOG_INCLUDED

#include "my_time_t.h"
#include "sql/dd/impl/tables/mlog.h"     // dd::M
#include "sql/dd/types/entity_object.h"  // dd::Entity_object

namespace dd {

class Materialized_view_log_impl;

class Materialized_view_log : virtual public Entity_object {
 public:
  typedef tables::Materialized_view_logs DD_table;
  typedef Materialized_view_log_impl Impl;  // register_tables
  ~Materialized_view_log() override = default;

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

  virtual const String_type &referenced_table_catalog_name() const = 0;
  virtual void set_referenced_table_catalog_name(const String_type &name) = 0;

  /////////////////////////////////////////////////////////////////////////
  // the schema name of the referenced table.
  /////////////////////////////////////////////////////////////////////////

  virtual const String_type &referenced_table_schema_name() const = 0;
  virtual void set_referenced_table_schema_name(const String_type &name) = 0;

  /////////////////////////////////////////////////////////////////////////
  // the name of the referenced table.
  /////////////////////////////////////////////////////////////////////////

  virtual const String_type &referenced_table_name() const = 0;
  virtual void set_referenced_table_name(const String_type &name) = 0;

  virtual const String_type &referenced_trigger_name() const = 0;
  virtual void set_referenced_trigger_name(const String_type &name) = 0;

  /////////////////////////////////////////////////////////////////////////

  virtual int status() = 0;

  virtual void set_status(int statue_arg) = 0;

  virtual my_timeval last_purged() = 0;

  virtual void set_last_purged(my_timeval update_time) = 0;

  virtual uint64_t ref() = 0;
  virtual void set_ref(uint64_t ref_arg) = 0;

  /////////////////////////////////////////////////////////////////////////
  // Options.
  /////////////////////////////////////////////////////////////////////////

  virtual const Properties &options() const = 0;

  virtual Properties &options() = 0;

  virtual bool set_options(const String_type &) = 0;

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

};  // MATERIALIZED VIEW LOG
}  // namespace dd
#endif
