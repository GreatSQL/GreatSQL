/* Copyright (c) 2016, 2022, Oracle and/or its affiliates. All rights reserved.
   Copyright (c) 2023, 2024, GreatDB Software Co., Ltd.

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
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

#include "sql/dd/dd_routine.h"  // Routine methods

#include <stddef.h>
#include <string.h>
#include <sys/types.h>
#include <memory>

#include "include/scope_guard.h"
#include "lex_string.h"
#include "my_dbug.h"
#include "my_inttypes.h"
#include "my_time.h"
#include "sql/dd/cache/dictionary_client.h"  // dd::cache::Dictionary_client
#include "sql/dd/dd_table.h"                 // dd::get_new_field_type
#include "sql/dd/impl/utils.h"               // dd::my_time_t_to_ull_datetime
#include "sql/dd/properties.h"               // dd::Properties
#include "sql/dd/string_type.h"
#include "sql/dd/types/function.h"                // dd::Function
#include "sql/dd/types/oracle_type.h"             // dd::Oracle_type
#include "sql/dd/types/package_body.h"            // dd::Package_body
#include "sql/dd/types/package_spec.h"            // dd::Package_spec
#include "sql/dd/types/parameter.h"               // dd::Parameter
#include "sql/dd/types/parameter_type_element.h"  // dd::Parameter_type_element
#include "sql/dd/types/procedure.h"               // dd::Procedure
#include "sql/dd/types/routine.h"
#include "sql/dd/types/schema.h"  // dd::Schema
#include "sql/dd/types/view.h"
#include "sql/dd_table_share.h"
#include "sql/field.h"
#include "sql/histograms/value_map.h"
#include "sql/item_create.h"
#include "sql/key.h"
#include "sql/sp.h"
#include "sql/sp_head.h"      // sp_head
#include "sql/sp_pcontext.h"  // sp_variable
#include "sql/sql_class.h"
#include "sql/sql_connect.h"
#include "sql/sql_db.h"  // get_default_db_collation
#include "sql/sql_lex.h"
#include "sql/sql_tmp_table.h"
#include "sql/system_variables.h"
#include "sql/table.h"
#include "sql/tztime.h"  // Time_zone
#include "sql_string.h"
#include "typelib.h"

namespace dd {

////////////////////////////////////////////////////////////////////////////////

/**
  Helper method for create_routine() to fill return type information of stored
  routine from the sp_head.
  from the sp_head.

  @param[in]  thd        Thread handle.
  @param[in]  sp         Stored routine object.
  @param[out] sf         dd::Function object.
*/

static void fill_dd_function_return_type(THD *thd, sp_head *sp, Function *sf) {
  DBUG_TRACE;

  Create_field *return_field = &sp->m_return_field_def;
  assert(return_field != nullptr);

  // Set result data type.
  sf->set_result_data_type(get_new_field_type(return_field->sql_type));

  // We need a fake table and share to generate a utf8 string
  // representation of result data type.
  TABLE table;
  TABLE_SHARE share;
  table.s = &share;
  table.in_use = thd;
  table.s->db_low_byte_first = true;

  // Reset result data type in utf8
  sf->set_result_data_type_utf8(
      get_sql_type_by_create_field(&table, *return_field));

  // Set result is_zerofill flag.
  sf->set_result_zerofill(return_field->is_zerofill);

  // Set result is_unsigned flag.
  sf->set_result_unsigned(return_field->is_unsigned);

  /*
    set result char length.
    Note that setting this only affects information schema views, and not any
    actual definitions. When initializing functions/routines, length information
    is read from dd::Parameter and not this field.
  */
  sf->set_result_char_length(return_field->max_display_width_in_bytes());

  // Set result numeric precision.
  uint numeric_precision = 0;
  if (get_field_numeric_precision(return_field, &numeric_precision) == false)
    sf->set_result_numeric_precision(numeric_precision);

  // Set result numeric scale.
  uint scale = 0;
  if (get_field_numeric_scale(return_field, &scale) == false ||
      numeric_precision)
    sf->set_result_numeric_scale(scale);
  else
    assert(sf->is_result_numeric_scale_null());

  uint dt_precision = 0;
  if (get_field_datetime_precision(return_field, &dt_precision) == false)
    sf->set_result_datetime_precision(dt_precision);

  // Set result collation id.
  sf->set_result_collation_id(return_field->charset->number);
}

////////////////////////////////////////////////////////////////////////////////

/**
  Helper method for create_routine() to fill parameter information
  from the object of type Create_field.
  Method is called by the fill_routine_parameters_info().

  @param[in]  thd      Thread handle.
  @param[in]  field    Object of type Create_field.
  @param[out] param    Parameter object to be filled using the state of field
                       object.
*/

static void fill_parameter_info_from_field(THD *thd, Create_field *field,
                                           dd::Parameter *param) {
  DBUG_TRACE;

  // Set data type.
  param->set_data_type(get_new_field_type(field->sql_type));

  // We need a fake table and share to generate the default values.
  // We prepare these once, and reuse them for all fields.
  TABLE table;
  TABLE_SHARE share;
  table.s = &share;
  table.in_use = thd;
  table.s->db_low_byte_first = true;

  // Reset data type in utf8
  param->set_data_type_utf8(get_sql_type_by_create_field(&table, *field));

  // Set is_zerofill flag.
  param->set_zerofill(field->is_zerofill);

  // Set is_unsigned flag.
  param->set_unsigned(field->is_unsigned);

  // Set char length.
  param->set_char_length(field->max_display_width_in_bytes());

  // Set result numeric precision.
  uint numeric_precision = 0;
  if (get_field_numeric_precision(field, &numeric_precision) == false)
    param->set_numeric_precision(numeric_precision);

  // Set numeric scale.
  uint scale = 0;
  if (!get_field_numeric_scale(field, &scale))
    param->set_numeric_scale(scale);
  else
    assert(param->is_numeric_scale_null());

  uint dt_precision = 0;
  if (get_field_datetime_precision(field, &dt_precision) == false)
    param->set_datetime_precision(dt_precision);

  // Set geometry sub type
  if (field->sql_type == MYSQL_TYPE_GEOMETRY) {
    Properties *param_options = &param->options();
    param_options->set("geom_type", field->geom_type);
  }

  // Set sys_refcursor type
  if (field->ora_record.is_ref_cursor) {
    Properties *param_options = &param->options();
    param_options->set("sys_refcursor", 1);
  }

  // Set elements of enum or set data type.
  if (field->interval) {
    assert(field->sql_type == MYSQL_TYPE_ENUM ||
           field->sql_type == MYSQL_TYPE_SET);

    const char **pos = field->interval->type_names;
    for (uint i = 0; *pos != nullptr; pos++, i++) {
      // Create enum/set object.
      Parameter_type_element *elem_obj = param->add_element();

      String_type interval_name(*pos, field->interval->type_lengths[i]);

      elem_obj->set_name(interval_name);
    }
  }

  // Set collation id.
  param->set_collation_id(field->charset->number);
  if (field->udt_name.length != 0) {
    Properties *param_options = &param->options();
    String_type udt_name(field->udt_name.str, field->udt_name.length);
    String_type udt_db_name(field->udt_db_name.str, field->udt_db_name.length);
    param_options->set("udt_name", udt_name);
    param_options->set("udt_db_name", udt_db_name);
  }
}

////////////////////////////////////////////////////////////////////////////////

/**
  Helper method for create_routine() to fill parameters of routine to
  dd::Routine object from the sp_head.
  Method is called from the fill_dd_routine_info().

  @param[in]  thd        Thread handle.
  @param[in]  sp         Stored routine object.
  @param[out] routine    dd::Routine object prepared from sp_head.

  @retval false  ON SUCCESS
  @retval true   ON FAILURE
*/

static bool fill_routine_parameters_info(THD *thd, sp_head *sp,
                                         Routine *routine) {
  DBUG_TRACE;

  // Helper class which takes care of restoration of
  // THD::check_for_truncated_fields after it was temporarily changed to
  // CHECK_FIELD_WARN in order to prepare default values and freeing buffer
  // which is allocated for the same purpose.
  class Context_handler {
   private:
    THD *m_thd;
    uchar *m_buf;
    enum_check_fields m_check_for_truncated_fields;

   public:
    Context_handler(THD *thd, uchar *buf)
        : m_thd(thd),
          m_buf(buf),
          m_check_for_truncated_fields(m_thd->check_for_truncated_fields) {
      // Set to warn about wrong default values.
      m_thd->check_for_truncated_fields = CHECK_FIELD_WARN;
    }
    ~Context_handler() {
      // Delete buffer and restore context.
      my_free(m_buf);
      m_thd->check_for_truncated_fields = m_check_for_truncated_fields;
    }
  };

  /*
    The return type of the stored function is listed as first parameter from
    the Information_schema.parameters. Storing return type as first parameter
    for the stored functions.
  */
  if (sp->m_type == enum_sp_type::FUNCTION) {
    // Add parameter.
    dd::Parameter *param = routine->add_parameter();

    // Fill return type information.
    fill_parameter_info_from_field(thd, &sp->m_return_field_def, param);
  }

  // Fill parameter information of the stored routine.
  sp_pcontext *sp_root_parsing_ctx = sp->get_root_parsing_context();
  assert(sp_root_parsing_ctx != nullptr);

  // check routine default parameter, ref prepare_default_value()
  TABLE table;
  TABLE_SHARE share;
  table.s = &share;
  table.in_use = thd;
  table.s->db_low_byte_first = false;

  size_t buf_size = 2;

  // find biggest required buf_size
  for (uint i = 0; i < sp_root_parsing_ctx->context_var_count(); ++i) {
    sp_variable *sp_var = sp_root_parsing_ctx->find_variable(i);
    Create_field *field_def = &sp_var->field_def;

    buf_size = std::max<size_t>(field_def->pack_length() + 1, buf_size);
  }

  uchar *buf = reinterpret_cast<uchar *>(
      my_malloc(key_memory_DD_default_values, buf_size, MYF(MY_WME)));
  if (buf == nullptr) return true;
  // use RAII to save old context and restore at function return
  Context_handler save_and_restore(thd, buf);

  for (uint i = 0; i < sp_root_parsing_ctx->context_var_count(); i++) {
    sp_variable *sp_var = sp_root_parsing_ctx->find_variable(i);
    Create_field *field_def = &sp_var->field_def;
    Item *d = sp_var->default_value;

    if ((thd->variables.sql_mode & MODE_ORACLE) && d) {
      memset(buf, 0, buf_size);

      // create a fake field with data buffer to store value
      Field *f = make_field(*field_def, table.s, buf + 1, buf, 0U);

      if (f == nullptr) return true;
      f->init(&table);
      if (!(field_def->flags & NOT_NULL_FLAG)) f->set_null();

      type_conversion_status res = d->save_in_field(f, true);
      if (f->is_flag_set(BLOB_FLAG)) {
        Field_blob *field_blob = dynamic_cast<Field_blob *>(f);
        if (field_blob) field_blob->mem_free();
      }

      if (res != TYPE_OK && res != TYPE_NOTE_TIME_TRUNCATED &&
          res != TYPE_NOTE_TRUNCATED) {
        // clear current error and report ER_INVALID_DEFAULT
        if (thd->is_error()) thd->clear_error();
        my_error(ER_SP_INVALID_VALUE_DEFAULT_PARAM, MYF(0), sp_var->name.str);

        return true;
      }
    }

    // Add parameter.
    dd::Parameter *param = routine->add_parameter();

    // Set parameter name.
    param->set_name(sp_var->name.str);

    // Set parameter mode.
    Parameter::enum_parameter_mode mode;
    switch (sp_var->mode) {
      case sp_variable::MODE_IN:
        mode = Parameter::PM_IN;
        break;
      case sp_variable::MODE_OUT:
        mode = Parameter::PM_OUT;
        break;
      case sp_variable::MODE_INOUT:
        mode = Parameter::PM_INOUT;
        break;
      default:
        assert(false); /* purecov: deadcode */
        return true;   /* purecov: deadcode */
    }
    param->set_mode(mode);

    // Set parameter default value.
    if ((thd->variables.sql_mode & MODE_ORACLE) && sp_var->default_value) {
      Properties *param_options = &param->options();
      param_options->set("default_value",
                         sp_var->default_value->orig_name.ptr());
    }
    // Fill return type information.
    fill_parameter_info_from_field(thd, field_def, param);
  }

  return false;
}

////////////////////////////////////////////////////////////////////////////////

/**
  Helper method for create_routine() to prepare dd::Routine object
  from the sp_head.

  @param[in]  thd        Thread handle.
  @param[in]  schema     Schema where the routine is to be created.
  @param[in]  sp         Stored routine object.
  @param[in]  definer    Definer of the routine.
  @param[out] routine    dd::Routine object to be prepared from the sp_head.

  @retval false  ON SUCCESS
  @retval true   ON FAILURE
*/

static bool fill_dd_routine_info(THD *thd, const dd::Schema &schema,
                                 sp_head *sp, Routine *routine,
                                 const LEX_USER *definer) {
  DBUG_TRACE;

  // Set name.
  routine->set_name(sp->m_name.str);

  // Set definition.
  routine->set_definition(sp->m_body.str);

  // Set definition_utf8.
  routine->set_definition_utf8(sp->m_body_utf8.str);

  // Set parameter str for show routine operations.
  routine->set_parameter_str(sp->m_params.str);

  // Set is_deterministic.
  routine->set_deterministic(sp->m_chistics->detistic);

  // Set SQL data access.
  Routine::enum_sql_data_access daccess;
  enum_sp_data_access sp_daccess =
      (sp->m_chistics->daccess == SP_DEFAULT_ACCESS) ? SP_DEFAULT_ACCESS_MAPPING
                                                     : sp->m_chistics->daccess;
  switch (sp_daccess) {
    case SP_NO_SQL:
      daccess = Routine::SDA_NO_SQL;
      break;
    case SP_CONTAINS_SQL:
      daccess = Routine::SDA_CONTAINS_SQL;
      break;
    case SP_READS_SQL_DATA:
      daccess = Routine::SDA_READS_SQL_DATA;
      break;
    case SP_MODIFIES_SQL_DATA:
      daccess = Routine::SDA_MODIFIES_SQL_DATA;
      break;
    default:
      assert(false); /* purecov: deadcode */
      return true;   /* purecov: deadcode */
  }
  routine->set_sql_data_access(daccess);

  // Set security type.
  View::enum_security_type sec_type;
  enum_sp_suid_behaviour sp_suid = (sp->m_chistics->suid == SP_IS_DEFAULT_SUID)
                                       ? SP_DEFAULT_SUID_MAPPING
                                       : sp->m_chistics->suid;
  switch (sp_suid) {
    case SP_IS_SUID:
      sec_type = View::ST_DEFINER;
      break;
    case SP_IS_NOT_SUID:
      sec_type = View::ST_INVOKER;
      break;
    default:
      assert(false); /* purecov: deadcode */
      return true;   /* purecov: deadcode */
  }
  routine->set_security_type(sec_type);

  // Set definer.
  routine->set_definer(definer->user.str, definer->host.str);

  // Set sql_mode.
  routine->set_sql_mode(thd->variables.sql_mode);

  // Set client collation id.
  routine->set_client_collation_id(thd->charset()->number);

  // Set connection collation id.
  routine->set_connection_collation_id(
      thd->variables.collation_connection->number);

  // Set schema collation id.
  const CHARSET_INFO *db_cs = nullptr;
  if (get_default_db_collation(schema, &db_cs)) {
    assert(thd->is_error());
    return true;
  }
  if (db_cs == nullptr) db_cs = thd->collation();

  routine->set_schema_collation_id(db_cs->number);

  // Set comment.
  routine->set_comment(sp->m_chistics->comment.str ? sp->m_chistics->comment.str
                                                   : "");
  // Fill routine parameters
  if (fill_routine_parameters_info(thd, sp, routine)) return true;

  // Set options.
  if (sp->get_ora_type()) {
    Properties *options = &routine->options();
    options->set("ref_count", 0);
    ulong reclength = 0;
    if (sp->type != enum_udt_table_of_type::TYPE_INVALID) {
      if (sp->nested_table_udt.str) {
        bool ret = alter_type_ref_count(
            thd, sp->m_db.str, sp->nested_table_udt.str, true, &reclength);
        if (ret || !reclength) return true;
        options->set("nested_table_udt",
                     (char const *)sp->nested_table_udt.str);
      }
      options->set("table_type", (uint)sp->type);
      options->set("varray_limit", sp->size_limit);
    }
    if (reclength) {
      options->set("udt_size", reclength);
      return false;
    }
    sp_pcontext *sp_root_parsing_ctx = sp->get_root_parsing_context();
    List<Create_field> field_def_lst;
    List<sp_variable> vars_list;
    uint udt_var_count_diff = 0;
    sp_root_parsing_ctx->retrieve_field_definitions(
        &field_def_lst, &vars_list, nullptr, &udt_var_count_diff);
    if (create_cfield_from_dd_columns(thd, sp->m_db.str, routine,
                                      &field_def_lst, &reclength))
      return true;
    options->set("udt_size", reclength);
  }
  return false;
}

////////////////////////////////////////////////////////////////////////////////

bool create_routine(THD *thd, const Schema &schema, sp_head *sp,
                    const LEX_USER *definer) {
  DBUG_TRACE;

  bool error = false;
  // Create Function or Procedure object.
  if (sp->m_type == enum_sp_type::FUNCTION) {
    std::unique_ptr<Function> func(schema.create_function(thd));

    // Fill stored function return type.
    fill_dd_function_return_type(thd, sp, func.get());

    // Fill routine object.
    if (fill_dd_routine_info(thd, schema, sp, func.get(), definer)) return true;

    // Store routine metadata in DD table.
    enum_check_fields saved_check_for_truncated_fields =
        thd->check_for_truncated_fields;
    thd->check_for_truncated_fields = CHECK_FIELD_WARN;
    error = thd->dd_client()->store(func.get());
    thd->check_for_truncated_fields = saved_check_for_truncated_fields;
  } else if (sp->m_type == enum_sp_type::PROCEDURE) {
    std::unique_ptr<Procedure> proc(schema.create_procedure(thd));

    // Fill routine object.
    if (fill_dd_routine_info(thd, schema, sp, proc.get(), definer)) return true;

    // Store routine metadata in DD table.
    enum_check_fields saved_check_for_truncated_fields =
        thd->check_for_truncated_fields;
    thd->check_for_truncated_fields = CHECK_FIELD_WARN;
    error = thd->dd_client()->store(proc.get());
    thd->check_for_truncated_fields = saved_check_for_truncated_fields;
  } else if (sp->m_type == enum_sp_type::PACKAGE_SPEC) {
    std::unique_ptr<Package_spec> pkg(schema.create_package_spec(thd));

    // Fill routine object.
    if (fill_dd_routine_info(thd, schema, sp, pkg.get(), definer)) return true;

    // Store routine metadata in DD table.
    enum_check_fields saved_check_for_truncated_fields =
        thd->check_for_truncated_fields;
    thd->check_for_truncated_fields = CHECK_FIELD_WARN;
    error = thd->dd_client()->store(pkg.get());
    thd->check_for_truncated_fields = saved_check_for_truncated_fields;
  } else if (sp->m_type == enum_sp_type::PACKAGE_BODY) {
    std::unique_ptr<Package_body> pkg(schema.create_package_body(thd));

    // Fill routine object.
    if (fill_dd_routine_info(thd, schema, sp, pkg.get(), definer)) return true;

    // Store routine metadata in DD table.
    enum_check_fields saved_check_for_truncated_fields =
        thd->check_for_truncated_fields;
    thd->check_for_truncated_fields = CHECK_FIELD_WARN;
    error = thd->dd_client()->store(pkg.get());
    thd->check_for_truncated_fields = saved_check_for_truncated_fields;
  } else if (sp->m_type == enum_sp_type::TYPE) {
    std::unique_ptr<Oracle_type> type(schema.create_ora_type(thd));
    // Fill routine object.
    if (fill_dd_routine_info(thd, schema, sp, type.get(), definer)) return true;

    // Store routine metadata in DD table.
    enum_check_fields saved_check_for_truncated_fields =
        thd->check_for_truncated_fields;
    thd->check_for_truncated_fields = CHECK_FIELD_WARN;
    error = thd->dd_client()->store(type.get());
    if (!error) error = update_udt_object_comment(thd, sp);
    thd->check_for_truncated_fields = saved_check_for_truncated_fields;
  } else {
    return true;
  }

  return error;
}

////////////////////////////////////////////////////////////////////////////////

bool alter_routine(THD *thd, Routine *routine, st_sp_chistics *chistics) {
  DBUG_TRACE;

  // Set last altered time.
  routine->set_last_altered(
      dd::my_time_t_to_ull_datetime(thd->query_start_in_secs()));

  // Set security type.
  if (chistics->suid != SP_IS_DEFAULT_SUID) {
    View::enum_security_type sec_type;

    switch (chistics->suid) {
      case SP_IS_SUID:
        sec_type = View::ST_DEFINER;
        break;
      case SP_IS_NOT_SUID:
        sec_type = View::ST_INVOKER;
        break;
      default:
        assert(false); /* purecov: deadcode */
        return true;   /* purecov: deadcode */
    }

    routine->set_security_type(sec_type);
  }

  // Set sql data access.
  if (chistics->daccess != SP_DEFAULT_ACCESS) {
    Routine::enum_sql_data_access daccess;
    switch (chistics->daccess) {
      case SP_NO_SQL:
        daccess = Routine::SDA_NO_SQL;
        break;
      case SP_CONTAINS_SQL:
        daccess = Routine::SDA_CONTAINS_SQL;
        break;
      case SP_READS_SQL_DATA:
        daccess = Routine::SDA_READS_SQL_DATA;
        break;
      case SP_MODIFIES_SQL_DATA:
        daccess = Routine::SDA_MODIFIES_SQL_DATA;
        break;
      default:
        assert(false); /* purecov: deadcode */
        return true;   /* purecov: deadcode */
    }
    routine->set_sql_data_access(daccess);
  }

  // Set comment.
  if (chistics->comment.str) routine->set_comment(chistics->comment.str);

  // Update routine.
  return thd->dd_client()->update(routine);
}

////////////////////////////////////////////////////////////////////////////////

bool alter_type_ref_count(THD *thd, const char *db, const char *type_name,
                          bool increase, ulong *reclength) {
  dd::cache::Dictionary_client::Auto_releaser releaser(thd->dd_client());
  MDL_key::enum_mdl_namespace mdl_type = MDL_key::TYPE;
  if (lock_object_name(thd, mdl_type, db, type_name)) {
    my_error(ER_SP_CANT_ALTER, MYF(0), "TYPE", type_name);
    return true;
  }
  dd::Routine *to_routine = nullptr;
  if (thd->dd_client()->acquire_for_modification<dd::Oracle_type>(
          db, type_name, &to_routine)) {
    my_error(ER_SP_DOES_NOT_EXIST, MYF(0), "TYPE", type_name);
    return true;
  }
  Properties *options = &to_routine->options();
  int table_count;
  options->get("ref_count", &table_count);
  if (increase)
    options->set("ref_count", table_count + 1);
  else
    options->set("ref_count", table_count - 1);
  options->get("ref_count", &table_count);
  assert(table_count >= 0);
  if (reclength) options->get("udt_size", reclength);
  // Set last altered time.
  to_routine->set_last_altered(
      dd::my_time_t_to_ull_datetime(thd->query_start_in_secs()));
  if (thd->dd_client()->update(to_routine)) return true;
  return false;
}

ulong get_udt_size_and_supporting_table(THD *thd, MEM_ROOT *mem_root,
                                        const char *db,
                                        const MYSQL_LEX_CSTRING ident,
                                        TABLE **m_type_table,
                                        const char *field_name) {
  auto guard = create_scope_guard(
      [thd, mem_root_tmp = thd->mem_root]() { thd->mem_root = mem_root_tmp; });
  thd->mem_root = mem_root;
  List<Create_field> *field_def_lst = nullptr;
  ulong reclength = 0;
  LEX_CSTRING nested_table_udt = NULL_CSTR;
  uint table_type = 255;
  ulonglong varray_limit = 0;
  bool ret = sp_find_ora_type_create_fields(
      thd, to_lex_string(to_lex_cstring(db)), to_lex_string(ident), false,
      &field_def_lst, &reclength, nullptr, &nested_table_udt, &table_type,
      &varray_limit);
  if (ret) {
    return 0;
  }
  if (table_type == 0 || table_type == 1) {
    my_error(ER_NOT_SUPPORTED_YET, MYF(0), "create table with udt table");
    return 0;
  }
  if (!(*m_type_table = create_tmp_table_from_fields(thd, *field_def_lst)))
    return 0;
  (*m_type_table)->alias = "";
  if ((*m_type_table)->s->reclength > MAX_FIELD_VARCHARLENGTH) {
    my_error(ER_TOO_BIG_FIELDLENGTH, MYF(0), field_name,
             static_cast<ulong>(MAX_FIELD_VARCHARLENGTH));
    free_blobs(*m_type_table);
    return 0;
  }
  return (*m_type_table)->s->reclength;
}
////////////////////////////////////////////////////////////////////////////////

bool type_is_referenced(const dd::Routine *routine) {
  const Properties *options = &routine->options();
  int table_count;
  options->get("ref_count", &table_count);
  if (table_count > 0) return true;
  return false;
}

////////////////////////////////////////////////////////////////////////////////
}  // namespace dd
