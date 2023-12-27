/* Copyright (c) 2023, GreatDB Software Co., Ltd. All rights reserved.

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

#include "storage/innobase/handler/i_s_file_purge.h"

#include <field.h>
#include <sql_show.h>

#include "fil0purge.h"
#include "mysql/plugin.h"
#include "sql/table.h"

static const char plugin_author[] = "GreatDB";

#define OK(expr)     \
  if ((expr) != 0) { \
    DBUG_RETURN(1);  \
  }

#if !defined __STRICT_ANSI__ && defined __GNUC__ && !defined __clang__
#define STRUCT_FLD(name, value) \
  name:                         \
  value
#else
#define STRUCT_FLD(name, value) value
#endif

/* Don't use a static const variable here, as some C++ compilers (notably
HPUX aCC: HP ANSI C++ B3910B A.03.65) can't handle it. */
#define END_OF_ST_FIELD_INFO                                           \
  {                                                                    \
    STRUCT_FLD(field_name, NULL), STRUCT_FLD(field_length, 0),         \
        STRUCT_FLD(field_type, MYSQL_TYPE_NULL), STRUCT_FLD(value, 0), \
        STRUCT_FLD(field_flags, 0), STRUCT_FLD(old_name, ""),          \
        STRUCT_FLD(open_method, 0)                                     \
  }

ST_FIELD_INFO innodb_purge_file_fields_info[] = {
#define IDX_LOG_ID 0
    {STRUCT_FLD(field_name, "log_id"),
     STRUCT_FLD(field_length, MY_INT64_NUM_DECIMAL_DIGITS),
     STRUCT_FLD(field_type, MYSQL_TYPE_LONGLONG), STRUCT_FLD(value, 0),
     STRUCT_FLD(field_flags, MY_I_S_UNSIGNED), STRUCT_FLD(old_name, ""),
     STRUCT_FLD(open_method, 0)},

#define IDX_PURGE_START 1
    {STRUCT_FLD(field_name, "start_time"), STRUCT_FLD(field_length, 0),
     STRUCT_FLD(field_type, MYSQL_TYPE_DATETIME), STRUCT_FLD(value, 0),
     STRUCT_FLD(field_flags, 0), STRUCT_FLD(old_name, ""),
     STRUCT_FLD(open_method, 0)},

#define IDX_ORIGINAL_PATH 2
    {STRUCT_FLD(field_name, "original_path"),
     STRUCT_FLD(field_length, PURGE_FILE_NAME_MAX_LEN + 1),
     STRUCT_FLD(field_type, MYSQL_TYPE_STRING), STRUCT_FLD(value, 0),
     STRUCT_FLD(field_flags, MY_I_S_MAYBE_NULL), STRUCT_FLD(old_name, ""),
     STRUCT_FLD(open_method, 0)},

#define IDX_ORIGNINAL_SIZE 3
    {STRUCT_FLD(field_name, "original_size"),
     STRUCT_FLD(field_length, MY_INT64_NUM_DECIMAL_DIGITS),
     STRUCT_FLD(field_type, MYSQL_TYPE_LONGLONG), STRUCT_FLD(value, 0),
     STRUCT_FLD(field_flags, MY_I_S_UNSIGNED), STRUCT_FLD(old_name, ""),
     STRUCT_FLD(open_method, 0)},

#define IDX_TEMPORARY_PATH 4
    {STRUCT_FLD(field_name, "temporary_path"),
     STRUCT_FLD(field_length, PURGE_FILE_NAME_MAX_LEN + 1),
     STRUCT_FLD(field_type, MYSQL_TYPE_STRING), STRUCT_FLD(value, 0),
     STRUCT_FLD(field_flags, MY_I_S_MAYBE_NULL), STRUCT_FLD(old_name, ""),
     STRUCT_FLD(open_method, 0)},

#define IDX_CURRENT_SIZE 5
    {STRUCT_FLD(field_name, "current_size"),
     STRUCT_FLD(field_length, MY_INT64_NUM_DECIMAL_DIGITS),
     STRUCT_FLD(field_type, MYSQL_TYPE_LONGLONG), STRUCT_FLD(value, 0),
     STRUCT_FLD(field_flags, MY_I_S_UNSIGNED), STRUCT_FLD(old_name, ""),
     STRUCT_FLD(open_method, 0)},

#define IDX_MESSAGE 6
    {STRUCT_FLD(field_name, "message"),
     STRUCT_FLD(field_length, PURGE_FILE_MESSAGE_MAX_LEN + 1),
     STRUCT_FLD(field_type, MYSQL_TYPE_STRING), STRUCT_FLD(value, 0),
     STRUCT_FLD(field_flags, MY_I_S_MAYBE_NULL), STRUCT_FLD(old_name, ""),
     STRUCT_FLD(open_method, 0)},

    END_OF_ST_FIELD_INFO};

/** Fill the purging file
 INFORMATION_SCHEMA.innodb_async_purge_files
 @return 0 on SUCCESS */

static int innodb_purge_file_fill_table(
    THD *thd,          /*!< in: thread */
    Table_ref *tables, /*!< in/out: tables to fill */
    Item *)            /*!< in: condition (not used) */
{
  Field **fields;
  TABLE *table;
  file_purge_node_t *node = nullptr;
  bool first = true;
  DBUG_ENTER("innodb_purge_file_fill_table");

  table = tables->table;
  fields = table->field;

  if (file_purge_sys) {
    file_purge_sys->lock();

    while ((node = file_purge_sys->iterate_node(first, node))) {
      first = false;

      /* log_id */
      OK(fields[IDX_LOG_ID]->store(node->m_log_ddl_id, true));

      /* start_time */
      OK(field_store_time_t(fields[IDX_PURGE_START], node->m_start_time));

      /* original_path */
      OK(field_store_string(fields[IDX_ORIGINAL_PATH], node->m_original_path));

      /* original_size */
      OK(fields[IDX_ORIGNINAL_SIZE]->store(node->m_original_size, true));

      /* temporary_path */
      OK(field_store_string(fields[IDX_TEMPORARY_PATH], node->m_file_path));

      /* current_size */
      OK(fields[IDX_CURRENT_SIZE]->store(node->m_current_size, true));

      /* message */
      if (!node->m_message.empty()) {
        OK(field_store_string(fields[IDX_MESSAGE], node->m_message.c_str()));
      } else {
        fields[IDX_MESSAGE]->set_null();
      }

      OK(schema_table_store_record(thd, table));
    }

    file_purge_sys->unlock();
  }

  DBUG_RETURN(0);
}

/** Bind the dynamic table INFORMATION_SCHEMA.innodb_async_purge_files
 @return 0 on success */
static int innodb_purge_file_init(void *p) {
  ST_SCHEMA_TABLE *schema;
  DBUG_ENTER("innodb_purge_file_init");

  schema = (ST_SCHEMA_TABLE *)p;

  schema->fields_info = innodb_purge_file_fields_info;
  schema->fill_table = innodb_purge_file_fill_table;

  DBUG_RETURN(0);
}

/** Unbind a dynamic INFORMATION_SCHEMA table.
 @return 0 on success */
static int i_s_common_deinit(void *p) /*!< in/out: table schema object */
{
  DBUG_ENTER("i_s_common_deinit");

  /* Do nothing */

  DBUG_RETURN(0);
}

static struct st_mysql_information_schema i_s_info = {
    MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};

constexpr uint64_t i_s_innodb_plugin_version_for_purge_file =
    (INNODB_VERSION_MAJOR << 8 | i_s_innodb_plugin_version_postfix);

struct st_mysql_plugin i_s_innodb_data_file_async_purge = {
    /* the plugin type (a MYSQL_XXX_PLUGIN value) */
    /* int */
    STRUCT_FLD(type, MYSQL_INFORMATION_SCHEMA_PLUGIN),

    /* pointer to type-specific plugin descriptor */
    /* void* */
    STRUCT_FLD(info, &i_s_info),

    /* plugin name */
    STRUCT_FLD(name, "INNODB_ASYNC_PURGE_FILES"),

    /* plugin author (for SHOW PLUGINS) */
    /* const char* */
    STRUCT_FLD(author, plugin_author),

    /* general descriptive text (for SHOW PLUGINS) */
    /* const char* */
    STRUCT_FLD(descr, "InnoDB purging data file list"),

    /* the plugin license (PLUGIN_LICENSE_XXX) */
    /* int */
    STRUCT_FLD(license, PLUGIN_LICENSE_GPL),

    /* the function to invoke when plugin is loaded */
    /* int (*)(void*); */
    STRUCT_FLD(init, innodb_purge_file_init),

    /* the function to invoke when plugin is un installed */
    /* int (*)(void*); */
    NULL,

    /* the function to invoke when plugin is unloaded */
    /* int (*)(void*); */
    STRUCT_FLD(deinit, i_s_common_deinit),

    /* plugin version (for SHOW PLUGINS) */
    /* unsigned int */
    STRUCT_FLD(version, i_s_innodb_plugin_version_for_purge_file),

    /* SHOW_VAR* */
    STRUCT_FLD(status_vars, NULL),

    /* SYS_VAR** */
    STRUCT_FLD(system_vars, NULL),

    /* reserved for dependency checking */
    /* void* */
    STRUCT_FLD(__reserved1, NULL),

    /* Plugin flags */
    /* unsigned long */
    STRUCT_FLD(flags, 0UL),
};
