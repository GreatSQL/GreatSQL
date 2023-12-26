/* Copyright (c) 2004, 2022, Oracle and/or its affiliates.
   Copyright (c) 2023, GreatDB Software Co., Ltd.

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

/*
  Make sure to look at ha_dlk.h for more details.

  First off, this is a play thing for me, there are a number of things
  wrong with it:
    *) It was designed for dlk and therefore its performance is highly
       questionable.
    *) Indexes have not been implemented. This is because the files can
       be traded in and out of the table directory without having to worry
       about rebuilding anything.
    *) NULLs and "" are treated equally (like a spreadsheet).
    *) There was in the beginning no point to anyone seeing this other
       then me, so there is a good chance that I haven't quite documented
       it well.
    *) Less design, more "make it work"

  Now there are a few cool things with it:
    *) Errors can result in corrupted data files.
    *) Data files can be read by spreadsheets directly.

TODO:
 *) Move to a block system for larger files
 *) Error recovery, its all there, just need to finish it
 *) Document how the chains work.

 -Brian
*/

#include "storage/dlk/ha_dlk.h"

#include <fcntl.h>
#include <mysql/plugin.h>
#include <mysql/psi/mysql_file.h>
#include <algorithm>

#include "map_helpers.h"
#include "my_byteorder.h"
#include "my_dbug.h"
#include "my_psi_config.h"
#include "mysql/plugin.h"
#include "mysql/psi/mysql_memory.h"
#include "sql/derror.h"
#include "sql/field.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"
#include "sql/system_variables.h"
#include "sql/table.h"
#include "template_utils.h"

using std::max;
using std::min;
using std::string;
using std::unique_ptr;
using namespace dlk;
/*
  uchar + uchar + ulonglong + ulonglong + ulonglong + ulonglong + uchar
*/
#define META_BUFFER_SIZE                                                  \
  sizeof(uchar) + sizeof(uchar) + sizeof(ulonglong) + sizeof(ulonglong) + \
      sizeof(ulonglong) + sizeof(ulonglong) + sizeof(uchar)
#define DLK_CHECK_HEADER 254  // The number we use to determine corruption
#define BLOB_MEMROOT_ALLOC_SIZE 8192

/* The file extension */
#define CSV_EXT ".CSV"  // The data file
#define CSN_EXT ".CSN"  // Files used during repair and update
#define CSM_EXT ".CSM"  // Meta file

static DLK_SHARE *get_share(const char *table_name, TABLE *table);
static int free_share(DLK_SHARE *share);

/* Stuff for shares */
mysql_mutex_t dlk_mutex;
static unique_ptr<collation_unordered_multimap<string, DLK_SHARE *>>
    dlk_open_tables;
static handler *dlk_create_handler(handlerton *hton, TABLE_SHARE *table,
                                   bool partitioned, MEM_ROOT *mem_root);

/* Interface to mysqld, to check system tables supported by SE */
static bool dlk_is_supported_system_table(const char *db,
                                          const char *table_name,
                                          bool is_sql_layer_system_table);

/*****************************************************************************
 ** TINA tables
 *****************************************************************************/

static PSI_memory_key dlk_key_memory_dlk_share;
static PSI_memory_key dlk_key_memory_blobroot;
static PSI_memory_key dlk_key_memory_dlk_set;
static PSI_memory_key dlk_key_memory_row;

#ifdef HAVE_PSI_INTERFACE

static PSI_mutex_key dlk_key_mutex_dlk, dlk_key_mutex_DLK_SHARE_mutex;

static PSI_mutex_info all_dlk_mutexes[] = {
    {&dlk_key_mutex_dlk, "dlk", PSI_FLAG_SINGLETON, 0, PSI_DOCUMENT_ME},
    {&dlk_key_mutex_DLK_SHARE_mutex, "DLK_SHARE::mutex", 0, 0,
     PSI_DOCUMENT_ME}};

static PSI_file_key dlk_key_file_metadata, dlk_key_file_data,
    dlk_key_file_update;

static PSI_file_info all_dlk_files[] = {
    {&dlk_key_file_metadata, "metadata", 0, 0, PSI_DOCUMENT_ME},
    {&dlk_key_file_data, "data", 0, 0, PSI_DOCUMENT_ME},
    {&dlk_key_file_update, "update", 0, 0, PSI_DOCUMENT_ME}};

static PSI_memory_info all_dlk_memory[] = {
    {&dlk_key_memory_dlk_share, "DLK_SHARE", PSI_FLAG_ONLY_GLOBAL_STAT, 0,
     PSI_DOCUMENT_ME},
    {&dlk_key_memory_blobroot, "blobroot", 0, 0, PSI_DOCUMENT_ME},
    {&dlk_key_memory_dlk_set, "dlk_set", 0, 0, PSI_DOCUMENT_ME},
    {&dlk_key_memory_row, "row", 0, 0, PSI_DOCUMENT_ME},
    {&dlk_key_memory_Transparent_file, "Transparent_file", 0, 0,
     PSI_DOCUMENT_ME}};

static void init_dlk_psi_keys(void) {
  const char *category = "dlk";
  int count;

  count = static_cast<int>(array_elements(all_dlk_mutexes));
  mysql_mutex_register(category, all_dlk_mutexes, count);

  count = static_cast<int>(array_elements(all_dlk_files));
  mysql_file_register(category, all_dlk_files, count);

  count = static_cast<int>(array_elements(all_dlk_memory));
  mysql_memory_register(category, all_dlk_memory, count);
}
#endif /* HAVE_PSI_INTERFACE */

static int dlk_init_func(void *p) {
  handlerton *dlk_hton;

#ifdef HAVE_PSI_INTERFACE
  init_dlk_psi_keys();
#endif

  dlk_hton = (handlerton *)p;
  mysql_mutex_init(dlk_key_mutex_dlk, &dlk_mutex, MY_MUTEX_INIT_FAST);
  dlk_open_tables.reset(new collation_unordered_multimap<string, DLK_SHARE *>(
      system_charset_info, dlk_key_memory_dlk_share));
  dlk_hton->state = SHOW_OPTION_YES;
  dlk_hton->db_type = DB_TYPE_DLK_DB;
  dlk_hton->create = dlk_create_handler;
  dlk_hton->flags =
      (HTON_ALTER_NOT_SUPPORTED | HTON_SUPPORT_LOG_TABLES | HTON_NO_PARTITION);
  dlk_hton->is_supported_system_table = dlk_is_supported_system_table;
  return 0;
}

static int dlk_done_func(void *) {
  dlk_open_tables.reset();
  mysql_mutex_destroy(&dlk_mutex);

  return 0;
}

/*
  Simple lock controls.
*/
static DLK_SHARE *get_share(const char *table_name, TABLE *table) {
  DLK_SHARE *share;
  char *tmp_name;
  uint length;

  mysql_mutex_lock(&dlk_mutex);
  length = (uint)strlen(table_name);

  /*
    If share is not present in the hash, create a new share and
    initialize its members.
  */
  const auto it = dlk_open_tables->find(table_name);
  if (it == dlk_open_tables->end()) {
    if (!my_multi_malloc(dlk_key_memory_dlk_share, MYF(MY_WME | MY_ZEROFILL),
                         &share, sizeof(*share), &tmp_name, length + 1,
                         NullS)) {
      mysql_mutex_unlock(&dlk_mutex);
      return nullptr;
    }

    share->use_count = 0;
    share->table_name_length = length;
    share->table_name = tmp_name;
    share->crashed = false;
    my_stpcpy(share->table_name, table_name);
    if (!table->s->external_data_dir) {
      auto dir_len = dirname_length(share->table_name);
      char file_name[FN_REFLEN] = {0};
      strncpy(file_name, share->table_name, dir_len);
      fn_format(share->data_file_name, table->s->external_file_name.str,
                file_name, "", MY_UNPACK_FILENAME);
    } else {
      my_stpncpy(share->data_file_name, table->s->external_file_name.str,
                 table->s->external_file_name.length);
    }
    dlk_open_tables->emplace(table_name, share);
    thr_lock_init(&share->lock);
    mysql_mutex_init(dlk_key_mutex_DLK_SHARE_mutex, &share->mutex,
                     MY_MUTEX_INIT_FAST);
  } else {
    share = it->second;
  }

  share->use_count++;
  mysql_mutex_unlock(&dlk_mutex);

  return share;
}

/*
  Free lock controls.
*/
static int free_share(DLK_SHARE *share) {
  DBUG_TRACE;
  mysql_mutex_lock(&dlk_mutex);
  int result_code = 0;
  if (!--share->use_count) {
    dlk_open_tables->erase(share->table_name);
    thr_lock_delete(&share->lock);
    mysql_mutex_destroy(&share->mutex);
    my_free(share);
  }
  mysql_mutex_unlock(&dlk_mutex);

  return result_code;
}

/*
  This function finds the end of a line and returns the length
  of the line ending.

  We support three kinds of line endings:
  '\r'     --  Old Mac OS line ending
  '\n'     --  Traditional Unix and Mac OS X line ending
  '\r''\n' --  DOS\Windows line ending
*/

static my_off_t find_eoln_buff(Transparent_file *data_buff, my_off_t begin,
                               my_off_t end, int *eoln_len) {
  *eoln_len = 0;

  for (my_off_t x = begin; x < end; x++) {
    /* Unix (includes Mac OS X) */
    if (data_buff->get_value(x) == '\n')
      *eoln_len = 1;
    else if (data_buff->get_value(x) == '\r')  // Mac or Dos
    {
      /* old Mac line ending */
      if (x + 1 == end || (data_buff->get_value(x + 1) != '\n'))
        *eoln_len = 1;
      else  // DOS style ending
        *eoln_len = 2;
    }

    if (*eoln_len)  // end of line was found
      return x;
  }

  return 0;
}

static handler *dlk_create_handler(handlerton *hton, TABLE_SHARE *table, bool,
                                   MEM_ROOT *mem_root) {
  return new (mem_root) ha_dlk(hton, table);
}

/**
  @brief Check if the given db.tablename is a system table for this SE.

  @param db                         database name.
  @param table_name                 table name to check.
  @param is_sql_layer_system_table  if the supplied db.table_name is a SQL
                                    layer system table.

  @retval true   Given db.table_name is supported system table.
  @retval false  Given db.table_name is not a supported system table.
*/
static bool dlk_is_supported_system_table(const char *db,
                                          const char *table_name,
                                          bool is_sql_layer_system_table) {
  /*
   Currently dlk does not support any other SE specific system tables. If
   in future it does, please see ha_example.cc for reference implementation
  */
  if (!is_sql_layer_system_table) return false;

  // Creating system tables in this SE is allowed to support logical upgrade.
  THD *thd = current_thd;
  if (thd->lex->sql_command == SQLCOM_CREATE_TABLE) {
    push_warning_printf(thd, Sql_condition::SL_WARNING, ER_UNSUPPORTED_ENGINE,
                        ER_THD(thd, ER_UNSUPPORTED_ENGINE), "dlk", db,
                        table_name);
    return true;
  }

  // Any other usage is not allowed.
  return false;
}

ha_dlk::ha_dlk(handlerton *hton, TABLE_SHARE *table_arg)
    : handler(hton, table_arg),
      current_position(0),
      next_position(0),
      local_saved_data_file_length(0),
      file_buff(nullptr),
      data_file(-1),
      records_is_known(false),
      blobroot(dlk_key_memory_blobroot, BLOB_MEMROOT_ALLOC_SIZE) {
  /* Set our original buffers from pre-allocated memory */
  buffer.set((char *)byte_buffer, IO_SIZE, &my_charset_bin);
  file_buff = new Transparent_file();
}

/*
  Scans for a row.
*/
int ha_dlk::find_current_row(uchar *buf) {
  my_off_t end_offset, curr_offset = current_position;
  int eoln_len;
  my_bitmap_map *org_bitmap;
  int error;
  bool read_all;
  DBUG_TRACE;

  // Clear BLOB data from the previous row.
  blobroot.ClearForReuse();

  /*
    We do not read further then local_saved_data_file_length in order
    not to conflict with undergoing concurrent insert.
  */
  if ((end_offset = find_eoln_buff(file_buff, current_position,
                                   local_saved_data_file_length, &eoln_len)) ==
      0)
    return HA_ERR_END_OF_FILE;

  /* We must read all columns in case a table is opened for update */
  read_all = !bitmap_is_clear_all(table->write_set);
  /* Avoid asserts in ::store() for columns that are not going to be updated */
  org_bitmap = dbug_tmp_use_all_columns(table, table->write_set);
  error = HA_ERR_CRASHED_ON_USAGE;

  memset(buf, 0, table->s->null_bytes);

  /*
    Parse the line obtained using the following algorithm

    BEGIN
      1) Store the EOL (end of line) for the current row
      2) Until all the fields in the current query have not been
         filled
         2.1) If the current character is a quote
              2.1.1) Until EOL has not been reached
                     a) If end of current field is reached, move
                        to next field and jump to step 2.3
                     b) If current character is a \\ handle
                        \\n, \\r, \\, \\"
                     c) else append the current character into the buffer
                        before checking that EOL has not been reached.
          2.2) If the current character does not begin with a quote
               2.2.1) Until EOL has not been reached
                      a) If the end of field has been reached move to the
                         next field and jump to step 2.3
                      b) If current character begins with \\ handle
                        \\n, \\r, \\, \\"
                      c) else append the current character into the buffer
                         before checking that EOL has not been reached.
          2.3) Store the current field value and jump to 2)
    TERMINATE
  */

  for (Field **field = table->field; *field; field++) {
    char curr_char;

    buffer.length(0);
    if (curr_offset >= end_offset) goto err;
    curr_char = file_buff->get_value(curr_offset);
    /* Handle the case where the first character is a quote */
    if (curr_char == '"') {
      /* Increment past the first quote */
      curr_offset++;

      /* Loop through the row to extract the values for the current field */
      for (; curr_offset < end_offset; curr_offset++) {
        curr_char = file_buff->get_value(curr_offset);
        /* check for end of the current field */
        if (curr_char == '"' &&
            (curr_offset == end_offset - 1 ||
             file_buff->get_value(curr_offset + 1) == ',')) {
          /* Move past the , and the " */
          curr_offset += 2;
          break;
        }
        if (curr_char == '\\' && curr_offset != (end_offset - 1)) {
          curr_offset++;
          curr_char = file_buff->get_value(curr_offset);
          if (curr_char == 'r')
            buffer.append('\r');
          else if (curr_char == 'n')
            buffer.append('\n');
          else if (curr_char == '\\' || curr_char == '"')
            buffer.append(curr_char);
          else /* This could only happed with an externally created file */
          {
            buffer.append('\\');
            buffer.append(curr_char);
          }
        } else  // ordinary symbol
        {
          /*
            If we are at final symbol and no last quote was found =>
            we are working with a damaged file.
          */
          if (curr_offset == end_offset - 1) goto err;
          buffer.append(curr_char);
        }
      }
    } else {
      for (; curr_offset < end_offset; curr_offset++) {
        curr_char = file_buff->get_value(curr_offset);
        /* Move past the ,*/
        if (curr_char == ',') {
          curr_offset++;
          break;
        }
        if (curr_char == '\\' && curr_offset != (end_offset - 1)) {
          curr_offset++;
          curr_char = file_buff->get_value(curr_offset);
          if (curr_char == 'r')
            buffer.append('\r');
          else if (curr_char == 'n')
            buffer.append('\n');
          else if (curr_char == '\\' || curr_char == '"')
            buffer.append(curr_char);
          else /* This could only happed with an externally created file */
          {
            buffer.append('\\');
            buffer.append(curr_char);
          }
        } else {
          /*
             We are at the final symbol and a quote was found for the
             unquoted field => We are working with a damaged field.
          */
          if (curr_offset == end_offset - 1 && curr_char == '"') goto err;
          buffer.append(curr_char);
        }
      }
    }

    if (read_all || bitmap_is_set(table->read_set, (*field)->field_index())) {
      bool is_enum = ((*field)->real_type() == MYSQL_TYPE_ENUM);
      /*
        Here CHECK_FIELD_WARN checks that all values in the dlk file are valid
        which is normally the case, if they were written  by
        INSERT -> ha_dlk::write_row. '0' values on ENUM fields are considered
        invalid by Field_enum::store() but it can store them on INSERT anyway.
        Thus, for enums we silence the warning, as it doesn't really mean
        an invalid value.
      */
      if ((*field)->store(buffer.ptr(), buffer.length(), (*field)->charset(),
                          is_enum ? CHECK_FIELD_IGNORE : CHECK_FIELD_WARN)) {
        if (!is_enum) goto err;
      }
      if ((*field)->is_flag_set(BLOB_FLAG)) {
        Field_blob *blob_field = down_cast<Field_blob *>(*field);
        size_t length = blob_field->get_length();
        // BLOB data is not stored inside buffer. It only contains a
        // pointer to it. Copy the BLOB data into a separate memory
        // area so that it is not overwritten by subsequent calls to
        // Field::store() after moving the offset.
        if (length > 0) {
          unsigned char *new_blob = new (&blobroot) unsigned char[length];
          if (new_blob == nullptr) return HA_ERR_OUT_OF_MEM;
          memcpy(new_blob, blob_field->get_blob_data(), length);
          blob_field->set_ptr(length, new_blob);
        }
      }
    }
  }
  next_position = end_offset + eoln_len;
  error = 0;

err:
  dbug_tmp_restore_column_map(table->write_set, org_bitmap);

  return error;
}

/*
  Open a database file. Keep in mind that tables are caches, so
  this will not be called for every request. Any sort of positions
  that need to be reset should be kept in the ::extra() call.
*/
int ha_dlk::open(const char *name, int, uint open_options, const dd::Table *) {
  DBUG_TRACE;

  if (!(share = get_share(name, table))) return HA_ERR_OUT_OF_MEM;

  if (share->crashed && !(open_options & HA_OPEN_FOR_REPAIR)) {
    free_share(share);
    return HA_ERR_CRASHED_ON_USAGE;
  }
  /*
    Init locking. Pass handler object to the locking routines,
    so that they could save/update local_saved_data_file_length value
    during locking. This is needed to enable concurrent inserts.
  */

  thr_lock_data_init(&share->lock, &lock, (void *)this);
  ref_length = sizeof(my_off_t);

  return 0;
}

/*
  Close a database file. We remove ourselves from the shared structure.
  If it is empty we destroy it.
*/
int ha_dlk::close(void) {
  int rc = 0;
  DBUG_TRACE;
  if (data_file >= 0) {
    rc = mysql_file_close(data_file, MYF(0));
    data_file = -1;
  }
  return free_share(share) || rc;
}

/**
  @brief Initialize the data file.

  @details Compare the local version of the data file with the shared one.
  If they differ, there are some changes behind and we have to reopen
  the data file to make the changes visible.
  Call @c file_buff->init_buff() at the end to read the beginning of the
  data file into buffer.

  @retval  0  OK.
  @retval  1  There was an error.
*/
int ha_dlk::init_data_file() {
  if (data_file == -1) {
    MY_STAT file_stat;
    if (mysql_file_stat(dlk_key_file_data, share->data_file_name, &file_stat,
                        MYF(MY_WME)) == nullptr)
      return -1;
    local_saved_data_file_length = file_stat.st_size;
    if ((data_file = mysql_file_open(dlk_key_file_data, share->data_file_name,
                                     O_RDONLY, MYF(MY_WME))) == -1)
      return my_errno() ? my_errno() : -1;
  }
  file_buff->init_buff(data_file);
  return 0;
}

/*
  All table scans call this first.
  The order of a table scan is:

  ha_dlk::store_lock
  ha_dlk::external_lock
  ha_dlk::info
  ha_dlk::rnd_init
  ha_dlk::extra
  ha_dlk::rnd_next
  ha_dlk::rnd_next
  ha_dlk::rnd_next
  ha_dlk::rnd_next
  ha_dlk::rnd_next
  ha_dlk::rnd_next
  ha_dlk::rnd_next
  ha_dlk::rnd_next
  ha_dlk::rnd_next
  ha_dlk::extra
  ha_dlk::external_lock
  ha_dlk::extra
  ENUM HA_EXTRA_RESET   Reset database to after open

  Each call to ::rnd_next() represents a row returned in the can. When no more
  rows can be returned, rnd_next() returns a value of HA_ERR_END_OF_FILE.
  The ::info() call is just for the optimizer.

*/

int ha_dlk::rnd_init(bool) {
  DBUG_TRACE;

  /* set buffer to the beginning of the file */
  if (share->crashed || init_data_file()) return HA_ERR_CRASHED_ON_USAGE;

  current_position = next_position = 0;
  stats.records = 0;
  records_is_known = false;

  return 0;
}

/*
  ::rnd_next() does all the heavy lifting for a table scan. You will need to
  populate *buf with the correct field data. You can walk the field to
  determine at what position you should store the data (take a look at how
  ::find_current_row() works). The structure is something like:
  0Foo  Dog  Friend
  The first offset is for the first attribute. All space before that is
  reserved for null count.
  Basically this works as a mask for which rows are nulled (compared to just
  empty).
  This table handler doesn't do nulls and does not know the difference between
  NULL and "". This is ok since this table handler is for spreadsheets and
  they don't know about them either :)
*/
int ha_dlk::rnd_next(uchar *buf) {
  int rc;
  DBUG_TRACE;

  if (share->crashed) {
    rc = HA_ERR_CRASHED_ON_USAGE;
    goto end;
  }

  ha_statistic_increment(&System_status_var::ha_read_rnd_next_count);

  current_position = next_position;

  /* don't scan an empty file */
  if (!local_saved_data_file_length) {
    rc = HA_ERR_END_OF_FILE;
    goto end;
  }

  if ((rc = find_current_row(buf))) goto end;

  stats.records++;
  rc = 0;
end:
  return rc;
}

/*
  In the case of an order by rows will need to be sorted.
  ::position() is called after each call to ::rnd_next(),
  the data it stores is to a byte array. You can store this
  data via my_store_ptr(). ref_length is a variable defined to the
  class that is the sizeof() of position being stored. In our case
  its just a position. Look at the bdb code if you want to see a case
  where something other then a number is stored.
*/
void ha_dlk::position(const uchar *) {
  DBUG_TRACE;
  my_store_ptr(ref, ref_length, current_position);
}

/*
  Used to fetch a row from a posiion stored with ::position().
  my_get_ptr() retrieves the data for you.
*/

int ha_dlk::rnd_pos(uchar *buf, uchar *pos) {
  int rc;
  DBUG_TRACE;
  ha_statistic_increment(&System_status_var::ha_read_rnd_count);
  current_position = my_get_ptr(pos, ref_length);
  rc = find_current_row(buf);
  return rc;
}

/*
  ::info() is used to return information to the optimizer.
  Currently this table handler doesn't implement most of the fields
  really needed. SHOW also makes use of this data
*/
int ha_dlk::info(uint) {
  DBUG_TRACE;
  /* This is a lie, but you don't want the optimizer to see zero or 1 */
  if (!records_is_known && stats.records < 2) stats.records = 2;
  return 0;
}

/*
  Called after each table scan. In particular after deletes,
  and updates. In the last case we employ chain of deleted
  slots to clean up all of the dead space we have collected while
  performing deletes/updates.
*/
int ha_dlk::rnd_end() {
  DBUG_TRACE;
  blobroot.Clear();
  records_is_known = true;
  return 0;
}

/*
  Called by the database to lock the table. Keep in mind that this
  is an internal lock.
*/
THR_LOCK_DATA **ha_dlk::store_lock(THD *, THR_LOCK_DATA **to,
                                   enum thr_lock_type lock_type) {
  if (lock_type != TL_IGNORE && lock.type == TL_UNLOCK) lock.type = lock_type;
  *to++ = &lock;
  return to;
}

/*
  Create a table. You do not want to leave the table open after a call to
  this (the database will call ::open() if it needs to).
*/

int ha_dlk::create(const char *, TABLE *table_arg, HA_CREATE_INFO *info,
                   dd::Table *) {
  DBUG_TRACE;
  /*
    check columns
  */
  for (Field **field = table_arg->s->field; *field; field++) {
    if ((*field)->is_nullable()) {
      my_error(ER_CHECK_NOT_IMPLEMENTED, MYF(0), "nullable columns");
      return HA_ERR_UNSUPPORTED;
    }
  }
  if (info->external_file_name == nullptr) {
    my_error(ER_CHECK_NOT_IMPLEMENTED, MYF(0), "external table");
    return HA_ERR_UNSUPPORTED;
  }

  return 0;
}

int ha_dlk::check(THD *thd, HA_CHECK_OPT *) {
  int rc = 0;
  uchar *buf;
  const char *old_proc_info;
  DBUG_TRACE;

  old_proc_info = thd_proc_info(thd, "Checking table");

  if (data_file >= 0) {
    if (mysql_file_close(data_file, MYF(0))) return HA_ADMIN_CORRUPT;
    data_file = -1;
  }

  /* position buffer to the start of the file */
  if (init_data_file()) return HA_ADMIN_CORRUPT;

  if (!(buf = (uchar *)my_malloc(dlk_key_memory_row, table->s->reclength,
                                 MYF(MY_WME))))
    return HA_ERR_OUT_OF_MEM;

  /* set current position to the beginning of the file */
  current_position = next_position = 0;

  /* Read the file row-by-row. If everything is ok, repair is not needed. */
  while (!(rc = find_current_row(buf))) {
    thd_inc_row_count(thd);
    current_position = next_position;
  }

  blobroot.Clear();

  my_free(buf);
  thd_proc_info(thd, old_proc_info);

  if ((rc != HA_ERR_END_OF_FILE)) {
    share->crashed = true;
    return HA_ADMIN_CORRUPT;
  }
  return HA_ADMIN_OK;
}

bool ha_dlk::check_and_repair(THD *thd) {
  HA_CHECK_OPT check_opt;
  return repair(thd, &check_opt);
}

int ha_dlk::repair(THD *thd, HA_CHECK_OPT *opt) {
  DBUG_TRACE;
  share->crashed = false;
  return check(thd, opt);
}

bool ha_dlk::check_if_incompatible_data(HA_CREATE_INFO *, uint) {
  return COMPATIBLE_DATA_YES;
}

enum_alter_inplace_result ha_dlk::check_if_supported_inplace_alter(
    TABLE *, Alter_inplace_info *) {
  my_error(ER_ALTER_OPERATION_NOT_SUPPORTED, MYF(0), "external table",
           "other operations");
  return HA_ALTER_ERROR;
}

struct st_mysql_storage_engine dlk_storage_engine = {
    MYSQL_HANDLERTON_INTERFACE_VERSION};

mysql_declare_plugin(dlk){
    MYSQL_STORAGE_ENGINE_PLUGIN,
    &dlk_storage_engine,
    "dlk",
    PLUGIN_AUTHOR_ORACLE,
    "datalink storage engine",
    PLUGIN_LICENSE_GPL,
    dlk_init_func, /* Plugin Init */
    nullptr,       /* Plugin check uninstall */
    dlk_done_func, /* Plugin Deinit */
    0x0100 /* 1.0 */,
    nullptr, /* status variables                */
    nullptr, /* system variables                */
    nullptr, /* config options                  */
    0,       /* flags                           */
} mysql_declare_plugin_end;
