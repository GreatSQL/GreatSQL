/*function in this file was copy from percona-xtrabackup-8.0.30-23*/
/******************************************************
Copyright (c) 2011-2023 Percona LLC and/or its affiliates.
Copyright (c) 2024, GreatDB Software Co., Ltd.

Declarations for xtrabackup.cc

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA

*******************************************************/

#include "arch0apply.h"
#include "clone0clone.h"

hash_table_t *inc_dir_tables_hash;
/* TODO: replace with appropriate macros used in InnoDB 5.6 */
#define PAGE_ZIP_MIN_SIZE_SHIFT 10
#define DICT_TF_ZSSIZE_SHIFT 1
#define DICT_TF_FORMAT_ZIP 1
#define DICT_TF_FORMAT_SHIFT 5
static MEM_ROOT argv_alloc{PSI_NOT_INSTRUMENTED, 512};

/***********************************************************************
Computes bit shift for a given value. If the argument is not a power
of 2, returns 0.*/
static inline ulong get_bit_shift(ulong value) {
  ulong shift;

  if (value == 0) return 0;

  for (shift = 0; !(value & 1UL); shift++) {
    value >>= 1;
  }
  return (value >> 1) ? 0 : shift;
}

/** Make sure that we have a read access to the given datadir entry
@param[in]	statinfo	entry stat info
@param[out]	name		entry name */
static void check_datadir_enctry_access(const char *name,
                                        const struct stat *statinfo) {
  const char *entry_type = S_ISDIR(statinfo->st_mode) ? "directory" : "file";
  if ((statinfo->st_mode & S_IRUSR) != S_IRUSR) {
    char errmsg[FN_REFLEN_SE];
    sprintf(errmsg, "%s '%s' is not readable by  mysqld", entry_type, name);
    ib::error(ER_CLONE_APPLY_FILE_ERROR) << errmsg;

    exit(EXIT_FAILURE);
  }
}

/***********************************************************************
Generates path to the meta file path from a given path to an incremental .delta
by replacing trailing ".delta" with ".meta", or returns error if 'delta_path'
does not end with the ".delta" character sequence.
@return true on success, false on error. */
static bool get_meta_path(
    const char *delta_path, /* in: path to a .delta file */
    char *meta_path)        /* out: path to the corresponding .meta
                            file */
{
  size_t len = strlen(delta_path);

  size_t name_len = len - 1;
  size_t num_len = 0;
  for (; name_len > 0; name_len--) {
    int tmp = delta_path[name_len] - '0';
    if (tmp >= 0 && tmp <= 9) {
      num_len++;
      continue;
    } else {
      break;
    }
  }

  if (len <= 6 || strncmp(delta_path + len - 6 - num_len, ".delta", 6)) {
    return false;
  }
  memcpy(meta_path, delta_path, len - 6 - num_len);
  strcpy(meta_path + len - 6 - num_len, GDB_DELTA_INFO_SUFFIX);

  return true;
}

/* Retreive space_id from page 0 of tablespace
@param[in] file_name tablespace file path
@return space_id or SPACE_UNKOWN */
static space_id_t get_space_id_from_page_0(const char *file_name) {
  bool ok;
  space_id_t space_id = SPACE_UNKNOWN;

  auto buf = static_cast<byte *>(
      ut::malloc_withkey(UT_NEW_THIS_FILE_PSI_KEY, 2 * srv_page_size));

  auto file = os_file_create_simple_no_error_handling(
      0, file_name, OS_FILE_OPEN, OS_FILE_READ_ONLY, srv_read_only_mode, &ok);

  if (ok) {
    auto *page = static_cast<buf_frame_t *>(ut_align(buf, srv_page_size));

    IORequest request(IORequest::READ);
    dberr_t err =
        os_file_read_first_page(request, file_name, file, page, UNIV_PAGE_SIZE);

    if (err == DB_SUCCESS) {
      space_id = fsp_header_get_space_id(page);
    } else {
      char errmsg[FN_REFLEN_SE];
      sprintf(errmsg, "error reading first page on file: %s", file_name);
      ib::error(ER_CLONE_APPLY_FILE_ERROR) << errmsg;
    }
    os_file_close(file);

  } else {
    char errmsg[FN_REFLEN_SE];
    sprintf(errmsg, "Cannot open file to read first page: %s", file_name);
    ib::error(ER_CLONE_APPLY_FILE_ERROR) << errmsg;
  }

  ut::free(buf);

  return (space_id);
}

/****************************************************************/ /**
 Create a new tablespace on disk and return the handle to its opened
 file. Code adopted from fiL_ibd_create with
 the main difference that only disk file is created without updating
 the InnoDB in-memory dictionary data structures.

 @return true on success, false on error.  */
static bool clone_space_create_file(
    /*==================*/
    const char *path,    /*!<in: path to tablespace */
    ulint space_id,      /*!<in: space id */
    ulint flags,         /*!<in: tablespace flags */
    fil_space_t *space,  /*!<in: space object */
    pfs_os_file_t *file) /*!<out: file handle */
{
  const ulint size = FIL_IBD_FILE_INITIAL_SIZE;
  dberr_t err;
  byte *buf2;
  byte *page;
  bool success;
  char errmsg[FN_REFLEN_SE];

  IORequest write_request(IORequest::WRITE);

  ut_ad(!fsp_is_system_or_temp_tablespace(space_id));
  ut_ad(!srv_read_only_mode);
  ut_a(space_id < dict_sys_t::s_log_space_id);
  ut_a(fsp_flags_is_valid(flags));

  /* Create the subdirectories in the path, if they are
  not there already. */
  err = os_file_create_subdirs_if_needed(path);
  if (err != DB_SUCCESS) {
    return (false);
  }

  *file = os_file_create(
      innodb_data_file_key, path, OS_FILE_CREATE | OS_FILE_ON_ERROR_NO_EXIT,
      OS_FILE_NORMAL, OS_DATA_FILE, srv_read_only_mode, &success);

  if (!success) {
    /* The following call will print an error message */
    ulint error = os_file_get_last_error(true);

    sprintf(errmsg, "Cannot create file '%s'", path);
    ib::error(ER_CLONE_APPLY_FILE_ERROR) << errmsg;

    if (error == OS_FILE_ALREADY_EXISTS) {
      sprintf(errmsg,
              "The file '%s' already exists though the corresponding table did "
              "not exist in the InnoDB data dictionary. Have you moved InnoDB "
              ".ibd files around without using the SQL commands DISCARD "
              "TABLESPACE and IMPORT TABLESPACE, or did mysqld crash in the "
              "middle of CREATE TABLE? You can resolve the problem by removing "
              "the file '%s' under the 'datadir' of MySQL.",
              path, path);

      ib::error(ER_CLONE_APPLY_FILE_ERROR) << errmsg;

      return (false);
    }

    return (false);
  }

#if !defined(NO_FALLOCATE) && defined(UNIV_LINUX)
  if (fil_fusionio_enable_atomic_write(*file)) {
    /* This is required by FusionIO HW/Firmware */
    int ret = posix_fallocate(file->m_file, 0, size * UNIV_PAGE_SIZE);

    if (ret != 0) {
      sprintf(errmsg,
              "posix_fallocate(): Failed to preallocate data for file %s, "
              "desired size %ld Operating system error number %d. Check that "
              "the disk is not full or a disk quota exceeded. Make sure the "
              "file system supports this function. Some operating system error "
              "numbers are described at operating-system-error-codes.html.",
              path, size * UNIV_PAGE_SIZE, ret);

      ib::error(ER_CLONE_APPLY_FILE_ERROR) << errmsg;
      success = false;
    } else {
      success = true;
    }
  } else {
    success = os_file_set_size(path, *file, 0, size * UNIV_PAGE_SIZE, false);
  }
#else
  success = os_file_set_size(path, *file, 0, size * UNIV_PAGE_SIZE, false);
#endif /* !NO_FALLOCATE && UNIV_LINUX */

  if (!success) {
    os_file_close(*file);
    os_file_delete(innodb_data_file_key, path);
    return (false);
  }

  /* We have to write the space id to the file immediately and flush the
  file to disk. This is because in crash recovery we must be aware what
  tablespaces exist and what are their space id's, so that we can apply
  the log records to the right file. It may take quite a while until
  buffer pool flush algorithms write anything to the file and flush it to
  disk. If we would not write here anything, the file would be filled
  with zeros from the call of os_file_set_size(), until a buffer pool
  flush would write to it. */

  buf2 = static_cast<byte *>(
      ut::malloc_withkey(UT_NEW_THIS_FILE_PSI_KEY, 3 * UNIV_PAGE_SIZE));
  /* Align the memory for file i/o if we might have O_DIRECT set */
  page = static_cast<byte *>(ut_align(buf2, UNIV_PAGE_SIZE));

  memset(page, '\0', UNIV_PAGE_SIZE);

  /* Add the UNIV_PAGE_SIZE to the table flags and write them to the
  tablespace header. */
  flags = fsp_flags_set_page_size(flags, univ_page_size);
  fsp_header_init_fields(page, space_id, flags);
  mach_write_to_4(page + FIL_PAGE_ARCH_LOG_NO_OR_SPACE_ID, space_id);

  const page_size_t page_size(flags);

  if (!page_size.is_compressed()) {
    buf_flush_init_for_writing(NULL, page, NULL, 0,
                               fsp_is_checksum_disabled(space_id), true);
    success = os_file_write(write_request, path, *file, page, 0,
                            page_size.physical());
  } else {
    page_zip_des_t page_zip;

    page_zip_set_size(&page_zip, page_size.physical());
    page_zip.data = page + UNIV_PAGE_SIZE;
#ifdef UNIV_DEBUG
    page_zip.m_start =
#endif /* UNIV_DEBUG */
        page_zip.m_end = (page_zip.m_nonempty = (page_zip.n_blobs = 0));

    buf_flush_init_for_writing(NULL, page, &page_zip, 0,
                               fsp_is_checksum_disabled(space_id), true);
    success = os_file_write(write_request, path, *file, page_zip.data, 0,
                            page_size.physical());
  }

  ut::free(buf2);

  if (!success) {
    sprintf(errmsg, "Could not write the first page to tablespace '%s'", path);
    ib::error(ER_CLONE_APPLY_FILE_ERROR) << errmsg;

    os_file_close(*file);
    os_file_delete(innodb_data_file_key, path);
    return (false);
  }

  success = os_file_flush(*file);

  if (!success) {
    sprintf(errmsg, "File flush of tablespace '%s' failed", path);
    ib::error(ER_CLONE_APPLY_FILE_ERROR) << errmsg;

    os_file_close(*file);
    os_file_delete(innodb_data_file_key, path);
    return (false);
  }

  if (fil_node_create(path, size, space, false, false) == nullptr) {
    ib::fatal(UT_LOCATION_HERE) << "Unable to add tablespace node '" << path
                                << "' to the tablespace cache.";
  }

  return (true);
}

/***********************************************************************
Read meta info for an incremental delta.
@return true on success, false on failure. */
static bool clone_read_delta_metadata(const char *filepath,
                                      clone_delta_info_t *info) {
  FILE *fp;
  char key[51];
  char value[51];
  bool r = true;
  char errmsg[FN_REFLEN_SE];

  /* set defaults */
  info->page_size = ULINT_UNDEFINED;
  info->zip_size = ULINT_UNDEFINED;
  info->space_id = SPACE_UNKNOWN;
  info->space_flags = 0;

  fp = fopen(filepath, "r");
  if (!fp) {
    /* Meta files for incremental deltas are optional */
    return (true);
  }

  while (!feof(fp)) {
    if (fscanf(fp, "%50s = %50s\n", key, value) == 2) {
      if (strcmp(key, "page_size") == 0) {
        info->page_size = strtoul(value, NULL, 10);
      } else if (strcmp(key, "zip_size") == 0) {
        info->zip_size = strtoul(value, NULL, 10);
      } else if (strcmp(key, "space_id") == 0) {
        info->space_id = strtoul(value, NULL, 10);
      } else if (strcmp(key, "space_flags") == 0) {
        info->space_flags = strtoul(value, NULL, 10);
      }
    }
  }

  fclose(fp);

  if (info->page_size == ULINT_UNDEFINED) {
    sprintf(errmsg, "page_size is required in %s", filepath);
    ib::error(ER_CLONE_APPLY_FILE_ERROR) << errmsg;
    r = false;
  }
  if (info->space_id == SPACE_UNKNOWN) {
    sprintf(errmsg, "invalid space_id in %s", filepath);
    ib::error(ER_CLONE_APPLY_FILE_ERROR) << errmsg;
    r = false;
  }

  return (r);
}

void clone_filter_hash_free(hash_table_t *hash) {
  ulint i;

  /* free the hash elements */
  for (i = 0; i < hash_get_n_cells(hash); i++) {
    clone_filter_entry_t *table;

    table = static_cast<clone_filter_entry_t *>(hash_get_first(hash, i));

    while (table) {
      clone_filter_entry_t *prev_table = table;

      table = static_cast<clone_filter_entry_t *>(
          HASH_GET_NEXT(name_hash, prev_table));

      HASH_DELETE(clone_filter_entry_t, name_hash, hash,
                  ut::hash_string(prev_table->name), prev_table);
      ut::free(prev_table);
    }
  }

  /* free hash */
  ut::delete_(hash);
}

/***********************************************************************
Searches for matching tablespace file for given .delta file and space_id
in given directory. When matching tablespace found, renames it to match the
name of .delta file. If there was a tablespace with matching name and
mismatching ID, renames it to clone_tmp_#ID.ibd. If there was no
matching file, creates a new tablespace.
@return file handle of matched or created file */
static pfs_os_file_t clone_delta_open_matching_space(
    const char *dbname,   /* in: path to destination database dir */
    const char *name,     /* in: name of delta file (without .delta) */
    space_id_t space_id,  /* in: space id of delta file */
    ulint space_flags,    /* in: space flags of delta file */
    ulint zip_size,       /* in: zip_size of tablespace */
    char *real_name,      /* out: full path of destination file */
    size_t real_name_len, /* out: buffer size for real_name */
    bool *success)        /* out: indicates error. true = success */
{
  char dest_dir[FN_REFLEN * 2 + 1];
  char dest_space_name[FN_REFLEN * 2 + 1];
  char errmsg[FN_REFLEN_SE];
  bool ok;
  pfs_os_file_t file = GDB_FILE_UNDEFINED;
  clone_filter_entry_t *table;
  fil_space_t *fil_space;
  space_id_t f_space_id;
  os_file_create_t create_option = OS_FILE_OPEN;
  char *clone_target_dir = mysql_data_home;
  *success = false;

  if (dbname) {
    snprintf(dest_dir, sizeof(dest_dir), "%s/%s", clone_target_dir, dbname);
    Fil_path::normalize(dest_dir);

    snprintf(dest_space_name, sizeof(dest_space_name), "%s/%s", dbname, name);
  } else {
    snprintf(dest_dir, sizeof(dest_dir), "%s", clone_target_dir);
    Fil_path::normalize(dest_dir);

    snprintf(dest_space_name, sizeof(dest_space_name), "%s", name);
  }

  snprintf(real_name, real_name_len, "%s/%s", clone_target_dir,
           dest_space_name);
  Fil_path::normalize(real_name);
  /* Truncate ".ibd" */
  dest_space_name[strlen(dest_space_name) - 4] = '\0';

  /* Create the database directory if it doesn't exist yet */
  if (!os_file_create_directory(dest_dir, false)) {
    sprintf(errmsg, "cannot create dir %s", dest_dir);
    ib::error(ER_CLONE_APPLY_FILE_ERROR) << errmsg;
    return file;
  }

  /* remember space name used by incremental prepare. This hash is later used to
  detect the dropped tablespaces and remove them. Check rm_if_not_found() */
  table = static_cast<clone_filter_entry_t *>(ut::malloc_withkey(
      UT_NEW_THIS_FILE_PSI_KEY,
      sizeof(clone_filter_entry_t) + strlen(dest_space_name) + 1));

  table->name = ((char *)table) + sizeof(clone_filter_entry_t);
  strcpy(table->name, dest_space_name);
  HASH_INSERT(clone_filter_entry_t, name_hash, inc_dir_tables_hash,
              ut::hash_string(table->name), table);

  if (space_id != SPACE_UNKNOWN && !fsp_is_ibd_tablespace(space_id)) {
    /* since undo tablespaces cannot be renamed, we must either open existing
    with the same name or create new one */
    if (fsp_is_undo_tablespace(space_id)) {
      bool exists;
      os_file_type_t type;
      os_file_status(real_name, &exists, &type);

      if (exists) {
        f_space_id = get_space_id_from_page_0(real_name);

        if (f_space_id == SPACE_UNKNOWN) {
          sprintf(errmsg, "could not find space id from file %s", real_name);
          ib::error(ER_CLONE_APPLY_FILE_ERROR) << errmsg;
          goto exit;
        }

        if (space_id == f_space_id) {
          goto found;
        }

        /* space_id of undo tablespace from incremental is different from the
        full backup. Rename the existing undo tablespace to a temporary name and
        create undo tablespace file with new space_id */
        char tmpname[FN_REFLEN];
        snprintf(tmpname, FN_REFLEN, "./clone_tmp_#" SPACE_ID_PF ".ibu",
                 f_space_id);

        char *oldpath, *space_name;
        bool res =
            fil_space_read_name_and_filepath(f_space_id, &space_name, &oldpath);
        ut_a(res);

        sprintf(errmsg, "Renaming %s to %s.ibu", dest_space_name, tmpname);
        ib::info(ER_CLONE_APPLY_FILE) << errmsg;

        ut_a(os_file_status(oldpath, &exists, &type));

        if (!fil_rename_tablespace(f_space_id, oldpath, tmpname, tmpname)) {
          sprintf(errmsg, "Cannot rename %s to %s", dest_space_name, tmpname);
          ib::error(ER_CLONE_APPLY_FILE_ERROR) << errmsg;

          ut::free(oldpath);
          ut::free(space_name);
          goto exit;
        }

        ut::free(oldpath);
        ut::free(space_name);
      }

      /* either file doesn't exist or it has been renamed above */
      fil_space = fil_space_create(dest_space_name, space_id, space_flags,
                                   FIL_TYPE_TABLESPACE);
      if (fil_space == nullptr) {
        sprintf(errmsg, "Cannot create tablespace %s", dest_space_name);
        ib::error(ER_CLONE_APPLY_FILE_ERROR) << errmsg;

        goto exit;
      }
      *success = clone_space_create_file(real_name, space_id, space_flags,
                                         fil_space, &file);
      goto exit;
    }
    goto found;
  }

  f_space_id = fil_space_get_id_by_name(dest_space_name);

  if (f_space_id != SPACE_UNKNOWN) {
    if (f_space_id == space_id || space_id == SPACE_UNKNOWN) {
      /* we found matching space */
      goto found;
    } else {
      char tmpname[FN_REFLEN];
      bool exists;
      os_file_type_t type;

      if (dbname != NULL) {
        snprintf(tmpname, FN_REFLEN, "%s/clone_tmp_#" SPACE_ID_PF, dbname,
                 f_space_id);
      } else {
        snprintf(tmpname, FN_REFLEN, "./clone_tmp_#" SPACE_ID_PF, f_space_id);
      }

      char *oldpath, *space_name;

      bool res =
          fil_space_read_name_and_filepath(f_space_id, &space_name, &oldpath);
      ut_a(res);

      sprintf(errmsg, "Renaming %s to %s.ibd", dest_space_name, tmpname);
      ib::info(ER_CLONE_APPLY_FILE) << errmsg;

      ut_a(os_file_status(oldpath, &exists, &type));

      if (exists &&
          !fil_rename_tablespace(f_space_id, oldpath, tmpname, NULL)) {
        sprintf(errmsg, "Cannot rename %s to %s", dest_space_name, tmpname);
        ib::error(ER_CLONE_APPLY_FILE_ERROR) << errmsg;

        ut::free(oldpath);
        ut::free(space_name);
        goto exit;
      }
      // close the ./clone_tmp_#XXX tablespace for extend tablespace
      if (fil_close_tablespace(f_space_id) != DB_SUCCESS) {
        sprintf(errmsg, "Close tablespace failed, tablesapceID: %u",
                f_space_id);
        ib::error(ER_CLONE_APPLY_FILE_ERROR) << errmsg;
        goto exit;
      }

      ut::free(oldpath);
      ut::free(space_name);
    }
  }

  if (space_id == SPACE_UNKNOWN) {
    sprintf(errmsg, "Cannot handle DDL operation on tablespace %s",
            dest_space_name);
    ib::error(ER_CLONE_APPLY_FILE_ERROR) << errmsg;

    exit(EXIT_FAILURE);
  }

  fil_space = fil_space_get(space_id);

  if (fil_space != NULL) {
    char tmpname[FN_REFLEN];
    bool exists;
    os_file_type_t type;

    strncpy(tmpname, dest_space_name, FN_REFLEN);

    char *oldpath, *space_name;

    bool res =
        fil_space_read_name_and_filepath(fil_space->id, &space_name, &oldpath);

    ut_a(res);

    sprintf(errmsg, "Renaming %s to %s ", fil_space->name, dest_space_name);
    ib::info(ER_CLONE_APPLY_FILE) << errmsg;

    ut_a(os_file_status(oldpath, &exists, &type));

    if (exists &&
        !fil_rename_tablespace(fil_space->id, oldpath, tmpname, NULL)) {
      sprintf(errmsg, "Cannot rename %s to %s", fil_space->name,
              dest_space_name);
      ib::error(ER_CLONE_APPLY_FILE_ERROR) << errmsg;

      ut::free(oldpath);
      ut::free(space_name);
      goto exit;
    }
    ut::free(oldpath);
    ut::free(space_name);

    goto found;
  }

  /* No matching space found. create the new one.  */

  fil_space = fil_space_create(dest_space_name, space_id, space_flags,
                               FIL_TYPE_TABLESPACE);
  if (fil_space == nullptr) {
    sprintf(errmsg, "Cannot create tablespace %s", dest_space_name);
    ib::error(ER_CLONE_APPLY_FILE_ERROR) << errmsg;

    goto exit;
  }

  /* Calculate correct tablespace flags for compressed tablespaces.  */
  if (zip_size != 0 && zip_size != ULINT_UNDEFINED) {
    space_flags |= (get_bit_shift(zip_size >> PAGE_ZIP_MIN_SIZE_SHIFT << 1)
                    << DICT_TF_ZSSIZE_SHIFT) |
                   DICT_TF_COMPACT |
                   (DICT_TF_FORMAT_ZIP << DICT_TF_FORMAT_SHIFT);
    ut_a(page_size_t(space_flags).physical() == zip_size);
  }
  *success = clone_space_create_file(real_name, space_id, space_flags,
                                     fil_space, &file);
  goto exit;

found:
  /* open the file and return it's handle */

  file = os_file_create_simple_no_error_handling(
      0, real_name, create_option, OS_FILE_READ_WRITE, srv_read_only_mode, &ok);

  if (ok) {
    *success = true;
  } else {
    sprintf(errmsg, "Cannot open file %s", real_name);
    ib::error(ER_CLONE_APPLY_FILE_ERROR) << errmsg;
  }

exit:

  return file;
}

/************************************************************************
Applies a given .delta file to the corresponding data file.
@return true on success */
static bool clone_apply_delta(
    const datadir_entry_t &entry, /*!<in: datadir entry */
    void *delete_src) {
  pfs_os_file_t src_file = GDB_FILE_UNDEFINED;
  pfs_os_file_t dst_file = GDB_FILE_UNDEFINED;
  char src_path[FN_REFLEN];
  char dst_path[FN_REFLEN * 2 + 1];
  char meta_path[FN_REFLEN];
  char space_name[FN_REFLEN];
  char errmsg[FN_REFLEN_SE];
  bool success;

  bool last_buffer = false;
  ulint page_in_buffer;
  ulint incremental_buffers = 0;

  clone_delta_info_t info;
  ulint page_size;
  ulint page_size_shift;
  byte *incremental_buffer_base = NULL;
  byte *incremental_buffer;

  size_t offset;
  os_file_stat_t stat_info;

  if (entry.is_empty_dir) {
    return true;
  }

  IORequest read_request(IORequest::READ);
  IORequest write_request(IORequest::WRITE | IORequest::PUNCH_HOLE);

  size_t name_len = entry.file_name.size() - 1;
  size_t num_len = 0;
  for (; name_len > 0; name_len--) {
    int tmp = entry.file_name[name_len] - '0';
    if (tmp >= 0 && tmp <= 9) {
      num_len++;
      continue;
    } else {
      break;
    }
  }

  if (!entry.db_name.empty()) {
    snprintf(src_path, sizeof(src_path), "%s/%s/%s", entry.datadir.c_str(),
             entry.db_name.c_str(), entry.file_name.c_str());
    snprintf(dst_path, sizeof(dst_path), "%s/%s/%s", mysql_real_data_home_ptr,
             entry.db_name.c_str(), entry.file_name.c_str());
  } else {
    snprintf(src_path, sizeof(src_path), "%s/%s", entry.datadir.c_str(),
             entry.file_name.c_str());
    snprintf(dst_path, sizeof(dst_path), "%s/%s", mysql_real_data_home_ptr,
             entry.file_name.c_str());
  }

  dst_path[strlen(dst_path) - 6 - num_len] = '\0';

  strncpy(space_name, entry.file_name.c_str(), FN_REFLEN);
  space_name[strlen(space_name) - 6 - num_len] = 0;

  if (!get_meta_path(src_path, meta_path)) {
    goto error;
  }

  Fil_path::normalize(dst_path);
  Fil_path::normalize(src_path);
  Fil_path::normalize(meta_path);

  if (!clone_read_delta_metadata(meta_path, &info)) {
    goto error;
  }

  page_size = info.page_size;
  page_size_shift = get_bit_shift(page_size);

  sprintf(errmsg, "page size for %s is %ld bytes", src_path, page_size);
  ib::info(ER_CLONE_APPLY_FILE) << errmsg;

  if (page_size_shift < 10 || page_size_shift > UNIV_PAGE_SIZE_SHIFT_MAX) {
    sprintf(errmsg, "invalid value of page_size ( %ld bytes) read from %s",
            page_size, meta_path);
    ib::error(ER_CLONE_APPLY_FILE_ERROR) << errmsg;

    goto error;
  }

  src_file = os_file_create_simple_no_error_handling(
      0, src_path, OS_FILE_OPEN, OS_FILE_READ_WRITE, srv_read_only_mode,
      &success);
  if (!success) {
    os_file_get_last_error(true);

    sprintf(errmsg, "cannot open %s", src_path);
    ib::error(ER_CLONE_APPLY_FILE_ERROR) << errmsg;

    goto error;
  }

  posix_fadvise(src_file.m_file, 0, 0, POSIX_FADV_SEQUENTIAL);

  os_file_set_nocache(src_file.m_file, src_path, "OPEN");

  dst_file = clone_delta_open_matching_space(
      entry.db_name.empty() ? nullptr : entry.db_name.c_str(), space_name,
      info.space_id, info.space_flags, info.zip_size, dst_path,
      sizeof(dst_path), &success);
  if (!success) {
    sprintf(errmsg, "cannot open %s", dst_path);
    ib::error(ER_CLONE_APPLY_FILE_ERROR) << errmsg;

    goto error;
  }

  posix_fadvise(dst_file.m_file, 0, 0, POSIX_FADV_DONTNEED);

  os_file_set_nocache(dst_file.m_file, dst_path, "OPEN");

  os_file_get_status(src_path, &stat_info, false, false);
  if (stat_info.size == 0) {
    goto clean;
  }

  os_file_get_status(dst_path, &stat_info, false, false);

  /* allocate buffer for incremental backup */
  incremental_buffer_base = static_cast<byte *>(
      ut::malloc_withkey(UT_NEW_THIS_FILE_PSI_KEY,
                         (page_size / 4 + 1) * page_size + UNIV_PAGE_SIZE_MAX));
  incremental_buffer = static_cast<byte *>(
      ut_align(incremental_buffer_base, UNIV_PAGE_SIZE_MAX));

  sprintf(errmsg, "Applying  %s to %s", src_path, dst_path);
  ib::info(ER_CLONE_APPLY_FILE) << errmsg;

  while (!last_buffer) {
    ulint cluster_header;

    /* read to buffer */
    /* first block of block cluster */
    offset = ((incremental_buffers * (page_size / 4)) << page_size_shift);
    success = os_file_read(read_request, src_path, src_file, incremental_buffer,
                           offset, page_size);
    if (!success) {
      goto error;
    }

    cluster_header = mach_read_from_4(incremental_buffer);
    switch (cluster_header) {
      case INIT_FLAG: /*"clon"*/
        break;
      case END_FLAG: /*"CLON"*/
        last_buffer = true;
        break;
      default:
        sprintf(errmsg, "%s is not valid .delta file.", src_path);
        ib::warn(ER_CLONE_APPLY_FILE) << errmsg;

        goto clean;
    }

    for (page_in_buffer = 1; page_in_buffer < page_size / 4; page_in_buffer++) {
      if (mach_read_from_4(incremental_buffer + page_in_buffer * 4) ==
          0xFFFFFFFFUL)
        break;
    }

    ut_a(last_buffer || page_in_buffer == page_size / 4);

    /* read whole of the cluster */
    success = os_file_read(read_request, src_path, src_file, incremental_buffer,
                           offset, page_in_buffer * page_size);
    if (!success) {
      goto error;
    }

    posix_fadvise(src_file.m_file, offset, page_in_buffer * page_size,
                  POSIX_FADV_DONTNEED);

    for (page_in_buffer = 1; page_in_buffer < page_size / 4; page_in_buffer++) {
      const page_t *page = incremental_buffer + page_in_buffer * page_size;
      const ulint offset_on_page =
          mach_read_from_4(incremental_buffer + page_in_buffer * 4);

      if (offset_on_page == 0xFFFFFFFFUL) break;

      const auto offset_in_file = offset_on_page << page_size_shift;

      success = os_file_write(write_request, dst_path, dst_file, page,
                              offset_in_file, page_size);
      if (!success) {
        goto error;
      }

      if (IORequest::is_punch_hole_supported() &&
          (Compression::is_compressed_page(page) ||
           fil_page_get_type(page) == FIL_PAGE_COMPRESSED_AND_ENCRYPTED)) {
        size_t compressed_len =
            mach_read_from_2(page + FIL_PAGE_COMPRESS_SIZE_V1) + FIL_PAGE_DATA;
        compressed_len = ut_calc_align(compressed_len, stat_info.block_size);
        if (compressed_len < page_size) {
          if (os_file_punch_hole(dst_file.m_file,
                                 offset_in_file + compressed_len,
                                 page_size - compressed_len) != DB_SUCCESS) {
            ib::error(ER_CLONE_APPLY_FILE_ERROR)
                << "os_file_punch_hole returned error";
            goto error;
          }
        }
      }
    }

    incremental_buffers++;
  }
clean:
  if (incremental_buffer_base) ut::free(incremental_buffer_base);
  if (src_file != GDB_FILE_UNDEFINED) os_file_close(src_file);
  if (dst_file != GDB_FILE_UNDEFINED) os_file_close(dst_file);
  if (*(bool *)delete_src) {
    os_file_delete(0, src_path);
    os_file_delete(0, meta_path);
  }
  return true;

error:
  if (incremental_buffer_base) ut::free(incremental_buffer_base);
  if (src_file != GDB_FILE_UNDEFINED) os_file_close(src_file);
  if (dst_file != GDB_FILE_UNDEFINED) os_file_close(dst_file);

  sprintf(errmsg, "clone_apply_delta(): failed to apply %s to %s", src_path,
          dst_path);
  ib::error(ER_CLONE_APPLY_FILE_ERROR) << errmsg;

  return true;
}

/************************************************************************
Callback to handle datadir entry. Deletes entry if it has no matching
fil_space in fil_system directory.
@return false if delete attempt was unsuccessful */
bool rm_if_not_found(const datadir_entry_t &entry, /*!<in: datadir entry */
                     void *arg __attribute__((unused))) {
  char name[FN_REFLEN];
  clone_filter_entry_t *table;

  if (entry.is_empty_dir) {
    return true;
  }

  if (!entry.db_name.empty()) {
    snprintf(name, FN_REFLEN, "%s/%s", entry.db_name.c_str(),
             entry.file_name.c_str());
  } else {
    snprintf(name, FN_REFLEN, "%s", entry.file_name.c_str());
  }

  /* Truncate ".ibd" */
  name[strlen(name) - 4] = '\0';

  HASH_SEARCH(name_hash, inc_dir_tables_hash, ut::hash_string(name),
              clone_filter_entry_t *, table, (void)0,
              !strcmp(table->name, name));

  if (!table) {
    if (!entry.db_name.empty()) {
      snprintf(name, FN_REFLEN, "%s/%s/%s", entry.datadir.c_str(),
               entry.db_name.c_str(), entry.file_name.c_str());
    } else {
      snprintf(name, FN_REFLEN, "%s/%s", entry.datadir.c_str(),
               entry.file_name.c_str());
    }
    return os_file_delete(0, name);
  }

  return (true);
}

/** Process single second level datadir entry for
clone_process_datadir
@param[in]	datadir	datadir path
@param[in]	path	path to the file
@param[in]	dbname	database name (first level entry name)
@param[in]	name	name of the file
@param[in]	suffix	suffix to match against
@param[in]	func	callback
@param[in]	data	additional argument for callback */
void process_datadir_l2cbk(const char *datadir, const char *dbname,
                           const char *path, const char *name,
                           const char *suffix, handle_datadir_entry_func_t func,
                           void *data) {
  struct stat statinfo;
  size_t suffix_len = strlen(suffix);

  if (stat(path, &statinfo) != 0) {
    return;
  }

  size_t name_len = strlen(name) - 1;
  size_t num_len = 0;
  for (; name_len > 0; name_len--) {
    int tmp = name[name_len] - '0';
    if (tmp >= 0 && tmp <= 9) {
      num_len++;
      continue;
    } else {
      break;
    }
  }

  if (S_ISREG(statinfo.st_mode) &&
      (strlen(name) > suffix_len &&
       strncmp(name + strlen(name) - suffix_len - num_len, suffix,
               suffix_len) == 0)) {
    check_datadir_enctry_access(name, &statinfo);
    func(datadir_entry_t(datadir, path, dbname, name, false), data);
  }
}

/** Process single top level datadir entry for clone_process_datadir
@param[in]	datadir	datadir path
@param[in]	path	path to the file
@param[in]	name	name of the file
@param[in]	suffix	suffix to match against
@param[in]	func	callback
@param[in]	data	additional argument for callback */
void process_datadir_l1cbk(const char *datadir, const char *path,
                           const char *name, const char *suffix,
                           handle_datadir_entry_func_t func, void *data) {
  struct stat statinfo;
  size_t suffix_len = strlen(suffix);

  if (stat(path, &statinfo) != 0) {
    return;
  }

  if (S_ISDIR(statinfo.st_mode)) {
    bool is_empty_dir = true;
    check_datadir_enctry_access(name, &statinfo);
    os_file_scan_directory(
        path,
        [&](const char *l2path, const char *l2name) mutable -> void {
          if (strcmp(l2name, ".") == 0 || strcmp(l2name, "..") == 0) {
            return;
          }
          is_empty_dir = false;
          char fullpath[FN_REFLEN];
          snprintf(fullpath, sizeof(fullpath), "%s/%s", l2path, l2name);
          process_datadir_l2cbk(datadir, name, fullpath, l2name, suffix, func,
                                data);
        },
        false);
    if (is_empty_dir) {
      func(datadir_entry_t(datadir, path, name, "", true), data);
    }
  }

  size_t name_len = strlen(name) - 1;
  size_t num_len = 0;
  for (; name_len > 0; name_len--) {
    int tmp = name[name_len] - '0';
    if (tmp >= 0 && tmp <= 9) {
      num_len++;
      continue;
    } else {
      break;
    }
  }

  if (S_ISREG(statinfo.st_mode) &&
      (strlen(name) > suffix_len &&
       strncmp(name + strlen(name) - suffix_len - num_len, suffix,
               suffix_len) == 0)) {
    check_datadir_enctry_access(name, &statinfo);
    func(datadir_entry_t(datadir, path, "", name, false), data);
  }
}

/************************************************************************
Function enumerates files in datadir (provided by path) which are matched
by provided suffix. For each entry callback is called.
@return false if callback for some entry returned false */
bool clone_process_datadir(const char *path,   /*!<in: datadir path */
                           const char *suffix, /*!<in: suffix to match
                                               against */
                           handle_datadir_entry_func_t func, /*!<in: callback */
                           void *data) /*!<in: additional argument for
                                       callback */
{
  bool ret = os_file_scan_directory(
      path,
      [&](const char *l1path, const char *l1name) -> void {
        if (strcmp(l1name, ".") == 0 || strcmp(l1name, "..") == 0) {
          return;
        }
        char fullpath[FN_REFLEN];
        snprintf(fullpath, sizeof(fullpath), "%s/%s", l1path, l1name);
        process_datadir_l1cbk(path, fullpath, l1name, suffix, func, data);
      },
      false);
  return ret;
}

/************************************************************************
Applies all .delta files from incremental_dir to the full clone.
@return true on success. */
bool clone_apply_deltas(bool delete_src) {
  return clone_process_datadir(clone_incremental_dir, ".delta",
                               clone_apply_delta, &delete_src);
}

/* ======== Datafiles iterator ======== */
datafiles_iter_t *datafiles_iter_new() {
  datafiles_iter_t *it = new datafiles_iter_t();

  Fil_iterator::for_each_file([&](fil_node_t *file) {
    it->nodes.push_back(file);
    return (DB_SUCCESS);
  });

  it->i = it->nodes.begin();

  return it;
}

fil_node_t *datafiles_iter_next(datafiles_iter_t *it) {
  fil_node_t *node = NULL;

  if (it->i != it->nodes.end()) {
    node = *it->i;
    it->i++;
  }

  return node;
}

void datafiles_iter_free(datafiles_iter_t *it) { delete it; }

void clone_extend_space() {
  char errmsg[FN_REFLEN_SE];
  sprintf(errmsg, "extend tablespace in  %s start", clone_incremental_dir);
  ib::info(ER_CLONE_APPLY_FILE) << errmsg;

  // extend file size
  datafiles_iter_t *it;
  fil_node_t *node;
  fil_space_t *space;
  it = datafiles_iter_new();
  if (it == NULL) {
    ib::error(ER_CLONE_APPLY_FILE_ERROR) << "datafiles_iter_new() failed.";
    exit(EXIT_FAILURE);
  }

  while ((node = datafiles_iter_next(it)) != NULL) {
    byte *header;
    ulint size;
    mtr_t mtr;
    buf_block_t *block;

    space = node->space;

    /* Align space sizes along with fsp header. We want to process
    each space once, so skip all nodes except the first one in a
    multi-node space. */
    if (node != &space->files.front()) {
      continue;
    }

    mtr_start(&mtr);

    mtr_s_lock(fil_space_get_latch(space->id), &mtr, UT_LOCATION_HERE);

    block = buf_page_get(page_id_t(space->id, 0), page_size_t(space->flags),
                         +RW_S_LATCH, UT_LOCATION_HERE, &mtr);
    header = FSP_HEADER_OFFSET + buf_block_get_frame(block);

    size = mtr_read_ulint(header + FSP_SIZE, MLOG_4BYTES, &mtr);

    mtr_commit(&mtr);

    bool res = fil_space_extend(space, size);

    ut_a(res);
  }

  datafiles_iter_free(it);

  sprintf(errmsg, "extend tablespace in  %s complete!", clone_incremental_dir);
  ib::info(ER_CLONE_APPLY_FILE) << errmsg;
}

bool execute_syscmd(std::string cmd) {
  std::string err_msg;
  FILE *f = NULL;
  f = popen(cmd.c_str(), "r");
  if (!f) {
    DBUG_PRINT("error",
               ("Error popen cmd [%s] with error [%d].\n", cmd.c_str(), errno));
    err_msg = "Error popen cmd [" + cmd + "] with error [" +
              std::to_string(errno) + "].";
    ib::error(ER_CLONE_APPLY_FILE_ERROR) << err_msg;
    return true;
  }

  char buff[2048];
  memset(buff, 0, sizeof(buff));

  // TODO: fgets and fclose may be blocked if command did not return any thing.
  std::string msg;
  while (fgets(buff, sizeof(buff), f) != NULL) {
    msg.append(buff);
  }
  int ret = pclose(f);
  if (ret) {
    if (!msg.empty())
      DBUG_PRINT("error", ("Execute [%s] failed with return value [%d].\n",
                           cmd.c_str(), ret));
    err_msg = "Execute [" + cmd + "] failed with return value [" +
              std::to_string(ret) + "].";
    ib::error(ER_CLONE_APPLY_FILE_ERROR) << err_msg;
    return true;
  }
  return false;
}
