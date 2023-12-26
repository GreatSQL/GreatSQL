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

#include "fil0purge.h"
#include "fil0fil.h"
#include "os0file.h"
#include "row0mysql.h"
#include "srv0file_purge.h"
#include "ut0mutex.h"

/* Global file purge system */
File_purge *file_purge_sys = nullptr;

/** Constructor */
File_purge::File_purge(my_thread_id thread_id, time_t start_time)
    : m_thread_id(thread_id), m_start_time(start_time), m_id(0), m_dir() {
  mutex_create(LATCH_ID_FILE_ASYNC_PURGE_LIST, &m_mutex);
  UT_LIST_INIT(m_list);
}

File_purge::~File_purge() {
  /**
    Clear the file nodes, those will be purged again when DDL log recovery after
    reboot
 */
  for (file_purge_node_t *node = UT_LIST_GET_FIRST(m_list); node != nullptr;
       node = UT_LIST_GET_FIRST(m_list)) {
    if (node->m_file_path) ut::free(node->m_file_path);
    if (node->m_original_path) ut::free(node->m_original_path);
    UT_LIST_REMOVE(m_list, node);
    ut::delete_(node);
  }

  mutex_free(&m_mutex);
}

void File_purge::lock() { mutex_enter(&m_mutex); }

void File_purge::unlock() { mutex_exit(&m_mutex); }

/**
  Iterate the file nodes in list.

  @param[in]  first       Whether it is first retrieve from list
  @param[in]  last        Last iterated file node

  @retval     node        The file purge node in list
*/
file_purge_node_t *File_purge::iterate_node(bool first,
                                            file_purge_node_t *last) {
  DBUG_ENTER("File_purge::iterate_node");

  if (first) {
    ut_ad(last == nullptr);
    DBUG_RETURN(UT_LIST_GET_FIRST(m_list));
  } else {
    ut_ad(last != nullptr);
    DBUG_RETURN(UT_LIST_GET_NEXT(LIST, last));
  }
}

/* Get next unique id number */
ulint File_purge::next_id() {
  ulint number;
  number = m_id++;
  return number;
}
/**
  Add file into purge list.
  If error occurs in os file operation, remove file purge log, the file should
  be manually deleted.
  TODO: Is there any better way to deal with errors in file operation?

  @param[in]    id                  Log DDL record id
  @param[int]   path                The temporary file name
  @param[int]   original_path       The original file name

  @retval       false     Success
  @retval       true      Failure
*/
bool File_purge::add_file(ulint id, const char *path,
                          const char *original_path) {
  os_file_size_t file_size{0, 0};
  file_purge_node_t *node = nullptr;

  if (os_file_exists(path)) {
    file_size = os_file_get_size(path);
    if (((os_offset_t)(~0) == file_size.m_total_size) &&
        ((os_offset_t)errno == file_size.m_alloc_size)) {
      ib::error(ER_IB_MSG_392)
          << "File purge : add file : cannot get temp data file size, path : "
          << path << ", errno=" << errno;
      goto deal_err;
    }
  } else {
    if (2 == errno) {
      ib::error(ER_IB_MSG_392)
          << "File purge : add file : temp data file not exist, path : "
          << path;
      ut::free(const_cast<char *>(path));
      log_ddl->remove_by_id(id);
      return true;
    } else {
      ib::error(ER_IB_MSG_392) << "File purge : add file : failed to detect if "
                                  "the file exists, path : "
                               << path << ", errno=" << errno;
      goto deal_err;
    }
  }

  node = ut::new_withkey<file_purge_node_t>(UT_NEW_THIS_FILE_PSI_KEY);
  if (!node) {
    ib::error(ER_IB_MSG_392)
        << "File purge : add file : cannot alloc memory for file purge, path : "
        << path;
    goto deal_err;
  }

  node->m_log_ddl_id = id;
  node->m_file_path = const_cast<char *>(path);
  node->m_start_time = time(NULL);

  /* Original path will be null when ddl log recovery */
  node->m_original_path = original_path ? mem_strdup(original_path) : nullptr;

  node->m_original_size = file_size.m_total_size;
  /**
    Current size will change when purge file,
    and is not protected by mutex.
  */
  node->m_current_size = file_size.m_total_size;

  node->m_error_cnt = 0;

  if (srv_print_data_file_async_purge_process)
    ib::info(ER_INNODB_ERROR_LOGGER_MSG)
        << "File purge : add file : add file to purge list, log_id=" << id
        << ", path : " << path;

  mutex_enter(&m_mutex);
  UT_LIST_ADD_LAST(m_list, node);
  mutex_exit(&m_mutex);

  /* Wake up background thread */
  srv_wakeup_file_purge_thread();

  return false;

deal_err:

  ib::error(ER_INNODB_ERROR_LOGGER_MSG)
      << "File purge : add file : add file failed, file must be manually "
         "deleted, path : "
      << path;
  ut::free(const_cast<char *>(path));
  log_ddl->remove_by_id(id);
  return true;
}

void File_purge::deal_purge_file_error(const char *file_operate,
                                       const ulint &os_file_err,
                                       file_purge_node_t *node, bool retry) {
  if (retry &&
      (node->m_error_cnt < srv_data_file_async_purge_error_retry_count)) {
    mutex_enter(&m_mutex);
    ulint len = UT_LIST_GET_LEN(m_list);
    if (len > 1) {
      UT_LIST_REMOVE(m_list, node);
      UT_LIST_ADD_LAST(m_list, node);
    }
    ++node->m_error_cnt;
    node->m_message.reserve(PURGE_FILE_MESSAGE_MAX_LEN);
    node->m_message.assign("File purge attempt ");
    node->m_message.append(std::to_string(node->m_error_cnt));
    node->m_message.append(" of ");
    node->m_message.append(
        std::to_string(srv_data_file_async_purge_error_retry_count));
    node->m_message.append(", the last OS_FILE_ERROR is ");
    node->m_message.append(std::to_string(os_file_err));
    mutex_exit(&m_mutex);

    ib::error(ER_INNODB_ERROR_LOGGER_MSG)
        << "File purge : purge file : " << file_operate
        << " failed, path : " << node->m_file_path
        << ", OS_FILE_ERROR=" << os_file_err << ", attempt "
        << node->m_error_cnt << " of "
        << srv_data_file_async_purge_error_retry_count;
  } else {
    ib::error(ER_INNODB_ERROR_LOGGER_MSG)
        << "File purge : purge file : " << file_operate
        << " failed, path : " << node->m_file_path
        << ", file must be manually deleted, error count=" << node->m_error_cnt
        << ", OS_FILE_ERROR=" << os_file_err;
    remove_file(node);
  }
}

/**
  Purge the first file node by purge max size.
  If error occurs in os file operation, move the file node to the end of the
  purge list, if errors count of a file reaches innodb_file_purge_error_retry,
  delete the file node from purge list, the temp file should be manually
  deleted.
  TODO: Is there any better way to deal with errors in file operation?

  @param[in]  size        Purge max size (bytes)
  @param[in]  force       Whether unlink file directly

  First return in std::pair
  @retval       -1        Error
  @retval       0         Success & no file purge
  @retval       >0        How many purged operation

  Second return in std::pair
  @retval       size      Truncated file size
*/
std::pair<int, unsigned long long> File_purge::purge_file(
    unsigned long long size, bool force) {
  bool success = false;
  uint truncated = 0;
  file_purge_node_t *node = nullptr;
  pfs_os_file_t handle;
  unsigned long long truncated_size = 0;
  ulint os_file_err = 0;

  mutex_enter(&m_mutex);
  node = UT_LIST_GET_FIRST(m_list);
  mutex_exit(&m_mutex);

  if (node) {
    handle = os_file_create_simple_no_error_handling(
        innodb_data_file_key, node->m_file_path, OS_FILE_OPEN,
        OS_FILE_READ_WRITE, false, &success);
    if (DBUG_EVALUATE_IF("fp_create_file_handle_fail", true, false) ||
        !success) {
      os_file_err = os_file_get_last_error(false);
      /* Maybe delete by DBA, so remove file directly */
      if (OS_FILE_NOT_FOUND == os_file_err) {
        ib::error(ER_IB_MSG_392)
            << "File purge : purge file : temp data file not exist: "
            << node->m_file_path << ", remove the file from list.";
        remove_file(node);
      } else {
        deal_purge_file_error("create file handle", os_file_err, node, !force);
      }
      return std::make_pair(-1, 0);
    }

    os_offset_t file_size = os_file_get_size(handle);
    if (DBUG_EVALUATE_IF("fp_get_file_size_fail", true, false) ||
        (((os_offset_t)(-1) == file_size) &&
         (os_file_err = os_file_get_last_error(false)))) {
      deal_purge_file_error("get file size", os_file_err, node, !force);
      return std::make_pair(-1, 0);
    }
    if (!force && file_size > size) {
      ut_ad(file_size == node->m_current_size);

      /* Truncate predefined size once */
      if (DBUG_EVALUATE_IF("fp_truncate_file_fail", true, false)) {
        success = false;
      } else {
        success = os_file_truncate(node->m_file_path, handle, file_size - size);
      }
      if (!success) {
        os_file_err = os_file_get_last_error(false);
        deal_purge_file_error("truncate file", os_file_err, node, !force);
        os_file_close(handle);
        return std::make_pair(-1, 0);
      }

      success = os_file_close(handle);
      if (!success) {
        os_file_err = os_file_get_last_error(false);
        deal_purge_file_error("close file", os_file_err, node, !force);
        return std::make_pair(-1, 0);
      }

      truncated++;
      node->m_current_size = (file_size - size);
      truncated_size += size;
      /* purge success, reset message, use m_mutex protect m_message. */
      if (node->m_error_cnt > 0) {
        mutex_enter(&m_mutex);
        node->m_error_cnt = 0;
        node->m_message = "";
        mutex_exit(&m_mutex);
      }
    } else {
      /* Direct delete the file when file_size < predefined size */
      success = os_file_close(handle);
      if (!success) {
        os_file_err = os_file_get_last_error(false);
        deal_purge_file_error("close file", os_file_err, node, !force);
        return std::make_pair(-1, 0);
      }

      success = os_file_delete(innodb_data_file_key, node->m_file_path);
      if (!success) {
        os_file_err = os_file_get_last_error(false);
        deal_purge_file_error("delete file", os_file_err, node, !force);
        return std::make_pair(-1, 0);
      }
      remove_file(node);
      truncated++;
      truncated_size += file_size;
    }
  } else {
    return std::make_pair(0, 0);
  }

  return std::make_pair(truncated, truncated_size);
}

/**
  The data file list length

  @retval       >=0
*/
ulint File_purge::length() {
  ulint len;
  mutex_enter(&m_mutex);
  len = UT_LIST_GET_LEN(m_list);
  mutex_exit(&m_mutex);

  return len;
}
/**
  Purge all the data file cached in list.

  @param[in]    size      Purge max size (bytes)
  @param[in]    force     Purge little by little
                          or unlink directly.
*/
void File_purge::purge_all(unsigned long long size, bool force) {
  while (length() > 0) {
    purge_file(size, force);
  }
}
/**
  Remove the file node from list

  @param[in]    node      File purge node pointer
*/
void File_purge::remove_file(file_purge_node_t *node) {
  ulint log_id = node->m_log_ddl_id;

  mutex_enter(&m_mutex);
  UT_LIST_REMOVE(m_list, node);
  mutex_exit(&m_mutex);

  if (srv_print_data_file_async_purge_process)
    ib::info(ER_INNODB_ERROR_LOGGER_MSG)
        << "File purge : remove file : log_id=" << log_id
        << ", path : " << node->m_file_path;
  ut::free(node->m_file_path);
  ut::free(node->m_original_path);
  ut::delete_(node);

  log_ddl->remove_by_id(log_id);
}
/**
  Generate a unique temporary file name.

  @param[in]    filepath      Original file name

  @retval       file name     Generated file name
*/
char *File_purge::generate_file(const char *filepath) {
  std::string new_path;

  new_path.assign(get_dir());

  std::string temp_filename;
  temp_filename.assign(PREFIX);
  temp_filename.append(std::to_string((ulong)m_start_time));
  temp_filename.append("_");
  temp_filename.append(std::to_string(next_id()));

  char *new_file = Fil_path::make(new_path, temp_filename, NO_EXT, false);

  ib::info(ER_INNODB_ERROR_LOGGER_MSG)
      << "File purge : generate file : origin path : " << filepath
      << ", new path : " << new_file;
  return new_file;
}

/**
  Drop a single-table tablespace and rename the data file as temporary file.
  This deletes the fil_space_t if found and rename the file on disk.

  @param[in]      space_id      Tablespace id
  @param[in]      filepath      File path of tablespace to delete

  @retval         error code */
dberr_t row_purge_single_table_tablespace(space_id_t space_id,
                                          const char *filepath) {
  dberr_t err = DB_SUCCESS;
  uint64_t log_id;

  char *new_filepath = file_purge_sys->generate_file(filepath);

  log_ddl->write_purge_file_log(&log_id, file_purge_sys->get_thread_id(),
                                new_filepath);

  if (srv_print_data_file_async_purge_process)
    ib::info(ER_INNODB_ERROR_LOGGER_MSG)
        << "File purge : write log : log_id=" << log_id
        << ", new file path : " << new_filepath;

  if (!fil_space_exists_in_mem(space_id, nullptr, true, false)) {
    std::string err_msg;
    if (fil_purge_file(filepath, new_filepath) &&
        srv_print_data_file_async_purge_process) {
      ib::info(ER_INNODB_ERROR_LOGGER_MSG)
          << "File purge : rename the ibd data file and delete the releted "
             "files success, log_id="
          << log_id;
    }

  } else {
    err = fil_delete_tablespace(space_id, BUF_REMOVE_FLUSH_NO_WRITE,
                                new_filepath);
  }

  DBUG_EXECUTE_IF("ddl_log_crash_after_rename_file", DBUG_SUICIDE(););

  file_purge_sys->add_file(log_id, new_filepath, filepath);

  return err;
}

/**
  Rename the ibd data file and delete the releted files

  @param[in]      old_filepath  The original data file
  @param[in]      new_filepath  The new data file

  @retval         TRUE         Success
  @retval         FALSE        Failure
*/
bool fil_purge_file(const char *old_filepath, const char *new_filepath) {
  bool success = true;
  bool exist;
  os_file_type_t type;
  ulint os_file_err = 0;

  /* If old file has been renamed or dropped, just skip it */
  success = os_file_status(old_filepath, &exist, &type);
  if (!success) {
    os_file_err = os_file_get_last_error(false);
    ib::error(ER_INNODB_ERROR_LOGGER_MSG)
        << "File purge : get ibd file status failed, path : " << old_filepath
        << ", OS_FILE_ERROR=" << os_file_err;
  }

  if (exist &&
      (DBUG_EVALUATE_IF("innodb_file_purge_rename_fail_2", true, false) ||
       (!(success = os_file_rename(innodb_data_file_key, old_filepath,
                                   new_filepath))))) {
    os_file_err = os_file_get_last_error(false);
    ib::error(ER_INNODB_ERROR_LOGGER_MSG)
        << "File purge : rename ibd file failed, delete ibd file directly, old "
           "path : "
        << old_filepath << ", new path : " << new_filepath
        << ", OS_FILE_ERROR=" << os_file_err;

    success =
        os_file_delete_if_exists(innodb_data_file_key, old_filepath, nullptr);
    if (!success) {
      os_file_err = os_file_get_last_error(false);
      ib::error(ER_INNODB_ERROR_LOGGER_MSG)
          << "FIle purge : delete ibd file failed, path : " << old_filepath
          << ", OS_FILE_ERROR=" << os_file_err;
    }
  }

  char *cfg_filepath = Fil_path ::make_cfg(old_filepath);

  if (cfg_filepath != nullptr) {
    success =
        os_file_delete_if_exists(innodb_data_file_key, cfg_filepath, nullptr);
    if (!success) {
      os_file_err = os_file_get_last_error(false);
      ib::error(ER_INNODB_ERROR_LOGGER_MSG)
          << "FIle purge : delete cfg file failed, path : " << cfg_filepath
          << ", OS_FILE_ERROR=" << os_file_err;
    }

    ut::free(cfg_filepath);
  }

  char *cfp_filepath = Fil_path ::make_cfp(old_filepath);

  if (cfp_filepath != nullptr) {
    success =
        os_file_delete_if_exists(innodb_data_file_key, cfp_filepath, nullptr);
    if (!success) {
      os_file_err = os_file_get_last_error(false);
      ib::error(ER_INNODB_ERROR_LOGGER_MSG)
          << "FIle purge : delete cfg file failed, path : " << cfp_filepath
          << ", OS_FILE_ERROR=" << os_file_err;
    }

    ut::free(cfp_filepath);
  }

  return (success);
}

/**
  Drop or purge single table tablespace

  @param[in]    space_id      tablespace id
  @param[in]    filepath      tablespace data file path


  @retval     DB_SUCCESS or error
*/
dberr_t row_drop_or_purge_single_table_tablespace(THD *thd, space_id_t space_id,
                                                  const char *filepath) {
  os_file_size_t file_size{0, 0};

  if (os_file_exists(filepath)) {
    file_size = os_file_get_size(filepath);
    if (((os_offset_t)(~0) == file_size.m_total_size) &&
        ((os_offset_t)errno == file_size.m_alloc_size)) {
      ib::error(ER_INNODB_ERROR_LOGGER_MSG)
          << "File purge : get file size failed, delete data file directly, "
             "file path : "
          << filepath << ", errno=" << errno;
      return row_drop_tablespace(space_id, filepath);
    }
  } else {
    if (2 != errno)
      ib::error(ER_INNODB_ERROR_LOGGER_MSG)
          << "File purge : failed to detect if the file exists, path : "
          << filepath << ", errno=" << errno;
    return DB_TABLESPACE_NOT_FOUND;
  }

  if (thd_data_file_async_purge(thd) ||
      file_size.m_total_size > srv_data_force_async_purge_file_size)
    return row_purge_single_table_tablespace(space_id, filepath);
  else
    return row_drop_tablespace(space_id, filepath);
}
