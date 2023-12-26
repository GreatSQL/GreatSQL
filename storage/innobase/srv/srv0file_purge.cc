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
#include "my_inttypes.h"
#include "sql/mysqld.h"
#include "sql/mysqld_thd_manager.h"
#include "srv0start.h"

#include "srv0file_purge.h"

#ifdef UNIV_PFS_THREAD
/* File purge thread PFS key */
mysql_pfs_key_t srv_file_async_purge_thread_key;
#endif

#ifdef UNIV_PFS_MUTEX
/* File purge list mutex PFS key */
mysql_pfs_key_t file_async_purge_list_mutex_key;
#endif

/** Whether purge all when normal shutdown */
bool srv_data_file_async_purge_all_at_shutdown = false;

/** Time interval (milliseconds) every data file purge operation */
ulong srv_data_file_async_purge_interval = 100;

/** Max size (Byte) every data file purge operation */
unsigned long long srv_data_file_async_purge_max_size = 256ULL
                                                        << 20; /** 256MB */

/** Retry count when purging file with errors */
uint srv_data_file_async_purge_error_retry_count = 10;

/** File which size(Byte) greater than this will be purge little by little */
unsigned long long srv_data_force_async_purge_file_size = 10ULL
                                                          << 30; /** 10GB */

/** Whether to print data file purge process */
bool srv_print_data_file_async_purge_process = false;

/** Indicate whether file purge system initted */
static bool file_async_purge_system_inited = false;

/** Purge thread event condition */
os_event_t file_async_purge_event;

/** Data file purge system initialize when InnoDB server boots */
void srv_file_purge_init() {
  file_purge_sys = ut::new_withkey<File_purge>(
      UT_NEW_THIS_FILE_PSI_KEY, Global_THD_manager::reserved_thread_id,
      server_start_time);

  file_async_purge_event = os_event_create();

  /** purge dir format is datadir/#file_purge, if create subdir #file_purge
   * failed, use datadir directly. */
  std::string abs_datadir = MySQL_datadir_path.abs_path();
  std::string purgedir = abs_datadir + "#file_purge";

  os_file_type_t type;
  bool exists;
  bool success = os_file_status(purgedir.c_str(), &exists, &type);

  if ((!success || !exists) &&
      ((os_file_create_subdirs_if_needed(purgedir.c_str()) != DB_SUCCESS) ||
       (!os_file_create_directory(purgedir.c_str(), false)))) {
    /* dir not exist and create failed, use innodb data dir */
    ulint os_err = os_file_get_last_error(false);
    if (0 != os_err)
      ib::info(ER_INNODB_ERROR_LOGGER_MSG)
          << "File purge : create purge dir failed : " << purgedir
          << ", OS_FILE_ERROR=" << os_err << ", use datadir as purge file.";
    purgedir = abs_datadir;
  }

  file_purge_sys->set_dir(purgedir);
  ib::info(ER_INNODB_ERROR_LOGGER_MSG)
      << "File purge : set file purge path : " << purgedir;

  file_async_purge_system_inited = true;
}

/** Data file purge system destroy when InnoDB server shutdown */
void srv_file_purge_destroy() {
  ut::delete_(file_purge_sys);
  os_event_destroy(file_async_purge_event);
  file_async_purge_system_inited = false;
}

/* Data file purge thread runtime */
void srv_file_async_purge_thread(void) {
  int64_t sig_count;
  ut_a(file_purge_sys);
  int truncated = 0;
  unsigned long long truncated_size = 0;

loop:
  std::pair<int, unsigned long long> result;
  if (DBUG_EVALUATE_IF("innodb_do_not_purge_file", true, false)) {
    result = std::make_pair(0, 0);
  } else {
    result =
        file_purge_sys->purge_file(srv_data_file_async_purge_max_size, false);
  }

  truncated = result.first;
  truncated_size += result.second;

  if (truncated <= 0) {
    sig_count = os_event_reset(file_async_purge_event);
    os_event_wait_time_low(file_async_purge_event,
                           std::chrono::microseconds(5000000), sig_count);
    truncated_size = 0;
  } else if (truncated > 0) {
    if (truncated_size >= srv_data_file_async_purge_max_size) {
      std::this_thread::sleep_for(
          std::chrono::microseconds(srv_data_file_async_purge_interval * 1000));
      truncated_size = 0;
    }
  }

  if (srv_shutdown_state.load() >= SRV_SHUTDOWN_CLEANUP) goto exit_func;

  goto loop;

exit_func:
  /**
    Purge all renamed tmp data file requirement when shutdown:
      - innodb_fast_shutdown = 0 or 1;
      - innodb_data_file_async_purge_all_at_shutdown is true;

    It will unlink files regardless of file size.
  */
  if (srv_fast_shutdown < 2 && srv_data_file_async_purge_all_at_shutdown) {
    ib::info(ER_INNODB_ERROR_LOGGER_MSG)
        << "File purge: cleaning begin, number of tmp files is "
        << file_purge_sys->length();

    file_purge_sys->purge_all(srv_data_file_async_purge_max_size, true);
    ib::info(ER_INNODB_ERROR_LOGGER_MSG) << "File purge: cleaning end";
  }
}

/** Wakeup the background thread when shutdown */
void srv_wakeup_file_purge_thread() { os_event_set(file_async_purge_event); }
