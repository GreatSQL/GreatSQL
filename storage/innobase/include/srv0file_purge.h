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

#ifndef srv0file_purge_h
#define srv0file_purge_h

#ifdef UNIV_PFS_THREAD
/* File purge thread PFS key */
extern mysql_pfs_key_t srv_file_async_purge_thread_key;
#endif

#ifdef UNIV_PFS_MUTEX
/* File purge list mutex PFS key */
extern mysql_pfs_key_t file_async_purge_list_mutex_key;
#endif

extern bool srv_data_file_async_purge_all_at_shutdown;
extern uint srv_data_file_async_purge_error_retry_count;
extern ulong srv_data_file_async_purge_interval;
extern unsigned long long srv_data_force_async_purge_file_size;
extern unsigned long long srv_data_file_async_purge_max_size;
extern bool srv_print_data_file_async_purge_process;

extern void srv_file_purge_init();
extern void srv_wakeup_file_purge_thread();
extern void srv_file_purge_destroy();
extern void srv_file_async_purge_thread(void);

extern bool thd_data_file_async_purge(THD *thd);

struct os_event;
typedef struct os_event *os_event_t;
extern os_event_t file_async_purge_event;

#endif  // srv0file_purge_h
