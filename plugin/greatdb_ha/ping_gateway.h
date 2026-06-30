/* Copyright (c) 2026, GreatDB Software Co., Ltd.

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

#ifndef PLUGIN_GDB_HA_PING_H
#define PLUGIN_GDB_HA_PING_H

#include <arpa/inet.h>
#include <netinet/ip_icmp.h>
#include "sql/rpl_group_replication.h"
#include "sql/server_component/gdb_cmd_service.h"
#include "sql/sql_class.h"

namespace greatdb {
#define PACKET_SIZE 4096
#define MIN_FLP_TIMEOUT \
  3  // in "plugin/group_replication/include/plugin_variables.h"
#define DEFAULT_PING_TIMEOUT 1
#define DEFAULT_PING_INTERVAL 1

extern MYSQL_PLUGIN plugin_ptr;
extern bool all_thread_need_exit;
extern my_thread_handle ping_thread;
extern bool is_stopped_by_ha;
extern int ping_sock;
extern pthread_mutex_t ping_mutex;
extern pthread_cond_t ping_cv;
extern time_t last_ping_succ_time;
extern time_t last_ping_fail_time;
extern char *gateway_address_var;
extern sa_family_t ping_family;

sa_family_t check_ip_version(const char *ip);

/*----------------------------------------------------------------
ping gateway thread begin
---------------------------------------------------------------*/
unsigned short get_checksum(unsigned short *buf, int len);

int send_icmp_packet(int ping_sock_fd);

ssize_t recv_icmp_packet(int ping_sock_fd);

void gdb_cmd_run_force_member();

void get_last_ping_time_char(char *ret, bool is_succ = true);

bool ping_gateway(int ping_sock_fd, const char *gateway_ip);

void *ping_func(void *);

/*----------------------------------------------------------------
ping gateway thread end
---------------------------------------------------------------*/
}  // namespace greatdb

#endif
