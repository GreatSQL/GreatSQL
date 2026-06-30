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

#include "ping_gateway.h"

namespace greatdb {
char sendpacket[PACKET_SIZE];
char recvpacket[PACKET_SIZE];
int datalen = 56;
int send_id = 0;
struct sockaddr_in dest_addr;
struct sockaddr_in6 dest_addr6;

/*----------------------------------------------------------------
ping gateway thread begin
---------------------------------------------------------------*/
unsigned short get_checksum(unsigned short *buf, int len) {
  unsigned int sum = 0;
  for (sum = 0; len > 1; len -= 2) sum += *buf++;
  if (len == 1) sum += *(unsigned char *)buf;
  sum = (sum >> 16) + (sum & 0xffff);
  sum += (sum >> 16);
  return ~sum;
}

int send_icmp_packet(int ping_sock_fd) {
  struct timeval tv;
  struct icmp *icmp;
  gettimeofday(&tv, NULL);
  icmp = (struct icmp *)sendpacket;
  icmp->icmp_type = ICMP_ECHO;
  icmp->icmp_code = 0;
  icmp->icmp_seq = 0;
  icmp->icmp_id = getpid() + ping_sock_fd;
  send_id = getpid();
  icmp->icmp_cksum = 0;
  memset(icmp->icmp_data, 0xff, datalen);
  icmp->icmp_cksum =
      get_checksum((unsigned short *)icmp, sizeof(struct icmp) + datalen);
  int ret =
      ping_family == AF_INET
          ? sendto(ping_sock_fd, sendpacket, sizeof(struct icmp) + datalen, 0,
                   (struct sockaddr *)&dest_addr, sizeof(dest_addr))
          : sendto(ping_sock_fd, sendpacket, sizeof(struct icmp) + datalen, 0,
                   (struct sockaddr *)&dest_addr6, sizeof(dest_addr6));
  return ret;
}

ssize_t recv_icmp_packet(int ping_sock_fd) {
  ssize_t len, n;
  struct iphdr *iph;
  struct icmp *icmp;
  while (1) {
    memset(recvpacket, 0, sizeof(recvpacket));
    len = ping_family == AF_INET ? sizeof(dest_addr) : sizeof(dest_addr6);
    n = ping_family == AF_INET
            ? recvfrom(ping_sock_fd, recvpacket, sizeof(recvpacket), 0,
                       (struct sockaddr *)&dest_addr, (socklen_t *)&len)
            : recvfrom(ping_sock_fd, recvpacket, sizeof(recvpacket), 0,
                       (struct sockaddr *)&dest_addr6, (socklen_t *)&len);
    if (n == -1) return -1;
    if (n < (ssize_t)(sizeof(struct iphdr) + sizeof(struct icmp))) continue;
    iph = (struct iphdr *)recvpacket;
    icmp = (struct icmp *)(recvpacket + (iph->ihl << 2));
    if (icmp->icmp_type == ICMP_ECHOREPLY &&
        icmp->icmp_id == (uint16_t)(send_id + ping_sock_fd)) {
      break;
    } else {
      continue;
    }
  }
  return n;
}

void gdb_cmd_run_force_member() {
  Gdb_cmd_service cmd_service;
  std::string set_force_member_local_address =
      "set global "
      "group_replication_force_members=@@group_replication_local_address";
  (void)cmd_service.execute_sql(set_force_member_local_address);
}

void get_last_ping_time_char(char *ret, bool is_succ) {
  if (!ret) return;
  tm *trans_local_time = nullptr;
  if (is_succ && last_ping_succ_time > 0) {
    trans_local_time = localtime(&last_ping_succ_time);
  } else if (!is_succ && last_ping_fail_time > 0) {
    trans_local_time = localtime(&last_ping_fail_time);
  }
  if (nullptr != trans_local_time) {
    strftime(ret, 64, "%Y-%m-%d %H:%M:%S", trans_local_time);
  }
}

bool ping_gateway(int ping_sock_fd, const char *gateway_ip) {
  if (!gateway_ip || !strlen(gateway_ip)) return true;
  int n;
  int size = 50 * 1024;
  struct timeval tv = {DEFAULT_PING_TIMEOUT, 0};
  if (setsockopt(ping_sock_fd, SOL_SOCKET, SO_RCVBUF, &size, sizeof(size)) <
          0 ||
      setsockopt(ping_sock_fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0) {
    my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                          "Error: set ping_gateway socket option failed.");
    return false;
  }
  bzero(&dest_addr, sizeof(dest_addr));
  bzero(&dest_addr6, sizeof(dest_addr6));
  if (ping_family == AF_INET &&
      inet_pton(AF_INET, gateway_ip, &dest_addr.sin_addr) == 1) {
    dest_addr.sin_family = AF_INET;
  } else if (ping_family == AF_INET6 &&
             inet_pton(AF_INET6, gateway_ip, &dest_addr6.sin6_addr) == 1) {
    dest_addr6.sin6_family = AF_INET6;
  } else {
    my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                          "Error: gateway ip is wrong.");
    return false;
  }
  if (send_icmp_packet(ping_sock_fd) < 0) {
    my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                          "Error:send ping to gateway failed.");
    return false;
  }
  n = recv_icmp_packet(ping_sock_fd);
  if (n == -1) {
    my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                          "Error:receive ping from gateway failed.");
    return false;
  }
  return true;
}

void *ping_func(void *) {
  my_thread_init();
  THD *thd;
  if (!(thd = new (std::nothrow) THD)) {
    my_thread_end();
    return nullptr;
  }
  thd->thread_stack = (char *)&thd;
  thd->store_globals();
  pthread_mutex_lock(&ping_mutex);

  if (!gateway_address_var || !strlen(gateway_address_var))
    pthread_cond_wait(&ping_cv, &ping_mutex);
  pthread_mutex_unlock(&ping_mutex);
  if (all_thread_need_exit) goto end;
  ping_family = greatdb::check_ip_version(gateway_address_var);
  ping_sock = socket(ping_family, SOCK_RAW, IPPROTO_ICMP);
  if (ping_sock < 0) {
    my_plugin_log_message(
        &plugin_ptr, MY_ERROR_LEVEL,
        "have no access to ping gateway, need set CAP_NET_RAW capability.");
    pthread_mutex_unlock(&ping_mutex);
    goto end;
  }
  while (1) {
    if (all_thread_need_exit) {
      break;
    }
    pthread_mutex_lock(&ping_mutex);
    if (!gateway_address_var || !strlen(gateway_address_var))
      pthread_cond_wait(&ping_cv, &ping_mutex);
    if (ping_gateway(ping_sock, gateway_address_var)) {
      last_ping_succ_time = time(NULL);
      // need restart group_replication if stop by HA
      if (is_stopped_by_ha && !is_group_replication_running()) {
        my_plugin_log_message(&plugin_ptr, MY_WARNING_LEVEL,
                              "Ping gateway success and stop group_replication "
                              "by HA, will try restart group_replication.");
        char *error_message = nullptr;
        if (group_replication_start(&error_message, thd))
          my_plugin_log_message(&plugin_ptr, MY_INFORMATION_LEVEL,
                                "start group replication failed, case %s",
                                error_message);
        is_stopped_by_ha = false;
      }
    } else {
      last_ping_fail_time = time(NULL);
    }
    pthread_mutex_unlock(&ping_mutex);
    if (all_thread_need_exit) {
      break;
    }
    sleep(DEFAULT_PING_INTERVAL);
  }
  close(ping_sock);
  goto end;
end:
  delete thd;
  my_thread_end();
  return nullptr;
}

/*----------------------------------------------------------------
ping gateway thread end
---------------------------------------------------------------*/
}  // namespace greatdb
