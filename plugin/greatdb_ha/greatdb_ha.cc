/* Copyright (c) 2023, 2024, GreatDB Software Co., Ltd.

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

#include <arpa/inet.h>
#include <ctype.h>
#include <fcntl.h>
#include <ifaddrs.h>
#include <net/ethernet.h>
#include <net/if.h>
#include <netdb.h>
#include <netinet/icmp6.h>
#include <netinet/in.h>
#include <netinet/ip6.h>
#include <netinet/ip_icmp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <atomic>
#include <cstring>

#include <queue>
#include <set>
#include "gdb_service_registrator.h"
#include "m_string.h"  // strlen
#include "my_dbug.h"   // for DBUG_TRACE
#include "my_dir.h"
#include "my_inttypes.h"
#include "my_io.h"
#include "my_loglevel.h"  // for INFORMATION_LEVEL
#include "my_psi_config.h"
#include "my_sys.h"  // my_write, my_malloc
#include "my_thread.h"
#include "mysql.h"
#include "mysql/components/service.h"
#include "mysql/components/service_implementation.h"  // for DEFINE_BOOL_METHOD
#include "mysql/components/services/group_member_status_listener.h"
#include "mysql/components/services/group_membership_listener.h"
#include "mysql/components/services/log_builtins.h"
#include "mysql/plugin.h"
#include "mysql/plugin_group_replication.h"  // for GROUP_REPLICATI...
#include "mysql/psi/mysql_memory.h"
#include "mysql_com.h"  // for NAME_LEN
#include "sql/mysqld.h"
#include "sql/mysqld_thd_manager.h"
#include "sql/replication.h"
#include "sql/rpl_group_replication.h"  // for get_group_repli...
#include "sql/rpl_msr.h"                // channel_map
#include "sql/server_component/gdb_cmd_service.h"
#include "sql/sql_class.h"
#include "sql/sql_const.h"

namespace greatdb {

#define HEART_STRING_BUFFER 200
#define DEST_MAC \
  { 0xFF, 0xff, 0xff, 0xff, 0xff, 0xff }
#define DEST_IP6_ADDR "ff02::1"
#define HA_CONNECT_TIMEOUT 3
#define HA_BIND_RETRY_TIMES 5

/*ping variables begin*/
#define PACKET_SIZE 4096
#define MAX_WAIT_TIME 5
char sendpacket[PACKET_SIZE];
char recvpacket[PACKET_SIZE];
int datalen = 56;
int send_id = 0;
int ping_sock = -1;
bool need_stopped_by_ping = false;
struct sockaddr_in dest_addr;
struct sockaddr_in6 dest_addr6;
my_thread_handle ping_thread;
pthread_mutex_t ping_mutex;
pthread_cond_t ping_cv;
bool is_register_services;
/*ping variables end*/

char *mgr_vip_addr;
enum read_vip_floating_policy_t {
  TO_PRIMARY,
  TO_ANOTHER_SECONDARY,
};

enum message_type_t {
  NO_MESSAGE = 0,
  GET_BIND_VIPS,
  SET_ALL_NODE_BIND_VIPS,
  GET_BIND_VIPS_REPLY,
  OK_REPLY,
  ERROR_REPLY,
  YOU_ARE_NOT_PRIMARY,
};

struct st_row_group_members {
  enum class State {
    MGR_ONLINE,
    MGR_RECOVERING,
    MGR_UNREACHABLE,
    MGR_OFFLINE,
    MGR_ERROR,
  };
  enum class Role {
    ROLE_PRIMARY,
    ROLE_SECONDARY,
  };
  std::string member_id;
  std::string member_host;
  unsigned int member_port;
  State member_state;
  Role member_role;
  bool is_invalid = false;
};

struct st_row_group_member_stats {
  char channel_name[CHANNEL_NAME_LENGTH];
  uint channel_name_length;
  char view_id[HOSTNAME_LENGTH];
  uint view_id_length;
  char member_id[UUID_LENGTH];
  uint member_id_length;
  ulonglong trx_in_queue;
  ulonglong trx_checked;
  ulonglong trx_conflicts;
  ulonglong trx_rows_validating;
  char *trx_committed;
  size_t trx_committed_length;
  char last_cert_trx[Gtid::MAX_TEXT_LENGTH + 1];
  int last_cert_trx_length;
  ulonglong trx_remote_applier_queue;
  ulonglong trx_remote_applied;
  ulonglong trx_local_proposed;
  ulonglong trx_local_rollback;
};

struct greatdb_ha_message {
  void set_message_type(enum message_type_t type) {
    message_length = 2 * sizeof(int) + sizeof(message_type_t);
    message_type = type;
    view_id_length = 0;
  }
  void set_view_id(std::string view_id_stamp, int view_id_version) {
    if (!view_id_stamp.empty() && view_id_version > 0) {
      view_id_stamp += ":" + std::to_string(view_id_version);
      view_id_length = view_id_stamp.size();
      memcpy(message_content, view_id_stamp.c_str(), view_id_length);
      message_length += view_id_length;
    }
  }
  void set_message_content(const char *message) {
    if (message) {
      memcpy(message_content + view_id_length, message, strlen(message));
      message_length += strlen(message);
    }
  }
  bool get_view_id(std::string &recv_view_id_stamp, int &recv_view_id_version) {
    if (view_id_length <= 0) return false;
    std::string str_view_id(message_content, view_id_length);
    std::size_t view_id_position = str_view_id.find(":");
    if (str_view_id.size() != static_cast<long unsigned int>(view_id_length) ||
        view_id_position == 0 || view_id_position == std::string::npos) {
      return false;
    }
    recv_view_id_stamp = str_view_id.substr(0, view_id_position);
    recv_view_id_version = std::stoi(str_view_id.substr(view_id_position + 1));
    return true;
  }
  message_type_t get_message_type() { return message_type; }
  char *get_message_content() { return message_content + view_id_length; }

  int message_length;
  message_type_t message_type;
  int view_id_length;
  char message_content[1];
};

char send_message_buf[1024];
char recv_message_buf[1024];
char *mgr_write_vip_addr;
char *mgr_read_vip_addrs;
char all_vip_tope_value[1024];
char *all_vip_tope = all_vip_tope_value;
ulong read_vip_floating_type;
bool is_primary_for_check_kill_connection = false;
bool is_primary_for_vip = false;
std::string view_id_stamp;
int view_id_version = 0;
std::map<std::string, std::string>
    bind_ips_with_nicname;                           // ip address, nicname
std::map<std::string, std::string> system_bind_ips;  // ip address, nicname
std::map<std::string, std::set<std::string>>
    all_node_bind_vips;  // uuid, ip address vector
std::queue<size_t> nic_pos_list;
std::map<std::string, int> server_sock_fds;  // server_uuid, sock_fd
std::vector<st_row_group_members> secondary_members;
std::map<std::string, int>
    secondary_plugin_ports;  // server_uuid, server_plugin_port
pthread_mutex_t vip_variable_mutex;
std::set<std::string> read_vips;
sa_family_t ping_family = AF_INET;
sa_family_t vip_family = AF_INET;
char *vip_nic;
char *vip_broadip;
char *plugin_port;
char *vip_netmask;
char *gateway_address_var;
bool enable_vip;
bool force_bind_vip;
bool need_exit = false;
bool check_killall_connection;
ulong send_arp_times;
std::atomic_bool need_break = true;
std::atomic_bool need_check_bind_vip;
std::atomic_bool need_check_killall_connection_and_force_member;
my_thread_handle listen_thread;         // run in every node
my_thread_handle primary_check_thread;  // run in primary node
my_thread_handle heartbeat_thread;
pthread_mutex_t mu_;
pthread_cond_t heartbeat_cv_;
my_thread_handle check_killconnection_thread_and_force_member;
pthread_mutex_t check_killconn_mu_;
pthread_cond_t check_killconn_cv_;

pthread_mutex_t msg_send_mu_;
pthread_cond_t msg_send_cv_;
static MYSQL_PLUGIN plugin_ptr;

struct arppacket {
  unsigned char dest_mac[ETH_ALEN];  // DEST MAC ADDRESS
  unsigned char src_mac[ETH_ALEN];   // SRC MAC ADDRESS
  unsigned short type;               // ARP type
  unsigned short ar_hrd;             // hard type0
  unsigned short ar_pro;             // IP
  unsigned char ar_hln;              // MAC ADDRESS LENGTH
  unsigned char ar_pln;              // IP ADDRESS LENGTH
  unsigned short ar_op;              // operation code
  unsigned char ar_sha[ETH_ALEN];    // SEND MAC ADDRESS
  unsigned char ar_sip[4];           // SEND IP
  unsigned char ar_tha[ETH_ALEN];    // RECEIVE MAC
  unsigned char ar_tip[4];           // RECEIVE IP
};

struct napacket {
  struct nd_neighbor_advert na;
  struct nd_opt_hdr hdr;
  uint8_t na_pl_mac[ETH_ALEN];
} __attribute__((packed));

struct in6_ifreq {
  struct in6_addr ifr6_addr;
  uint32_t ifr6_prefixlen;
  unsigned int ifr6_ifindex;
  short int ifr6_flags;
  char ifrn6_name[IFNAMSIZ];
};

struct sockaddr_ll {
  unsigned short int sll_family;
  unsigned short int sll_protocol;
  int sll_ifindex;
  unsigned short int sll_hatype;
  unsigned char sll_pkttype;
  unsigned char sll_halen;
  unsigned char sll_addr[8];
};

class Kill_All_Conn : public Do_THD_Impl {
  void operator()(THD *thd_to_kill) override {
    mysql_mutex_lock(&thd_to_kill->LOCK_thd_data);
    Security_context *sctx = thd_to_kill->security_context();
    const bool is_utility_user =
        acl_is_utility_user(sctx->user().str, sctx->host().str, sctx->ip().str);
    if (thd_to_kill->get_net()->vio && get_client_host(*thd_to_kill) &&
        thd_to_kill->killed != THD::KILL_CONNECTION &&
        !thd_to_kill->slave_thread && !is_utility_user)
      thd_to_kill->awake(THD::KILL_CONNECTION);
    mysql_mutex_unlock(&thd_to_kill->LOCK_thd_data);
  }
};

class Kill_Ip_Conn : public Do_THD_Impl {
 public:
  Kill_Ip_Conn(const char *ip) { ip_address_need_to_kill = ip; }
  void operator()(THD *thd_to_kill) override {
    mysql_mutex_lock(&thd_to_kill->LOCK_thd_data);
    Security_context *sctx = thd_to_kill->security_context();
    const bool is_utility_user =
        acl_is_utility_user(sctx->user().str, sctx->host().str, sctx->ip().str);
    if (thd_to_kill->get_net()->vio && get_client_host(*thd_to_kill) &&
        thd_to_kill->killed != THD::KILL_CONNECTION &&
        !thd_to_kill->slave_thread && !is_utility_user) {
      if (vip_family == AF_INET) {
        struct sockaddr_in addr;
        socklen_t addr_len = sizeof(addr);
        getsockname(thd_to_kill->get_net()->fd, (struct sockaddr *)&addr,
                    &addr_len);
        char ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &(addr.sin_addr), ip, INET_ADDRSTRLEN);
        if (strcmp(ip, ip_address_need_to_kill) == 0)
          thd_to_kill->awake(THD::KILL_CONNECTION);
      } else {
        struct sockaddr_in6 addr6;
        socklen_t addr6_len = sizeof(addr6);
        getsockname(thd_to_kill->get_net()->fd, (struct sockaddr *)&addr6,
                    &addr6_len);
        char ip[INET6_ADDRSTRLEN];
        inet_ntop(AF_INET6, &(addr6.sin6_addr), ip, INET6_ADDRSTRLEN);
        if (strcmp(ip, ip_address_need_to_kill) == 0)
          thd_to_kill->awake(THD::KILL_CONNECTION);
      }
    }
    mysql_mutex_unlock(&thd_to_kill->LOCK_thd_data);
  }

 private:
  const char *ip_address_need_to_kill;
};

static void kill_connection_bind_to_vip(const char *need_unbind_vip) {
  Global_THD_manager *thd_manager = Global_THD_manager::get_instance();
  my_plugin_log_message(&plugin_ptr, MY_WARNING_LEVEL,
                        "kill connections binding to vip: %s", need_unbind_vip);
  Kill_Ip_Conn unbind_vip_kill_conn(need_unbind_vip);
  thd_manager->do_for_all_thd(&unbind_vip_kill_conn);
}

/*----------------------------------------------------------------
ping gateway thread begin
---------------------------------------------------------------*/
static unsigned short get_checksum(unsigned short *buf, int len) {
  unsigned int sum = 0;
  for (sum = 0; len > 1; len -= 2) sum += *buf++;
  if (len == 1) sum += *(unsigned char *)buf;
  sum = (sum >> 16) + (sum & 0xffff);
  sum += (sum >> 16);
  return ~sum;
}

static int send_packet(int ping_sock_fd) {
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

static void gdb_cmd_run_force_member() {
  Gdb_cmd_service cmd_service;
  std::string set_force_member_local_address =
      "set global "
      "group_replication_force_members=@@group_replication_local_address";
  (void)cmd_service.execute_sql(set_force_member_local_address);
}

static ssize_t recv_packet(int ping_sock_fd) {
  ssize_t len, n;
  fd_set fds;
  struct timeval tv;
  struct iphdr *iph;
  struct icmp *icmp;
  tv.tv_sec = MAX_WAIT_TIME;
  tv.tv_usec = 0;
  int maxfds = 0;
  while (1) {
    FD_ZERO(&fds);
    FD_SET(ping_sock_fd, &fds);
    maxfds = ping_sock_fd + 1;
    n = select(maxfds, &fds, NULL, NULL, &tv);
    if (n <= 0) return -1;
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

sa_family_t check_ip_version(const char *ip) {
  if (!ip) return 0;
  struct sockaddr_storage sa;
  if (inet_pton(AF_INET, ip, &(((struct sockaddr_in *)&sa)->sin_addr)) == 1) {
    return AF_INET;
  } else if (inet_pton(AF_INET6, ip,
                       &(((struct sockaddr_in *)&sa)->sin_addr)) == 1) {
    return AF_INET6;
  } else
    return 0;
}

bool ping_gateway(int ping_sock_fd, const char *gateway_ip) {
  if (!gateway_ip || !strlen(gateway_ip)) return true;
  int n;
  int size = 50 * 1024;
  setsockopt(ping_sock_fd, SOL_SOCKET, SO_RCVBUF, &size, sizeof(size));
  bzero(&dest_addr, sizeof(dest_addr));
  bzero(&dest_addr6, sizeof(dest_addr6));
  if (ping_family == AF_INET &&
      inet_pton(AF_INET, gateway_ip, &dest_addr) == 1) {
    dest_addr.sin_family = AF_INET;
  } else if (ping_family == AF_INET6 &&
             inet_pton(AF_INET6, gateway_ip, &dest_addr6) == 1) {
    dest_addr6.sin6_family = AF_INET6;
  } else {
    my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                          "Error: gateway ip is wrong.");
    return false;
  }
  if (send_packet(ping_sock_fd) < 0) {
    my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                          "Error:send ping to gateway failed.");
    return false;
  }
  n = recv_packet(ping_sock_fd);
  if (n == -1) {
    my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                          "Error:receive ping from gateway failed.");
    return false;
  }
  my_plugin_log_message(&plugin_ptr, MY_INFORMATION_LEVEL,
                        "ping gateway success");
  return true;
}

void *ping_func(void *) {
  bool is_stopped_by_ping = false;
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
  if (need_exit) goto end;
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
    if (need_exit) {
      break;
    }
    pthread_mutex_lock(&ping_mutex);
    if (!gateway_address_var || !strlen(gateway_address_var))
      pthread_cond_wait(&ping_cv, &ping_mutex);
    if (!ping_gateway(ping_sock, gateway_address_var)) {
      if (need_stopped_by_ping && is_group_replication_running()) {
        char *error_message = nullptr;
        if (group_replication_stop(&error_message)) {
          my_plugin_log_message(&plugin_ptr, MY_INFORMATION_LEVEL,
                                "stop group replication failed, case %s",
                                error_message);
        }
        is_stopped_by_ping = true;
        need_stopped_by_ping = false;
      }
    } else {
      need_stopped_by_ping = false;
      if (is_stopped_by_ping && !is_group_replication_running()) {
        char *error_message = nullptr;
        if (group_replication_start(&error_message, thd))
          my_plugin_log_message(&plugin_ptr, MY_INFORMATION_LEVEL,
                                "start group replication failed, case %s",
                                error_message);
        is_stopped_by_ping = false;
      }
    }
    pthread_mutex_unlock(&ping_mutex);
    if (need_exit) {
      break;
    }
    sleep(5);
  }
  goto end;
end:
  delete thd;
  my_thread_end();
  return nullptr;
}
/*----------------------------------------------------------------
ping gateway thread end
---------------------------------------------------------------*/

int get_mac(const char *eno, unsigned char *mac) {
  struct ifreq ifreq;
  int sock;
  if ((sock = socket(vip_family, SOCK_STREAM, 0)) < 0) {
    return 0;
  }
  strcpy(ifreq.ifr_name, eno);
  if (ioctl(sock, SIOCGIFHWADDR, &ifreq) < 0) {
    close(sock);
    return 0;
  }
  memcpy(mac, ifreq.ifr_hwaddr.sa_data, 6);
  close(sock);
  return 1;
}

void get_all_ips() {
  if (!vip_nic) return;
  system_bind_ips.clear();
  struct ifaddrs *ifaddr, *ifa;
  int family, s;
  char host[NI_MAXHOST];

  if (getifaddrs(&ifaddr) == -1) return;

  // Walk through linked list, maintaining head pointer so we can free list
  // later
  for (ifa = ifaddr; ifa != NULL; ifa = ifa->ifa_next) {
    if (ifa->ifa_addr == NULL) continue;

    family = ifa->ifa_addr->sa_family;
    if (strncasecmp(ifa->ifa_name, vip_nic, strlen(vip_nic)) != 0) continue;

    // For an AF_INET* interface address, display the address
    if (family == AF_INET) {
      s = getnameinfo(ifa->ifa_addr, sizeof(struct sockaddr_in), host,
                      NI_MAXHOST, NULL, 0, NI_NUMERICHOST);
      if (s != 0) continue;
      system_bind_ips[host] = ifa->ifa_name;
    } else if (family == AF_INET6) {
      s = getnameinfo(ifa->ifa_addr, sizeof(struct sockaddr_in6), host,
                      NI_MAXHOST, NULL, 0, NI_NUMERICHOST);
      if (s != 0) continue;
      system_bind_ips[host] = ifa->ifa_name;
    }
  }
  freeifaddrs(ifaddr);
  return;
}

static std::string get_nic_name(size_t nic_pos) {
  std::string nic_name(vip_nic);
  if (nic_name.size() > IFNAMSIZ - 3)
    nic_name = nic_name.substr(0, IFNAMSIZ - 3);
  nic_name.append(":");
  nic_name.append(std::to_string(nic_pos));
  return nic_name;
}

/*
  Callbacks implementation for GROUP_REPLICATION_GROUP_MEMBER_STATS_CALLBACKS.
*/
static void set_channel_name_stats(void *const context, const char &value,
                                   size_t length) {
  struct st_row_group_member_stats *row =
      static_cast<struct st_row_group_member_stats *>(context);
  const size_t max = CHANNEL_NAME_LENGTH;
  length = std::min(length, max);

  row->channel_name_length = length;
  memcpy(row->channel_name, &value, length);
}

static void set_view_id_stats(void *const context, const char &value,
                              size_t length) {
  struct st_row_group_member_stats *row =
      static_cast<struct st_row_group_member_stats *>(context);
  const size_t max = HOSTNAME_LENGTH;
  length = std::min(length, max);

  row->view_id_length = length;
  memcpy(row->view_id, &value, length);
}

static void set_member_id_stats(void *const context, const char &value,
                                size_t length) {
  struct st_row_group_member_stats *row =
      static_cast<struct st_row_group_member_stats *>(context);
  const size_t max = UUID_LENGTH;
  length = std::min(length, max);

  row->member_id_length = length;
  memcpy(row->member_id, &value, length);
}

static void set_transactions_committed(void *const context, const char &value,
                                       size_t length) {
  struct st_row_group_member_stats *row =
      static_cast<struct st_row_group_member_stats *>(context);

  if (row->trx_committed != nullptr) {
    my_free(row->trx_committed);
  }

  row->trx_committed_length = length;
  row->trx_committed = (char *)my_malloc(PSI_NOT_INSTRUMENTED, length, MYF(0));
  memcpy(row->trx_committed, &value, length);
}

static void set_last_conflict_free_transaction(void *const context,
                                               const char &value,
                                               size_t length) {
  struct st_row_group_member_stats *row =
      static_cast<struct st_row_group_member_stats *>(context);
  const size_t max = Gtid::MAX_TEXT_LENGTH + 1;
  length = std::min(length, max);

  row->last_cert_trx_length = length;
  memcpy(row->last_cert_trx, &value, length);
}

static void set_transactions_in_queue(void *const context,
                                      unsigned long long int value) {
  struct st_row_group_member_stats *row =
      static_cast<struct st_row_group_member_stats *>(context);
  row->trx_in_queue = value;
}

static void set_transactions_certified(void *const context,
                                       unsigned long long int value) {
  struct st_row_group_member_stats *row =
      static_cast<struct st_row_group_member_stats *>(context);
  row->trx_checked = value;
}

static void set_transactions_conflicts_detected(void *const context,
                                                unsigned long long int value) {
  struct st_row_group_member_stats *row =
      static_cast<struct st_row_group_member_stats *>(context);
  row->trx_conflicts = value;
}

static void set_transactions_rows_in_validation(void *const context,
                                                unsigned long long int value) {
  struct st_row_group_member_stats *row =
      static_cast<struct st_row_group_member_stats *>(context);
  row->trx_rows_validating = value;
}

static void set_transactions_remote_applier_queue(
    void *const context, unsigned long long int value) {
  struct st_row_group_member_stats *row =
      static_cast<struct st_row_group_member_stats *>(context);
  row->trx_remote_applier_queue = value;
}

static void set_transactions_remote_applied(void *const context,
                                            unsigned long long int value) {
  struct st_row_group_member_stats *row =
      static_cast<struct st_row_group_member_stats *>(context);
  row->trx_remote_applied = value;
}

static void set_transactions_local_proposed(void *const context,
                                            unsigned long long int value) {
  struct st_row_group_member_stats *row =
      static_cast<struct st_row_group_member_stats *>(context);
  row->trx_local_proposed = value;
}

static void set_transactions_local_rollback(void *const context,
                                            unsigned long long int value) {
  struct st_row_group_member_stats *row =
      static_cast<struct st_row_group_member_stats *>(context);
  row->trx_local_rollback = value;
}

static void set_member_id(void *const context, const char &value,
                          size_t length) {
  struct st_row_group_members *row =
      static_cast<struct st_row_group_members *>(context);
  const size_t max = UUID_LENGTH;
  length = std::min(length, max);

  row->member_id = std::string(&value, length);
}

static void set_member_state(void *const context, const char &value,
                             size_t length) {
  struct st_row_group_members *row =
      static_cast<struct st_row_group_members *>(context);
  const size_t max = NAME_LEN;
  length = std::min(length, max);

  std::string str(&value, length);
  if (str == "ONLINE") {
    row->member_state = st_row_group_members::State::MGR_ONLINE;
  } else if (str == "OFFLINE") {
    row->member_state = st_row_group_members::State::MGR_OFFLINE;
  } else if (str == "RECOVERING") {
    row->member_state = st_row_group_members::State::MGR_RECOVERING;
  } else if (str == "UNREACHABLE") {
    row->member_state = st_row_group_members::State::MGR_UNREACHABLE;
  } else {
    row->member_state = st_row_group_members::State::MGR_ERROR;
  }
}

static void set_channel_name(void *const /*context*/, const char & /*value*/,
                             size_t /*length*/) {}

static void set_member_host(void *const context, const char &value,
                            size_t length) {
  struct st_row_group_members *row =
      static_cast<struct st_row_group_members *>(context);
  const size_t max = NAME_LEN;
  length = std::min(length, max);
  row->member_host = std::string(&value, length);
}

static void set_member_role(void *const context, const char &value,
                            size_t length) {
  struct st_row_group_members *row =
      static_cast<struct st_row_group_members *>(context);
  const size_t max = NAME_LEN;
  length = std::min(length, max);

  std::string str(&value, length);
  if (str == "PRIMARY") {
    row->member_role = st_row_group_members::Role::ROLE_PRIMARY;
  } else {
    row->member_role = st_row_group_members::Role::ROLE_SECONDARY;
  }
}

static void set_member_port(void *const context, unsigned int value) {
  struct st_row_group_members *row =
      static_cast<struct st_row_group_members *>(context);
  row->member_port = value;
}

static void set_member_version(void *const /*context*/,
                               const char &
                               /*value*/,
                               size_t /*length*/) {}

static void set_member_incoming_communication_protocol(void *const /*context*/,
                                                       const char &
                                                       /*value*/,
                                                       size_t /*length*/) {}

static int send_arp(const char *vip) {
  int sock_fd;
  struct in_addr s, r;
  sockaddr_ll sl;
  unsigned char mac[6];
  if (!get_mac(vip_nic, mac)) return 0;
  struct arppacket arp = {DEST_MAC,    DEST_MAC,      htons(0x0806),
                          htons(0x01), htons(0x0800), ETH_ALEN,
                          4,           htons(0x01),   DEST_MAC,
                          {0},         DEST_MAC,      {0}};
  memcpy(arp.src_mac, mac, 6);
  memcpy(arp.ar_sha, mac, 6);
  sock_fd = socket(AF_PACKET, SOCK_RAW, htons(ETH_P_ALL));
  if (sock_fd < 0) {
    return 0;
  }
  memset(&sl, 0, sizeof(sl));

  inet_aton(vip, &s);
  memcpy(&arp.ar_sip, &s, sizeof(s));
  inet_aton(vip, &r);
  memcpy(&arp.ar_tip, &r, sizeof(r));

  sl.sll_family = AF_PACKET;
  sl.sll_ifindex = IFF_BROADCAST;

  for (size_t i = 0; i < send_arp_times; i++) {
    if (sendto(sock_fd, &arp, sizeof(arp), 0, (struct sockaddr *)&sl,
               sizeof(sl)) <= 0) {
      close(sock_fd);
      return 0;
    }
    my_sleep(100000);
  }
  close(sock_fd);
  return 1;
}

static int send_na(const char *vip) {
  if (DBUG_EVALUATE_IF("test_vip", true, false)) {
    return 1;
  }
  int sockfd = socket(AF_INET6, SOCK_RAW, IPPROTO_ICMPV6);
  if (sockfd < 0) {
    my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                          "Get socket IPPROTO_ICMPV6 failed. %s",
                          strerror(errno));
    return 0;
  }

  int ifindex = if_nametoindex(vip_nic);
  if (setsockopt(sockfd, IPPROTO_IPV6, IPV6_MULTICAST_IF, &ifindex,
                 sizeof(ifindex)) < 0) {
    my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                          "setsockopt IPV6_MULTICAST_IF failed %s",
                          strerror(errno));
    close(sockfd);
    return 0;
  }

  /* must be 255. see rfc4861 7.1.2 */
  int hop_limit = 255;
  if (setsockopt(sockfd, IPPROTO_IPV6, IPV6_MULTICAST_HOPS, &hop_limit,
                 sizeof(hop_limit)) < 0) {
    my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                          "setsockopt IPV6_MULTICAST_HOPS failed %s",
                          strerror(errno));
    close(sockfd);
    return 0;
  }

  struct napacket na_packet;
  memset(&na_packet, 0, sizeof(na_packet));
  na_packet.na.nd_na_type = ND_NEIGHBOR_ADVERT;
  na_packet.na.nd_na_code = 0;
  na_packet.na.nd_na_cksum = 0;
  na_packet.na.nd_na_flags_reserved = ND_NA_FLAG_OVERRIDE;
  inet_pton(AF_INET6, vip, &na_packet.na.nd_na_target);

  na_packet.hdr.nd_opt_type = ND_OPT_TARGET_LINKADDR;
  na_packet.hdr.nd_opt_len = 1;
  if (!get_mac(vip_nic, na_packet.na_pl_mac)) {
    my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                          "get mac failed when send u-na packet %s",
                          strerror(errno));
    close(sockfd);
    return 0;
  }

  size_t packet_size =
      sizeof(struct nd_neighbor_advert) + sizeof(struct nd_opt_hdr) + ETH_ALEN;

  // Define source address and bind it to the socket
  struct sockaddr_in6 na_src_addr;
  memset(&na_src_addr, 0, sizeof(na_src_addr));
  na_src_addr.sin6_family = AF_INET6;
  inet_pton(AF_INET6, vip, &na_src_addr.sin6_addr);
  if (IN6_IS_ADDR_LINKLOCAL(&na_src_addr.sin6_addr) ||
      IN6_IS_ADDR_MC_LINKLOCAL(&na_src_addr.sin6_addr)) {
    na_src_addr.sin6_scope_id = ifindex;
  }

  ulong retry_time = 0;
  while (retry_time < HA_BIND_RETRY_TIMES) {
    if (bind(sockfd, (struct sockaddr *)&na_src_addr, sizeof(na_src_addr)) ==
        0) {
      break;
    }
    retry_time++;
    my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                          "bind %s na_src_addr failed %s", vip,
                          strerror(errno));
    sleep(1);
  }
  if (retry_time >= HA_BIND_RETRY_TIMES) {
    close(sockfd);
    return 0;
  }

  struct sockaddr_in6 na_dest_addr;
  memset(&na_dest_addr, 0, sizeof(na_dest_addr));
  na_dest_addr.sin6_family = AF_INET6;
  inet_pton(AF_INET6, DEST_IP6_ADDR, &na_dest_addr.sin6_addr);
  if (sendto(sockfd, &na_packet, packet_size, 0,
             (struct sockaddr *)&na_dest_addr, sizeof(na_dest_addr)) <= 0) {
    my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                          "sendto na_dest_addr fail %s", strerror(errno));
    close(sockfd);
    return 0;
  }

  close(sockfd);
  return 1;
}

static void release_nic_pos(const char *nic_name) {
  if (!nic_name) return;
  std::string nic_name_str(nic_name);
  int nic_pos = atoi(nic_name_str.substr(nic_name_str.length() - 1).c_str());
  nic_pos_list.push(nic_pos);
}

static bool unbind_vip(const char *vip, const char *nic_name) {
  if (!vip || !vip_netmask || !vip_nic) return false;
  if (DBUG_EVALUATE_IF("test_vip", true, false)) {
    bind_ips_with_nicname.erase(vip);
    release_nic_pos(nic_name);
    return true;
  }
  int fd = 0;
  if ((fd = socket(vip_family, SOCK_DGRAM, 0)) < 0) {
    return false;
  }
  if (vip_family == AF_INET) {
    struct sockaddr_in inet_addr;
    inet_addr.sin_family = AF_INET;
    if (inet_pton(AF_INET, vip, &(inet_addr.sin_addr)) != 1) return false;
    struct ifreq ifr;
    memcpy(ifr.ifr_name, nic_name, strlen(nic_name) + 1);
    memcpy(&ifr.ifr_addr, &inet_addr, sizeof(struct sockaddr));
    if (ioctl(fd, SIOCSIFADDR, &ifr) < 0) {
      my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                            "unbind AF_INET SIOCSIFADDR failed  %s",
                            strerror(errno));
      close(fd);
      return false;
    }
    if (ioctl(fd, SIOCGIFFLAGS, &ifr) < 0) {
      my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                            "unbind AF_INET SIOCSIFFLAGS %s", strerror(errno));
      close(fd);
      return false;
    }
    ifr.ifr_flags &= ~IFF_UP;
    if (ioctl(fd, SIOCSIFFLAGS, &ifr) < 0) {
      my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                            "unbind AF_INET SIOCSIFFLAGS %s", strerror(errno));
      close(fd);
      return false;
    }
  } else {
    struct in6_ifreq ifr6;
    struct ifreq ifr;
    struct sockaddr_in6 sa6;

    memset(&sa6, 0, sizeof(struct sockaddr_in6));
    sa6.sin6_family = AF_INET6;

    if (inet_pton(AF_INET6, vip, &sa6.sin6_addr) != 1) {
      my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                            "unbind AF_INET6 inet_pton %s", strerror(errno));
      close(fd);
      return false;
    }

    // get interface index and check
    strncpy(ifr.ifr_name, nic_name, strlen(nic_name) + 1);
    if (ioctl(fd, SIOGIFINDEX, &ifr) != 0) {
      my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                            "unbind AF_INET6 SIOGIFINDEX %s", strerror(errno));
      close(fd);
      return false;
    }

    memset(&ifr6, 0, sizeof(ifr6));
    memcpy(&ifr6.ifr6_addr, &sa6.sin6_addr, sizeof(struct in6_addr));

    ifr6.ifr6_prefixlen = atoi(vip_netmask);
    ifr6.ifr6_ifindex = ifr.ifr_ifindex;

    if (ioctl(fd, SIOCDIFADDR, &ifr6) < 0) {
      my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                            "unbind AF_INET6 SIOCDIFADDR %s", strerror(errno));
      close(fd);
      return false;
    }
  }
  bind_ips_with_nicname.erase(vip);
  release_nic_pos(nic_name);
  kill_connection_bind_to_vip(vip);
  return true;
}

void unbind_vips(std::map<std::string, std::string> vips) {
  for (auto it_to_unbind = vips.begin(); it_to_unbind != vips.end();
       it_to_unbind++) {
    if (!unbind_vip(it_to_unbind->first.c_str(),
                    it_to_unbind->second.c_str())) {
      my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                            "Error: try to unbind vip: %s failed on nic: %s",
                            it_to_unbind->first.c_str(),
                            it_to_unbind->second.c_str());
    } else {
      if (DBUG_EVALUATE_IF("test_vip", true, false))
        my_plugin_log_message(&plugin_ptr, MY_INFORMATION_LEVEL,
                              "try to unbind vip: %s success on nic: %s",
                              it_to_unbind->first.c_str(),
                              it_to_unbind->second.c_str());
      else
        my_plugin_log_message(&plugin_ptr, MY_WARNING_LEVEL,
                              "try to unbind vip: %s success on nic: %s",
                              it_to_unbind->first.c_str(),
                              it_to_unbind->second.c_str());
    }
  }
}

void unbind_all_vips() {
  get_all_ips();
  if (mgr_write_vip_addr &&
      system_bind_ips.find(mgr_write_vip_addr) != system_bind_ips.end()) {
    if (unbind_vip(mgr_write_vip_addr,
                   system_bind_ips[mgr_write_vip_addr].c_str())) {
      my_plugin_log_message(&plugin_ptr, MY_WARNING_LEVEL,
                            "try to unbind vip : %s success",
                            mgr_write_vip_addr);
    } else {
      my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                            "try to unbind vip : %s failed",
                            mgr_write_vip_addr);
    }
  }
  for (auto it = read_vips.begin(); it != read_vips.end(); it++) {
    if (system_bind_ips.find(*it) != system_bind_ips.end()) {
      if (unbind_vip(it->c_str(), system_bind_ips[*it].c_str())) {
        my_plugin_log_message(&plugin_ptr, MY_WARNING_LEVEL,
                              "try to unbind vip : %s success", it->c_str());
      } else {
        my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                              "try to unbind vip : %s failed", it->c_str());
      }
    }
  }
}

static bool bind_vip_ipv6(const char *vip) {
  struct in6_ifreq ifr6;
  int sockfd;
  struct ifreq ifr;
  struct sockaddr_in6 sa6;

  sockfd = socket(AF_INET6, SOCK_DGRAM, IPPROTO_IP);
  if (sockfd < 0) {
    my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                          "bind AF_INET6 socket %s", strerror(errno));
    return false;
  }

  memset(&sa6, 0, sizeof(struct sockaddr_in6));
  sa6.sin6_family = AF_INET6;

  if (inet_pton(AF_INET6, vip, &sa6.sin6_addr) != 1) {
    my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                          "bind AF_INET6 inet_pton %s", strerror(errno));
    close(sockfd);
    return false;
  }

  strncpy(ifr.ifr_name, vip_nic, strlen(vip_nic) + 1);
  if (ioctl(sockfd, SIOGIFINDEX, &ifr) != 0) {
    my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                          "bind AF_INET6 SIOGIFINDEX %s", strerror(errno));
    close(sockfd);
    return false;
  }

  memset(&ifr6, 0, sizeof(ifr6));
  memcpy(&ifr6.ifr6_addr, &sa6.sin6_addr, sizeof(struct in6_addr));

  ifr6.ifr6_prefixlen = atoi(vip_netmask);
  if (ifr6.ifr6_prefixlen > 128) {
    my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                          "vip_netmask format is error");
    close(sockfd);
    return false;
  }
  ifr6.ifr6_ifindex = ifr.ifr_ifindex;

  if (ioctl(sockfd, SIOCSIFADDR, &ifr6) < 0 && errno != 17) {
    my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                          "bind AF_INET6 SIOCSIFADDR %s", strerror(errno));
    close(sockfd);
    return false;
  }
  return true;
}

static bool bind_vip_ipv4(const char *vip) {
  struct sockaddr_in inet_addr;
  struct sockaddr_in mask_addr;
  int fd = 0;
  if ((fd = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
    return false;
  }
  inet_addr.sin_family = AF_INET;
  if (inet_pton(AF_INET, vip, &(inet_addr.sin_addr)) != 1) return false;
  mask_addr.sin_family = AF_INET;
  if (inet_pton(AF_INET, vip_netmask, &(mask_addr.sin_addr)) != 1) return false;
  struct ifreq ifr;
  std::string nic_name = get_nic_name(nic_pos_list.front());
  memcpy(ifr.ifr_ifrn.ifrn_name, nic_name.c_str(), nic_name.size() + 1);
  memcpy(&ifr.ifr_addr, &inet_addr, sizeof(struct sockaddr));
  if (ioctl(fd, SIOCSIFADDR, &ifr) < 0) {
    my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                          "bind AF_INET SIOCSIFADDR %s", strerror(errno));
    close(fd);
    return false;
  }
  memcpy(&ifr.ifr_addr, &mask_addr, sizeof(struct sockaddr));
  if (ioctl(fd, SIOCSIFNETMASK, &ifr) < 0) {
    my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                          "bind AF_INET SIOCSIFNETMASK %s", strerror(errno));
    close(fd);
    return false;
  }
  return true;
}

static bool bind_vip(const char *vip) {
  if (!vip || !vip_netmask || !vip_nic) return false;
  if (bind_ips_with_nicname.find(vip) != bind_ips_with_nicname.end())
    return true;
  if (nic_pos_list.empty()) {
    my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                          "Virtual Adapter list is empty when bind vip");
    return false;
  }

  std::string nic_name = get_nic_name(nic_pos_list.front());
  if (DBUG_EVALUATE_IF("test_vip", true, false)) {
    bind_ips_with_nicname[vip] = vip_family == AF_INET ? nic_name : vip_nic;
    nic_pos_list.pop();
    return true;
  }

  if (vip_family == AF_INET ? !bind_vip_ipv4(vip) : !bind_vip_ipv6(vip)) {
    return false;
  }

  bind_ips_with_nicname[vip] = vip_family == AF_INET ? nic_name : vip_nic;
  if (vip_family == AF_INET ? !send_arp(vip) : !send_na(vip)) {
    return false;
  }
  nic_pos_list.pop();
  return true;
}

static void killall_connections() {
  Global_THD_manager *thd_manager = Global_THD_manager::get_instance();
  my_plugin_log_message(&plugin_ptr, MY_WARNING_LEVEL,
                        "kill all connections after primary changed");
  Kill_All_Conn set_kill_conn;
  thd_manager->do_for_all_thd(&set_kill_conn);
}

static void *check_kill_connection_and_force_member() {
  if (!is_group_replication_running()) {
    return nullptr;
  }
  if (!check_killall_connection && !gateway_address_var) return nullptr;
  bool need_kill = false;
  bool need_force = false;
  struct st_row_group_members m_row;
  const GROUP_REPLICATION_GROUP_MEMBERS_CALLBACKS callbacks = {
      &m_row,
      &set_channel_name,
      &set_member_id,
      &set_member_host,
      &set_member_port,
      &set_member_state,
      &set_member_role,
      &set_member_version,
      &set_member_incoming_communication_protocol /* set_member_incoming_communication_protocol
                                                   */
      ,
  };
  unsigned int n = get_group_replication_members_number_info();
  for (size_t i = 0; i < n; i++) {
    if (get_group_replication_group_members_info(i, callbacks)) {
      break;
    }
    if (!strcasecmp(m_row.member_id.c_str(), server_uuid)) {
      if (m_row.member_role == st_row_group_members::Role::ROLE_PRIMARY) {
        is_primary_for_check_kill_connection = true;
      } else {
        if (is_primary_for_check_kill_connection) {
          need_kill = true;
        }
        is_primary_for_check_kill_connection = false;
      }
    } else if (n == 2 && m_row.member_state ==
                             st_row_group_members::State::MGR_UNREACHABLE) {
      need_force = true;
    }
  }
  if (need_kill && check_killall_connection) killall_connections();
  if (need_force && gateway_address_var && ping_sock > 0) {
    pthread_mutex_lock(&ping_mutex);
    if (ping_gateway(ping_sock, gateway_address_var))
      gdb_cmd_run_force_member();
    else {
      need_stopped_by_ping = true;
    }
    pthread_mutex_unlock(&ping_mutex);
  }
  return nullptr;
}

void check_bind_vips() {
  if (DBUG_EVALUATE_IF("test_vip", true, false)) {
    return;
  }
  get_all_ips();
  std::vector<std::string> need_rebind_vips;
  for (auto it = bind_ips_with_nicname.begin();
       it != bind_ips_with_nicname.end(); it++) {
    if (system_bind_ips.find(it->first) == system_bind_ips.end())
      need_rebind_vips.push_back(it->first);
  }
  for (size_t i = 0; i < need_rebind_vips.size(); i++) {
    std::string nic_name = bind_ips_with_nicname[need_rebind_vips[i]];
    bind_ips_with_nicname.erase(need_rebind_vips[i]);
    release_nic_pos(nic_name.c_str());
    if (bind_vip(need_rebind_vips[i].c_str())) {
      if (DBUG_EVALUATE_IF("test_vip", true, false))
        my_plugin_log_message(&plugin_ptr, MY_INFORMATION_LEVEL,
                              "try to bind vip : %s success",
                              need_rebind_vips[i].c_str());
      else
        my_plugin_log_message(&plugin_ptr, MY_WARNING_LEVEL,
                              "try to bind vip : %s success",
                              need_rebind_vips[i].c_str());
    } else {
      my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                            "try to bind vip : %s failed",
                            need_rebind_vips[i].c_str());
    }
  }
}

static void bind_vip_according_map() {
  if (all_node_bind_vips.find(server_uuid) != all_node_bind_vips.end()) {
    std::set<std::string> need_bind_vips = all_node_bind_vips[server_uuid];
    for (auto it = need_bind_vips.begin(); it != need_bind_vips.end(); it++) {
      if (bind_ips_with_nicname.find(*it) == bind_ips_with_nicname.end()) {
        if ((*it) == "") continue;
        if (bind_vip((*it).c_str())) {
          if (DBUG_EVALUATE_IF("test_vip", true, false))
            my_plugin_log_message(&plugin_ptr, MY_INFORMATION_LEVEL,
                                  "try to bind vip : %s success",
                                  (*it).c_str());
          else
            my_plugin_log_message(&plugin_ptr, MY_WARNING_LEVEL,
                                  "try to bind vip : %s success",
                                  (*it).c_str());
        } else {
          my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                                "try to bind vip : %s failed", (*it).c_str());
        }
      } else {
        // when signal_hand() receive kill signal
        bool mysqld_begin_exit = connection_events_loop_aborted();
        if (!mysqld_begin_exit) {
          vip_family == AF_INET ? send_arp((*it).c_str())
                                : send_na((*it).c_str());
        } else {
          my_plugin_log_message(&plugin_ptr, MY_INFORMATION_LEVEL,
                                "not send arp to keep vip due to mysqld exit.");
        }
      }
    }
    std::map<std::string, std::string> need_unbind_vips;
    for (auto it = bind_ips_with_nicname.begin();
         it != bind_ips_with_nicname.end(); it++) {
      if (need_bind_vips.find(it->first) == need_bind_vips.end())
        need_unbind_vips[it->first] = it->second;
    }
    for (auto it = need_unbind_vips.begin(); it != need_unbind_vips.end();
         it++) {
      if (unbind_vip(it->first.c_str(), it->second.c_str())) {
        if (DBUG_EVALUATE_IF("test_vip", true, false))
          my_plugin_log_message(&plugin_ptr, MY_INFORMATION_LEVEL,
                                "try to unbind vip : %s success",
                                it->first.c_str());
        else
          my_plugin_log_message(&plugin_ptr, MY_WARNING_LEVEL,
                                "try to unbind vip : %s success",
                                it->first.c_str());
      } else {
        my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                              "try to unbind vip : %s failed",
                              it->first.c_str());
      }
    }
  }
  check_bind_vips();
}

static int send_message(int sock) {
  return send(sock, send_message_buf, 1024, 0);
}

static int receive_message(int sock) {
  int ret;
  memset(recv_message_buf, 0, 1024);
  ret = recv(sock, recv_message_buf, 1024, 0);
  return ret;
}

static void split_string_according_delimiter(const char *split,
                                             std::set<std::string> &result,
                                             std::string delimiter) {
  if (!split) return;
  result.clear();
  std::string split_string(split);
  split_string.erase(std::remove(split_string.begin(), split_string.end(), ' '),
                     split_string.end());
  std::string::size_type last_pos =
      split_string.find_first_not_of(delimiter, 0);

  // Find first "non-delimiter".
  std::string::size_type pos = split_string.find_first_of(delimiter, last_pos);
  while (std::string::npos != pos || std::string::npos != last_pos) {
    std::string peer(split_string.substr(last_pos, pos - last_pos));
    // Skip delimiter
    result.insert(peer);
    last_pos = split_string.find_first_not_of(delimiter, pos);

    // Find next "non-delimiter"
    pos = split_string.find_first_of(delimiter, last_pos);
  }
}

static int get_all_node_ips(
    const char *vip_tope_value,
    std::map<std::string, std::set<std::string>> &uuid_vip_map) {
  // the format is "uuid1::vip1;uuid2::vip2,vip3;uuid3::vip4"
  if (!vip_tope_value) return 0;
  std::set<std::string> result;
  split_string_according_delimiter(vip_tope_value, result, ";");
  for (auto it = result.begin(); it != result.end(); it++) {
    std::string::size_type pos = (*it).find_first_of("::");
    if (pos + 2 > (*it).size()) return -1;
    std::string node_uuid = (*it).substr(0, pos);
    std::string node_vips = (*it).substr(pos + 2);
    std::set<std::string> need_bind_ips;
    split_string_according_delimiter(node_vips.c_str(), need_bind_ips, ",");
    uuid_vip_map[node_uuid] = need_bind_ips;
  }
  return 0;
}

static void handle_received_message(int sock) {
  pthread_mutex_lock(&vip_variable_mutex);
  greatdb_ha_message *receive_message = (greatdb_ha_message *)recv_message_buf;
  bool is_old_view_id = false;
  std::string recv_view_id_stamp;
  int recv_view_id_version = 0;
  if (!receive_message->get_view_id(recv_view_id_stamp, recv_view_id_version) ||
      (recv_view_id_stamp == view_id_stamp &&
       recv_view_id_version < view_id_version)) {
    is_old_view_id = true;
  }
  memset(send_message_buf, 0, 1024);
  greatdb_ha_message *need_send_message =
      (greatdb_ha_message *)send_message_buf;
  switch (receive_message->get_message_type()) {
    case GET_BIND_VIPS: {
      if (is_old_view_id) {
        need_send_message->set_message_type(YOU_ARE_NOT_PRIMARY);
        send_message(sock);
        break;
      }
      std::string bind_vips = "";
      for (auto it = bind_ips_with_nicname.begin();
           it != bind_ips_with_nicname.end(); it++) {
        bind_vips.append(it->first.c_str());
        bind_vips.append(",");
      }
      if (bind_vips != "")
        bind_vips = bind_vips.substr(0, bind_vips.length() - 1);
      need_send_message->set_message_type(GET_BIND_VIPS_REPLY);
      need_send_message->set_message_content(bind_vips.c_str());
      send_message(sock);
      break;
    }
    case SET_ALL_NODE_BIND_VIPS: {
      if (is_old_view_id) {
        break;
      }
      char *receive_content = receive_message->get_message_content();
      memset(all_vip_tope_value, 0, 1024);
      memcpy(all_vip_tope_value, receive_content, strlen(receive_content));
      get_all_node_ips(receive_content, all_node_bind_vips);
      bind_vip_according_map();
      need_send_message->set_message_type(OK_REPLY);
      send_message(sock);
      break;
    }
    default:
      break;
  }
  pthread_mutex_unlock(&vip_variable_mutex);
}

static std::string gen_messages_according_nodes_relationship() {
  std::string all_node_bind_vips_message = "";
  auto it = all_node_bind_vips.begin();
  for (; it != all_node_bind_vips.end(); it++) {
    all_node_bind_vips_message.append(it->first);
    all_node_bind_vips_message.append("::");
    for (auto it1 = it->second.begin(); it1 != it->second.end(); it1++) {
      all_node_bind_vips_message.append(*it1);
      all_node_bind_vips_message.append(",");
    }
    if (it->second.size() != 0)
      all_node_bind_vips_message = all_node_bind_vips_message.substr(
          0, all_node_bind_vips_message.length() - 1);
    all_node_bind_vips_message.append(";");
  }
  if (all_node_bind_vips_message != "")
    return all_node_bind_vips_message.substr(
        0, all_node_bind_vips_message.length() - 1);
  return all_node_bind_vips_message;
}

static int get_secondary_plugin_port(
    st_row_group_members &secondary_member_info,
    bool need_force_update_plugin_port) {
  if (need_force_update_plugin_port)
    secondary_plugin_ports.erase(secondary_member_info.member_id);
  else if (secondary_plugin_ports.find(secondary_member_info.member_id) !=
           secondary_plugin_ports.end())
    return secondary_plugin_ports[secondary_member_info.member_id];
  MYSQL *mysql = nullptr;
  MYSQL_RES *result = nullptr;
  MYSQL_ROW row;
  const char *user;
  char password[MAX_PASSWORD_LENGTH + 1];
  int secondary_port = -1;
  mysql = mysql_init(mysql);
  ulong ha_connect_timeout = HA_CONNECT_TIMEOUT;
  mysql_options(mysql, MYSQL_OPT_CONNECT_TIMEOUT, &ha_connect_timeout);
  mysql_options(mysql, MYSQL_OPT_READ_TIMEOUT, &ha_connect_timeout);
  mysql_options(mysql, MYSQL_OPT_WRITE_TIMEOUT, &ha_connect_timeout);

  size_t password_size = sizeof(password);
  channel_map.rdlock();
  Master_info *recover_info = channel_map.get_mi("group_replication_recovery");
  if (!recover_info) goto error;
  user = recover_info->get_user();
  if (!user) goto error;
  if (recover_info->get_password(password, &password_size)) goto error;
  if (!mysql_real_connect(mysql, secondary_member_info.member_host.c_str(),
                          user, password, nullptr,
                          secondary_member_info.member_port, nullptr, 0))
    goto error;
  if (mysql_real_query(
          mysql, STRING_WITH_LEN("show variables like 'greatdb_ha_port'")))
    goto error;
  result = mysql_store_result(mysql);
  if (!result) goto error;
  row = mysql_fetch_row(result);
  if (!row || !row[1]) goto error;
  secondary_port = atoi(row[1]);
  secondary_plugin_ports[secondary_member_info.member_id] = secondary_port;
  channel_map.unlock();
  if (result) mysql_free_result(result);
  mysql_close(mysql);
  return secondary_port;
error:
  channel_map.unlock();
  if (result) mysql_free_result(result);
  mysql_close(mysql);
  return -1;
}

int connect_with_timeout(int sockfd, struct sockaddr *servaddr,
                         socklen_t servaddr_size) {
  unsigned long ul = 1;
  // set not block
  ul = 1;
  ioctl(sockfd, FIONBIO, &ul);
  if (connect(sockfd, servaddr, servaddr_size) >= 0) {
    // set block
    ul = 0;
    ioctl(sockfd, FIONBIO, &ul);
    return -1;
  }
  int ret = 0;
  fd_set set;
  struct timeval tm;
  tm.tv_sec = HA_CONNECT_TIMEOUT;
  tm.tv_usec = 0;
  FD_ZERO(&set);
  FD_SET(sockfd, &set);

  ret = select(sockfd + 1, NULL, &set, NULL, &tm);
  if (ret > 0) {
    // get socket status
    int error = -1, len = sizeof(int);
    getsockopt(sockfd, SOL_SOCKET, SO_ERROR, &error, (socklen_t *)&len);
    if (error == 0) {
      ret = 0;
    } else {
      ret = -3;
    }
  } else {
    ret = -2;
  }

  // set block
  ul = 0;
  ioctl(sockfd, FIONBIO, &ul);
  return ret;
}

static int get_socket_by_host(st_row_group_members &secondary_member_info,
                              bool need_force_update_sock_fd) {
  if (need_force_update_sock_fd)
    server_sock_fds.erase(secondary_member_info.member_id);
  else if (server_sock_fds.find(secondary_member_info.member_id) !=
           server_sock_fds.end())
    return server_sock_fds[secondary_member_info.member_id];
  bool retry_count = 0;
  // maybe secondary node's changed its greatdb_ha_port, retry and force
  // update the value save in secondary_port_map
  int sockfd;
  sa_family_t cur_family =
      check_ip_version(secondary_member_info.member_host.c_str());
  while (1) {
    if ((sockfd = socket(cur_family, SOCK_STREAM, 0)) == -1) {
      return -1;
    }
    if (cur_family == AF_INET) {
      struct sockaddr_in servaddr;
      memset(&servaddr, 0, sizeof(servaddr));
      servaddr.sin_family = AF_INET;
      // if retry count is 1 means used plugin_port is not available,
      // so force update plugin port if retry count is 1
      int secondary_plugin_port =
          get_secondary_plugin_port(secondary_member_info, retry_count);
      if (secondary_plugin_port == -1) {
        close(sockfd);
        return -1;
      }
      servaddr.sin_port = htons(secondary_plugin_port);
      servaddr.sin_addr.s_addr =
          inet_addr(secondary_member_info.member_host.c_str());
      int ret = connect_with_timeout(sockfd, (struct sockaddr *)&servaddr,
                                     sizeof(servaddr));
      if (ret != 0) {
        close(sockfd);
        my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                              "connect_with_timeout %s:%d failed return %d",
                              secondary_member_info.member_host.c_str(),
                              secondary_plugin_port, ret);
        if (retry_count == 1) return -1;
        retry_count += 1;
        continue;
      }
    } else {
      struct sockaddr_in6 servaddr6;
      memset(&servaddr6, 0, sizeof(servaddr6));
      servaddr6.sin6_family = AF_INET6;
      // if retry count is 1 means used plugin_port is not available,
      // so force update plugin port if retry count is 1
      int secondary_plugin_port =
          get_secondary_plugin_port(secondary_member_info, retry_count);
      if (secondary_plugin_port == -1) {
        close(sockfd);
        return -1;
      }
      servaddr6.sin6_port = htons(secondary_plugin_port);
      if (inet_pton(AF_INET6, secondary_member_info.member_host.c_str(),
                    &servaddr6.sin6_addr) != 1) {
        my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                              "get_socket_by_host AF_INET6 Invalid address/ "
                              "Address not supported %s",
                              strerror(errno));
        close(sockfd);
        return -1;
      }
      int ret = connect_with_timeout(sockfd, (struct sockaddr *)&servaddr6,
                                     sizeof(servaddr6));
      if (ret != 0) {
        close(sockfd);
        my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                              "connect_with_timeout %s:%d failed return %d",
                              secondary_member_info.member_host.c_str(),
                              secondary_plugin_port, ret);
        if (retry_count == 1) return -1;
        retry_count += 1;
        continue;
      }
    }
    break;
  }
  struct timeval timeout;
  timeout.tv_sec = HA_CONNECT_TIMEOUT;
  timeout.tv_usec = 0;
  setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
  setsockopt(sockfd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
  server_sock_fds[secondary_member_info.member_id] = sockfd;
  return sockfd;
}

static bool get_secondary_node_bind_ips() {
  memset(send_message_buf, 0, 1024);
  greatdb_ha_message *need_send_message =
      (greatdb_ha_message *)send_message_buf;
  need_send_message->set_message_type(GET_BIND_VIPS);
  need_send_message->set_view_id(view_id_stamp, view_id_version);
  std::set<std::string> has_bind_vips = all_node_bind_vips[server_uuid];
  for (auto it = secondary_members.begin(); it != secondary_members.end();
       ++it) {
    int retry_count = 0;
    while (retry_count < 2) {
      int sock = get_socket_by_host(*it, retry_count);
      if (sock <= 0) {
        retry_count++;
        continue;
      }
      if (send_message(sock) <= 0) {
        close(sock);
        retry_count++;
        continue;
      }
      if (receive_message(sock) <= 0) {
        close(sock);
        retry_count++;
        continue;
      }
      greatdb_ha_message *receive_message =
          (greatdb_ha_message *)recv_message_buf;
      if (receive_message->get_message_type() == YOU_ARE_NOT_PRIMARY) {
        my_plugin_log_message(
            &plugin_ptr, MY_INFORMATION_LEVEL,
            "Cur node receive YOU_ARE_NOT_PRIMARY, will skip this round of "
            "vip-bind-relationship calculation.");
        return false;
      } else {
        assert(receive_message->get_message_type() == GET_BIND_VIPS_REPLY);
      }
      std::set<std::string> bind_vips;
      std::vector<std::string> need_erase_vips;
      split_string_according_delimiter(receive_message->get_message_content(),
                                       bind_vips, ",");
      // delete vips that not in read vips or has already be bound by another
      // node
      for (auto it1 = bind_vips.begin(); it1 != bind_vips.end(); ++it1) {
        if (read_vips.find(*it1) == read_vips.end() ||
            has_bind_vips.find(*it1) != has_bind_vips.end())
          need_erase_vips.push_back(*it1);
        else
          has_bind_vips.insert(*it1);
      }
      for (size_t i = 0; i < need_erase_vips.size(); i++)
        bind_vips.erase(need_erase_vips[i]);
      all_node_bind_vips[it->member_id] = bind_vips;
      break;
    }
    if (retry_count == 2) {
      // means retry failed
      it->is_invalid = true;
      server_sock_fds.erase(it->member_id);
      my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                            "try to connect or send message to %s failed when "
                            "get node bind message",
                            it->member_id.c_str());
    }
  }
  return true;
}

static void send_secondary_bind_vips_message() {
  std::string message = gen_messages_according_nodes_relationship();
  memset(all_vip_tope_value, 0, 1024);
  memcpy(all_vip_tope_value, message.c_str(), message.size());
  if (message == "") {
    return;
  }
  if (!secondary_members.size()) return;
  memset(send_message_buf, 0, 1024);
  greatdb_ha_message *need_send_message =
      (greatdb_ha_message *)send_message_buf;
  need_send_message->set_message_type(SET_ALL_NODE_BIND_VIPS);
  need_send_message->set_view_id(view_id_stamp, view_id_version);
  need_send_message->set_message_content(message.c_str());
  for (auto it = secondary_members.begin(); it != secondary_members.end();
       ++it) {
    if (it->is_invalid) continue;
    int retry_count = 0;
    while (retry_count < 2) {
      int sock = get_socket_by_host(*it, retry_count);
      if (sock <= 0) {
        retry_count++;
        continue;
      }
      if (send_message(sock) <= 0) {
        close(sock);
        retry_count++;
        continue;
      }
      if (receive_message(sock) <= 0) {
        close(sock);
        retry_count++;
        continue;
      }
      greatdb_ha_message *receive_message =
          (greatdb_ha_message *)recv_message_buf;
      if (receive_message->get_message_type() == ERROR_REPLY) {
        my_plugin_log_message(
            &plugin_ptr, MY_ERROR_LEVEL,
            "Error: member: %s apply vip message error failed, because: %s",
            it->member_id.c_str(), receive_message->get_message_content());
      }
      break;
    }
    if (retry_count == 2) {
      // means retry failed
      server_sock_fds.erase(it->member_id);
      my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                            "try to connect or send message to %s failed when "
                            "send node bind message",
                            it->member_id.c_str());
    }
  }
}

static std::string get_min_secondary_bind_vip_server_uuid() {
  size_t min = INT_MAX;
  std::string min_vip_server_uuid = "";
  for (auto it = all_node_bind_vips.begin(); it != all_node_bind_vips.end();
       it++) {
    if (min > (it->second.size()) &&
        strcasecmp(it->first.c_str(), server_uuid) != 0) {
      min_vip_server_uuid = it->first;
      min = it->second.size();
    }
  }
  return min_vip_server_uuid;
}

static std::string get_max_bind_vip_server_uuid() {
  size_t max = 1;
  std::string max_vip_server_uuid = "";
  for (auto it = all_node_bind_vips.begin(); it != all_node_bind_vips.end();
       it++) {
    if (max < (it->second.size())) {
      max_vip_server_uuid = it->first;
      max = it->second.size();
    }
  }
  return max_vip_server_uuid;
}

static void caculate_new_bind_relationship() {
  std::set<std::string> has_bind_vips;
  std::vector<std::string> new_members_uuid;
  std::vector<std::string> need_bind_vips;
  // get vips have already been bound
  for (auto it = all_node_bind_vips.begin(); it != all_node_bind_vips.end();
       it++) {
    for (auto it1 = it->second.begin(); it1 != it->second.end(); it1++) {
      has_bind_vips.insert(*it1);
    }
  }
  // get vips need to be allocated
  for (auto it = read_vips.begin(); it != read_vips.end(); it++) {
    if (has_bind_vips.find(*it) == has_bind_vips.end()) {
      need_bind_vips.push_back(*it);
    }
  }
  // get new members
  for (auto it = secondary_members.begin(); it != secondary_members.end();
       it++) {
    if (!it->is_invalid &&
        (all_node_bind_vips.find(it->member_id) == all_node_bind_vips.end() ||
         all_node_bind_vips[it->member_id].size() == 0))
      new_members_uuid.push_back(
          it->member_id);  // if a secondary member can not be connected by it's
                           // plugin_port, then not allocate vip for it
  }

  if (need_bind_vips.size() >= new_members_uuid.size()) {
    // allocate one vip for every new member
    for (size_t i = 0; i < new_members_uuid.size(); i++) {
      all_node_bind_vips[new_members_uuid[i]].insert(need_bind_vips[i]);
    }
    // remaining vips allocated according to read_vip_floating_type
    if (read_vip_floating_type == TO_PRIMARY) {
      // TO_PRIMARY means all remaining vips should allocate to primary node
      for (size_t i = new_members_uuid.size(); i < need_bind_vips.size(); i++) {
        all_node_bind_vips[server_uuid].insert(need_bind_vips[i]);
      }
    } else if (read_vip_floating_type == TO_ANOTHER_SECONDARY) {
      // TO_ANOTHER_SECONDARY means all remaining vips should allocate to
      // secondary node which has min vip nums
      for (size_t i = new_members_uuid.size(); i < need_bind_vips.size(); i++) {
        std::string min_secondary = get_min_secondary_bind_vip_server_uuid();
        if (!min_secondary.empty())
          all_node_bind_vips[min_secondary].insert(need_bind_vips[i]);
        else
          all_node_bind_vips[server_uuid].insert(need_bind_vips[i]);
      }
    }
  } else {
    // allocate one vip for new members
    for (size_t i = 0; i < need_bind_vips.size(); i++) {
      all_node_bind_vips[new_members_uuid[i]].insert(need_bind_vips[i]);
    }
    // means should get vip from another node's bind vip
    for (size_t i = need_bind_vips.size(); i < new_members_uuid.size(); i++) {
      std::string need_unbind_uuid = get_max_bind_vip_server_uuid();
      if (!need_unbind_uuid.empty()) {
        for (auto one_vip = all_node_bind_vips[need_unbind_uuid].begin();
             one_vip != all_node_bind_vips[need_unbind_uuid].end(); one_vip++) {
          // get one read vip from member that has max vip nums
          if (strcasecmp(one_vip->c_str(), mgr_write_vip_addr)) {
            all_node_bind_vips[new_members_uuid[i]].insert(*one_vip);
            all_node_bind_vips[need_unbind_uuid].erase(*one_vip);
            break;
          }
        }
      }
    }
  }
}

static bool get_cur_group_view_id(std::string &view_id_stamp,
                                  int &view_id_version) {
  struct st_row_group_member_stats tmp_row;
  // Set default values.
  tmp_row.channel_name_length = 0;
  tmp_row.trx_committed = nullptr;
  tmp_row.view_id_length = 0;
  tmp_row.member_id_length = 0;
  tmp_row.trx_committed_length = 0;
  tmp_row.last_cert_trx_length = 0;
  tmp_row.trx_in_queue = 0;
  tmp_row.trx_checked = 0;
  tmp_row.trx_conflicts = 0;
  tmp_row.trx_rows_validating = 0;
  tmp_row.trx_remote_applier_queue = 0;
  tmp_row.trx_remote_applied = 0;
  tmp_row.trx_local_proposed = 0;
  tmp_row.trx_local_rollback = 0;
  // Set callbacks on GROUP_REPLICATION_GROUP_MEMBER_STATS_CALLBACKS.
  const GROUP_REPLICATION_GROUP_MEMBER_STATS_CALLBACKS callbacks = {
      &tmp_row,
      &set_channel_name_stats,
      &set_view_id_stats,
      &set_member_id_stats,
      &set_transactions_committed,
      &set_last_conflict_free_transaction,
      &set_transactions_in_queue,
      &set_transactions_certified,
      &set_transactions_conflicts_detected,
      &set_transactions_rows_in_validation,
      &set_transactions_remote_applier_queue,
      &set_transactions_remote_applied,
      &set_transactions_local_proposed,
      &set_transactions_local_rollback,
  };
  // Query plugin and let callbacks do their job.
  if (get_group_replication_group_member_stats_info(0, callbacks) ||
      tmp_row.view_id_length == 0) {
    if (tmp_row.trx_committed != nullptr) {
      my_free(tmp_row.trx_committed);
      tmp_row.trx_committed = nullptr;
    }
    return false;
  }
  if (tmp_row.trx_committed != nullptr) {
    my_free(tmp_row.trx_committed);
    tmp_row.trx_committed = nullptr;
  }
  std::string str_view_id = tmp_row.view_id;
  std::size_t view_id_position = str_view_id.find(":");
  if (view_id_position > 0) {
    view_id_stamp = str_view_id.substr(0, view_id_position);
    view_id_version = std::stoi(str_view_id.substr(view_id_position + 1));
  }
  if (view_id_position <= 0 || view_id_version < 1 || view_id_stamp.empty()) {
    return false;
  }
  return true;
}

bool update_vip_family() {
  sa_family_t cur_family = 0;
  if (!mgr_write_vip_addr || strlen(mgr_write_vip_addr) < 2) {
    if (read_vips.size() > 0)
      cur_family = greatdb::check_ip_version(read_vips.begin()->c_str());
  } else {
    cur_family = greatdb::check_ip_version(mgr_write_vip_addr);
  }
  if (cur_family == 0) {
    my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                          "Error:greatdb_ha_mgr_vip_ip is in the wrong format, "
                          "please correct it");
    return false;
  }
  vip_family = cur_family;
  return true;
}

static void *mysql_heartbeat() {
  if (!is_group_replication_running()) {
    unbind_vips(bind_ips_with_nicname);
    return nullptr;
  }
  pthread_mutex_lock(&msg_send_mu_);
  pthread_mutex_lock(&vip_variable_mutex);
  if (!enable_vip) {
    pthread_mutex_unlock(&vip_variable_mutex);
    pthread_mutex_unlock(&msg_send_mu_);
    return nullptr;
  }
  if (!mgr_write_vip_addr) {
    my_plugin_log_message(
        &plugin_ptr, MY_ERROR_LEVEL,
        "Error:greatdb_ha_mgr_vip_ip is not defined, please define it");
    pthread_mutex_unlock(&vip_variable_mutex);
    pthread_mutex_unlock(&msg_send_mu_);
    return nullptr;
  }
  if (!update_vip_family()) {
    pthread_mutex_unlock(&vip_variable_mutex);
    pthread_mutex_unlock(&msg_send_mu_);
    return nullptr;
  }
  need_break = true;
  for (auto it = server_sock_fds.begin(); it != server_sock_fds.end(); it++) {
    close(it->second);
  }
  server_sock_fds.clear();
  bool is_master = false;
  bool need_unbind_all_vips = false;
  bool master_is_running = true;
  struct st_row_group_members m_row;

  const GROUP_REPLICATION_GROUP_MEMBERS_CALLBACKS callbacks = {
      &m_row,
      &set_channel_name,
      &set_member_id,
      &set_member_host,
      &set_member_port,
      &set_member_state,
      &set_member_role,
      &set_member_version,
      &set_member_incoming_communication_protocol /* set_member_incoming_communication_protocol
                                                   */
      ,
  };
  unsigned int work_number = 0;
  unsigned int recover_number = 0;
  secondary_members.clear();
  unsigned int n = get_group_replication_members_number_info();
  for (size_t i = 0; i < n; i++) {
    if (get_group_replication_group_members_info(i, callbacks)) {
      break;
    }
    if (m_row.member_state == st_row_group_members::State::MGR_ONLINE)
      work_number += 1;
    else if (m_row.member_state == st_row_group_members::State::MGR_RECOVERING)
      recover_number += 1;
    else if ((m_row.member_state == st_row_group_members::State::MGR_OFFLINE ||
              m_row.member_state == st_row_group_members::State::MGR_ERROR) &&
             n == 1)
      need_unbind_all_vips = true;  // This member has left the group
    if (m_row.member_role == st_row_group_members::Role::ROLE_PRIMARY &&
        !strcasecmp(m_row.member_id.c_str(), server_uuid)) {
      is_master = true;
    } else if (m_row.member_role ==
                   st_row_group_members::Role::ROLE_SECONDARY &&
               (m_row.member_state == st_row_group_members::State::MGR_ONLINE ||
                m_row.member_state ==
                    st_row_group_members::State::MGR_RECOVERING)) {
      secondary_members.push_back(m_row);
    }
  }
  if (need_unbind_all_vips) {
    unbind_all_vips();
    pthread_mutex_unlock(&vip_variable_mutex);
    pthread_mutex_unlock(&msg_send_mu_);
    return nullptr;
  }
  if (work_number <= (n - recover_number) / 2) {
    master_is_running = false;
  } else {
    if (!get_cur_group_view_id(view_id_stamp, view_id_version)) {
      my_plugin_log_message(
          &plugin_ptr, MY_INFORMATION_LEVEL,
          "Cannot get MGR group view_id info, maybe need wait "
          "MGR complete initialization.");
    }
  }
  is_primary_for_vip = is_master;
  if (master_is_running && is_master) {
    all_node_bind_vips.clear();
    if (read_vip_floating_type == TO_PRIMARY) {
      // if read_vip_floating_type is not primary, used read vip should not
      // allocate to primary node
      for (auto it = bind_ips_with_nicname.begin();
           it != bind_ips_with_nicname.end(); it++) {
        // old vip not need anymore
        if (read_vips.find(it->first.c_str()) != read_vips.end())
          all_node_bind_vips[server_uuid].insert(it->first.c_str());
      }
    }
    if (secondary_members.size() == 0) {
      all_node_bind_vips[server_uuid] = read_vips;
    } else if (!get_secondary_node_bind_ips()) {
      pthread_mutex_unlock(&vip_variable_mutex);
      pthread_mutex_unlock(&msg_send_mu_);
      return nullptr;
    }
    all_node_bind_vips[server_uuid].insert(mgr_write_vip_addr);
    if (secondary_members.size()) {
      caculate_new_bind_relationship();
      bind_vip_according_map();
      send_secondary_bind_vips_message();
    } else {
      bind_vip_according_map();
    }
    need_break = false;
    pthread_cond_signal(&msg_send_cv_);
  }
  pthread_mutex_unlock(&vip_variable_mutex);
  pthread_mutex_unlock(&msg_send_mu_);
  return nullptr;
}

static void notify_group_replication_view() {
  pthread_mutex_lock(&check_killconn_mu_);
  need_check_killall_connection_and_force_member = true;
  pthread_cond_signal(&check_killconn_cv_);
  pthread_mutex_unlock(&check_killconn_mu_);
  pthread_mutex_lock(&mu_);
  need_check_bind_vip = true;
  pthread_cond_signal(&heartbeat_cv_);
  pthread_mutex_unlock(&mu_);
}

DEFINE_BOOL_METHOD(gdb_notify_view_change, (const char *)) {
  notify_group_replication_view();
  return false;
}
DEFINE_BOOL_METHOD(gdb_notify_quorum_loss, (const char *)) {
  notify_group_replication_view();
  return false;
}

SERVICE_TYPE_NO_CONST(group_membership_listener)
SERVICE_IMPLEMENTATION(greatdb_ha, group_membership_listener) = {
    gdb_notify_view_change, gdb_notify_quorum_loss};

DEFINE_BOOL_METHOD(gdb_notify_member_role_change, (const char *)) {
  notify_group_replication_view();
  return false;
}
DEFINE_BOOL_METHOD(gdb_notify_member_state_change, (const char *)) {
  notify_group_replication_view();
  return false;
}
//
SERVICE_TYPE_NO_CONST(group_member_status_listener)
SERVICE_IMPLEMENTATION(greatdb_ha, group_member_status_listener) = {
    gdb_notify_member_role_change, gdb_notify_member_state_change};

static void register_services() {
  Service_registrator r;

  r.register_service(SERVICE(greatdb_ha, group_membership_listener));
  r.register_service(SERVICE(greatdb_ha, group_member_status_listener));
}
static void unregister_services() {
  Service_registrator r;

  r.unregister_service(SERVICE_ID(greatdb_ha, group_membership_listener));
  r.unregister_service(SERVICE_ID(greatdb_ha, group_member_status_listener));
}

/*
  Initialize the daemon example at server start or plugin installation.

  SYNOPSIS
    greatdb_ha_plugin_init()

  DESCRIPTION
    Starts up heartbeatbeat thread

  RETURN VALUE
    0                    success
    1                    failure (cannot happen)
*/

static void *greatdb_ha_func(void *) {
  my_thread_init();
  while (1) {
    pthread_mutex_lock(&mu_);
    if (need_exit) {
      pthread_mutex_unlock(&mu_);
      break;
    }
    if (!need_check_bind_vip) {
      pthread_cond_wait(&heartbeat_cv_, &mu_);
    }
    if (need_exit) {
      pthread_mutex_unlock(&mu_);
      break;
    }
    need_check_bind_vip = false;
    pthread_mutex_unlock(&mu_);
    mysql_heartbeat();
  }
  my_thread_end();
  return nullptr;
}

static void *greatdb_ha_check_killconnection_and_force_member_func(void *) {
  my_thread_init();
  THD *thd;
  if (!(thd = new (std::nothrow) THD)) {
    my_thread_end();
    return nullptr;
  }
  thd->thread_stack = (char *)&thd;
  thd->store_globals();
  while (1) {
    pthread_mutex_lock(&check_killconn_mu_);
    if (need_exit) {
      pthread_mutex_unlock(&check_killconn_mu_);
      break;
    }
    if (!need_check_killall_connection_and_force_member) {
      pthread_cond_wait(&check_killconn_cv_, &check_killconn_mu_);
    }
    if (need_exit) {
      pthread_mutex_unlock(&check_killconn_mu_);
      break;
    }
    need_check_killall_connection_and_force_member = false;
    pthread_mutex_unlock(&check_killconn_mu_);
    check_kill_connection_and_force_member();
  }
  delete thd;
  my_thread_end();
  return nullptr;
}

static int get_local_listen_sock() {
  if (!plugin_port || !strlen(plugin_port)) return -1;
  int listenfd;
  listenfd = socket(vip_family, SOCK_STREAM, 0);
  if (listenfd == -1) {
    return -1;
  }
  int flags = fcntl(listenfd, F_GETFL, 0);
  fcntl(listenfd, F_SETFL, flags | O_NONBLOCK);
  int reuse = 1;
  setsockopt(listenfd, SOL_SOCKET, SO_REUSEPORT, &reuse, sizeof(reuse));
  if (vip_family == AF_INET) {
    struct sockaddr_in serveraddr;
    memset(&serveraddr, 0, sizeof(serveraddr));
    serveraddr.sin_family = AF_INET;
    serveraddr.sin_addr.s_addr = htonl(INADDR_ANY);
    serveraddr.sin_port = htons(atoi(plugin_port));
    if (bind(listenfd, (struct sockaddr *)&serveraddr, sizeof(serveraddr)) !=
        0) {
      my_plugin_log_message(
          &plugin_ptr, MY_ERROR_LEVEL,
          "Error: bind port to listen primary message failed, please check "
          "whether port defined by greatdb_ha_plugin_port is available");
      return -1;
    }
    if (listen(listenfd, 5) != 0) {
      my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                            "Error: listening failed");
      close(listenfd);
      return -1;
    }
  } else {
    struct sockaddr_in6 serveraddr6;
    memset(&serveraddr6, 0, sizeof(serveraddr6));
    serveraddr6.sin6_family = AF_INET6;
    serveraddr6.sin6_addr = in6addr_any;
    serveraddr6.sin6_port = htons(atoi(plugin_port));
    if (bind(listenfd, (struct sockaddr *)&serveraddr6, sizeof(serveraddr6)) !=
        0) {
      my_plugin_log_message(
          &plugin_ptr, MY_ERROR_LEVEL,
          "Error: bind port to listen primary message failed, please check "
          "whether port defined by greatdb_ha_plugin_port is available");
      close(listenfd);
      return -1;
    }
    if (listen(listenfd, 5) != 0) {
      my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                            "Error: listening failed");
      close(listenfd);
      return -1;
    }
  }
  return listenfd;
}

static void *greatdb_ha_receive_from_primary(void *) {
  my_thread_init();
  int max_fd = -1;
  int sock = get_local_listen_sock();
  if (sock == -1) {
    my_thread_end();
    return nullptr;
  }
  if (max_fd < sock) max_fd = sock;
  int clintfd = 0;
  while (1) {
    if (need_exit) {
      break;
    }
    fd_set readSet;
    FD_ZERO(&readSet);
    FD_SET(sock, &readSet);
    if (clintfd) {
      FD_SET(clintfd, &readSet);
      if (max_fd < clintfd) max_fd = clintfd;
    }
    struct timeval timeout;
    timeout.tv_sec = 5;
    timeout.tv_usec = 0;
    int n = select(max_fd + 1, &readSet, NULL, NULL, &timeout);
    if (n <= 0) continue;
    if (FD_ISSET(sock, &readSet)) {
      if (clintfd) close(clintfd);
      clintfd = accept(sock, NULL, NULL);
      if (clintfd == -1) continue;
      setsockopt(clintfd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    }
    if (FD_ISSET(clintfd, &readSet)) {
      int iret = receive_message(clintfd);
      if (iret == -1 &&
          (errno == EINTR || errno == EWOULDBLOCK || errno == EAGAIN))
        continue;
      else if (iret <= 0) {
        close(clintfd);
        clintfd = 0;
      } else
        handle_received_message(clintfd);
    }
  }
  close(sock);
  my_thread_end();
  return nullptr;
}

static void *greatdb_ha_primary_work_func(void *) {
  my_thread_init();
  while (true) {
    pthread_mutex_lock(&msg_send_mu_);
    if (need_exit) {
      pthread_mutex_unlock(&msg_send_mu_);
      break;
    }
    if (need_break) {
      pthread_cond_wait(&msg_send_cv_, &msg_send_mu_);
    }
    if (need_exit) {
      pthread_mutex_unlock(&msg_send_mu_);
      break;
    }
    pthread_mutex_lock(&vip_variable_mutex);
    bind_vip_according_map();
    send_secondary_bind_vips_message();
    pthread_mutex_unlock(&vip_variable_mutex);
    pthread_mutex_unlock(&msg_send_mu_);
    sleep(20);
  }
  my_thread_end();
  return nullptr;
}

static void process_read_vip_ips(const char *read_vip_ips) {
  if (!read_vip_ips) return;
  std::set<std::string> vips;
  split_string_according_delimiter(read_vip_ips, vips, ",");
  int old_size = nic_pos_list.size() + bind_ips_with_nicname.size();
  read_vips.clear();
  read_vips = vips;
  for (size_t i = old_size; i < read_vips.size() + 1; i++) {
    nic_pos_list.push(i);
  }
}

static int greatdb_ha_plugin_init(MYSQL_PLUGIN plugin_info) {
  DBUG_TRACE;
  my_thread_attr_t attr; /* Thread attributes */

  /*
    No threads exist at this point in time, so this is thread safe.
  */
  is_register_services = false;
  plugin_ptr = plugin_info;
  greatdb::all_vip_tope = greatdb::all_vip_tope_value;
  greatdb::nic_pos_list.push(0);
  process_read_vip_ips(greatdb::mgr_read_vip_addrs);
  my_thread_attr_init(&attr);
  my_thread_attr_setdetachstate(&attr, MY_THREAD_CREATE_JOINABLE);
  update_vip_family();

  if (my_thread_create(&ping_thread, &attr, ping_func, nullptr) != 0) {
    fprintf(stderr, "Could not create ping gateway thread!\n");
    return 0;
  }

  /* now create the thread */
  if (my_thread_create(&heartbeat_thread, &attr, greatdb_ha_func, nullptr) !=
      0) {
    fprintf(stderr, "Could not create heartbeat thread!\n");
    return 0;
  }
  if (my_thread_create(&check_killconnection_thread_and_force_member, &attr,
                       greatdb_ha_check_killconnection_and_force_member_func,
                       nullptr) != 0) {
    fprintf(stderr,
            "Could not create check killall connection and force member "
            "thread!\n");
    return 0;
  }
  if (my_thread_create(&listen_thread, &attr, greatdb_ha_receive_from_primary,
                       nullptr) != 0) {
    fprintf(stderr, "Could not create receive thread!\n");
    return 0;
  }
  if (my_thread_create(&primary_check_thread, &attr,
                       greatdb_ha_primary_work_func, nullptr) != 0) {
    fprintf(stderr, "Could not create primary send thread!\n");
    return 0;
  }
  unbind_all_vips();
  register_services();
  is_register_services = true;
  return 0;
}

/*
  Terminate the daemon example at server shutdown or plugin deinstallation.

  SYNOPSIS
    greatdb_ha_plugin_deinit()
    Does nothing.

  RETURN VALUE
    0                    success
    1                    failure (cannot happen)

*/

static int greatdb_ha_plugin_deinit(void *) {
  DBUG_TRACE;
  unbind_vips(bind_ips_with_nicname);
  // deinit_logging_service_for_plugin(&reg_srv, &log_bi, &log_bs);

  /*
    Need to wait for the hearbeat thread to terminate before closing
    the file it writes to and freeing the memory it uses
  */
  need_exit = true;
  pthread_mutex_lock(&mu_);
  pthread_cond_signal(&heartbeat_cv_);
  pthread_mutex_unlock(&mu_);
  if (heartbeat_thread.thread != 0) {
    my_thread_join(&heartbeat_thread, nullptr);
  }
  pthread_mutex_lock(&check_killconn_mu_);
  pthread_cond_signal(&check_killconn_cv_);
  pthread_mutex_unlock(&check_killconn_mu_);
  if (check_killconnection_thread_and_force_member.thread != 0) {
    my_thread_join(&check_killconnection_thread_and_force_member, nullptr);
  }
  pthread_mutex_lock(&greatdb::ping_mutex);
  pthread_cond_signal(&greatdb::ping_cv);
  pthread_mutex_unlock(&greatdb::ping_mutex);
  if (ping_thread.thread != 0) {
    my_thread_join(&ping_thread, nullptr);
  }
  pthread_mutex_lock(&msg_send_mu_);
  pthread_cond_signal(&msg_send_cv_);
  pthread_mutex_unlock(&msg_send_mu_);
  if (primary_check_thread.thread != 0) {
    my_thread_join(&primary_check_thread, nullptr);
  }
  if (listen_thread.thread != 0) {
    my_thread_join(&listen_thread, nullptr);
  }
  if (is_register_services) {
    unregister_services();
  }
  return 0;
}

}  // namespace greatdb

struct st_mysql_daemon greatdb_ha_plugin = {MYSQL_DAEMON_INTERFACE_VERSION};

static int check_write_vip(MYSQL_THD thd, SYS_VAR *, void *save,
                           struct st_mysql_value *value) {
  pthread_mutex_lock(&greatdb::vip_variable_mutex);
  DBUG_TRACE;
  char buff[NAME_CHAR_LEN];
  const char *str;
  (*(const char **)save) = nullptr;
  int length = sizeof(buff);
  if ((str = value->val_str(value, buff, &length)))
    str = thd->strmake(str, length);
  else {
    pthread_mutex_unlock(&greatdb::vip_variable_mutex);
    return 1; /* purecov: inspected */
  }
  if ((strlen(str) != 0) && !greatdb::check_ip_version(str)) {
    my_message(ER_WRONG_VALUE_FOR_VAR, "vip format is incorrect", MYF(0));
    pthread_mutex_unlock(&greatdb::vip_variable_mutex);
    return 1;
  }
  *(const char **)save = str;
  pthread_mutex_unlock(&greatdb::vip_variable_mutex);
  return 0;
}
static void mgr_vip_addr_update(MYSQL_THD thd MY_ATTRIBUTE((unused)),
                                SYS_VAR *var MY_ATTRIBUTE((unused)),
                                void *var_ptr, const void *save) {
  pthread_mutex_lock(&greatdb::vip_variable_mutex);
  const char *new_val = *(static_cast<const char **>(const_cast<void *>(save)));
  if (var_ptr != nullptr) {
    *((const char **)var_ptr) = new_val;
  }
  pthread_mutex_unlock(&greatdb::vip_variable_mutex);
  greatdb::mysql_heartbeat();
}

static int check_read_vip(MYSQL_THD thd, SYS_VAR *, void *save,
                          struct st_mysql_value *value) {
  DBUG_TRACE;
  pthread_mutex_lock(&greatdb::vip_variable_mutex);
  char buff[NAME_CHAR_LEN];
  const char *str;
  (*(const char **)save) = nullptr;
  int length = sizeof(buff);
  if ((str = value->val_str(value, buff, &length)))
    str = thd->strmake(str, length);
  else {
    pthread_mutex_unlock(&greatdb::vip_variable_mutex);
    return 1; /* purecov: inspected */
  }
  std::set<std::string> vips;
  greatdb::split_string_according_delimiter(str, vips, ",");
  sa_family_t write_vip_family = 0;
  if (greatdb::mgr_write_vip_addr && strlen(greatdb::mgr_write_vip_addr) > 2) {
    write_vip_family = greatdb::check_ip_version(greatdb::mgr_write_vip_addr);
  }
  for (auto it = vips.begin(); it != vips.end(); it++) {
    sa_family_t ret = greatdb::check_ip_version((*it).c_str());
    if (ret == 0) {
      my_message(ER_WRONG_VALUE_FOR_VAR, "read vip format is incorrect",
                 MYF(0));
      pthread_mutex_unlock(&greatdb::vip_variable_mutex);
      return 1;
    } else if (write_vip_family != 0 && ret != write_vip_family) {
      my_message(ER_WRONG_VALUE_FOR_VAR,
                 "only support read vip version is the same as write vip",
                 MYF(0));
      pthread_mutex_unlock(&greatdb::vip_variable_mutex);
      return 1;
    }
  }

  *(const char **)save = str;
  pthread_mutex_unlock(&greatdb::vip_variable_mutex);
  return 0;
}

static void mgr_read_vip_addr_update(MYSQL_THD thd MY_ATTRIBUTE((unused)),
                                     SYS_VAR *var MY_ATTRIBUTE((unused)),
                                     void *var_ptr, const void *save) {
  pthread_mutex_lock(&greatdb::vip_variable_mutex);
  const char *new_val = *(static_cast<const char **>(const_cast<void *>(save)));
  if (var_ptr != nullptr) {
    *((const char **)var_ptr) = new_val;
  }
  greatdb::process_read_vip_ips(greatdb::mgr_read_vip_addrs);
  pthread_mutex_unlock(&greatdb::vip_variable_mutex);
  greatdb::mysql_heartbeat();
}

static bool check_vip_bind_relationship(
    std::map<std::string, std::set<std::string>> &vip_bind_maps) {
  size_t old_relation = greatdb::all_node_bind_vips.size();
  size_t new_relation = vip_bind_maps.size();
  if (new_relation != old_relation) return true;
  for (auto it = vip_bind_maps.begin(); it != vip_bind_maps.end(); it++) {
    // can not allocate vip to unknown member
    if (greatdb::all_node_bind_vips.find(it->first) ==
        greatdb::all_node_bind_vips.end())
      return true;
    for (auto it2 = it->second.begin(); it2 != it->second.end(); it2++) {
      // can not change tope to add new vip
      if (greatdb::read_vips.find(*it2) == greatdb::read_vips.end() &&
          strcasecmp((*it2).c_str(), greatdb::mgr_write_vip_addr))
        return true;
      // can not change write vip tope
      if (!strcasecmp((*it2).c_str(), greatdb::mgr_write_vip_addr) &&
          greatdb::all_node_bind_vips[it->first].find(
              greatdb::mgr_write_vip_addr) ==
              greatdb::all_node_bind_vips[it->first].end()) {
        return true;
      }
    }
  }
  return false;
}
static int check_vip_tope(MYSQL_THD thd, SYS_VAR *, void *save,
                          struct st_mysql_value *value) {
  pthread_mutex_lock(&greatdb::vip_variable_mutex);
  if (!greatdb::is_primary_for_vip) {
    my_message(ER_WRONG_VALUE_FOR_VAR,
               "this operation can only run in primary node", MYF(0));
    pthread_mutex_unlock(&greatdb::vip_variable_mutex);
    return 1;
  }
  char buff[STRING_BUFFER_USUAL_SIZE];
  std::map<std::string, std::set<std::string>> new_val_map;
  const char *str = nullptr;
  (*(const char **)save) = nullptr;
  int length = 0;
  length = sizeof(buff);

  if ((str = value->val_str(value, buff, &length)))
    str = thd->strmake(str, length);
  else {
    pthread_mutex_unlock(&greatdb::vip_variable_mutex);
    return 1;
  }
  // If option value is empty string, just update its value.
  if (length == 0) goto update_value;
  if (greatdb::get_all_node_ips(str, new_val_map) == -1 ||
      check_vip_bind_relationship(new_val_map)) {
    my_message(ER_WRONG_VALUE_FOR_VAR, "vip tope value format error", MYF(0));
    pthread_mutex_unlock(&greatdb::vip_variable_mutex);
    return 1;
  }
  pthread_mutex_unlock(&greatdb::vip_variable_mutex);
update_value:
  *(const char **)save = str;
  pthread_mutex_unlock(&greatdb::vip_variable_mutex);
  return 0;
}

static void mgr_vip_tope_update(MYSQL_THD thd MY_ATTRIBUTE((unused)),
                                SYS_VAR *var MY_ATTRIBUTE((unused)),
                                void *var_ptr, const void *save) {
  pthread_mutex_lock(&greatdb::msg_send_mu_);
  pthread_mutex_lock(&greatdb::vip_variable_mutex);
  const char *new_val = *(static_cast<const char **>(const_cast<void *>(save)));
  if (var_ptr != nullptr) {
    memset(greatdb::all_vip_tope_value, 0, 1024);
    memcpy(greatdb::all_vip_tope_value, new_val, strlen(new_val));
    greatdb::get_all_node_ips(new_val, greatdb::all_node_bind_vips);
    greatdb::bind_vip_according_map();
    greatdb::send_secondary_bind_vips_message();
  }
  pthread_mutex_unlock(&greatdb::vip_variable_mutex);
  pthread_mutex_unlock(&greatdb::msg_send_mu_);
}

static void mgr_vip_nic_update(MYSQL_THD thd MY_ATTRIBUTE((unused)),
                               SYS_VAR *var MY_ATTRIBUTE((unused)),
                               void *var_ptr, const void *save) {
  pthread_mutex_lock(&greatdb::vip_variable_mutex);
  greatdb::unbind_vips(greatdb::bind_ips_with_nicname);
  const char *new_val = *(static_cast<const char **>(const_cast<void *>(save)));
  if (var_ptr != nullptr) {
    *((const char **)var_ptr) = new_val;
  }
  pthread_mutex_unlock(&greatdb::vip_variable_mutex);
  greatdb::mysql_heartbeat();
}

static void mgr_vip_mask_update(MYSQL_THD thd MY_ATTRIBUTE((unused)),
                                SYS_VAR *var MY_ATTRIBUTE((unused)),
                                void *var_ptr, const void *save) {
  pthread_mutex_lock(&greatdb::vip_variable_mutex);
  greatdb::unbind_vips(greatdb::bind_ips_with_nicname);
  const char *new_val = *(static_cast<const char **>(const_cast<void *>(save)));
  if (var_ptr != nullptr) {
    *((const char **)var_ptr) = new_val;
  }
  pthread_mutex_unlock(&greatdb::vip_variable_mutex);
  greatdb::mysql_heartbeat();
}

static void force_change_mgr_vip_enabled_update(
    MYSQL_THD thd MY_ATTRIBUTE((unused)), SYS_VAR *var MY_ATTRIBUTE((unused)),
    void *var_ptr, const void *save) {
  const bool set_val = *static_cast<const bool *>(save);
  if (set_val) {
    greatdb::mysql_heartbeat();
  }
  *(bool *)var_ptr = false;
}

static void kill_connection_mode_enabled_update(
    MYSQL_THD thd MY_ATTRIBUTE((unused)), SYS_VAR *var MY_ATTRIBUTE((unused)),
    void *var_ptr, const void *save) {
  const bool set_val = *static_cast<const bool *>(save);
  *(bool *)var_ptr = set_val;
  if (set_val) {
    greatdb::check_kill_connection_and_force_member();
  }
}

static void mgr_vip_enabled_update(MYSQL_THD thd MY_ATTRIBUTE((unused)),
                                   SYS_VAR *var MY_ATTRIBUTE((unused)),
                                   void *var_ptr, const void *save) {
  pthread_mutex_lock(&greatdb::vip_variable_mutex);
  const bool set_val = *static_cast<const bool *>(save);
  *(bool *)var_ptr = set_val;
  pthread_mutex_unlock(&greatdb::vip_variable_mutex);
  if (set_val) {
    greatdb::mysql_heartbeat();
  }
}

static int check_gateway_address(MYSQL_THD thd, SYS_VAR *, void *save,
                                 struct st_mysql_value *value) {
  DBUG_TRACE;

  char buff[NAME_CHAR_LEN];
  const char *str;

  (*(const char **)save) = nullptr;
  int length = sizeof(buff);
  if ((str = value->val_str(value, buff, &length)))
    str = thd->strmake(str, length);
  else {
    return 1; /* purecov: inspected */
  }

  sa_family_t cur_family = greatdb::check_ip_version(str);
  int check_ping_sock = socket(cur_family, SOCK_RAW, IPPROTO_ICMP);
  if (check_ping_sock < 0) {
    my_message(ER_WRONG_VALUE_FOR_VAR,
               "ping gateway need set CAP_NET_RAW capability", MYF(0));
    return 1;
  }

  if (!greatdb::ping_gateway(check_ping_sock, str)) {
    my_message(ER_WRONG_VALUE_FOR_VAR,
               "please check whether gateway address can be connected", MYF(0));
    close(check_ping_sock);
    return 1;
  }
  close(check_ping_sock);
  *(const char **)save = str;

  return 0;
}

static void update_gateway_address(MYSQL_THD, SYS_VAR *, void *var_ptr,
                                   const void *save) {
  DBUG_TRACE;
  const char *new_val = *(static_cast<const char **>(const_cast<void *>(save)));
  pthread_mutex_lock(&greatdb::ping_mutex);
  pthread_cond_signal(&greatdb::ping_cv);
  if (var_ptr != nullptr) {
    *((const char **)var_ptr) = new_val;
  }
  pthread_mutex_unlock(&greatdb::ping_mutex);
}

/*
  Plugin library descriptor
*/
static MYSQL_SYSVAR_STR(
    mgr_vip_ip,                                /* name */
    greatdb::mgr_write_vip_addr,               /* var */
    PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_MEMALLOC, /* optional var | malloc string*/
    "The mgr vip address, host.", check_write_vip, /* check func*/
    mgr_vip_addr_update,                           /* update func*/
    nullptr);                                      /* default*/

static MYSQL_SYSVAR_STR(
    mgr_read_vip_ips,                          /* name */
    greatdb::mgr_read_vip_addrs,               /* var */
    PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_MEMALLOC, /* optional var | malloc string*/
    "The mgr read vip address, host,host", check_read_vip, /* check func*/
    mgr_read_vip_addr_update,                              /* update func*/
    nullptr);                                              /* default*/

static const char *read_vip_floating_type_names[] = {
    "TO_PRIMARY", "TO_ANOTHER_SECONDARY", nullptr};
static TYPELIB read_vip_floating_typelib = {
    array_elements(read_vip_floating_type_names) - 1,
    "read_vip_floating_typelib", read_vip_floating_type_names, nullptr};

static MYSQL_SYSVAR_ENUM(
    mgr_read_vip_floating_type, greatdb::read_vip_floating_type,
    PLUGIN_VAR_RQCMDARG,
    "if a secondary node is removed from group, then this node's read_vip "
    "should be floated to other nodes, "
    "TO_PRIMARY means this read vip will be floated to primary node, "
    "TO_ANOTHER_SECONDARY floating to one of the other secondary node",
    nullptr, nullptr, greatdb::TO_PRIMARY, &read_vip_floating_typelib);

static MYSQL_SYSVAR_STR(
    mgr_vip_broad,                             /* name */
    greatdb::vip_broadip,                      /* var */
    PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_MEMALLOC, /* optional var | malloc string*/
    "The mgr vip broadcast, host.", nullptr,   /* check func*/
    nullptr,                                   /* update func*/
    "255.255.255.255");                        /* default*/

static MYSQL_SYSVAR_ULONG(
    send_arp_packge_times,                                 /* name */
    greatdb::send_arp_times,                               /* var */
    PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_PERSIST_AS_READ_ONLY, /* optional var */
    "The number of times to broad arp packge after bind vip.",
    nullptr, /* check func. */
    nullptr, /* update func. */
    5,       /* default */
    3,       /* min */
    20,      /* max */
    0        /* block */
);

static MYSQL_SYSVAR_STR(
    port,                 /* name */
    greatdb::plugin_port, /* var */
    PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_MEMALLOC |
        PLUGIN_VAR_READONLY, /* optional var | malloc string*/
    "greatdb ha plugin transfer port", nullptr, /* check func*/
    nullptr,                                    /* update func*/
    nullptr);                                   /* default*/

static MYSQL_SYSVAR_STR(vip_tope,              /* name */
                        greatdb::all_vip_tope, /* var */
                        PLUGIN_VAR_OPCMDARG,   /* optional var*/
                        "relationship between vip add mgr nodes"
                        "uuid1::vip1; uuid2::vip2,vip3; uuid3::vip4",
                        check_vip_tope,      /* check func*/
                        mgr_vip_tope_update, /* update func*/
                        nullptr);            /* default*/

static MYSQL_SYSVAR_STR(
    mgr_vip_nic,                               /* name */
    greatdb::vip_nic,                          /* var */
    PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_MEMALLOC, /* optional var | malloc string*/
    "name of the network card", nullptr,       /* check func*/
    mgr_vip_nic_update,                        /* update func*/
    nullptr);                                  /* default*/

static MYSQL_SYSVAR_STR(
    mgr_vip_mask,                              /* name */
    greatdb::vip_netmask,                      /* var */
    PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_MEMALLOC, /* optional var | malloc string*/
    "netmask of vip ", nullptr,                /* check func*/
    mgr_vip_mask_update,                       /* update func*/
    "255.255.255.0");

static MYSQL_SYSVAR_BOOL(enable_mgr_vip,      /* name */
                         greatdb::enable_vip, /* var */
                         PLUGIN_VAR_OPCMDARG, "whether enable mgr vip.",
                         nullptr,                /* check func. */
                         mgr_vip_enabled_update, /* update func*/
                         0                       /* default */
);

static MYSQL_SYSVAR_STR(gateway_address,              /* name */
                        greatdb::gateway_address_var, /* var */
                        PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_MEMALLOC,
                        /* optional var | malloc string*/
                        "The address of gateway",
                        check_gateway_address,  /* check func*/
                        update_gateway_address, /* update func*/
                        nullptr);               /* default*/

static MYSQL_SYSVAR_BOOL(
    mgr_exit_primary_kill_connection_mode, /* name */
    greatdb::check_killall_connection,     /* var */
    PLUGIN_VAR_OPCMDARG,
    "whether check kill old primary's connection after primary changed.",
    nullptr,                             /* check func. */
    kill_connection_mode_enabled_update, /* update func*/
    0                                    /* default */
);

static MYSQL_SYSVAR_BOOL(force_change_mgr_vip,    /* name */
                         greatdb::force_bind_vip, /* var */
                         PLUGIN_VAR_OPCMDARG, "Force binding floating IP",
                         nullptr,                             /* check func. */
                         force_change_mgr_vip_enabled_update, /* update func*/
                         0                                    /* default */
);

static SYS_VAR *greatdb_ha_system_vars[] = {
    MYSQL_SYSVAR(mgr_vip_ip),
    MYSQL_SYSVAR(mgr_read_vip_ips),
    MYSQL_SYSVAR(mgr_vip_nic),
    MYSQL_SYSVAR(vip_tope),
    MYSQL_SYSVAR(port),
    MYSQL_SYSVAR(mgr_vip_broad),
    MYSQL_SYSVAR(enable_mgr_vip),
    MYSQL_SYSVAR(mgr_exit_primary_kill_connection_mode),
    MYSQL_SYSVAR(gateway_address),
    MYSQL_SYSVAR(mgr_vip_mask),
    MYSQL_SYSVAR(force_change_mgr_vip),
    MYSQL_SYSVAR(send_arp_packge_times),
    MYSQL_SYSVAR(mgr_read_vip_floating_type),
    nullptr,
};

mysql_declare_plugin(greatdb_ha){
    MYSQL_DAEMON_PLUGIN,
    &greatdb_ha_plugin,
    "greatdb_ha",
    "GreatOpenSource",
    "greatdb ha plugin",
    PLUGIN_LICENSE_GPL,
    greatdb::greatdb_ha_plugin_init,   /* Plugin Init */
    nullptr,                           /* Plugin Check uninstall */
    greatdb::greatdb_ha_plugin_deinit, /* Plugin Deinit */
    0x0100 /* 1.0 */,
    nullptr,                /* status variables                */
    greatdb_ha_system_vars, /* system variables                */
    nullptr,                /* config options                  */
    0,                      /* flags                           */
} mysql_declare_plugin_end;
