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

#ifndef PLUGIN_GDB_HA_NETWORK_H
#define PLUGIN_GDB_HA_NETWORK_H

#include <arpa/inet.h>
#include <fcntl.h>
#include <ifaddrs.h>
#include <net/ethernet.h>
#include <net/if.h>
#include <net/if_arp.h>
#include <netdb.h>
#include <netinet/icmp6.h>
#include <netinet/in.h>
#include <netinet/ip6.h>
#include <netinet/ip_icmp.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <mutex>
#include <queue>
#include <string>
#include <utility>

#include "mysql.h"
#include "mysql/my_loglevel.h"  // for INFORMATION_LEVEL
#include "mysql/plugin.h"
#include "sql/dd/dd_kill_immunizer.h"  // dd:DD_kill_immunizer
#include "sql/mysqld_thd_manager.h"

namespace greatdb {
struct st_row_group_members;

#define HEART_STRING_BUFFER 200
#define DEST_MAC \
  { 0xFF, 0xff, 0xff, 0xff, 0xff, 0xff }
#define DEST_IP6_ADDR "ff02::1"
#define HA_CONNECT_TIMEOUT 3
#define HA_BIND_RETRY_TIMES 5

extern MYSQL_PLUGIN plugin_ptr;
extern sa_family_t vip_family;
extern char *vip_nic;
extern std::map<std::string, std::string>
    system_bind_ips;  // ip address, nicname
extern char *mgr_vip_label_var;
extern char *vip_netmask;
extern std::map<std::string, std::string> bind_ips_with_nicname;
extern std::queue<char> nic_pos_list;
extern ulong send_arp_times;

void release_nic_pos(const char *nic_name);

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

bool get_mac_and_index(int sock_fd, unsigned char *mac, int &sll_ifindex);
int get_mac(const char *eno, unsigned char *mac);
void get_all_ips();
std::string get_nic_name(char nic_label_pos);
int send_arp(const char *vip);
int send_na(const char *vip);
bool unbind_vip(const char *vip, const char *nic_name);
bool bind_vip_ipv6(const char *vip);
bool bind_vip_ipv4(const char *vip);
sa_family_t check_ip_version(const char *ip);
bool validate_uuid(const std::string arg_uuid);
bool validate_vips(std::set<std::string> vips);

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
      char ip[INET6_ADDRSTRLEN];
      memset(ip, 0, INET6_ADDRSTRLEN);
      if (vip_family == AF_INET) {
        struct sockaddr_in addr;
        socklen_t addr_len = sizeof(addr);
        getsockname(thd_to_kill->get_net()->fd, (struct sockaddr *)&addr,
                    &addr_len);
        inet_ntop(AF_INET, &(addr.sin_addr), ip, INET_ADDRSTRLEN);
      } else {
        struct sockaddr_in6 addr6;
        socklen_t addr6_len = sizeof(addr6);
        getsockname(thd_to_kill->get_net()->fd, (struct sockaddr *)&addr6,
                    &addr6_len);
        inet_ntop(AF_INET6, &(addr6.sin6_addr), ip, INET6_ADDRSTRLEN);
      }
      if (strcmp(ip, ip_address_need_to_kill) == 0) {
        // normal method like kill [connection |query] processlist_id
        thd_to_kill->awake(THD::KILL_CONNECTION);
      }
    }
    mysql_mutex_unlock(&thd_to_kill->LOCK_thd_data);
  }

 private:
  const char *ip_address_need_to_kill;
};

void kill_connection_bind_to_vip(const char *need_unbind_vip);

void killall_connections();

class SlaveConnManager {
 public:
  SlaveConnManager() {}
  ~SlaveConnManager() {
    clear_slave_conn_map();
    clear_mgr_recovery_user();
  }

  std::pair<bool, std::string> send_message(
      const st_row_group_members &secondary_member_info, std::string message);
  void clear_slave_conn_map();
  void clear_mgr_recovery_user();

  /**
   * make SlaveConnManager singleton
   * @return
   */
  static SlaveConnManager &get_instance() {
    static SlaveConnManager instance;
    return instance;
  }
  SlaveConnManager(const SlaveConnManager &) = delete;
  SlaveConnManager(SlaveConnManager &&) = delete;
  SlaveConnManager &operator=(const SlaveConnManager &) = delete;
  SlaveConnManager &operator=(SlaveConnManager &&) = delete;

 private:
  MYSQL *get_conn_by_info(const st_row_group_members &secondary_member_info);
  MYSQL *init_conn_by_info(const st_row_group_members &secondary_member_info);
  void del_conn_by_id(const std::string member_id);
  std::map<std::string, MYSQL *> slave_conn_map;  // slave_uuid, mysql
  bool get_mgr_recovery_user();
  std::string m_rpl_user;
  std::string m_rpl_password;
};

}  // namespace greatdb

#endif
