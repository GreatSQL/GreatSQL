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

#include "network_func.h"
#include <regex>
#include "mgr_status_notify.h"
#include "mysql.h"
#include "mysql/service_my_plugin_log.h"

namespace greatdb {

bool get_mac_and_index(int sock_fd, unsigned char *mac, int &sll_ifindex) {
  struct ifreq ifr;
  memset(&ifr, 0, sizeof(ifr));
  strncpy(ifr.ifr_name, vip_nic, sizeof(ifr.ifr_name) - 1);
  ifr.ifr_name[sizeof(ifr.ifr_name) - 1] = '\0';

  if (ioctl(sock_fd, SIOCGIFINDEX, &ifr) == -1) {
    my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                          "Get mac SIOCGIFINDEX failed. %s", strerror(errno));
    return false;
  }
  sll_ifindex = ifr.ifr_ifindex;

  if (ioctl(sock_fd, SIOCGIFHWADDR, &ifr) < 0) {
    my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                          "Get mac SIOCGIFHWADDR failed. %s", strerror(errno));
    return false;
  }
  memcpy(mac, ifr.ifr_hwaddr.sa_data, 6);

  // Set or get the broadcast flag.
  int tmp_flag = 1;
  if (setsockopt(sock_fd, SOL_SOCKET, SO_BROADCAST, &tmp_flag,
                 sizeof(tmp_flag)) == -1) {
    my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                          "Set mac SO_BROADCAST failed. %s", strerror(errno));
    return false;
  }
  return true;
}

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

std::string get_nic_name(char nic_label_pos) {
  std::string nic_name(vip_nic);
  if (nic_name.size() > IFNAMSIZ - 3)
    nic_name = nic_name.substr(0, IFNAMSIZ - 3);
  nic_name.append(":");
  if (mgr_vip_label_var) nic_name.append(mgr_vip_label_var);
  nic_name.append(1, nic_label_pos);
  return nic_name;
}

int send_arp(const char *vip) {
  int sock_fd;
  struct in_addr s, r;
  sockaddr_ll sl;
  unsigned char mac[6];
  memset(&sl, 0, sizeof(sl));

  sock_fd = socket(AF_PACKET, SOCK_RAW, htons(ETH_P_ALL));
  if (sock_fd < 0) {
    return 0;
  }
  if (!get_mac_and_index(sock_fd, mac, sl.sll_ifindex)) return 0;
  sl.sll_family = AF_PACKET;
  sl.sll_protocol = htons(ETH_P_ARP);

  struct arppacket arp = {DEST_MAC,
                          DEST_MAC,
                          htons(ETH_P_ARP),
                          htons(ARPHRD_ETHER),
                          htons(ETHERTYPE_IP),
                          ETH_ALEN,
                          4,
                          htons(ARPOP_REQUEST),
                          DEST_MAC,
                          {0},
                          DEST_MAC,
                          {0}};
  memcpy(arp.src_mac, mac, 6);
  memcpy(arp.ar_sha, mac, 6);

  inet_aton(vip, &s);
  memcpy(&arp.ar_sip, &s, sizeof(s));
  inet_aton(vip, &r);
  memcpy(&arp.ar_tip, &r, sizeof(r));

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

int send_na(const char *vip) {
#ifndef NDEBUG
  if (DBUG_EVALUATE_IF("test_vip", true, false)) {
    return 1;
  }
#endif
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

bool unbind_vip(const char *vip, const char *nic_name) {
  if (!vip || !vip_netmask || !vip_nic) return false;
#ifndef NDEBUG
  if (DBUG_EVALUATE_IF("test_vip", true, false)) {
    bind_ips_with_nicname.erase(vip);
    release_nic_pos(nic_name);
    return true;
  }
#endif
  int fd = 0;
  if ((fd = socket(vip_family, SOCK_DGRAM, 0)) < 0) {
    return false;
  }
  kill_connection_bind_to_vip(vip);
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
  close(fd);
  return true;
}

bool bind_vip_ipv6(const char *vip) {
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
  close(sockfd);
  return true;
}

bool bind_vip_ipv4(const char *vip) {
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
  close(fd);
  return true;
}

bool validate_uuid(const std::string arg_uuid) {
  std::regex pattern(
      "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$");
  if (!std::regex_match(arg_uuid, pattern)) {
    my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                          "skip error format arg_vip_tope: %s",
                          arg_uuid.c_str());
    return true;
  }
  return false;
}

bool validate_vips(std::set<std::string> vips) {
  sa_family_t pre_vip_family = 0;
  for (const auto &it : vips) {
    sa_family_t ret = greatdb::check_ip_version(it.c_str());
    if (ret == 0) {
      my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                            "read vip format is incorrect: %s", it.c_str());
      return true;
    } else if (pre_vip_family != 0 && ret != pre_vip_family) {
      my_plugin_log_message(
          &plugin_ptr, MY_ERROR_LEVEL,
          "only support read vip version is the same as others: %s",
          it.c_str());
      return true;
    }
    pre_vip_family = ret;
  }
  return false;
}

void kill_connection_bind_to_vip(const char *need_unbind_vip) {
  Global_THD_manager *thd_manager = Global_THD_manager::get_instance();
  my_plugin_log_message(&plugin_ptr, MY_WARNING_LEVEL,
                        "kill connections binding to vip: %s", need_unbind_vip);
  Kill_Ip_Conn unbind_vip_kill_conn(need_unbind_vip);
  thd_manager->do_for_all_thd(&unbind_vip_kill_conn);
}

void killall_connections() {
  Global_THD_manager *thd_manager = Global_THD_manager::get_instance();
  my_plugin_log_message(&plugin_ptr, MY_WARNING_LEVEL,
                        "kill all connections after primary changed");
  Kill_All_Conn set_kill_conn;
  thd_manager->do_for_all_thd(&set_kill_conn);
}

void SlaveConnManager::clear_slave_conn_map() {
  for (const auto &it : slave_conn_map) {
    mysql_close(it.second);
  }
  slave_conn_map.clear();
}

void SlaveConnManager::del_conn_by_id(const std::string member_id) {
  if (slave_conn_map.find(member_id) != slave_conn_map.end()) {
    mysql_close(slave_conn_map[member_id]);
    slave_conn_map.erase(member_id);
  }
}

MYSQL *SlaveConnManager::get_conn_by_info(
    const st_row_group_members &secondary_member_info) {
  std::string cur_member_id = secondary_member_info.member_id;
  if (slave_conn_map.find(cur_member_id) != slave_conn_map.end()) {
    return slave_conn_map[cur_member_id];
  }
  return init_conn_by_info(secondary_member_info);
}

std::pair<bool, std::string> SlaveConnManager::send_message(
    const st_row_group_members &secondary_member_info, std::string message) {
  std::string ret_msg = "";
  size_t retry_count = 0;
  for (; retry_count < 3; retry_count++) {
    MYSQL *mysql = nullptr;
    MYSQL_RES *result = nullptr;
    MYSQL_ROW row;
    if (retry_count > 0) {
      mysql = init_conn_by_info(secondary_member_info);
    } else {
      mysql = get_conn_by_info(secondary_member_info);
    }

    if (nullptr == mysql) continue;
    if (mysql_real_query(mysql, message.c_str(), message.length())) {
      my_plugin_log_message(
          &plugin_ptr, MY_WARNING_LEVEL,
          "SlaveConnManager send_message to %s:%d failed due to %s",
          secondary_member_info.member_host.c_str(),
          secondary_member_info.member_port, mysql_error(mysql));
      continue;
    }
    result = mysql_store_result(mysql);
    if (!result) {
      my_plugin_log_message(
          &plugin_ptr, MY_WARNING_LEVEL,
          "SlaveConnManager get result %s from %s:%d failed due to %s",
          message.c_str(), secondary_member_info.member_host.c_str(),
          secondary_member_info.member_port, mysql_error(mysql));
      continue;
    }
    row = mysql_fetch_row(result);
    if (!row || !row[0]) {
      mysql_free_result(result);
      continue;
    }
    ret_msg = row[0];
    mysql_free_result(result);
    break;
  }
  if (retry_count >= 3) {
    my_plugin_log_message(
        &plugin_ptr, MY_ERROR_LEVEL,
        "SlaveConnManager::send_message %s:%u retry 3 times failed",
        secondary_member_info.member_host.c_str(),
        secondary_member_info.member_port);
    del_conn_by_id(secondary_member_info.member_id);
    return std::pair<bool, std::string>(true, ret_msg);
  }
  return std::pair<bool, std::string>(false, ret_msg);
}

void SlaveConnManager::clear_mgr_recovery_user() {
  m_rpl_user.clear();
  m_rpl_password.clear();
}

bool SlaveConnManager::get_mgr_recovery_user() {
  // will update after ER_ACCESS_DENIED_ERROR
  if (!m_rpl_user.empty()) return false;
  const char *tmp_user;
  char tmp_password[MAX_PASSWORD_LENGTH + 1];
  size_t password_size = sizeof(tmp_password);
  channel_map.rdlock();
  Master_info *recover_info = channel_map.get_mi("group_replication_recovery");
  if (!recover_info) goto fail;
  tmp_user = recover_info->get_user();
  if (!tmp_user) goto fail;
  if (recover_info->get_password(tmp_password, &password_size)) goto fail;
  channel_map.unlock();
  m_rpl_user = tmp_user;
  m_rpl_password = tmp_password;
  return false;
fail:
  my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                        "SlaveConnManager get rpl_user info failed");
  channel_map.unlock();
  return true;
}

MYSQL *SlaveConnManager::init_conn_by_info(
    const st_row_group_members &secondary_member_info) {
  del_conn_by_id(secondary_member_info.member_id);
  MYSQL *mysql = nullptr;
  std::string user, password;
  mysql = mysql_init(mysql);
  ulong ha_connect_timeout = HA_CONNECT_TIMEOUT;
  mysql_options(mysql, MYSQL_OPT_CONNECT_TIMEOUT, &ha_connect_timeout);
  mysql_options(mysql, MYSQL_OPT_READ_TIMEOUT, &ha_connect_timeout);
  mysql_options(mysql, MYSQL_OPT_WRITE_TIMEOUT, &ha_connect_timeout);

  bool get_key = true;
  mysql_options(mysql, MYSQL_OPT_GET_SERVER_PUBLIC_KEY, &get_key);
  auto ssl_mode = SSL_MODE_DISABLED;
  mysql_options(mysql, MYSQL_OPT_SSL_MODE, &ssl_mode);

  if (get_mgr_recovery_user()) {
    return nullptr;
  }

  if (!mysql_real_connect(mysql, secondary_member_info.member_host.c_str(),
                          m_rpl_user.c_str(), m_rpl_password.c_str(), nullptr,
                          secondary_member_info.member_port, nullptr, 0)) {
    if (ER_ACCESS_DENIED_ERROR == mysql_errno(mysql)) {
      clear_mgr_recovery_user();
    }
    return nullptr;
  }

  slave_conn_map[secondary_member_info.member_id] = mysql;
  return slave_conn_map[secondary_member_info.member_id];
}

}  // namespace greatdb
