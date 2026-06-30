/* Copyright (c) 2023, 2026, GreatDB Software Co., Ltd.

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
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <atomic>
#include <chrono>              // std::chrono::seconds
#include <condition_variable>  // std::condition_variable, std::cv_status
#include <cstring>
#include <mutex>  // std::mutex, std::unique_lock
#include <queue>
#include <regex>
#include <set>

#include "libbinlogevents/include/uuid.h"
#include "mgr_status_notify.h"
#include "my_dbug.h"  // for DBUG_TRACE
#include "my_inttypes.h"
#include "my_sys.h"  // my_write, my_malloc
#include "my_thread.h"
#include "mysql/plugin.h"
#include "mysql/plugin_group_replication.h"  // for GROUP_REPLICATI...
#include "mysql_com.h"                       // for NAME_LEN
#include "network_func.h"
#include "ping_gateway.h"
#include "sql/mysqld.h"
#include "sql/mysqld_thd_manager.h"
#include "sql/replication.h"
#include "sql/rpl_group_replication.h"  // for get_group_repli...
#include "sql/sql_class.h"
#include "sql/sql_const.h"
#include "vip_udf_func.h"

namespace greatdb {
#define MAX_WAIT_TIME 2
#define MAX_ALL_VIP_TOPE_LENGTH 2048
char *mgr_vip_addr;
enum read_vip_floating_policy_t {
  TO_PRIMARY,
  TO_ANOTHER_SECONDARY,
};
char *mgr_write_vip_addr;
char *mgr_read_vip_addrs;
char all_vip_tope_value[MAX_ALL_VIP_TOPE_LENGTH];
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
std::queue<char> nic_pos_list;
std::vector<st_row_group_members> secondary_members;
pthread_mutex_t vip_variable_mutex;
std::set<std::string> read_vips;
sa_family_t vip_family = AF_INET;
char *vip_nic;
char *vip_netmask;
bool enable_vip;
bool force_bind_vip;
bool all_thread_need_exit = false;
bool check_killall_connection;
char *mgr_vip_label_var;
ulong send_arp_times;
ulong force_wait_timeout_var;
std::atomic_bool broadcast_thread_need_wait_next_alloc = true;
std::atomic_bool need_check_bind_vip;
std::atomic_bool need_check_killall_connection_and_force_member;
my_thread_handle primary_broadcast_tope_thread;  // run in primary node
my_thread_handle alloc_new_vip_tope_thread;
pthread_mutex_t alloc_new_vip_tope_mu_;
pthread_cond_t alloc_new_vip_tope_cv_;
my_thread_handle check_killconnection_thread_and_force_member;
std::mutex check_killconn_mu_;
std::condition_variable check_killconn_cv_;

pthread_mutex_t primary_broadcast_tope_mu_;
pthread_cond_t primary_broadcast_tope_cv_;
MYSQL_PLUGIN plugin_ptr;
bool is_register_services;

/*ping variables begin*/
my_thread_handle ping_thread;
bool is_stopped_by_ha = false;
pthread_mutex_t ping_mutex;
pthread_cond_t ping_cv;
int ping_sock = -1;
time_t last_ping_succ_time;
time_t last_ping_fail_time;
char *gateway_address_var;
sa_family_t ping_family = AF_INET;
/*ping variables end*/

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

static char pos_to_label(size_t pos) {
  if (pos >= 36) {
    return 'A' + pos - 36;
  } else if (pos >= 10) {
    return 'a' + pos - 10;
  } else {
    return '0' + pos;
  }
}

void release_nic_pos(const char *nic_name) {
  if (!nic_name) return;
  std::string nic_name_str(nic_name);
  int nic_pos = atoi(nic_name_str.substr(nic_name_str.length() - 1).c_str());
  nic_pos_list.push(pos_to_label(nic_pos));
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
#ifndef NDEBUG
      if (DBUG_EVALUATE_IF("test_vip", true, false))
        my_plugin_log_message(&plugin_ptr, MY_INFORMATION_LEVEL,
                              "try to unbind vip: %s success on nic: %s",
                              it_to_unbind->first.c_str(),
                              it_to_unbind->second.c_str());
      else
#endif
        my_plugin_log_message(&plugin_ptr, MY_INFORMATION_LEVEL,
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
      my_plugin_log_message(&plugin_ptr, MY_INFORMATION_LEVEL,
                            "try to unbind all vip : %s success on nic: %s",
                            mgr_write_vip_addr,
                            system_bind_ips[mgr_write_vip_addr].c_str());
    } else {
      my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                            "try to unbind all vip : %s failed on nic: %s",
                            mgr_write_vip_addr,
                            system_bind_ips[mgr_write_vip_addr].c_str());
    }
  }
  for (auto it = read_vips.begin(); it != read_vips.end(); it++) {
    if (system_bind_ips.find(*it) != system_bind_ips.end()) {
      if (unbind_vip(it->c_str(), system_bind_ips[*it].c_str())) {
        my_plugin_log_message(&plugin_ptr, MY_INFORMATION_LEVEL,
                              "try to unbind all vip : %s success on nic: %s",
                              it->c_str(), system_bind_ips[*it].c_str());
      } else {
        my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                              "try to unbind all vip : %s failed on nic: %s",
                              it->c_str(), system_bind_ips[*it].c_str());
      }
    }
  }
}

static bool bind_vip(const char *vip, std::string &nic_label_name) {
  if (!vip || !vip_netmask || !vip_nic) return false;
  if (bind_ips_with_nicname.find(vip) != bind_ips_with_nicname.end())
    return true;
  if (nic_pos_list.empty()) {
    my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                          "Virtual Adapter list is empty when bind vip");
    return false;
  }

  std::string nic_name = get_nic_name(nic_pos_list.front());
#ifndef NDEBUG
  if (DBUG_EVALUATE_IF("test_vip", true, false)) {
    bind_ips_with_nicname[vip] = vip_family == AF_INET ? nic_name : vip_nic;
    nic_label_name = bind_ips_with_nicname[vip];
    nic_pos_list.pop();
    return true;
  }
#endif

  if (vip_family == AF_INET ? !bind_vip_ipv4(vip) : !bind_vip_ipv6(vip)) {
    return false;
  }

  bind_ips_with_nicname[vip] = vip_family == AF_INET ? nic_name : vip_nic;
  nic_label_name = bind_ips_with_nicname[vip];
  if (vip_family == AF_INET ? !send_arp(vip) : !send_na(vip)) {
    return false;
  }
  nic_pos_list.pop();
  return true;
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
    /*
       Only 2 node mgr could enter below code.
       The other node become unreachable, will wait force_wait_timeout_var then
       execute force action.
       if view change in wait period, there are only two possibilities, and
       neither should execute force action:
        1. Cur node stop group_replication or shutdown;
        2. The other node become reachable.
    */
    {
      std::unique_lock<std::mutex> lck(check_killconn_mu_);
      ulong need_wait_time = force_wait_timeout_var;
      time_t start_time = time(NULL);
      while (need_wait_time) {
        // wait_for() return no_timeout if Spurious wakeup
        if (check_killconn_cv_.wait_for(lck,
                                        std::chrono::seconds(need_wait_time)) ==
            std::cv_status::timeout) {
          break;
        }
        // mgr view has changed
        if (need_check_killall_connection_and_force_member)
          return nullptr;
        else {
          // Spurious wakeup, need continue sleep
          time_t wake_time = time(NULL);
          ulong sleepd_time = difftime(wake_time, start_time);
          need_wait_time =
              need_wait_time > sleepd_time ? need_wait_time - sleepd_time : 0;
        }
      }
    }
    // The result of the last ping is reliable (during the MIN_FLP_TIMEOUT +
    // force_wait_timeout_var period).
    time_t cur_time = time(NULL);
    if (last_ping_fail_time > 0 &&
        difftime(cur_time, last_ping_fail_time) <
            MIN_FLP_TIMEOUT + force_wait_timeout_var - 1) {
      // cur node's network status is not normal.
      char last_fail_time[SHOW_VAR_FUNC_BUFF_SIZE] = {0};
      get_last_ping_time_char(last_fail_time, /*bool is_succ = */ false);
      my_plugin_log_message(
          &plugin_ptr, MY_WARNING_LEVEL,
          "Ping gateway failed [%s], will stop group_replication.",
          last_fail_time);
      char *error_message = nullptr;
      if (group_replication_stop(&error_message)) {
        my_plugin_log_message(&plugin_ptr, MY_INFORMATION_LEVEL,
                              "stop group replication failed, case %s",
                              error_message);
        return nullptr;
      }
      is_stopped_by_ha = true;
    } else {
      // cur node's network status is normal.
      my_plugin_log_message(&plugin_ptr, MY_WARNING_LEVEL,
                            "Ping gateway success, will execute force member.");
      gdb_cmd_run_force_member();
    }
  }
  return nullptr;
}

void check_bind_vips() {
#ifndef NDEBUG
  if (DBUG_EVALUATE_IF("test_vip", true, false)) {
    return;
  }
#endif
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
    std::string nic_label_name;
    if (bind_vip(need_rebind_vips[i].c_str(), nic_label_name)) {
#ifndef NDEBUG
      if (DBUG_EVALUATE_IF("test_vip", true, false))
        my_plugin_log_message(&plugin_ptr, MY_INFORMATION_LEVEL,
                              "try to bind vip : %s success on nic: %s",
                              need_rebind_vips[i].c_str(),
                              nic_label_name.c_str());
      else
#endif
        my_plugin_log_message(&plugin_ptr, MY_INFORMATION_LEVEL,
                              "try to bind vip : %s success on nic: %s",
                              need_rebind_vips[i].c_str(),
                              nic_label_name.c_str());
    } else {
      my_plugin_log_message(
          &plugin_ptr, MY_ERROR_LEVEL, "try to bind vip : %s failed on nic: %s",
          need_rebind_vips[i].c_str(), nic_label_name.c_str());
    }
  }
}

static void bind_vip_according_map() {
  if (all_node_bind_vips.find(server_uuid) != all_node_bind_vips.end()) {
    std::set<std::string> need_bind_vips = all_node_bind_vips[server_uuid];
    // resize nic_pos_list
    int old_size = nic_pos_list.size() + bind_ips_with_nicname.size();
    for (size_t i = old_size; i < need_bind_vips.size() + 1; i++) {
      nic_pos_list.push(pos_to_label(i));
    }
    // bind vip
    for (auto it = need_bind_vips.begin(); it != need_bind_vips.end(); it++) {
      if (bind_ips_with_nicname.find(*it) == bind_ips_with_nicname.end()) {
        if ((*it) == "") continue;
        std::string nic_label_name;
        if (bind_vip((*it).c_str(), nic_label_name)) {
#ifndef NDEBUG
          if (DBUG_EVALUATE_IF("test_vip", true, false))
            my_plugin_log_message(&plugin_ptr, MY_INFORMATION_LEVEL,
                                  "try to bind vip : %s success on nic: %s",
                                  (*it).c_str(), nic_label_name.c_str());
          else
#endif
            my_plugin_log_message(&plugin_ptr, MY_INFORMATION_LEVEL,
                                  "try to bind vip : %s success on nic: %s",
                                  (*it).c_str(), nic_label_name.c_str());
        } else {
          my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                                "try to bind vip : %s failed on nic: %s",
                                (*it).c_str(), nic_label_name.c_str());
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
#ifndef NDEBUG
        if (DBUG_EVALUATE_IF("test_vip", true, false))
          my_plugin_log_message(&plugin_ptr, MY_INFORMATION_LEVEL,
                                "try to unbind vip : %s success on nic: %s",
                                it->first.c_str(), it->second.c_str());
        else
#endif
          my_plugin_log_message(&plugin_ptr, MY_INFORMATION_LEVEL,
                                "try to unbind vip : %s success on nic: %s",
                                it->first.c_str(), it->second.c_str());
      } else {
        my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                              "try to unbind vip : %s failed on nic: %s",
                              it->first.c_str(), it->second.c_str());
      }
    }
  }
  check_bind_vips();
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

/* Primary node HA plugin communication BEGIN */

static int send_and_validate_receive_content_head(
    const st_row_group_members &secondary_member_info,
    const std::string &send_message, std::string &receive_message) {
  std::string member_host_port =
      secondary_member_info.member_host +
      std::to_string(secondary_member_info.member_port);
  std::pair<bool, std::string> ret =
      SlaveConnManager::get_instance().send_message(secondary_member_info,
                                                    send_message);
  if (ret.first) {
    my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                          "try to connect or send message to %s failed when "
                          "get secondary bind vips message",
                          member_host_port.c_str());
    return 1;
  }
  // packet format check
  if (ret.second.empty()) {
    my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                          "Receive %s send unexcepted empty packet",
                          member_host_port.c_str());
    return 1;
  }

  if (HA_REPLY_YOU_ARE_NOT_PRIMARY == ret.second.at(0)) {
    my_plugin_log_message(
        &plugin_ptr, MY_ERROR_LEVEL,
        "Receive %s send YOU_ARE_NOT_PRIMARY, current view_id is %s:%u",
        member_host_port.c_str(), view_id_stamp.c_str(), view_id_version);
    return -1;
  } else if (HA_REPLY_OK != ret.second.at(0)) {
    my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                          "Receive %s send unexcepted packet: [%s]",
                          member_host_port.c_str(), ret.second.c_str());
    return 1;
  }
  // remove head
  receive_message = ret.second.substr(1);
  return 0;
}

static bool get_secondary_node_bind_ips() {
  std::string send_message = "select ";
  send_message.append(HA_GET_BIND_VIPS_FUNC_NAME)
      .append("('")
      .append(view_id_stamp)
      .append("', ")
      .append(std::to_string(view_id_version))
      .append(")");
  std::set<std::string> has_bind_vips = all_node_bind_vips[server_uuid];
  for (auto &it : secondary_members) {
    std::string receive_message;
    int validate_ret = send_and_validate_receive_content_head(it, send_message,
                                                              receive_message);
    if (-1 == validate_ret) {
      return true;
    } else if (1 == validate_ret) {
      // not send set packet
      it.is_invalid = true;
      continue;
    }

    std::set<std::string> bind_vips;
    std::vector<std::string> need_erase_vips;
    split_string_according_delimiter(receive_message.c_str(), bind_vips, ",");
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
    all_node_bind_vips[it.member_id] = bind_vips;
  }
  return false;
}

static bool send_secondary_bind_vips_message(bool is_update = false) {
  // generate bind vip message
  std::string message = gen_messages_according_nodes_relationship();
  if (message.size() >= MAX_ALL_VIP_TOPE_LENGTH) {
    my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                          "vip tope [%s] was too long, max length is :[%d].",
                          message.c_str(), MAX_ALL_VIP_TOPE_LENGTH);
    return true;
  }
  memset(all_vip_tope_value, 0, MAX_ALL_VIP_TOPE_LENGTH);
  memcpy(all_vip_tope_value, message.c_str(), message.size());
  if (message == "") {
    return false;
  }
  if (is_update) {
    my_plugin_log_message(&plugin_ptr, MY_INFORMATION_LEVEL,
                          "calculated new vip_tope:[%s].", all_vip_tope_value);
  }
  if (!secondary_members.size()) return false;
  std::string send_message = "select ";
  send_message.append(HA_SET_ALL_NODE_BIND_VIPS_FUNC_NAME)
      .append("('")
      .append(view_id_stamp)
      .append("', ")
      .append(std::to_string(view_id_version))
      .append(", '")
      .append(message)
      .append("')");
  // send message
  for (const auto &it : secondary_members) {
    if (it.is_invalid) continue;
    std::string receive_message;
    int validate_ret = send_and_validate_receive_content_head(it, send_message,
                                                              receive_message);
    if (-1 == validate_ret) {
      return true;
    } else if (1 == validate_ret) {
      continue;
    }
  }
  return false;
}

/* Primary node HA plugin communication END */

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

static void calculate_new_bind_relationship() {
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
          it->member_id);  // if a secondary member can not be connected, then
                           // not allocate vip for it
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
                                  int &view_id_version,
                                  bool &is_update_view_id) {
  struct st_row_group_member_stats tmp_row;
  // Set default values.
  tmp_row.channel_name_length = 0;
  tmp_row.trx_committed = nullptr;
  memset(tmp_row.view_id, 0, HOSTNAME_LENGTH);
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
  tmp_row.second_behind_group = 0;
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
      &set_second_behind_group,
  };
  // Query plugin and let callbacks do their job.
  bool ret = get_group_replication_group_member_stats_info(0, callbacks);
  if (tmp_row.trx_committed != nullptr) {
    my_free(tmp_row.trx_committed);
    tmp_row.trx_committed = nullptr;
  }
  if (ret || tmp_row.view_id_length == 0) {
    return false;
  }
  std::string str_view_id = tmp_row.view_id;
  std::size_t view_id_position = str_view_id.find(":");
  if (view_id_position > 0) {
    std::string tmp_view_id_stamp = str_view_id.substr(0, view_id_position);
    int tmp_view_id_version = std::stoi(
        str_view_id.substr(view_id_position + 1, tmp_row.view_id_length));
    if (view_id_stamp != tmp_view_id_stamp ||
        view_id_version != tmp_view_id_version) {
      view_id_stamp = tmp_view_id_stamp;
      view_id_version = tmp_view_id_version;
      is_update_view_id = true;
    }
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

static void *refresh_cluster_info_alloc_new_vip_tope() {
  if (!is_group_replication_running()) {
    broadcast_thread_need_wait_next_alloc = true;
    unbind_vips(bind_ips_with_nicname);
    SlaveConnManager::get_instance().clear_mgr_recovery_user();
    return nullptr;
  }
  pthread_mutex_lock(&primary_broadcast_tope_mu_);
  pthread_mutex_lock(&vip_variable_mutex);
  if (!enable_vip) {
    pthread_mutex_unlock(&vip_variable_mutex);
    pthread_mutex_unlock(&primary_broadcast_tope_mu_);
    return nullptr;
  }
  if (!mgr_write_vip_addr) {
    my_plugin_log_message(
        &plugin_ptr, MY_ERROR_LEVEL,
        "Error:greatdb_ha_mgr_vip_ip is not defined, please define it");
    pthread_mutex_unlock(&vip_variable_mutex);
    pthread_mutex_unlock(&primary_broadcast_tope_mu_);
    return nullptr;
  }
  if (!update_vip_family()) {
    pthread_mutex_unlock(&vip_variable_mutex);
    pthread_mutex_unlock(&primary_broadcast_tope_mu_);
    return nullptr;
  }
  broadcast_thread_need_wait_next_alloc = true;
  SlaveConnManager::get_instance().clear_slave_conn_map();
  bool is_master = false;
  bool need_unbind_all_vips = false;
  bool master_is_running = true;
  struct st_row_group_members m_row;

  // refresh mgr node info
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
               m_row.member_state == st_row_group_members::State::MGR_ONLINE) {
      /*
        The new node join MGR group will get vip after status from RECOVERING
        change to ONLINE
      */
      secondary_members.push_back(m_row);
    } else if (m_row.member_role ==
                   st_row_group_members::Role::ROLE_ARBITRATOR &&
               !strcasecmp(m_row.member_id.c_str(), server_uuid)) {
      my_plugin_log_message(
          &plugin_ptr, MY_WARNING_LEVEL,
          "HA plugin is disabled for ARBITRATOR node, is being turned off.");
      pthread_mutex_unlock(&vip_variable_mutex);
      pthread_mutex_unlock(&primary_broadcast_tope_mu_);
      all_thread_need_exit = true;
      return nullptr;
    }
  }
  if (need_unbind_all_vips) {
    unbind_all_vips();
    pthread_mutex_unlock(&vip_variable_mutex);
    pthread_mutex_unlock(&primary_broadcast_tope_mu_);
    return nullptr;
  }
  if (work_number <= (n - recover_number) / 2) {
    master_is_running = false;
  } else {
    bool is_update_view_id = false;
    if (!get_cur_group_view_id(view_id_stamp, view_id_version,
                               is_update_view_id)) {
      my_plugin_log_message(
          &plugin_ptr, MY_INFORMATION_LEVEL,
          "Cannot get MGR group view_id info, maybe need wait "
          "MGR complete initialization.");
    } else if (is_update_view_id) {
      my_plugin_log_message(&plugin_ptr, MY_INFORMATION_LEVEL,
                            "Cur MGR group view_id change to [%s:%d].",
                            view_id_stamp.c_str(), view_id_version);
    }
  }
  // calculate new vip  tope
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
    } else if (get_secondary_node_bind_ips()) {
      pthread_mutex_unlock(&vip_variable_mutex);
      pthread_mutex_unlock(&primary_broadcast_tope_mu_);
      return nullptr;
    }
    all_node_bind_vips[server_uuid].insert(mgr_write_vip_addr);
    if (secondary_members.size()) {
      calculate_new_bind_relationship();
      bind_vip_according_map();
      if (send_secondary_bind_vips_message(true)) {
        pthread_mutex_unlock(&vip_variable_mutex);
        pthread_mutex_unlock(&primary_broadcast_tope_mu_);
        return nullptr;
      }
    } else {
      bind_vip_according_map();
    }
    broadcast_thread_need_wait_next_alloc = false;
    pthread_cond_signal(&primary_broadcast_tope_cv_);
  }
  pthread_mutex_unlock(&vip_variable_mutex);
  pthread_mutex_unlock(&primary_broadcast_tope_mu_);
  return nullptr;
}

void notify_group_replication_view() {
  {
    std::unique_lock<std::mutex> lck(check_killconn_mu_);
    need_check_killall_connection_and_force_member = true;
    check_killconn_cv_.notify_one();
  }
  pthread_mutex_lock(&alloc_new_vip_tope_mu_);
  need_check_bind_vip = true;
  pthread_cond_signal(&alloc_new_vip_tope_cv_);
  pthread_mutex_unlock(&alloc_new_vip_tope_mu_);
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

static void *alloc_new_vip_tope_func(void *) {
  my_thread_init();
  while (1) {
    pthread_mutex_lock(&alloc_new_vip_tope_mu_);
    if (all_thread_need_exit) {
      pthread_mutex_unlock(&alloc_new_vip_tope_mu_);
      break;
    }
    if (!need_check_bind_vip) {
      pthread_cond_wait(&alloc_new_vip_tope_cv_, &alloc_new_vip_tope_mu_);
    }
    if (all_thread_need_exit) {
      pthread_mutex_unlock(&alloc_new_vip_tope_mu_);
      break;
    }
    need_check_bind_vip = false;
    pthread_mutex_unlock(&alloc_new_vip_tope_mu_);
    refresh_cluster_info_alloc_new_vip_tope();
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
    {
      std::unique_lock<std::mutex> lck(check_killconn_mu_);
      if (all_thread_need_exit) break;
      if (!need_check_killall_connection_and_force_member) {
        check_killconn_cv_.wait(lck);
      }
      if (all_thread_need_exit) break;
      need_check_killall_connection_and_force_member = false;
    }
    check_kill_connection_and_force_member();
  }
  delete thd;
  my_thread_end();
  return nullptr;
}

/*
  Primary node broadcast vip tope every 20 seconds
  Secondart node receive message will bind vip (or broadcast arp/na packet if
  already bind)
*/
static void *greatdb_ha_primary_broadcast_tope_func(void *) {
  my_thread_init();
  while (true) {
    pthread_mutex_lock(&primary_broadcast_tope_mu_);
    if (all_thread_need_exit) {
      pthread_mutex_unlock(&primary_broadcast_tope_mu_);
      break;
    }
    if (broadcast_thread_need_wait_next_alloc) {
      pthread_cond_wait(&primary_broadcast_tope_cv_,
                        &primary_broadcast_tope_mu_);
    }
    if (all_thread_need_exit) {
      pthread_mutex_unlock(&primary_broadcast_tope_mu_);
      break;
    }
    pthread_mutex_lock(&vip_variable_mutex);
    bind_vip_according_map();
    if (send_secondary_bind_vips_message()) {
      // wait for next refresh_cluster_info_alloc_new_vip_tope()
      broadcast_thread_need_wait_next_alloc = true;
    }
    pthread_mutex_unlock(&vip_variable_mutex);
    pthread_mutex_unlock(&primary_broadcast_tope_mu_);
    sleep(20);
  }
  my_thread_end();
  return nullptr;
}

/* Secondary node HA plugin UDF functions BEGIN */

// arg_format :view_stamp:view_version
static bool validate_arg_view_id(const std::string &arg_view_id_stamp,
                                 int arg_view_id_version) {
  // only support 0-9 under 64 bits
  std::regex pattern("^\\d{1,64}$");
  if (!std::regex_match(arg_view_id_stamp, pattern)) {
    my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                          "skip error format arg_view_id: %s",
                          arg_view_id_stamp.c_str());
    throw std::invalid_argument("Error format args[0]");
  }

  if (arg_view_id_stamp == view_id_stamp &&
      arg_view_id_version < view_id_version) {
    my_plugin_log_message(
        &plugin_ptr, MY_ERROR_LEVEL,
        "Receive packet`s view_id[%s:%d] older than cur view_id[%s:%d], will "
        "send YOU_ARE_NOT_PRIMARY packet.",
        arg_view_id_stamp.c_str(), arg_view_id_version, view_id_stamp.c_str(),
        view_id_version);
    return true;
  }

  return false;
}

char *udf_get_bind_vips(UDF_INIT *initid, UDF_ARGS *args, char *,
                        unsigned long *length, char *is_null, char *error) {
  assert(args && length && is_null && error);
  std::string ret;
  try {
    pthread_mutex_lock(&vip_variable_mutex);
    std::string arg_view_id_stamp(args->args[0]);
    int arg_view_id_version = *(int *)args->args[1];
    if (validate_arg_view_id(arg_view_id_stamp, arg_view_id_version)) {
      ret.insert(ret.begin(), HA_REPLY_YOU_ARE_NOT_PRIMARY);
    } else {
      ret.insert(ret.begin(), HA_REPLY_OK);
      for (auto it = bind_ips_with_nicname.begin();
           it != bind_ips_with_nicname.end(); it++) {
        ret.append(it->first.c_str());
        ret.append(",");
      }
      // 1 is head
      if (ret.length() > 1) ret.pop_back();
    }
    pthread_mutex_unlock(&vip_variable_mutex);
  } catch (const std::exception &e) {
    *error = 1;
    *is_null = 1;
    pthread_mutex_unlock(&vip_variable_mutex);
    my_error(ER_WRONG_ARGUMENTS, MYF(0), e.what());
    return nullptr;
  }
  *error = 0;
  size_t return_length = ret.length();
  if (return_length > 0) {
    *is_null = 0;
    if (initid->ptr) delete[] initid->ptr;
    initid->ptr = new char[return_length + 1];
    strncpy(initid->ptr, ret.c_str(), return_length);
    initid->ptr[return_length] = '\0';
  }
  *length = return_length;
  return initid->ptr;
}

static bool validate_and_get_all_node_ips(
    const char *vip_tope_value,
    std::map<std::string, std::set<std::string>> &uuid_vip_map) {
  // the format is "uuid1::vip1;uuid2::vip2,vip3;uuid3::vip4"
  if (!vip_tope_value) return true;
  // only support 0-9 a-z A-Z . ; , : - and not empty
  std::regex pattern("^[0-9a-zA-Z.;,:-]+$");
  if (!std::regex_match(vip_tope_value, pattern)) {
    my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                          "skip error format arg_vip_tope: %s", vip_tope_value);
    return true;
  }
  std::set<std::string> result;
  std::map<std::string, std::set<std::string>> tmp_uuid_vip_map;
  split_string_according_delimiter(vip_tope_value, result, ";");
  for (const auto &it : result) {
    std::string::size_type pos = it.find("::");
    if (std::string::npos == pos) return true;
    // validate_uuid
    std::string node_uuid = it.substr(0, pos);
    if (validate_uuid(node_uuid)) return true;
    // validate_vips
    std::string node_vips = it.substr(pos + 2);
    std::set<std::string> need_bind_ips;
    split_string_according_delimiter(node_vips.c_str(), need_bind_ips, ",");
    if (validate_vips(need_bind_ips)) return true;
    tmp_uuid_vip_map[node_uuid] = need_bind_ips;
  }
  uuid_vip_map.swap(tmp_uuid_vip_map);
  return false;
}

char *udf_set_all_node_bind_vips(UDF_INIT *initid, UDF_ARGS *args, char *,
                                 unsigned long *length, char *is_null,
                                 char *error) {
  assert(args && length && is_null && error);
  std::string ret;
  try {
    pthread_mutex_lock(&vip_variable_mutex);
    std::string arg_view_id_stamp(args->args[0]);
    int arg_view_id_version = *(int *)args->args[1];
    std::string arg_vip_tope(args->args[2]);
    if (validate_arg_view_id(arg_view_id_stamp, arg_view_id_version)) {
      ret.insert(ret.begin(), HA_REPLY_YOU_ARE_NOT_PRIMARY);
    } else if (validate_and_get_all_node_ips(arg_vip_tope.c_str(),
                                             all_node_bind_vips)) {
      throw std::invalid_argument("Error format args[2]");
    } else {
      ret.insert(ret.begin(), HA_REPLY_OK);
      if (arg_vip_tope.size() >= MAX_ALL_VIP_TOPE_LENGTH) {
        my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                              "args[2] [%s] was too long",
                              arg_vip_tope.c_str());
        throw std::invalid_argument("args[2] was too long");
      }
      // change tope value by point
      memset(all_vip_tope_value, 0, MAX_ALL_VIP_TOPE_LENGTH);
      memcpy(all_vip_tope_value, arg_vip_tope.c_str(), arg_vip_tope.length());
      bind_vip_according_map();
    }
    pthread_mutex_unlock(&vip_variable_mutex);
  } catch (const std::exception &e) {
    *error = 1;
    *is_null = 1;
    pthread_mutex_unlock(&vip_variable_mutex);
    my_error(ER_WRONG_ARGUMENTS, MYF(0), e.what());
    return nullptr;
  }

  *error = 0;
  size_t return_length = ret.length();
  if (return_length > 0) {
    *is_null = 0;
    initid->ptr = new char[return_length + 1];
    strncpy(initid->ptr, ret.c_str(), return_length);
    initid->ptr[return_length] = '\0';
  }
  *length = return_length;
  return initid->ptr;
}

/* Secondary node HA plugin UDF functions END */

static void process_read_vip_ips(const char *read_vip_ips) {
  if (!read_vip_ips) return;
  std::set<std::string> vips;
  split_string_according_delimiter(read_vip_ips, vips, ",");
  read_vips.clear();
  read_vips = vips;
}

static int greatdb_ha_plugin_init(MYSQL_PLUGIN plugin_info) {
  DBUG_TRACE;
  all_thread_need_exit = false;
  broadcast_thread_need_wait_next_alloc = true;
  my_thread_attr_t attr; /* Thread attributes */

  /*
    No threads exist at this point in time, so this is thread safe.
  */
  is_register_services = false;
  plugin_ptr = plugin_info;
  register_udfs();
  greatdb::all_vip_tope = greatdb::all_vip_tope_value;
  greatdb::nic_pos_list.push('0');
  process_read_vip_ips(greatdb::mgr_read_vip_addrs);
  my_thread_attr_init(&attr);
  my_thread_attr_setdetachstate(&attr, MY_THREAD_CREATE_JOINABLE);
  update_vip_family();
  last_ping_succ_time = 0;
  last_ping_fail_time = 0;
  if (my_thread_create(&ping_thread, &attr, ping_func, nullptr) != 0) {
    my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                          "Error: Could not create ping gateway thread!");
    all_thread_need_exit = true;
    return 0;
  }

  /* now create the thread */
  if (my_thread_create(&alloc_new_vip_tope_thread, &attr,
                       alloc_new_vip_tope_func, nullptr) != 0) {
    my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                          "Error: Could not create new vip alloc thread!");
    all_thread_need_exit = true;
    return 0;
  }
  if (my_thread_create(&check_killconnection_thread_and_force_member, &attr,
                       greatdb_ha_check_killconnection_and_force_member_func,
                       nullptr) != 0) {
    my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                          "Error: Could not create check killall connection "
                          "and force member thread!");
    all_thread_need_exit = true;
    return 0;
  }
  if (my_thread_create(&primary_broadcast_tope_thread, &attr,
                       greatdb_ha_primary_broadcast_tope_func, nullptr) != 0) {
    my_plugin_log_message(
        &plugin_ptr, MY_ERROR_LEVEL,
        "Error: Could not create primary broadcast tope thread!");
    all_thread_need_exit = true;
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
  all_thread_need_exit = true;
  SlaveConnManager::get_instance().clear_slave_conn_map();
  SlaveConnManager::get_instance().clear_mgr_recovery_user();
  unregister_udfs();
  pthread_mutex_lock(&alloc_new_vip_tope_mu_);
  pthread_cond_signal(&alloc_new_vip_tope_cv_);
  pthread_mutex_unlock(&alloc_new_vip_tope_mu_);
  if (alloc_new_vip_tope_thread.thread != 0) {
    my_thread_join(&alloc_new_vip_tope_thread, nullptr);
    alloc_new_vip_tope_thread.thread = 0;
  }
  {
    std::unique_lock<std::mutex> lck(check_killconn_mu_);
    check_killconn_cv_.notify_one();
  }
  if (check_killconnection_thread_and_force_member.thread != 0) {
    my_thread_join(&check_killconnection_thread_and_force_member, nullptr);
    check_killconnection_thread_and_force_member.thread = 0;
  }
  pthread_mutex_lock(&greatdb::ping_mutex);
  pthread_cond_signal(&greatdb::ping_cv);
  pthread_mutex_unlock(&greatdb::ping_mutex);
  if (ping_thread.thread != 0) {
    my_thread_join(&ping_thread, nullptr);
    ping_thread.thread = 0;
  }
  pthread_mutex_lock(&primary_broadcast_tope_mu_);
  pthread_cond_signal(&primary_broadcast_tope_cv_);
  pthread_mutex_unlock(&primary_broadcast_tope_mu_);
  if (primary_broadcast_tope_thread.thread != 0) {
    my_thread_join(&primary_broadcast_tope_thread, nullptr);
    primary_broadcast_tope_thread.thread = 0;
  }
  if (is_register_services) {
    unregister_services();
    is_register_services = false;
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
  greatdb::refresh_cluster_info_alloc_new_vip_tope();
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
  // The maximum number of MGR nodes is 61
  if (vips.size() > 61) {
    my_message(ER_WRONG_VALUE_FOR_VAR,
               "read vip members was too many, the maximum number is 61",
               MYF(0));
    pthread_mutex_unlock(&greatdb::vip_variable_mutex);
    return 1;
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
  greatdb::refresh_cluster_info_alloc_new_vip_tope();
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

static void mgr_vip_label_update(MYSQL_THD thd MY_ATTRIBUTE((unused)),
                                 SYS_VAR *var MY_ATTRIBUTE((unused)),
                                 void *var_ptr, const void *save) {
  pthread_mutex_lock(&greatdb::vip_variable_mutex);
  greatdb::unbind_vips(greatdb::bind_ips_with_nicname);
  const char *new_val = *(static_cast<const char **>(const_cast<void *>(save)));
  if (var_ptr != nullptr) {
    *((const char **)var_ptr) = new_val;
  }
  pthread_mutex_unlock(&greatdb::vip_variable_mutex);
  greatdb::refresh_cluster_info_alloc_new_vip_tope();
}

static int mgr_vip_label_check(MYSQL_THD thd, SYS_VAR *, void *save,
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
  if (length > 1) {
    my_message(ER_WRONG_VALUE_FOR_VAR, "vip label format is not one character",
               MYF(0));
    pthread_mutex_unlock(&greatdb::vip_variable_mutex);
    return 1;
  } else if (length == 1) {
    char label = str[0];
    if (!((label >= '0' && label <= '9') || (label >= 'a' && label <= 'z') ||
          (label >= 'A' && label <= 'Z'))) {
      my_message(ER_WRONG_VALUE_FOR_VAR, "vip label format is not 0-9a-zA-Z",
                 MYF(0));
      pthread_mutex_unlock(&greatdb::vip_variable_mutex);
      return 1;
    }
  }
  *(const char **)save = str;
  pthread_mutex_unlock(&greatdb::vip_variable_mutex);
  return 0;
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
  if (greatdb::validate_and_get_all_node_ips(str, new_val_map) ||
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
  const char *new_val = *(static_cast<const char **>(const_cast<void *>(save)));
  if (strlen(new_val) >= MAX_ALL_VIP_TOPE_LENGTH) {
    std::string tmp_msg = "vip tope was too long, max length is ";
    tmp_msg.append(std::to_string(MAX_ALL_VIP_TOPE_LENGTH)).append(" bytes.");
    my_message(ER_WRONG_VALUE_FOR_VAR, tmp_msg.c_str(), MYF(0));
    return;
  }
  pthread_mutex_lock(&greatdb::primary_broadcast_tope_mu_);
  pthread_mutex_lock(&greatdb::vip_variable_mutex);
  if (var_ptr != nullptr) {
    memset(greatdb::all_vip_tope_value, 0, MAX_ALL_VIP_TOPE_LENGTH);
    memcpy(greatdb::all_vip_tope_value, new_val, strlen(new_val));
    greatdb::validate_and_get_all_node_ips(new_val,
                                           greatdb::all_node_bind_vips);
    greatdb::bind_vip_according_map();
    greatdb::send_secondary_bind_vips_message();
  }
  pthread_mutex_unlock(&greatdb::vip_variable_mutex);
  pthread_mutex_unlock(&greatdb::primary_broadcast_tope_mu_);
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
  greatdb::refresh_cluster_info_alloc_new_vip_tope();
}

static int mgr_vip_nic_check(MYSQL_THD thd, SYS_VAR *, void *save,
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
  /* linux nic name must be less than or equal to 15, vip nic need other 3
   * characters*/
  if (length > 12) {
    my_message(ER_WRONG_VALUE_FOR_VAR,
               "nic name was too long, max length is 12 bytes.", MYF(0));
    pthread_mutex_unlock(&greatdb::vip_variable_mutex);
    return 1;
  }
  *(const char **)save = str;
  pthread_mutex_unlock(&greatdb::vip_variable_mutex);
  return 0;
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
  greatdb::refresh_cluster_info_alloc_new_vip_tope();
}

static void force_change_mgr_vip_enabled_update(
    MYSQL_THD thd MY_ATTRIBUTE((unused)), SYS_VAR *var MY_ATTRIBUTE((unused)),
    void *var_ptr, const void *save) {
  const bool set_val = *static_cast<const bool *>(save);
  if (set_val) {
    greatdb::refresh_cluster_info_alloc_new_vip_tope();
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
    greatdb::refresh_cluster_info_alloc_new_vip_tope();
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
    "name of the network card", mgr_vip_nic_check, /* check func*/
    mgr_vip_nic_update,                            /* update func*/
    nullptr);                                      /* default*/

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

static MYSQL_SYSVAR_STR(mgr_vip_label,              /* name */
                        greatdb::mgr_vip_label_var, /* var */
                        PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_MEMALLOC,
                        /* optional var | malloc string*/
                        "mgr vip label.", mgr_vip_label_check, /* check func*/
                        mgr_vip_label_update,                  /* update func*/
                        nullptr);                              /* default*/

static MYSQL_SYSVAR_ULONG(
    force_wait_timeout,                                    /* name */
    greatdb::force_wait_timeout_var,                       /* var */
    PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_PERSIST_AS_READ_ONLY, /* optional var */
    "The number of times to broad arp packge after bind vip.",
    nullptr, /* check func. */
    nullptr, /* update func. */
    5,       /* default */
    3,       /* min */
    3600,    /* max */
    0        /* block */
);

static SYS_VAR *greatdb_ha_system_vars[] = {
    MYSQL_SYSVAR(mgr_vip_ip),
    MYSQL_SYSVAR(mgr_read_vip_ips),
    MYSQL_SYSVAR(mgr_vip_nic),
    MYSQL_SYSVAR(vip_tope),
    MYSQL_SYSVAR(enable_mgr_vip),
    MYSQL_SYSVAR(mgr_exit_primary_kill_connection_mode),
    MYSQL_SYSVAR(gateway_address),
    MYSQL_SYSVAR(mgr_vip_mask),
    MYSQL_SYSVAR(force_change_mgr_vip),
    MYSQL_SYSVAR(send_arp_packge_times),
    MYSQL_SYSVAR(mgr_read_vip_floating_type),
    MYSQL_SYSVAR(mgr_vip_label),
    MYSQL_SYSVAR(force_wait_timeout),
    nullptr,
};

static int get_last_ping_succ_time(THD *, SHOW_VAR *var, char *value_buf) {
  var->type = SHOW_CHAR;
  char local_time_buf[SHOW_VAR_FUNC_BUFF_SIZE] = {0};
  greatdb::get_last_ping_time_char(local_time_buf);
  strncpy(value_buf, local_time_buf, SHOW_VAR_FUNC_BUFF_SIZE);
  value_buf[SHOW_VAR_FUNC_BUFF_SIZE - 1] = 0;
  var->value = value_buf;
  return 0;
}

static int get_last_ping_fail_time(THD *, SHOW_VAR *var, char *value_buf) {
  var->type = SHOW_CHAR;
  char local_time_buf[SHOW_VAR_FUNC_BUFF_SIZE] = {0};
  greatdb::get_last_ping_time_char(local_time_buf, /*bool is_succ = */ false);
  strncpy(value_buf, local_time_buf, SHOW_VAR_FUNC_BUFF_SIZE);
  value_buf[SHOW_VAR_FUNC_BUFF_SIZE - 1] = 0;
  var->value = value_buf;
  return 0;
}

static SHOW_VAR greatdb_ha_status_vars[] = {
    {"greatdb_ha_last_ping_succ_time", (char *)&get_last_ping_succ_time,
     SHOW_FUNC, SHOW_SCOPE_GLOBAL},
    {"greatdb_ha_last_ping_fail_time", (char *)&get_last_ping_fail_time,
     SHOW_FUNC, SHOW_SCOPE_GLOBAL},
    {nullptr, nullptr, SHOW_UNDEF, SHOW_SCOPE_UNDEF}  // end
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
    greatdb_ha_status_vars, /* status variables                */
    greatdb_ha_system_vars, /* system variables                */
    nullptr,                /* config options                  */
    0,                      /* flags                           */
} mysql_declare_plugin_end;
