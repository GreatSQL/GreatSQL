/* Copyright (c) 2023, GreatDB Software Co., Ltd.

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
#include <net/ethernet.h>
#include <net/if.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>
#include <atomic>

#include <queue>
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
#include "mysql/components/service.h"
#include "mysql/components/service_implementation.h"  // for DEFINE_BOOL_METHOD
#include "mysql/components/services/group_member_status_listener.h"
#include "mysql/components/services/group_membership_listener.h"
#include "mysql/components/services/log_builtins.h"
#include "mysql/plugin.h"
#include "mysql/plugin_group_replication.h"  // for GROUP_REPLICATI...
#include "mysql/plugin_group_replication.h"
#include "mysql/psi/mysql_memory.h"
#include "mysql_com.h"  // for NAME_LEN
#include "sql/mysqld.h"
#include "sql/replication.h"
#include "sql/rpl_group_replication.h"  // for get_group_repli...
#include "sql/sql_const.h"

namespace greatdb {

#define HEART_STRING_BUFFER 200
#define DEST_MAC \
  { 0xFF, 0xff, 0xff, 0xff, 0xff, 0xff }

char *mgr_vip_addr;
char *vip_nic;
char *vip_broadip;
char *vip_netmask;
bool enable_vip;
bool force_bind_vip;
bool is_bind_vip = false;
bool need_exit = false;
std::queue<int> tasks;
my_thread_handle heartbeat_thread;
pthread_mutex_t mu_;
pthread_cond_t heartbeat_cv_;
static MYSQL_PLUGIN plugin_ptr;

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
    ROLE_SECONDLY,
  };
  std::string member_id;
  std::string member_host;
  unsigned int member_port;
  State member_state;
  Role member_role;
};

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

struct sockaddr_ll {
  unsigned short int sll_family;
  unsigned short int sll_protocol;
  int sll_ifindex;
  unsigned short int sll_hatype;
  unsigned char sll_pkttype;
  unsigned char sll_halen;
  unsigned char sll_addr[8];
};

int get_mac(const char *eno, unsigned char *mac) {
  struct ifreq ifreq;
  int sock;
  if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
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
    row->member_role = st_row_group_members::Role::ROLE_SECONDLY;
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

static int send_arp() {
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

  inet_aton(mgr_vip_addr, &s);
  memcpy(&arp.ar_sip, &s, sizeof(s));
  inet_aton(vip_broadip, &r);
  memcpy(&arp.ar_tip, &r, sizeof(r));

  sl.sll_family = AF_PACKET;
  sl.sll_ifindex = IFF_BROADCAST;

  if (sendto(sock_fd, &arp, sizeof(arp), 0, (struct sockaddr *)&sl,
             sizeof(sl)) <= 0)
    return 0;
  close(sock_fd);
  return 1;
}

static bool bind_vip() {
  if (is_bind_vip) return true;
  if (!mgr_vip_addr || !vip_netmask || !vip_nic) return false;
  struct sockaddr_in inet_addr;
  struct sockaddr_in mask_addr;
  int fd = 0;
  if ((fd = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
    return false;
  }
  inet_addr.sin_family = AF_INET;
  inet_pton(AF_INET, mgr_vip_addr, &(inet_addr.sin_addr));
  mask_addr.sin_family = AF_INET;
  inet_pton(AF_INET, vip_netmask, &(mask_addr.sin_addr));
  struct ifreq ifr;
  size_t nic_len =
      strlen(vip_nic) > (IFNAMSIZ - 3) ? (IFNAMSIZ - 3) : strlen(vip_nic);
  memcpy(ifr.ifr_ifrn.ifrn_name, vip_nic, nic_len);
  memcpy(ifr.ifr_ifrn.ifrn_name + nic_len, ":0",
         3);  // add :0 for nic name
  memcpy(&ifr.ifr_addr, &inet_addr, sizeof(struct sockaddr));
  if (ioctl(fd, SIOCSIFADDR, &ifr) < 0) {
    perror("SIOCSIFADDR");
    close(fd);
    return false;
  }
  memcpy(&ifr.ifr_addr, &mask_addr, sizeof(struct sockaddr));
  if (ioctl(fd, SIOCSIFNETMASK, &ifr) < 0) {
    perror("SIOCSIFNETMASK");
    close(fd);
    return false;
  }
  is_bind_vip = true;
  if (!send_arp()) {
    return false;
  }
  return true;
}

static bool unbind_vip() {
  if (!mgr_vip_addr || !vip_netmask || !vip_nic) return false;
  struct sockaddr_in inet_addr;
  int fd = 0;
  if ((fd = socket(AF_INET, SOCK_DGRAM, 0)) < 0) {
    return false;
  }
  inet_addr.sin_family = AF_INET;
  inet_pton(AF_INET, mgr_vip_addr, &(inet_addr.sin_addr));
  struct ifreq ifr;
  size_t nic_len =
      strlen(vip_nic) > (IFNAMSIZ - 3) ? (IFNAMSIZ - 3) : strlen(vip_nic);
  memcpy(ifr.ifr_ifrn.ifrn_name, vip_nic, nic_len);
  memcpy(ifr.ifr_ifrn.ifrn_name + nic_len, ":0",
         3);  // add :0 for nic name
  memcpy(&ifr.ifr_addr, &inet_addr, sizeof(struct sockaddr));
  if (ioctl(fd, SIOCSIFADDR, &ifr) < 0) {
    perror("SIOCSIFADDR");
    close(fd);
    return false;
  }
  if (ioctl(fd, SIOCGIFFLAGS, &ifr) < 0) {
    perror("SIOCSIFFLAGS");
    close(fd);
    return false;
  }
  ifr.ifr_flags &= ~IFF_UP;
  if (ioctl(fd, SIOCSIFFLAGS, &ifr) < 0) {
    perror("SIOCSIFFLAGS");
    close(fd);
    return false;
  }
  is_bind_vip = false;
  return true;
}

static void *mysql_heartbeat() {
  if (!is_group_replication_running()) {
    return nullptr;
  }
  if (!enable_vip) return nullptr;
  DBUG_TRACE;
  bool is_master = false;
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
  unsigned int n = get_group_replication_members_number_info();
  for (unsigned int i = 0; i < n; i++) {
    if (get_group_replication_group_members_info(i, callbacks)) {
      break;
    }
    if (m_row.member_state == st_row_group_members::State::MGR_ONLINE)
      work_number += 1;
    else if (m_row.member_state == st_row_group_members::State::MGR_RECOVERING)
      recover_number += 1;
    if (m_row.member_role == st_row_group_members::Role::ROLE_PRIMARY &&
        !strcasecmp(m_row.member_id.c_str(), server_uuid)) {
      is_master = true;
    }
  }
  if (work_number <= (n - recover_number) / 2) {
    master_is_running = false;
  }
  if (is_master && master_is_running) {
    if (!bind_vip()) {
      my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                            "Error: try to bind vip failed");
    } else {
      my_plugin_log_message(&plugin_ptr, MY_WARNING_LEVEL,
                            "try to bind vip success");
    }
  } else {
    if (!unbind_vip()) {
      my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                            "Error: try to unbind vip failed");
    } else {
      my_plugin_log_message(&plugin_ptr, MY_WARNING_LEVEL,
                            "try to unbind vip success");
    }
  }
  return nullptr;
}

static void notify_group_replication_view() {
  pthread_mutex_lock(&mu_);
  tasks.push(1);
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
  while (1) {
    pthread_mutex_lock(&mu_);
    if (need_exit) {
      pthread_mutex_unlock(&mu_);
      break;
    }
    while (tasks.empty()) {
      pthread_cond_wait(&heartbeat_cv_, &mu_);
      if (need_exit) {
        pthread_mutex_unlock(&mu_);
        break;
      }
    }
    while (!tasks.empty()) tasks.pop();
    pthread_mutex_unlock(&mu_);
    mysql_heartbeat();
  }
  return nullptr;
}

static int greatdb_ha_plugin_init(MYSQL_PLUGIN plugin_info) {
  DBUG_TRACE;
  // if (init_logging_service_for_plugin(&reg_srv, &log_bi, &log_bs)) return -1;
  my_thread_attr_t attr; /* Thread attributes */

  /*
    No threads exist at this point in time, so this is thread safe.
  */
  plugin_ptr = plugin_info;
  my_thread_attr_init(&attr);
  my_thread_attr_setdetachstate(&attr, MY_THREAD_CREATE_JOINABLE);

  /* now create the thread */
  if (my_thread_create(&heartbeat_thread, &attr, greatdb_ha_func, nullptr) !=
      0) {
    fprintf(stderr, "Could not create heartbeat thread!\n");
    return 0;
  }
  unbind_vip();
  register_services();

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
  unbind_vip();
  // deinit_logging_service_for_plugin(&reg_srv, &log_bi, &log_bs);

  /*
    Need to wait for the hearbeat thread to terminate before closing
    the file it writes to and freeing the memory it uses
  */
  need_exit = true;
  pthread_mutex_lock(&mu_);
  pthread_cond_signal(&heartbeat_cv_);
  pthread_mutex_unlock(&mu_);
  my_thread_join(&heartbeat_thread, nullptr);
  unregister_services();
  return 0;
}

}  // namespace greatdb

struct st_mysql_daemon greatdb_ha_plugin = {MYSQL_DAEMON_INTERFACE_VERSION};

static void mgr_vip_addr_update(MYSQL_THD thd MY_ATTRIBUTE((unused)),
                                SYS_VAR *var MY_ATTRIBUTE((unused)),
                                void *var_ptr, const void *save) {
  greatdb::is_bind_vip = false;
  greatdb::unbind_vip();
  const char *new_val = *(static_cast<const char **>(const_cast<void *>(save)));
  if (var_ptr != nullptr) {
    *((const char **)var_ptr) = new_val;
  }
  greatdb::mysql_heartbeat();
}

static void mgr_vip_nic_update(MYSQL_THD thd MY_ATTRIBUTE((unused)),
                               SYS_VAR *var MY_ATTRIBUTE((unused)),
                               void *var_ptr, const void *save) {
  greatdb::is_bind_vip = false;
  greatdb::unbind_vip();
  const char *new_val = *(static_cast<const char **>(const_cast<void *>(save)));
  if (var_ptr != nullptr) {
    *((const char **)var_ptr) = new_val;
  }
  greatdb::mysql_heartbeat();
}

static void mgr_vip_mask_update(MYSQL_THD thd MY_ATTRIBUTE((unused)),
                                SYS_VAR *var MY_ATTRIBUTE((unused)),
                                void *var_ptr, const void *save) {
  greatdb::is_bind_vip = false;
  greatdb::unbind_vip();
  const char *new_val = *(static_cast<const char **>(const_cast<void *>(save)));
  if (var_ptr != nullptr) {
    *((const char **)var_ptr) = new_val;
  }
  greatdb::mysql_heartbeat();
}

static void force_change_mgr_vip_enabled_update(
    MYSQL_THD thd MY_ATTRIBUTE((unused)), SYS_VAR *var MY_ATTRIBUTE((unused)),
    void *var_ptr, const void *save) {
  const bool set_val = *static_cast<const bool *>(save);
  if (set_val) {
    greatdb::is_bind_vip = false;
    greatdb::mysql_heartbeat();
  }
  *(bool *)var_ptr = false;
}

/*
  Plugin library descriptor
*/
static MYSQL_SYSVAR_STR(
    mgr_vip_ip,                                /* name */
    greatdb::mgr_vip_addr,                     /* var */
    PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_MEMALLOC, /* optional var | malloc string*/
    "The mgr vip address, host.", nullptr,     /* check func*/
    mgr_vip_addr_update,                       /* update func*/
    nullptr);                                  /* default*/

static MYSQL_SYSVAR_STR(
    mgr_vip_broad,                             /* name */
    greatdb::vip_broadip,                      /* var */
    PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_MEMALLOC, /* optional var | malloc string*/
    "The mgr vip broadcast, host.", nullptr,   /* check func*/
    nullptr,                                   /* update func*/
    "255.255.255.255");                        /* default*/

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
                         nullptr, /* check func. */
                         nullptr, /* update func*/
                         1        /* default */
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
    MYSQL_SYSVAR(mgr_vip_nic),
    MYSQL_SYSVAR(mgr_vip_broad),
    MYSQL_SYSVAR(enable_mgr_vip),
    MYSQL_SYSVAR(mgr_vip_mask),
    MYSQL_SYSVAR(force_change_mgr_vip),
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
