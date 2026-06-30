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

#ifndef PLUGIN_GDB_HA_MGR_H
#define PLUGIN_GDB_HA_MGR_H

#include <shared_mutex>
#include "gdb_service_registrator.h"
#include "mysql/components/service.h"
#include "mysql/components/service_implementation.h"  // for DEFINE_BOOL_METHOD
#include "mysql/components/services/group_member_status_listener.h"
#include "mysql/components/services/group_membership_listener.h"
#include "mysql/plugin_group_replication.h"  // for GROUP_REPLICATI...
#include "mysql_com.h"                       // for NAME_LEN
#include "sql/replication.h"
#include "sql/rpl_group_replication.h"  // for get_group_repli...
#include "sql/rpl_msr.h"  // channel_map, CHANNEL_NAME_LENGTH, HOSTNAME_LENGTH

namespace greatdb {
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
  ulonglong second_behind_group;
};

struct st_row_group_members {
  enum class State {
    MGR_ONLINE,
    MGR_RECOVERING,
    MGR_UNREACHABLE,
    MGR_OFFLINE,
    MGR_ERROR,
  };
  enum class Role { ROLE_PRIMARY, ROLE_SECONDARY, ROLE_ARBITRATOR };
  std::string member_id;
  std::string member_host;
  unsigned int member_port;
  State member_state;
  Role member_role;
  bool is_invalid = false;
};

/*
Callbacks implementation for GROUP_REPLICATION_GROUP_MEMBER_STATS_CALLBACKS.
*/
void set_channel_name_stats(void *const context, const char &value,
                            size_t length);

void set_view_id_stats(void *const context, const char &value, size_t length);

void set_member_id_stats(void *const context, const char &value, size_t length);

void set_transactions_committed(void *const context, const char &value,
                                size_t length);

void set_last_conflict_free_transaction(void *const context, const char &value,
                                        size_t length);

void set_transactions_in_queue(void *const context,
                               unsigned long long int value);

void set_transactions_certified(void *const context,
                                unsigned long long int value);

void set_transactions_conflicts_detected(void *const context,
                                         unsigned long long int value);

void set_transactions_rows_in_validation(void *const context,
                                         unsigned long long int value);

void set_transactions_remote_applier_queue(void *const context,
                                           unsigned long long int value);

void set_transactions_remote_applied(void *const context,
                                     unsigned long long int value);

void set_transactions_local_proposed(void *const context,
                                     unsigned long long int value);

void set_transactions_local_rollback(void *const context,
                                     unsigned long long int value);

void set_member_id(void *const context, const char &value, size_t length);

void set_member_state(void *const context, const char &value, size_t length);

void set_channel_name(void *const /*context*/, const char & /*value*/,
                      size_t /*length*/);

void set_member_host(void *const context, const char &value, size_t length);

void set_member_role(void *const context, const char &value, size_t length);

void set_member_port(void *const context, unsigned int value);

void set_member_version(void *const /*context*/,
                        const char &
                        /*value*/,
                        size_t /*length*/);

void set_second_behind_group(void *const context, unsigned long long int value);

void notify_group_replication_view();

DEFINE_BOOL_METHOD(gdb_notify_view_change, (const char *));
DEFINE_BOOL_METHOD(gdb_notify_quorum_loss, (const char *));

DEFINE_BOOL_METHOD(gdb_notify_member_role_change, (const char *));
DEFINE_BOOL_METHOD(gdb_notify_member_state_change, (const char *));

void register_services();
void unregister_services();

void set_member_incoming_communication_protocol(void *const /*context*/,
                                                const char &
                                                /*value*/,
                                                size_t /*length*/);
}  // namespace greatdb

#endif
