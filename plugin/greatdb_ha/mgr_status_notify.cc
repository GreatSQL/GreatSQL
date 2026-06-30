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

#include "mgr_status_notify.h"

namespace greatdb {
/*
Callbacks implementation for GROUP_REPLICATION_GROUP_MEMBER_STATS_CALLBACKS.
*/
void set_channel_name_stats(void *const context, const char &value,
                            size_t length) {
  struct st_row_group_member_stats *row =
      static_cast<struct st_row_group_member_stats *>(context);
  const size_t max = CHANNEL_NAME_LENGTH;
  length = std::min(length, max);

  row->channel_name_length = length;
  memcpy(row->channel_name, &value, length);
}

void set_view_id_stats(void *const context, const char &value, size_t length) {
  struct st_row_group_member_stats *row =
      static_cast<struct st_row_group_member_stats *>(context);
  const size_t max = HOSTNAME_LENGTH;
  length = std::min(length, max);

  row->view_id_length = length;
  memcpy(row->view_id, &value, length);
}

void set_member_id_stats(void *const context, const char &value,
                         size_t length) {
  struct st_row_group_member_stats *row =
      static_cast<struct st_row_group_member_stats *>(context);
  const size_t max = UUID_LENGTH;
  length = std::min(length, max);

  row->member_id_length = length;
  memcpy(row->member_id, &value, length);
}

void set_transactions_committed(void *const context, const char &value,
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

void set_last_conflict_free_transaction(void *const context, const char &value,
                                        size_t length) {
  struct st_row_group_member_stats *row =
      static_cast<struct st_row_group_member_stats *>(context);
  const size_t max = Gtid::MAX_TEXT_LENGTH + 1;
  length = std::min(length, max);

  row->last_cert_trx_length = length;
  memcpy(row->last_cert_trx, &value, length);
}

void set_transactions_in_queue(void *const context,
                               unsigned long long int value) {
  struct st_row_group_member_stats *row =
      static_cast<struct st_row_group_member_stats *>(context);
  row->trx_in_queue = value;
}

void set_transactions_certified(void *const context,
                                unsigned long long int value) {
  struct st_row_group_member_stats *row =
      static_cast<struct st_row_group_member_stats *>(context);
  row->trx_checked = value;
}

void set_transactions_conflicts_detected(void *const context,
                                         unsigned long long int value) {
  struct st_row_group_member_stats *row =
      static_cast<struct st_row_group_member_stats *>(context);
  row->trx_conflicts = value;
}

void set_transactions_rows_in_validation(void *const context,
                                         unsigned long long int value) {
  struct st_row_group_member_stats *row =
      static_cast<struct st_row_group_member_stats *>(context);
  row->trx_rows_validating = value;
}

void set_transactions_remote_applier_queue(void *const context,
                                           unsigned long long int value) {
  struct st_row_group_member_stats *row =
      static_cast<struct st_row_group_member_stats *>(context);
  row->trx_remote_applier_queue = value;
}

void set_transactions_remote_applied(void *const context,
                                     unsigned long long int value) {
  struct st_row_group_member_stats *row =
      static_cast<struct st_row_group_member_stats *>(context);
  row->trx_remote_applied = value;
}

void set_transactions_local_proposed(void *const context,
                                     unsigned long long int value) {
  struct st_row_group_member_stats *row =
      static_cast<struct st_row_group_member_stats *>(context);
  row->trx_local_proposed = value;
}

void set_transactions_local_rollback(void *const context,
                                     unsigned long long int value) {
  struct st_row_group_member_stats *row =
      static_cast<struct st_row_group_member_stats *>(context);
  row->trx_local_rollback = value;
}

void set_member_id(void *const context, const char &value, size_t length) {
  struct st_row_group_members *row =
      static_cast<struct st_row_group_members *>(context);
  const size_t max = UUID_LENGTH;
  length = std::min(length, max);

  row->member_id = std::string(&value, length);
}

void set_member_state(void *const context, const char &value, size_t length) {
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

void set_channel_name(void *const /*context*/, const char & /*value*/,
                      size_t /*length*/) {}

void set_member_host(void *const context, const char &value, size_t length) {
  struct st_row_group_members *row =
      static_cast<struct st_row_group_members *>(context);
  const size_t max = NAME_LEN;
  length = std::min(length, max);
  row->member_host = std::string(&value, length);
}

void set_member_role(void *const context, const char &value, size_t length) {
  struct st_row_group_members *row =
      static_cast<struct st_row_group_members *>(context);
  const size_t max = NAME_LEN;
  length = std::min(length, max);

  std::string str(&value, length);
  if (str == "PRIMARY") {
    row->member_role = st_row_group_members::Role::ROLE_PRIMARY;
  } else if (str == "ARBITRATOR") {
    row->member_role = st_row_group_members::Role::ROLE_ARBITRATOR;
  } else {
    row->member_role = st_row_group_members::Role::ROLE_SECONDARY;
  }
}

void set_member_port(void *const context, unsigned int value) {
  struct st_row_group_members *row =
      static_cast<struct st_row_group_members *>(context);
  row->member_port = value;
}

void set_member_version(void *const /*context*/,
                        const char &
                        /*value*/,
                        size_t /*length*/) {}

void set_second_behind_group(void *const context,
                             unsigned long long int value) {
  struct st_row_group_member_stats *row =
      static_cast<struct st_row_group_member_stats *>(context);
  row->second_behind_group = value;
}

DEFINE_BOOL_METHOD(gdb_notify_view_change, (const char *)) {
  notify_group_replication_view();
  return false;
}
DEFINE_BOOL_METHOD(gdb_notify_quorum_loss, (const char *)) {
  /*
    This function SHALL be called whenever the state of a member
    changes to UNREACHABLE and that makes the system block.
  */
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

SERVICE_TYPE_NO_CONST(group_member_status_listener)
SERVICE_IMPLEMENTATION(greatdb_ha, group_member_status_listener) = {
    gdb_notify_member_role_change, gdb_notify_member_state_change};

void register_services() {
  Service_registrator r;

  r.register_service(SERVICE(greatdb_ha, group_membership_listener));
  r.register_service(SERVICE(greatdb_ha, group_member_status_listener));
}
void unregister_services() {
  Service_registrator r;

  r.unregister_service(SERVICE_ID(greatdb_ha, group_membership_listener));
  r.unregister_service(SERVICE_ID(greatdb_ha, group_member_status_listener));
}

void set_member_incoming_communication_protocol(void *const /*context*/,
                                                const char &
                                                /*value*/,
                                                size_t /*length*/) {}

}  // namespace greatdb
