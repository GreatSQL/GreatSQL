/* Copyright (c) 2016, 2021, Oracle and/or its affiliates. All rights reserved.
   Copyright (c) 2021, GreatDB Software Co., Ltd

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

#include "plugin/group_replication/include/pipeline_stats.h"

#include <time.h>

#include <mysql/components/services/log_builtins.h>
#include "my_byteorder.h"
#include "my_dbug.h"
#include "my_systime.h"
#include "plugin/group_replication/include/plugin.h"
#include "plugin/group_replication/include/plugin_server_include.h"

/*
  The QUOTA based flow control tries to calculate how many
  transactions the slowest members can handle, at the certifier or
  at the applier level, by checking which members have a queue
  larger than the user-specified thresholds and, on those, checking
  which one has the lowest number of transactions certified/applied
  on the last step - let's call it MMT, which stands for Minimum
  Member Throughput. We then divide MMT by the number of writing
  members in the last step to specify how many transactions a
  member can safely send to the group (if a new member starts to
  write then the quota will be larger for one period but will be
  corrected on the next).
  About these factors:
    1. If we used MMT as the assigned quota (and if MMT represented
       well the capacity of the nodes) then the queue size would
       stabilize but would not decrease. To allow a delayed node to
       catch up on the certifier and/or queues we need to reserve
       some capacity on the slowest node, which this HOLD_FACTOR
       represents: 10% reserved to catch up.
    2. Once the queue is reduced below the user-specified threshold,
       the nodes would start to issue transactions at full speed
       even if that full speed meant pilling up many transactions
       in a single period. To avoid that we introduce the
       RELEASE_FACTOR (50%), which is enough to let the write
       capacity to grow quickly but still maintain a relation with
       the last throttled value so that the oscillation in number
       of transactions per second is not very steep, letting the
       throughput oscillate smoothly around the real cluster
       capacity.
*/
const int64 Flow_control_module::MAXTPS = INT_MAX32;

Pipeline_stats_member_message::Pipeline_stats_member_message(
    int32 transactions_waiting_certification, int32 transactions_waiting_apply,
    int64 transactions_certified, int64 transactions_applied,
    int64 transactions_local, int64 transactions_negative_certified,
    int64 transactions_rows_in_validation, bool transaction_gtids,
    const std::string &transactions_all_committed,
    const std::string &transactions_last_conflict_free,
    int64 transactions_local_rollback, Flow_control_mode mode)
    : Plugin_gcs_message(CT_PIPELINE_STATS_MEMBER_MESSAGE),
      m_transactions_waiting_certification(transactions_waiting_certification),
      m_transactions_waiting_apply(transactions_waiting_apply),
      m_transactions_certified(transactions_certified),
      m_transactions_applied(transactions_applied),
      m_transactions_local(transactions_local),
      m_transactions_negative_certified(transactions_negative_certified),
      m_transactions_rows_validating(transactions_rows_in_validation),
      m_transaction_gtids_present(transaction_gtids),
      m_transactions_committed_all_members(transactions_all_committed),
      m_transaction_last_conflict_free(transactions_last_conflict_free),
      m_transactions_local_rollback(transactions_local_rollback),
      m_flow_control_mode(mode) {}

Pipeline_stats_member_message::Pipeline_stats_member_message(
    const unsigned char *buf, size_t len)
    : Plugin_gcs_message(CT_PIPELINE_STATS_MEMBER_MESSAGE),
      m_transactions_waiting_certification(0),
      m_transactions_waiting_apply(0),
      m_transactions_certified(0),
      m_transactions_applied(0),
      m_transactions_local(0),
      m_transactions_negative_certified(0),
      m_transactions_rows_validating(0),
      m_transaction_gtids_present(false),
      m_transactions_committed_all_members(""),
      m_transaction_last_conflict_free(""),
      m_transactions_local_rollback(0),
      m_flow_control_mode(FCM_QUOTA) {
  decode(buf, len);
}

Pipeline_stats_member_message::~Pipeline_stats_member_message() {}

int32 Pipeline_stats_member_message::get_transactions_waiting_certification() {
  DBUG_TRACE;
  return m_transactions_waiting_certification;
}

int64 Pipeline_stats_member_message::get_transactions_certified() {
  DBUG_TRACE;
  return m_transactions_certified;
}

int32 Pipeline_stats_member_message::get_transactions_waiting_apply() {
  DBUG_TRACE;
  return m_transactions_waiting_apply;
}

int64 Pipeline_stats_member_message::get_transactions_applied() {
  DBUG_TRACE;
  return m_transactions_applied;
}

int64 Pipeline_stats_member_message::get_transactions_local() {
  DBUG_TRACE;
  return m_transactions_local;
}

int64 Pipeline_stats_member_message::get_transactions_negative_certified() {
  DBUG_TRACE;
  return m_transactions_negative_certified;
}

int64 Pipeline_stats_member_message::get_transactions_rows_validating() {
  DBUG_TRACE;
  return m_transactions_rows_validating;
}

bool Pipeline_stats_member_message::get_transation_gtids_present() const {
  return m_transaction_gtids_present;
}

int64 Pipeline_stats_member_message::get_transactions_local_rollback() {
  DBUG_TRACE;
  return m_transactions_local_rollback;
}

const std::string &
Pipeline_stats_member_message::get_transaction_committed_all_members() {
  DBUG_TRACE;
  return m_transactions_committed_all_members;
}

const std::string &
Pipeline_stats_member_message::get_transaction_last_conflict_free() {
  DBUG_TRACE;
  return m_transaction_last_conflict_free;
}

Flow_control_mode Pipeline_stats_member_message::get_flow_control_mode() {
  DBUG_TRACE;
  return m_flow_control_mode;
}

void Pipeline_stats_member_message::encode_payload(
    std::vector<unsigned char> *buffer) const {
  DBUG_TRACE;

  uint32 transactions_waiting_certification_aux =
      (uint32)m_transactions_waiting_certification;
  encode_payload_item_int4(buffer, PIT_TRANSACTIONS_WAITING_CERTIFICATION,
                           transactions_waiting_certification_aux);

  uint32 transactions_waiting_apply_aux = (uint32)m_transactions_waiting_apply;
  encode_payload_item_int4(buffer, PIT_TRANSACTIONS_WAITING_APPLY,
                           transactions_waiting_apply_aux);

  uint64 transactions_certified_aux = (uint64)m_transactions_certified;
  encode_payload_item_int8(buffer, PIT_TRANSACTIONS_CERTIFIED,
                           transactions_certified_aux);

  uint64 transactions_applied_aux = (uint64)m_transactions_applied;
  encode_payload_item_int8(buffer, PIT_TRANSACTIONS_APPLIED,
                           transactions_applied_aux);

  uint64 transactions_local_aux = (uint64)m_transactions_local;
  encode_payload_item_int8(buffer, PIT_TRANSACTIONS_LOCAL,
                           transactions_local_aux);

  uint64 transactions_negative_certified_aux =
      (uint64)m_transactions_negative_certified;
  encode_payload_item_int8(buffer, PIT_TRANSACTIONS_NEGATIVE_CERTIFIED,
                           transactions_negative_certified_aux);

  uint64 transactions_rows_validating_aux =
      (uint64)m_transactions_rows_validating;
  encode_payload_item_int8(buffer, PIT_TRANSACTIONS_ROWS_VALIDATING,
                           transactions_rows_validating_aux);

  encode_payload_item_string(buffer, PIT_TRANSACTIONS_COMMITTED_ALL_MEMBERS,
                             m_transactions_committed_all_members.c_str(),
                             m_transactions_committed_all_members.length());

  encode_payload_item_string(buffer, PIT_TRANSACTION_LAST_CONFLICT_FREE,
                             m_transaction_last_conflict_free.c_str(),
                             m_transaction_last_conflict_free.length());

  uint64 transactions_local_rollback_aux =
      (uint64)m_transactions_local_rollback;
  encode_payload_item_int8(buffer, PIT_TRANSACTIONS_LOCAL_ROLLBACK,
                           transactions_local_rollback_aux);

  char flow_control_mode_aux = static_cast<char>(get_flow_control_mode_var());
  encode_payload_item_char(buffer, PIT_FLOW_CONTROL_MODE,
                           flow_control_mode_aux);

  char aux_transaction_gtids_present = m_transaction_gtids_present ? '1' : '0';
  encode_payload_item_char(buffer, PIT_TRANSACTION_GTIDS_PRESENT,
                           aux_transaction_gtids_present);
}

void Pipeline_stats_member_message::decode_payload(const unsigned char *buffer,
                                                   const unsigned char *end) {
  DBUG_TRACE;
  const unsigned char *slider = buffer;
  uint16 payload_item_type = 0;
  unsigned long long payload_item_length = 0;

  uint32 transactions_waiting_certification_aux = 0;
  decode_payload_item_int4(&slider, &payload_item_type,
                           &transactions_waiting_certification_aux);
  m_transactions_waiting_certification =
      (int32)transactions_waiting_certification_aux;

  uint32 transactions_waiting_apply_aux = 0;
  decode_payload_item_int4(&slider, &payload_item_type,
                           &transactions_waiting_apply_aux);
  m_transactions_waiting_apply = (int32)transactions_waiting_apply_aux;

  uint64 transactions_certified_aux = 0;
  decode_payload_item_int8(&slider, &payload_item_type,
                           &transactions_certified_aux);
  m_transactions_certified = (int64)transactions_certified_aux;

  uint64 transactions_applied_aux = 0;
  decode_payload_item_int8(&slider, &payload_item_type,
                           &transactions_applied_aux);
  m_transactions_applied = (int64)transactions_applied_aux;

  uint64 transactions_local_aux = 0;
  decode_payload_item_int8(&slider, &payload_item_type,
                           &transactions_local_aux);
  m_transactions_local = (int64)transactions_local_aux;

  while (slider + Plugin_gcs_message::WIRE_PAYLOAD_ITEM_HEADER_SIZE <= end) {
    // Read payload item header to find payload item length.
    decode_payload_item_type_and_length(&slider, &payload_item_type,
                                        &payload_item_length);

    switch (payload_item_type) {
      case PIT_TRANSACTIONS_NEGATIVE_CERTIFIED:
        if (slider + payload_item_length <= end) {
          uint64 transactions_negative_certified_aux = uint8korr(slider);
          slider += payload_item_length;
          m_transactions_negative_certified =
              static_cast<int64>(transactions_negative_certified_aux);
        }
        break;

      case PIT_TRANSACTIONS_ROWS_VALIDATING:
        if (slider + payload_item_length <= end) {
          uint64 transactions_rows_validating_aux = uint8korr(slider);
          slider += payload_item_length;
          m_transactions_rows_validating =
              static_cast<int64>(transactions_rows_validating_aux);
        }
        break;

      case PIT_TRANSACTIONS_COMMITTED_ALL_MEMBERS:
        if (slider + payload_item_length <= end) {
          m_transactions_committed_all_members.assign(
              slider, slider + payload_item_length);
          slider += payload_item_length;
        }
        break;

      case PIT_TRANSACTION_LAST_CONFLICT_FREE:
        if (slider + payload_item_length <= end) {
          m_transaction_last_conflict_free.assign(slider,
                                                  slider + payload_item_length);
          slider += payload_item_length;
        }
        break;

      case PIT_TRANSACTIONS_LOCAL_ROLLBACK:
        if (slider + payload_item_length <= end) {
          uint64 transactions_local_rollback_aux = uint8korr(slider);
          slider += payload_item_length;
          m_transactions_local_rollback =
              static_cast<int64>(transactions_local_rollback_aux);
        }
        break;

      case PIT_FLOW_CONTROL_MODE:
        if (slider + payload_item_length <= end) {
          unsigned char flow_control_mode_aux = *slider;
          slider += payload_item_length;
          m_flow_control_mode = (Flow_control_mode)flow_control_mode_aux;
        }
        break;

      case PIT_TRANSACTION_GTIDS_PRESENT:
        if (slider + payload_item_length <= end) {
          unsigned char aux_transaction_gtids_present = *slider;
          slider += payload_item_length;
          m_transaction_gtids_present =
              (aux_transaction_gtids_present == '1') ? true : false;
        }
        break;
    }
  }
}

Pipeline_stats_member_collector::Pipeline_stats_member_collector()
    : m_transactions_waiting_apply(0),
      m_transactions_certified(0),
      m_transactions_applied(0),
      m_transactions_local(0),
      m_transactions_local_rollback(0),
      m_transactions_certified_during_recovery(0),
      m_transactions_certified_negatively_during_recovery(0),
      m_transactions_applied_during_recovery(0),
      m_previous_transactions_applied_during_recovery(0),
      m_delta_transactions_applied_during_recovery(0),
      m_transactions_delivered_during_recovery(0),
      send_transaction_identifiers(false) {
  mysql_mutex_init(key_GR_LOCK_pipeline_stats_transactions_waiting_apply,
                   &m_transactions_waiting_apply_lock, MY_MUTEX_INIT_FAST);
}

Pipeline_stats_member_collector::~Pipeline_stats_member_collector() {
  mysql_mutex_destroy(&m_transactions_waiting_apply_lock);
}

void Pipeline_stats_member_collector::increment_transactions_waiting_apply() {
  mysql_mutex_lock(&m_transactions_waiting_apply_lock);
  assert(m_transactions_waiting_apply.load() >= 0);
  ++m_transactions_waiting_apply;
  mysql_mutex_unlock(&m_transactions_waiting_apply_lock);
}

void Pipeline_stats_member_collector::decrement_transactions_waiting_apply() {
  mysql_mutex_lock(&m_transactions_waiting_apply_lock);
  if (m_transactions_waiting_apply.load() > 0) --m_transactions_waiting_apply;
  assert(m_transactions_waiting_apply.load() >= 0);
  mysql_mutex_unlock(&m_transactions_waiting_apply_lock);
}

void Pipeline_stats_member_collector::increment_transactions_certified() {
  ++m_transactions_certified;
}

void Pipeline_stats_member_collector::increment_transactions_applied() {
  ++m_transactions_applied;
}

void Pipeline_stats_member_collector::increment_transactions_local() {
  ++m_transactions_local;
}

void Pipeline_stats_member_collector::increment_transactions_local_rollback() {
  ++m_transactions_local_rollback;
}

int32 Pipeline_stats_member_collector::get_transactions_waiting_apply() {
  return m_transactions_waiting_apply.load();
}

int64 Pipeline_stats_member_collector::get_transactions_certified() {
  return m_transactions_certified.load();
}

int64 Pipeline_stats_member_collector::get_transactions_applied() {
  return m_transactions_applied.load();
}

int64 Pipeline_stats_member_collector::get_transactions_local() {
  return m_transactions_local.load();
}

int64 Pipeline_stats_member_collector::get_transactions_local_rollback() {
  return m_transactions_local_rollback.load();
}

void Pipeline_stats_member_collector::set_send_transaction_identifiers() {
  send_transaction_identifiers = true;
}

void Pipeline_stats_member_collector::
    increment_transactions_certified_during_recovery() {
  ++m_transactions_certified_during_recovery;
}

void Pipeline_stats_member_collector::
    increment_transactions_certified_negatively_during_recovery() {
  ++m_transactions_certified_negatively_during_recovery;
}

void Pipeline_stats_member_collector::
    increment_transactions_applied_during_recovery() {
  ++m_transactions_applied_during_recovery;
}

void Pipeline_stats_member_collector::
    compute_transactions_deltas_during_recovery() {
  m_delta_transactions_applied_during_recovery.store(
      m_transactions_applied_during_recovery.load() -
      m_previous_transactions_applied_during_recovery);
  m_previous_transactions_applied_during_recovery =
      m_transactions_applied_during_recovery.load();
}

uint64 Pipeline_stats_member_collector::
    get_delta_transactions_applied_during_recovery() {
  return m_delta_transactions_applied_during_recovery.load();
}

uint64 Pipeline_stats_member_collector::
    get_transactions_waiting_apply_during_recovery() {
  uint64 transactions_delivered_during_recovery =
      m_transactions_delivered_during_recovery.load();
  uint64 transactions_applied_during_recovery =
      m_transactions_applied_during_recovery.load();
  uint64 transactions_certified_negatively_during_recovery =
      m_transactions_certified_negatively_during_recovery.load();

  /* view change transactions were applied */
  if ((transactions_applied_during_recovery +
       transactions_certified_negatively_during_recovery) >
      transactions_delivered_during_recovery) {
    return 0;
  }

  return transactions_delivered_during_recovery -
         transactions_applied_during_recovery -
         transactions_certified_negatively_during_recovery;
}

void Pipeline_stats_member_collector::
    increment_transactions_delivered_during_recovery() {
  ++m_transactions_delivered_during_recovery;
}

uint64 Pipeline_stats_member_collector::
    get_transactions_waiting_certification_during_recovery() {
  assert(m_transactions_delivered_during_recovery.load() >=
         m_transactions_certified_during_recovery.load());
  return m_transactions_delivered_during_recovery.load() -
         m_transactions_certified_during_recovery.load();
}

void Pipeline_stats_member_collector::send_stats_member_message(
    Flow_control_mode mode) {
  if (local_member_info == nullptr) return; /* purecov: inspected */
  Group_member_info::Group_member_status member_status =
      local_member_info->get_recovery_status();
  if (member_status != Group_member_info::MEMBER_ONLINE &&
      member_status != Group_member_info::MEMBER_IN_RECOVERY)
    return;

  std::string last_conflict_free_transaction;
  std::string committed_transactions;

  Certifier_interface *cert_interface =
      (applier_module && applier_module->get_certification_handler())
          ? applier_module->get_certification_handler()->get_certifier()
          : nullptr;

  if (send_transaction_identifiers && cert_interface != nullptr) {
    char *committed_transactions_buf = nullptr;
    size_t committed_transactions_buf_length = 0;
    int get_group_stable_transactions_set_string_outcome =
        cert_interface->get_group_stable_transactions_set_string(
            &committed_transactions_buf, &committed_transactions_buf_length);
    if (!get_group_stable_transactions_set_string_outcome &&
        committed_transactions_buf_length > 0) {
      committed_transactions.assign(committed_transactions_buf);
    }
    my_free(committed_transactions_buf);
    cert_interface->get_last_conflict_free_transaction(
        &last_conflict_free_transaction);
  }

  Pipeline_stats_member_message message(
      static_cast<int32>(applier_module->get_message_queue_size()),
      m_transactions_waiting_apply.load(), m_transactions_certified.load(),
      m_transactions_applied.load(), m_transactions_local.load(),
      (cert_interface != nullptr) ? cert_interface->get_negative_certified()
                                  : 0,
      (cert_interface != nullptr)
          ? cert_interface->get_certification_info_size()
          : 0,
      send_transaction_identifiers, committed_transactions,
      last_conflict_free_transaction, m_transactions_local_rollback.load(),
      mode);

  enum_gcs_error msg_error = gcs_module->send_message(message, true);
  if (msg_error != GCS_OK) {
    LogPluginErr(INFORMATION_LEVEL,
                 ER_GRP_RPL_SEND_STATS_ERROR); /* purecov: inspected */
  }
  send_transaction_identifiers = false;
}

Pipeline_member_stats::Pipeline_member_stats()
    : m_transactions_waiting_certification(0),
      m_transactions_waiting_apply(0),
      m_transactions_certified(0),
      m_delta_transactions_certified(0),
      m_transactions_applied(0),
      m_delta_transactions_applied(0),
      m_transactions_local(0),
      m_delta_transactions_local(0),
      m_transactions_negative_certified(0),
      m_transactions_rows_validating(0),
      m_transactions_committed_all_members(),
      m_transaction_last_conflict_free(),
      m_transactions_local_rollback(0),
      m_flow_control_mode(FCM_QUOTA),
      m_stamp(0) {}

Pipeline_member_stats::Pipeline_member_stats(Pipeline_stats_member_message &msg)
    : m_transactions_waiting_certification(
          msg.get_transactions_waiting_certification()),
      m_transactions_waiting_apply(msg.get_transactions_waiting_apply()),
      m_transactions_certified(msg.get_transactions_certified()),
      m_delta_transactions_certified(0),
      m_transactions_applied(msg.get_transactions_applied()),
      m_delta_transactions_applied(0),
      m_transactions_local(msg.get_transactions_local()),
      m_delta_transactions_local(0),
      m_transactions_negative_certified(
          msg.get_transactions_negative_certified()),
      m_transactions_rows_validating(msg.get_transactions_rows_validating()),
      m_transactions_committed_all_members(
          msg.get_transaction_committed_all_members()),
      m_transaction_last_conflict_free(
          msg.get_transaction_last_conflict_free()),
      m_transactions_local_rollback(msg.get_transactions_local_rollback()),
      m_flow_control_mode(msg.get_flow_control_mode()),
      m_stamp(0) {}

Pipeline_member_stats::Pipeline_member_stats(
    Pipeline_stats_member_collector *pipeline_stats, ulonglong applier_queue,
    ulonglong negative_certified, ulonglong certification_size) {
  m_transactions_waiting_certification = applier_queue;
  m_transactions_waiting_apply =
      pipeline_stats->get_transactions_waiting_apply();
  m_transactions_certified = pipeline_stats->get_transactions_certified();
  m_delta_transactions_certified = 0;
  m_transactions_applied = pipeline_stats->get_transactions_applied();
  m_delta_transactions_applied = 0;
  m_transactions_local = pipeline_stats->get_transactions_local();
  m_delta_transactions_local = 0;
  m_transactions_negative_certified = negative_certified;
  m_transactions_rows_validating = certification_size;
  m_transactions_local_rollback =
      pipeline_stats->get_transactions_local_rollback();
  m_flow_control_mode = FCM_DISABLED;
  m_stamp = 0;
}

void Pipeline_member_stats::update_member_stats(
    Pipeline_stats_member_message &msg, uint64 stamp) {
  m_transactions_waiting_certification =
      msg.get_transactions_waiting_certification();

  m_transactions_waiting_apply = msg.get_transactions_waiting_apply();

  int64 previous_transactions_certified = m_transactions_certified;
  m_transactions_certified = msg.get_transactions_certified();
  m_delta_transactions_certified =
      m_transactions_certified - previous_transactions_certified;

  int64 previous_transactions_applied = m_transactions_applied;
  m_transactions_applied = msg.get_transactions_applied();
  m_delta_transactions_applied =
      m_transactions_applied - previous_transactions_applied;

  int64 previous_transactions_local = m_transactions_local;
  m_transactions_local = msg.get_transactions_local();
  m_delta_transactions_local =
      m_transactions_local - previous_transactions_local;

  m_transactions_negative_certified = msg.get_transactions_negative_certified();

  m_transactions_rows_validating = msg.get_transactions_rows_validating();

  /*
    Only update the transaction GTIDs if the current stats message contains
    these GTIDs, i.e. if they are "dirty" and in need of an update.
   */
  if (msg.get_transation_gtids_present()) {
    m_transactions_committed_all_members =
        msg.get_transaction_committed_all_members();
    m_transaction_last_conflict_free = msg.get_transaction_last_conflict_free();
  }

  m_transactions_local_rollback = msg.get_transactions_local_rollback();

  m_flow_control_mode = msg.get_flow_control_mode();

  m_stamp = stamp;
}

bool Pipeline_member_stats::is_flow_control_needed() {
  return m_flow_control_mode == FCM_QUOTA;
}

int32 Pipeline_member_stats::get_transactions_waiting_certification() {
  return m_transactions_waiting_certification;
}

int32 Pipeline_member_stats::get_transactions_waiting_apply() {
  return m_transactions_waiting_apply;
}

int64 Pipeline_member_stats::get_delta_transactions_certified() {
  return m_delta_transactions_certified;
}

int64 Pipeline_member_stats::get_delta_transactions_applied() {
  return m_delta_transactions_applied;
}

int64 Pipeline_member_stats::get_delta_transactions_local() {
  return m_delta_transactions_local;
}

int64 Pipeline_member_stats::get_transactions_certified() {
  return m_transactions_certified;
}

int64 Pipeline_member_stats::get_transactions_applied() {
  return m_transactions_applied;
}

int64 Pipeline_member_stats::get_transactions_local() {
  return m_transactions_local;
}

int64 Pipeline_member_stats::get_transactions_negative_certified() {
  return m_transactions_negative_certified;
}

int64 Pipeline_member_stats::get_transactions_rows_validating() {
  return m_transactions_rows_validating;
}

int64 Pipeline_member_stats::get_transactions_local_rollback() {
  return m_transactions_local_rollback;
}

void Pipeline_member_stats::get_transaction_committed_all_members(
    std::string &value) {
  value.assign(m_transactions_committed_all_members);
}

void Pipeline_member_stats::set_transaction_committed_all_members(char *str,
                                                                  size_t len) {
  m_transactions_committed_all_members.assign(str, len);
}

void Pipeline_member_stats::get_transaction_last_conflict_free(
    std::string &value) {
  value.assign(m_transaction_last_conflict_free);
}

void Pipeline_member_stats::set_transaction_last_conflict_free(
    std::string &value) {
  m_transaction_last_conflict_free.assign(value);
}

Flow_control_mode Pipeline_member_stats::get_flow_control_mode() {
  return m_flow_control_mode;
}

uint64 Pipeline_member_stats::get_stamp() { return m_stamp; }

#ifndef NDEBUG
void Pipeline_member_stats::debug(const char *member, int64 quota_size,
                                  int64 quota_used) {
  LogPluginErr(INFORMATION_LEVEL, ER_GRP_RPL_MEMBER_STATS_INFO, member,
               m_transactions_waiting_certification,
               m_transactions_waiting_apply, m_transactions_certified,
               m_delta_transactions_certified, m_transactions_applied,
               m_delta_transactions_applied, m_transactions_local,
               m_delta_transactions_local, quota_size, quota_used,
               m_flow_control_mode); /* purecov: inspected */
}
#endif

Flow_control_module::Flow_control_module()
    : m_holds_in_period(0),
      m_quota_used(0),
      m_quota_size(0),
      m_stamp(0),
      seconds_to_skip(1) {
  mysql_mutex_init(key_GR_LOCK_pipeline_stats_flow_control,
                   &m_flow_control_lock, MY_MUTEX_INIT_FAST);
  mysql_cond_init(key_GR_COND_pipeline_stats_flow_control,
                  &m_flow_control_cond);
  m_wait_counter = 0;
  m_leave_counter = 0;
  m_current_wait_msec = 10;
  m_last_cert_database_size = 0;
  m_flow_control_need_refreshed = 0;
  m_flow_control_flag = 0;
  m_max_wait_time = FLOW_CONTROL_MAX_WAIT_TIME;
  m_flow_control_module_info_lock = new Checkable_rwlock(
#ifdef HAVE_PSI_INTERFACE
      key_GR_RWLOCK_flow_control_module_info
#endif
  );
}

Flow_control_module::~Flow_control_module() {
  mysql_mutex_destroy(&m_flow_control_lock);
  mysql_cond_destroy(&m_flow_control_cond);
  delete m_flow_control_module_info_lock;
}

void Flow_control_module::flow_control_step(
    Pipeline_stats_member_collector *member, bool send_stats_flag) {
  Flow_control_mode fcm =
      static_cast<Flow_control_mode>(get_flow_control_mode_var());

  /*
    Send statistics to other members
  */
  if (send_stats_flag) {
    member->send_stats_member_message(fcm);
  }

  if (fcm == FCM_QUOTA) {
    Certifier_interface *cert_interface =
        (applier_module && applier_module->get_certification_handler())
            ? applier_module->get_certification_handler()->get_certifier()
            : nullptr;
    if (cert_interface) {
      ulonglong size = cert_interface->get_certification_info_size();

      double estimated_replay_time =
          cert_interface->get_certification_estimated_replay_time();
      int max_wait_time = get_flow_control_max_wait_time_var() * 1000;

      mysql_mutex_lock(&m_flow_control_lock);
      if (estimated_replay_time < get_flow_control_replay_lag_behind_var()) {
        if (m_wait_counter != m_leave_counter) {
          mysql_cond_broadcast(&m_flow_control_cond);
          m_current_wait_msec = 10;
          m_flow_control_need_refreshed = 0;
          m_max_wait_time = max_wait_time;
          m_flow_control_flag = 1;
        } else {
          m_flow_control_flag = 0;
        }
      } else {
        m_flow_control_flag = 1;
        double added = cert_interface->get_certification_add_velocity();
        double deleted = cert_interface->get_certification_delete_velocity();
        if (added > deleted) {
          m_flow_control_need_refreshed = 1;
        }

        if (max_wait_time > FLOW_CONTROL_MAX_WAIT_TIME &&
            max_wait_time > m_max_wait_time) {
          m_max_wait_time = max_wait_time;
        }
      }
      m_last_cert_database_size = size;

      mysql_mutex_unlock(&m_flow_control_lock);
    } else {
      m_flow_control_flag = 1;
    }
  } else {
    m_flow_control_flag = 0;
  }
}

int Flow_control_module::handle_stats_data(const uchar *data, size_t len,
                                           const std::string &member_id) {
  DBUG_TRACE;
  int error = 0;
  Pipeline_stats_member_message message(data, len);

  /*
    This method is called synchronously by communication layer, so
    we do not need concurrency control.
  */
  m_flow_control_module_info_lock->wrlock();
  Flow_control_module_info::iterator it = m_info.find(member_id);
  if (it == m_info.end()) {
    Pipeline_member_stats stats;

    std::pair<Flow_control_module_info::iterator, bool> ret = m_info.insert(
        std::pair<std::string, Pipeline_member_stats>(member_id, stats));
    error = !ret.second;
    it = ret.first;
  }
  it->second.update_member_stats(message, m_stamp);

  /*
    Verify if flow control is required.
  */
  if (it->second.is_flow_control_needed()) {
    ++m_holds_in_period;
#ifndef NDEBUG
    it->second.debug(it->first.c_str(), m_quota_size.load(),
                     m_quota_used.load());
#endif
  }

  m_flow_control_module_info_lock->unlock();
  return error;
}

Pipeline_member_stats *Flow_control_module::get_pipeline_stats(
    const std::string &member_id) {
  Pipeline_member_stats *member_pipeline_stats = nullptr;
  m_flow_control_module_info_lock->rdlock();
  Flow_control_module_info::iterator it = m_info.find(member_id);
  if (it != m_info.end()) {
    try {
      DBUG_EXECUTE_IF("flow_control_simulate_bad_alloc_exception",
                      throw std::bad_alloc(););
      member_pipeline_stats = new Pipeline_member_stats(it->second);
    } catch (const std::bad_alloc &) {
      my_error(ER_STD_BAD_ALLOC_ERROR, MYF(0),
               "while getting replication_group_member_stats table rows",
               "get_pipeline_stats");
      m_flow_control_module_info_lock->unlock();
      return nullptr;
    }
  }
  m_flow_control_module_info_lock->unlock();
  return member_pipeline_stats;
}

bool Flow_control_module::check_still_waiting() {
  bool result = false;
  mysql_mutex_lock(&m_flow_control_lock);
  if (m_wait_counter != m_leave_counter) {
    result = true;
  }
  mysql_mutex_unlock(&m_flow_control_lock);

  return result;
}

int32 Flow_control_module::do_wait() {
  DBUG_TRACE;

  if (m_flow_control_flag &&
      (m_last_cert_database_size > FLOW_CONTROL_LOWER_THRESHOLD ||
       check_still_waiting())) {
    struct timespec delay;
    /* 10ms as the first wait time */
    set_timespec_nsec(&delay, m_current_wait_msec * 1000000ULL);

    mysql_mutex_lock(&m_flow_control_lock);
    m_wait_counter = m_wait_counter + 1;
    if (m_flow_control_need_refreshed) {
      m_flow_control_need_refreshed = 0;
      if (m_last_cert_database_size > FLOW_CONTROL_LOWER_THRESHOLD) {
        int added = 0;
        if (m_last_cert_database_size < FLOW_CONTROL_LOW_THRESHOLD) {
          added = FLOW_CONTROL_ADD_LEVEL1_WAIT_TIME;
        } else if (m_last_cert_database_size < FLOW_CONTROL_MID_THRESHOLD) {
          added = FLOW_CONTROL_ADD_LEVEL2_WAIT_TIME;
        } else if (m_last_cert_database_size < FLOW_CONTROL_HIGH_THRESHOLD) {
          added = FLOW_CONTROL_ADD_LEVEL3_WAIT_TIME;
        } else if (m_last_cert_database_size < FLOW_CONTROL_HIGHER_THRESHOLD) {
          added = FLOW_CONTROL_ADD_LEVEL4_WAIT_TIME;
        } else if (m_last_cert_database_size <
                   FLOW_CONTROL_MUCH_HIGHER_THRESHOLD) {
          added = FLOW_CONTROL_ADD_LEVEL5_WAIT_TIME;
        } else if (m_last_cert_database_size <
                   FLOW_CONTROL_DANGEROUS_THRESHOLD) {
          added = FLOW_CONTROL_ADD_LEVEL6_WAIT_TIME;
        } else {
          added = FLOW_CONTROL_ADD_LEVEL7_WAIT_TIME;
        }

        m_current_wait_msec = m_current_wait_msec + added;

        if (m_current_wait_msec > m_max_wait_time) {
          m_current_wait_msec = m_max_wait_time;
        }
      }
    }
    mysql_cond_timedwait(&m_flow_control_cond, &m_flow_control_lock, &delay);
    m_leave_counter = m_leave_counter + 1;
    mysql_mutex_unlock(&m_flow_control_lock);
  }

  return 0;
}
