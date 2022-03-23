/* Copyright (c) 2016, 2021, Oracle and/or its affiliates. All rights reserved.
   Copyright (c) 2021, 2022, GreatDB Software Co., Ltd

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
   along with this program; if not, write to the Free Software Foundation,
   51 Franklin Street, Suite 500, Boston, MA 02110-1335 USA */

#include <mysql/group_replication_priv.h>
#include "pipeline_stats.h"
#include "plugin_server_include.h"
#include "plugin_log.h"
#include "plugin.h"

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
const int64 Flow_control_module::MAXTPS= INT_MAX32;
const double Flow_control_module::HOLD_FACTOR= 0.9;
const double Flow_control_module::RELEASE_FACTOR= 1.5;

Pipeline_stats_member_message::Pipeline_stats_member_message(
    int32 transactions_waiting_certification, int32 transactions_waiting_apply,
    int64 transactions_certified, int64 transactions_applied,
    int64 transactions_local)
    : Plugin_gcs_message(CT_PIPELINE_STATS_MEMBER_MESSAGE),
      m_transactions_waiting_certification(transactions_waiting_certification),
      m_transactions_waiting_apply(transactions_waiting_apply),
      m_transactions_certified(transactions_certified),
      m_transactions_applied(transactions_applied),
      m_transactions_local(transactions_local) {}

Pipeline_stats_member_message::Pipeline_stats_member_message(
    const unsigned char *buf, uint64 len)
    : Plugin_gcs_message(CT_PIPELINE_STATS_MEMBER_MESSAGE),
      m_transactions_waiting_certification(0), m_transactions_waiting_apply(0),
      m_transactions_certified(0), m_transactions_applied(0),
      m_transactions_local(0) {
  decode(buf, len);
}

Pipeline_stats_member_message::~Pipeline_stats_member_message()
{}

int32 Pipeline_stats_member_message::get_transactions_waiting_certification() {
  DBUG_ENTER(
      "Pipeline_stats_member_message::get_transactions_waiting_certification");
  DBUG_RETURN(m_transactions_waiting_certification);
}

int64 Pipeline_stats_member_message::get_transactions_certified() {
  DBUG_ENTER("Pipeline_stats_member_message::get_transactions_certified");
  DBUG_RETURN(m_transactions_certified);
}

int32 Pipeline_stats_member_message::get_transactions_waiting_apply() {
  DBUG_ENTER("Pipeline_stats_member_message::get_transactions_waiting_apply");
  DBUG_RETURN(m_transactions_waiting_apply);
}

int64 Pipeline_stats_member_message::get_transactions_applied() {
  DBUG_ENTER("Pipeline_stats_member_message::get_transactions_applied");
  DBUG_RETURN(m_transactions_applied);
}

int64 Pipeline_stats_member_message::get_transactions_local() {
  DBUG_ENTER("Pipeline_stats_member_message::get_transactions_local");
  DBUG_RETURN(m_transactions_local);
}

void
Pipeline_stats_member_message::encode_payload(std::vector<unsigned char> *buffer) const
{
  DBUG_ENTER("Pipeline_stats_member_message::encode_payload");

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

  DBUG_VOID_RETURN;
}

void Pipeline_stats_member_message::decode_payload(const unsigned char *buffer,
                                                   const unsigned char *end) {
  DBUG_ENTER("Pipeline_stats_member_message::decode_payload");
  const unsigned char *slider = buffer;
  uint16 payload_item_type = 0;

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

  DBUG_VOID_RETURN;
}

Pipeline_stats_member_collector::Pipeline_stats_member_collector()
    : m_transactions_waiting_apply(0), m_transactions_certified(0),
      m_transactions_applied(0), m_transactions_local(0) {
  mysql_mutex_init(key_GR_LOCK_pipeline_stats_transactions_waiting_apply,
                   &m_transactions_waiting_apply_lock, MY_MUTEX_INIT_FAST);
}

Pipeline_stats_member_collector::~Pipeline_stats_member_collector()
{
  mysql_mutex_destroy(&m_transactions_waiting_apply_lock);
}

void Pipeline_stats_member_collector::increment_transactions_waiting_apply() {
  mysql_mutex_lock(&m_transactions_waiting_apply_lock);
  assert(my_atomic_load32(&m_transactions_waiting_apply) >= 0);
  my_atomic_add32(&m_transactions_waiting_apply, 1);
  mysql_mutex_unlock(&m_transactions_waiting_apply_lock);
}

void Pipeline_stats_member_collector::decrement_transactions_waiting_apply() {
  mysql_mutex_lock(&m_transactions_waiting_apply_lock);
  if (m_transactions_waiting_apply > 0)
    my_atomic_add32(&m_transactions_waiting_apply, -1);
  assert(my_atomic_load32(&m_transactions_waiting_apply) >= 0);
  mysql_mutex_unlock(&m_transactions_waiting_apply_lock);
}

void Pipeline_stats_member_collector::increment_transactions_certified() {
  my_atomic_add64(&m_transactions_certified, 1);
}

void Pipeline_stats_member_collector::increment_transactions_applied() {
  my_atomic_add64(&m_transactions_applied, 1);
}

void Pipeline_stats_member_collector::increment_transactions_local() {
  my_atomic_add64(&m_transactions_local, 1);
}

int32 Pipeline_stats_member_collector::get_transactions_waiting_apply()
{
  return my_atomic_load32(&m_transactions_waiting_apply);
}

int64 Pipeline_stats_member_collector::get_transactions_certified()
{
  return my_atomic_load64(&m_transactions_certified);
}

int64 Pipeline_stats_member_collector::get_transactions_applied()
{
  return my_atomic_load64(&m_transactions_applied);
}

int64 Pipeline_stats_member_collector::get_transactions_local()
{
  return my_atomic_load64(&m_transactions_local);
}

void Pipeline_stats_member_collector::send_stats_member_message() {
  if (local_member_info == NULL)
    return; /* purecov: inspected */
  Group_member_info::Group_member_status member_status =
      local_member_info->get_recovery_status();
  if (member_status != Group_member_info::MEMBER_ONLINE &&
      member_status != Group_member_info::MEMBER_IN_RECOVERY)
    return;

  Pipeline_stats_member_message message(
      static_cast<int32>(applier_module->get_message_queue_size()),
      my_atomic_load32(&m_transactions_waiting_apply),
      my_atomic_load64(&m_transactions_certified),
      my_atomic_load64(&m_transactions_applied),
      my_atomic_load64(&m_transactions_local));

  enum_gcs_error msg_error = gcs_module->send_message(message, true);
  if (msg_error != GCS_OK) {
    log_message(MY_INFORMATION_LEVEL,
                "Error while sending stats message"); /* purecov: inspected */
  }
}

Pipeline_member_stats::Pipeline_member_stats()
    : m_transactions_waiting_certification(0), m_transactions_waiting_apply(0),
      m_transactions_certified(0), m_delta_transactions_certified(0),
      m_transactions_applied(0), m_delta_transactions_applied(0),
      m_transactions_local(0), m_delta_transactions_local(0),
      m_transactions_negative_certified(0), m_transactions_rows_validating(0),
      m_transactions_committed_all_members(),
      m_transaction_last_conflict_free(), m_stamp(0) {}

Pipeline_member_stats::Pipeline_member_stats(Pipeline_stats_member_message &msg)
    : m_transactions_waiting_certification(
          msg.get_transactions_waiting_certification()),
      m_transactions_waiting_apply(msg.get_transactions_waiting_apply()),
      m_transactions_certified(msg.get_transactions_certified()),
      m_delta_transactions_certified(0),
      m_transactions_applied(msg.get_transactions_applied()),
      m_delta_transactions_applied(0),
      m_transactions_local(msg.get_transactions_local()),
      m_delta_transactions_local(0), m_transactions_negative_certified(0),
      m_transactions_rows_validating(0), m_transactions_committed_all_members(),
      m_transaction_last_conflict_free(), m_stamp(0) {}

Pipeline_member_stats::Pipeline_member_stats(
    Pipeline_stats_member_collector *pipeline_stats, ulonglong applier_queue,
    ulonglong negative_certified, ulonglong certification_size)
    : m_transactions_committed_all_members(),
      m_transaction_last_conflict_free() {
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
  m_stamp = 0;
}

Pipeline_member_stats::~Pipeline_member_stats()
{}

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

  m_stamp = stamp;
}

bool Pipeline_member_stats::is_flow_control_needed() {
  return static_cast<Flow_control_mode>(flow_control_mode_var) == FCM_QUOTA;
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

int64 Pipeline_member_stats::get_transactions_negative_certified()
{
  return m_transactions_negative_certified;
}

int64 Pipeline_member_stats::get_transactions_rows_validating()
{
  return m_transactions_rows_validating;
}

void Pipeline_member_stats::get_transaction_committed_all_members(std::string &value)
{
  value.assign(m_transactions_committed_all_members);
}

void Pipeline_member_stats::set_transaction_committed_all_members(char *str, size_t len)
{
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

int64 Pipeline_member_stats::get_transactions_certified()
{
  return m_transactions_certified;
}

uint64 Pipeline_member_stats::get_stamp() { return m_stamp; }

#ifndef NDEBUG
void Pipeline_member_stats::debug(const char *member, int64 quota_size,
                                  int64 quota_used) {
  log_message(MY_INFORMATION_LEVEL,
              "Flow control - update member stats: "
              "%s stats: certifier_queue %d, applier_queue %d,"
              " certified %ld (%ld), applied %ld (%ld), local %ld (%ld), quota "
              "%ld (%ld)",
              member, m_transactions_waiting_certification,
              m_transactions_waiting_apply, m_transactions_certified,
              m_delta_transactions_certified, m_transactions_applied,
              m_delta_transactions_applied, m_transactions_local,
              m_delta_transactions_local, quota_size,
              quota_used); /* purecov: inspected */
}
#endif

Flow_control_module::Flow_control_module()
    : m_holds_in_period(0), m_quota_used(0), m_quota_size(0), m_stamp(0) {
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
}

Flow_control_module::~Flow_control_module()
{
  mysql_mutex_destroy(&m_flow_control_lock);
  mysql_cond_destroy(&m_flow_control_cond);
}

void Flow_control_module::flow_control_step() {
  if (static_cast<Flow_control_mode>(flow_control_mode_var) == FCM_QUOTA) {
    Certifier_interface *cert_interface =
        (applier_module && applier_module->get_certification_handler())
            ? applier_module->get_certification_handler()->get_certifier()
            : NULL;
    if (cert_interface) {
      ulonglong size = cert_interface->get_certification_info_size();

      double estimated_replay_time =
          cert_interface->get_certification_estimated_replay_time();
      int max_wait_time = flow_control_max_wait_time_var * 1000;

      mysql_mutex_lock(&m_flow_control_lock);
      if (estimated_replay_time < flow_control_replay_lag_behind_var) {
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

int Flow_control_module::handle_stats_data(const uchar *data, uint64 len,
                                           const std::string &member_id) {
  DBUG_ENTER("Flow_control_module::handle_stats_data");
  int error = 0;
  Pipeline_stats_member_message message(data, len);

  /*
     This method is called synchronously by communication layer, so
     we do not need concurrency control.
   */
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
    my_atomic_add32(&m_holds_in_period, 1);
#ifndef NDEBUG
    it->second.debug(it->first.c_str(), my_atomic_load64(&m_quota_size),
                     my_atomic_load64(&m_quota_used));
#endif
  }

  DBUG_RETURN(error);
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
  DBUG_ENTER("Flow_control_module::do_wait");

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

  DBUG_RETURN(0);
}
