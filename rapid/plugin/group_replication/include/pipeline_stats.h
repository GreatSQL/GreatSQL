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

#ifndef PIPELINE_STATS_INCLUDED
#define PIPELINE_STATS_INCLUDED

#include <map>
#include <string>
#include <vector>

#include "gcs_plugin_messages.h"
#include "plugin_psi.h"

#define FLOW_CONTROL_LOWER_THRESHOLD 65536
#define FLOW_CONTROL_LOW_THRESHOLD 131072
#define FLOW_CONTROL_MID_THRESHOLD 1048576
#define FLOW_CONTROL_HIGH_THRESHOLD 10485760
#define FLOW_CONTROL_HIGHER_THRESHOLD 41943040
#define FLOW_CONTROL_MUCH_HIGHER_THRESHOLD 104857600
#define FLOW_CONTROL_DANGEROUS_THRESHOLD 209715200

#define FLOW_CONTROL_MAX_WAIT_TIME 3600000
#define FLOW_CONTROL_ADD_LEVEL7_WAIT_TIME 300000
#define FLOW_CONTROL_ADD_LEVEL6_WAIT_TIME 60000
#define FLOW_CONTROL_ADD_LEVEL5_WAIT_TIME 5000
#define FLOW_CONTROL_ADD_LEVEL4_WAIT_TIME 500
#define FLOW_CONTROL_ADD_LEVEL3_WAIT_TIME 50
#define FLOW_CONTROL_ADD_LEVEL2_WAIT_TIME 20
#define FLOW_CONTROL_ADD_LEVEL1_WAIT_TIME 10

/**
  Flow control modes:
    FCM_DISABLED  flow control disabled
    FCM_QUOTA introduces a delay only on transactions the exceed a quota
*/
enum Flow_control_mode
{
  FCM_DISABLED= 0,
  FCM_QUOTA
};

#define MAX_FLOW_CONTROL_REPLAY_LAG_BEHIND 86400
#define MAX_FLOW_CONTROL_WAIT_TIME 86400

extern ulong flow_control_mode_var;
extern ulong flow_control_replay_lag_behind_var;
extern ulong flow_control_max_wait_time_var;

/**
  Flow control queue threshold for certifier and for applier.
*/
extern int flow_control_certifier_threshold_var;
extern int flow_control_applier_threshold_var;


/**
  @class Pipeline_stats_member_message

  Describes all statistics sent by members.
*/
class Pipeline_stats_member_message : public Plugin_gcs_message
{
public:
  enum enum_payload_item_type
  {
    // This type should not be used anywhere.
    PIT_UNKNOWN= 0,

    // Length of the payload item: 4 bytes
    PIT_TRANSACTIONS_WAITING_CERTIFICATION= 1,

    // Length of the payload item: 4 bytes
    PIT_TRANSACTIONS_WAITING_APPLY= 2,

    // Length of the payload item: 8 bytes
    PIT_TRANSACTIONS_CERTIFIED= 3,

    // Length of the payload item: 8 bytes
    PIT_TRANSACTIONS_APPLIED= 4,

    // Length of the payload item: 8 bytes
    PIT_TRANSACTIONS_LOCAL= 5,

    // No valid type codes can appear after this one.
    PIT_MAX= 6
  };

  /**
    Message constructor

    @param[in] transactions_waiting_certification
    @param[in] transactions_waiting_apply
    @param[in] transactions_certified
    @param[in] transactions_applied
    @param[in] transactions_local
  */
  Pipeline_stats_member_message(int32 transactions_waiting_certification,
                                int32 transactions_waiting_apply,
                                int64 transactions_certified,
                                int64 transactions_applied,
                                int64 transactions_local);

  /**
    Message constructor for raw data

    @param[in] buf raw data
    @param[in] len raw length
  */
  Pipeline_stats_member_message(const unsigned char *buf, uint64 len);

  /**
    Message destructor
   */
  virtual ~Pipeline_stats_member_message();

  /**
    Get transactions waiting certification counter value.

    @return the counter value
  */
  int32 get_transactions_waiting_certification();

  /**
    Get transactions waiting apply counter value.

    @return the counter value
  */
  int32 get_transactions_waiting_apply();

  /**
    Get transactions certified.

    @return the counter value
  */
  int64 get_transactions_certified();

  /**
    Get transactions applied.

    @return the counter value
  */
  int64 get_transactions_applied();

  /**
    Get local transactions that member tried to commmit.

    @return the counter value
  */
  int64 get_transactions_local();

protected:
  /**
    Encodes the message contents for transmission.

    @param[out] buffer   the message buffer to be written
  */
  void encode_payload(std::vector<unsigned char> *buffer) const;

  /**
    Message decoding method

    @param[in] buffer the received data
    @param[in] end    the end of the buffer
  */
  void decode_payload(const unsigned char *buffer, const unsigned char* end);

private:
  int32 m_transactions_waiting_certification;
  int32 m_transactions_waiting_apply;
  int64 m_transactions_certified;
  int64 m_transactions_applied;
  int64 m_transactions_local;
};


/**
  @class Pipeline_stats_member_collector

  The pipeline collector for the local member stats.
*/
class Pipeline_stats_member_collector
{
public:
  /**
    Default constructor.
  */
  Pipeline_stats_member_collector();

  /**
    Destructor.
  */
  virtual ~Pipeline_stats_member_collector();

  /**
    Increment transactions waiting apply counter value.
  */
  void increment_transactions_waiting_apply();

  /**
    Decrement transactions waiting apply counter value.
  */
  void decrement_transactions_waiting_apply();

  /**
    Increment transactions certified counter value.
  */
  void increment_transactions_certified();

  /**
    Increment transactions applied counter value.
  */
  void increment_transactions_applied();

  /**
    Increment local transactions counter value.
  */
  void increment_transactions_local();

  /**
    @returns transactions waiting to be applied.
  */
  int32 get_transactions_waiting_apply();

  /**
    @returns transactions certified.
  */
  int64 get_transactions_certified();

  /**
    @returns transactions applied of local member.
  */
  int64 get_transactions_applied();

  /**
    @returns local transactions proposed by member.
  */
  int64 get_transactions_local();

  /**
    Send member statistics to group.
  */
  void send_stats_member_message();

private:
  int32 m_transactions_waiting_apply;
  int64 m_transactions_certified;
  int64 m_transactions_applied;
  int64 m_transactions_local;
  mysql_mutex_t m_transactions_waiting_apply_lock;
};


/**
  @class Pipeline_member_stats

  Computed statistics per member.
*/
class Pipeline_member_stats
{
public:
  /**
    Default constructor.
  */
  Pipeline_member_stats();

  /**
    Constructor.
  */
  Pipeline_member_stats(Pipeline_stats_member_message &msg);

  /**
    Constructor.
  */
  Pipeline_member_stats(Pipeline_stats_member_collector *pipeline_stats,
                        ulonglong applier_queue, ulonglong negative_certified,
                        ulonglong certification_size);

  /**
    Destructor.
  */
  virtual ~Pipeline_member_stats();

  /**
    Updates member statistics with a new message from the network
  */
  void update_member_stats(Pipeline_stats_member_message &msg,
                           uint64 stamp);

  /**
    Returns true if the node is behind on some user-defined criteria
  */
  bool is_flow_control_needed();

  /**
    Get transactions waiting certification counter value.

    @return the counter value
  */
  int32 get_transactions_waiting_certification();

  /**
    Get transactions waiting apply counter value.

    @return the counter value
  */
  int32 get_transactions_waiting_apply();

  /**
    Get transactions certified counter value.

    @return the counter value
  */
  int64 get_transactions_certified();

  /**
    Get transactions negatively certified.

    @return the counter value
  */
  int64 get_transactions_negative_certified();

  /**
    Get certification database counter value.

    @return the counter value
  */
  int64 get_transactions_rows_validating();

  /**
    Get the stable group transactions.
  */
  void get_transaction_committed_all_members(std::string &value);

  /**
    Set the stable group transactions.
  */
  void set_transaction_committed_all_members(char *str, size_t len);

  /**
    Get the last positive certified transaction.
  */
  void get_transaction_last_conflict_free(std::string &value);

  /**
    Set the last positive certified transaction.
  */
  void set_transaction_last_conflict_free(std::string &value);

  /**
    Get transactions certified since last stats message.

    @return the counter value
  */
  int64 get_delta_transactions_certified();

  /**
    Get transactions applied since last stats message.

    @return the counter value
  */
  int64 get_delta_transactions_applied();

  /**
    Get local transactions that member tried to commmit
    since last stats message.

    @return the counter value
  */
  int64 get_delta_transactions_local();

  /**
    Get the last stats update stamp.

    @return the counter value
  */
  uint64 get_stamp();

#ifndef NDEBUG
  void debug(const char *member, int64 quota_size, int64 quota_used);
#endif

private:
  int32 m_transactions_waiting_certification;
  int32 m_transactions_waiting_apply;
  int64 m_transactions_certified;
  int64 m_delta_transactions_certified;
  int64 m_transactions_applied;
  int64 m_delta_transactions_applied;
  int64 m_transactions_local;
  int64 m_delta_transactions_local;
  int64 m_transactions_negative_certified;
  int64 m_transactions_rows_validating;
  std::string m_transactions_committed_all_members;
  std::string m_transaction_last_conflict_free;
  uint64 m_stamp;
};


/**
  Data type that holds all members stats.
  The key value is the GCS member_id.
*/
typedef std::map<std::string, Pipeline_member_stats>
    Flow_control_module_info;

/**
  @class Flow_control_module

  The pipeline stats aggregator of all group members stats and
  flow control module.
*/
class Flow_control_module
{
public:
  static const int64 MAXTPS;
  static const double HOLD_FACTOR;
  static const double RELEASE_FACTOR;

  /**
    Default constructor.
  */
  Flow_control_module();

  /**
    Destructor.
  */
  virtual ~Flow_control_module();

  /**
    Handles a Pipeline_stats_message, updating the
    Flow_control_module_info and the delay, if needed.

    @param[in] data      the packet data
    @param[in] len       the packet length
    @param[in] member_id the GCS member_id which sent the message

    @return the operation status
      @retval 0      OK
      @retval !=0    Error on queue
  */
  int handle_stats_data(const uchar *data, uint64 len,
                        const std::string& member_id);

  /**
    Evaluate the information received in the last flow control period
    and adjust the system parameters accordingly
  */
  void flow_control_step();

  /**
    Compute and wait the amount of time in microseconds that must
    be elapsed before a new message is sent.
    If there is no need to wait, the method returns immediately.

    @return the wait time
      @retval 0      No wait was done
      @retval >0     The wait time
  */
  int32 do_wait();

private:
  bool check_still_waiting();

private:
  mysql_mutex_t m_flow_control_lock;
  mysql_cond_t  m_flow_control_cond;

  int64 m_wait_counter;
  int64 m_leave_counter;
  ulonglong m_last_cert_database_size;
  int m_current_wait_msec;
  int m_flow_control_need_refreshed;
  int m_flow_control_flag;
  int m_max_wait_time;

  Flow_control_module_info m_info;

  /*
    Number of members that did have waiting transactions on
    certification and/or apply.
  */
  int32 m_holds_in_period;

  /*
   FCM_QUOTA
  */
  int64 m_quota_used;
  int64 m_quota_size;

  /*
    Counter incremented on every flow control step.
  */
  uint64 m_stamp;
};

#endif /* PIPELINE_STATS_INCLUDED */
