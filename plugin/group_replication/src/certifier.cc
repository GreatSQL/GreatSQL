/* Copyright (c) 2014, 2021, Oracle and/or its affiliates. All rights reserved.
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
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include <assert.h>
#include <signal.h>
#include <time.h>
#include <map>

#include <mysql/components/services/log_builtins.h>
#include "my_dbug.h"
#include "my_systime.h"
#include "plugin/group_replication/include/certifier.h"
#include "plugin/group_replication/include/leave_group_on_failure.h"
#include "plugin/group_replication/include/observer_trans.h"
#include "plugin/group_replication/include/plugin.h"
#include "plugin/group_replication/include/sql_service/sql_service_command.h"
#include "plugin/group_replication/libmysqlgcs/include/mysql/gcs/gcs_logging_system.h"

#define MAX_CERT_DELETE_ITEMS 65536

const std::string Certifier::GTID_EXTRACTED_NAME = "gtid_extracted";
const std::string Certifier::CERTIFICATION_INFO_ERROR_NAME =
    "certification_info_error";

static void *launch_broadcast_thread(void *arg) {
  Certifier_broadcast_thread *handler = (Certifier_broadcast_thread *)arg;
  handler->dispatcher();
  return nullptr;
}

Certifier_broadcast_thread::Certifier_broadcast_thread()
    : aborted(false), broadcast_thd_state(), gc_counter(0) {
  mysql_mutex_init(key_GR_LOCK_cert_broadcast_run, &broadcast_run_lock,
                   MY_MUTEX_INIT_FAST);
  mysql_cond_init(key_GR_COND_cert_broadcast_run, &broadcast_run_cond);
  mysql_mutex_init(key_GR_LOCK_cert_broadcast_dispatcher_run,
                   &broadcast_dispatcher_lock, MY_MUTEX_INIT_FAST);
  mysql_cond_init(key_GR_COND_cert_broadcast_dispatcher_run,
                  &broadcast_dispatcher_cond);
  LogPluginErrMsg(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
                  "Init certifier broadcast thread");
}

Certifier_broadcast_thread::~Certifier_broadcast_thread() {
  mysql_mutex_destroy(&broadcast_run_lock);
  mysql_cond_destroy(&broadcast_run_cond);
  mysql_mutex_destroy(&broadcast_dispatcher_lock);
  mysql_cond_destroy(&broadcast_dispatcher_cond);
  LogPluginErrMsg(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
                  "Destroy certifier broadcast thread");
}

int Certifier_broadcast_thread::initialize() {
  DBUG_TRACE;

  mysql_mutex_lock(&broadcast_run_lock);
  if (broadcast_thd_state.is_thread_alive()) {
    mysql_mutex_unlock(&broadcast_run_lock); /* purecov: inspected */
    return 0;                                /* purecov: inspected */
  }

  aborted = false;

  if ((mysql_thread_create(key_GR_THD_cert_broadcast, &broadcast_pthd,
                           get_connection_attrib(), launch_broadcast_thread,
                           (void *)this))) {
    mysql_mutex_unlock(&broadcast_run_lock); /* purecov: inspected */
    return 1;                                /* purecov: inspected */
  }
  broadcast_thd_state.set_created();

  while (broadcast_thd_state.is_alive_not_running()) {
    DBUG_PRINT("sleep", ("Waiting for certifier broadcast thread to start"));
    mysql_cond_wait(&broadcast_run_cond, &broadcast_run_lock);
  }
  mysql_mutex_unlock(&broadcast_run_lock);

  return 0;
}

int Certifier_broadcast_thread::terminate() {
  DBUG_TRACE;

  mysql_mutex_lock(&broadcast_run_lock);
  if (broadcast_thd_state.is_thread_dead()) {
    mysql_mutex_unlock(&broadcast_run_lock);
    return 0;
  }

  aborted = true;
  while (broadcast_thd_state.is_thread_alive()) {
    DBUG_PRINT("loop", ("killing certifier broadcast thread"));
    mysql_mutex_lock(&broadcast_thd->LOCK_thd_data);

    // awake the cycle
    mysql_mutex_lock(&broadcast_dispatcher_lock);
    mysql_cond_broadcast(&broadcast_dispatcher_cond);
    mysql_mutex_unlock(&broadcast_dispatcher_lock);

    broadcast_thd->awake(THD::NOT_KILLED);
    mysql_mutex_unlock(&broadcast_thd->LOCK_thd_data);
    mysql_cond_wait(&broadcast_run_cond, &broadcast_run_lock);
  }
  mysql_mutex_unlock(&broadcast_run_lock);

  return 0;
}

static bool do_io_health_check(const char *directory) {
  File fd;
  size_t written;
  const char *message = "MGR IO CHECK";
  char filename[FN_REFLEN];

  if (my_access(directory, (F_OK | W_OK))) {
    MYSQL_GCS_LOG_ERROR("Error in associated permissions to path '" << directory
                                                                    << "'.");
    return true;
  }

  if ((fd = create_temp_file(filename, directory, "DISK_CHECK",
                             O_CREAT | O_WRONLY, KEEP_FILE, MYF(MY_WME))) < 0) {
    return false;
  }

  written = my_write(fd, (const uchar *)message, strlen(message), MYF(MY_WME));

  bool result = true;
  if (written == MY_FILE_ERROR) {
    result = false;
  }

  (void)my_close(fd, MYF(0));
  (void)my_delete(filename, MYF(MY_WME));

  return result;
}

/**
 * Check if data and binlog's directories are full or not
 */
static bool check_io_health() {
  bool result;
  if (opt_bin_logname) {
    const char slash = '/';
    char binlog_home[FN_REFLEN];
    int length = strlen(opt_bin_logname);
    int i;
    for (i = length; i > 0; --i) {
      if (opt_bin_logname[i] == slash) {
        strncpy(binlog_home, opt_bin_logname, i + 1);
        binlog_home[i + 1] = '\0';
        break;
      }
    }
    if (i > 0) {
      if (strcmp(mysql_real_data_home, binlog_home) != 0) {
        result = do_io_health_check(binlog_home);
        if (!result) {
          return result;
        }
      }
    }
  }

  result = do_io_health_check(mysql_real_data_home);
  return result;
}

void Certifier_broadcast_thread::rest_for_a_while() {
  mysql_mutex_lock(&broadcast_dispatcher_lock);
  struct timespec abstime;
  set_timespec_nsec(&abstime, 1 * 100000000ULL);
  mysql_cond_timedwait(&broadcast_dispatcher_cond, &broadcast_dispatcher_lock,
                       &abstime);
  mysql_mutex_unlock(&broadcast_dispatcher_lock);
}

void Certifier_broadcast_thread::dispatcher() {
  DBUG_TRACE;

  // Thread context operations
  THD *thd = new THD;
  int broadcast_gtid_executed_period = 10;
  int multiplied_sleep_times = 0;
  my_thread_init();
  thd->set_new_thread_id();
  thd->thread_stack = (char *)&thd;
  thd->store_globals();
  global_thd_manager_add_thd(thd);
  broadcast_thd = thd;

  mysql_mutex_lock(&broadcast_run_lock);
  broadcast_thd_state.set_running();
  mysql_cond_broadcast(&broadcast_run_cond);
  mysql_mutex_unlock(&broadcast_run_lock);

  Certifier *certify =
      (Certifier *)applier_module->get_certification_handler()->get_certifier();

  while (!aborted) {
    broadcast_gtid_executed_period =
        get_broadcast_gtid_executed_period_var() / 100;

    if (gc_counter % 10 == 0) {
      /* Do the following for each second */
      applier_module->get_pipeline_stats_member_collector()
          ->set_send_transaction_identifiers();

      /* Do health check for data and binlog directory */
      if (!check_io_health()) {
        aborted = true;
        LogPluginErrMsg(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
                        "io full on data or binlog directory");
        const char *exit_state_action_abort_log_message =
            "io full on data or binlog directory.";
        leave_group_on_failure::mask leave_actions;
        leave_actions.set(leave_group_on_failure::STOP_APPLIER, true);
        leave_actions.set(leave_group_on_failure::HANDLE_EXIT_STATE_ACTION,
                          true);
        /* Leave the Group Replication */
        leave_group_on_failure::leave(
            leave_actions, ER_GRP_RPL_SERVER_SET_TO_READ_ONLY_DUE_TO_ERRORS,
            PSESSION_USE_THREAD, nullptr, exit_state_action_abort_log_message);
      }
    }

    if (gc_counter % broadcast_gtid_executed_period == 0) {
      if (!is_arbitrator_role()) {
        broadcast_gtid_executed();
      }
    }

    if (aborted) {
      break;
    }

    if (multiplied_sleep_times > 0) {
      multiplied_sleep_times--;
    }
    rest_for_a_while();

    if (multiplied_sleep_times == 0) {
      certify->garbage_collect(&multiplied_sleep_times);
    }

    if (!is_arbitrator_role()) {
      if (gc_counter % 10 == 0) {
        applier_module->run_flow_control_step(true);
      } else {
        applier_module->run_flow_control_step(false);
      }
    } else {
      Pipeline_member_stats *pipeline_stats =
          applier_module->get_local_pipeline_stats();
      std::string transaction_committed_all_members;
      pipeline_stats->get_transaction_committed_all_members(
          transaction_committed_all_members);
      if (transaction_committed_all_members.length() > 0) {
        if (add_text_to_gtid_executed(
                transaction_committed_all_members.c_str()) == -1) {
          LogPluginErrMsg(
              ERROR_LEVEL, ER_LOG_PRINTF_MSG,
              "transfer transaction_committed_all_members failed:%s",
              transaction_committed_all_members.c_str());
        }
      }
      /* clean-up */
      delete pipeline_stats;
    }

    gc_counter++;
  }

  Gcs_interface_factory::cleanup_thread_communication_resources(
      Gcs_operations::get_gcs_engine());

  thd->release_resources();
  global_thd_manager_remove_thd(thd);
  delete thd;

  mysql_mutex_lock(&broadcast_run_lock);
  broadcast_thd_state.set_terminated();
  mysql_cond_broadcast(&broadcast_run_cond);
  mysql_mutex_unlock(&broadcast_run_lock);

  my_thread_end();
  my_thread_exit(nullptr);
}

int Certifier_broadcast_thread::broadcast_gtid_executed() {
  DBUG_TRACE;

  /*
    Member may be still joining group so we need to check if:
      1) communication interfaces are ready to be used;
      2) member is ONLINE, that is, distributed recovery is complete.
  */
  if (local_member_info == nullptr) return 0; /* purecov: inspected */
  Group_member_info::Group_member_status member_status =
      local_member_info->get_recovery_status();
  if (member_status != Group_member_info::MEMBER_ONLINE &&
      member_status != Group_member_info::MEMBER_IN_RECOVERY)
    return 0;

  int error = 0;
  uchar *encoded_gtid_executed = nullptr;
  size_t length;
  get_server_encoded_gtid_executed(&encoded_gtid_executed, &length);

  Gtid_Executed_Message gtid_executed_message;
  std::vector<uchar> encoded_gtid_executed_message;
  gtid_executed_message.append_gtid_executed(encoded_gtid_executed, length);

  enum enum_gcs_error send_err =
      gcs_module->send_message(gtid_executed_message, true);
  if (send_err == GCS_MESSAGE_TOO_BIG) {
    LogPluginErr(
        ERROR_LEVEL,
        ER_GRP_RPL_BROADCAST_COMMIT_MSSG_TOO_BIG); /* purecov: inspected */
    error = 1;                                     /* purecov: inspected */
  } else if (send_err == GCS_NOK) {
    LogPluginErr(
        INFORMATION_LEVEL,
        ER_GRP_RPL_BROADCAST_COMMIT_TRANS_MSSG_FAILED); /* purecov: inspected */
    error = 1;                                          /* purecov: inspected */
  }

#if !defined(NDEBUG)
  char *encoded_gtid_executed_string =
      encoded_gtid_set_to_string(encoded_gtid_executed, length);
  DBUG_PRINT("info", ("Certifier broadcast executed_set: %s",
                      encoded_gtid_executed_string));
  my_free(encoded_gtid_executed_string);
#endif

  my_free(encoded_gtid_executed);
  return error;
}

Certifier::Certifier()
    : initialized(false),
      positive_cert(0),
      negative_cert(0),
      parallel_applier_last_committed_global(1),
      parallel_applier_sequence_number(2),
      certifying_already_applied_transactions(false),
      gtid_assignment_block_size(1),
      gtids_assigned_in_blocks_counter(1),
      conflict_detection_enable(!local_member_info->in_primary_mode()),
      single_primary_fast_mode(
          local_member_info->in_single_primary_fast_mode()) {
  last_conflict_free_transaction.clear();

#if !defined(NDEBUG)
  certifier_garbage_collection_block = false;
  /*
    Debug flag to block the garbage collection and discard incoming stable
    set messages while garbage collection is on going.
  */
  DBUG_EXECUTE_IF("certifier_garbage_collection_block",
                  certifier_garbage_collection_block = true;);

  same_member_message_discarded = false;
  /*
    Debug flag to check for similar member sending multiple messages.
  */
  DBUG_EXECUTE_IF("certifier_inject_duplicate_certifier_data_message",
                  same_member_message_discarded = true;);
#endif

  certification_info_sid_map = new Sid_map(nullptr);
  incoming = new Synchronized_queue<Data_packet *>();
  timestamp_counter = 0;
  last_delete_usec = -1;
  last_add_usec = -1;
  last_add_velocity = 0;
  last_replay_velocity = 0;
  last_delete_items = 0;
  estimated_replay_time = 0;
  last_cert_size = 0;

  if (is_arbitrator_role()) {
    conflict_detection_enable = false;
  }

  stable_gtid_set_lock = new Checkable_rwlock(
#ifdef HAVE_PSI_INTERFACE
      key_GR_RWLOCK_cert_stable_gtid_set
#endif
  );
  stable_sid_map = new Sid_map(stable_gtid_set_lock);
  stable_gtid_set = new Gtid_set(stable_sid_map, stable_gtid_set_lock);
  broadcast_thread = new Certifier_broadcast_thread();

  group_gtid_sid_map = new Sid_map(nullptr);
  group_gtid_executed = new Gtid_set(group_gtid_sid_map, nullptr);
  group_gtid_extracted = new Gtid_set(group_gtid_sid_map, nullptr);

  last_local_gtid.clear();

  mysql_mutex_init(key_GR_LOCK_certification_info, &LOCK_certification_info,
                   MY_MUTEX_INIT_FAST);
  mysql_mutex_init(key_GR_LOCK_cert_members, &LOCK_members, MY_MUTEX_INIT_FAST);
}

Certifier::~Certifier() {
  clear_certification_info();
  delete certification_info_sid_map;

  delete stable_gtid_set;
  delete stable_sid_map;
  delete stable_gtid_set_lock;
  delete broadcast_thread;
  delete group_gtid_executed;
  delete group_gtid_extracted;
  delete group_gtid_sid_map;

  clear_incoming();
  delete incoming;

  clear_members();
  mysql_mutex_destroy(&LOCK_certification_info);
  mysql_mutex_destroy(&LOCK_members);
}

int Certifier::initialize_server_gtid_set(bool get_server_gtid_retrieved) {
  DBUG_TRACE;
  mysql_mutex_assert_owner(&LOCK_certification_info);
  int error = 0;
  Sql_service_command_interface *sql_command_interface = nullptr;
  std::string gtid_executed;
  std::string applier_retrieved_gtids;

  rpl_sid group_sid;
  const char *group_name = get_group_name_var();
  if (group_sid.parse(group_name, strlen(group_name)) != RETURN_STATUS_OK) {
    LogPluginErr(ERROR_LEVEL,
                 ER_GRP_RPL_GROUP_NAME_PARSE_ERROR); /* purecov: inspected */
    error = 1;                                       /* purecov: inspected */
    goto end;                                        /* purecov: inspected */
  }

  group_gtid_sid_map_group_sidno = group_gtid_sid_map->add_sid(group_sid);
  if (group_gtid_sid_map_group_sidno < 0) {
    LogPluginErr(
        ERROR_LEVEL,
        ER_GRP_RPL_ADD_GRPSID_TO_GRPGTIDSID_MAP_ERROR); /* purecov: inspected */
    error = 1;                                          /* purecov: inspected */
    goto end;                                           /* purecov: inspected */
  }

  if (group_gtid_executed->ensure_sidno(group_gtid_sid_map_group_sidno) !=
      RETURN_STATUS_OK) {
    LogPluginErr(
        ERROR_LEVEL,
        ER_GRP_RPL_UPDATE_GRPGTID_EXECUTED_ERROR); /* purecov: inspected */
    error = 1;                                     /* purecov: inspected */
    goto end;                                      /* purecov: inspected */
  }

  if (group_gtid_extracted->ensure_sidno(group_gtid_sid_map_group_sidno) !=
      RETURN_STATUS_OK) {
    LogPluginErr(ERROR_LEVEL,
                 ER_GRP_RPL_DONOR_TRANS_INFO_ERROR); /* purecov: inspected */
    error = 1;                                       /* purecov: inspected */
    goto end;                                        /* purecov: inspected */
  }

  sql_command_interface = new Sql_service_command_interface();
  if (sql_command_interface->establish_session_connection(PSESSION_USE_THREAD,
                                                          GROUPREPL_USER)) {
    LogPluginErr(ERROR_LEVEL,
                 ER_GRP_RPL_SERVER_CONN_ERROR); /* purecov: inspected */
    error = 1;                                  /* purecov: inspected */
    goto end;                                   /* purecov: inspected */
  }

  error = sql_command_interface->get_server_gtid_executed(gtid_executed);
  DBUG_EXECUTE_IF("gr_server_gtid_executed_extraction_error", error = 1;);
  if (error) {
    LogPluginErr(WARNING_LEVEL, ER_GRP_RPL_ERROR_FETCHING_GTID_EXECUTED_SET);
    goto end;
  }

  if (group_gtid_executed->add_gtid_text(gtid_executed.c_str()) !=
      RETURN_STATUS_OK) {
    LogPluginErr(
        ERROR_LEVEL,
        ER_GRP_RPL_ADD_GTID_TO_GRPGTID_EXECUTED_ERROR); /* purecov: inspected */
    error = 1;                                          /* purecov: inspected */
    goto end;                                           /* purecov: inspected */
  }

  if (get_server_gtid_retrieved) {
    Replication_thread_api applier_channel("group_replication_applier");
    if (applier_channel.get_retrieved_gtid_set(applier_retrieved_gtids)) {
      LogPluginErr(WARNING_LEVEL,
                   ER_GRP_RPL_ERROR_FETCHING_GTID_SET); /* purecov: inspected */
      error = 1;                                        /* purecov: inspected */
      goto end;                                         /* purecov: inspected */
    }

    if (group_gtid_executed->add_gtid_text(applier_retrieved_gtids.c_str()) !=
        RETURN_STATUS_OK) {
      LogPluginErr(
          ERROR_LEVEL,
          ER_GRP_RPL_ADD_RETRIEVED_SET_TO_GRP_GTID_EXECUTED_ERROR); /* purecov:
                                                                       inspected
                                                                     */
      error = 1; /* purecov: inspected */
      goto end;  /* purecov: inspected */
    }
  }

  compute_group_available_gtid_intervals();

end:
  delete sql_command_interface;

  return error;
}

void Certifier::compute_group_available_gtid_intervals() {
  DBUG_TRACE;
  mysql_mutex_assert_owner(&LOCK_certification_info);

  gtids_assigned_in_blocks_counter = 1;
  member_gtids.clear();
  group_available_gtid_intervals.clear();

  /*
    Compute the GTID intervals that are available by inverting the
    group_gtid_executed or group_gtid_extracted intervals.
  */
  Gtid_set::Const_interval_iterator ivit(certifying_already_applied_transactions
                                             ? group_gtid_extracted
                                             : group_gtid_executed,
                                         group_gtid_sid_map_group_sidno);
#ifndef NDEBUG
  if (certifying_already_applied_transactions)
    DBUG_PRINT(
        "Certifier::compute_group_available_gtid_intervals()",
        ("Generating group transaction intervals from group_gtid_extracted"));
#endif

  const Gtid_set::Interval *iv = nullptr, *iv_next = nullptr;

  // The fist interval: UUID:100 -> we have the interval 1-99
  if ((iv = ivit.get()) != nullptr) {
    if (iv->start > 1) {
      Gtid_set::Interval interval = {1, iv->start - 1, nullptr};
      group_available_gtid_intervals.push_back(interval);
    }
  }

  // For each used interval find the upper bound and from there
  // add the free GTIDs up to the next interval or MAX_GNO.
  while ((iv = ivit.get()) != nullptr) {
    ivit.next();
    iv_next = ivit.get();

    rpl_gno start = iv->end;
    rpl_gno end = MAX_GNO;
    if (iv_next != nullptr) end = iv_next->start - 1;

    assert(start <= end);
    Gtid_set::Interval interval = {start, end, nullptr};
    group_available_gtid_intervals.push_back(interval);
  }

  // No GTIDs used, so the available interval is the complete set.
  if (group_available_gtid_intervals.size() == 0) {
    Gtid_set::Interval interval = {1, MAX_GNO, nullptr};
    group_available_gtid_intervals.push_back(interval);
  }
}

Gtid_set::Interval Certifier::reserve_gtid_block(longlong block_size) {
  DBUG_TRACE;
  assert(block_size > 1);
  mysql_mutex_assert_owner(&LOCK_certification_info);

  Gtid_set::Interval result;

  // We are out of intervals, we need to force intervals computation.
  if (group_available_gtid_intervals.size() == 0)
    compute_group_available_gtid_intervals();

  std::list<Gtid_set::Interval>::iterator it =
      group_available_gtid_intervals.begin();
  assert(it != group_available_gtid_intervals.end());

  /*
    We always have one or more intervals, the only thing to check
    is if the first interval is exhausted, if so we need to purge
    it to avoid future use.
  */
  if (block_size > it->end - it->start) {
    result = *it;
    group_available_gtid_intervals.erase(it);
  } else {
    result.start = it->start;
    result.end = it->start + block_size - 1;
    it->start = result.end + 1;
    assert(result.start <= result.end);
    assert(result.start < it->start);
  }

  return result;
}

void Certifier::add_to_group_gtid_executed_internal(rpl_sidno sidno,
                                                    rpl_gno gno, bool local) {
  DBUG_TRACE;
  mysql_mutex_assert_owner(&LOCK_certification_info);
  group_gtid_executed->_add_gtid(sidno, gno);
  if (local) {
    assert(sidno > 0 && gno > 0);
    last_local_gtid.set(sidno, gno);
  }
  /*
    We only need to track certified transactions on
    group_gtid_extracted while:
     1) certifier is handling already applied transactions
        on distributed recovery procedure;
     2) the transaction does have a group GTID.
  */
  if (certifying_already_applied_transactions &&
      sidno == group_gtid_sid_map_group_sidno)
    group_gtid_extracted->_add_gtid(sidno, gno);
}

void Certifier::clear_certification_info() {
  certification_index.clear();
  for (Certification_info::iterator it = certification_info.begin();
       it != certification_info.end(); ++it) {
    // We can only delete the last reference.
    if (it->second.gtid_ref->unlink() == 0) delete it->second.gtid_ref;
  }

  certification_info.clear();
}

void Certifier::clear_incoming() {
  DBUG_TRACE;
  while (!this->incoming->empty()) {
    Data_packet *packet = nullptr;
    this->incoming->pop(&packet);
    delete packet;
  }
}

void Certifier::clear_members() {
  DBUG_TRACE;
  mysql_mutex_lock(&LOCK_members);
  members.clear();
  mysql_mutex_unlock(&LOCK_members);
}

int Certifier::initialize(ulonglong gtid_assignment_block_size) {
  DBUG_TRACE;
  int error = 0;
  mysql_mutex_lock(&LOCK_certification_info);

  if (is_initialized()) {
    error = 1; /* purecov: inspected */
    goto end;  /* purecov: inspected */
  }

  assert(gtid_assignment_block_size >= 1);
  this->gtid_assignment_block_size = gtid_assignment_block_size;

  /*
    We need to initialize group_gtid_executed from both GTID_EXECUTED
    and applier retrieved GTID set to consider the already certified
    but not yet applied GTIDs, that may exist on applier relay log when
    this member is the one bootstrapping the group.
  */
  if (initialize_server_gtid_set(true)) {
    LogPluginErr(ERROR_LEVEL, ER_GRP_RPL_CERTIFICATION_INITIALIZATION_FAILURE);
    error = 1;
    goto end;
  }

  error = broadcast_thread->initialize();
  initialized = !error;

end:
  mysql_mutex_unlock(&LOCK_certification_info);
  return error;
}

int Certifier::terminate() {
  DBUG_TRACE;
  int error = 0;

  if (is_initialized()) error = broadcast_thread->terminate();

  return error;
}

void Certifier::increment_parallel_applier_sequence_number(
    bool update_parallel_applier_last_committed_global) {
  DBUG_TRACE;
  mysql_mutex_assert_owner(&LOCK_certification_info);

  assert(parallel_applier_last_committed_global <
         parallel_applier_sequence_number);
  if (update_parallel_applier_last_committed_global)
    parallel_applier_last_committed_global = parallel_applier_sequence_number;

  parallel_applier_sequence_number++;
}

rpl_gno Certifier::certify(Gtid_set *snapshot_version,
                           std::list<const char *> *write_set,
                           bool generate_group_id, const char *member_uuid,
                           Gtid_log_event *gle, bool local_transaction) {
  DBUG_TRACE;
  rpl_gno result = 0;
  const bool has_write_set = !write_set->empty();

  if (!is_initialized()) {
    LogPluginErr(INFORMATION_LEVEL, ER_GRP_RPL_CERTIFY_IS_INITIALIZED);
    return -1;
  }

  mysql_mutex_lock(&LOCK_certification_info);
  int64 transaction_last_committed = parallel_applier_last_committed_global;

  DBUG_EXECUTE_IF("certifier_force_1_negative_certification", {
    DBUG_SET("-d,certifier_force_1_negative_certification");
    goto end;
  });

  if (conflict_detection_enable) {
    for (std::list<const char *>::iterator it = write_set->begin();
         it != write_set->end(); ++it) {
      Gtid_set *certified_write_set_snapshot_version =
          get_certified_write_set_snapshot_version(*it);

      /*
        If the previous certified transaction snapshot version is not
        a subset of the incoming transaction snapshot version, the current
        transaction was executed on top of outdated data, so it will be
        negatively certified. Otherwise, this transaction is marked
        certified and goes into applier.
      */
      if (certified_write_set_snapshot_version != nullptr &&
          !certified_write_set_snapshot_version->is_subset(snapshot_version))
        goto end;
    }
  }

  if (certifying_already_applied_transactions &&
      !group_gtid_extracted->is_subset_not_equals(group_gtid_executed)) {
    certifying_already_applied_transactions = false;

#ifndef NDEBUG
    char *group_gtid_executed_string = nullptr;
    char *group_gtid_extracted_string = nullptr;
    group_gtid_executed->to_string(&group_gtid_executed_string, true);
    group_gtid_extracted->to_string(&group_gtid_extracted_string, true);
    DBUG_PRINT(
        "Certifier::certify()",
        ("Set certifying_already_applied_transactions to false. "
         "group_gtid_executed: \"%s\"; group_gtid_extracted_string: \"%s\"",
         group_gtid_executed_string, group_gtid_extracted_string));
    my_free(group_gtid_executed_string);
    my_free(group_gtid_extracted_string);
#endif
  }

  /*
    If the current transaction doesn't have a specified GTID, one
    for group UUID will be generated.
    This situation happens when transactions are executed with
    GTID_NEXT equal to AUTOMATIC_GTID (the default case).
  */
  if (generate_group_id) {
    /*
      We need to ensure that group sidno does exist on snapshot
      version due to the following scenario:
        1) Member joins the group.
        2) Goes through recovery procedure, view change is queued to
           apply, member is marked ONLINE. This requires
             --group_replication_recovery_complete_at=TRANSACTIONS_CERTIFIED
           to happen.
        3) Despite the view change log event is still being applied,
           since the member is already ONLINE it can execute
           transactions. The first transaction from this member will
           not include any group GTID, since no group transaction is
           yet applied.
        4) As a result of this sequence snapshot_version will not
           contain any group GTID and the below instruction
             snapshot_version->_add_gtid(group_sidno, result);
           would fail because of that.
    */
    if (snapshot_version->ensure_sidno(get_group_sidno()) != RETURN_STATUS_OK) {
      LogPluginErr(
          ERROR_LEVEL,
          ER_GRP_RPL_UPDATE_TRANS_SNAPSHOT_VER_ERROR); /* purecov: inspected */
      goto end;                                        /* purecov: inspected */
    }

    result = get_group_next_available_gtid(member_uuid);
    if (result < 0) {
      LogPluginErr(INFORMATION_LEVEL, ER_GRP_RPL_CERTIFY_GET_NEXT_GTID);
      goto end;
    }

    /*
      Add generated transaction GTID to transaction snapshot version.
    */
    snapshot_version->_add_gtid(get_group_sidno(), result);

    if (is_arbitrator_role()) {
      goto end;
    }

    /*
      Store last conflict free transaction identification.
      sidno must be relative to group_gtid_sid_map.
    */
    last_conflict_free_transaction.set(group_gtid_sid_map_group_sidno, result);

    DBUG_PRINT("info", ("Group replication Certifier: generated transaction "
                        "identifier: %llu",
                        result));
  } else {
    /*
      Check if it is an already used GTID
    */
    rpl_sidno sidno_for_group_gtid_sid_map = gle->get_sidno(group_gtid_sid_map);
    if (sidno_for_group_gtid_sid_map < 1) {
      LogPluginErr(ERROR_LEVEL,
                   ER_GRP_RPL_SIDNO_FETCH_ERROR); /* purecov: inspected */
      goto end;                                   /* purecov: inspected */
    }
    if (group_gtid_executed->contains_gtid(sidno_for_group_gtid_sid_map,
                                           gle->get_gno())) {
      char buf[rpl_sid::TEXT_LENGTH + 1];
      gle->get_sid()->to_string(buf);

      LogPluginErr(ERROR_LEVEL, ER_GRP_RPL_GTID_ALREADY_USED, buf,
                   gle->get_gno());
      goto end;
    }
    /*
      Add received transaction GTID to transaction snapshot version.
    */
    rpl_sidno sidno = gle->get_sidno(snapshot_version->get_sid_map());
    if (sidno < 1) {
      LogPluginErr(ERROR_LEVEL,
                   ER_GRP_RPL_SIDNO_FETCH_ERROR); /* purecov: inspected */
      goto end;                                   /* purecov: inspected */
    }

    if (snapshot_version->ensure_sidno(sidno) != RETURN_STATUS_OK) {
      LogPluginErr(
          ERROR_LEVEL,
          ER_GRP_RPL_UPDATE_TRANS_SNAPSHOT_VER_ERROR); /* purecov: inspected */
      goto end;                                        /* purecov: inspected */
    }
    snapshot_version->_add_gtid(sidno, gle->get_gno());

    /*
      Store last conflict free transaction identification.
      sidno must be relative to group_gtid_sid_map.
    */
    rpl_sidno last_conflict_free_transaction_sidno =
        gle->get_sidno(group_gtid_sid_map);
    if (last_conflict_free_transaction_sidno < 1) {
      LogPluginErr(
          WARNING_LEVEL,
          ER_GRP_RPL_UPDATE_LAST_CONFLICT_FREE_TRANS_ERROR); /* purecov:
                                                                inspected */
    } else {
      last_conflict_free_transaction.set(last_conflict_free_transaction_sidno,
                                         gle->get_gno());
    }

    result = 1;
    DBUG_PRINT(
        "info",
        ("Group replication Certifier: there was no transaction identifier "
         "generated since transaction already had a GTID specified"));
  }

  /*
    Add the transaction's write set to certification info.
  */
  if (has_write_set &&
      single_primary_fast_mode != MGR_FAST_MODE_WITHOUT_PARALLEL_REPLAY) {
    if (!single_primary_fast_mode || !local_transaction) {
      // Only consider remote transactions for parallel applier indexes.
      int64 transaction_sequence_number =
          local_transaction ? -1 : parallel_applier_sequence_number;
      Gtid_set_ref *snapshot_version_value = new Gtid_set_ref(
          certification_info_sid_map, transaction_sequence_number);
      if (snapshot_version_value->add_gtid_set(snapshot_version) !=
          RETURN_STATUS_OK) {
        result = 0;                    /* purecov: inspected */
        delete snapshot_version_value; /* purecov: inspected */
        LogPluginErr(
            ERROR_LEVEL,
            ER_GRP_RPL_UPDATE_TRANS_SNAPSHOT_REF_VER_ERROR); /* purecov:
                                                              * inspected
                                                              */
        goto end; /* purecov: inspected */
      }

      for (std::list<const char *>::iterator it = write_set->begin();
           it != write_set->end(); ++it) {
        int64 item_previous_sequence_number = -1;

        add_item(*it, snapshot_version_value, &item_previous_sequence_number);

        /*
           Exclude previous sequence number that are smaller than global
           last committed and that are the current sequence number.
           transaction_last_committed is initialized with
           parallel_applier_last_committed_global on the beginning of
           this method.
         */
        if (item_previous_sequence_number > transaction_last_committed &&
            item_previous_sequence_number != parallel_applier_sequence_number)
          transaction_last_committed = item_previous_sequence_number;
      }
    }
  }

  /*
    Update parallel applier indexes.
  */
  if (!local_transaction) {
    /*
      'CREATE TABLE ... AS SELECT' is considered a DML, though in reality it
      is DDL + DML, which write-sets do not capture all dependencies.
      It is flagged through gle->last_committed and gle->sequence_number so
      that it is only executed on parallel applier after all precedent
      transactions like any other DDL.
    */
    bool update_parallel_applier_last_committed_global = false;
    if (0 == gle->last_committed && 0 == gle->sequence_number) {
      update_parallel_applier_last_committed_global = true;
    } else if (single_primary_fast_mode ==
               MGR_FAST_MODE_WITHOUT_PARALLEL_REPLAY) {
      update_parallel_applier_last_committed_global = true;
    }

    if (!has_write_set || update_parallel_applier_last_committed_global) {
      /*
        DDL does not have write-set, so we need to ensure that it
        is applied without any other transaction in parallel.
      */
      transaction_last_committed = parallel_applier_sequence_number - 1;
    }

    gle->last_committed = transaction_last_committed;
    gle->sequence_number = parallel_applier_sequence_number;
    assert(gle->last_committed >= 0);
    assert(gle->sequence_number > 0);
    assert(gle->last_committed < gle->sequence_number);

    increment_parallel_applier_sequence_number(
        !has_write_set || update_parallel_applier_last_committed_global);
  }

end:
  update_certified_transaction_count(result > 0, local_transaction);

  mysql_mutex_unlock(&LOCK_certification_info);
  DBUG_PRINT("info", ("Group replication Certifier: certification result: %llu",
                      result));
  return result;
}

int Certifier::add_specified_gtid_to_group_gtid_executed(Gtid_log_event *gle,
                                                         bool local) {
  DBUG_TRACE;

  mysql_mutex_lock(&LOCK_certification_info);
  rpl_sidno sidno = gle->get_sidno(group_gtid_sid_map);

  if (sidno < 1) {
    LogPluginErr(ERROR_LEVEL,
                 ER_GRP_RPL_FETCH_TRANS_SIDNO_ERROR); /* purecov: inspected */
    mysql_mutex_unlock(&LOCK_certification_info);     /* purecov: inspected */
    return 1;                                         /* purecov: inspected */
  }

  if (group_gtid_executed->ensure_sidno(sidno) != RETURN_STATUS_OK) {
    LogPluginErr(ERROR_LEVEL,
                 ER_GRP_RPL_ERROR_VERIFYING_SIDNO); /* purecov: inspected */
    mysql_mutex_unlock(&LOCK_certification_info);   /* purecov: inspected */
    return 1;                                       /* purecov: inspected */
  }

  add_to_group_gtid_executed_internal(sidno, gle->get_gno(), local);

  mysql_mutex_unlock(&LOCK_certification_info);
  return 0;
}

int Certifier::add_group_gtid_to_group_gtid_executed(rpl_gno gno, bool local) {
  DBUG_TRACE;
  mysql_mutex_lock(&LOCK_certification_info);
  add_to_group_gtid_executed_internal(group_gtid_sid_map_group_sidno, gno,
                                      local);
  mysql_mutex_unlock(&LOCK_certification_info);
  return 0;
}

/*
  This method will return the next GNO for the current transaction, it
  will work with two behaviours:

  1) member_uuid == NULL || gtid_assignment_block_size <= 1
     View_change_log_events creation does call this method with
     member_uuid set to NULL to force it to be created with the
     first available GNO of the group. This will ensure that all
     members do use the same GNO for it.
     After a View_change_log_event is created we recompute available
     GNOs to ensure that all members do have the same available GNOs
     set.
     This branch is also used when gtid_assignment_block_size is
     set to 1, meaning that GNO will be assigned sequentially
     according with certification order.

  2) On the second branch we assign GNOs according to intervals
     assigned to each member.
     To avoid having eternal gaps when a member do use all of its
     assigned GNOs, periodically we recompute the intervals, this
     will make that GNOs available to other members.
     The GNO is generated within the interval of available GNOs for
     a given member.
     When a member exhaust its assigned GNOs we reserve more for it
     from the available GNOs set.
*/
rpl_gno Certifier::get_group_next_available_gtid(const char *member_uuid) {
  DBUG_TRACE;
  mysql_mutex_assert_owner(&LOCK_certification_info);
  rpl_gno result = 0;

  if (member_uuid == nullptr || gtid_assignment_block_size <= 1) {
    result = get_group_next_available_gtid_candidate(1, MAX_GNO);
    if (result < 0) {
      assert(result == -1);
      return result;
    }

    /*
      If we did log a view change event we need to recompute
      intervals, so that all members start from the same
      intervals.
    */
    if (member_uuid == nullptr && gtid_assignment_block_size > 1)
      compute_group_available_gtid_intervals();
  } else {
    /*
      After a number of rounds equal to block size the blocks are
      collected back so that the GTID holes can be filled up by
      following transactions from other members.
    */
    if (gtids_assigned_in_blocks_counter % (gtid_assignment_block_size + 1) ==
        0)
      compute_group_available_gtid_intervals();

    /*
      GTID is assigned in blocks to each member and are consumed
      from that block unless a new block is needed.
    */
    std::string member(member_uuid);
    std::map<std::string, Gtid_set::Interval>::iterator it =
        member_gtids.find(member);

    if (it == member_gtids.end()) {
      // There is no block assigned to this member so get one.
      std::pair<std::map<std::string, Gtid_set::Interval>::iterator, bool>
          insert_ret;
      std::pair<std::string, Gtid_set::Interval> member_pair(
          member, reserve_gtid_block(gtid_assignment_block_size));
      insert_ret = member_gtids.insert(member_pair);
      assert(insert_ret.second == true);
      it = insert_ret.first;
    }

    result = get_group_next_available_gtid_candidate(it->second.start,
                                                     it->second.end);
    while (result == -2) {
      // Block has no available GTIDs, reserve more.
      it->second = reserve_gtid_block(gtid_assignment_block_size);
      result = get_group_next_available_gtid_candidate(it->second.start,
                                                       it->second.end);
    }
    if (result < 0) return result;

    it->second.start = result;
    gtids_assigned_in_blocks_counter++;
  }

  assert(result > 0);
  return result;
}

rpl_gno Certifier::get_group_next_available_gtid_candidate(rpl_gno start,
                                                           rpl_gno end) const {
  DBUG_TRACE;
  assert(start > 0);
  assert(start <= end);
  mysql_mutex_assert_owner(&LOCK_certification_info);

  rpl_gno candidate = start;
  Gtid_set::Const_interval_iterator ivit(certifying_already_applied_transactions
                                             ? group_gtid_extracted
                                             : group_gtid_executed,
                                         group_gtid_sid_map_group_sidno);
#ifndef NDEBUG
  if (certifying_already_applied_transactions)
    DBUG_PRINT("Certifier::get_group_next_available_gtid_candidate()",
               ("Generating group transaction id from group_gtid_extracted"));
#endif

  /*
    Walk through available intervals until we find the correct one
    or return GNO exhausted error.
  */
  while (true) {
    assert(candidate >= start);
    const Gtid_set::Interval *iv = ivit.get();
    rpl_gno next_interval_start = iv != nullptr ? iv->start : MAX_GNO;

    // Correct interval.
    if (candidate < next_interval_start) {
      if (candidate <= end)
        return candidate;
      else
        return -2;
    }

    if (iv == nullptr) {
      LogPluginErr(ERROR_LEVEL, ER_GRP_RPL_CANT_GENERATE_GTID);
      return -1;
    }

    candidate = std::max(candidate, iv->end);
    ivit.next();
  }
}

bool Certifier::add_item(const char *item, Gtid_set_ref *snapshot_version,
                         int64 *item_previous_sequence_number) {
  DBUG_TRACE;
  mysql_mutex_assert_owner(&LOCK_certification_info);
  bool error = true;
  Cert_basic_info info;
  std::string key(item);
  Certification_info::iterator it = certification_info.find(key);
  snapshot_version->link();

  if (it == certification_info.end()) {
    info.gtid_ref = snapshot_version;
    info.recorded_timestamp = timestamp_counter;
    std::pair<Certification_info::iterator, bool> ret =
        certification_info.insert(
            std::pair<std::string, Cert_basic_info>(key, info));
    certification_index[timestamp_counter] = key;
    timestamp_counter = timestamp_counter + 1;
    error = !ret.second;
  } else {
    *item_previous_sequence_number =
        it->second.gtid_ref->get_parallel_applier_sequence_number();

    uint64 orig_timestamp = it->second.recorded_timestamp;
    auto search = certification_index.find(orig_timestamp);
    if (search == certification_index.end()) {
      LogPluginErrMsg(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
                      "not found, orig_timestamp:%lu", orig_timestamp);
    } else {
      certification_index.erase(search);
    }
    it->second.recorded_timestamp = timestamp_counter;
    certification_index[timestamp_counter] = key;
    timestamp_counter = timestamp_counter + 1;

    if (it->second.gtid_ref->unlink() == 0) delete it->second.gtid_ref;

    it->second.gtid_ref = snapshot_version;
    error = false;
  }

  return error;
}

Gtid_set *Certifier::get_certified_write_set_snapshot_version(
    const char *item) {
  DBUG_TRACE;
  mysql_mutex_assert_owner(&LOCK_certification_info);

  if (!is_initialized()) return nullptr; /* purecov: inspected */

  Certification_info::iterator it;
  std::string item_str(item);

  it = certification_info.find(item_str);

  if (it == certification_info.end())
    return nullptr;
  else
    return it->second.gtid_ref;
}

int Certifier::get_group_stable_transactions_set_string(char **buffer,
                                                        size_t *length) {
  DBUG_TRACE;
  int error = 1;

  /*
    Stable transactions set may not be accurate during recovery,
    thence we do not externalize it on
    performance_schema.replication_group_member_stats table.
  */
  if (local_member_info->get_recovery_status() ==
      Group_member_info::MEMBER_IN_RECOVERY) {
    return 0;
  }

  char *m_buffer = nullptr;
  int m_length = stable_gtid_set->to_string(&m_buffer, true);
  if (m_length >= 0) {
    *buffer = m_buffer;
    *length = static_cast<size_t>(m_length);
    error = 0;
  } else
    my_free(m_buffer); /* purecov: inspected */

  return error;
}

bool Certifier::set_group_stable_transactions_set(Gtid_set *executed_gtid_set) {
  DBUG_TRACE;

  if (!is_initialized()) return true; /* purecov: inspected */

  if (executed_gtid_set == nullptr) {
    LogPluginErr(ERROR_LEVEL,
                 ER_GRP_RPL_INVALID_GTID_SET); /* purecov: inspected */
    return true;                               /* purecov: inspected */
  }

  stable_gtid_set_lock->wrlock();
  if (stable_gtid_set->add_gtid_set(executed_gtid_set) != RETURN_STATUS_OK) {
    stable_gtid_set_lock->unlock(); /* purecov: inspected */
    LogPluginErr(ERROR_LEVEL,
                 ER_GRP_RPL_UPDATE_GTID_SET_ERROR); /* purecov: inspected */
    return true;                                    /* purecov: inspected */
  }
  stable_gtid_set_lock->unlock();

  /*
    Applier channel received set does only contain the GTIDs of the
    remote (committed by other members) transactions. On the long
    term, the gaps may create performance issues on the received
    set update. To avoid that, periodically, we update the received
    set with the full set of transactions committed on the group,
    closing the gaps.
  */
  if (channel_add_executed_gtids_to_received_gtids(
          applier_module_channel_name)) {
    LogPluginErr(
        WARNING_LEVEL,
        ER_GRP_RPL_RECEIVED_SET_MISSING_GTIDS); /* purecov: inspected */
  }

  return false;
}

static double getusec() {
  struct timeval tp;
  gettimeofday(&tp, NULL);
  double sec = tp.tv_sec * 1000000 + (1.0 * tp.tv_usec);
  return sec;
}

void Certifier::garbage_collect(int *multiplied_sleep_times) {
  DBUG_TRACE;
  if (single_primary_fast_mode == MGR_FAST_MODE_WITHOUT_PARALLEL_REPLAY) {
    return;
  }

  /*
    This debug option works together with
    `group_replication_certifier_broadcast_thread_big_period`
    by disabling the manual garbage collection that happens when
    a View_log_change_event is logged.
    Applier_module::apply_view_change_packet() does call
    Certifier::set_group_stable_transactions_set().
  */
  DBUG_EXECUTE_IF("group_replication_do_not_clear_certification_database",
                  { return; };);

  int deleted = 0, first_deleted = -1, is_delete_limited = 0,
      candidate_delete_limited = 0;
  size_t count = 0;

  mysql_mutex_lock(&LOCK_certification_info);

  size_t cert_size = (size_t)get_certification_info_size();

  int cert_diff = (int)(cert_size - last_cert_size);
  double curr = getusec();
  if (cert_diff > 0) {
    if (last_add_usec > 0) {
      double diff = curr - last_add_usec;
      if (diff > 0) {
        last_add_velocity = 1000000.0 * cert_diff / diff;
      }
    }
  } else {
    last_add_velocity = 0.0;
  }
  last_add_usec = curr;

  /*
    When a transaction "t" is applied to all group members and for all
    ongoing, i.e., not yet committed or aborted transactions,
    "t" was already committed when they executed (thus "t"
    precedes them), then "t" is stable and can be removed from
    the certification info.
  */
  auto current = certification_index.begin();
  auto end = certification_index.end();
  stable_gtid_set_lock->wrlock();

  while (current != end && count < cert_size) {
    const std::string &value = current->second;
    auto it = certification_info.find(value);

    if (it != certification_info.end()) {
      bool could_be_deleted = false;
      if (single_primary_fast_mode) {
        could_be_deleted = true;
      } else {
        if (it->second.gtid_ref->is_subset_not_equals(stable_gtid_set)) {
          could_be_deleted = true;
        }
      }
      if (could_be_deleted) {
        if (candidate_delete_limited) {
          is_delete_limited = 1;
          break;
        }
        if (it->second.gtid_ref->unlink() == 0) delete it->second.gtid_ref;
        certification_info.erase(it);
        deleted++;
        if (first_deleted == -1) {
          first_deleted = (int)count;
        }
        certification_index.erase(current);
      } else {
        break;
      }
    } else {
      LogPluginErrMsg(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
                      "Certification index is corrupted");
      break;
    }

    count++;
    /* Reduce performance jitter */
    if (deleted >= MAX_CERT_DELETE_ITEMS) {
      candidate_delete_limited = 1;
    }
    current = certification_index.begin();
  }

  stable_gtid_set_lock->unlock();

  /*
    We need to update parallel applier indexes since we do not know
    what write sets were purged, which may cause transactions
    last committed to be incorrectly computed.
  */
  if (deleted > 0) {
    /* Not friendly for large transactions */
    increment_parallel_applier_sequence_number(true);
  }

  last_cert_size = get_certification_info_size();

  mysql_mutex_unlock(&LOCK_certification_info);

  if (single_primary_fast_mode && (!is_delete_limited)) {
    *multiplied_sleep_times = 10;
  }

  if (deleted > 0) {
    if (candidate_delete_limited) {
      if (is_delete_limited) {
        last_delete_items += deleted;
        return;
      }
    }
    last_delete_items += deleted;

    double curr = getusec();
    if (last_delete_usec > 0) {
      double diff = curr - last_delete_usec;
      if (diff > 0) {
        last_replay_velocity = 1000000.0 * last_delete_items / diff;
        /* multiply 1.1 by experience */
        estimated_replay_time = 1.1 * last_cert_size / last_replay_velocity;
        last_delete_items = 0;
      }
    }
    last_delete_usec = curr;
  }
}

int Certifier::handle_certifier_data(
    const uchar *data, ulong len, const Gcs_member_identifier &gcs_member_id) {
  DBUG_TRACE;
  bool member_message_received = false;

  if (!is_initialized()) return 1; /* purecov: inspected */

  /*
    On members recovering through clone the GTID_EXECUTED is only
    updated after the server restart that finishes the procedure.
    During that procedure they will periodically send the GTID_EXECUTED
    that the server had once joined the group. This will restrain the
    common set of transactions applied on all members, which in consequence
    will render the certification garbage collection void.
    As such, we only consider ONLINE members for the common set of
    transactions applied on all members.
    When recovering members change to ONLINE state, their certification
    info will be updated with the one of the donor at the join, being
    garbage collect on the future calls of this method.
  */
  if (group_member_mgr->get_group_member_status_by_member_id(gcs_member_id) !=
      Group_member_info::MEMBER_ONLINE) {
    return 0;
  }

  mysql_mutex_lock(&LOCK_members);
  std::string member_id = gcs_member_id.get_member_id();
#if !defined(NDEBUG)
  if (same_member_message_discarded) {
    /*
      Injecting the member_id in the member's vector to simulate the case of
      same member sending multiple messages.
    */
    this->members.push_back(member_id);
  }
#endif

  const size_t number_of_members_online =
      group_member_mgr->get_number_of_members_online();
  if (this->members.size() != number_of_members_online) {
    /*
      We check for the member_id of the current message if it is present in
      the member vector or not. If it is present, we will need to discard the
      message. If not we will add the message in the incoming message
      synchronized queue for stable set handling.
    */
    std::vector<std::string>::iterator it;
    it = std::find(members.begin(), members.end(), member_id);
    if (it != members.end())
      member_message_received = true;
    else
      this->members.push_back(member_id);

    /*
      Since member is not present we can queue this message.
    */
    if (!member_message_received) {
      this->incoming->push(new Data_packet(data, len));
    }
    // else: ignore the message, no point in alerting the user about this.

    mysql_mutex_unlock(&LOCK_members);

    /*
      If the incoming message queue size is equal to the number of the ONLINE
      members in the group, we are sure that each ONLINE member has sent
      their gtid_executed. So we can go ahead with the stable set handling.
    */
    if (this->incoming->size() == number_of_members_online) {
      int error = stable_set_handle();
      /*
        Clearing the members to proceed with the next round of garbage
        collection.
      */
      clear_members();
      return error;
    }
  } else {
    LogPluginErr(
        INFORMATION_LEVEL,
        ER_GRP_RPL_SKIP_COMPUTATION_TRANS_COMMITTED); /* purecov: inspected */
    mysql_mutex_unlock(&LOCK_members);                /* purecov: inspected */
  }

#if !defined(NDEBUG)
  if (same_member_message_discarded) {
    /*
      Clearing the flag here as the members vector is not cleaned above.
    */
    same_member_message_discarded = false;
    clear_members();
  }
#endif

  return 0;
}

int Certifier::stable_set_handle() {
  DBUG_TRACE;

  Data_packet *packet = nullptr;
  int error = 0;

  Sid_map sid_map(nullptr);
  Gtid_set executed_set(&sid_map, nullptr);

  /*
    Compute intersection between all received sets.
  */
  while (!error && !this->incoming->empty()) {
    this->incoming->pop(&packet);

    if (packet == nullptr) {
      LogPluginErr(ERROR_LEVEL,
                   ER_GRP_RPL_NULL_PACKET); /* purecov: inspected */
      error = 1;                            /* purecov: inspected */
      break;                                /* purecov: inspected */
    }

    uchar *payload = packet->payload;
    Gtid_set member_set(&sid_map, nullptr);
    Gtid_set intersection_result(&sid_map, nullptr);

    if (member_set.add_gtid_encoding(payload, packet->len) !=
        RETURN_STATUS_OK) {
      LogPluginErr(ERROR_LEVEL,
                   ER_GRP_RPL_CANT_READ_GTID); /* purecov: inspected */
      error = 1;                               /* purecov: inspected */
    } else {
      /*
        First member set? If so we only need to add it to executed set.
      */
      if (executed_set.is_empty()) {
        if (executed_set.add_gtid_set(&member_set)) {
          LogPluginErr(
              ERROR_LEVEL,
              ER_GRP_RPL_PROCESS_GTID_SET_ERROR); /* purecov: inspected */
          error = 1;                              /* purecov: inspected */
        }
      } else {
        /*
          We have three sets:
            member_set:          the one sent from a given member;
            executed_set:        the one that contains the intersection of
                                 the computed sets until now;
            intersection_result: the intersection between set and
                                 intersection_result.
          So we compute the intersection between set and executed_set, and
          set that value to executed_set to be used on the next intersection.
        */
        if (member_set.intersection(&executed_set, &intersection_result) !=
            RETURN_STATUS_OK) {
          LogPluginErr(
              ERROR_LEVEL,
              ER_GRP_RPL_PROCESS_INTERSECTION_GTID_SET_ERROR); /* purecov:
                                                                  inspected */
          error = 1; /* purecov: inspected */
        } else {
          executed_set.clear();
          if (executed_set.add_gtid_set(&intersection_result) !=
              RETURN_STATUS_OK) {
            LogPluginErr(
                ERROR_LEVEL,
                ER_GRP_RPL_PROCESS_GTID_SET_ERROR); /* purecov: inspected */
            error = 1;                              /* purecov: inspected */
          }
        }
      }
    }

    delete packet;
  }

  if (!error && set_group_stable_transactions_set(&executed_set)) {
    LogPluginErr(ERROR_LEVEL,
                 ER_GRP_RPL_SET_STABLE_TRANS_ERROR); /* purecov: inspected */
    error = 1;                                       /* purecov: inspected */
  }

#if !defined(NDEBUG)
  char *executed_set_string;
  executed_set.to_string(&executed_set_string);
  DBUG_PRINT("info", ("Certifier stable_set_handle: executed_set: %s",
                      executed_set_string));
  my_free(executed_set_string);
#endif

  return error;
}

void Certifier::handle_view_change() {
  DBUG_TRACE;
  clear_incoming();
  clear_members();
}

void Certifier::get_certification_info(
    std::map<std::string, std::string> *cert_info) {
  DBUG_TRACE;
  mysql_mutex_lock(&LOCK_certification_info);

  for (Certification_info::iterator it = certification_info.begin();
       it != certification_info.end(); ++it) {
    std::string key = it->first;
    assert(key.compare(GTID_EXTRACTED_NAME) != 0);

    size_t len = it->second.gtid_ref->get_encoded_length();
    uchar *buf = (uchar *)my_malloc(PSI_NOT_INSTRUMENTED, len, MYF(0));
    it->second.gtid_ref->encode(buf);
    std::string value(reinterpret_cast<const char *>(buf), len);
    my_free(buf);

    (*cert_info).insert(std::pair<std::string, std::string>(key, value));
  }

  // Add the group_gtid_executed to certification info sent to joiners.
  size_t len = group_gtid_executed->get_encoded_length();
  uchar *buf = (uchar *)my_malloc(PSI_NOT_INSTRUMENTED, len, MYF(0));
  group_gtid_executed->encode(buf);
  std::string value(reinterpret_cast<const char *>(buf), len);
  my_free(buf);
  (*cert_info)
      .insert(std::pair<std::string, std::string>(GTID_EXTRACTED_NAME, value));

  mysql_mutex_unlock(&LOCK_certification_info);
}

rpl_gno Certifier::generate_view_change_group_gno() {
  DBUG_TRACE;

  mysql_mutex_lock(&LOCK_certification_info);
  rpl_gno result = get_group_next_available_gtid(nullptr);

  DBUG_EXECUTE_IF("certifier_assert_next_seqno_equal_5", assert(result == 5););
  DBUG_EXECUTE_IF("certifier_assert_next_seqno_equal_7", assert(result == 7););

  if (result > 0) {
    add_to_group_gtid_executed_internal(group_gtid_sid_map_group_sidno, result,
                                        false);
    if (is_arbitrator_role()) {
      add_to_gtid_executed(get_group_sidno(), result);
    }
  }

  mysql_mutex_unlock(&LOCK_certification_info);

  return result;
}

int Certifier::set_certification_info(
    std::map<std::string, std::string> *cert_info) {
  DBUG_TRACE;
  assert(cert_info != nullptr);

  if (cert_info->size() == 1) {
    std::map<std::string, std::string>::iterator it =
        cert_info->find(CERTIFICATION_INFO_ERROR_NAME);
    if (it != cert_info->end()) {
      // The certification database could not be transmitted
      LogPluginErr(ERROR_LEVEL, ER_GRP_RPL_ERROR_ON_CERT_DB_INSTALL,
                   it->second.c_str());
      return 1;
    }
  }

  mysql_mutex_lock(&LOCK_certification_info);

  clear_certification_info();
  for (std::map<std::string, std::string>::iterator it = cert_info->begin();
       it != cert_info->end(); ++it) {
    std::string key = it->first;

    /*
      Extract the donor group_gtid_executed so that it can be used to
      while member is applying transactions that were already applied
      by distrubuted recovery procedure.
    */
    if (it->first.compare(GTID_EXTRACTED_NAME) == 0) {
      if (group_gtid_extracted->add_gtid_encoding(
              reinterpret_cast<const uchar *>(it->second.c_str()),
              it->second.length()) != RETURN_STATUS_OK) {
        LogPluginErr(
            ERROR_LEVEL,
            ER_GRP_RPL_CANT_READ_GRP_GTID_EXTRACTED); /* purecov: inspected */
        mysql_mutex_unlock(&LOCK_certification_info); /* purecov: inspected */
        return 1;                                     /* purecov: inspected */
      }
      continue;
    }

    Gtid_set_ref *value = new Gtid_set_ref(certification_info_sid_map, -1);
    if (value->add_gtid_encoding(
            reinterpret_cast<const uchar *>(it->second.c_str()),
            it->second.length()) != RETURN_STATUS_OK) {
      LogPluginErr(ERROR_LEVEL, ER_GRP_RPL_CANT_READ_WRITE_SET_ITEM,
                   key.c_str());                    /* purecov: inspected */
      mysql_mutex_unlock(&LOCK_certification_info); /* purecov: inspected */
      return 1;                                     /* purecov: inspected */
    }
    value->link();
    Cert_basic_info info;
    info.gtid_ref = value;
    info.recorded_timestamp = timestamp_counter;
    certification_info.insert(
        std::pair<std::string, Cert_basic_info>(key, info));
    certification_index[timestamp_counter] = key;
    timestamp_counter = timestamp_counter + 1;
  }

  if (initialize_server_gtid_set()) {
    LogPluginErr(
        ERROR_LEVEL,
        ER_GRP_RPL_INIT_CERTIFICATION_INFO_FAILURE); /* purecov: inspected */
    mysql_mutex_unlock(&LOCK_certification_info);    /* purecov: inspected */
    return 1;                                        /* purecov: inspected */
  }

  if (group_gtid_extracted->is_subset_not_equals(group_gtid_executed)) {
    certifying_already_applied_transactions = true;
    compute_group_available_gtid_intervals();

#ifndef NDEBUG
    char *group_gtid_executed_string = nullptr;
    char *group_gtid_extracted_string = nullptr;
    group_gtid_executed->to_string(&group_gtid_executed_string, true);
    group_gtid_extracted->to_string(&group_gtid_extracted_string, true);
    DBUG_PRINT(
        "Certifier::set_certification_info()",
        ("Set certifying_already_applied_transactions to true. "
         "group_gtid_executed: \"%s\"; group_gtid_extracted_string: \"%s\"",
         group_gtid_executed_string, group_gtid_extracted_string));
    my_free(group_gtid_executed_string);
    my_free(group_gtid_extracted_string);
#endif
  }

  mysql_mutex_unlock(&LOCK_certification_info);
  return 0;
}

void Certifier::update_certified_transaction_count(bool result,
                                                   bool local_transaction) {
  if (result)
    positive_cert++;
  else
    negative_cert++;

  const Group_member_info::Group_member_status member_status =
      local_member_info->get_recovery_status();
  if (member_status == Group_member_info::MEMBER_ONLINE) {
    applier_module->get_pipeline_stats_member_collector()
        ->increment_transactions_certified();

    /*
      If transaction is local and rolledback
      increment local negative certifier count
    */
    if (local_transaction && !result) {
      applier_module->get_pipeline_stats_member_collector()
          ->increment_transactions_local_rollback();
    }
  } else if (member_status == Group_member_info::MEMBER_IN_RECOVERY) {
    applier_module->get_pipeline_stats_member_collector()
        ->increment_transactions_certified_during_recovery();

    if (!result) {
      applier_module->get_pipeline_stats_member_collector()
          ->increment_transactions_certified_negatively_during_recovery();
    }
  }
}

ulonglong Certifier::get_positive_certified() { return positive_cert; }

ulonglong Certifier::get_negative_certified() { return negative_cert; }

ulonglong Certifier::get_certification_info_size() {
  return certification_info.size();
}

ulonglong Certifier::get_certification_add_velocity() {
  return last_add_velocity;
}

ulonglong Certifier::get_certification_delete_velocity() {
  return last_replay_velocity;
}

ulonglong Certifier::get_certification_estimated_replay_time() {
  return estimated_replay_time;
}

void Certifier::get_last_conflict_free_transaction(std::string *value) {
  int length = 0;
  char buffer[Gtid::MAX_TEXT_LENGTH + 1];

  mysql_mutex_lock(&LOCK_certification_info);
  if (last_conflict_free_transaction.is_empty()) goto end;

  length = last_conflict_free_transaction.to_string(group_gtid_sid_map, buffer);
  if (length > 0) value->assign(buffer);

end:
  mysql_mutex_unlock(&LOCK_certification_info);
}

size_t Certifier::get_local_certified_gtid(
    std::string &local_gtid_certified_string) {
  if (last_local_gtid.is_empty()) return 0;

  char buf[Gtid::MAX_TEXT_LENGTH + 1];
  last_local_gtid.to_string(group_gtid_sid_map, buf);
  local_gtid_certified_string.assign(buf);
  return local_gtid_certified_string.size();
}

void Certifier::enable_conflict_detection() {
  DBUG_TRACE;

  mysql_mutex_lock(&LOCK_certification_info);
  conflict_detection_enable = true;
  local_member_info->enable_conflict_detection();
  mysql_mutex_unlock(&LOCK_certification_info);
}

void Certifier::disable_conflict_detection() {
  DBUG_TRACE;
  assert(local_member_info->in_primary_mode());

  mysql_mutex_lock(&LOCK_certification_info);
  conflict_detection_enable = false;
  local_member_info->disable_conflict_detection();
  mysql_mutex_unlock(&LOCK_certification_info);

  LogPluginErr(INFORMATION_LEVEL, ER_GRP_RPL_CONFLICT_DETECTION_DISABLED);
}

bool Certifier::is_conflict_detection_enable() {
  DBUG_TRACE;

  mysql_mutex_lock(&LOCK_certification_info);
  bool result = conflict_detection_enable;
  mysql_mutex_unlock(&LOCK_certification_info);

  return result;
}

bool Certifier::is_gtid_committed(const Gtid &gtid) {
  DBUG_TRACE;

  auto sid = global_sid_map->sidno_to_sid(gtid.sidno, true);
  auto sidno = group_gtid_sid_map->sid_to_sidno(sid);

  if (sidno == 0) {
    return false;
  } else {
    auto ret = group_gtid_executed->contains_gtid({sidno, gtid.gno});
    return ret;
  }
}

/*
  Gtid_Executed_Message implementation
 */

Gtid_Executed_Message::Gtid_Executed_Message()
    : Plugin_gcs_message(CT_CERTIFICATION_MESSAGE) {}

Gtid_Executed_Message::~Gtid_Executed_Message() {}

void Gtid_Executed_Message::append_gtid_executed(uchar *gtid_data, size_t len) {
  data.insert(data.end(), gtid_data, gtid_data + len);
}

void Gtid_Executed_Message::encode_payload(
    std::vector<unsigned char> *buffer) const {
  DBUG_TRACE;

  encode_payload_item_type_and_length(buffer, PIT_GTID_EXECUTED, data.size());
  buffer->insert(buffer->end(), data.begin(), data.end());
}

void Gtid_Executed_Message::decode_payload(const unsigned char *buffer,
                                           const unsigned char *) {
  DBUG_TRACE;
  const unsigned char *slider = buffer;
  uint16 payload_item_type = 0;
  unsigned long long payload_item_length = 0;

  decode_payload_item_type_and_length(&slider, &payload_item_type,
                                      &payload_item_length);
  data.clear();
  data.insert(data.end(), slider, slider + payload_item_length);
}
