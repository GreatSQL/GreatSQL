/* Copyright (c) 2019, 2022, Oracle and/or its affiliates. All rights reserved.
   Copyright (c) 2023, 2025, GreatDB Software Co., Ltd.

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

#ifndef LEAVE_GROUP_ON_FAILURE_INCLUDED
#define LEAVE_GROUP_ON_FAILURE_INCLUDED

#include <bitset>

#include "my_inttypes.h"
#include "plugin/group_replication/include/services/notification/notification.h"

class Group_leaving_stage_on_failure {
 public:
  enum leaving_stage {
    NOT_LEAVING_STAGE,
    BEGIN_LEAVING_STAGE,
    START_LEAVE_VIEW_STAGE,
    KILL_BINLOG_DUMP_THREAD_STAGE,
    STOP_APPLIER_STAGE,
    STOP_SLAVE_CHANNEL_STAGE,
    SET_READ_ONLY_STAGE,
    SET_OFFLINE_MODE_STAGE,
    WAIT_VIEW_CHANGE_STAGE,
    ABORT_SERVER_STAGE,
    AUTO_REJOIN_STAGE,
    END_LEAVING_STAGE,
  };

  Group_leaving_stage_on_failure() : m_stage(BEGIN_LEAVING_STAGE) {}
  void set_leaving_stage(enum leaving_stage stage) { m_stage = stage; }
  enum leaving_stage get_leaving_stage() { return m_stage; }
  const char *stage_to_string() {
    const char *str = "";
    switch (m_stage) {
      case NOT_LEAVING_STAGE: {
        str = "not_leaveing_stage";
      } break;
      case BEGIN_LEAVING_STAGE: {
        str = "begin_leaving_stage";
      } break;
      case START_LEAVE_VIEW_STAGE: {
        str = "start_leave_view_stage";
      } break;
      case KILL_BINLOG_DUMP_THREAD_STAGE: {
        str = "kill_binlog_dump_thread_stage";
      } break;
      case STOP_APPLIER_STAGE: {
        str = "stop_applier_stage";
      } break;
      case STOP_SLAVE_CHANNEL_STAGE: {
        str = "stop_slave_channel_stage";
      } break;
      case SET_READ_ONLY_STAGE: {
        str = "enable_super_read_only_mode_stage";
      } break;
      case SET_OFFLINE_MODE_STAGE: {
        str = "set_offline_mode_stage";
      } break;
      case WAIT_VIEW_CHANGE_STAGE: {
        str = "wait_view_change_stage";
      } break;
      case ABORT_SERVER_STAGE: {
        str = "abort_server_stage";
      } break;
      case AUTO_REJOIN_STAGE: {
        str = "auto_rejoin_stage";
      } break;
      case END_LEAVING_STAGE: {
        str = "end_leaving_stage";
      } break;
    }
    return str;
  }

 private:
  enum leaving_stage m_stage;
};

/**
  Structure that holds the actions taken by the plugin when the
  member leaves the group after a failure.
*/
struct leave_group_on_failure {
  /**
    @enum enum_actions
    @brief Actions taken by the plugin when the member leaves the
    group after a failure.
  */
  enum enum_actions {
    ALREADY_LEFT_GROUP = 0,
    SKIP_SET_READ_ONLY,
    SKIP_LEAVE_VIEW_WAIT,
    CLEAN_GROUP_MEMBERSHIP,
    STOP_APPLIER,
    HANDLE_EXIT_STATE_ACTION,
    HANDLE_AUTO_REJOIN,
    ACTION_MAX
  };
  using mask = std::bitset<ACTION_MAX>;

  /**
    Do the instructed actions after a failure.

    @param[in]  actions
                  Actions performed.
    @param[in]  error_to_log
                  Error logged into error log.
    @param[in]  caller_notification_context
                  If defined the member state change notification
                  will update this notification context and the
                  notification signal responsibility belongs to the
                  caller.
                  If not defined (nullptr) a notification will be
                  sent by this function.
    @param[in]  exit_state_action_abort_log_message
                  The log message used on abort_plugin_process() if
                  that function is called.
  */
  static void leave(const mask &actions, longlong error_to_log,
                    Notification_context *caller_notification_context,
                    const char *exit_state_action_abort_log_message,
                    Group_leaving_stage_on_failure *leaving_stage = nullptr);
};

#endif /* LEAVE_GROUP_ON_FAILURE_INCLUDED */
