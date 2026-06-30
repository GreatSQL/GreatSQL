/* Copyright (c) 2024 Percona LLC and/or its affiliates. All rights reserved.
   Copyright (c) 2026, GreatDB Software Co., Ltd.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA */

#pragma once

#include <string>
#include <variant>
#include "components/audit_log_filter/audit_record.h"

namespace audit_log_filter::audit_table {

class AuditRecordConverter {
 public:
  static void convert_to_json(const AuditRecordVariant &record,
                              std::string &ev_class, std::string &ev_sub,
                              std::string &user, std::string &host,
                              std::string &record_str);

 private:
  static std::string escape_json_string(const std::string &input);
  static std::string build_json_record(
      const std::string &event_class, const std::string &event_subclass,
      const std::string &user, const std::string &host,
      const std::map<std::string, std::string> &fields);

  static void convert_general_record(const AuditRecordGeneral &record,
                                     std::string &ev_class, std::string &ev_sub,
                                     std::string &user, std::string &host,
                                     std::string &record_str);
  static void convert_connection_record(const AuditRecordConnection &record,
                                        std::string &ev_class,
                                        std::string &ev_sub, std::string &user,
                                        std::string &host,
                                        std::string &record_str);
  static void convert_command_record(const AuditRecordCommand &record,
                                     std::string &ev_class, std::string &ev_sub,
                                     std::string &user, std::string &host,
                                     std::string &record_str);
  static void convert_query_record(const AuditRecordQuery &record,
                                   std::string &ev_class, std::string &ev_sub,
                                   std::string &user, std::string &host,
                                   std::string &record_str);
  static void convert_authentication_record(
      const AuditRecordAuthentication &record, std::string &ev_class,
      std::string &ev_sub, std::string &user, std::string &host,
      std::string &record_str);
  static void convert_table_access_record(const AuditRecordTableAccess &record,
                                          std::string &ev_class,
                                          std::string &ev_sub,
                                          std::string &user, std::string &host,
                                          std::string &record_str);
  static void convert_audit_record(const AuditRecordAudit &record,
                                   std::string &ev_class, std::string &ev_sub,
                                   std::string &user, std::string &host,
                                   std::string &record_str);
  static void convert_global_variable_record(
      const AuditRecordGlobalVariable &record, std::string &ev_class,
      std::string &ev_sub, std::string &user, std::string &host,
      std::string &record_str);
  static void convert_server_startup_record(
      const AuditRecordServerStartup &record, std::string &ev_class,
      std::string &ev_sub, std::string &user, std::string &host,
      std::string &record_str);
  static void convert_server_shutdown_record(
      const AuditRecordServerShutdown &record, std::string &ev_class,
      std::string &ev_sub, std::string &user, std::string &host,
      std::string &record_str);
  static void convert_stored_program_record(
      const AuditRecordStoredProgram &record, std::string &ev_class,
      std::string &ev_sub, std::string &user, std::string &host,
      std::string &record_str);
  static void convert_message_record(const AuditRecordMessage &record,
                                     std::string &ev_class, std::string &ev_sub,
                                     std::string &user, std::string &host,
                                     std::string &record_str);
  static void convert_parse_record(const AuditRecordParse &record,
                                   std::string &ev_class, std::string &ev_sub,
                                   std::string &user, std::string &host,
                                   std::string &record_str);
  static void convert_unknown_record(const AuditRecordUnknown &record,
                                     std::string &ev_class, std::string &ev_sub,
                                     std::string &user, std::string &host,
                                     std::string &record_str);
};

}  // namespace audit_log_filter::audit_table
