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

#include "components/audit_log_filter/audit_table/audit_record_converter.h"
#include <iomanip>
#include <sstream>
#include "components/audit_log_filter/audit_log_filter.h"
#include "components/audit_log_filter/audit_record.h"
#include "components/audit_log_filter/sys_vars.h"
#include "my_rapidjson_size_t.h"
#include "mysql/components/services/defs/event_tracking_global_variable_defs.h"
#include "mysql/components/services/dynamic_privilege.h"
#include "rapidjson/document.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "sql/current_thd.h"
#include "sql/sql_class.h"

namespace audit_log_filter::audit_table {

std::string AuditRecordConverter::escape_json_string(const std::string &input) {
  std::string output;
  output.reserve(input.length());

  for (char c : input) {
    switch (c) {
      case '"':
        output += "\\\"";
        break;
      case '\\':
        output += "\\\\";
        break;
      case '\b':
        output += "\\b";
        break;
      case '\f':
        output += "\\f";
        break;
      case '\n':
        output += "\\n";
        break;
      case '\r':
        output += "\\r";
        break;
      case '\t':
        output += "\\t";
        break;
      default:
        output += c;
        break;
    }
  }

  return output;
}

std::string AuditRecordConverter::build_json_record(
    const std::string &event_class [[maybe_unused]],
    const std::string &event_subclass [[maybe_unused]],
    const std::string &user [[maybe_unused]],
    const std::string &host [[maybe_unused]],
    const std::map<std::string, std::string> &fields) {
  rapidjson::Document doc;
  doc.SetObject();
  auto &allocator = doc.GetAllocator();

  // Add each map entry to the JSON object
  for (const auto &[key, value] : fields) {
    rapidjson::Value jsonKey(key.c_str(), allocator);
    rapidjson::Value jsonValue(value.c_str(), allocator);
    doc.AddMember(jsonKey, jsonValue, allocator);
  }

  // add user unique id
  if (SysVars::get_to_table_eal()) {
    std::string uid =
        std::to_string(current_thd->security_context()->get_uid());

    if (!uid.empty()) {
      rapidjson::Value jsonKey("uid", allocator);
      rapidjson::Value jsonValue(uid.c_str(), allocator);
      doc.AddMember(jsonKey, jsonValue, allocator);
    }
  }

  // Serialize JSON to string
  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  doc.Accept(writer);

  return buffer.GetString();
}

void AuditRecordConverter::convert_to_json(const AuditRecordVariant &record,
                                           std::string &ev_class,
                                           std::string &ev_sub,
                                           std::string &user, std::string &host,
                                           std::string &record_str) {
  if (std::holds_alternative<AuditRecordGeneral>(record)) {
    convert_general_record(std::get<AuditRecordGeneral>(record), ev_class,
                           ev_sub, user, host, record_str);
  } else if (std::holds_alternative<AuditRecordConnection>(record)) {
    convert_connection_record(std::get<AuditRecordConnection>(record), ev_class,
                              ev_sub, user, host, record_str);
  } else if (std::holds_alternative<AuditRecordCommand>(record)) {
    convert_command_record(std::get<AuditRecordCommand>(record), ev_class,
                           ev_sub, user, host, record_str);
  } else if (std::holds_alternative<AuditRecordQuery>(record)) {
    convert_query_record(std::get<AuditRecordQuery>(record), ev_class, ev_sub,
                         user, host, record_str);
  } else if (std::holds_alternative<AuditRecordAuthentication>(record)) {
    convert_authentication_record(std::get<AuditRecordAuthentication>(record),
                                  ev_class, ev_sub, user, host, record_str);
  } else if (std::holds_alternative<AuditRecordTableAccess>(record)) {
    convert_table_access_record(std::get<AuditRecordTableAccess>(record),
                                ev_class, ev_sub, user, host, record_str);
  } else if (std::holds_alternative<AuditRecordAudit>(record)) {
    convert_audit_record(std::get<AuditRecordAudit>(record), ev_class, ev_sub,
                         user, host, record_str);
  } else if (std::holds_alternative<AuditRecordGlobalVariable>(record)) {
    convert_global_variable_record(std::get<AuditRecordGlobalVariable>(record),
                                   ev_class, ev_sub, user, host, record_str);
  } else if (std::holds_alternative<AuditRecordServerStartup>(record)) {
    convert_server_startup_record(std::get<AuditRecordServerStartup>(record),
                                  ev_class, ev_sub, user, host, record_str);
  } else if (std::holds_alternative<AuditRecordServerShutdown>(record)) {
    convert_server_shutdown_record(std::get<AuditRecordServerShutdown>(record),
                                   ev_class, ev_sub, user, host, record_str);
  } else if (std::holds_alternative<AuditRecordStoredProgram>(record)) {
    convert_stored_program_record(std::get<AuditRecordStoredProgram>(record),
                                  ev_class, ev_sub, user, host, record_str);
  } else if (std::holds_alternative<AuditRecordMessage>(record)) {
    convert_message_record(std::get<AuditRecordMessage>(record), ev_class,
                           ev_sub, user, host, record_str);
  } else if (std::holds_alternative<AuditRecordParse>(record)) {
    convert_parse_record(std::get<AuditRecordParse>(record), ev_class, ev_sub,
                         user, host, record_str);
  } else if (std::holds_alternative<AuditRecordUnknown>(record)) {
    convert_unknown_record(std::get<AuditRecordUnknown>(record), ev_class,
                           ev_sub, user, host, record_str);
  }
}

void AuditRecordConverter::convert_general_record(
    const AuditRecordGeneral &record, std::string &ev_class,
    std::string &ev_sub, std::string &user, std::string &host,
    std::string &record_str) {
  ev_class = record.event_class_name;
  ev_sub = record.event_subclass_name;
  auto fields = get_audit_record_fields(record);
  if (!record.msg.empty()) {
    fields["msg"] = record.msg;
  }
  record_str = build_json_record(ev_class, ev_sub, user, host, fields);
}

void AuditRecordConverter::convert_connection_record(
    const AuditRecordConnection &record, std::string &ev_class,
    std::string &ev_sub, std::string &user, std::string &host,
    std::string &record_str) {
  ev_class = record.event_class_name;
  ev_sub = record.event_subclass_name;
  record_str = build_json_record(ev_class, ev_sub, user, host,
                                 get_audit_record_fields(record));
}

void AuditRecordConverter::convert_command_record(
    const AuditRecordCommand &record, std::string &ev_class,
    std::string &ev_sub, std::string &user, std::string &host,
    std::string &record_str) {
  ev_class = record.event_class_name;
  ev_sub = record.event_subclass_name;
  record_str = build_json_record(ev_class, ev_sub, user, host,
                                 get_audit_record_fields(record));
}

void AuditRecordConverter::convert_query_record(
    const AuditRecordQuery &record, std::string &ev_class, std::string &ev_sub,
    std::string &user, std::string &host, std::string &record_str) {
  ev_class = record.event_class_name;
  ev_sub = record.event_subclass_name;
  record_str = build_json_record(ev_class, ev_sub, user, host,
                                 get_audit_record_fields(record));
}

void AuditRecordConverter::convert_authentication_record(
    const AuditRecordAuthentication &record, std::string &ev_class,
    std::string &ev_sub, std::string &user, std::string &host,
    std::string &record_str) {
  ev_class = record.event_class_name;
  ev_sub = record.event_subclass_name;
  record_str = build_json_record(ev_class, ev_sub, user, host,
                                 get_audit_record_fields(record));
}

void AuditRecordConverter::convert_table_access_record(
    const AuditRecordTableAccess &record, std::string &ev_class,
    std::string &ev_sub, std::string &user, std::string &host,
    std::string &record_str) {
  ev_class = record.event_class_name;
  ev_sub = record.event_subclass_name;
  record_str = build_json_record(ev_class, ev_sub, user, host,
                                 get_audit_record_fields(record));
}

void AuditRecordConverter::convert_audit_record(
    const AuditRecordAudit &record, std::string &ev_class, std::string &ev_sub,
    std::string &user, std::string &host, std::string &record_str) {
  ev_class = record.event_class_name;
  ev_sub = record.event_subclass_name;
  record_str = build_json_record(ev_class, ev_sub, user, host,
                                 get_audit_record_fields(record));
}

void AuditRecordConverter::convert_global_variable_record(
    const AuditRecordGlobalVariable &record, std::string &ev_class,
    std::string &ev_sub, std::string &user, std::string &host,
    std::string &record_str) {
  ev_class = record.event_class_name;
  ev_sub = record.event_subclass_name;
  record_str = build_json_record(ev_class, ev_sub, user, host,
                                 get_audit_record_fields(record));
}

void AuditRecordConverter::convert_server_startup_record(
    const AuditRecordServerStartup &record, std::string &ev_class,
    std::string &ev_sub, std::string &user, std::string &host,
    std::string &record_str) {
  ev_class = record.event_class_name;
  ev_sub = record.event_subclass_name;
  record_str = build_json_record(ev_class, ev_sub, user, host,
                                 get_audit_record_fields(record));
}

void AuditRecordConverter::convert_server_shutdown_record(
    const AuditRecordServerShutdown &record, std::string &ev_class,
    std::string &ev_sub, std::string &user, std::string &host,
    std::string &record_str) {
  ev_class = record.event_class_name;
  ev_sub = record.event_subclass_name;
  record_str = build_json_record(ev_class, ev_sub, user, host,
                                 get_audit_record_fields(record));
}

void AuditRecordConverter::convert_stored_program_record(
    const AuditRecordStoredProgram &record, std::string &ev_class,
    std::string &ev_sub, std::string &user, std::string &host,
    std::string &record_str) {
  ev_class = record.event_class_name;
  ev_sub = record.event_subclass_name;
  record_str = build_json_record(ev_class, ev_sub, user, host,
                                 get_audit_record_fields(record));
}

void AuditRecordConverter::convert_message_record(
    const AuditRecordMessage &record, std::string &ev_class,
    std::string &ev_sub, std::string &user, std::string &host,
    std::string &record_str) {
  ev_class = record.event_class_name;
  ev_sub = record.event_subclass_name;
  record_str = build_json_record(ev_class, ev_sub, user, host,
                                 get_audit_record_fields(record));
}

void AuditRecordConverter::convert_parse_record(
    const AuditRecordParse &record, std::string &ev_class, std::string &ev_sub,
    std::string &user, std::string &host, std::string &record_str) {
  ev_class = record.event_class_name;
  ev_sub = record.event_subclass_name;
  record_str = build_json_record(ev_class, ev_sub, user, host,
                                 get_audit_record_fields(record));
}

void AuditRecordConverter::convert_unknown_record(
    const AuditRecordUnknown &record, std::string &ev_class,
    std::string &ev_sub, std::string &user, std::string &host,
    std::string &record_str) {
  ev_class = record.event_class_name;
  ev_sub = record.event_subclass_name;
  record_str = build_json_record(ev_class, ev_sub, user, host,
                                 get_audit_record_fields(record));
}

}  // namespace audit_log_filter::audit_table
