/* Copyright (c) 2022 Percona LLC and/or its affiliates. All rights reserved.
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

#ifndef ABNORMAL_SHUTDOWN_FINDER_INCLUDED
#define ABNORMAL_SHUTDOWN_FINDER_INCLUDED

#include "components/audit_log_filter/audit_error_log.h"
#include "components/audit_log_filter/audit_event_class_internal.h"
#include "components/audit_log_filter/audit_record.h"
#include "components/audit_log_filter/sys_vars.h"

#include <mysql/components/services/dynamic_privilege.h>
#include <mysql/components/services/mysql_current_thread_reader.h>
#include <mysql/components/services/security_context.h>
#include "sql/mysqld.h"

#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <regex>
#include <sstream>

namespace audit_log_filter {

struct LogEntry {
  time_t timestamp;
  std::string thread_id;
  std::string system_tag;
  std::string error_code;
  std::string message;
};

static const std::string pattern_date = R"(^(\d{4}-\d{2}-\d{2}))";
static const std::string pattern_time = R"((\d{2}:\d{2}:\d{2}))";
static const std::string pattern_milisecond = R"((\.\d+))";
static const std::string pattern_timezone = R"(([+-]\d{2}:\d{2}|Z))";
static const std::string pattern_space = R"(\s+)";
static const std::string pattern_timestamp =
    R"(^(\d{4}-\d{2}-\d{2}T[\d:\.+-]*))";
static const std::string pattern_timestamp_no_begin =
    R"((\d{4}-\d{2}-\d{2}T[\d:\.+-]*))";
static const std::string pattern_thread_id = R"((\d+))";
static const std::string pattern_level = R"((\[[A-Za-z]+\]))";
static const std::string pattern_error_code = R"((\[[A-Za-z\-0-9]+\]))";
static const std::string pattern_message = R"((.*))";

class ErrorLogDigger final {
  ErrorLogDigger() {}

  ~ErrorLogDigger() {}

  ErrorLogDigger(const ErrorLogDigger &) = delete;
  ErrorLogDigger &operator=(const ErrorLogDigger &) = delete;
  ErrorLogDigger(ErrorLogDigger &&) = delete;
  ErrorLogDigger &operator=(ErrorLogDigger &&) = delete;

 public:
  static ErrorLogDigger &get_instance() {
    static ErrorLogDigger instance;
    return instance;
  }

  std::string readTailMBOfErrorLog(size_t sizeInMb = 1) {
    std::ifstream error_log(std::string(log_error_dest, strlen(log_error_dest)),
                            std::ios::binary | std::ios::ate);

    auto size = error_log.tellg();

    long int read_size = sizeInMb * 1024 * 1024;
    if (size < read_size) {
      read_size = size;
    }

    error_log.seekg(-read_size, std::ios::end);
    std::string content(read_size, '\0');

    error_log.read(content.data(), read_size);

    error_log.close();

    // try to remove the content before the SECOND LAST start
    const std::regex pattern(
        pattern_timestamp_no_begin + pattern_space + pattern_thread_id +
        pattern_space + R"(\[System\])" + pattern_space + R"(\[MY\-015015\])" +
        pattern_space + pattern_message);

    size_t last_pos = std::string::npos;
    size_t second_last = std::string::npos;

    for (std::sregex_iterator it(content.begin(), content.end(), pattern), end;
         it != end; ++it) {
      second_last = last_pos;
      last_pos = it->position();
    }

    content = content.substr(second_last);

    // remove the content after the last \n
    auto pos = content.find_last_of('\n');
    if (pos != std::string::npos) {
      content = content.substr(0, pos + 1);
    }

    // remove the content before the first \n and the \n itself
    pos = content.find_first_of('\n');
    if (pos != std::string::npos) {
      content = content.substr(pos + 1, content.size() - pos - 1);
    }

    {
      auto file_path = std::filesystem::path{SysVars::get_file_dir() +
                                             "/audit_log_digger.txt"};

      std::ofstream file(file_path, std::ios::out | std::ios::app);

      if (file.is_open()) {
        file << content;
        file.close();
      }
    }

    return content;
  }

  time_t parse_timestamp_iso8601(const std::string &timestamp) {
    static const std::regex pattern(pattern_date + "T" + pattern_time +
                                    pattern_milisecond + pattern_timezone);

    std::smatch matches;
    if (!std::regex_match(timestamp, matches, pattern)) {
      return 0;
    }

    std::tm tm = {};
    std::istringstream iss(matches[1].str() + "T" + matches[2].str());
    iss >> std::get_time(&tm, "%Y-%m-%dT%H:%M:%S");

    // convert to utc time
    auto tp = std::chrono::system_clock::from_time_t(std::mktime(&tm));

    return std::chrono::system_clock::to_time_t(tp);
    ;
  }

  std::vector<std::string> splitLines(const std::string &content) {
    std::vector<std::string> lines;
    std::string line;
    std::istringstream stream(content);

    while (std::getline(stream, line)) {
      line.erase(0, line.find_first_not_of(" \t\n\r\f\v"));
      line.erase(line.find_last_not_of(" \t\n\r\f\v") + 1);
      lines.push_back(line);
    }
    return lines;
  }

  LogEntry parse_log_line(const std::string &line) {
    static const std::regex pattern(
        pattern_timestamp + pattern_space + pattern_thread_id + pattern_space +
        pattern_level + pattern_space + pattern_error_code + pattern_space +
        pattern_message);

    std::smatch matches;

    if (!std::regex_match(line, matches, pattern)) {
      throw std::runtime_error("Invalid log format:[" + line + "]");
    }

    return {parse_timestamp_iso8601(matches[1].str()), matches[2].str(),
            matches[3].str(),  // [System]
            matches[4].str(),  // [MY-010910]
            matches[5].str()};
  }

  std::vector<LogEntry> parseLogEntries() {
    auto lines = splitLines(readTailMBOfErrorLog(2));

    std::vector<LogEntry> log_entries;

    for (const auto &line : lines) {
      try {
        auto log_entry = parse_log_line(line);
        if (log_entry.timestamp > 0) {
          log_entries.push_back(log_entry);
        }
      } catch (const std::exception &e) {
        LogComponentErr(
            INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
            "Parsing log line failed in audit error log digger: %s ",
            line.c_str());
      }
    }

    return log_entries;
  }
};

}  // namespace audit_log_filter

#endif  // ABNORMAL_SHUTDOWN_FINDER_INCLUDED
