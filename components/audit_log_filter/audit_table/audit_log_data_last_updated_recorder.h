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

#ifndef AUDIT_LOG_FILTER_AUDIT_TABLE_AUDIT_LOG_DATA_LAST_UPDATED_RECORDER_H_INCLUDED
#define AUDIT_LOG_FILTER_AUDIT_TABLE_AUDIT_LOG_DATA_LAST_UPDATED_RECORDER_H_INCLUDED

#include "base.h"
#include "components/audit_log_filter/audit_error_log.h"
#include "components/audit_log_filter/audit_record.h"
#include "components/audit_log_filter/sys_vars.h"

#include <ctime>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>
namespace audit_log_filter::audit_table {

class LastUpdatedRecorder {
  explicit LastUpdatedRecorder() : last_updated_time(0) {
    file_path = std::filesystem::path{SysVars::get_file_dir() +
                                      "/audit_last_updated.txt"};
  }
  ~LastUpdatedRecorder() {}
  LastUpdatedRecorder(const LastUpdatedRecorder &) = delete;
  LastUpdatedRecorder &operator=(const LastUpdatedRecorder &) = delete;
  LastUpdatedRecorder(LastUpdatedRecorder &&) = delete;
  LastUpdatedRecorder &operator=(LastUpdatedRecorder &&) = delete;

  time_t last_updated_time;
  std::mutex mtx;
  std::string file_path;

 public:
  static LastUpdatedRecorder &get_instance() {
    static LastUpdatedRecorder instance;
    return instance;
  }
  void update_last_updated_time() {
    try {
      std::lock_guard<std::mutex> lock(mtx);
      last_updated_time = time(nullptr);

      std::ofstream file(file_path, std::ios::out | std::ios::trunc);

      if (file.is_open()) {
        file << last_updated_time;
        file.close();
      }
    } catch (...) {
    }
  }

  time_t get_last_updated_time() {
    std::lock_guard<std::mutex> lock(mtx);
    if (last_updated_time == 0) {
      std::ifstream file(file_path);
      if (file.is_open()) {
        file >> last_updated_time;
        file.close();
      }
    }
    return last_updated_time;
  }

  void drop() {
    std::lock_guard<std::mutex> lock(mtx);
    try {
      std::filesystem::remove(file_path);
    } catch (...) {
    }
  }
};

}  // namespace audit_log_filter::audit_table

#endif  // AUDIT_LOG_FILTER_AUDIT_TABLE_AUDIT_LOG_DATA_LAST_UPDATED_RECORDER_H_INCLUDED
