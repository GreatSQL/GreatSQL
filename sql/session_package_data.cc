/* Copyright (c) 2023, GreatDB Software Co., Ltd. All rights reserved.

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
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

#include "session_package_data.h"
#include "my_systime.h"
#include "my_time.h"
#include "sql/current_thd.h"
#include "sql/sql_class.h"

void Dbms_profiler_data::before_exec() {
  if (exec_begin_time != 0) {
    // when recursion call, exec_begin_time > 0
    // we should save upper level begin time
    s.push(exec_begin_time);
  }
  exec_begin_time = my_getsystime();
}

void Dbms_profiler_data::after_exec() {
  uint64 exec_end_time = my_getsystime();
  if (exec_begin_time == 0 && !s.empty()) {
    exec_begin_time = s.top();
    s.pop();
  }
  uint64 exec_time = exec_end_time - exec_begin_time;
  total_occur++;
  total_time += exec_time;
  if (min_time == 0) min_time = exec_time;
  if (max_time == 0) max_time = exec_time;
  if (exec_time < min_time) min_time = exec_time;
  if (exec_time > max_time) max_time = exec_time;
  exec_begin_time = 0;
}

std::shared_ptr<Dbms_profiler_data> Dbms_profiler_units::find_data(
    uint64 line) {
  auto it = data.find(line);
  if (it == data.end()) {
    return nullptr;
  }

  return it->second;
}

std::shared_ptr<Dbms_profiler_data> Dbms_profiler_units::create_data(
    uint64 line) {
  if (data.size() >= current_thd->variables.dbms_profiler_max_data_size) {
    return nullptr;
  }

  std::shared_ptr<Dbms_profiler_data> dpd(new Dbms_profiler_data);
  dpd->line = line;
  dpd->total_occur = 0;
  dpd->total_time = 0;
  dpd->min_time = 0;
  dpd->max_time = 0;
  dpd->exec_begin_time = 0;
  dpd->need_flush = true;
  data.emplace(line, dpd);
  return dpd;
}

Session_dbms_profiler_info::Session_dbms_profiler_info()
    : status_(DBMS_PROFILER_STATUS_STOP),
      runid_(0),
      start_time_(0),
      elapse_time_(0) {}

Session_dbms_profiler_info::~Session_dbms_profiler_info() { clear(); }

void Session_dbms_profiler_info::clear() {
  status_ = DBMS_PROFILER_STATUS_STOP;
  runid_ = 0;
  start_time_ = 0;
  elapse_time_ = 0;
  for (auto &info : infos_) {
    info.data.clear();
  }
  infos_.clear();
}

int Session_dbms_profiler_info::status() { return status_; }

int Session_dbms_profiler_info::start(uint64 runid) {
  if (runid == 0 || status_ != DBMS_PROFILER_STATUS_STOP) return -1;
  runid_ = runid;
  status_ = DBMS_PROFILER_STATUS_RUNING;
  start_time_ = my_getsystime();
  elapse_time_ = 0;

  return 0;
}

int Session_dbms_profiler_info::pause() {
  if (status_ != DBMS_PROFILER_STATUS_RUNING) return -1;

  status_ = DBMS_PROFILER_STATUS_PAUSE;
  elapse_time_ = get_run_total_time();

  return 0;
}

int Session_dbms_profiler_info::resume() {
  if (status_ != DBMS_PROFILER_STATUS_PAUSE) return -1;

  status_ = DBMS_PROFILER_STATUS_RUNING;
  start_time_ = my_getsystime();
  return 0;
}

int Session_dbms_profiler_info::stop() {
  if (status_ == DBMS_PROFILER_STATUS_STOP) return -1;

  clear();

  return 0;
}

std::string Session_dbms_profiler_info::get_current_time() {
  std::time_t current_time = std::time(nullptr);
  struct tm *t = localtime(&current_time);

  char to[64] = {0};
  if (t->tm_sec >= 60) {
    t->tm_sec = 0;
  }

  strftime(to, 64, "%Y-%m-%d %H:%M:%S.000", t);
  return std::string(to, strlen(to));
}

Dbms_profiler_units *Session_dbms_profiler_info::find_uint(
    const std::string &unit_db, int type, const std::string &unit_name,
    int64 version) {
  for (auto &info : infos_) {
    if (info.change_version == version && info.type == type &&
        info.unit_name.compare(unit_name) == 0 &&
        info.unit_db.compare(unit_db) == 0) {
      return &info;
    }
  }
  return nullptr;
}

Dbms_profiler_units *Session_dbms_profiler_info::create_uint(
    std::string &&unit_type, std::string &&unit_owner, std::string &&unit_name,
    const std::string &unit_db, int type, int64 version) {
  if (infos_.size() >= current_thd->variables.dbms_profiler_max_units_size) {
    return nullptr;
  }

  Dbms_profiler_units dpu;
  dpu.unit_number = infos_.size() + 1;
  dpu.unit_type.swap(unit_type);
  dpu.unit_owner.swap(unit_owner);
  dpu.unit_name.swap(unit_name);
  dpu.unit_db = unit_db;
  dpu.type = type;
  dpu.change_version = version;
  dpu.unit_timestamp = Session_dbms_profiler_info::get_current_time();
  dpu.total_time = 0;
  dpu.need_flush = true;

  infos_.push_back(std::move(dpu));

  return &infos_.back();
}

bool Session_dbms_profiler_info::serialize(String *str) {
  if (status_ == DBMS_PROFILER_STATUS_STOP) return true;

  std::string units = serialize_data();
  uint64 run_total_time = get_run_total_time();

  char content[8192] = {0};
  sprintf(content, "{\"runid\":%lu,\"run_total_time\":%lu,\"units\":%s}",
          runid_, run_total_time, units.c_str());

  str->length(0);
  str->append(content, strlen(content));

  return false;
}

std::string Session_dbms_profiler_info::serialize_data() {
  std::string str;
  str.append("[", 1);
  char content[1024] = {0};
  bool first_uint = true;
  for (auto &info : infos_) {
    if (!first_uint) {
      str.append(",", 1);
    }

    int need_flush = info.need_flush ? 1 : 0;
    memset(content, 0, 1024);
    sprintf(content,
            "{\"need_flush\":%d, \"unit_number\":%lu, "
            "\"unit_type\":\"%s\", "
            "\"unit_owner\":\"%s\", \"unit_name\":\"%s\", "
            "\"unit_timestamp\":\"%s\", \"data\":",
            need_flush, info.unit_number, info.unit_type.c_str(),
            info.unit_owner.c_str(), info.unit_name.c_str(),
            info.unit_timestamp.c_str());
    str.append(content, strlen(content));
    info.need_flush = false;

    bool first_data = true;
    std::string tmp;
    tmp.append("[", 1);
    for (auto &d : info.data) {
      auto &data = d.second;
      if (!data->need_flush) continue;

      memset(content, 0, 1024);
      if (!first_data) {
        tmp.append(",", 1);
      }

      sprintf(content,
              "{\"linen\":%lu, \"total_occur\":%lu, \"total_time\":%lu, "
              "\"min_time\":%lu, \"max_time\":%lu}",
              data->line, data->total_occur, data->total_time, data->min_time,
              data->max_time);

      tmp.append(content, strlen(content));
      data->need_flush = false;
      first_data = false;
    }
    tmp.append("]", 1);
    str.append(tmp);

    first_uint = false;
    str.append("}", 1);
  }

  str.append("]", 1);
  return str;
}

uint64 Session_dbms_profiler_info::get_run_total_time() {
  uint64 exec_time = my_getsystime() - start_time_;
  return elapse_time_ + exec_time;
}
