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

#ifndef SESSION_PACKAGE_DATA_INCLUDED
#define SESSION_PACKAGE_DATA_INCLUDED

#include <list>
#include <map>
#include <stack>
#include <string>
#include <string_view>
#include "sql_string.h"

enum enum_dbms_profiler_status {
  DBMS_PROFILER_STATUS_STOP,
  DBMS_PROFILER_STATUS_RUNING,
  DBMS_PROFILER_STATUS_PAUSE,
};

struct Dbms_profiler_data {
  bool need_flush;
  uint64 line;
  uint64 total_occur;
  uint64 total_time;
  uint64 min_time;
  uint64 max_time;
  uint64 exec_begin_time;
  std::stack<uint64> s;

  void before_exec();
  void after_exec();
};

struct Dbms_profiler_units {
  bool need_flush;
  uint64 unit_number;
  std::string unit_type;
  std::string unit_owner;
  std::string unit_name;
  std::string unit_db;
  int type;
  int64 change_version;
  std::string unit_timestamp;
  uint64 total_time;
  std::map<uint64, std::shared_ptr<Dbms_profiler_data>>
      data;  // key Dbms_profiler_data.line

  std::shared_ptr<Dbms_profiler_data> find_data(uint64 line);
  std::shared_ptr<Dbms_profiler_data> create_data(uint64 line);
};

class Session_dbms_profiler_info {
 public:
  static std::string get_current_time();

  Session_dbms_profiler_info();
  ~Session_dbms_profiler_info();
  void clear();

  int status();
  int start(uint64 runid);
  int pause();
  int resume();
  int stop();
  bool serialize(String *str);

  Dbms_profiler_units *find_uint(const std::string &unit_db, int type,
                                 const std::string &unit_name, int64 version);
  Dbms_profiler_units *create_uint(std::string &&unit_type,
                                   std::string &&unit_owner,
                                   std::string &&unit_name,
                                   const std::string &unit_db, int type,
                                   int64 version);

 private:
  std::string serialize_data();
  uint64 get_run_total_time();

 private:
  int status_;
  uint64 runid_;
  uint64 start_time_;
  uint64 elapse_time_;
  std::list<Dbms_profiler_units> infos_;
};

#endif /* SESSION_PACKAGE_DATA_INCLUDED */
