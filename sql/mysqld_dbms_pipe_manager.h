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

#ifndef MYSQLD_DBMS_PIPE_MANAGER_INCLUDED
#define MYSQLD_DBMS_PIPE_MANAGER_INCLUDED

#include <list>
#include <string>
#include <unordered_map>
#include "mysql/psi/mysql_cond.h"
#include "mysql/psi/mysql_mutex.h"
#include "sql_string.h"

class THD;

struct Dbms_pipe {
  /*
  the pipe is created implicit
  true: the pipe is created implicitly by the function send_message or
  receive_message.
  false: the pipe is created explicitly by the function
  create_pipe
  */
  bool implicit_create;
  /*
  true: private
  false: public, the pipe is created implicitly always public
  */
  bool self;
  std::string owner;
  std::list<std::string> msg;
  int maxsize;
  int size;
  mysql_cond_t cond;
  Dbms_pipe();
  ~Dbms_pipe() { mysql_cond_destroy(&cond); }
};

class Global_dbms_pipe_manager {
 public:
  static Global_dbms_pipe_manager *get_instance() {
    assert(instance);
    return instance;
  }

  static bool create_instance();
  static void destroy_instance();

  int create_pipe(THD *thd, String *pipename, int pipesize, bool self);
  int remove_pipe(THD *thd, String *pipename);
  int send_message(THD *thd, String *pipename, int timeout, int maxpipesize,
                   std::string &&msg);
  int receive_message(THD *thd, String *pipename, int timeout);
  int purge(THD *thd, String *pipename);

 private:
  Global_dbms_pipe_manager();
  ~Global_dbms_pipe_manager();

  static Global_dbms_pipe_manager *instance;

 private:
  std::unordered_map<std::string, std::shared_ptr<Dbms_pipe>> dbms_pipe_map;
  mysql_mutex_t LOCK_dbms_pipe_map_mutex;
};

#endif /* MYSQLD_DBMS_PIPE_MANAGER_INCLUDED */
