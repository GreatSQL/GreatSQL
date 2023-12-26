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

#include "mysqld_dbms_pipe_manager.h"
#include <algorithm>
#include <new>
#include "mysqld.h"
#include "sql/sql_class.h"

class my_lock_guard {
 public:
  my_lock_guard(mysql_mutex_t *m) {
    mutex = m;
    mysql_mutex_lock(mutex);
  }
  ~my_lock_guard() { mysql_mutex_unlock(mutex); }

 private:
  mysql_mutex_t *mutex;
};

static void my_to_upper(std::string &source) {
  for (uint i = 0; i < source.length(); i++) {
    source[i] = std::toupper(source[i]);
  }
}

Dbms_pipe::Dbms_pipe()
    : implicit_create(false), self(false), maxsize(0), size(0) {
  mysql_cond_init(key_COND_dbms_pipe_cond_var, &cond);
}

Global_dbms_pipe_manager *Global_dbms_pipe_manager::instance = nullptr;

Global_dbms_pipe_manager::Global_dbms_pipe_manager() {
  mysql_mutex_init(key_LOCK_dbms_pipe_map_mutex, &LOCK_dbms_pipe_map_mutex,
                   MY_MUTEX_INIT_FAST);
}

Global_dbms_pipe_manager::~Global_dbms_pipe_manager() {
  dbms_pipe_map.clear();
  mysql_mutex_destroy(&LOCK_dbms_pipe_map_mutex);
}

bool Global_dbms_pipe_manager::create_instance() {
  if (instance == nullptr)
    instance = new (std::nothrow) Global_dbms_pipe_manager();
  return (instance == nullptr);
}

void Global_dbms_pipe_manager::destroy_instance() {
  delete instance;
  instance = nullptr;
}

/*
  return
    0 :succ
   -1 :privilege error
*/
int Global_dbms_pipe_manager::create_pipe(THD *thd, String *pipename,
                                          int pipesize, bool self) {
  std::string name(pipename->c_ptr(), pipename->length());
  my_to_upper(name);
  const LEX_CSTRING &u = thd->security_context()->user();
  std::string user(u.str, u.length);

  int ret = 0;
  mysql_mutex_lock(&LOCK_dbms_pipe_map_mutex);
  auto it = dbms_pipe_map.find(name);
  if (it != dbms_pipe_map.end()) {
    if (it->second->implicit_create && it->second->size == 0) {
      it->second->implicit_create = false;
      it->second->self = self;
      it->second->owner.swap(user);
      if (pipesize > it->second->maxsize) {
        it->second->maxsize = pipesize;
        mysql_cond_signal(&it->second->cond);
      }
    } else {
      if (self ^ it->second->self) {
        ret = -1;
      } else {
        if (self && it->second->owner.compare(user) != 0) {
          ret = -1;
        }
      }
    }
  } else {
    std::shared_ptr<Dbms_pipe> dbms_pipe(new Dbms_pipe);
    dbms_pipe->implicit_create = false;
    dbms_pipe->self = self;
    dbms_pipe->owner.swap(user);
    dbms_pipe->maxsize = pipesize;
    dbms_pipe->size = 0;
    dbms_pipe_map.emplace(std::move(name), dbms_pipe);
  }

  mysql_mutex_unlock(&LOCK_dbms_pipe_map_mutex);

  return ret;
}

/*
  return
    0 :succ
   -1 :privilege error
*/
int Global_dbms_pipe_manager::remove_pipe(THD *thd, String *pipename) {
  std::string name(pipename->c_ptr(), pipename->length());
  my_to_upper(name);
  const LEX_CSTRING &u = thd->security_context()->user();
  std::string user(u.str, u.length);

  my_lock_guard lock(&LOCK_dbms_pipe_map_mutex);
  auto it = dbms_pipe_map.find(name);
  if (it == dbms_pipe_map.end()) {
    return 0;
  }

  if (it->second->self && it->second->owner.compare(user) != 0) {
    return -1;
  }

  dbms_pipe_map.erase(it);

  return 0;
}

/*
  maxpipesize: if maxpipesize greater than create_pipe pipesize, it will cause
  the dbms_pipe maxpipesize be increased. if smaller using the existing value.

  return
    0 :succ
    1 :timeout
   -1 :privilege error
*/
int Global_dbms_pipe_manager::send_message(THD *thd, String *pipename,
                                           int timeout, int maxpipesize,
                                           std::string &&msg) {
  std::string name(pipename->c_ptr(), pipename->length());
  my_to_upper(name);
  const LEX_CSTRING &u = thd->security_context()->user();
  std::string user(u.str, u.length);
  int maxsize = maxpipesize;

  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  ts.tv_sec += timeout;
  mysql_mutex_lock(&LOCK_dbms_pipe_map_mutex);

  std::shared_ptr<Dbms_pipe> dbms_pipe;
  auto it = dbms_pipe_map.find(name);
  if (it == dbms_pipe_map.end()) {
    dbms_pipe = std::shared_ptr<Dbms_pipe>(new Dbms_pipe);
    dbms_pipe->implicit_create = true;
    dbms_pipe->self = false;
    dbms_pipe->owner.swap(user);
    dbms_pipe->maxsize = maxpipesize;
    dbms_pipe->size = 0;
    dbms_pipe_map.emplace(std::move(name), dbms_pipe);
  } else {
    dbms_pipe = it->second;
    if (dbms_pipe->self && dbms_pipe->owner.compare(user) != 0) {
      mysql_mutex_unlock(&LOCK_dbms_pipe_map_mutex);
      return -1;
    }
  }

  if (dbms_pipe->maxsize > maxsize) maxsize = dbms_pipe->maxsize;
  bool need_cond_exit = false;
  if (maxsize >= dbms_pipe->size + (int)msg.length()) {
    dbms_pipe->maxsize = maxsize;
  } else {
    thd->ENTER_COND(&dbms_pipe->cond, &LOCK_dbms_pipe_map_mutex,
                    &stage_dbms_pipe_receive_message, nullptr);
    need_cond_exit = true;
    while (dbms_pipe->maxsize < dbms_pipe->size + (int)msg.length()) {
      int error = mysql_cond_timedwait(&dbms_pipe->cond,
                                       &LOCK_dbms_pipe_map_mutex, &ts);
      if (is_timeout(error) || thd->killed) {
        /*
          why use_count() <= 2
          dbms_pipe variable ref 1
          dbms_pipe_map ref 1
          so if use_count() <= 2 means just current user ref the dbms_pipe.
          receive_message for the same reason
        */
        if (dbms_pipe->implicit_create && dbms_pipe->size == 0 &&
            dbms_pipe.use_count() <= 2) {
          dbms_pipe_map.erase(name);
        }
        mysql_mutex_unlock(&LOCK_dbms_pipe_map_mutex);
        thd->EXIT_COND(nullptr);
        return 1;
      }
      // check pri again
      if (dbms_pipe->self && dbms_pipe->owner.compare(user) != 0) {
        mysql_mutex_unlock(&LOCK_dbms_pipe_map_mutex);
        thd->EXIT_COND(nullptr);
        return -1;
      }
    }
  }

  dbms_pipe->size += msg.length();
  dbms_pipe->msg.push_back(std::forward<std::string>(msg));
  mysql_mutex_unlock(&LOCK_dbms_pipe_map_mutex);
  mysql_cond_signal(&dbms_pipe->cond);
  if (need_cond_exit) {
    thd->EXIT_COND(nullptr);
  }

  return 0;
}

/*
  return
    0 :succ
    1 :timeout
   -1 :privilege error
*/
int Global_dbms_pipe_manager::receive_message(THD *thd, String *pipename,
                                              int timeout) {
  std::string name(pipename->c_ptr(), pipename->length());
  my_to_upper(name);
  const LEX_CSTRING &u = thd->security_context()->user();
  std::string user(u.str, u.length);

  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  ts.tv_sec += timeout;
  mysql_mutex_lock(&LOCK_dbms_pipe_map_mutex);

  std::shared_ptr<Dbms_pipe> dbms_pipe;
  auto it = dbms_pipe_map.find(name);
  if (it == dbms_pipe_map.end()) {
    dbms_pipe = std::shared_ptr<Dbms_pipe>(new Dbms_pipe);
    dbms_pipe->implicit_create = true;
    dbms_pipe->self = false;
    dbms_pipe->owner.swap(user);
    dbms_pipe->maxsize = 0;
    dbms_pipe->size = 0;
    dbms_pipe_map.emplace(name, dbms_pipe);
  } else {
    dbms_pipe = it->second;
    if (dbms_pipe->self && dbms_pipe->owner.compare(user) != 0) {
      mysql_mutex_unlock(&LOCK_dbms_pipe_map_mutex);
      return -1;
    }
  }

  bool need_cond_exit = false;
  if (dbms_pipe->msg.empty()) {
    thd->ENTER_COND(&dbms_pipe->cond, &LOCK_dbms_pipe_map_mutex,
                    &stage_dbms_pipe_receive_message, nullptr);
    need_cond_exit = true;
    while (dbms_pipe->msg.empty()) {
      int error = mysql_cond_timedwait(&dbms_pipe->cond,
                                       &LOCK_dbms_pipe_map_mutex, &ts);
      if (is_timeout(error) || thd->killed) {
        if (dbms_pipe->implicit_create && dbms_pipe->msg.empty() &&
            dbms_pipe.use_count() <= 2) {
          dbms_pipe_map.erase(name);
        }
        mysql_mutex_unlock(&LOCK_dbms_pipe_map_mutex);
        thd->EXIT_COND(nullptr);
        return 1;
      }
      // check pri again
      if (dbms_pipe->self && dbms_pipe->owner.compare(user) != 0) {
        mysql_mutex_unlock(&LOCK_dbms_pipe_map_mutex);
        thd->EXIT_COND(nullptr);
        return -1;
      }
    }
  }

  thd->dbms_pipe_receive_message.swap(dbms_pipe->msg.front());
  dbms_pipe->msg.pop_front();
  dbms_pipe->size -= thd->dbms_pipe_receive_message.size();
  if (dbms_pipe->implicit_create && dbms_pipe->msg.empty() &&
      dbms_pipe.use_count() <= 2) {
    dbms_pipe_map.erase(name);
  }

  mysql_mutex_unlock(&LOCK_dbms_pipe_map_mutex);
  if (need_cond_exit) {
    thd->EXIT_COND(nullptr);
  }

  return 0;
}

/*
  return
    0 :succ
   -1 :privilege error
*/
int Global_dbms_pipe_manager::purge(THD *thd, String *pipename) {
  std::string name(pipename->c_ptr(), pipename->length());
  my_to_upper(name);
  const LEX_CSTRING &u = thd->security_context()->user();
  std::string user(u.str, u.length);

  my_lock_guard lock(&LOCK_dbms_pipe_map_mutex);
  auto it = dbms_pipe_map.find(name);
  if (it == dbms_pipe_map.end()) {
    return 0;
  }

  std::shared_ptr<Dbms_pipe> dbms_pipe = it->second;
  if (dbms_pipe->self && dbms_pipe->owner.compare(user) != 0) {
    return -1;
  }

  dbms_pipe->msg.clear();
  if (dbms_pipe->implicit_create) {
    dbms_pipe_map.erase(it);
  }

  return 0;
}
