/* -*- C++ -*- */
/* Copyright (c) 2002, 2022, Oracle and/or its affiliates. All rights reserved.
   Copyright (c) 2023, GreatDB Software Co., Ltd.

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

#ifndef _SP_CACHE_H_
#define _SP_CACHE_H_

#include <sys/types.h>
#include <atomic>
#include <map>
#include <string>
#include "my_inttypes.h"
#include "mysql/psi/mysql_mutex.h"

/*
  Stored procedures/functions cache. This is used as follows:
   * Each thread has its own cache.
   * Each sp_head object is put into its thread cache before it is used, and
     then remains in the cache until deleted.
*/

class sp_cache;
class sp_head;
class sp_name;

/*
  Cache usage scenarios:
  1. SP execution in thread:
  1.1 While holding sp_head* pointers:

    // look up a routine in the cache (no checks if it is up to date or not)
    sp_cache_lookup();

    sp_cache_insert();
    sp_cache_invalidate();

  1.2 When not holding any sp_head* pointers:
    sp_cache_flush_obsolete();

  2. Before thread exit:
    sp_cache_clear();
*/

void sp_cache_clear(sp_cache **cp);
void sp_cache_insert(sp_cache **cp, sp_head *sp);
sp_head *sp_cache_lookup(sp_cache **cp, char *qname, size_t length);
sp_head *sp_cache_lookup(sp_cache **cp, const sp_name *name);
void sp_cache_invalidate();
void sp_cache_flush_obsolete(sp_cache **cp, sp_head **sp);
int64 sp_cache_version();
void sp_cache_enforce_limit(sp_cache *cp, ulong upper_limit_for_elements);

// sp_change_version
class Sp_version_changed {
 public:
  static Sp_version_changed *get_instance() {
    assert(instance);
    return instance;
  }

  static bool create_instance();
  static void destroy_instance();

  void sp_version_changed(const std::string &db, int type,
                          const std::string &name);
  void sp_version_clear(const std::string &db, int type,
                        const std::string &name);
  int64 sp_version_lookup(const std::string &db, int type,
                          const std::string &name);

  int64 sp_anonymous_version() { return anonymous_version++; }

 private:
  Sp_version_changed();
  ~Sp_version_changed();

  static Sp_version_changed *instance;
  std::atomic<int64> anonymous_version;

  mysql_mutex_t LOCK_sp_version_change_mutex;
  // key:db, type, name   value:version
  std::map<std::string, std::map<int, std::map<std::string, int64>>>
      map_version;
};

#endif /* _SP_CACHE_H_ */
