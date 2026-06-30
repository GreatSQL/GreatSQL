/* Copyright (c) 2026, GreatDB Software Co., Ltd.

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

#ifndef PLUGIN_GDB_PLAN_BASELINE_H
#define PLUGIN_GDB_PLAN_BASELINE_H
/*
  Includes only from server include folder.
*/
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include <mysql/components/my_service.h>
#include <mysql/components/service_implementation.h>
#include <mysql/components/services/log_builtins.h>
#include <mysql/components/services/registry.h>
#include "mysql/plugin.h"
#include "pfs.h"
#include "psi.h"
#include "sql/plugin_plan_baseline.h"
#include "sql/sql_plugin.h"

extern const char *plan_baseline_database;
extern ulonglong explain_count;

struct plan_baseline_variables {
  ulong refresh_interval;
  bool enable_summary;
  bool enable_persistent;
  ulong max_rows_count;
  ulong max_tables_count;
};

extern plan_baseline_variables global_plan_baseline_var;
extern mysql_mutex_t g_report_mutex;

extern SERVICE_TYPE(registry) * reg_srv;

#ifndef ACQUIRE_SERVICE_BY_NAME
#define ACQUIRE_SERVICE_BY_NAME(r, ptr, name)                               \
  {                                                                         \
    my_h_service mysql_service;                                             \
    if ((r)->acquire(#name, &mysql_service)) {                              \
      LogPluginErrMsg(ERROR_LEVEL, ER_LOG_PRINTF_MSG,                       \
                      "%s acquire_service failed", #name);                  \
      break;                                                                \
    }                                                                       \
    (ptr) = reinterpret_cast<SERVICE_TYPE_NO_CONST(name) *>(mysql_service); \
  }

#endif

#ifndef RELEASE_SERVIE
#define RELEASE_SERVIE(r, ptr)                         \
  if ((ptr) && (r)) {                                  \
    (r)->release(reinterpret_cast<my_h_service>(ptr)); \
    (ptr) = nullptr;                                   \
  }
#endif

std::vector<std::string> split(const std::string &str, char delimiter,
                               int count);

struct explain_query_result {
  std::shared_ptr<std::string> digest_text;
  std::shared_ptr<std::string> access_path_serialization;
  // the min cost of plan in digest sql
  double cost;
  ulonglong id;
  explain_query_result(std::shared_ptr<std::string> m_digest_text,
                       std::shared_ptr<std::string> m_access_path_serialization,
                       double m_cost, ulonglong m_id)
      : digest_text(m_digest_text),
        access_path_serialization(m_access_path_serialization),
        cost(m_cost),
        id(m_id) {}
};

struct explain_query_sql {
  std::shared_ptr<std::string> digest_hash;
  // this id == explain_query_result->id
  ulonglong id;
  double cost;
  ulonglong rows;
  explain_query_sql(std::shared_ptr<std::string> m_digest_hash, ulonglong m_id,
                    ulonglong m_cost, ulonglong m_rows)
      : digest_hash(m_digest_hash), id(m_id), cost(m_cost), rows(m_rows) {}
};

namespace std {
template <>
struct hash<std::shared_ptr<std::string>> {
  size_t operator()(const std::shared_ptr<std::string> &ptr) const {
    if (!ptr) return 0;
    return std::hash<std::string>{}(*ptr);
  }
};
}  // namespace std

/** A Hasher that hashes std::strings according to a MySQL collation. */
class gdb_Collation_hasher {
 public:
  explicit gdb_Collation_hasher(const CHARSET_INFO *cs_arg)
      : cs(cs_arg), hash_sort(cs->coll->hash_sort) {}

  size_t operator()(const std::shared_ptr<std::string> &s) const {
    uint64 nr1 = 1, nr2 = 4;
    hash_sort(cs, pointer_cast<const uchar *>(s->data()), s->size(), &nr1,
              &nr2);
    return nr1;
  }

 private:
  const CHARSET_INFO *cs;
  decltype(cs->coll->hash_sort) hash_sort;
};

/** A KeyEqual that compares std::strings according to a MySQL collation. */
class gdb_Collation_key_equal {
 public:
  explicit gdb_Collation_key_equal(const CHARSET_INFO *cs_arg)
      : cs(cs_arg), strnncollsp(cs->coll->strnncollsp) {}

  size_t operator()(const std::shared_ptr<std::string> &a,
                    const std::shared_ptr<std::string> &b) const {
    return strnncollsp(cs, pointer_cast<const uchar *>(a->data()), a->size(),
                       pointer_cast<const uchar *>(b->data()), b->size()) == 0;
  }

 private:
  const CHARSET_INFO *cs;
  decltype(cs->coll->strnncollsp) strnncollsp;
};

template <class Key, class Value, class Hash = std::hash<Key>>
class gdb_collation_unordered_map
    : public std::unordered_map<Key, Value, gdb_Collation_hasher,
                                gdb_Collation_key_equal,
                                std::allocator<std::pair<const Key, Value>>> {
 public:
  gdb_collation_unordered_map(const CHARSET_INFO *cs)
      : std::unordered_map<Key, Value, gdb_Collation_hasher,
                           gdb_Collation_key_equal,
                           std::allocator<std::pair<const Key, Value>>>(
            /*bucket_count=*/10, gdb_Collation_hasher(cs),
            gdb_Collation_key_equal(cs),
            std::allocator<std::pair<const Key, Value>>()) {}
};

// key : db#digest_hash#plan_name
extern gdb_collation_unordered_map<std::shared_ptr<std::string>,
                                   std::unique_ptr<explain_query_result>>
    plan_hash_map;
// key : db#query_sql
extern gdb_collation_unordered_map<std::shared_ptr<std::string>,
                                   std::unique_ptr<explain_query_sql>>
    plan_sql_map;
extern std::deque<ulonglong> map_id_list;

extern mysql_mutex_t lock_plan_hash_map;
extern mysql_mutex_t lock_plan_sql_map;
extern mysql_mutex_t lock_map_id_list;

class Mutex_guard_plan {
 public:
  Mutex_guard_plan(mysql_mutex_t &mutex_hash, mysql_mutex_t &mutex_sql,
                   mysql_mutex_t &mutex_id)
      : m_mutex_hash(mutex_hash), m_mutex_sql(mutex_sql), m_mutex_id(mutex_id) {
    mysql_mutex_lock(&m_mutex_hash);
    mysql_mutex_lock(&m_mutex_sql);
    mysql_mutex_lock(&m_mutex_id);
  }
  ~Mutex_guard_plan() {
    mysql_mutex_unlock(&m_mutex_hash);
    mysql_mutex_unlock(&m_mutex_sql);
    mysql_mutex_unlock(&m_mutex_id);
  }

 private:
  mysql_mutex_t &m_mutex_hash;
  mysql_mutex_t &m_mutex_sql;
  mysql_mutex_t &m_mutex_id;
};

void update_plan_baseline_enable_summary(MYSQL_THD, SYS_VAR *, void *var_ptr,
                                         const void *save);
void update_max_rows_count(MYSQL_THD, SYS_VAR *, void *var_ptr,
                           const void *save);
#endif
