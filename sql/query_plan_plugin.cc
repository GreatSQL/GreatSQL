/* Copyright (c) 2025, GreatDB Software Co., Ltd.

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

#include "sql/query_plan_plugin.h"
#include "mysql/plugin.h"
#include "mysqld_thd_manager.h"
#include "sql/item_timefunc.h"
#include "sql/join_optimizer/access_path.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"
#include "sql/sql_plugin.h"

mysql_rwlock_t THR_LOCK_query_plan;

#ifdef HAVE_PSI_INTERFACE
#include "mysql/psi/mysql_memory.h"
static PSI_memory_key key_memory_query_plan_plugin_data;
static PSI_memory_info key_query_plan_pluing_data[] = {
    {&key_memory_query_plan_plugin_data, "query_plan_plugin_data", 0, 0,
     PSI_DOCUMENT_ME}};

static PSI_rwlock_key key_query_plan_plugin_lock;

static PSI_rwlock_info all_query_plan_rwlocks[] = {
    {&key_query_plan_plugin_lock, "query_plan_plugin_lock", PSI_FLAG_SINGLETON,
     0, PSI_DOCUMENT_ME},
};
static void init_query_plan_psi_keys(void) {
  const char *category = "query_plan_plugin";
  mysql_rwlock_register(category, all_query_plan_rwlocks,
                        array_elements(all_query_plan_rwlocks));
  mysql_memory_register(category, key_query_plan_pluing_data,
                        array_elements(key_query_plan_pluing_data));
}

#endif /* HAVE_PSI_INTERFACE  */

static bool initialized = false;
void query_plan_plugin_init_globals() {
  if (initialized) return;
#ifdef HAVE_PSI_INTERFACE
  init_query_plan_psi_keys();
  mysql_rwlock_init(key_query_plan_plugin_lock, &THR_LOCK_query_plan);
#else
  mysql_rwlock_init(0, &THR_LOCK_query_plan);
#endif
  initialized = true;
}

void query_plan_plugin_deinit_globals() {
  if (!initialized) return;
  mysql_rwlock_destroy(&THR_LOCK_query_plan);
  initialized = false;
}

class Query_plan_lock_guard {
  mysql_mutex_t *m_mutex;

 public:
  Query_plan_lock_guard(mysql_mutex_t *mutex) : m_mutex(mutex) {
    mysql_mutex_lock(m_mutex);
  }
  ~Query_plan_lock_guard() { mysql_mutex_unlock(m_mutex); }
};

std::function<void(THD *, LEX *)> prepare_plan_context_func = nullptr;
std::function<void(LEX *)> destroy_plan_context_func = nullptr;
std::function<bool(THD *, LEX *)> has_plan_context_func = nullptr;
std::function<void(THD *, JOIN *, Query_expression *)> create_runtime_func =
    nullptr;
std::function<void(THD *, bool)> free_runtime_func = nullptr;
std::function<void(THD *)> kill_query_func = nullptr;

std::function<bool(std::string &, std::vector<ExplainChild> *, AccessPath *,
                   mem_root_deque<Item *> *)>
    explain_query_func = nullptr;

std::function<unique_ptr_destroy_only<RowIterator>(
    THD *, MEM_ROOT *, JOIN *, unique_ptr_destroy_only<RowIterator>,
    mem_root_deque<Item *> *)>
    query_plan_iterator_func = nullptr;

/* 0: uninstall turbo
 * 1: install turbo
 * >1: get active sql threads
 */
static std::atomic<int> enable_check_ref{0};

bool quick_check_plugin_enable() { return false; }
std::function<bool()> quick_check_plugin_enable_func =
    quick_check_plugin_enable;

static bool check_ref_add_or_exit(int exit_val) {
  while (1) {
    int cur = enable_check_ref;
    if (cur == exit_val) {
      return false;
    }
    if (enable_check_ref.compare_exchange_weak(cur, cur + 1)) {
      return true;
    }
  }
}

void prepare_additional_query_plan(THD *thd [[maybe_unused]],
                                   LEX *lex [[maybe_unused]]) {
#ifdef HAVE_QUERY_PLAN_PLUGIN
  if (check_ref_add_or_exit(0)) {
    lex->query_plan_plugin_enabled = quick_check_plugin_enable_func();
    enable_check_ref.fetch_sub(1);
  }
  if (!lex->query_plan_plugin_enabled) {
    return;
  }
  if (thd) {
    Query_plan_lock_guard lock(&thd->LOCK_thd_query_plan_plugin);
    if (thd->query_plan_plugin_interface &&
        thd->query_plan_plugin_interface->prepare_query_plan) {
      thd->query_plan_plugin_interface->prepare_query_plan(thd, lex);
    }
  }
#endif
}
void destroy_additional_query_plan(LEX *lex [[maybe_unused]]) {
#ifdef HAVE_QUERY_PLAN_PLUGIN
  auto thd = current_thd;
  if (thd) {
    Query_plan_lock_guard lock(&thd->LOCK_thd_query_plan_plugin);
    if (thd->query_plan_plugin_interface &&
        thd->query_plan_plugin_interface->destroy_query_plan) {
      thd->query_plan_plugin_interface->destroy_query_plan(lex);
    }
  }
#endif
}

bool has_additional_query_plan(THD *thd [[maybe_unused]],
                               LEX *lex [[maybe_unused]]) {
  bool ret = false;
#ifdef HAVE_QUERY_PLAN_PLUGIN
  if (!enable_check_ref.load()) return false;
  if (thd) {
    Query_plan_lock_guard lock(&thd->LOCK_thd_query_plan_plugin);
    if (thd->query_plan_plugin_interface &&
        thd->query_plan_plugin_interface->has_query_plan) {
      ret = thd->query_plan_plugin_interface->has_query_plan(thd, lex);
    }
  }
#endif
  return ret;
}

void create_additional_query_plan_runtime(THD *thd [[maybe_unused]],
                                          JOIN *join [[maybe_unused]],
                                          Query_expression *unit
                                          [[maybe_unused]]) {
#ifdef HAVE_QUERY_PLAN_PLUGIN

  if (!thd->lex->query_plan_plugin_enabled) {
    return;
  }

  if (thd) {
    Query_plan_lock_guard lock(&thd->LOCK_thd_query_plan_plugin);
    if (thd->query_plan_plugin_interface &&
        thd->query_plan_plugin_interface->create_runtime) {
      thd->query_plan_plugin_interface->create_runtime(thd, join, unit);
    }
  }
#endif
}
void free_additional_query_plan_runtime(THD *thd [[maybe_unused]],
                                        bool full [[maybe_unused]]) {
#ifdef HAVE_QUERY_PLAN_PLUGIN
  if (!thd->lex->query_plan_plugin_enabled) {
    return;
  }

  if (thd) {
    Query_plan_lock_guard lock(&thd->LOCK_thd_query_plan_plugin);
    if (thd->query_plan_plugin_interface &&
        thd->query_plan_plugin_interface->free_runtime) {
      thd->query_plan_plugin_interface->free_runtime(thd, full);
    }
  }
#endif
}
bool explain_additional_query_plan(std::string &msg [[maybe_unused]],
                                   std::vector<ExplainChild> *child
                                   [[maybe_unused]],
                                   AccessPath *native_path [[maybe_unused]],
                                   mem_root_deque<Item *> *native_outlist
                                   [[maybe_unused]]) {
#ifdef HAVE_QUERY_PLAN_PLUGIN
  auto thd = current_thd;
  if (thd) {
    Query_plan_lock_guard lock(&thd->LOCK_thd_query_plan_plugin);
    if (thd->query_plan_plugin_interface &&
        thd->query_plan_plugin_interface->explain_query) {
      return thd->query_plan_plugin_interface->explain_query(
          msg, child, native_path, native_outlist);
    }
  }
#endif
  return false;
}
void kill_additional_query_plan(THD *thd [[maybe_unused]]) {
#ifdef HAVE_QUERY_PLAN_PLUGIN
  if (thd) {
    Query_plan_lock_guard lock(&thd->LOCK_thd_query_plan_plugin);
    if (thd->query_plan_plugin_interface &&
        thd->query_plan_plugin_interface->kill_query) {
      thd->query_plan_plugin_interface->kill_query(thd);
    }
  }
#endif
}

unique_ptr_destroy_only<RowIterator> create_additional_query_plan_iterator(
    THD *thd [[maybe_unused]], MEM_ROOT *mem_root [[maybe_unused]],
    JOIN *join [[maybe_unused]],
    unique_ptr_destroy_only<RowIterator> src [[maybe_unused]],
    mem_root_deque<Item *> *fields [[maybe_unused]]) {
#ifdef HAVE_QUERY_PLAN_PLUGIN
  if (thd->query_plan_plugin_interface != nullptr) {
    Query_plan_lock_guard lock(&thd->LOCK_thd_query_plan_plugin);
    if (thd->query_plan_plugin_interface &&
        thd->query_plan_plugin_interface->executor_iterator) {
      return thd->query_plan_plugin_interface->executor_iterator(
          thd, mem_root, join, std::move(src), fields);
    }
  }
#endif
  return src;
}

#ifdef HAVE_QUERY_PLAN_PLUGIN
class set_query_plan_plugin_all_conn : public Do_THD_Impl {
  void operator()(THD *thd) override {
    thd->set_query_plan_plugin_interface(global_query_plan_plugin_interface);
  }
};
#endif

ST_QUERY_PLAN *global_query_plan_plugin_interface = nullptr;

int initialize_query_plan_plugin(st_plugin_int *plugin) {
  /* Make the interface info more easily accessible */
  plugin->data = plugin->plugin->info;
  if (plugin->plugin->init && plugin->plugin->init(plugin)) {
    return 1;
  }
#ifdef HAVE_QUERY_PLAN_PLUGIN
  mysql_rwlock_wrlock(&THR_LOCK_query_plan);
  global_query_plan_plugin_interface = (ST_QUERY_PLAN *)(plugin->plugin->info);
  auto global_thd = Global_THD_manager::get_instance();
  set_query_plan_plugin_all_conn all_conn;
  global_thd->do_for_all_thd(&all_conn);
  mysql_rwlock_unlock(&THR_LOCK_query_plan);
  quick_check_plugin_enable_func =
      global_query_plan_plugin_interface->is_plugin_enable;
  assert(enable_check_ref == 0);
  enable_check_ref++;
#endif
  return 0;
}

int finalize_query_plan_plugin(st_plugin_int *plugin) {
  if (plugin->plugin->deinit && plugin->plugin->deinit(plugin)) {
    return 1;
  }
  plugin->data = nullptr;
#ifdef HAVE_QUERY_PLAN_PLUGIN

  // if cur is 0 means install has error
  while (enable_check_ref.load() != 0) {
    auto cur = enable_check_ref.load();
    if (cur == 1 && enable_check_ref.compare_exchange_weak(cur, 0)) {
      break;
    }
  }
  quick_check_plugin_enable_func = quick_check_plugin_enable;
  mysql_rwlock_wrlock(&THR_LOCK_query_plan);
  global_query_plan_plugin_interface = nullptr;
  auto global_thd = Global_THD_manager::get_instance();
  set_query_plan_plugin_all_conn all_conn;
  global_thd->do_for_all_thd(&all_conn);
  mysql_rwlock_unlock(&THR_LOCK_query_plan);

#endif
  return 0;
}

/** END */
