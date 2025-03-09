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

#ifndef QUERY_PLAN_PLUGIN_H
#define QUERY_PLAN_PLUGIN_H
#include <vector>
#include "sql/item.h"
#include "sql/tztime.h"

class AccessPath;
class THD;
struct LEX;
class JOIN;
class RowIterator;
struct ExplainChild;

#define MYSQL_QUERY_PLAN_INTERFACE_VERSION 0x0100
extern mysql_rwlock_t THR_LOCK_query_plan;

/** Values for turbo_enable sysvar. */
enum use_turbo_plugin {
  TURBO_PLUGIN_OFF = 0,
  TURBO_PLUGIN_ON = 1,
  TURBO_PLUGIN_FORCED = 2
};

typedef void (*prepare_query_plan_func)(THD *, LEX *);
typedef void (*destroy_query_plan_func)(LEX *);
typedef bool (*has_query_plan_func)(THD *, LEX *);
typedef void (*create_query_plan_runtime_func)(THD *, JOIN *,
                                               Query_expression *);
typedef void (*free_query_plan_runtime_func)(THD *, bool);
typedef bool (*explain_query_plan_func)(std::string &,
                                        std::vector<ExplainChild> *,
                                        AccessPath *, mem_root_deque<Item *> *);
typedef void (*kill_query_plan_func)(THD *);

typedef unique_ptr_destroy_only<RowIterator> (*executor_iterator_func)(
    THD *, MEM_ROOT *, JOIN *, unique_ptr_destroy_only<RowIterator>,
    mem_root_deque<Item *> *);
typedef bool (*is_plugin_enable_func)();
typedef use_turbo_plugin (*get_turbo_enable_func)();
struct ST_QUERY_PLAN {
  int interface_version;
  prepare_query_plan_func prepare_query_plan;
  destroy_query_plan_func destroy_query_plan;
  has_query_plan_func has_query_plan;
  create_query_plan_runtime_func create_runtime;
  free_query_plan_runtime_func free_runtime;
  explain_query_plan_func explain_query;
  kill_query_plan_func kill_query;
  executor_iterator_func executor_iterator;
  is_plugin_enable_func is_plugin_enable;
  get_turbo_enable_func get_turbo_enable;
};

//////////////////////////////////////////////////////////////////////////////

extern ST_QUERY_PLAN *global_query_plan_plugin_interface;

int initialize_query_plan_plugin(st_plugin_int *plugin);
int finalize_query_plan_plugin(st_plugin_int *plugin);

void query_plan_plugin_init_globals();
void query_plan_plugin_deinit_globals();

void prepare_additional_query_plan(THD *thd, LEX *lex);
void destroy_additional_query_plan(LEX *lex);
bool has_additional_query_plan(THD *thd, LEX *lex);
bool is_plugin_enable();

void create_additional_query_plan_runtime(THD *thd, JOIN *join,
                                          Query_expression *unit);
void free_additional_query_plan_runtime(THD *thd, bool full = true);

bool explain_additional_query_plan(std::string &msg,
                                   std::vector<ExplainChild> *child,
                                   AccessPath *native_path,
                                   mem_root_deque<Item *> *native_outlist);

void kill_additional_query_plan(THD *thd);

unique_ptr_destroy_only<RowIterator> create_additional_query_plan_iterator(
    THD *thd, MEM_ROOT *mem_root, JOIN *join,
    unique_ptr_destroy_only<RowIterator> src, mem_root_deque<Item *> *fields);

#endif
