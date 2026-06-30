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

#ifndef PLUGIN_GDB_hist_sql_plan_H
#define PLUGIN_GDB_hist_sql_plan_H
#include <mysql/components/service_implementation.h>
#include <mysql/components/services/pfs_plugin_table_service.h>
#include <map>
#include <vector>
#include "mysql/components/services/log_builtins.h"
#include "pfs.h"
#include "plan_baseline.h"
#include "thr_mutex.h"

namespace greatdb_plan_baseline {

#define GENERATE_ENUM(x, y, z) E_##x,
#define GENERATE_VAL(x, y, z) data.emplace_back(std::make_unique<y>());
#define GENERATE_STRING(x, y, z) "," #x " " z

#ifndef FOREACH_HIST_SQL_FIELED
#define FOREACH_HIST_SQL_FIELED(F)          \
  F(id, PSI_Val_ulonglong, "bigint")        \
  F(db_name, PSI_Val_string, "varchar(64)") \
  F(operation, PSI_Val_blob, "longtext")
#endif

class hist_sql_plan_data : public Gdb_plan_baseline_pfs_data {
  enum DATA_FIELD_E { FOREACH_HIST_SQL_FIELED(GENERATE_ENUM) };
  malloc_unordered_map<std::shared_ptr<std::string>,
                       std::unique_ptr<explain_query_result>>::iterator
      iterator;
  ulonglong pos;

 public:
  hist_sql_plan_data() : pos(0) {}
  int Init() override;
  int GetData(std::vector<std::unique_ptr<PSI_Val>> &data, uint32_t position,
              uint32_t index) override;
  int End() override;
};

class hist_sql_plan_table : public Gdb_plan_baseline_pfs_table {
  std::vector<std::unique_ptr<PSI_Val>> data;

 public:
  hist_sql_plan_table() { FOREACH_HIST_SQL_FIELED(GENERATE_VAL) }
  int rnd_init() override;
  int rnd_next() override;
  int rnd_end() override;
  uint32_t get_row_count() override { return plan_hash_map.size(); }
  int read_column_value(PSI_field *field, uint32_t index) override;
};

class hist_sql_plan : public Gdb_plan_baseline_pfs_base {
  typedef Gdb_plan_baseline_pfs_base super;

 private:
  hist_sql_plan() = default;
  hist_sql_plan(const hist_sql_plan &) = delete;
  hist_sql_plan(hist_sql_plan &&) = delete;
  hist_sql_plan &operator=(const hist_sql_plan &) = delete;
  hist_sql_plan &operator=(hist_sql_plan &&) = delete;

 public:
  static hist_sql_plan *get_instance() {
    static hist_sql_plan instance;
    return &instance;
  }

 public:
  const char *tabDef = FOREACH_HIST_SQL_FIELED(GENERATE_STRING);
  bool init() override {
    if (super::_init(std::make_unique<hist_sql_plan_data>())) {
      return true;
    }
    auto table = get_proxy_share();
    table->m_table_name = "gdb_hist_sql_plan";
    table->m_table_name_length = strlen(table->m_table_name);

    table->m_table_definition = tabDef + 1;
    table->get_row_count = plan_baseline_row_count;
    table->m_proxy_engine_table.open_table = plan_baseline_open_table;
    return false;
  }

  static unsigned long long plan_baseline_row_count() {
    if (get_instance()->m_data) {
      return get_instance()->m_data->get_row_count();
    }
    return 0;
  }

  static PSI_table_handle *plan_baseline_open_table(PSI_pos **pos) {
    auto row_pos = reinterpret_cast<uint32_t **>(pos);
    hist_sql_plan_table *table = new hist_sql_plan_table();
    *row_pos = table->get_position_address();
    PSI_table_handle *handle = reinterpret_cast<PSI_table_handle *>(table);
    return handle;
  }
};

}  // namespace greatdb_plan_baseline

#endif
