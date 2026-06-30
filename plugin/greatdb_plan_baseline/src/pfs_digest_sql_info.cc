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

#include "pfs_digest_sql_info.h"
#include <algorithm>
#include <numeric>
#include "mysql/components/services/pfs_plugin_table_service.h"
#include "pfs.h"
#include "plan_baseline.h"
#include "psi.h"
namespace greatdb_plan_baseline {

int digest_sql_info_data::Init() {
  pos = 0;
  mysql_mutex_lock(&lock_plan_sql_map);
  iterator = plan_sql_map.begin();
  inited = true;
  return 0;
}

// called from digest_sql_info_table::rnd_next
int digest_sql_info_data::GetData(std::vector<std::unique_ptr<PSI_Val>> &data,
                                  uint32_t, uint32_t) {
  if (iterator == plan_sql_map.end()) {
    mysql_mutex_unlock(&lock_plan_sql_map);
    return HA_ERR_END_OF_FILE;  // end of data
  }

  if (!inited) {
    mysql_mutex_unlock(&lock_plan_sql_map);
    LogPluginErrMsg(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
                    "sql_plan_baselines_data get data failed : not inited");
    return 1;
  }
  std::string key_value = *iterator->first.get();
  std::vector<std::string> result_key = split(key_value, '#', 2);
  explain_query_sql *result = iterator->second.get();
  pos++;
  data[E_id]->Set(result->id);
  data[E_db_name]->Set(result_key[0]);
  data[E_query_sql]->Set(result_key[1]);
  data[E_cost]->Set(result->cost);
  data[E_table_rows]->Set(result->rows);
  iterator++;
  return 0;
}

int digest_sql_info_data::End() { return 0; }

int digest_sql_info_table::rnd_init() {
  return digest_sql_info::get_instance()->InitData();
}

int digest_sql_info_table::rnd_next() {
  return digest_sql_info::get_instance()->ReadData(data);
}

int digest_sql_info_table::rnd_end() {
  return digest_sql_info::get_instance()->EndData();
}

int digest_sql_info_table::read_column_value(PSI_field *f, uint32_t index) {
  if (index > data.size()) {
    return 1;
  }
  data[index]->SetField(f);
  return 0;
}

}  // namespace greatdb_plan_baseline
