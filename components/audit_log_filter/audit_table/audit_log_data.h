/* Copyright (c) 2022 Percona LLC and/or its affiliates. All rights reserved.
   Copyright (c) 2026, GreatDB Software Co., Ltd.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA */

#ifndef AUDIT_LOG_FILTER_AUDIT_TABLE_AUDIT_LOG_DATA_H_INCLUDED
#define AUDIT_LOG_FILTER_AUDIT_TABLE_AUDIT_LOG_DATA_H_INCLUDED

#include "base.h"
#include "components/audit_log_filter/audit_record.h"

#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace audit_log_filter::audit_table {

class AuditLogData : public AuditTableBase {
  inline static long long num_of_records = 0;
  inline static long long next_pkey = 0;

 public:
  static long long get_num_of_records() { return num_of_records; }

  static long long get_next_pk() { return next_pkey; }

  static void increment_next_pk() {
    next_pkey++;
    num_of_records++;
  }

  static void set_num_of_records(long long num) { num_of_records = num; }
  struct AuditRecordData {
    long long id;
    std::string timestamp;
    std::string ev_class;
    std::string ev_sub;
    std::string record_str;
  };

  static bool is_pk_initialized() {
    return next_pkey != 0 && num_of_records != 0;
  }

  static void flush_thread_func();
  static ulonglong drop_datas_lessorequal_id(AuditLogData *audit_table,
                                             TableAccessContext *ta_context,
                                             long long id) noexcept;
  static std::vector<AuditRecordData> g_audit_buffer;
  static std::mutex g_buffer_mutex;
  static std::condition_variable g_buffer_cv;
  static pthread_t g_flush_thread;
  static bool g_running;
  static bool g_is_shutdown_event;

  explicit AuditLogData(std::string db_name);
  const char *get_table_name() noexcept override;
  const TA_table_field_def *get_table_field_def() noexcept override;
  size_t get_table_field_count() noexcept override;
  void index_scan_end(TableAccessContext *ta_context, TA_key key) noexcept;
  TableResult get_next_pk_value(TableAccessContext *ta_context) noexcept;

  TableResult insert_data_with_id(TableAccessContext *ta_context,
                                  const std::string &ts,
                                  const std::string &e_class,
                                  const std::string &e_sub,
                                  const std::string &fields,
                                  long long id) noexcept;
  TableResult insert_data(const std::string &ts, const std::string &e_class,
                          const std::string &e_sub, const std::string &user,
                          const std::string &host,
                          const std::string &fields) noexcept;
  static void reset() noexcept;
};

void enqueue_audit_record(const AuditRecordVariant &record, time_t ts = 0);
void wait_for_flush_round_end();
void init_audit_buffer();
void cleanup_audit_buffer();
bool flush_records_to_table(
    std::vector<AuditLogData::AuditRecordData> &records_to_write);

}  // namespace audit_log_filter::audit_table

#endif  // AUDIT_LOG_FILTER_AUDIT_TABLE_AUDIT_LOG_DATA_H_INCLUDED
