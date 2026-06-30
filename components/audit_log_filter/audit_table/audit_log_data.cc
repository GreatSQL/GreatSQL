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

#include "components/audit_log_filter/audit_table/audit_log_data.h"
#include <openssl/sha.h>
#include "components/audit_log_filter/audit_error_log.h"
#include "components/audit_log_filter/audit_log_filter.h"
#include "components/audit_log_filter/audit_record.h"
#include "components/audit_log_filter/audit_table/audit_log_data_last_updated_recorder.h"
#include "components/audit_log_filter/audit_table/audit_log_digest.h"
#include "components/audit_log_filter/audit_table/audit_record_converter.h"
#include "components/audit_log_filter/error_log_digger.h"
#include "components/audit_log_filter/sys_vars.h"

// #include <chrono>
// #include <condition_variable>
// #include <mutex>
// #include <thread>
// #include <vector>
// #include "my_thread.h"
//#include "sql/item_func.h"

namespace audit_log_filter::audit_table {

inline constexpr const char *kAuditDataTableName = "audit_log_data";

/*
 * The audit_log_data table columns description
 */

#define CSTRING_WITH_LENGTH(str) str, strlen(str)

const size_t kAuditLogDataId = 0;
const size_t kAuditLogDataTs = 1;
const size_t kAuditLogDataEvClass = 2;
const size_t kAuditLogDataEvSub = 3;
const size_t kAuditLogDataFields = 4;
const TA_table_field_def columns_audit_log_data[] = {
    {kAuditLogDataId, CSTRING_WITH_LENGTH("ID"), TA_TYPE_INTEGER, false, 0},
    {kAuditLogDataTs, CSTRING_WITH_LENGTH("TS"), TA_TYPE_VARCHAR, false,
     kAuditFieldLengthDataTs},
    {kAuditLogDataEvClass, CSTRING_WITH_LENGTH("E_CLASS"), TA_TYPE_VARCHAR,
     false, kAuditFieldLengthDataEClass},
    {kAuditLogDataEvSub, CSTRING_WITH_LENGTH("E_SUB"), TA_TYPE_VARCHAR, false,
     kAuditFieldLengthDataESub},
    {kAuditLogDataFields, CSTRING_WITH_LENGTH("FIELDS"), TA_TYPE_VARCHAR, false,
     kAuditFieldLengthDataFields}};

const size_t kAuditLogDataFieldsCount = 5;

/*
 * Primary key info
 */
const TA_index_field_def key_data_primary_cols[] = {
    {CSTRING_WITH_LENGTH("ID"), false}};
const size_t kKeyDataPrimaryNumcol = 1;
const char *kKeyDataPrimaryName = "PRIMARY";
const size_t kKeyDataPrimaryNameLength = 7;

// Buffer queue size
constexpr size_t kBufferSize = 200;
// Flush interval (milliseconds)
constexpr size_t kFlushIntervalMs = 100;

// Global variables
std::vector<AuditLogData::AuditRecordData> AuditLogData::g_audit_buffer;
std::mutex AuditLogData::g_buffer_mutex;
std::condition_variable AuditLogData::g_buffer_cv;
pthread_t AuditLogData::g_flush_thread;
bool AuditLogData::g_running = true;
bool AuditLogData::g_is_shutdown_event = false;

ulonglong AuditLogData::drop_datas_lessorequal_id(
    AuditLogData *audit_table, TableAccessContext *ta_context,
    long long id) noexcept {
  ulonglong dropped_rows = 0;

  TA_key id_key = nullptr;

  my_service<SERVICE_TYPE(table_access_index_v1)> index_srv(
      "table_access_index_v1", SysVars::get_comp_registry_srv());
  my_service<SERVICE_TYPE(field_integer_access_v1)> integer_srv(
      "field_integer_access_v1", SysVars::get_comp_registry_srv());
  my_service<SERVICE_TYPE(table_access_update_v1)> table_update_srv(
      "table_access_update_v1", SysVars::get_comp_registry_srv());

  if (index_srv->init(ta_context->ta_session, ta_context->ta_table,
                      kKeyDataPrimaryName, kKeyDataPrimaryNameLength,
                      key_data_primary_cols, kKeyDataPrimaryNumcol,
                      &id_key) != 0) {
    LogComponentErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
                    "Failed to init index scan of %s table",
                    audit_table->get_table_name());
    return 0ULL;
  }

  int rc =
      index_srv->first(ta_context->ta_session, ta_context->ta_table, id_key);

  // TODO: Find an optimal way for determining next PK value
  while (rc == 0) {
    long long found_id = 0;

    if (integer_srv->get(ta_context->ta_session, ta_context->ta_table,
                         kAuditLogDataId, &found_id)) {
      LogComponentErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
                      "Failed to read %s.filter_id",
                      audit_table->get_table_name());
      audit_table->index_scan_end(ta_context, id_key);
      return 0ULL;
    }

    if (found_id > id) {
      break;
    }

    if (table_update_srv->delete_row(ta_context->ta_session,
                                     ta_context->ta_table) != 0) {
      LogComponentErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
                      "Failed to delete row from %s table",
                      audit_table->get_table_name());
    } else {
      dropped_rows++;
    }

    rc = index_srv->next(ta_context->ta_session, ta_context->ta_table, id_key);
  }

  audit_table->index_scan_end(ta_context, id_key);

  return dropped_rows;
}

void keep_last_n(std::vector<AuditLogData::AuditRecordData> &vec, size_t n) {
  size_t keep = std::min(n, vec.size());
  vec.assign(vec.end() - static_cast<std::ptrdiff_t>(keep), vec.end());
}

bool flush_records_to_table(
    std::vector<AuditLogData::AuditRecordData> &records_to_write) {
  std::unique_ptr<AuditLogData> audit_table_data =
      std::make_unique<AuditLogData>(SysVars::get_config_database_name());
  std::unique_ptr<TableAccessContext> ta_context;

  ta_context = audit_table_data->open_table();

  if (ta_context == nullptr) {
    LogComponentErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
                    "Failed to open audit_log_data table");
    return true;
  }

  try {
    auto current_type = audit_log_filter::SysVars::get_file_strategy_type();

    if (current_type == audit_log_filter::AuditLogStrategyType::Synchronous ||
        !audit_table_data->is_pk_initialized()) {
      if (audit_table_data->get_next_pk_value(ta_context.get()) !=
          TableResult::Ok) {
        LogComponentErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
                        "Failed to get next primary key value");
        return true;
      }
    }

    for (auto &record : records_to_write) {
      record.id = audit_table_data->get_next_pk();

      auto result = audit_table_data->insert_data_with_id(
          ta_context.get(), record.timestamp, record.ev_class, record.ev_sub,
          record.record_str, record.id);
      if (result != TableResult::Ok) {
        LogComponentErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
                        "Failed to inser audit data with id: %lld (%s)",
                        audit_table_data->get_next_pk(),
                        record.record_str.c_str());
      }

      audit_table_data->increment_next_pk();
    }

    // Drop old rows, and keep table_max_records rows accuratelly
    if (SysVars::get_to_table_eal() &&
        audit_table_data->get_num_of_records() >
            static_cast<long long>(SysVars::get_table_max_records())) {
      long long deleted_max_id = audit_table_data->get_next_pk() -
                                 SysVars::get_table_max_records() - 1;

      auto dropped_rows = AuditLogData::drop_datas_lessorequal_id(
          audit_table_data.get(), ta_context.get(), deleted_max_id);
      AuditLogData::set_num_of_records(AuditLogData::get_num_of_records() -
                                       dropped_rows);
    }

    my_service<SERVICE_TYPE(table_access_v1)> table_access_srv(
        "table_access_v1", SysVars::get_comp_registry_srv());
    if (table_access_srv->commit(ta_context->ta_session) != 0) {
      LogComponentErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
                      "Failed to commit audit data");
    }

    if (SysVars::get_to_table_eal()) {
      LogComponentErr(INFORMATION_LEVEL, ER_LOG_PRINTF_MSG,
                      "Update digest table");
      AuditLogDigest::update_digest_table(records_to_write);
    }
  } catch (const std::exception &e) {
    LogComponentErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
                    "Exception while inserting audit data: %s", e.what());

    return true;
  } catch (...) {
    LogComponentErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
                    "Unknown exception while inserting audit data.");
    return true;
  }

  return false;
}

void AuditLogData::flush_thread_func() {
  while (g_running) {
    std::vector<AuditLogData::AuditRecordData> records_to_write;
    {
      std::unique_lock<std::mutex> lock(g_buffer_mutex);

      g_buffer_cv.wait_for(
          lock, std::chrono::milliseconds(kFlushIntervalMs),
          [] { return !g_audit_buffer.empty() || !g_running; });

      LastUpdatedRecorder::get_instance().update_last_updated_time();

      if (g_is_shutdown_event && g_audit_buffer.empty()) {
        break;
      }

      if (!g_running && g_audit_buffer.empty()) {
        break;
      }

      if (!g_audit_buffer.empty()) {
        records_to_write.swap(g_audit_buffer);
      }
    }

    if (!records_to_write.empty()) {
      // during extreme heavy load, just keep the last table_max_records number
      // of records
      if (records_to_write.size() > SysVars::get_table_max_records()) {
        keep_last_n(records_to_write, SysVars::get_table_max_records());
      }

      flush_records_to_table(records_to_write);
    }
  }
  g_running = false;

  // remove abnormal shutdown indicator
  LastUpdatedRecorder::get_instance().drop();
}

audit_log_filter::audit_table::AuditLogData::AuditLogData(std::string db_name)
    : AuditTableBase{std::move(db_name)} {}

const char *
audit_log_filter::audit_table::AuditLogData::get_table_name() noexcept {
  return kAuditDataTableName;
}

const TA_table_field_def *
audit_log_filter::audit_table::AuditLogData::get_table_field_def() noexcept {
  return columns_audit_log_data;
}

size_t
audit_log_filter::audit_table::AuditLogData::get_table_field_count() noexcept {
  return kAuditLogDataFieldsCount;
}

void audit_log_filter::audit_table::AuditLogData::index_scan_end(
    TableAccessContext *ta_context, TA_key key) noexcept {
  if (key != nullptr) {
    my_service<SERVICE_TYPE(table_access_index_v1)> index_srv(
        "table_access_index_v1", SysVars::get_comp_registry_srv());
    index_srv->end(ta_context->ta_session, ta_context->ta_table, key);
  }
}

audit_log_filter::audit_table::TableResult
audit_log_filter::audit_table::AuditLogData::get_next_pk_value(
    TableAccessContext *ta_context) noexcept {
  TA_key filter_id_key = nullptr;
  next_pkey = 1;
  num_of_records = 0;

  my_service<SERVICE_TYPE(table_access_index_v1)> index_srv(
      "table_access_index_v1", SysVars::get_comp_registry_srv());
  my_service<SERVICE_TYPE(field_integer_access_v1)> integer_srv(
      "field_integer_access_v1", SysVars::get_comp_registry_srv());

  if (index_srv->init(ta_context->ta_session, ta_context->ta_table,
                      kKeyDataPrimaryName, kKeyDataPrimaryNameLength,
                      key_data_primary_cols, kKeyDataPrimaryNumcol,
                      &filter_id_key) != 0) {
    LogComponentErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
                    "Failed to init index scan of %s table", get_table_name());
    return TableResult::Fail;
  }

  int rc = index_srv->first(ta_context->ta_session, ta_context->ta_table,
                            filter_id_key);

  // TODO: Find an optimal way for determining next PK value
  while (rc == 0) {
    long long found_filter_id = 0;

    if (integer_srv->get(ta_context->ta_session, ta_context->ta_table,
                         kAuditLogDataId, &found_filter_id)) {
      LogComponentErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
                      "Failed to read %s.filter_id", get_table_name());
      index_scan_end(ta_context, filter_id_key);
      return TableResult::Fail;
    }

    next_pkey = found_filter_id + 1;
    num_of_records++;

    rc = index_srv->next(ta_context->ta_session, ta_context->ta_table,
                         filter_id_key);
  }

  index_scan_end(ta_context, filter_id_key);

  return TableResult::Ok;
}

audit_log_filter::audit_table::TableResult
audit_log_filter::audit_table::AuditLogData::insert_data_with_id(
    TableAccessContext *ta_context, const std::string &ts,
    const std::string &e_class, const std::string &e_sub,
    const std::string &fields, long long id) noexcept {
  my_service<SERVICE_TYPE(mysql_charset)> charset_srv(
      "mysql_charset", SysVars::get_comp_registry_srv());
  my_service<SERVICE_TYPE(mysql_string_factory)> string_srv(
      "mysql_string_factory", SysVars::get_comp_registry_srv());
  my_service<SERVICE_TYPE(mysql_string_charset_converter)> string_convert_srv(
      "mysql_string_charset_converter", SysVars::get_comp_registry_srv());
  my_service<SERVICE_TYPE(field_varchar_access_v1)> varchar_srv(
      "field_varchar_access_v1", SysVars::get_comp_registry_srv());
  my_service<SERVICE_TYPE(field_integer_access_v1)> integer_srv(
      "field_integer_access_v1", SysVars::get_comp_registry_srv());
  my_service<SERVICE_TYPE(table_access_update_v1)> table_update_srv(
      "table_access_update_v1", SysVars::get_comp_registry_srv());

  CHARSET_INFO_h utf8 = charset_srv->get_utf8mb4();
  HStringContainer ts_value{string_srv};
  HStringContainer e_class_value{string_srv};
  HStringContainer e_sub_value{string_srv};
  HStringContainer user_value{string_srv};
  HStringContainer host_value{string_srv};
  HStringContainer fields_value{string_srv};

  string_convert_srv->convert_from_buffer(ts_value.get(), ts.c_str(),
                                          ts.length(), utf8);
  string_convert_srv->convert_from_buffer(e_class_value.get(), e_class.c_str(),
                                          e_class.length(), utf8);
  string_convert_srv->convert_from_buffer(e_sub_value.get(), e_sub.c_str(),
                                          e_sub.length(), utf8);
  string_convert_srv->convert_from_buffer(fields_value.get(), fields.c_str(),
                                          fields.length(), utf8);

  integer_srv->set(ta_context->ta_session, ta_context->ta_table,
                   kAuditLogDataId, id);
  varchar_srv->set(ta_context->ta_session, ta_context->ta_table,
                   kAuditLogDataTs, ts_value.get());
  varchar_srv->set(ta_context->ta_session, ta_context->ta_table,
                   kAuditLogDataEvClass, e_class_value.get());
  varchar_srv->set(ta_context->ta_session, ta_context->ta_table,
                   kAuditLogDataEvSub, e_sub_value.get());
  varchar_srv->set(ta_context->ta_session, ta_context->ta_table,
                   kAuditLogDataFields, fields_value.get());

  int rc =
      table_update_srv->insert(ta_context->ta_session, ta_context->ta_table);

  if (rc != 0) {
    LogComponentErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
                    "Failed to insert audit data '%s'", fields.c_str());

    return TableResult::Fail;
  }

  return TableResult::Ok;
}

void audit_log_filter::audit_table::AuditLogData::reset() noexcept {
  g_running = false;
  g_is_shutdown_event = true;
  g_audit_buffer.clear();
  next_pkey = 0;
  num_of_records = 0;
}

void enqueue_audit_record(const audit_log_filter::AuditRecordVariant &record,
                          time_t ts) {
  if (AuditLogData::g_is_shutdown_event) return;

  std::string timestamp;
  if (ts == 0) {
    timestamp = std::to_string(time(nullptr));
  } else {
    timestamp = std::to_string(ts);
  }
  std::string ev_class;
  std::string ev_sub;
  std::string user;
  std::string host;
  std::string record_str;
  bool is_shutdown = false;

  audit_log_filter::audit_table::AuditRecordConverter::convert_to_json(
      record, ev_class, ev_sub, user, host, record_str);

  if (std::holds_alternative<audit_log_filter::AuditRecordQuery>(record)) {
    const auto &query_record =
        std::get<audit_log_filter::AuditRecordQuery>(record);
    auto fields = get_audit_record_fields(query_record);

    if (ev_sub == "start" && fields.find("query.str") != fields.end()) {
      std::string query = fields["query.str"];

      std::transform(query.begin(), query.end(), query.begin(),
                     [](unsigned char c) { return std::toupper(c); });

      if (query == "SHUTDOWN") {
        is_shutdown = true;
      }
    }
  }

  std::vector<AuditLogData::AuditRecordData> records;
  AuditLogData::AuditRecordData audit_data{0, timestamp, ev_class, ev_sub,
                                           record_str};

  records.push_back(std::move(audit_data));

  if (is_shutdown) {
    AuditLogData::AuditRecordData shutdown_data{0, timestamp, "server_shutdown",
                                                "shutdown", record_str};
    records.push_back(std::move(shutdown_data));
  }

  if (SysVars::get_to_table_eal() &&
      (records.size() + AuditLogData::get_num_of_records() >
       SysVars::get_table_max_records())) {
    ulonglong expected_total =
        records.size() + AuditLogData::get_num_of_records();
    ulonglong purged = expected_total - SysVars::get_table_max_records() + 1;

    std::string purge_str =
        R"({"purge_num":)" + std::to_string(purged) + R"(})";

    AuditLogData::AuditRecordData purge_data{0, timestamp, "general", "purge",
                                             purge_str};

    // since purge is less important, insert in front
    records.insert(records.begin(), purge_data);
  }

  if (audit_log_filter::SysVars::get_file_strategy_type() !=
      audit_log_filter::AuditLogStrategyType::Synchronous) {
    std::unique_lock<std::mutex> lock(AuditLogData::g_buffer_mutex);

    // AuditLogData::g_audit_buffer.push_back(std::move(audit_data));
    std::move(records.begin(), records.end(),
              std::back_inserter(AuditLogData::g_audit_buffer));

    if (AuditLogData::g_audit_buffer.size() >
        SysVars::get_table_max_records()) {
      keep_last_n(AuditLogData::g_audit_buffer,
                  SysVars::get_table_max_records());
    }

    if (is_shutdown) {
      auto last = AuditLogData::g_audit_buffer.back();
      AuditLogData::g_buffer_cv.notify_one();
      AuditLogData::g_is_shutdown_event = true;
    }
  } else {
    std::unique_lock<std::mutex> lock(
        audit_table::AuditLogData::g_buffer_mutex);
    flush_records_to_table(records);
  }

  // wait for the flush thread to finish
  while (is_shutdown && AuditLogData::g_running) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
}

void *buffer_flush_worker(void *arg [[maybe_unused]]) {
  my_thread_init();
  AuditLogData::flush_thread_func();
  my_thread_end();

  return nullptr;
}

/**
 * @brief wait for the flush round end
 *
 * @note only for async strategy
 *
 * @note example: set filter with class [general, query], and effective
 * immediately, but since the async write, the data in g_audit_buffer may
 * include other class data, so we need to wait for the flush round end to
 * ensure the data in g_audit_buffer is only the data with class [general,
 * query]
 */
void wait_for_flush_round_end() {
  if (!SysVars::get_to_table() && !SysVars::get_to_table_eal()) {
    return;
  }

  time_t audit_last_updated =
      audit_table::LastUpdatedRecorder::get_instance().get_last_updated_time();
  time_t latest = 0;
  while (audit_last_updated == 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    audit_last_updated = audit_table::LastUpdatedRecorder::get_instance()
                             .get_last_updated_time();
  }

  int cnt = 0;
  while (cnt < 2) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    latest = audit_table::LastUpdatedRecorder::get_instance()
                 .get_last_updated_time();
    if (latest > audit_last_updated) {
      audit_last_updated = latest;
      cnt++;
    }
  }
}

void init_audit_buffer() {
  cleanup_audit_buffer();

  AuditLogData::g_is_shutdown_event = false;
  AuditLogData::g_running = true;
  pthread_create(&AuditLogData::g_flush_thread, nullptr, buffer_flush_worker,
                 nullptr);
}

// Cleanup function
void cleanup_audit_buffer() {
  AuditLogData::g_is_shutdown_event = true;

  try {
    AuditLogData::g_buffer_cv.notify_one();
    if (AuditLogData::g_flush_thread != 0) {
      pthread_join(AuditLogData::g_flush_thread, nullptr);
    }
  } catch (std::exception &e) {
  } catch (...) {
  }
  AuditLogData::reset();
}

}  // namespace audit_log_filter::audit_table
