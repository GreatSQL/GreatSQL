/* Copyright (c) 2013, 2022, Oracle and/or its affiliates.
   Copyright (c) 2024, GreatDB Software Co., Ltd.

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

#ifndef DEFINED_LOCAL_BINLOG_BASE
#define DEFINED_LOCAL_BINLOG_BASE

#include <memory>
#include <set>
#include <string>

#include "my_bitmap.h"
#include "sql/log_event.h"
#include "sql/rpl_record.h"
#include "sql/table.h"

struct Query_event_info {
  Query_event_info() : command(SQLCOM_END), transaction_ddl(false) {}
  enum_sql_command command;
  bool transaction_ddl;
  std::list<std::pair<std::string, std::string>> tables;
};

class Local_binlog_filter {
 public:
  Local_binlog_filter() = default;
  virtual ~Local_binlog_filter() = default;

  virtual bool filter_event(std::shared_ptr<Log_event> &ev,
                            Query_event_info &event_info) = 0;

  virtual bool has_error() { return false; }
  virtual std::string error() { return ""; }
};

class Local_tables_filter : public Local_binlog_filter {
 public:
  Local_tables_filter() : m_has_error(false) {}
  ~Local_tables_filter() override { m_tables.clear(); }
  bool filter_event(std::shared_ptr<Log_event> &ev,
                    Query_event_info &event_info) override;

  void add_filter_table(const std::string &db_name,
                        const std::string &table_name);
  void remove_filter_table(const std::string &db_name,
                           const std::string &table_name);

  bool has_error() override { return m_has_error; }
  std::string error() override { return m_error; }

 private:
  std::set<std::pair<std::string, std::string>> m_tables;
  std::set<uint64_t> m_filter_out_ids;

  bool m_has_error;
  std::string m_error;
};

class Local_no_filter : public Local_binlog_filter {
 public:
  Local_no_filter() = default;
  ~Local_no_filter() override {}
  bool filter_event(std::shared_ptr<Log_event> &, Query_event_info &) override {
    return true;
  }
};

struct Binlog_field_meta_info {
  MY_BITMAP *cols_bitmap;
  size_t field_no;
  uchar field_type;
  uint field_meta;
  size_t size;
  bool null_flag;
  const uchar *value;
};

struct Binlog_row_meta_info {
  std::string db_name;
  std::string table_name;
  MY_BITMAP *cols_bitmap;
  enum_row_image_type row_image_type;
  unsigned char *fields_type;
  unsigned char *fields_metadata;
  const uchar *value;
  unsigned long colcnt;
  uint32_t row_size;
  Bit_reader null_bits;
};

class Binlog_data_executor;
class Rows_log_sweeper {
 public:
  Rows_log_sweeper() = default;
  ~Rows_log_sweeper() = default;

  uint32_t sweep_one_row(TABLE *table, const std::string &db_name,
                         const std::string &table_name, unsigned long colcnt,
                         MY_BITMAP *cols_bitmap, const uchar *value,
                         const uchar *rows_end,
                         enum_row_image_type row_image_type,
                         unsigned char *fields_type,
                         unsigned char *fields_metadata);
  uint32_t sweep_one_field(TABLE *table, MY_BITMAP *cols_bitmap,
                           size_t field_no, uchar field_type, uint field_meta,
                           Bit_reader &null_bits, const uchar *value,
                           enum_row_image_type row_image_type);

  void set_executor_callback(Binlog_data_executor *executor);

  std::string error() { return m_error; }

 private:
  Binlog_data_executor *m_executor_cbk;
  std::string m_error;
};

class Binlog_data_executor {
 public:
  Binlog_data_executor() = default;
  virtual ~Binlog_data_executor() = default;

  virtual int process_field(TABLE *table [[maybe_unused]],
                            Binlog_field_meta_info &field_info [[maybe_unused]],
                            enum_row_image_type row_image_type
                            [[maybe_unused]]) {
    return 0;
  }
  virtual int process_row(TABLE *table [[maybe_unused]],
                          Binlog_row_meta_info &row_info [[maybe_unused]]) {
    return 0;
  }
  virtual int execute_row(const char *schema_name [[maybe_unused]],
                          const char *table_name [[maybe_unused]],
                          int dml_type [[maybe_unused]],
                          bool end_flag [[maybe_unused]]) {
    return 0;
  }
  virtual void set_cols_bitmap(TABLE *table [[maybe_unused]],
                               MY_BITMAP *cols_bitmap [[maybe_unused]]) {}
  virtual int execute_gtid(std::shared_ptr<Gtid_log_event> &gtid_event
                           [[maybe_unused]]) {
    return 0;
  }
  virtual int start_trx() { return 0; }
  virtual int commit_trx() { return 0; }
  virtual int execute_ddl(std::shared_ptr<Query_log_event> &query_event
                          [[maybe_unused]]) {
    return 0;
  }

  virtual int execute_heartbeat_event(
      std::shared_ptr<Heartbeat_log_event> &heartbeat_event [[maybe_unused]]) {
    return 0;
  }

  virtual void cleanup() {}

  std::string error() { return m_error; }
  void set_error(const std::string &err) { m_error = err; }

 private:
  std::string m_error;
};

#ifndef NDEBUG
// in execute_row/commit_trx, engine can use callback to write data
// 1. execute_row:
//    a. end_flag is false: cache
//    b. end_flag is false and cache is full: callback
//    c. end_flag is true: mark
// 2. execute_row:
//    a. same op as last row: can cache, if end_flag is true, always can cache
//        if same table
//    b. not same op: cannot cache, callback
// 3. commit_trx:
//    a. cache
//    b. cache is full, callback
// 4. execute_gtid: do nothing
// 5. begin_trx: do nothing
// 6. execute_field: fullfil row cache for each field
struct MYSQL;
class Output_test_data_executor : public Binlog_data_executor {
 public:
  Output_test_data_executor();
  ~Output_test_data_executor() override;

  int process_field(TABLE *table, Binlog_field_meta_info &field_info,
                    enum_row_image_type row_image_type) override;
  int process_row(TABLE *table, Binlog_row_meta_info &row_info) override;
  int execute_row(const char *schema_name, const char *table_name, int dml_type,
                  bool end_flag) override;
  int execute_gtid(std::shared_ptr<Gtid_log_event> &gtid_event) override;
  int start_trx() override;
  int commit_trx() override;
  int execute_ddl(std::shared_ptr<Query_log_event> &query_event) override;

 private:
  std::shared_ptr<Gtid_log_event> m_current_gtid_log_event;

  MYSQL *m_client;
};
#endif  // NDEBUG

TABLE *open_table_for_task(THD *thd, const char *schema_name,
                           const char *table_name);
void close_table_for_task(THD *thd);
#endif  // DEFINED_LOCAL_BINLOG_BASE
