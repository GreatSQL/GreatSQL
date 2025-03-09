/* Copyright (c) 2024, 2025, GreatDB Software Co., Ltd.

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

#include "sql/binlog_base.h"

#include "field_types.h"
#include "libbinlogevents/export/binary_log_funcs.h"
#include "my_time.h"
#include "mysql.h"
#include "mysql/components/services/log_builtins.h"
#include "mysql/thread_pool_priv.h"
#include "mysql_time.h"
#include "sql/handler.h"
#include "sql/my_decimal.h"
#include "sql/mysqld.h"
#include "sql/rpl_utility.h"
#include "sql/sql_base.h"
#include "sql/sql_lex.h"
#include "sql/sql_parse.h"
#include "sql_common.h"

#include <string>

enum query_type {
  DML_QUERY,
  DDL_TABLE_QUERY,
  OTHER_QUERY,
};

static enum query_type get_query_type(enum_sql_command command) {
  enum query_type type = OTHER_QUERY;
  switch (command) {
    case SQLCOM_UPDATE:
    case SQLCOM_INSERT:
    case SQLCOM_INSERT_SELECT:
    case SQLCOM_DELETE:
    case SQLCOM_LOAD:
    case SQLCOM_REPLACE:
    case SQLCOM_REPLACE_SELECT:
    case SQLCOM_DELETE_MULTI:
    case SQLCOM_UPDATE_MULTI:
    case SQLCOM_CALL:
    case SQLCOM_PREPARE:
    case SQLCOM_EXECUTE:
    case SQLCOM_EXECUTE_IMMEDIATE:
    case SQLCOM_DEALLOCATE_PREPARE:
    case SQLCOM_INSERT_ALL_SELECT:
      type = DML_QUERY;
      break;
    case SQLCOM_CREATE_TABLE:
    case SQLCOM_CREATE_INDEX:
    case SQLCOM_ALTER_TABLE:
    case SQLCOM_TRUNCATE:
    case SQLCOM_DROP_TABLE:
    case SQLCOM_DROP_INDEX:
    case SQLCOM_ALTER_DB:
    case SQLCOM_REPAIR:
    case SQLCOM_OPTIMIZE:
    case SQLCOM_RENAME_TABLE:
    case SQLCOM_ALTER_TABLESPACE:
      type = DDL_TABLE_QUERY;
      break;
    default:
      break;
  }
  return type;
}

static int parse_query_event(THD *thd, Query_log_event *ev,
                             Query_event_info &info) {
  DBUG_TRACE;
  LEX *lex = thd->lex;
  sql_digest_state *parent_digest = thd->m_digest;
  PSI_statement_locker *parent_locker = thd->m_statement_psi;

  if (ev->catalog_len) {
    LEX_CSTRING catalog = {ev->catalog, ev->catalog_len};
    thd->set_catalog(catalog);
  } else {
    thd->set_catalog(EMPTY_CSTR);
  }
  LEX_CSTRING db = {ev->db, ev->db_len};
  thd->set_db(db);
  auto tmp_sqlmode = thd->variables.sql_mode;
  thd->variables.sql_mode = ev->sql_mode & ~MODE_NO_ENGINE_SUBSTITUTION;
  int ret = 0;
  // the parser sql part is copy from mysql_test_parse_for_slave, the interface
  // is only use for slave thread, so, it cannot reuse here
  thd->set_query(ev->query, ev->q_len);
  thd->set_query_for_display(ev->query, ev->q_len);
  thd->set_query_id(next_query_id());
  Parser_state parser_state;
  if (!parser_state.init(thd, thd->query().str, thd->query().length)) {
    lex_start(thd);
    mysql_reset_thd_for_next_command(thd);

    thd->m_digest = nullptr;
    thd->m_statement_psi = nullptr;
    if (parse_sql(thd, &parser_state, nullptr) == 0) {
      info.command = thd->lex->sql_command;
      if (thd->lex->sql_command == SQLCOM_CREATE_TABLE) {
        info.transaction_ddl = thd->lex->create_info->m_transactional_ddl;
      }
      Table_ref *tables = lex->query_block->m_table_list.first;
      for (; tables; tables = tables->next_global) {
        info.tables.push_back(std::make_pair(tables->db ? tables->db : ev->db,
                                             tables->table_name));
        // CREATE TABLE SELECT or CREATE TABLE LIKE may have multiple tables
        // while only the first table is the created table
        if (thd->lex->sql_command == SQLCOM_CREATE_TABLE) {
          break;
        }
      }
    } else {
      ret = 1;
    }
    thd->m_digest = parent_digest;
    thd->m_statement_psi = parent_locker;
    thd->end_statement();
  } else {
    ret = 1;
  }

  thd->variables.sql_mode = tmp_sqlmode;
  thd->cleanup_after_query();
  return ret;
}
bool Local_tables_filter::filter_event(std::shared_ptr<Log_event> &ev,
                                       Query_event_info &event_info) {
  DBUG_TRACE;
  bool filter = true;
  switch (ev->get_type_code()) {
    case binary_log::WRITE_ROWS_EVENT:
    case binary_log::UPDATE_ROWS_EVENT:
    case binary_log::DELETE_ROWS_EVENT: {
      auto *row_event = (Rows_log_event *)(ev.get());
      uint64_t table_id = row_event->get_table_id().id();
      if (m_filter_out_ids.count(table_id)) {
        filter = false;
      }
      if (row_event->get_flags(binary_log::Rows_event::STMT_END_F)) {
        m_filter_out_ids.clear();
      }
    } break;
    case binary_log::TABLE_MAP_EVENT: {
      // TODO: use more efficient way to compare dbname.table_name
      auto *tbl_map_event = (Table_map_log_event *)(ev.get());
      uint64_t table_id = tbl_map_event->get_table_id().id();
      std::string db_name(tbl_map_event->get_db_name());
      std::string table_name(tbl_map_event->get_table_name());
      if (!m_tables.count({db_name, table_name})) {
        filter = false;
        m_filter_out_ids.insert(table_id);
      }
    } break;
    case binary_log::QUERY_EVENT: {
      // TODO: support filter DDL event
      Query_log_event *query_event = (Query_log_event *)(ev.get());
      if (query_event->is_trans_keyword()) return true;

      if (parse_query_event(thd_get_current_thd(), query_event, event_info)) {
        m_has_error = true;
        m_error = "cannot parse query for query_log_event";
        return false;
      }
      auto type = get_query_type(event_info.command);
      bool find_table = false;
      for (auto &it : event_info.tables) {
        if (m_tables.count(it)) {
          find_table = true;
          break;
        }
      }
      if (find_table) {
        if (event_info.transaction_ddl) {
          m_has_error = true;
          filter = false;
          m_error = "unsupport create select yet";
        } else if (type == DML_QUERY) {
          m_has_error = true;
          filter = false;
          m_error = "unsupport mix or statement binlog format";
        } else if (type == DDL_TABLE_QUERY) {
          m_has_error = true;
          filter = false;
          m_error = "cannot sync DDL statement";
        } else {
          filter = true;
        }
        return filter;
      } else {
        if (type == DML_QUERY)
          filter = false;
        else
          filter = true;
      }
    } break;
    case binary_log::HEARTBEAT_LOG_EVENT: {
      filter = false;
    } break;
    default:
      break;
  }

  return filter;
}

void Local_tables_filter::add_filter_table(const std::string &db_name,
                                           const std::string &table_name) {
  m_tables.insert({db_name, table_name});
}
void Local_tables_filter::remove_filter_table(const std::string &db_name,
                                              const std::string &table_name) {
  m_tables.erase({db_name, table_name});
}

uint32_t Rows_log_sweeper::sweep_one_row(
    TABLE *table, const std::string &db_name, const std::string &table_name,
    unsigned long colcnt, MY_BITMAP *cols_bitmap, const uchar *value,
    const uchar *rows_end [[maybe_unused]], enum_row_image_type row_image_type,
    unsigned char *fields_type, unsigned char *fields_metadata) {
  if (m_executor_cbk) {
    m_executor_cbk->set_cols_bitmap(table, cols_bitmap);
  }
  const uchar *value0 = value;
  Bit_reader null_bits(value);
  value += (bitmap_bits_set(cols_bitmap) + 7) / 8;
  unsigned int index = 0;
  for (unsigned long field_no = 0; field_no < colcnt; field_no++) {
    uchar field_type = fields_type[field_no];
    std::pair<my_off_t, std::pair<uint, bool>> pack = read_field_metadata(
        fields_metadata + index, static_cast<enum_field_types>(field_type));
    if (field_type == MYSQL_TYPE_TYPED_ARRAY) {
      field_type = static_cast<enum_field_types>(fields_metadata[index]);
    }
    auto field_meta = pack.second.first;
    index += pack.first;

    bitmap_set_bit(table->read_set, field_no);
    uint32_t size =
        sweep_one_field(table, cols_bitmap, field_no, field_type, field_meta,
                        null_bits, value, row_image_type);
    if (size == UINT32_MAX) {
      return size;
    } else {
      value += size;
    }
  }
  if (m_executor_cbk) {
    Bit_reader m_null_bits(value0);
    Binlog_row_meta_info row_info{
        db_name,        table_name,  cols_bitmap,
        row_image_type, fields_type, fields_metadata,
        value0,         colcnt,      (uint32_t)(value - value0),
        m_null_bits};
    if (m_executor_cbk->process_row(table, row_info)) {
      auto err_msg = m_executor_cbk->error();
      if (err_msg.empty()) {
        m_error = "process binlog row error";
      } else {
        m_error = err_msg;
      }
      return UINT32_MAX;
    }
  }
  return value - value0;
}

uint32_t Rows_log_sweeper::sweep_one_field(TABLE *table, MY_BITMAP *cols_bitmap,
                                           size_t field_no, uchar field_type,
                                           uint field_meta,
                                           Bit_reader &null_bits,
                                           const uchar *value,
                                           enum_row_image_type row_image_type) {
  uchar real_field_type = field_type;
  uint32_t size = 0;
  bool null_flag = false;

  do {
    if (bitmap_is_set(cols_bitmap, field_no) == 0) break;
    null_flag = null_bits.get();
    if (null_flag) break;

    switch (field_type) {
      case MYSQL_TYPE_STRING: {
        int real_type = field_meta >> 8;
        if (real_type == MYSQL_TYPE_ENUM || real_type == MYSQL_TYPE_SET) {
          real_field_type = static_cast<enum_field_types>(real_type);
        }
      } break;

      /*
        This type has not been used since before row-based replication,
        so we can safely assume that it really is MYSQL_TYPE_NEWDATE.
       */
      case MYSQL_TYPE_DATE: {
        real_field_type = MYSQL_TYPE_NEWDATE;
      } break;
    }

    size = calc_field_size(real_field_type, value, field_meta);
  } while (false);

  if (m_executor_cbk) {
    Binlog_field_meta_info field_info{cols_bitmap, field_no, real_field_type,
                                      field_meta,  size,     null_flag,
                                      value};
    if (m_executor_cbk->process_field(table, field_info, row_image_type)) {
      auto err_msg = m_executor_cbk->error();
      if (err_msg.empty()) {
        m_error = "process binlog field error";
      } else {
        m_error = err_msg;
      }
      size = UINT32_MAX;
    }
  }
  return size;
}

void Rows_log_sweeper::set_executor_callback(Binlog_data_executor *executor) {
  m_executor_cbk = executor;
}

#ifndef NDEBUG
/* begin class Output_test_data_executor */
Output_test_data_executor::Output_test_data_executor() {
  m_client = mysql_init(nullptr);
  assert(m_client);
  int connect_timeout = 3;
  int read_timeout = 10;
  int write_timeout = 10;
  mysql_options(m_client, MYSQL_OPT_CONNECT_TIMEOUT, &connect_timeout);
  mysql_options(m_client, MYSQL_OPT_READ_TIMEOUT, &read_timeout);
  mysql_options(m_client, MYSQL_OPT_WRITE_TIMEOUT, &write_timeout);
  int value = SSL_MODE_DISABLED;
  mysql_options(m_client, MYSQL_OPT_SSL_MODE, &value);
  bool get_server_public_key = true;
  mysql_options(m_client, MYSQL_OPT_GET_SERVER_PUBLIC_KEY,
                &get_server_public_key);

  (void)mysql_real_connect(m_client, "localhost", "root", "", nullptr,
                           mysqld_port, nullptr, CLIENT_MULTI_STATEMENTS);

  (void)mysql_send_query(m_client, STRING_WITH_LEN("SET sql_log_bin=OFF"));
  const struct MYSQL_METHODS *methods = m_client->methods;
  (void)(methods->read_query_result)(m_client);
}
Output_test_data_executor::~Output_test_data_executor() {
  if (m_client) {
    mysql_close(m_client);
    m_client = nullptr;
  }
}
int Output_test_data_executor::process_field(TABLE *table [[maybe_unused]],
                                             Binlog_field_meta_info &field_info,
                                             enum_row_image_type row_image_type
                                             [[maybe_unused]]) {
  std::string value;
  switch (field_info.field_type) {
    case MYSQL_TYPE_LONG: {
      uint32_t temp = 0;
      memcpy(&temp, field_info.value, 4);
      value = std::to_string(temp);
    } break;
    case MYSQL_TYPE_VARCHAR: {
      int meta_length = field_info.field_meta > 255 ? 2 : 1;
      value.append((const char *)(field_info.value + meta_length),
                   field_info.size - meta_length);
    } break;
    default:
      break;
  }
  char sql[1024];
  size_t len = sprintf(
      sql,
      "INSERT INTO test.output (type, info) VALUES ('FIELD', 'field_no: "
      "%lu, field_type: %d, field_meta: %d, field_size=%lu, field_in_used: "
      "%d, field_null: %d, field_value: %s')",
      field_info.field_no, (int)field_info.field_type, field_info.field_meta,
      field_info.size,
      bitmap_is_set(field_info.cols_bitmap, field_info.field_no),
      field_info.null_flag, value.c_str());
  (void)mysql_send_query(m_client, sql, len);
  const struct MYSQL_METHODS *methods = m_client->methods;
  (void)(methods->read_query_result)(m_client);
  return 0;
}

int Output_test_data_executor::process_row(TABLE *table [[maybe_unused]],
                                           Binlog_row_meta_info &row_info) {
  char sql[1024];
  size_t len = sprintf(
      sql,
      "INSERT INTO test.output (type, info) VALUES ('ROW_DATA', 'row_type: "
      "%d, row_size: %u'",
      (int)row_info.row_image_type, row_info.row_size);
  (void)mysql_send_query(m_client, sql, len);
  const struct MYSQL_METHODS *methods = m_client->methods;
  (void)(methods->read_query_result)(m_client);
  return 0;
}

int Output_test_data_executor::execute_row(const char *db_name,
                                           const char *table_name, int dml_type,
                                           bool end_flag) {
  char sql[1024];
  size_t len = sprintf(
      sql,
      "INSERT INTO test.output (type, info) VALUES ('ROW_EVENT', 'db: %s, "
      "table: %s, op: %s, end_stmt: %d')",
      db_name, table_name,
      (dml_type == 0 ? "INSERT" : (dml_type == 1 ? "UPDATE" : "DELETE")),
      end_flag);
  (void)mysql_send_query(m_client, sql, len);
  const struct MYSQL_METHODS *methods = m_client->methods;
  (void)(methods->read_query_result)(m_client);
  return 0;
}

int Output_test_data_executor::execute_gtid(
    std::shared_ptr<Gtid_log_event> &gtid_event) {
  char sql[1024];
  size_t len =
      sprintf(sql, "INSERT INTO test.output (type, info) VALUES ('GTID', '%s')",
              gtid_event->get_gtid().c_str());
  (void)mysql_send_query(m_client, sql, len);
  const struct MYSQL_METHODS *methods = m_client->methods;
  (void)(methods->read_query_result)(m_client);
  m_current_gtid_log_event = gtid_event;
  return 0;
}

int Output_test_data_executor::start_trx() {
  char sql[1024];
  size_t len = sprintf(
      sql, "INSERT INTO test.output (type, info) VALUES ('START_TRX', '')");
  (void)mysql_send_query(m_client, sql, len);
  const struct MYSQL_METHODS *methods = m_client->methods;
  (void)(methods->read_query_result)(m_client);
  return 0;
}

int Output_test_data_executor::commit_trx() {
  char sql[1024];
  size_t len = sprintf(
      sql, "INSERT INTO test.output (type, info) VALUES ('COMMIT_TRX', '')");
  (void)mysql_send_query(m_client, sql, len);
  const struct MYSQL_METHODS *methods = m_client->methods;
  (void)(methods->read_query_result)(m_client);
  m_current_gtid_log_event = nullptr;
  return 0;
}

int Output_test_data_executor::execute_ddl(
    std::shared_ptr<Query_log_event> &query_event) {
  std::string ddl(query_event->query, query_event->q_len);
  char sql[1024];
  size_t len =
      sprintf(sql, "INSERT INTO test.output (type, info) VALUES ('DDL', '%s')",
              ddl.c_str());
  (void)mysql_send_query(m_client, sql, len);
  const struct MYSQL_METHODS *methods = m_client->methods;
  (void)(methods->read_query_result)(m_client);
  return 0;
}

/* end class Output_test_data_executor */
#endif  // NDEBUG

TABLE *open_table_for_task(THD *thd, const char *schema_name,
                           const char *table_name) {
  Table_ref tables(schema_name, table_name, TL_READ);
  if (thd->lex->current_query_block() == nullptr) {
    thd->lex->thd = thd;
    Query_block *const select = thd->lex->new_empty_query_block();
    thd->lex->set_current_query_block(select);
  }
  tables.query_block = thd->lex->current_query_block();
  Open_table_context ot_ctx(thd, 0);
  if (open_table(thd, &tables, &ot_ctx)) {
    LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG, "open table failed");
    return nullptr;
  }
  TABLE *table = thd->open_tables;
  if (table == nullptr) {
    LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
           "open table failed or not base table");
    return nullptr;
  }
  return table;
}

void close_table_for_task(THD *thd) {
  while (thd->open_tables) {
    close_thread_table(thd, &thd->open_tables);
  }
  thd->mdl_context.release_transactional_locks();
}
