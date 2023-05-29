/*
   Copyright (c) 2000, 2022, Oracle and/or its affiliates. All rights reserved.
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

/* Copy data from a text file to table */

#include "sql/sql_load.h"

#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <shared_mutex>
// Execute_load_query_log_event,
// LOG_EVENT_UPDATE_TABLE_MAP_VERSION_F
#include <string.h>
#include <sys/types.h>

#include <algorithm>
#include <atomic>
#include <limits>

#include "libbinlogevents/include/load_data_events.h"
#include "m_ctype.h"
#include "m_string.h"
#include "my_base.h"
#include "my_bitmap.h"
#include "my_dbug.h"
#include "my_dir.h"
#include "my_inttypes.h"
#include "my_io.h"
#include "my_loglevel.h"
#include "my_macros.h"
#include "my_sys.h"
#include "my_thread_local.h"
#include "mysql/components/services/log_builtins.h"
#include "mysql/psi/mysql_file.h"
#include "mysql/service_mysql_alloc.h"
#include "mysql/thread_type.h"
#include "mysql_com.h"
#include "mysqld_error.h"
#include "sql/auth/auth_acls.h"
#include "sql/auth/auth_common.h"
#include "sql/binlog.h"
#include "sql/derror.h"
#include "sql/error_handler.h"  // Ignore_error_handler
#include "sql/field.h"
#include "sql/gdb_common.h"
#include "sql/handler.h"
#include "sql/item.h"
#include "sql/item_func.h"
#include "sql/item_timefunc.h"  // Item_func_now_local
#include "sql/log.h"
#include "sql/log_event.h"                       // Delete_file_log_event,
#include "sql/mysqld.h"                          // mysql_real_data_home
#include "sql/partitioning/partition_handler.h"  // for parallel load
#include "sql/protocol.h"
#include "sql/protocol_classic.h"
#include "sql/psi_memory_key.h"
#include "sql/query_result.h"
#include "sql/rpl_replica.h"
#include "sql/rpl_rli.h"                           // Relay_log_info
#include "sql/server_component/gdb_cmd_service.h"  // GreatDB
#include "sql/sql_base.h"  // fill_record_n_invoke_before_triggers
#include "sql/sql_class.h"
#include "sql/sql_error.h"
#include "sql/sql_insert.h"  // check_that_all_fields_are_given_values,
#include "sql/sql_lex.h"
#include "sql/sql_list.h"
#include "sql/sql_show.h"
#include "sql/sql_view.h"  // check_key_in_view
#include "sql/system_variables.h"
#include "sql/table.h"
#include "sql/table_trigger_dispatcher.h"  // Table_trigger_dispatcher
#include "sql/thr_malloc.h"
#include "sql/transaction_info.h"
#include "sql/trigger_def.h"
#include "sql_string.h"
#include "thr_lock.h"

class READ_INFO;

using std::max;
using std::min;

class XML_TAG {
 public:
  int level;
  String field;
  String value;
  XML_TAG(int l, String f, String v);
};

XML_TAG::XML_TAG(int l, String f, String v) {
  level = l;
  field.append(f);
  value.append(v);
}

#define GET (stack_pos != stack ? *--stack_pos : my_b_get(&cache))
#define PUSH(A) *(stack_pos++) = (A)
#define SUB_LOAD_SESSION_LOCK_TIME_OUT 60

class READ_INFO {
  File file;
  uchar *buffer;      /* Buffer for read text */
  uchar *end_of_buff; /* Data in buffer ends here */
  size_t buff_length; /* Length of buffer */
  const uchar *field_term_ptr, *line_term_ptr;
  const char *line_start_ptr, *line_start_end;
  size_t field_term_length, line_term_length, enclosed_length;
  int field_term_char, line_term_char, enclosed_char, escape_char;
  int *stack, *stack_pos;
  bool found_end_of_line, start_of_line, eof;
  bool need_end_io_cache;
  IO_CACHE cache;
  int level; /* for load xml */

  size_t max_size() { return std::numeric_limits<size_t>::max() - 1; }

  size_t check_length(size_t length, size_t grow) {
    // Adding new element to the end of the buffer in amortized constant time is
    // possible only if buffer capacity grows geometrically (capacity * 2) when
    // buffer is full.
    const size_t new_length = length + std::max(length, grow);
    return ((new_length < length || new_length > max_size()) ? max_size()
                                                             : new_length);
  }

 public:
  bool error, line_truncated, found_null, enclosed;
  uchar *row_start, /* Found row starts here */
      *row_end;     /* Found row ends here */
  const CHARSET_INFO *read_charset;

  READ_INFO(File file, size_t tot_length, const CHARSET_INFO *cs,
            const String &field_term, const String &line_start,
            const String &line_term, const String &enclosed, int escape,
            bool get_it_from_net, bool is_fifo,
            IO_CACHE *chunk_cache = nullptr);
  ~READ_INFO();
  bool read_field();
  bool read_fixed_length();
  bool next_line();
  char unescape(char chr);
  bool terminator(const uchar *ptr, size_t length);
  bool find_start_of_fields();
  /* load xml */
  List<XML_TAG> taglist;
  int read_value(int delim, String *val);
  int read_cdata(String *val, bool *have_cdata);
  bool read_xml();
  void clear_level(int level);

  /* parallel load data */
  bool never_end_io_cache{false};  // set to true if is load worker
  int read_line(IO_CACHE *dst_cache, size_t &lines, size_t &totoal_read_len);

  /*
    We need to force cache close before destructor is invoked to log
    the last read block
  */
  void end_io_cache() {
    if (!never_end_io_cache) ::end_io_cache(&cache);
    need_end_io_cache = false;
  }

  /*
    Either this method, or we need to make cache public
    Arg must be set from Sql_cmd_load_table::execute_inner()
    since constructor does not see either the table or THD value
  */
  void set_io_cache_arg(void *arg) { cache.arg = arg; }

  /**
    skip all data till the eof.
  */
  void skip_data_till_eof() {
    while (GET != my_b_EOF)
      ;
  }
};

/* used for manage file chunk cache for parallel load */
class Gdb_load_job;
/* since parallel forbid handle duplicates, and the skipped n lines is handled
   by master session, and the ignore eror option is removed from load worker
   (enable batch insert). So the skipped rows is always 0 for now. */
class Gdb_load_worker_result {
 public:
  Gdb_load_worker_result() { reset(); }
  void reset() {
    m_errno = 0;
    m_errmsg = "";
    m_lines = 0;
    m_inserted_rows = 0;
    m_skipped_rows = 0;
    m_warnings = 0;
  }
  int m_errno;
  std::string m_errmsg;
  uint64_t m_lines;
  uint64_t m_inserted_rows;
  uint64_t m_skipped_rows;
  uint m_warnings;  // always 0, not implement collect warnings yet
};
class Gdb_load_worker {
 public:
  Gdb_load_worker(Gdb_load_job *master_job, uint worker_id)
      : m_master_job(master_job),
        m_cur_chunk_no(0),
        m_cur_chunk(nullptr),
        m_cur_stop(true),
        m_cur_result(),
        m_worker_id(worker_id),
        m_inited(false),
        m_exit(false),
        m_do_abort(false) {
    mysql_mutex_init(0, &m_worker_mutex, MY_MUTEX_INIT_FAST);
    mysql_cond_init(0, &m_worker_cond);
  }
  ~Gdb_load_worker() {
    mysql_mutex_destroy(&m_worker_mutex);
    mysql_cond_destroy(&m_worker_cond);
  }

  static void *thread_func(void *worker_handler);

  /* end io_cache in worker if start success */
  int start();
  int setup_session_env(Gdb_cmd_service &cmd_service);
  bool exited() { return m_exit; }
  int add_subtask(std::shared_ptr<IO_CACHE> chunk_cache, uint chunk_no,
                  size_t lines);

  Gdb_load_job *m_master_job;

  /* subtask variables */
  uint m_cur_chunk_no;
  std::shared_ptr<IO_CACHE> m_cur_chunk;
  std::atomic<bool> m_cur_stop;
  Gdb_load_worker_result m_cur_result;

  /* worker thread variables */
  mysql_mutex_t m_worker_mutex;
  mysql_cond_t m_worker_cond;
  uint m_worker_id;
  my_thread_handle m_handle;
  std::atomic<bool> m_inited;
  std::atomic<bool> m_exit;
  std::atomic<bool> m_do_abort;  // set to true by master when finish or abort
};
class Gdb_load_job {
 public:
  Gdb_load_job(THD *thd, uint session_id) {
    /* called by master session */
    assert(thd->variables.gdb_parallel_load);
    m_session_id = session_id;
    m_max_workers = thd->variables.gdb_parallel_load_workers;
    m_chunk_size = thd->variables.gdb_parallel_load_chunk_size;
    m_chunk_no_idx = 1;
    m_master_is_waiting = false;
    mysql_mutex_init(0, &m_master_mutex, MY_MUTEX_INIT_FAST);
    mysql_cond_init(0, &m_master_cond);
  }
  ~Gdb_load_job() {
    mysql_mutex_destroy(&m_master_mutex);
    mysql_cond_destroy(&m_master_cond);
  }
  inline uint get_session_id() { return m_session_id; }
  std::shared_ptr<Gdb_load_worker> get_handler_by_worker_id(uint worker_id);
  bool can_add_subtask() {
    std::lock_guard<std::mutex> lock(m_list_mutex);
    return m_idle_workers.size() > 0 || m_workers.size() < m_max_workers;
  }
  int spawn_worker();
  int add_subtask_to_worker(std::shared_ptr<IO_CACHE> chunk_cache,
                            size_t lines);
  int wait_finished_subtask(Gdb_load_worker_result &result);
  void join_workers();

  void set_cmd_sql(const String *cmd_sql) { m_cmd_sql = cmd_sql; }
  void set_current_db(const LEX_CSTRING *current_db) { m_db = current_db; }
  /* not consider lower/upper case */
  void add_session_var(const std::string &var_name,
                       const std::string &var_value, bool add_quota = false) {
    if (!add_quota)
      m_session_vars[var_name] = var_value;
    else
      m_session_vars[var_name] =
          gdb_string_add_quote(var_value, VALUE_QUOTE_CHAR);
  }
  int add_collected_vars(THD *thd,
                         Item_func::Collect_session_vars &session_vars);

 private:
  uint m_session_id;
  uint m_max_workers;
  uint m_chunk_size;
  uint m_chunk_no_idx;  // inc for numbering file chunk, start from 1

  /* m_workers contains all worker's handler, until parallel loading finish.
     m_idle_workers only contain the idle worker's handler. When add a file
     chunk to a worker(add_subtask_to_worker), master session will first
     check whether idle_workers is empty. If not, mv the front worker out of
     m_idle_workers, signal it to running stage(no long in m_idle_workers).
     If empty, spawn a worker and add it into m_workers as well as
     m_idle_workers.

     When worker finish loading file chunk, it will add its handler into
     m_finished_workers(waiting for master session read execute result).

     After master session read all data of file, it will continally waiting
     and reading m_finished_workers, until all workers is in idle(Since
     master session always try add subtask before read finnished subtask,
     if m_idle_workers.size() == m_workers.size(), it means the parallel
     load is complete). */
  std::vector<std::shared_ptr<Gdb_load_worker>> m_workers;
  std::list<Gdb_load_worker *> m_finished_workers;
  std::list<Gdb_load_worker *> m_idle_workers;

  std::mutex m_list_mutex;
  bool m_master_is_waiting;
  const String *m_cmd_sql;
  const LEX_CSTRING *m_db;
  std::map<std::string, std::string> m_session_vars;
  // NOTE: don't hold master&worker mutex at the same time in master thread
  mysql_mutex_t m_master_mutex;
  mysql_cond_t m_master_cond;

  friend class Gdb_load_worker;
};
class Gdb_load_jobs_mgr {
 public:
  static Gdb_load_jobs_mgr &instance() {
    static Gdb_load_jobs_mgr load_jobs_mgr;
    return load_jobs_mgr;
  }

  std::shared_ptr<Gdb_load_job> get_load_job(uint session_id) {
    std::lock_guard<std::mutex> lock(m_mutex);
    auto itr = m_load_jobs.find(session_id);
    if (itr == m_load_jobs.end()) return nullptr;
    return itr->second;
  }

  bool register_load_job(std::shared_ptr<Gdb_load_job> job) {
    std::lock_guard<std::mutex> lock(m_mutex);
    auto session_id = job->get_session_id();
    auto itr = m_load_jobs.find(session_id);
    if (itr != m_load_jobs.end()) return true;
    m_load_jobs.emplace(session_id, job);
    return false;
  }

  void unregister_load_job(std::shared_ptr<Gdb_load_job> job) {
    std::lock_guard<std::mutex> lock(m_mutex);
    auto session_id = job->get_session_id();
    auto itr = m_load_jobs.find(session_id);
    if (itr == m_load_jobs.end()) return;
    m_load_jobs.erase(itr);
  }

 private:
  /* <session_id, load_job> */
  std::map<uint, std::shared_ptr<Gdb_load_job>> m_load_jobs;
  /* since most of CPUs do execution, use mutex is enough */
  std::mutex m_mutex;
};

static std::shared_ptr<Gdb_load_worker> get_load_worker_by_id(
    uint master_session_id, uint worker_id) {
  DBUG_TRACE;
  auto &job_mgr = Gdb_load_jobs_mgr::instance();
  auto load_job = job_mgr.get_load_job(master_session_id);
  if (load_job == nullptr) return nullptr;

  /* lock job mutex and get worker handler */
  return load_job->get_handler_by_worker_id(worker_id);
}

/* The file name for parallel load worker is combined master_thd_id and
   file_chunk_id. Given those two id, slave worker can locate the address
   of IO_CACHE which contain file chunk data. */
bool Sql_cmd_load_table::parse_id_from_filename(const char *file_name) {
  DBUG_TRACE;
  size_t len = strlen(file_name);
  auto pos = strstr(file_name, ":");
  if (pos == nullptr || pos == file_name ||
      pos > file_name + len - 2 /*':' at the last*/)
    return true;
  for (auto ptr = file_name; ptr < pos; ptr++) {
    if (*ptr > '9' || *ptr < '0') return true;
  }
  for (auto ptr = pos + 1, end = file_name + len; ptr < end; ptr++) {
    if (*ptr > '9' || *ptr < '0') return true;
  }
  m_master_session_id = std::stoul(std::string(file_name, pos - file_name));
  m_worker_id = std::stoul(std::string(pos + 1));
  return false;
}

/**
  Execute LOAD DATA query

  @param thd                 Current thread.
  @param handle_duplicates   Indicates whenever we should emit error or
                             replace row if we will meet duplicates.

  @returns true if error
*/
bool Sql_cmd_load_table::execute_inner(THD *thd,
                                       enum enum_duplicates handle_duplicates) {
  char name[FN_REFLEN];
  File file;
  bool error = false;
  const String *field_term = m_exchange.field.field_term;
  const String *escaped = m_exchange.field.escaped;
  const String *enclosed = m_exchange.field.enclosed;
  bool is_fifo = false;
  Query_block *select = thd->lex->query_block;
  LOAD_FILE_INFO lf_info;
  THD::killed_state killed_status = THD::NOT_KILLED;
  bool is_concurrent;
  bool transactional_table;
  Table_ref *const table_list = thd->lex->query_tables;
  const char *db = table_list->db;  // This is never null
  /*
    If path for file is not defined, we will use the current database.
    If this is not set, we will use the directory where the table to be
    loaded is located
  */
  const char *tdb = thd->db().str ? thd->db().str : db;  // Result is never null
  ulong skip_lines = m_exchange.skip_lines;
  DBUG_TRACE;

  /*
    Bug #34283
    mysqlbinlog leaves tmpfile after termination if binlog contains
    load data infile, so in mixed mode we go to row-based for
    avoiding the problem.
  */
  thd->set_current_stmt_binlog_format_row_if_mixed();

  if (escaped->length() > 1 || enclosed->length() > 1) {
    my_error(ER_WRONG_FIELD_TERMINATORS, MYF(0));
    return true;
  }

  /* Report problems with non-ascii separators */
  if (!escaped->is_ascii() || !enclosed->is_ascii() ||
      !field_term->is_ascii() || !m_exchange.line.line_term->is_ascii() ||
      !m_exchange.line.line_start->is_ascii()) {
    push_warning(thd, Sql_condition::SL_WARNING,
                 WARN_NON_ASCII_SEPARATOR_NOT_IMPLEMENTED,
                 ER_THD(thd, WARN_NON_ASCII_SEPARATOR_NOT_IMPLEMENTED));
  }

  if (open_and_lock_tables(thd, table_list, 0)) return true;

  THD_STAGE_INFO(thd, stage_executing);
  if (select->setup_tables(thd, table_list, false)) return true;

  if (run_before_dml_hook(thd)) return true;

  if (table_list->is_view() && select->resolve_placeholder_tables(thd, false))
    return true; /* purecov: inspected */

  Table_ref *const insert_table_ref =
      table_list->is_updatable() &&  // View must be updatable
              !table_list
                   ->is_multiple_tables() &&  // Multi-table view not allowed
              !table_list->is_derived()
          ?  // derived tables not allowed
          table_list->updatable_base_table()
          : nullptr;

  if (insert_table_ref == nullptr ||
      check_key_in_view(thd, table_list, insert_table_ref)) {
    my_error(ER_NON_UPDATABLE_TABLE, MYF(0), table_list->alias, "LOAD");
    return true;
  }
  if (select->derived_table_count &&
      select->check_view_privileges(thd, INSERT_ACL, SELECT_ACL))
    return true; /* purecov: inspected */

  if (table_list->is_merged()) {
    if (table_list->prepare_check_option(thd)) return true;

    if (handle_duplicates == DUP_REPLACE &&
        table_list->prepare_replace_filter(thd))
      return true;
  }

  // Pass the check option down to the underlying table:
  insert_table_ref->check_option = table_list->check_option;
  /*
    Let us emit an error if we are loading data to table which is used
    in subselect in SET clause like we do it for INSERT.

    The main thing to fix to remove this restriction is to ensure that the
    table is marked to be 'used for insert' in which case we should never
    mark this table as 'const table' (ie, one that has only one row).
  */
  if (unique_table(insert_table_ref, table_list->next_global, false)) {
    my_error(ER_UPDATE_TABLE_USED, MYF(0), table_list->table_name);
    return true;
  }

  TABLE *const table = insert_table_ref->table;

  for (Field **cur_field = table->field; *cur_field; ++cur_field)
    (*cur_field)->reset_warnings();

  transactional_table = table->file->has_transactions();
  is_concurrent =
      (table_list->lock_descriptor().type == TL_WRITE_CONCURRENT_INSERT);

  if (m_opt_fields_or_vars.empty()) {
    Field_iterator_table_ref field_iterator;
    field_iterator.set(table_list);
    for (; !field_iterator.end_of_fields(); field_iterator.next()) {
      // Do not include user hidden fields.
      if (field_iterator.field() != nullptr &&
          field_iterator.field()->is_hidden())
        continue;

      Item *item;
      if (!(item = field_iterator.create_item(thd))) return true;

      if (item->field_for_view_update() == nullptr) {
        my_error(ER_NONUPDATEABLE_COLUMN, MYF(0), item->item_name.ptr());
        return true;
      }
      m_opt_fields_or_vars.push_back(item->real_item());
    }
    bitmap_set_all(table->write_set);
    /*
      Let us also prepare SET clause, although it is probably empty
      in this case.
    */
    if (setup_fields(thd, /*want_privilege=*/INSERT_ACL,
                     /*allow_sum_func=*/false, /*split_sum_funcs=*/false,
                     /*column_update=*/true, /*typed_items=*/nullptr,
                     &m_opt_set_fields, Ref_item_array()) ||
        setup_fields(thd, /*want_privilege=*/SELECT_ACL,
                     /*allow_sum_func=*/false, /*split_sum_funcs=*/false,
                     /*column_update=*/false, /*typed_items=*/nullptr,
                     &m_opt_set_exprs, Ref_item_array()))
      return true;
  } else {  // Part field list
    /*
      Because m_opt_fields_or_vars may contain user variables,
      pass false for column_update in first call below.
    */
    if (setup_fields(thd, /*want_privilege=*/INSERT_ACL,
                     /*allow_sum_func=*/false, /*split_sum_funcs=*/false,
                     /*column_update=*/false, /*typed_items=*/nullptr,
                     &m_opt_fields_or_vars, Ref_item_array()) ||
        setup_fields(thd, /*want_privilege=*/INSERT_ACL,
                     /*allow_sum_func=*/false, /*split_sum_funcs=*/false,
                     /*column_update=*/true, /*typed_items=*/nullptr,
                     &m_opt_set_fields, Ref_item_array()))
      return true;

    /*
      Special updatability test is needed because m_opt_fields_or_vars may
      contain a mix of column references and user variables.
    */
    for (Item *item : m_opt_fields_or_vars) {
      if ((item->type() == Item::FIELD_ITEM ||
           item->type() == Item::REF_ITEM) &&
          item->field_for_view_update() == nullptr) {
        my_error(ER_NONUPDATEABLE_COLUMN, MYF(0), item->item_name.ptr());
        return true;
      }
      if (item->type() == Item::STRING_ITEM) {
        /*
          This item represents a user variable. Create a new item with the
          same name that can be added to LEX::set_var_list. This ensures
          that corresponding Item_func_get_user_var items are resolved as
          non-const items.
        */
        Item_func_set_user_var *user_var =
            new (thd->mem_root) Item_func_set_user_var(item->item_name, item);
        if (user_var == nullptr) return true;
        thd->lex->set_var_list.push_back(user_var);
      }
    }

    // Consider the following table:
    //
    //   CREATE TABLE t1 (x DOUBLE, y DOUBLE, g POINT SRID 4326 NOT NULL);
    //
    // If the user wants to load a file which only contains two values (x and y
    // coordinates), it is possible to do it by executing the following
    // statement:
    //
    //  LOAD DATA INFILE 'data' (@x, @y)
    //    SET x = @x, y = @y, g = ST_SRID(POINT(@x, @y));
    //
    // However, the columns that are specified in the SET clause are only marked
    // in the write set, and not in fields_set_during_insert. The latter is the
    // bitmap used during check_that_all_fields_are_given_values(), so we need
    // to copy the bits from the write set over to said bitmap. If not, the
    // server will return an error saying that column 'g' doesn't have a default
    // value.
    bitmap_union(table->fields_set_during_insert, table->write_set);

    if (check_that_all_fields_are_given_values(thd, table, table_list))
      return true;
    /* Fix the expressions in SET clause */
    if (setup_fields(thd, /*want_privilege=*/SELECT_ACL,
                     /*allow_sum_func=*/false, /*split_sum_funcs=*/false,
                     /*column_update=*/false, /*typed_items=*/nullptr,
                     &m_opt_set_exprs, Ref_item_array()))
      return true;
  }

  const int escape_char =
      (escaped->length() &&
       (m_exchange.escaped_given() ||
        !(thd->variables.sql_mode & MODE_NO_BACKSLASH_ESCAPES)))
          ? (*escaped)[0]
          : INT_MAX;

  /*
    * LOAD DATA INFILE fff INTO TABLE xxx SET columns2
    sets all columns, except if file's row lacks some: in that case,
    defaults are set by read_fixed_length() and read_sep_field(),
    not by COPY_INFO.
    * LOAD DATA INFILE fff INTO TABLE xxx (columns1) SET columns2=
    may need a default for columns other than columns1 and columns2.
  */
  const bool manage_defaults = !m_opt_fields_or_vars.empty();
  COPY_INFO info(COPY_INFO::INSERT_OPERATION, &m_opt_fields_or_vars,
                 &m_opt_set_fields, manage_defaults, handle_duplicates,
                 escape_char);

  if (info.add_function_default_columns(table, table->write_set)) return true;

  if (table->triggers) {
    if (table->triggers->mark_fields(TRG_EVENT_INSERT)) return true;
  }

  prepare_triggers_for_insert_stmt(thd, table);

  size_t tot_length = 0;
  bool use_blobs = false, use_vars = false;

  for (Item *item : m_opt_fields_or_vars) {
    const Item *real_item = item->real_item();

    if (real_item->type() == Item::FIELD_ITEM) {
      const Field *field = down_cast<const Item_field *>(real_item)->field;
      if (field->is_flag_set(BLOB_FLAG)) {
        use_blobs = true;
        tot_length += 4096;  // Will be extended if needed
      } else {
        tot_length += field->field_length;
      }
    } else if (item->type() == Item::STRING_ITEM) {
      use_vars = true;
    }
  }
  if (use_blobs && m_exchange.line.line_term->is_empty() &&
      field_term->is_empty()) {
    my_error(ER_BLOBS_AND_NO_TERMINATED, MYF(0));
    return true;
  }
  if (use_vars && !field_term->length() && !enclosed->length()) {
    my_error(ER_LOAD_FROM_FIXED_SIZE_ROWS_TO_VAR, MYF(0));
    return true;
  }

  std::shared_ptr<Gdb_load_worker> worker_handler(nullptr);  // parallel load
  if (thd->is_parallel_load_executor()) {
    /* if is the executor of parallel load, get IO_CACHE directly */
    assert(!thd->variables.gdb_parallel_load);
    file = -1;
    if (parse_id_from_filename(m_exchange.file_name)) {
      my_error(ER_GDB_PARALLEL_LOAD_WORKER, MYF(0),
               "worker handler name parser failed");
      return true;
    }
    worker_handler = get_load_worker_by_id(m_master_session_id, m_worker_id);
    if (worker_handler == nullptr || worker_handler->m_cur_chunk == nullptr) {
      my_error(ER_GDB_PARALLEL_LOAD_WORKER, MYF(0),
               "worker handler not found or file_chunk empty");
      return true;
    }
  } else if (m_is_local_file) {
    (void)net_request_file(thd->get_protocol_classic()->get_net(),
                           m_exchange.file_name);
    file = -1;
  } else {
    if (!dirname_length(m_exchange.file_name)) {
      strxnmov(name, FN_REFLEN - 1, mysql_real_data_home, tdb, NullS);
      (void)fn_format(name, m_exchange.file_name, name, "",
                      MY_RELATIVE_PATH | MY_UNPACK_FILENAME);
    } else {
      (void)fn_format(
          name, m_exchange.file_name, mysql_real_data_home, "",
          MY_RELATIVE_PATH | MY_UNPACK_FILENAME | MY_RETURN_REAL_PATH);
    }

    if ((thd->system_thread &
         (SYSTEM_THREAD_SLAVE_SQL | SYSTEM_THREAD_SLAVE_WORKER)) != 0) {
      Relay_log_info *rli = thd->rli_slave->get_c_rli();

      if (strncmp(rli->slave_patternload_file, name,
                  rli->slave_patternload_file_size)) {
        /*
          LOAD DATA INFILE in the slave SQL Thread can only read from
          --replica-load-tmpdir". This should never happen. Please, report a
          bug.
        */
        LogErr(ERROR_LEVEL, ER_LOAD_DATA_INFILE_FAILED_IN_UNEXPECTED_WAY);
        my_error(ER_OPTION_PREVENTS_STATEMENT, MYF(0), "--replica-load-tmpdir");
        return true;
      }
    } else if (!is_secure_file_path(name)) {
      /* Read only allowed from within dir specified by secure_file_priv */
      my_error(ER_OPTION_PREVENTS_STATEMENT, MYF(0), "--secure-file-priv");
      return true;
    }

#if !defined(_WIN32)
    MY_STAT stat_info;
    if (!my_stat(name, &stat_info, MYF(MY_WME))) return true;

    // if we are not in slave thread, the file must be:
    if (!thd->slave_thread &&
        !((stat_info.st_mode & S_IFLNK) != S_IFLNK &&   // symlink
          ((stat_info.st_mode & S_IFREG) == S_IFREG ||  // regular file
           (stat_info.st_mode & S_IFIFO) == S_IFIFO)))  // named pipe
    {
      my_error(ER_TEXTFILE_NOT_READABLE, MYF(0), name);
      return true;
    }
    if ((stat_info.st_mode & S_IFIFO) == S_IFIFO) is_fifo = true;
#endif
    if ((file = mysql_file_open(key_file_load, name, O_RDONLY, MYF(MY_WME))) <
        0)

      return true;
  }

  IO_CACHE *file_chunk_cache =
      worker_handler == nullptr ? nullptr : worker_handler->m_cur_chunk.get();
  READ_INFO read_info(
      file, tot_length,
      m_exchange.cs ? m_exchange.cs : thd->variables.collation_database,
      *field_term, *m_exchange.line.line_start, *m_exchange.line.line_term,
      *enclosed, info.escape_char, m_is_local_file, is_fifo, file_chunk_cache);
  if (read_info.error) {
    if (file >= 0) mysql_file_close(file, MYF(0));  // no files in net reading
    return true;                                    // Can't allocate buffers
  }

  if (mysql_bin_log.is_open()) {
    lf_info.thd = thd;
    lf_info.logged_data_file = false;
    lf_info.last_pos_in_file = HA_POS_ERROR;
    lf_info.log_delayed = transactional_table;
    read_info.set_io_cache_arg((void *)&lf_info);
  }

  thd->check_for_truncated_fields = CHECK_FIELD_WARN;
  thd->num_truncated_fields = 0L;
  /* Skip lines if there is a line terminator */
  if (m_exchange.line.line_term->length() &&
      m_exchange.filetype != FILETYPE_XML) {
    /* m_exchange.skip_lines needs to be preserved for logging */
    while (skip_lines > 0) {
      skip_lines--;
      if (read_info.next_line()) break;
    }
  }

  if (!(error = read_info.error)) {
    table->next_number_field = table->found_next_number_field;
    if (thd->lex->is_ignore() || handle_duplicates == DUP_REPLACE)
      table->file->ha_extra(HA_EXTRA_IGNORE_DUP_KEY);
    if (handle_duplicates == DUP_REPLACE &&
        (!table->triggers || !table->triggers->has_delete_triggers()))
      table->file->ha_extra(HA_EXTRA_WRITE_CAN_REPLACE);
    if (thd->locked_tables_mode <= LTM_LOCK_TABLES)
      table->file->ha_start_bulk_insert((ha_rows)0);
    table->copy_blobs = true;

    if (m_exchange.filetype == FILETYPE_XML) /* load xml */
      error =
          read_xml_field(thd, info, insert_table_ref, read_info, skip_lines);
    else if (!field_term->length() && !enclosed->length())
      error =
          read_fixed_length(thd, info, insert_table_ref, read_info, skip_lines);
    else {
      if (thd->variables.gdb_parallel_load) m_parallel_load = true;
      if (m_parallel_load && handle_duplicates == DUP_REPLACE) {
        push_warning_printf(
            thd, Sql_condition::SL_WARNING, ER_GDB_PARALLEL_LOAD,
            ER_THD(thd, ER_GDB_PARALLEL_LOAD),
            "unsupport handle duplicates replace, use normal load data");
        m_parallel_load = false;  // forbid parallel for DUP_REPLACE
      }
      /* NOTE: if use parallel load, just load first row for test */
      error = read_sep_field(thd, info, insert_table_ref, read_info, *enclosed,
                             skip_lines);
      if (!error && m_parallel_load) /* parallel load, using sub sessions */
        error = read_sep_line(thd, info, read_info);
    }
    if (thd->locked_tables_mode <= LTM_LOCK_TABLES &&
        table->file->ha_end_bulk_insert() && !error) {
      table->file->print_error(my_errno(), MYF(0));
      error = true;
    }
    table->next_number_field = nullptr;
  }
  if (file >= 0) mysql_file_close(file, MYF(0));
  free_blobs(table); /* if pack_blob was used */
  table->copy_blobs = false;
  thd->check_for_truncated_fields = CHECK_FIELD_IGNORE;
  /*
     simulated killing in the middle of per-row loop
     must be effective for binlogging
  */
  DBUG_EXECUTE_IF("simulate_kill_bug27571", {
    error = true;
    thd->killed = THD::KILL_QUERY;
  };);

  killed_status = error ? thd->killed.load() : THD::NOT_KILLED;

  if (error) {
    if (m_is_local_file) read_info.skip_data_till_eof();

    if (mysql_bin_log.is_open()) {
      {
        /*
          Make sure last block (the one which caused the error) gets
          logged.  This is needed because otherwise after write of (to
          the binlog, not to read_info (which is a cache))
          Delete_file_log_event the bad block will remain in read_info
          (because pre_read is not called at the end of the last
          block; remember pre_read is called whenever a new block is
          read from disk).  At the end of Sql_cmd_load_table::execute_inner(),
          the destructor of read_info will call end_io_cache() which will flush
          read_info, so we will finally have this in the binlog:

          Append_block # The last successful block
          Delete_file
          Append_block # The failing block
          which is nonsense.
          Or could also be (for a small file)
          Create_file  # The failing block
          which is nonsense (Delete_file is not written in this case, because:
          Create_file has not been written, so Delete_file is not written, then
          when read_info is destroyed end_io_cache() is called which writes
          Create_file.
        */
        read_info.end_io_cache();
        /* If the file was not empty, wrote_create_file is true */
        if (lf_info.logged_data_file) {
          int errcode = query_error_code(thd, killed_status == THD::NOT_KILLED);

          /* since there is already an error, the possible error of
             writing binary log will be ignored */
          if (thd->get_transaction()->cannot_safely_rollback(
                  Transaction_ctx::STMT))
            (void)write_execute_load_query_log_event(
                thd, table_list->db, table_list->table_name, is_concurrent,
                handle_duplicates, transactional_table, errcode);
          else {
            Delete_file_log_event d(thd, db, transactional_table);
            (void)mysql_bin_log.write_event(&d);
          }
        }
      }
    }
    error = true;  // Error on read
    goto err;
  }

  snprintf(name, sizeof(name), ER_THD(thd, ER_LOAD_INFO),
           (long)info.stats.records, (long)info.stats.deleted,
           (long)(info.stats.records - info.stats.copied),
           (long)thd->get_stmt_da()->current_statement_cond_count());

  if (mysql_bin_log.is_open()) {
    /*
      We need to do the job that is normally done inside
      binlog_query() here, which is to ensure that the pending event
      is written before tables are unlocked and before any other
      events are written.  We also need to update the table map
      version for the binary log to mark that table maps are invalid
      after this point.
     */
    if (thd->is_current_stmt_binlog_format_row())
      error = thd->binlog_flush_pending_rows_event(true, transactional_table);
    else {
      /*
        As already explained above, we need to call end_io_cache() or the last
        block will be logged only after Execute_load_query_log_event (which is
        wrong), when read_info is destroyed.
      */
      read_info.end_io_cache();
      if (lf_info.logged_data_file) {
        int errcode = query_error_code(thd, killed_status == THD::NOT_KILLED);
        error = write_execute_load_query_log_event(
            thd, table_list->db, table_list->table_name, is_concurrent,
            handle_duplicates, transactional_table, errcode);
      }
    }
    if (error) goto err;
  }

  /* ok to client sent only after binlog write and engine commit */
  my_ok(thd, info.stats.copied + info.stats.deleted, 0L, name);
err:
  assert(table->file->has_transactions() ||
         !(info.stats.copied || info.stats.deleted) ||
         thd->get_transaction()->cannot_safely_rollback(Transaction_ctx::STMT));
  table->file->ha_release_auto_increment();
  return error;
}

/**
  @note Not a very useful function; just to avoid duplication of code

  @returns true if error
*/
bool Sql_cmd_load_table::write_execute_load_query_log_event(
    THD *thd, const char *db_arg, const char *table_name_arg,
    bool is_concurrent, enum enum_duplicates duplicates,
    bool transactional_table, int errcode) {
  const char *tbl = table_name_arg;
  const char *tdb = (thd->db().str != nullptr ? thd->db().str : db_arg);
  const String *query = nullptr;
  String string_buf;
  size_t fname_start = 0;
  size_t fname_end = 0;

  if (thd->db().str == nullptr || strcmp(db_arg, thd->db().str)) {
    /*
      If used database differs from table's database,
      prefix table name with database name so that it
      becomes a FQ name.
     */
    string_buf.set_charset(system_charset_info);
    append_identifier(thd, &string_buf, db_arg, strlen(db_arg));
    string_buf.append(".");
  }
  append_identifier(thd, &string_buf, table_name_arg, strlen(table_name_arg));
  tbl = string_buf.c_ptr_safe();
  Load_query_generator gen(thd, &m_exchange, tdb, tbl, is_concurrent,
                           duplicates == DUP_REPLACE, thd->lex->is_ignore());
  query = gen.generate(&fname_start, &fname_end);

  Execute_load_query_log_event e(
      thd, query->ptr(), query->length(), fname_start, fname_end,
      (duplicates == DUP_REPLACE)
          ? binary_log::LOAD_DUP_REPLACE
          : (thd->lex->is_ignore() ? binary_log::LOAD_DUP_IGNORE
                                   : binary_log::LOAD_DUP_ERROR),
      transactional_table, false, false, errcode);

  return mysql_bin_log.write_event(&e);
}

namespace {
/**
  Checks if an item is a hidden generated column.

  @param table       Pointer to TABLE object
  @param item        Item to check

  @returns true if checked item is a hidden generated column.
*/
inline bool is_hidden_generated_column(TABLE *table, Item *item) {
  Item *real_item = item->real_item();
  if (table->has_gcol() && real_item->type() == Item::FIELD_ITEM) {
    const Field *field = down_cast<Item_field *>(real_item)->field;
    if (bitmap_is_set(&table->fields_for_functional_indexes,
                      field->field_index()))
      return true;
  }
  return false;
}
}  // namespace

/**
  Read of rows of fixed size + optional garbage + optional newline

  @param thd         Pointer to THD object
  @param info        Pointer to COPY_INFO object
  @param table_list  Pointer to Table_ref object
  @param read_info   Pointer to READ_INFO object
  @param skip_lines  Number of ignored lines
                     at the start of the file.

  @returns true if error
*/
bool Sql_cmd_load_table::read_fixed_length(THD *thd, COPY_INFO &info,
                                           Table_ref *table_list,
                                           READ_INFO &read_info,
                                           ulong skip_lines) {
  TABLE *table = table_list->table;
  bool err;
  DBUG_TRACE;

  while (!read_info.read_fixed_length()) {
    if (thd->killed) {
      thd->send_kill_message();
      return true;
    }
    if (skip_lines) {
      /*
        We could implement this with a simple seek if:
        - We are not using DATA INFILE LOCAL
        - escape character is  ""
        - line starting prefix is ""
      */
      skip_lines--;
      continue;
    }
    uchar *pos = read_info.row_start;

    restore_record(table, s->default_values);
    /*
      Check whether default values of the fields not specified in column list
      are correct or not.
    */
    if (validate_default_values_of_unset_fields(thd, table)) {
      read_info.error = true;
      break;
    }

    Autoinc_field_has_explicit_non_null_value_reset_guard after_each_row(table);

    for (Item *item : m_opt_fields_or_vars) {
      // Skip hidden generated columns.
      if (is_hidden_generated_column(table, item)) continue;
      /*
        There is no variables in fields_vars list in this format so
        this conversion is safe (no need to check for STRING_ITEM).
      */
      assert(item->real_item()->type() == Item::FIELD_ITEM);
      Item_field *sql_field = static_cast<Item_field *>(item->real_item());
      Field *field = sql_field->field;
      if (field == table->next_number_field)
        table->autoinc_field_has_explicit_non_null_value = true;
      /*
        No fields specified in fields_vars list can be null in this format.
        Mark field as not null, we should do this for each row because of
        restore_record...
      */
      field->set_notnull();

      if (pos == read_info.row_end) {
        thd->num_truncated_fields++; /* Not enough fields */
        push_warning_printf(thd, Sql_condition::SL_WARNING,
                            ER_WARN_TOO_FEW_RECORDS,
                            ER_THD(thd, ER_WARN_TOO_FEW_RECORDS),
                            thd->get_stmt_da()->current_row_for_condition());
        if (field->type() == FIELD_TYPE_TIMESTAMP && !field->is_nullable()) {
          // Specific of TIMESTAMP NOT NULL: set to CURRENT_TIMESTAMP.
          Item_func_now_local::store_in(field);
        }
      } else {
        uint length;
        uchar save_chr;
        if ((length = (uint)(read_info.row_end - pos)) > field->field_length)
          length = field->field_length;
        save_chr = pos[length];
        pos[length] = '\0';  // Safeguard aganst malloc
        field->store((char *)pos, length, read_info.read_charset);
        pos[length] = save_chr;
        if ((pos += length) > read_info.row_end)
          pos = read_info.row_end; /* Fills rest with space */
      }
    }
    if (pos != read_info.row_end) {
      thd->num_truncated_fields++; /* Too long row */
      push_warning_printf(thd, Sql_condition::SL_WARNING,
                          ER_WARN_TOO_MANY_RECORDS,
                          ER_THD(thd, ER_WARN_TOO_MANY_RECORDS),
                          thd->get_stmt_da()->current_row_for_condition());
    }

    if (thd->killed || fill_record_n_invoke_before_triggers(
                           thd, &info, m_opt_set_fields, m_opt_set_exprs, table,
                           TRG_EVENT_INSERT, table->s->fields, true, nullptr))
      return true;

    switch (table_list->view_check_option(thd)) {
      case VIEW_CHECK_SKIP:
        read_info.next_line();
        goto continue_loop;
      case VIEW_CHECK_ERROR:
        return true;
    }

    if (invoke_table_check_constraints(thd, table)) {
      if (thd->is_error()) return true;
      // continue when IGNORE clause is used.
      read_info.next_line();
      goto continue_loop;
    }

    err = write_record(thd, table, &info, nullptr);
    if (err) return true;

    /*
      We don't need to reset auto-increment field since we are restoring
      its default value at the beginning of each loop iteration.
    */
    if (read_info.next_line())  // Skip to next line
      break;
    if (read_info.line_truncated) {
      thd->num_truncated_fields++; /* Too long row */
      push_warning_printf(thd, Sql_condition::SL_WARNING,
                          ER_WARN_TOO_MANY_RECORDS,
                          ER_THD(thd, ER_WARN_TOO_MANY_RECORDS),
                          thd->get_stmt_da()->current_row_for_condition());
    }
    thd->get_stmt_da()->inc_current_row_for_condition();
  continue_loop:;
  }
  return read_info.error;
}

class Field_tmp_nullability_guard {
 public:
  explicit Field_tmp_nullability_guard(Item *item) : m_field(nullptr) {
    if (item->type() == Item::FIELD_ITEM) {
      m_field = ((Item_field *)item)->field;
      /*
        Enable temporary nullability for items that corresponds
        to table fields.
      */
      m_field->set_tmp_nullable();
    }
  }

  ~Field_tmp_nullability_guard() {
    if (m_field) m_field->reset_tmp_nullable();
  }

 private:
  Field *m_field;
};

/**
  Read rows in delimiter-separated formats.

  @param thd         Pointer to THD object
  @param info        Pointer to COPY_INFO object
  @param table_list  Pointer to Table_ref object
  @param read_info   Pointer to READ_INFO object
  @param enclosed    ENCLOSED BY character
  @param skip_lines  Number of ignored lines
                     at the start of the file.

  @returns true if error
*/
bool Sql_cmd_load_table::read_sep_field(THD *thd, COPY_INFO &info,
                                        Table_ref *table_list,
                                        READ_INFO &read_info,
                                        const String &enclosed,
                                        ulong skip_lines) {
  TABLE *table = table_list->table;
  size_t enclosed_length;
  bool err;
  DBUG_TRACE;

  enclosed_length = enclosed.length();

  for (;;) {
    if (thd->killed) {
      thd->send_kill_message();
      return true;
    }

    restore_record(table, s->default_values);
    /*
      Check whether default values of the fields not specified in column list
      are correct or not.
    */
    if (validate_default_values_of_unset_fields(thd, table)) {
      read_info.error = true;
      break;
    }

    Autoinc_field_has_explicit_non_null_value_reset_guard after_each_row(table);

    auto it = m_opt_fields_or_vars.begin();
    for (; it != m_opt_fields_or_vars.end(); ++it) {
      Item *item = *it;
      uint length;
      uchar *pos;
      Item *real_item;

      // Skip hidden generated columns.
      if (is_hidden_generated_column(table, item)) continue;

      if (read_info.read_field()) break;

      /* If this line is to be skipped we don't want to fill field or var */
      if (skip_lines) continue;

      pos = read_info.row_start;
      length = (uint)(read_info.row_end - pos);

      real_item = item->real_item();

      Field_tmp_nullability_guard fld_tmp_nullability_guard(real_item);

      if ((!read_info.enclosed && (enclosed_length && length == 4 &&
                                   !memcmp(pos, STRING_WITH_LEN("NULL")))) ||
          (length == 1 && read_info.found_null)) {
        if (real_item->type() == Item::FIELD_ITEM) {
          Field *field = ((Item_field *)real_item)->field;
          if (field->reset())  // Set to 0
          {
            my_error(ER_WARN_NULL_TO_NOTNULL, MYF(0), field->field_name,
                     thd->get_stmt_da()->current_row_for_condition());
            return true;
          }
          if (!field->is_nullable() && field->type() == FIELD_TYPE_TIMESTAMP) {
            // Specific of TIMESTAMP NOT NULL: set to CURRENT_TIMESTAMP.
            Item_func_now_local::store_in(field);
          } else {
            /*
              Set field to NULL. Later we will clear temporary nullability flag
              and check NOT NULL constraint.
            */
            field->set_null();
          }
        } else if (item->type() == Item::STRING_ITEM) {
          assert(nullptr != dynamic_cast<Item_user_var_as_out_param *>(item));
          ((Item_user_var_as_out_param *)item)
              ->set_null_value(read_info.read_charset);
        }

        continue;
      }

      if (real_item->type() == Item::FIELD_ITEM) {
        Field *field = ((Item_field *)real_item)->field;
        field->set_notnull();
        read_info.row_end[0] = 0;  // Safe to change end marker
        if (field == table->next_number_field)
          table->autoinc_field_has_explicit_non_null_value = true;
        field->store((char *)pos, length, read_info.read_charset);
      } else if (item->type() == Item::STRING_ITEM) {
        assert(nullptr != dynamic_cast<Item_user_var_as_out_param *>(item));
        ((Item_user_var_as_out_param *)item)
            ->set_value((char *)pos, length, read_info.read_charset);
      }
    }

    if (thd->is_error()) read_info.error = true;

    if (read_info.error) break;
    if (skip_lines) {
      skip_lines--;
      continue;
    }
    if (it != m_opt_fields_or_vars.end()) {
      /* Have not read any field, thus input file is simply ended */
      if (it == m_opt_fields_or_vars.begin()) break;

      for (; it != m_opt_fields_or_vars.end(); ++it) {
        Item *item = *it;
        Item *real_item = item->real_item();
        if (real_item->type() == Item::FIELD_ITEM) {
          Field *field = ((Item_field *)real_item)->field;
          /*
            We set to 0. But if the field is DEFAULT NULL, the "null bit"
            turned on by restore_record() above remains so field will be NULL.
          */
          if (field->reset()) {
            my_error(ER_WARN_NULL_TO_NOTNULL, MYF(0), field->field_name,
                     thd->get_stmt_da()->current_row_for_condition());
            return true;
          }
          if (field->type() == FIELD_TYPE_TIMESTAMP && !field->is_nullable())
            // Specific of TIMESTAMP NOT NULL: set to CURRENT_TIMESTAMP.
            Item_func_now_local::store_in(field);
          /*
            QQ: We probably should not throw warning for each field.
            But how about intention to always have the same number
            of warnings in THD::num_truncated_fields (and get rid of
            num_truncated_fields in the end?)
          */
          thd->num_truncated_fields++;
          push_warning_printf(thd, Sql_condition::SL_WARNING,
                              ER_WARN_TOO_FEW_RECORDS,
                              ER_THD(thd, ER_WARN_TOO_FEW_RECORDS),
                              thd->get_stmt_da()->current_row_for_condition());
        } else if (item->type() == Item::STRING_ITEM) {
          assert(nullptr != dynamic_cast<Item_user_var_as_out_param *>(item));
          ((Item_user_var_as_out_param *)item)
              ->set_null_value(read_info.read_charset);
        }
      }
    }

    if (thd->killed || fill_record_n_invoke_before_triggers(
                           thd, &info, m_opt_set_fields, m_opt_set_exprs, table,
                           TRG_EVENT_INSERT, table->s->fields, true, nullptr))
      return true;

    if (!table->triggers) {
      /*
        If there is no trigger for the table then check the NOT NULL constraint
        for every table field.

        For the table that has BEFORE-INSERT trigger installed checking for
        NOT NULL constraint is done inside function
        fill_record_n_invoke_before_triggers() after all trigger instructions
        has been executed.
      */
      for (Item *item : m_opt_fields_or_vars) {
        Item *real_item = item->real_item();
        if (real_item->type() == Item::FIELD_ITEM)
          ((Item_field *)real_item)
              ->field->check_constraints(ER_WARN_NULL_TO_NOTNULL);
      }
    }

    if (thd->is_error()) return true;

    switch (table_list->view_check_option(thd)) {
      case VIEW_CHECK_SKIP:
        read_info.next_line();
        goto continue_loop;
      case VIEW_CHECK_ERROR:
        return true;
    }

    if (invoke_table_check_constraints(thd, table)) {
      if (thd->is_error()) return true;
      // continue when IGNORE clause is used.
      read_info.next_line();
      goto continue_loop;
    }

    err = write_record(thd, table, &info, nullptr);
    if (err) return true;
    /*
      We don't need to reset auto-increment field since we are restoring
      its default value at the beginning of each loop iteration.
    */
    if (read_info.next_line())  // Skip to next line
      break;
    if (read_info.line_truncated) {
      thd->num_truncated_fields++; /* Too long row */
      push_warning_printf(thd, Sql_condition::SL_WARNING,
                          ER_WARN_TOO_MANY_RECORDS,
                          ER_THD(thd, ER_WARN_TOO_MANY_RECORDS),
                          thd->get_stmt_da()->current_row_for_condition());
      if (thd->killed) return true;
    }
    thd->get_stmt_da()->inc_current_row_for_condition();

    if (m_parallel_load) break;  // exit after insert the first row
  continue_loop:;
  }
  return read_info.error;
}

/* extra length used for avoid flush disk when wirte last record to cache */
#define FILE_CHUNK_CACHE_EXTRA_BUFFER (256 * 1024)

/* read lines in delimiter-separated formats.
     @returns true if error*/
bool Sql_cmd_load_table::read_sep_line(THD *thd, COPY_INFO &info,
                                       READ_INFO &read_info) {
  DBUG_TRACE;

  auto &jobs_mgr = Gdb_load_jobs_mgr::instance();
  auto load_job = std::make_shared<Gdb_load_job>(thd, thd->thread_id());
  /* unregister job by caller */
  if (load_job == nullptr || jobs_mgr.register_load_job(load_job)) return true;

  // build worker load statment part and set
  String load_str;
  load_str.set_charset(default_charset_info);
  load_str.reserve(1024);
  if (build_worker_sql_stmt(thd, load_str)) {
    my_error(ER_GDB_PARALLEL_LOAD, MYF(0), "build worker load statmemt failed");
    return true;
  }
  load_job->set_cmd_sql(&load_str);

  // get current db and set
  auto &cur_db = thd->db();
  load_job->set_current_db(&cur_db);

  // get master session sql_mode and set
  auto sql_mode_lex = std::make_shared<LEX_STRING>();
  if (sql_mode_string_representation(thd, thd->variables.sql_mode,
                                     sql_mode_lex.get())) {
    my_error(ER_GDB_PARALLEL_LOAD, MYF(0), "build sql_mode string failed");
    return true;
  }
  load_job->add_session_var("@@sql_mode", sql_mode_lex->str, true);

  // get master session time_zone and set
  auto time_zone_str =
      get_timezone_offset_str(get_timezone_offset_seconds(thd->time_zone()));
  load_job->add_session_var("@@time_zone", time_zone_str, true);

  // get master session auto_increment params
  load_job->add_session_var(
      "@@auto_increment_increment",
      std::to_string(thd->variables.auto_increment_increment));
  load_job->add_session_var(
      "@@auto_increment_offset",
      std::to_string(thd->variables.auto_increment_offset));
  // SET SESSION group_replication_consistency = EVENTUAL
  load_job->add_session_var("@@group_replication_consistency", "EVENTUAL",
                            true);
  // SET SESSION lock_wait_timeout = SUB_LOAD_SESSION_LOCK_TIME_OUT;
  load_job->add_session_var("@@lock_wait_timeout",
                            std::to_string(SUB_LOAD_SESSION_LOCK_TIME_OUT));
  // collect used user vars and sys vars from session
  Item_func::Collect_session_vars session_vars;
  if (collect_used_session_vars(session_vars)) {
    my_error(ER_GDB_PARALLEL_LOAD, MYF(0),
             "collect used user/sys vars failed ");
    return true;
  }
  load_job->add_collected_vars(thd, session_vars);

  size_t chunk_size = thd->variables.gdb_parallel_load_chunk_size;
  size_t buffer_size = chunk_size + FILE_CHUNK_CACHE_EXTRA_BUFFER;

  DEBUG_SYNC(thd, "gdb_parallel_load_start");

  int ret = 0;
  while (!ret) {
    auto chunk_cache = std::make_shared<IO_CACHE>();
    if (chunk_cache == nullptr ||
        init_io_cache(chunk_cache.get(), -1, buffer_size, WRITE_CACHE, 0L, true,
                      MYF(MY_WME))) {
      ret = -2;  // init io_cache fail
      my_error(ER_GDB_PARALLEL_LOAD, MYF(0), "init io_cache buffer failed");
      break;
    }

    /* continuously read line until read end of file or */
    size_t lines = 0;
    size_t total_read_len = 0;
    while (!(
        ret = read_info.read_line(chunk_cache.get(), lines, total_read_len))) {
      if (total_read_len >= chunk_size) break;
    }

    /* add a subtask to job */
    if (total_read_len > 0) {
      Gdb_load_worker_result prev_result;
      if (!load_job->can_add_subtask()) {
        /* subtask will close it's io_cache in worker thread */
        load_job->wait_finished_subtask(prev_result);

        if (prev_result.m_errno) {
          /* exit if other worker error, close current io_cache */
          close_cached_file(chunk_cache.get());
          ret = -3;
          std::string exec_err_msg = "worker execute error[";
          exec_err_msg += prev_result.m_errmsg;
          exec_err_msg += "]";
          my_error(ER_GDB_PARALLEL_LOAD, MYF(0), exec_err_msg.c_str());
          break;
        }
        /* add stats info */
        info.stats.records += prev_result.m_lines;
        info.stats.copied += prev_result.m_inserted_rows;
      }

      /* check thd killed, need wait other worker finished */
      if (thd->killed) {
        close_cached_file(chunk_cache.get());
        thd->send_kill_message();
        ret = -4;
        break;
      }

      if (load_job->add_subtask_to_worker(chunk_cache, lines)) {
        close_cached_file(chunk_cache.get());
        ret = -5;  // add subtask fail, already called my_error()
        break;
      }
    } else {
      // error or read end of file
      close_cached_file(chunk_cache.get());
    }
  }

  /* wait all worker finished */
  Gdb_load_worker_result prev_result;
  while (load_job->wait_finished_subtask(prev_result)) {
    if (prev_result.m_errno) {
      ret = -6;
      std::string exec_err_msg = "worker execute error[";
      exec_err_msg += prev_result.m_errmsg;
      exec_err_msg += "]";
      my_error(ER_GDB_PARALLEL_LOAD, MYF(0), exec_err_msg.c_str());
    }
    /* add stats info */
    info.stats.records += prev_result.m_lines;
    info.stats.copied += prev_result.m_inserted_rows;
  }

  load_job->join_workers();

  // TODO: collect warning messagess
  jobs_mgr.unregister_load_job(load_job);
  if (ret < 0) return ret;
  return false;
}

/*
  copied from log_event.cc, for parallel load
*/
static void pretty_print_str(String *packet, const char *str, size_t len) {
  packet->append('\'');

  for (size_t i = 0; i < len; i++) {
    switch (str[i]) {
      case '\n':
        packet->append("\\n");
        break;
      case '\r':
        packet->append("\\r");
        break;
      case '\\':
        packet->append("\\\\");
        break;
      case '\b':
        packet->append("\\b");
        break;
      case '\t':
        packet->append("\\t");
        break;
      case '\'':
        packet->append("\\'");
        break;
      case 0:
        packet->append("\\0");
        break;
      default:
        packet->append(str[i]);
        break;
    }
  }
  packet->append('\'');
}
static inline void pretty_print_str(String *packet, const String *str) {
  pretty_print_str(packet, str->ptr(), str->length());
}

/* copied and modified from Load_query_generator::generate().
   only contain partial statment after "infile 'xxx'" */
bool Sql_cmd_load_table::build_worker_sql_stmt(THD *thd, String &str) {
  DBUG_TRACE;
  Table_ref *const table_list = thd->lex->query_tables;
  const char *tbl = table_list->table_name;
  assert(tbl != nullptr && strlen(tbl) > 0);
  const char *db = table_list->db;  // This is never null
  assert(db != nullptr && strlen(db) > 0);
  str.length(0);

  /* not support DUP_REPLACE for parallel worker */
  if (thd->lex->is_ignore()) {
    str.append(" IGNORE");
  }

  str.append(" INTO");
  str.append(" TABLE ");
  append_ident(&str, db, strlen(db), IDENT_QUOTE_CHAR);
  str.append(".");
  append_ident(&str, tbl, strlen(tbl), IDENT_QUOTE_CHAR);
  str.append(" ");

  /* add partition list */
  auto partstr_start_len = str.length();
  str.append("PARTITION (");
  partition_info *part_info_share = nullptr;
  if (table_list->table) {
    part_info_share = table_list->table->s->m_part_info;
  } else {
    // its a view, let part_info_share = nullptr
    assert(table_list->is_view_or_derived());
  }
  if (part_info_share) {
    auto part_helper =
        dynamic_cast<Partition_helper *>(table_list->table->file);
    assert(part_helper != nullptr);
    auto part_info = part_helper->get_part_info();
    auto num_parts = part_info->num_parts;
    auto num_subparts = part_info->num_subparts;
    bool sub_parted = num_subparts ? true : false;
    uint i = 0;
    bool contain_all_parts = true;
    bool contain_part = true;
    List_iterator<partition_element> part_it(part_info->partitions);
    do {
      partition_element *part_elem = part_it++;
      auto part_name = part_elem->partition_name;
      auto curpart_start_len = str.length();
      contain_part = true;
      if (sub_parted) {
        List_iterator<partition_element> sub_part_it(part_elem->subpartitions);
        uint j = 0;
        do {
          partition_element *sub_part_elem = sub_part_it++;
          auto subpart_name = sub_part_elem->partition_name;
          auto part_id = j + (i * num_subparts);
          if (part_info->is_partition_locked(part_id)) {
            append_ident(&str, subpart_name, strlen(subpart_name),
                         IDENT_QUOTE_CHAR);
            str.append(",");
          } else {
            contain_part = false;
          }
        } while (++j < num_subparts);
      } else {
        auto part_id = i;
        if (!part_info->is_partition_locked(part_id)) {
          contain_part = false;
        }
      }
      if (!contain_part) contain_all_parts = false;

      /* back to position of string where subparts started if contains all
         subparts(also execute following code is no sub parts). */
      if (contain_part) {
        str.length(curpart_start_len);  // set position
        append_ident(&str, part_name, strlen(part_name), IDENT_QUOTE_CHAR);
        str.append(",");
      }
    } while (++i < num_parts);

    if (contain_all_parts) {
      /* rm "PARTITION (..." if contains all parts */
      str.length(partstr_start_len);
    } else {
      str.chop();  // rm last ","
      str.append(") ");
    }
  } else {
    str.length(partstr_start_len);
  }

  if (m_exchange.cs != nullptr) {
    str.append(" CHARACTER SET ");
    str.append(m_exchange.cs->csname);
  }

  str.append(" FIELDS TERMINATED BY ");
  pretty_print_str(&str, m_exchange.field.field_term);

  if (m_exchange.field.opt_enclosed) str.append(" OPTIONALLY ");
  str.append(" ENCLOSED BY ");
  pretty_print_str(&str, m_exchange.field.enclosed);

  str.append(" ESCAPED BY ");
  pretty_print_str(&str, m_exchange.field.escaped);

  str.append(" LINES TERMINATED BY ");
  pretty_print_str(&str, m_exchange.line.line_term);
  if (m_exchange.line.line_start->length() > 0) {
    str.append(" STARTING BY ");
    pretty_print_str(&str, m_exchange.line.line_start);
  }

  /* prepare fields-list */
  if (!m_opt_fields_or_vars.empty()) {
    str.append(" (");

    for (Item *item : m_opt_fields_or_vars) {
      if (item->type() == Item::FIELD_ITEM || item->type() == Item::REF_ITEM)
        append_identifier(thd, &str, item->item_name.ptr(),
                          strlen(item->item_name.ptr()));
      else
        item->print(thd, &str, QT_ORDINARY);
      str.append(", ");
    }
    // remvoe the last ", " and add '\0' at tail
    str.chop();
    str.chop();
    str.append(')');
  }

  if (!m_opt_set_fields.empty()) {
    List_iterator<String> ls(*m_opt_set_expr_strings);

    str.append(" SET ");

    for (Item *item : m_opt_set_fields) {
      String *s = ls++;

      append_identifier(thd, &str, item->item_name.ptr(),
                        strlen(item->item_name.ptr()));
      str.append(*s);
      str.append(", ");
    }
    // remvoe the last ", " and add '\0' at tail
    str.chop();
    str.chop();
  }
  return false;
}

bool Sql_cmd_load_table::collect_used_session_vars(
    Item_func::Collect_session_vars &collection) {
  DBUG_TRACE;

  // collect from field vars
  for (Item *item : m_opt_fields_or_vars) {
    if (item->type() == Item::FIELD_ITEM || item->type() == Item::REF_ITEM)
      continue;
    if (item->walk(&Item_func::collect_session_vars, enum_walk::SUBQUERY_PREFIX,
                   pointer_cast<uchar *>(&collection)))
      return true;
  }

  // collect from set expression
  for (Item *item : m_opt_set_exprs) {
    if (item->walk(&Item_func::collect_session_vars, enum_walk::SUBQUERY_PREFIX,
                   pointer_cast<uchar *>(&collection)))
      return true;
  }

  // rm item (Item_user_var_as_out_param) from user vars
  auto user_vars = collection.user_vars;
  collection.user_vars.clear();
  for (auto *var : user_vars) {
    bool skip = false;
    for (Item *item : m_opt_fields_or_vars) {
      /* class Item_user_var_as_out_param use type "STRING_ITEM" */
      if (item->type() != Item::STRING_ITEM) continue;
      auto item_outparam = pointer_cast<Item_user_var_as_out_param *>(item);
      if (var->name.eq(item_outparam->name)) {
        skip = true;
        break;
      }
    }
    if (!skip) collection.user_vars.push_back(var);
  }
  return false;
}

/**
  Read rows in xml format

  @param thd         Pointer to THD object
  @param info        Pointer to COPY_INFO object
  @param table_list  Pointer to Table_ref object
  @param read_info   Pointer to READ_INFO object
  @param skip_lines  Number of ignored lines
                     at the start of the file.

  @returns true if error
*/
bool Sql_cmd_load_table::read_xml_field(THD *thd, COPY_INFO &info,
                                        Table_ref *table_list,
                                        READ_INFO &read_info,
                                        ulong skip_lines) {
  TABLE *table = table_list->table;
  const CHARSET_INFO *cs = read_info.read_charset;
  DBUG_TRACE;

  for (;;) {
    if (thd->killed) {
      thd->send_kill_message();
      return true;
    }

    // read row tag and save values into tag list
    if (read_info.read_xml()) break;

    List_iterator_fast<XML_TAG> xmlit(read_info.taglist);
    xmlit.rewind();
    XML_TAG *tag = nullptr;

#ifndef NDEBUG
    DBUG_PRINT("read_xml_field", ("skip_lines=%d", (int)skip_lines));
    while ((tag = xmlit++)) {
      DBUG_PRINT("read_xml_field", ("got tag:%i '%s' '%s'", tag->level,
                                    tag->field.c_ptr(), tag->value.c_ptr()));
    }
#endif

    restore_record(table, s->default_values);
    /*
      Check whether default values of the fields not specified in column list
      are correct or not.
    */
    if (validate_default_values_of_unset_fields(thd, table)) {
      read_info.error = true;
      break;
    }

    Autoinc_field_has_explicit_non_null_value_reset_guard after_each_row(table);

    auto it = m_opt_fields_or_vars.begin();
    Item *item = nullptr;
    for (; it != m_opt_fields_or_vars.end(); ++it) {
      item = *it;
      /* If this line is to be skipped we don't want to fill field or var */
      if (skip_lines) continue;

      // Skip hidden generated columns.
      if (is_hidden_generated_column(table, item)) continue;

      /* find field in tag list */
      xmlit.rewind();
      tag = xmlit++;

      while (tag && strcmp(tag->field.c_ptr(), item->item_name.ptr()) != 0)
        tag = xmlit++;

      item = item->real_item();

      if (!tag)  // found null
      {
        if (item->type() == Item::FIELD_ITEM) {
          Field *field = (static_cast<Item_field *>(item))->field;
          field->reset();
          field->set_null();
          if (field == table->next_number_field)
            table->autoinc_field_has_explicit_non_null_value = true;
          if (!field->is_nullable()) {
            if (field->type() == FIELD_TYPE_TIMESTAMP)
              // Specific of TIMESTAMP NOT NULL: set to CURRENT_TIMESTAMP.
              Item_func_now_local::store_in(field);
            else if (field != table->next_number_field)
              field->set_warning(Sql_condition::SL_WARNING,
                                 ER_WARN_NULL_TO_NOTNULL, 1);
          }
        } else {
          assert(nullptr != dynamic_cast<Item_user_var_as_out_param *>(item));
          ((Item_user_var_as_out_param *)item)->set_null_value(cs);
        }
        continue;
      }

      if (item->type() == Item::FIELD_ITEM) {
        Field *field = ((Item_field *)item)->field;
        field->set_notnull();
        if (field == table->next_number_field)
          table->autoinc_field_has_explicit_non_null_value = true;
        field->store(tag->value.ptr(), tag->value.length(), cs);
      } else {
        assert(nullptr != dynamic_cast<Item_user_var_as_out_param *>(item));
        ((Item_user_var_as_out_param *)item)
            ->set_value(tag->value.ptr(), tag->value.length(), cs);
      }
    }

    if (read_info.error) break;

    if (skip_lines) {
      skip_lines--;
      continue;
    }

    if (item != nullptr) {
      /* Have not read any field, thus input file is simply ended */
      if (it == m_opt_fields_or_vars.begin()) break;

      for (; it != m_opt_fields_or_vars.end(); item = *it++) {
        if (item->type() == Item::FIELD_ITEM) {
          /*
            QQ: We probably should not throw warning for each field.
            But how about intention to always have the same number
            of warnings in THD::num_truncated_fields (and get rid of
            num_truncated_fields in the end?)
          */
          thd->num_truncated_fields++;
          push_warning_printf(thd, Sql_condition::SL_WARNING,
                              ER_WARN_TOO_FEW_RECORDS,
                              ER_THD(thd, ER_WARN_TOO_FEW_RECORDS),
                              thd->get_stmt_da()->current_row_for_condition());
        } else {
          assert(nullptr != dynamic_cast<Item_user_var_as_out_param *>(item));
          ((Item_user_var_as_out_param *)item)->set_null_value(cs);
        }
      }
    }

    if (thd->killed || fill_record_n_invoke_before_triggers(
                           thd, &info, m_opt_set_fields, m_opt_set_exprs, table,
                           TRG_EVENT_INSERT, table->s->fields, true, nullptr))
      return true;

    switch (table_list->view_check_option(thd)) {
      case VIEW_CHECK_SKIP:
        goto continue_loop;
      case VIEW_CHECK_ERROR:
        return true;
    }

    if (invoke_table_check_constraints(thd, table)) {
      if (thd->is_error()) return true;
      // continue when IGNORE clause is used.
      goto continue_loop;
    }

    if (write_record(thd, table, &info, nullptr)) return true;

    /*
      We don't need to reset auto-increment field since we are restoring
      its default value at the beginning of each loop iteration.
    */
    thd->get_stmt_da()->inc_current_row_for_condition();
  continue_loop:;
  }
  return read_info.error || thd->is_error();
} /* load xml end */

/* Unescape all escape characters, mark \N as null */

char READ_INFO::unescape(char chr) {
  /* keep this switch synchornous with the ESCAPE_CHARS macro */
  switch (chr) {
    case 'n':
      return '\n';
    case 't':
      return '\t';
    case 'r':
      return '\r';
    case 'b':
      return '\b';
    case '0':
      return 0;  // Ascii null
    case 'Z':
      return '\032';  // Win32 end of file
    case 'N':
      found_null = true;

      [[fallthrough]];
    default:
      return chr;
  }
}

/*
  Read a line using buffering
  If last line is empty (in line mode) then it isn't outputted
*/

READ_INFO::READ_INFO(File file_par, size_t tot_length, const CHARSET_INFO *cs,
                     const String &field_term, const String &line_start,
                     const String &line_term, const String &enclosed_par,
                     int escape, bool get_it_from_net, bool is_fifo,
                     IO_CACHE *chunk_cache)
    : file(file_par),
      buff_length(tot_length),
      escape_char(escape),
      found_end_of_line(false),
      eof(false),
      need_end_io_cache(false),
      error(false),
      line_truncated(false),
      found_null(false),
      read_charset(cs) {
  /*
    Field and line terminators must be interpreted as sequence of unsigned char.
    Otherwise, non-ascii terminators will be negative on some platforms,
    and positive on others (depending on the implementation of char).
  */
  field_term_ptr =
      static_cast<const uchar *>(static_cast<const void *>(field_term.ptr()));
  field_term_length = field_term.length();
  line_term_ptr =
      static_cast<const uchar *>(static_cast<const void *>(line_term.ptr()));
  line_term_length = line_term.length();

  level = 0; /* for load xml */
  if (line_start.length() == 0) {
    line_start_ptr = nullptr;
    start_of_line = false;
  } else {
    line_start_ptr = line_start.ptr();
    line_start_end = line_start_ptr + line_start.length();
    start_of_line = true;
  }
  /* If field_terminator == line_terminator, don't use line_terminator */
  if (field_term_length == line_term_length &&
      !memcmp(field_term_ptr, line_term_ptr, field_term_length)) {
    line_term_length = 0;
    line_term_ptr = nullptr;
  }
  enclosed_char = (enclosed_length = enclosed_par.length())
                      ? (uchar)enclosed_par[0]
                      : INT_MAX;
  field_term_char = field_term_length ? field_term_ptr[0] : INT_MAX;
  line_term_char = line_term_length ? line_term_ptr[0] : INT_MAX;

  /* Set of a stack for unget if long terminators */
  size_t length =
      max<size_t>(cs->mbmaxlen, max(field_term_length, line_term_length)) + 1;
  length = std::max(length, line_start.length());
  stack = stack_pos = (int *)(*THR_MALLOC)->Alloc(sizeof(int) * length);

  if (buff_length > max_size() ||
      !(buffer = (uchar *)my_malloc(key_memory_READ_INFO, buff_length + 1,
                                    MYF(MY_WME)))) {
    error = true; /* purecov: inspected */
  } else {
    end_of_buff = buffer + buff_length;
    if (chunk_cache == nullptr) {
      if (init_io_cache(
              &cache, (get_it_from_net) ? -1 : file, 0,
              (get_it_from_net) ? READ_NET : (is_fifo ? READ_FIFO : READ_CACHE),
              0L, true, MYF(MY_WME))) {
        my_free(buffer); /* purecov: inspected */
        buffer = nullptr;
        error = true;
      } else {
        /*
          init_io_cache() will not initialize read_function member
          if the cache is READ_NET. So we work around the problem with a
          manual assignment
        */
        need_end_io_cache = true;

        if (get_it_from_net) cache.read_function = _my_b_net_read;

        if (mysql_bin_log.is_open())
          cache.pre_read = cache.pre_close =
              (IO_CACHE_CALLBACK)log_loaded_block;
      }
    } else {
      /* used for parallel load worker, directly read IO_CACHE of file chunk */
      move_io_cache(&cache, chunk_cache);
      never_end_io_cache = true;
      need_end_io_cache = false;
      if (mysql_bin_log.is_open())
        cache.pre_read = cache.pre_close = (IO_CACHE_CALLBACK)log_loaded_block;
    }
  }
}

READ_INFO::~READ_INFO() {
  if (need_end_io_cache) ::end_io_cache(&cache);

  if (buffer != nullptr) my_free(buffer);
  List_iterator<XML_TAG> xmlit(taglist);
  XML_TAG *t;
  while ((t = xmlit++)) delete (t);
}

/**
  The logic here is similar with my_mbcharlen, except for GET and PUSH

  @param[in]  cs  charset info
  @param[in]  chr the first char of sequence
  @param[out] len the length of multi-byte char
*/
#define GET_MBCHARLEN(cs, chr, len)                     \
  do {                                                  \
    len = my_mbcharlen((cs), (chr));                    \
    if (len == 0 && my_mbmaxlenlen((cs)) == 2) {        \
      int chr1 = GET;                                   \
      if (chr1 != my_b_EOF) {                           \
        len = my_mbcharlen_2((cs), (chr), chr1);        \
        /* Character is gb18030 or invalid (len = 0) */ \
        assert(len == 0 || len == 2 || len == 4);       \
      }                                                 \
      if (len != 0) PUSH(chr1);                         \
    }                                                   \
  } while (0)

/**
  Skip the terminator string (if any) in the input stream.

  @param ptr    Terminator string.
  @param length Terminator string length.

  @returns false if terminator was found and skipped,
           true if terminator was not found
*/
inline bool READ_INFO::terminator(const uchar *ptr, size_t length) {
  int chr = 0;
  size_t i;
  for (i = 1; i < length; i++) {
    chr = GET;
    if (chr != *++ptr) {
      break;
    }
  }
  if (i == length) return true;
  PUSH(chr);
  while (i-- > 1) PUSH(*--ptr);
  return false;
}

/**
  @returns true if error. If READ_INFO::error is true, then error is fatal (OOM
           or charset error). Otherwise see READ_INFO::found_end_of_line for
           unexpected EOL error or READ_INFO::eof for EOF error respectively.
*/
bool READ_INFO::read_field() {
  int chr, found_enclosed_char;
  uchar *to, *new_buffer;

  found_null = false;
  if (found_end_of_line) return true;  // One have to call next_line

  /* Skip until we find 'line_start' */

  if (start_of_line) {  // Skip until line_start
    start_of_line = false;
    if (find_start_of_fields()) return true;
  }
  if ((chr = GET) == my_b_EOF) {
    found_end_of_line = eof = true;
    return true;
  }
  to = buffer;
  if (chr == enclosed_char) {
    found_enclosed_char = enclosed_char;
    *to++ = (uchar)chr;  // If error
  } else {
    found_enclosed_char = INT_MAX;
    PUSH(chr);
  }

  for (;;) {
    bool escaped_mb = false;
    while (to < end_of_buff) {
      chr = GET;
      if (chr == my_b_EOF) goto found_eof;
      if (chr == escape_char) {
        if ((chr = GET) == my_b_EOF) {
          *to++ = (uchar)escape_char;
          goto found_eof;
        }
        /*
          When escape_char == enclosed_char, we treat it like we do for
          handling quotes in SQL parsing -- you can double-up the
          escape_char to include it literally, but it doesn't do escapes
          like \n. This allows: LOAD DATA ... ENCLOSED BY '"' ESCAPED BY '"'
          with data like: "fie""ld1", "field2"
         */
        if (escape_char != enclosed_char || chr == escape_char) {
          uint ml;
          GET_MBCHARLEN(read_charset, chr, ml);
          /*
            For escaped multibyte character, push back the first byte,
            and will handle it below.
            Because multibyte character's second byte is possible to be
            0x5C, per Query_result_export::send_data, both head byte and
            tail byte are escaped for such characters. So mark it if the
            head byte is escaped and will handle it below.
          */
          if (ml == 1)
            *to++ = (uchar)unescape((char)chr);
          else {
            escaped_mb = true;
            PUSH(chr);
          }
          continue;
        }
        PUSH(chr);
        chr = escape_char;
      }
      if (chr == line_term_char && found_enclosed_char == INT_MAX) {
        if (terminator(line_term_ptr,
                       line_term_length)) {  // Maybe unexpected linefeed
          enclosed = false;
          found_end_of_line = true;
          row_start = buffer;
          row_end = to;
          return false;
        }
      }
      if (chr == found_enclosed_char) {
        if ((chr = GET) == found_enclosed_char) {  // Remove duplicated
          *to++ = (uchar)chr;
          continue;
        }
        // End of enclosed field if followed by field_term or line_term
        if (chr == my_b_EOF ||
            (chr == line_term_char &&
             terminator(line_term_ptr,
                        line_term_length))) {  // Maybe unexpected linefeed
          enclosed = true;
          found_end_of_line = true;
          row_start = buffer + 1;
          row_end = to;
          return false;
        }
        if (chr == field_term_char &&
            terminator(field_term_ptr, field_term_length)) {
          enclosed = true;
          row_start = buffer + 1;
          row_end = to;
          return false;
        }
        /*
          The string didn't terminate yet.
          Store back next character for the loop
        */
        PUSH(chr);
        /* copy the found term character to 'to' */
        chr = found_enclosed_char;
      } else if (chr == field_term_char && found_enclosed_char == INT_MAX) {
        if (terminator(field_term_ptr, field_term_length)) {
          enclosed = false;
          row_start = buffer;
          row_end = to;
          return false;
        }
      }

      uint ml;
      GET_MBCHARLEN(read_charset, chr, ml);
      if (ml == 0) {
        *to = '\0';
        my_error(ER_INVALID_CHARACTER_STRING, MYF(0), read_charset->csname,
                 buffer);
        error = true;
        return true;
      }

      if (ml > 1 && to + ml <= end_of_buff) {
        uchar *p = to;
        *to++ = chr;

        for (uint i = 1; i < ml; i++) {
          chr = GET;
          if (chr == my_b_EOF) {
            /*
             Need to back up the bytes already ready from illformed
             multi-byte char
            */
            to -= i;
            goto found_eof;
          } else if (chr == escape_char && escaped_mb) {
            // Unescape the second byte if it is escaped.
            chr = GET;
            chr = (uchar)unescape((char)chr);
          }
          *to++ = chr;
        }
        if (escaped_mb) escaped_mb = false;
        if (my_ismbchar(read_charset, (const char *)p, (const char *)to))
          continue;
        for (uint i = 0; i < ml; i++) PUSH(*--to);
        chr = GET;
      } else if (ml > 1) {
        // Buffer is too small, exit while loop, and reallocate.
        PUSH(chr);
        break;
      }
      *to++ = (uchar)chr;
    }
    // We come here if buffer is too small. Enlarge it and continue. Fail if we
    // cannot extend buffer anymore.
    const size_t new_buffer_length = check_length(buff_length, IO_SIZE);
    if ((new_buffer_length == buff_length) ||
        !(new_buffer =
              (uchar *)my_realloc(key_memory_READ_INFO, (char *)buffer,
                                  new_buffer_length + 1, MYF(MY_WME)))) {
      error = true;
      return true;
    }
    to = new_buffer + (to - buffer);
    buffer = new_buffer;
    buff_length = new_buffer_length;
    end_of_buff = buffer + buff_length;
  }

found_eof:
  enclosed = false;
  found_end_of_line = eof = true;
  row_start = buffer;
  row_end = to;
  return false;
}

/**
   Read a line including start and terminators, and write_into dst_cache.
   @returns -1 if error
             0 read a line
             1 read end of file
*/
int READ_INFO::read_line(IO_CACHE *dst_cache, size_t &lines,
                         size_t &total_read_len) {
  assert(dst_cache != nullptr);
  int chr;
  uchar *to;
  to = buffer;
  int ret = 0;

  for (;;) {
    if ((chr = GET) == my_b_EOF) {
      found_end_of_line = eof = true;
      ret = 1;
      break;
    }

    if (chr == line_term_char) {
      if (terminator(line_term_ptr, line_term_length)) {
        found_end_of_line = true;
        memcpy(to, line_term_ptr, line_term_length);
        to += line_term_length;
        /* write line terminator(also contain line data) */
        if (to > buffer && my_b_write(dst_cache, buffer, to - buffer)) {
          my_error(ER_GDB_PARALLEL_LOAD, MYF(0),
                   "write io_cache buffer failed");
          return -1;
        }
        lines++;  // got a line
        total_read_len += to - buffer;
        break;
      }
    }

    *to++ = (uchar)chr;
    assert(to <= end_of_buff - line_term_length);
    /* write to io_cache if buffer nearly full, no need extend buffer. Need
       reserve spaces for line terminator */
    if (to == end_of_buff - line_term_length) {
      if (my_b_write(dst_cache, buffer, to - buffer)) {
        my_error(ER_GDB_PARALLEL_LOAD, MYF(0), "write io_cache buffer failed");
        return -1;
      }
      total_read_len += to - buffer;
      to = buffer;
    }
  }

  return ret;
}

/**
  Read a row with fixed length.

  @note
    The row may not be fixed size on disk if there are escape
    characters in the file.

  @note
    One can't use fixed length with multi-byte charset **

  @returns true if error (unexpected end of file/line)
*/
bool READ_INFO::read_fixed_length() {
  int chr;
  uchar *to;
  if (found_end_of_line) return true;  // One have to call next_line

  if (start_of_line) {  // Skip until line_start
    start_of_line = false;
    if (find_start_of_fields()) return true;
  }

  to = row_start = buffer;
  while (to < end_of_buff) {
    if ((chr = GET) == my_b_EOF) goto found_eof;
    if (chr == escape_char) {
      if ((chr = GET) == my_b_EOF) {
        *to++ = (uchar)escape_char;
        goto found_eof;
      }
      *to++ = (uchar)unescape((char)chr);
      continue;
    }
    if (chr == line_term_char) {
      if (terminator(line_term_ptr,
                     line_term_length)) {  // Maybe unexpected linefeed
        found_end_of_line = true;
        row_end = to;
        return false;
      }
    }
    *to++ = (uchar)chr;
  }
  row_end = to;  // Found full line
  return false;

found_eof:
  found_end_of_line = eof = true;
  row_start = buffer;
  row_end = to;
  return to == buffer;
}

/**
  @returns true if error (unexpected end of file/line)
*/
bool READ_INFO::next_line() {
  line_truncated = false;
  start_of_line = line_start_ptr != nullptr;
  if (found_end_of_line || eof) {
    found_end_of_line = false;
    return eof;
  }
  found_end_of_line = false;
  if (!line_term_length) return false;  // No lines
  for (;;) {
    int chr = GET;
    uint ml;
    if (chr == my_b_EOF) {
      eof = true;
      return true;
    }
    GET_MBCHARLEN(read_charset, chr, ml);
    if (ml > 1) {
      for (uint i = 1; chr != my_b_EOF && i < ml; i++) chr = GET;
      if (chr == escape_char) continue;
    }
    if (chr == my_b_EOF) {
      eof = true;
      return true;
    }
    if (chr == escape_char) {
      line_truncated = true;
      if (GET == my_b_EOF) return true;
      continue;
    }
    if (chr == line_term_char && terminator(line_term_ptr, line_term_length))
      return false;
    line_truncated = true;
  }
}

/**
  @returns true if error (unexpected end of file/line)
*/
bool READ_INFO::find_start_of_fields() {
  int chr;
try_again:
  do {
    if ((chr = GET) == my_b_EOF) {
      found_end_of_line = eof = true;
      return true;
    }
  } while ((char)chr != line_start_ptr[0]);
  for (const char *ptr = line_start_ptr + 1; ptr != line_start_end; ptr++) {
    chr = GET;                // Eof will be checked later
    if ((char)chr != *ptr) {  // Can't be line_start
      PUSH(chr);
      while (--ptr != line_start_ptr) {  // Restart with next char
        PUSH(*ptr);
      }
      goto try_again;
    }
  }
  return false;
}

/*
  Clear taglist from tags with a specified level
*/
void READ_INFO::clear_level(int level_arg) {
  DBUG_TRACE;
  List_iterator<XML_TAG> xmlit(taglist);
  xmlit.rewind();
  XML_TAG *tag;

  while ((tag = xmlit++)) {
    if (tag->level >= level_arg) {
      xmlit.remove();
      delete tag;
    }
  }
}

/*
  Convert an XML entity to Unicode value.
  Return -1 on error;
*/
static int my_xml_entity_to_char(const char *name, size_t length) {
  if (length == 2) {
    if (!memcmp(name, "gt", length)) return '>';
    if (!memcmp(name, "lt", length)) return '<';
  } else if (length == 3) {
    if (!memcmp(name, "amp", length)) return '&';
  } else if (length == 4) {
    if (!memcmp(name, "quot", length)) return '"';
    if (!memcmp(name, "apos", length)) return '\'';
  }
  return -1;
}

/**
  @brief Convert newline, linefeed, tab to space

  @param chr    character

  @details According to the "XML 1.0" standard,
           only space (@#x20) characters, carriage returns,
           line feeds or tabs are considered as spaces.
           Convert all of them to space (@#x20) for parsing simplicity.
*/
static int my_tospace(int chr) {
  return (chr == '\t' || chr == '\r' || chr == '\n') ? ' ' : chr;
}

/*
  Read an xml value: handle multibyte and xml escape

  @param      delim  Delimiter character.
  @param[out] val    Resulting value string.

  @returns next character after delim
           or
           my_b_EOF in case of charset error/unexpected EOF.
*/
int READ_INFO::read_value(int delim, String *val) {
  int chr;
  String tmp;

  for (chr = GET; my_tospace(chr) != delim && chr != my_b_EOF;) {
    uint ml;
    GET_MBCHARLEN(read_charset, chr, ml);
    if (ml == 0) {
      chr = my_b_EOF;
      val->length(0);
      return chr;
    }

    if (ml > 1) {
      DBUG_PRINT("read_xml", ("multi byte"));

      for (uint i = 1; i < ml; i++) {
        val->append(chr);
        /*
          Don't use my_tospace() in the middle of a multi-byte character
          TODO: check that the multi-byte sequence is valid.
        */
        chr = GET;
        if (chr == my_b_EOF) return chr;
      }
    }
    if (chr == '&') {
      tmp.length(0);
      for (chr = my_tospace(GET); chr != ';'; chr = my_tospace(GET)) {
        if (chr == my_b_EOF) return chr;
        tmp.append(chr);
      }
      if ((chr = my_xml_entity_to_char(tmp.ptr(), tmp.length())) >= 0)
        val->append(chr);
      else {
        val->append('&');
        val->append(tmp);
        val->append(';');
      }
    } else
      val->append(chr);
    chr = GET;
  }
  return my_tospace(chr);
}

/*
  Read CDATA value if any.
  Ignore multibyte and XML escape.
  Note: the last character read must be '<' before calling this function.

  @param[out] val           Resulting CDATA string.
  @param[out] have_cdata    Set if really read CDATA.

  @returns    Last character read or
              my_b_EOF in case of unexpected EOF.
*/
int READ_INFO::read_cdata(String *val, bool *have_cdata) {
  const char cdata_head[] = "![CDATA[";
  const char *head_ptr = cdata_head;

  /* Check for CDATA head "![CDATA[" */
  for (size_t i = 0; i < strlen(cdata_head); i++) {
    int chr = GET;

    if (chr != *head_ptr++) {
      /*
        Didn't find "![CDATA[" head,
        push back the last (unmatched) character
      */
      PUSH(chr);
      /* and all matched from the head. */
      while (i--) PUSH(*--head_ptr);

      *have_cdata = false;
      return '<';
    }
  }

  int tail[3]{0};
  for (tail[2] = GET; tail[2] != my_b_EOF; tail[2] = GET) {
    /* Check for CDATA tail "]]>" */
    if (tail[0] == ']' && tail[1] == ']' && tail[2] == '>') {
      /* Cut last two characters ("]]") which were appended to val. */
      assert(val->length() >= 2);
      val->length(val->length() - 2);

      *have_cdata = true;
      return '>';
    }
    /* Shift the tail */
    tail[0] = tail[1];
    tail[1] = tail[2];

    val->append(tail[2]);
  }

  /* Didn't find CDATA tail "]]>", the last character read must be my_b_EOF. */
  assert(tail[2] == my_b_EOF);
  *have_cdata = false;
  return my_b_EOF;
}

/*
  Read a record in xml format
  tags and attributes are stored in taglist
  when tag set in ROWS IDENTIFIED BY is closed, we are ready and return

  @returns true if error (unexpected end of file)
*/
bool READ_INFO::read_xml() {
  DBUG_TRACE;
  int chr, chr2, chr3;
  int delim = 0;
  String tag, attribute, value;
  bool in_tag = false;

  tag.length(0);
  attribute.length(0);
  value.length(0);

  for (chr = my_tospace(GET); chr != my_b_EOF;) {
    switch (chr) {
      case '<': /* read tag */
        /* TODO: check if this is a comment <!-- comment -->  */
        chr = my_tospace(GET);
        if (chr == '!') {
          chr2 = GET;
          chr3 = GET;

          if (chr2 == '-' && chr3 == '-') {
            chr2 = 0;
            chr3 = 0;
            chr = my_tospace(GET);

            while (chr != '>' || chr2 != '-' || chr3 != '-') {
              if (chr == '-') {
                chr3 = chr2;
                chr2 = chr;
              } else if (chr2 == '-') {
                chr2 = 0;
                chr3 = 0;
              }
              chr = my_tospace(GET);
              if (chr == my_b_EOF) goto found_eof;
            }
            break;
          }
        }

        tag.length(0);
        while (chr != '>' && chr != ' ' && chr != '/' && chr != my_b_EOF) {
          if (chr != delim) /* fix for the '<field name =' format */
            tag.append(chr);
          chr = my_tospace(GET);
        }

        // row tag should be in ROWS IDENTIFIED BY '<row>' - stored in line_term
        if ((tag.length() == line_term_length - 2) &&
            (memcmp(tag.ptr(), line_term_ptr + 1, tag.length()) == 0)) {
          DBUG_PRINT("read_xml", ("start-of-row: %i %s %s", level,
                                  tag.c_ptr_safe(), line_term_ptr));
        }

        if (chr == ' ' || chr == '>') {
          level++;
          clear_level(level + 1);
        }

        if (chr == ' ')
          in_tag = true;
        else
          in_tag = false;
        break;

      case ' ':            /* read attribute */
        while (chr == ' ') /* skip blanks */
          chr = my_tospace(GET);

        if (!in_tag) break;

        while (chr != '=' && chr != '/' && chr != '>' && chr != my_b_EOF) {
          attribute.append(chr);
          chr = my_tospace(GET);
        }
        break;

      case '>': /* end tag - read tag value */
        in_tag = false;
        /* Skip all whitespaces */
        while (' ' == (chr = my_tospace(GET))) {
        }
        /*
          Push the first non-whitespace char back to Stack. This char would be
          read in the upcoming call to read_value()
         */
        PUSH(chr);

        /* Read <![CDATA[ ... ]]> and tag's value. */
        bool have_cdata;
        do {
          chr = read_value('<', &value);
          if (chr == my_b_EOF) goto found_eof;

          chr = read_cdata(&value, &have_cdata);
          if (chr == my_b_EOF) goto found_eof;
        } while (have_cdata);

        /* save value to list */
        if (tag.length() > 0 && value.length() > 0) {
          DBUG_PRINT("read_xml", ("lev:%i tag:%s val:%s", level,
                                  tag.c_ptr_safe(), value.c_ptr_safe()));
          taglist.push_front(new XML_TAG(level, tag, value));
        }
        tag.length(0);
        value.length(0);
        attribute.length(0);
        break;

      case '/': /* close tag */
        chr = my_tospace(GET);
        /* Decrease the 'level' only when (i) It's not an */
        /* (without space) empty tag i.e. <tag/> or, (ii) */
        /* It is of format <row col="val" .../>           */
        if (chr != '>' || in_tag) {
          level--;
          in_tag = false;
        }
        if (chr != '>')  /* if this is an empty tag <tag   /> */
          tag.length(0); /* we should keep tag value          */
        while (chr != '>' && chr != my_b_EOF) {
          tag.append(chr);
          chr = my_tospace(GET);
        }

        if ((tag.length() == line_term_length - 2) &&
            (memcmp(tag.ptr(), line_term_ptr + 1, tag.length()) == 0)) {
          DBUG_PRINT("read_xml",
                     ("found end-of-row %i %s", level, tag.c_ptr_safe()));
          return false;  // normal return
        }
        chr = my_tospace(GET);
        break;

      case '=': /* attribute name end - read the value */
        // check for tag field and attribute name
        if (!memcmp(tag.c_ptr_safe(), STRING_WITH_LEN("field")) &&
            !memcmp(attribute.c_ptr_safe(), STRING_WITH_LEN("name"))) {
          /*
            this is format <field name="xx">xx</field>
            where actual fieldname is in attribute
          */
          delim = my_tospace(GET);
          tag.length(0);
          attribute.length(0);
          chr = '<'; /* we pretend that it is a tag */
          level--;
          break;
        }

        // check for " or '
        chr = GET;
        if (chr == my_b_EOF) goto found_eof;
        if (chr == '"' || chr == '\'') {
          delim = chr;
        } else {
          delim = ' '; /* no delimiter, use space */
          PUSH(chr);
        }

        chr = read_value(delim, &value);
        if (attribute.length() > 0 && value.length() > 0) {
          DBUG_PRINT("read_xml", ("lev:%i att:%s val:%s\n", level + 1,
                                  attribute.c_ptr_safe(), value.c_ptr_safe()));
          taglist.push_front(new XML_TAG(level + 1, attribute, value));
        }
        attribute.length(0);
        value.length(0);
        if (chr != ' ') chr = my_tospace(GET);
        break;

      default:
        chr = my_tospace(GET);
    } /* end switch */
  }   /* end while */

found_eof:
  DBUG_PRINT("read_xml", ("Found eof"));
  eof = true;
  return true;
}

/* called by master session */
int Gdb_load_worker::start() {
  DBUG_TRACE;

  /* create worker thread */
  mysql_mutex_lock(&m_worker_mutex);
  PSI_thread_key background_psi_thread_key = 0;  // useless
  int err = mysql_thread_create(background_psi_thread_key, &m_handle, nullptr,
                                thread_func, this);
  if (err) {
    m_cur_result.m_errmsg = "[spawn worker] create worker thread failed";
    mysql_mutex_unlock(&m_worker_mutex);
    return -2;
  }

  /* wait worker thread init OK  */
  do {
    struct timespec abstime;
    set_timespec(&abstime, 2);
    mysql_cond_timedwait(&m_worker_cond, &m_worker_mutex, &abstime);
    if (m_inited) break;
  } while (!m_exit);

  mysql_mutex_unlock(&m_worker_mutex);
  return !m_inited;  // init worker fail
}

int Gdb_load_worker::setup_session_env(Gdb_cmd_service &cmd_service) {
  /* set LEX_CSTRING db */
  auto &db = m_master_job->m_db;
  cmd_service.set_db(db->str, db->length);

  /* set cmd_service session as parallel load executor */
  cmd_service.set_parallel_load_executor();

  for (auto &itr : m_master_job->m_session_vars) {
    std::string sql = "SET /*parallel load worker(init_session)*/ ";
    sql += itr.first;
    sql += " = ";
    sql += itr.second;
    if (cmd_service.execute_sql(sql)) {
      auto &cb_data = cmd_service.get_cb_data();
      m_cur_result.m_errno = cb_data.error_no();
      m_cur_result.m_errmsg = cb_data.error_msg();
      return -1;
    }
  }
  return 0;
}

void *Gdb_load_worker::thread_func(void *worker_handler) {
  DBUG_TRACE;
  assert(worker_handler != nullptr);
  auto handler = static_cast<Gdb_load_worker *>(worker_handler);
  auto *master_job = handler->m_master_job;
  auto &master_mutex = master_job->m_master_mutex;
  auto &master_cond = master_job->m_master_cond;
  auto &worker_mutex = handler->m_worker_mutex;
  auto &worker_cond = handler->m_worker_cond;
  auto &list_mutex = master_job->m_list_mutex;

  my_thread_init();
  THD *thd;
  if (!(thd = new (std::nothrow) THD)) {
    handler->m_cur_result.m_errno = -1;
    handler->m_cur_result.m_errmsg = "malloc THD failed";
    my_thread_end();
    return nullptr;
  }
  thd->thread_stack = (char *)&thd;
  thd->store_globals();

  do {
    /* cmd service should de-construct before call mysql_thread_end() */
    mysql_mutex_lock(&worker_mutex);
    Gdb_cmd_service cmd_service;
    bool init_ok = true;
    if (cmd_service.open_session()) {
      handler->m_cur_result.m_errno = -1;
      handler->m_cur_result.m_errmsg = "open cmd_service session failed";
      init_ok = false;
    }

    if (init_ok && handler->setup_session_env(cmd_service)) {
      init_ok = false;
    }

    handler->m_inited = init_ok;
    mysql_cond_signal(&worker_cond);  // signal: worker thread inited OK
    mysql_mutex_unlock(&worker_mutex);

    // exit if inti fail
    if (!init_ok) break;

    /* Now, worker is ready. wait feed chunk and do load data */
    do {
      mysql_mutex_lock(&worker_mutex);
      if (handler->m_do_abort) {
        mysql_mutex_unlock(&worker_mutex);
        break;
      }
      if (handler->m_cur_stop) {
        struct timespec abstime;
        set_timespec_nsec(&abstime, 200 * 1000000);  // timeout_wait: 0.2s
        mysql_cond_timedwait(&worker_cond, &worker_mutex, &abstime);
        mysql_mutex_unlock(&worker_mutex);
        continue;
      }
      mysql_mutex_unlock(&worker_mutex);

      /* build worker cmd sql and do load data chunk. */
      std::string cmd_sql = "LOAD /*parallel load worker(chunk_no:";
      cmd_sql += std::to_string(handler->m_cur_chunk_no);
      cmd_sql += ")*/ DATA INFILE '";
      cmd_sql += std::to_string(master_job->get_session_id());
      cmd_sql += ":";
      cmd_sql += std::to_string(handler->m_worker_id);
      cmd_sql += "' ";
      cmd_sql += master_job->m_cmd_sql->ptr();
      int ret = cmd_service.execute_sql(cmd_sql.c_str(), cmd_sql.size());

      // close io_cache of data chunk in worker thread is add_subtask OK
      close_cached_file(handler->m_cur_chunk.get());

      /* collect load chunk result */
      auto &cb_data = cmd_service.get_cb_data();
      mysql_mutex_lock(&worker_mutex);
      if (ret || cb_data.error_no()) {
        handler->m_cur_result.m_errno = cb_data.error_no();
        handler->m_cur_result.m_errmsg = cb_data.error_msg();
        /* need write error log here? */
        std::string log_error_sql = "[parallel load worker failed] sql is:";
        log_error_sql += cmd_sql;
        log_error_sql += " [err_msg]:";
        log_error_sql += handler->m_cur_result.m_errmsg;
        LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG, log_error_sql.c_str());
        handler->m_exit = true;  // exit worker if failed
      } else {
        handler->m_cur_result.m_inserted_rows = cb_data.affected_rows();
        assert(handler->m_cur_result.m_lines > 0);
        assert(handler->m_cur_result.m_lines >=
               handler->m_cur_result.m_inserted_rows);
        handler->m_cur_result.m_skipped_rows =
            handler->m_cur_result.m_lines -
            handler->m_cur_result.m_inserted_rows;
      }
      handler->m_cur_stop = true;  // current chunk finish

      list_mutex.lock();
      master_job->m_finished_workers.push_back(handler);
      list_mutex.unlock();

      mysql_mutex_unlock(&worker_mutex);

      /* signal master session */
      mysql_mutex_lock(&master_mutex);
      if (master_job->m_master_is_waiting) mysql_cond_signal(&master_cond);
      mysql_mutex_unlock(&master_mutex);
    } while (!handler->m_exit);
  } while (0);

  handler->m_exit = true;
  mysql_mutex_lock(&worker_mutex);
  mysql_cond_signal(&worker_cond);  // signal: worker thread end
  mysql_mutex_unlock(&worker_mutex);
  delete thd;
  my_thread_end();
  return nullptr;
}

int Gdb_load_worker::add_subtask(std::shared_ptr<IO_CACHE> chunk_cache,
                                 uint chunk_no, size_t lines) {
  DBUG_TRACE;
  mysql_mutex_lock(&m_worker_mutex);
  assert(m_inited);
  assert(!m_exit);
  assert(m_cur_stop);
  if (!m_inited || m_exit || !m_cur_stop) {
    m_master_job->m_list_mutex.lock();
    m_master_job->m_idle_workers.push_back(this);  // add to idle list if failed
    m_master_job->m_list_mutex.unlock();
    my_error(ER_GDB_PARALLEL_LOAD, MYF(0), "[add subtask] worker status error");
    return -1;
  }

  m_cur_stop = false;
  m_cur_chunk = chunk_cache;
  m_cur_chunk_no = chunk_no;
  m_cur_result.reset();
  m_cur_result.m_lines = lines;

  mysql_cond_signal(&m_worker_cond);
  mysql_mutex_unlock(&m_worker_mutex);
  return false;
}

std::shared_ptr<Gdb_load_worker> Gdb_load_job::get_handler_by_worker_id(
    uint worker_id) {
  std::shared_ptr<Gdb_load_worker> worker(nullptr);
  assert(worker_id < m_max_workers);
  std::lock_guard<std::mutex> lock(m_list_mutex);
  worker = m_workers[worker_id];
  return worker;
}

/* create a worker thread and attached session, and set vars */
int Gdb_load_job::spawn_worker() {
  DBUG_TRACE;
  assert(m_workers.size() < m_max_workers);
  uint worker_id = m_workers.size();  // gnerate worker_id
  auto worker = std::make_shared<Gdb_load_worker>(this, worker_id);
  if (worker == nullptr) {
    my_error(ER_GDB_PARALLEL_LOAD, MYF(0), "[spawn worker] malloc fail");
    return -1;
  }

  if (worker->start()) {
    my_error(ER_GDB_PARALLEL_LOAD, MYF(0),
             worker->m_cur_result.m_errmsg.c_str());
    return -2;
  }

  std::lock_guard<std::mutex> lock(m_list_mutex);
  m_workers.push_back(worker);
  m_idle_workers.push_back(worker.get());
  return 0;
}

/* add worker and start, called by master session */
int Gdb_load_job::add_subtask_to_worker(std::shared_ptr<IO_CACHE> chunk_cache,
                                        size_t lines) {
  DBUG_TRACE;
  if (reinit_io_cache(chunk_cache.get(), READ_CACHE, 0, false, false)) {
    return -1;
  }
  uint chunk_no = m_chunk_no_idx++;

  if (m_workers.size() < m_max_workers) {
    if (spawn_worker()) return -1;
  }

  m_list_mutex.lock();
  assert(m_idle_workers.size() > 0);
  auto worker = m_idle_workers.front();
  m_idle_workers.pop_front();  // rm from idle list
  m_list_mutex.unlock();
  /* if add_subtask() fail, will give back woker to idle list */
  return worker->add_subtask(chunk_cache, chunk_no, lines);
}

/* return value:
     >0, a worker finished
     0, no running workers any more */
int Gdb_load_job::wait_finished_subtask(Gdb_load_worker_result &result) {
  DBUG_TRACE;

  mysql_mutex_lock(&m_master_mutex);
  do {
    m_list_mutex.lock();

    /* all workers in idle */
    if (m_workers.size() == m_idle_workers.size()) {
      mysql_mutex_unlock(&m_master_mutex);
      m_list_mutex.unlock();
      return 0;  // parallel load complete
    }

    if (m_finished_workers.size() > 0) {
      auto worker = m_finished_workers.front();
      result = worker->m_cur_result;
      m_idle_workers.push_back(worker);
      m_finished_workers.pop_front();
      m_list_mutex.unlock();
      m_master_is_waiting = false;
      mysql_mutex_unlock(&m_master_mutex);
      return 1;
    }
    m_list_mutex.unlock();

    /* loop wait 2s util a worker finish */
    m_master_is_waiting = true;
    struct timespec abstime;
    set_timespec(&abstime, 2);
    mysql_cond_timedwait(&m_master_cond, &m_master_mutex, &abstime);
    m_master_is_waiting = false;
  } while (1);

  assert(0);
  return -1;
}

void Gdb_load_job::join_workers() {
  DBUG_TRACE;
  assert(m_workers.size() == m_idle_workers.size());
  assert(m_finished_workers.size() == 0);
  m_idle_workers.clear();
  m_finished_workers.clear();
  for (auto worker : m_workers) {
    mysql_mutex_lock(&worker->m_worker_mutex);
    worker->m_do_abort = true;
    mysql_cond_signal(&worker->m_worker_cond);
    mysql_mutex_unlock(&worker->m_worker_mutex);
  }
  for (auto worker : m_workers) {
    my_thread_join(&worker->m_handle, nullptr);
  }
  m_workers.clear();
}

int Gdb_load_job::add_collected_vars(
    THD *thd, Item_func::Collect_session_vars &session_vars) {
  DBUG_TRACE;
  char buffer[256 + 1];
  String str(buffer, sizeof(buffer), system_charset_info);

  // set user vars (Item_func_get_user_var*)
  auto &user_vars = session_vars.user_vars;
  for (auto *var_item : user_vars) {
    str.length(0);
    append_identifier(thd, &str, var_item->name.ptr(), var_item->name.length());
    std::string var_name = "@";
    var_name += std::string(str.ptr(), str.length());

    str.length(0);
    auto value_str = var_item->val_str(&str);
    auto type = var_item->result_type();
    assert(type != ROW_RESULT);
    assert(type != INVALID_RESULT);
    std::string var_value(value_str->ptr(), value_str->length());
    if (type == STRING_RESULT)
      var_value = gdb_string_add_quote(var_value, VALUE_QUOTE_CHAR);
    add_session_var(var_name, var_value);
  }

  // set sys vars (Item_func_get_system_var *)
  auto &sys_vars = session_vars.sys_vars;
  for (auto *var_item : sys_vars) {
    str.length(0);
    var_item->print_var_name(thd, &str, QT_ORDINARY /*useless*/);
    std::string var_name = "@@";
    var_name += std::string(str.ptr(), str.length());

    str.length(0);
    auto value_str = var_item->val_str(&str);
    auto type = var_item->result_type();
    std::string var_value(value_str->ptr(), value_str->length());
    if (type == STRING_RESULT)
      var_value = gdb_string_add_quote(var_value, VALUE_QUOTE_CHAR);
    add_session_var(var_name, var_value);
  }

  return 0;
}

bool Sql_cmd_load_table::execute(THD *thd) {
  LEX *const lex = thd->lex;

  uint privilege =
      (lex->duplicates == DUP_REPLACE ? INSERT_ACL | DELETE_ACL : INSERT_ACL) |
      (m_is_local_file ? 0 : FILE_ACL);

  if (m_is_local_file) {
    if (!thd->get_protocol()->has_client_capability(CLIENT_LOCAL_FILES) ||
        !opt_local_infile) {
      my_error(ER_CLIENT_LOCAL_FILES_DISABLED, MYF(0));
      return true;
    }
  }

  if (check_one_table_access(thd, privilege, lex->query_tables)) return true;

  /* Push strict / ignore error handler */
  Ignore_error_handler ignore_handler;
  Strict_error_handler strict_handler;
  if (thd->lex->is_ignore())
    thd->push_internal_handler(&ignore_handler);
  else if (thd->is_strict_mode())
    thd->push_internal_handler(&strict_handler);

  lex->using_hypergraph_optimizer =
      thd->optimizer_switch_flag(OPTIMIZER_SWITCH_HYPERGRAPH_OPTIMIZER);

  bool res = execute_inner(thd, lex->duplicates);

  /* Pop ignore / strict error handler */
  if (thd->lex->is_ignore() || thd->is_strict_mode())
    thd->pop_internal_handler();

  return res;
}
