/* Copyright (c) 2023, 2024, GreatDB Software Co., Ltd. All rights
   reserved.

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

#include "sql/gdb_sequence.h"

#include <stdlib.h>
#include <string.h>
#include <atomic>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include "m_ctype.h"
#include "m_string.h"
#include "map_helpers.h"
#include "my_alloc.h"
#include "my_base.h"
#include "my_dbug.h"
#include "my_inttypes.h"
#include "my_loglevel.h"
#include "my_macros.h"
#include "my_psi_config.h"
#include "my_sys.h"
#include "mysql/components/services/bits/mysql_rwlock_bits.h"
#include "mysql/components/services/bits/psi_rwlock_bits.h"
#include "mysql/components/services/log_builtins.h"
#include "mysql/psi/mysql_mutex.h"
#include "mysql/psi/mysql_rwlock.h"
#include "mysqld_error.h"
#include "sql/auth/auth_acls.h"
#include "sql/auth/auth_common.h"
#include "sql/dd/dd_table.h"
#include "sql/dd/types/function.h"
#include "sql/field.h"
#include "sql/gdb_common.h"
#include "sql/handler.h"
#include "sql/iterators/row_iterator.h"
#include "sql/server_component/gdb_cmd_service.h"
#include "sql/sql_backup_lock.h"  // acquire_shared_backup_lock
#include "sql/sql_base.h"         // close_mysql_tables
#include "sql/sql_class.h"
#include "sql/sql_const.h"
#include "sql/sql_error.h"
#include "sql/sql_executor.h"            // for init_table_iter..
#include "sql/sql_system_table_check.h"  // System_table_intact
#include "sql/sql_table.h"
#include "sql/system_variables.h"
#include "sql/table.h"
#include "sql/thd_raii.h"
#include "sql/thr_malloc.h"
#include "sql/transaction.h"  // trans_rollback_stmt, trans_commit_stmt
#include "thr_lock.h"

// global cache for in-memory objects of sequence
static sequences_cache_map *sequences_cache = nullptr;

#ifdef HAVE_PSI_INTERFACE
static mysql_rwlock_t THR_LOCK_sequences;
static PSI_rwlock_key key_rwlock_THR_LOCK_sequences;

static PSI_rwlock_info all_sequences_cache_rwlocks[] = {
    {&key_rwlock_THR_LOCK_sequences, "THR_LOCK_sequences", PSI_FLAG_SINGLETON,
     0, PSI_DOCUMENT_ME}};

static my_decimal calc_next_value_for_show(const my_decimal &currval,
                                           const Sequence_option &option);

static void init_sequences_cache_psi_keys(void) {
  const char *category = "sql";
  int count;

  count = static_cast<int>(array_elements(all_sequences_cache_rwlocks));
  mysql_rwlock_register(category, all_sequences_cache_rwlocks, count);
}
#endif /* HAVE_PSI_INTERFACE */

/* Metadata version of sequences only exists in memory. It was used for
   triggering repreparation on prepared statment or statement in stored
   procedure when related sequence is modified/dropped.

   When loadding sequence at startup, each sequence will get a version number
   and then "sequences_metadata_version++". After that, each DDL operation on
   sequence will update its version number and "sequences_metadata_version++"
*/
static std::atomic<uint64> sequences_metadata_version{1};
static inline uint64_t get_and_inc_version() {
  return sequences_metadata_version.fetch_add(1);
}

#define GREATDB_SEQUENCES_TABLE "greatdb_sequences"
#define GREATDB_SEQUENCES_PERSIST_TABLE "greatdb_sequences_persist"
#define GREATDB_SEQUENCES_TABLE_KEY_PRIMARY 0
#define GREATDB_SEQUENCE_MAX_KEY_LEN 512 + 4  // 2col*64*4(utf8mb4) + paddings

/**
   This enum describes the structure of the mysql.greatdb_sequences table.
*/
enum enum_sequences_table_field {
  SEQUENCE_FIELD_DB = 0,
  SEQUENCE_FIELD_NAME,
  SEQUENCE_FIELD_START_WITH,
  SEQUENCE_FIELD_MINVALUE,
  SEQUENCE_FIELD_MAXVALUE,
  SEQUENCE_FIELD_INCREMENT,
  SEQUENCE_FIELD_CYCLE_FLAG,
  SEQUENCE_FIELD_CACHE_NUM,
  SEQUENCE_FIELD_ORDER_FALG,
};

static bool get_sequence_from_table_to_cache(TABLE *table);

std::string normalize_seq_name(const char *seq_name) {
  if (lower_case_table_names) {
    char lowercase_name[NAME_LEN + 1];
    strcpy(lowercase_name, seq_name);
    my_casedn_str(system_charset_info, lowercase_name);
    return lowercase_name;
  } else {
    return seq_name;
  }
}

/* calc sequence's key in cache
   NOTE: need call normalize_seq_name() before it */
std::string calc_seq_key(const char *db_name, const char *seq_name) {
  assert(db_name != nullptr);
  assert(strlen(db_name) > 0);
  assert(seq_name != nullptr);
  assert(strlen(seq_name) > 0);
  return std::string(db_name) + "." + seq_name;
}

static inline std::string check_lowercase_dbname(const char *db_name) {
  if (lower_case_table_names) {
    char lowercase_dbname[NAME_LEN + 1];
    strcpy(lowercase_dbname, db_name);
    my_casedn_str(system_charset_info, lowercase_dbname);
    return lowercase_dbname;
  } else {
    return db_name;
  }
}

/* handler class for operate persist table of sequences
   TODO: implement post_ddl to ensure atomic */
class Gdb_seq_persist_handler {
 public:
  Gdb_seq_persist_handler(Gdb_sequence_entity *seq) : m_seq(seq) {}
  virtual ~Gdb_seq_persist_handler() {}
  virtual bool create_persist_record(const my_decimal &cur) = 0;
  virtual bool delete_persist_record() = 0;
  virtual bool delete_persist_record_without_commit() = 0;
  virtual bool read_persist_record(bool lock, my_decimal &val) = 0;
  virtual bool update_persist_record(const my_decimal &currval) = 0;
  virtual bool release_rec_lock(bool must_commit) = 0;
  inline void reset_errno_errmsg() {
    m_errno = 0;
    m_errmsg.clear();
  }
  virtual bool close() = 0;

  Gdb_sequence_entity *m_seq;
  int m_errno;
  std::string m_errmsg;
};

/* TODO: banch "8.0.22-CDC" has moved gdb_cmd_service.h from engine-level to
     sql layer. To avoid conflicts in future rebasing, just copy some code of
     gdb_cmd_service.h. Refactor those code after 8.0.22-CDC merged
 */
class Gdb_seq_persist_front_end : public Gdb_seq_persist_handler {
 public:
  Gdb_seq_persist_front_end(Gdb_sequence_entity *seq)
      : Gdb_seq_persist_handler(seq) {}
  ~Gdb_seq_persist_front_end() override;
  bool create_persist_record(const my_decimal &cur) override;
  bool delete_persist_record() override;
  bool delete_persist_record_without_commit() override {
    assert(0);
    return true;
  }
  bool read_persist_record(bool lock, my_decimal &val) override;
  bool update_persist_record(const my_decimal &currval) override;
  bool release_rec_lock(bool must_commit) override;
  bool close() override;

 private:
  Gdb_cmd_service m_cmd_service;
};

std::atomic<uint64> Gdb_sequence_entity::volatile_id_generator;

Gdb_sequence_entity::~Gdb_sequence_entity() {
  if (m_persist_handler) delete m_persist_handler;
}

bool Gdb_sequence_entity::setup_persist_handler() {
  DBUG_TRACE;
  assert(m_persist_handler == nullptr);
  m_persist_handler = new Gdb_seq_persist_front_end(this);
  return !m_persist_handler;
}

int Gdb_sequence_entity::get_persist_handler_errno() {
  assert(m_persist_handler != nullptr);
  return m_persist_handler->m_errno;
}

bool Gdb_sequence_entity::open_and_init() {
  DBUG_TRACE;

  /* if not ready, after acquire mutex, recheck m_status */
  if (unlikely(m_status.load() == SEQ_STATUS_CLOSED)) {
    std::lock_guard<std::mutex> lock(m_mutex);
    if (m_status.load() == SEQ_STATUS_CLOSED && open_sequence()) return true;
  }

  /* if unreadable, after acquire mutex, recheck m_status */
  if (m_status.load() == SEQ_STATUS_UNREADABLE) {
    std::lock_guard<std::mutex> lock(m_mutex);
    if (m_status.load() == SEQ_STATUS_UNREADABLE && init_sequence())
      return true;
  }
  return false;
}

bool Gdb_sequence_entity::refresh_start_with() {
  DBUG_TRACE;

  my_decimal val;

  bool res_fetch_latest_curval = this->do_refresh_start_with_option(val);

  if (!res_fetch_latest_curval) {
    this->m_option.start_with = val;
    return false;
  } else {
    return true;
  }
}

bool Gdb_sequence_entity::read_val(bool read_next, my_decimal &val) {
  DBUG_TRACE;

  if (open_and_init()) return true;

  /* fast-path that request current val */
  if (!read_next && !m_option.order_flag) {
    std::lock_guard<std::mutex> lock(m_mutex);
    if (is_seqval_invalid(m_cur)) {
      my_error(ER_GDB_READ_SEQUENCE, MYF(0), "currval not generated yet!");
      return true;
    }
    val = m_cur;
    return false;
  }

  /* slow-patch that request current val (order_flag=true)
     TODO: improve performance */
  if (!read_next && m_option.order_flag) {
    std::lock_guard<std::mutex> lock(m_mutex);
    if (init_sequence()) return true;
    if (is_seqval_invalid(m_cur)) {
      my_error(ER_GDB_READ_SEQUENCE, MYF(0), "currval not generated yet!");
      return true;
    }
    val = m_cur;
    return false;
  }

  /* get next value */
  bool is_start = false;
  std::lock_guard<std::mutex> lock(m_mutex);
  if (!m_cached_num) {
    auto cache_num = m_option.cache_num != 0 ? m_option.cache_num : 1;
    uint desires = m_option.order_flag ? 1 : cache_num;
    if (allocate_numbers(desires, is_start)) return true;
  }
  assert(!m_option.order_flag || m_cached_num <= 1);

  auto &incr = m_option.increment;
  if (is_start)
    ;  // do not need inc, use start value
  else {
    my_decimal res;
    my_decimal_add(E_DEC_FATAL_ERROR, &res, &m_cur, &incr);
    m_cur = res;
    m_cached_num--;  // consume one cached number
  }

  val = m_cur;
  return false;
}

static my_decimal calc_next_value_for_show(const my_decimal &currval,
                                           const Sequence_option &option) {
  assert(my_decimal_cmp(&currval, &gdb_seqval_invalid));
  bool cycle = option.cycle_flag;
  bool incr_neg = option.increment.sign();
  my_decimal nextval;
  my_decimal_add(E_DEC_FATAL_ERROR, &nextval, &currval, &option.increment);

  // check over boundary
  if (my_decimal_cmp(&nextval, &option.min_value) < 0 ||
      my_decimal_cmp(&nextval, &option.max_value) > 0) {
    if (cycle)
      return incr_neg ? option.max_value : option.min_value;
    else
      return currval;  // used up
  }
  return nextval;
}

bool Gdb_sequence_entity::do_refresh_start_with_option(my_decimal &val) {
  DBUG_TRACE;

  if (open_and_init()) return true;

  // 1. check persist record's value, since may mdified by other sqlnode
  my_decimal persist_val;
  auto ret = false;
  {
    std::lock_guard<std::mutex> lock(m_mutex);
    ret = m_persist_handler->read_persist_record(false, persist_val);
  }
  if (ret) {
    std::string err_msg = "[read persist]";
    err_msg += m_persist_handler->m_errmsg;
    my_error(ER_GDB_READ_SEQUENCE, MYF(0), err_msg.c_str());
    return true;
  }

  // 2. return "start_with" if "select nextval" not called on all sqlnodes
  if (my_decimal_cmp(&persist_val, &gdb_seqval_invalid) == 0) {
    val = m_option.start_with;
    return false;
  }

  // 3. calc nextval according currval
  my_decimal currval;
  if (my_decimal_cmp(&persist_val, &m_last_persist_val) == 0)
    currval = m_cur;
  else
    currval = persist_val;

  val = calc_next_value_for_show(currval, m_option);
  return false;
}

/* this function will auto commit operation */
bool Gdb_sequence_entity::create_persist_record(const my_decimal &cur) {
  DBUG_TRACE;
  assert(m_persist_handler != nullptr);

  if (m_persist_handler->create_persist_record(cur)) {
    std::string err_msg = "[insert persist record]";
    err_msg += m_persist_handler->m_errmsg;
    my_error(ER_GDB_CREATE_SEQUENCE, MYF(0), err_msg.c_str());
    return true;
  }
  return false;
}

/* need_commit
       true: auto commit delete operation.
       false: only delete operation without commit(used for rollback when
   migrate sequence). */
bool Gdb_sequence_entity::delete_persist_record(bool need_commit) {
  DBUG_TRACE;
  assert(m_persist_handler != nullptr);

  int ret = 0;
  if (!need_commit) {
    /* useless: for migrate sequence */
    assert(0);
    ret = m_persist_handler->delete_persist_record_without_commit();
  } else {
    /* for drop sequende */
    ret = m_persist_handler->delete_persist_record();
  }
  if (ret) {
    std::string err_msg = "[delete persist record]";
    err_msg += m_persist_handler->m_errmsg;
    my_error(ER_GDB_DROP_SEQUENCE, MYF(0), err_msg.c_str());
    return true;
  }
  return false;
}

/* NOTE: caller need ensure concurrency safety */
bool Gdb_sequence_entity::open_sequence() {
  DBUG_TRACE;
  assert(m_persist_handler == nullptr);
  assert(m_status == SEQ_STATUS_CLOSED);

  if (setup_persist_handler()) return true;

  m_status = SEQ_STATUS_UNREADABLE;
  return false;
}

/* NOTE: caller need ensure concurrency safety */
bool Gdb_sequence_entity::init_sequence() {
  DBUG_TRACE;
  assert(m_persist_handler != nullptr);

  my_decimal currval;
  auto ret = m_persist_handler->read_persist_record(false, currval);

  if (ret) {
    std::string err_msg = "[init sequence read persist]";
    err_msg += m_persist_handler->m_errmsg;
    my_error(ER_GDB_READ_SEQUENCE, MYF(0), err_msg.c_str());
    return true;
  }

  /* if order_flag=false, only call one time for sequence cache entity.
     While order_flag=true, the m_cached_num must be consumed to 0 */
  assert(m_cached_num == 0);

  if (m_status.load() == SEQ_STATUS_UNREADABLE) m_status = SEQ_STATUS_READY;
  m_cur = currval;
  return false;
}

/* NOTE: will use select ... for upate, need release record lock later
         caller need ensure concurrency safety */
bool Gdb_sequence_entity::get_persist_currval(my_decimal &currval) {
  DBUG_TRACE;
  assert(m_persist_handler != nullptr);

  auto ret =
      m_persist_handler->read_persist_record(true /*for_update*/, currval);
  if (ret) {
    std::string err_msg = "[get_persist_currval() read persist]";
    err_msg += m_persist_handler->m_errmsg;
    my_error(ER_GDB_READ_SEQUENCE, MYF(0), err_msg.c_str());
    return true;
  }

  return false;
}

bool Gdb_sequence_entity::release_record_lock(bool must_commit) {
  DBUG_TRACE;
  assert(m_persist_handler != nullptr);

  return m_persist_handler->release_rec_lock(must_commit);
}

void Gdb_sequence_entity::update_cache_sequence_attributes(
    const Sequence_option *new_option) {
  DBUG_TRACE;
  assert(nullptr != new_option);

  m_option.start_with = new_option->start_with;

  if (new_option->option_bitmap & (Sequence_option::OPTION_TYPE_MINVALUE |
                                   Sequence_option::OPTION_TYPE_NOMINVALUE)) {
    m_option.min_value = new_option->min_value;
  }

  if (new_option->option_bitmap & (Sequence_option::OPTION_TYPE_MAXVALUE |
                                   Sequence_option::OPTION_TYPE_NOMAXVALUE)) {
    m_option.max_value = new_option->max_value;
  }

  if (new_option->option_bitmap & Sequence_option::OPTION_TYPE_INCREMENT) {
    m_option.increment = new_option->increment;
  }

  if (new_option->option_bitmap & Sequence_option::OPTION_TYPE_CYCLEFLAG) {
    m_option.cycle_flag = new_option->cycle_flag;
  }

  if (new_option->option_bitmap & Sequence_option::OPTION_TYPE_CACHENUM) {
    m_option.cache_num = new_option->cache_num;
  }

  if (new_option->option_bitmap & Sequence_option::OPTION_TYPE_ORDERFLAG) {
    m_option.order_flag = new_option->order_flag;
  }
}

/* NOTE: caller need ensure concurrency safety */
bool Gdb_sequence_entity::allocate_numbers(uint desires,
                                           bool &start_or_restart) {
  DBUG_TRACE;
  assert(m_status >= SEQ_STATUS_READY);
  assert(m_persist_handler != nullptr);

  /* query the latest value from persist table */
  my_decimal cur;
  if (m_persist_handler->read_persist_record(true /*for_update*/, cur)) {
    std::string err_msg = "[allocate_numbers() read persist]";
    err_msg += m_persist_handler->m_errmsg;
    my_error(ER_GDB_READ_SEQUENCE, MYF(0), err_msg.c_str());
    return true;
  }

  bool is_start = (is_seqval_invalid(cur)) ? true : false;
  my_decimal valid_range, desire_range, available_num;
  auto &incr = m_option.increment;
  auto &min = m_option.min_value;
  auto &max = m_option.max_value;
  auto cycle = m_option.cycle_flag;

  do {
    // 1. check desires are met or not or partial
    if (is_start) {
      if (is_seqval_invalid(cur)) cur = m_option.start_with;
      desires--;  // the cur(start value) can be used
    }

    if (incr.sign())
      my_decimal_sub(E_DEC_FATAL_ERROR, &valid_range, &cur, &min);
    else
      my_decimal_sub(E_DEC_FATAL_ERROR, &valid_range, &max, &cur);
    assert(!valid_range.sign());

    my_decimal desires_decimal;
    int2my_decimal(E_DEC_FATAL_ERROR, desires, true, &desires_decimal);
    my_decimal_mul(E_DEC_FATAL_ERROR, &desire_range, &incr, &desires_decimal);
    if (desire_range.sign()) my_decimal_neg(&desire_range);

    if (my_decimal_cmp(&valid_range, &desire_range) >= 0)
      available_num = desires_decimal;
    else {
      my_decimal_div(E_DEC_FATAL_ERROR, &available_num, &valid_range, &incr, 0);
      if (available_num.sign()) my_decimal_neg(&available_num);
    }

    // 2. if from start=true, must success
    if (is_start) break;

    // 3. if no available_num, try restart from min/max value if cycle_flag=true
    if (my_decimal_is_zero(&available_num) && !is_start) {
      if (!cycle) {
        my_error(ER_GDB_READ_SEQUENCE, MYF(0), "sequence's value used up!");
        m_persist_handler->release_rec_lock(false);  // end transaction
        return true;
      } else {
        is_start = true;
        if (incr.sign())  // < 0
          cur = max;
        else
          cur = min;
        continue;  // restart
      }
    }
  } while (my_decimal_is_zero(&available_num));  // while loop for "cycle"

  /* update persist table */
  my_decimal tbl_val, used_range;
  my_decimal_mul(E_DEC_FATAL_ERROR, &used_range, &available_num, &incr);
  my_decimal_add(E_DEC_FATAL_ERROR, &tbl_val, &cur, &used_range);
  if (m_persist_handler->update_persist_record(tbl_val)) {
    std::string err_msg = "[update persist]";
    err_msg += m_persist_handler->m_errmsg;
    my_error(ER_GDB_READ_SEQUENCE, MYF(0), err_msg.c_str());
    return true;
  }

  m_cur = cur;
  m_last_persist_val = tbl_val;
  longlong cached_num = 0;
  my_decimal2int(E_DEC_FATAL_ERROR, &available_num, true /*unsigned*/,
                 &cached_num);
  m_cached_num = cached_num;
  start_or_restart = is_start;
  return false;
}

/* NOTE: caller need ensure concurrency safety */
void Gdb_sequence_entity::close_seq() {
  if (m_status.load() > SEQ_STATUS_CLOSED) {
    m_persist_handler->close();
  }
  m_status = SEQ_STATUS_CLOSED;
}

const sequences_cache_map *global_sequences_cache() { return sequences_cache; }

/*
  Initialize structures responsible for sequences.

  Params:
    @dont_read_sequences_table:  true if we want to skip loading data from
                                server table and disable privilege checking.

  NOTES
    This function is mostly responsible for preparatory steps, main work
    on initialization and grants loading is done in sequences_load().
*/
bool sequences_init(bool dont_read_sequences_table) {
  DBUG_TRACE;
  THD *thd;
  bool ret = false;

#ifdef HAVE_PSI_INTERFACE
  init_sequences_cache_psi_keys();
#endif

  /* init the mutex */
  if (mysql_rwlock_init(key_rwlock_THR_LOCK_sequences, &THR_LOCK_sequences))
    return true;

  /* initialise sequences cache */
  sequences_cache = new sequences_cache_map();

  if (dont_read_sequences_table) goto end;

  /*
    To be able to run this from boot, we allocate a temporary THD
  */
  if (!(thd = new THD)) return true;
  thd->thread_stack = (char *)&thd;
  thd->store_globals();
  ret = sequences_load(thd);
  delete thd;

end:
  return ret;
}

/*
  Initialize sequence structures
*/
static bool do_sequences_load(THD *thd, TABLE *table) {
  DBUG_TRACE;

  if (sequences_cache != nullptr) {
    sequences_cache->clear();
  }

  unique_ptr_destroy_only<RowIterator> iterator = init_table_iterator(
      thd, table,
      /*ignore_not_found_rows=*/false, /*count_examined_rows=*/false);
  if (iterator == nullptr) return true;

  while (!(iterator->Read())) {
    if ((get_sequence_from_table_to_cache(table))) return true;
  }

  return false;
}

/**
   Helper function for creating a record for updating
   an existing server in the mysql.servers table.

   Set a field to the given parser value unless
   the parser value is empty or equal to the existing value.
*/
static void store_altered_sequence_attr(TABLE *table,
                                        const Sequence_option *new_option) {
  assert((nullptr != table) && (nullptr != new_option));

  table->field[SEQUENCE_FIELD_START_WITH]->store_decimal(
      &new_option->start_with);

  if (new_option->option_bitmap & (Sequence_option::OPTION_TYPE_MINVALUE |
                                   Sequence_option::OPTION_TYPE_NOMINVALUE)) {
    table->field[SEQUENCE_FIELD_MINVALUE]->store_decimal(
        &new_option->min_value);
  }

  if (new_option->option_bitmap & (Sequence_option::OPTION_TYPE_MAXVALUE |
                                   Sequence_option::OPTION_TYPE_NOMAXVALUE)) {
    table->field[SEQUENCE_FIELD_MAXVALUE]->store_decimal(
        &new_option->max_value);
  }

  if (new_option->option_bitmap & Sequence_option::OPTION_TYPE_INCREMENT) {
    table->field[SEQUENCE_FIELD_INCREMENT]->store_decimal(
        &new_option->increment);
  }

  if (new_option->option_bitmap & Sequence_option::OPTION_TYPE_CYCLEFLAG) {
    table->field[SEQUENCE_FIELD_CYCLE_FLAG]->store(new_option->cycle_flag);
  }

  if (new_option->option_bitmap & Sequence_option::OPTION_TYPE_CACHENUM) {
    table->field[SEQUENCE_FIELD_CACHE_NUM]->store(new_option->cache_num);
  }

  if (new_option->option_bitmap & Sequence_option::OPTION_TYPE_ORDERFLAG) {
    table->field[SEQUENCE_FIELD_ORDER_FALG]->store(new_option->order_flag);
  }
}

/*
  Forget current sequences cache and read new sequences from table

  NOTE
    All tables of calling thread which were open and locked by LOCK TABLES
    statement will be unlocked and closed.
    This function is also used for initialization of structures responsible
    for user/db-level privilege checking.
*/
bool sequences_load(THD *thd) {
  bool ret = true;
  DBUG_TRACE;

  mysql_rwlock_wrlock(&THR_LOCK_sequences);

  Table_ref tables("mysql", GREATDB_SEQUENCES_TABLE, TL_READ);
  if (open_trans_system_tables_for_read(thd, &tables)) {
    if (thd->get_stmt_da()->is_error())
      LogErr(ERROR_LEVEL, ER_CANT_OPEN_AND_LOCK_PRIVILEGE_TABLES,
             thd->get_stmt_da()->message_text());
    goto end;
  }

  if ((ret = do_sequences_load(thd, tables.table))) {
    // Error. Revert to old list
    /* blast, for now, we have no servers, discuss later way to preserve */

    sequences_free();
  }

  close_trans_system_tables(thd);
end:
  mysql_rwlock_unlock(&THR_LOCK_sequences);
  return ret;
}

/* close connections for each sequence entity
   TODO: each sequence  will hold a connection
     using cmd_service, it may use up connection */
void sequences_close() {
  DBUG_TRACE;
  mysql_rwlock_rdlock(&THR_LOCK_sequences);
  for (auto &itr : *sequences_cache) {
    auto seq = itr.second;
    std::lock_guard<std::mutex> lock(seq->m_mutex);
    seq->close_seq();
  }
  mysql_rwlock_unlock(&THR_LOCK_sequences);
}

void sequences_free(bool end) {
  DBUG_TRACE;
  if (sequences_cache == nullptr) return;
  if (!end) {
    sequences_cache->clear();
    return;
  }
  mysql_rwlock_destroy(&THR_LOCK_sequences);
  delete sequences_cache;
  sequences_cache = nullptr;
}

Gdb_sequences_lock_guard::Gdb_sequences_lock_guard(bool readonly) {
  if (readonly) {
    mysql_rwlock_rdlock(&THR_LOCK_sequences);
  } else {
    mysql_rwlock_wrlock(&THR_LOCK_sequences);
  }
}

Gdb_sequences_lock_guard::~Gdb_sequences_lock_guard() {
  mysql_rwlock_unlock(&THR_LOCK_sequences);
}

/*
  Initialize structure of sequence
*/
static bool get_sequence_from_table_to_cache(TABLE *table) {
  DBUG_TRACE;
  char *ptr;
  std::string db;
  std::string name;

  MEM_ROOT mem{PSI_NOT_INSTRUMENTED, 512};
  table->use_all_columns();

  /* load sequence's db/name, persist_type, backend_shard_id */
  ptr = get_field(&mem, table->field[SEQUENCE_FIELD_DB]);
  assert(ptr != nullptr);
  db = std::string(ptr);
  ptr = get_field(&mem, table->field[SEQUENCE_FIELD_NAME]);
  assert(ptr != nullptr);
  name = std::string(ptr);

  /* load sequence options */
  Sequence_option seq_option;
  bool ret = 0;
  ptr = get_field(&mem, table->field[SEQUENCE_FIELD_START_WITH]);
  ret |= str2my_decimal(E_DEC_FATAL_ERROR, ptr, strlen(ptr), &my_charset_bin,
                        &seq_option.start_with);
  ptr = get_field(&mem, table->field[SEQUENCE_FIELD_MINVALUE]);
  ret |= str2my_decimal(E_DEC_FATAL_ERROR, ptr, strlen(ptr), &my_charset_bin,
                        &seq_option.min_value);
  ptr = get_field(&mem, table->field[SEQUENCE_FIELD_MAXVALUE]);
  ret |= str2my_decimal(E_DEC_FATAL_ERROR, ptr, strlen(ptr), &my_charset_bin,
                        &seq_option.max_value);
  ptr = get_field(&mem, table->field[SEQUENCE_FIELD_INCREMENT]);
  ret |= str2my_decimal(E_DEC_FATAL_ERROR, ptr, strlen(ptr), &my_charset_bin,
                        &seq_option.increment);
  ptr = get_field(&mem, table->field[SEQUENCE_FIELD_CYCLE_FLAG]);
  seq_option.cycle_flag = std::stoi(ptr);
  ptr = get_field(&mem, table->field[SEQUENCE_FIELD_CACHE_NUM]);
  seq_option.cache_num = std::stoul(ptr);
  ptr = get_field(&mem, table->field[SEQUENCE_FIELD_ORDER_FALG]);
  seq_option.order_flag = std::stoi(ptr);
  if (ret) {
    LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
           "read sequence decimal column error");
    return true;
  }

  auto seq_entity =
      std::make_shared<Gdb_sequence_entity>(db, name, &seq_option);
  if (seq_entity == nullptr) {
    LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG, "malloc memory for sequence error");
    return true;
  }

  auto version = get_and_inc_version();  // for re-prepare
  seq_entity->set_metadata_version(version);
  sequences_cache->emplace(seq_entity->key(), seq_entity);
  return false;
}

bool Sql_cmd_common_sequence::lock_and_open_table(THD *thd) {
  if (acquire_shared_backup_lock(thd, thd->variables.lock_wait_timeout))
    return true;

  Table_ref tables("mysql", GREATDB_SEQUENCES_TABLE, TL_WRITE);

  table = open_ltable(thd, &tables, TL_WRITE, MYSQL_LOCK_IGNORE_TIMEOUT);
  if (table == nullptr) return true;

  return false;
}

/*TODO: the same reason with class Gdb_seq_persist_front_end, wait "8.0.22-CDC"
    merged, then directly call "slave_mode" in "sql/gdb_utils.h" */
static bool seq_slave_mode(THD *thd) {
  return thd->is_mgr_slave() || thd->rli_slave;
}

#define PROC_NAME_AND_LOCK                                          \
  LEX_STRING db_lex = to_lex_string(*m_db);                         \
  if (!m_db->str || !m_db->length) {                                \
    /* if failed, automatically call my_error() in function */      \
    if (thd->copy_db_to(&db_lex.str, &db_lex.length)) return true;  \
  }                                                                 \
  std::string db = std::string(db_lex.str, db_lex.length);          \
  auto dbname = check_lowercase_dbname(db.c_str());                 \
  auto seq_name = normalize_seq_name(m_seq_name->str);              \
  auto seq_key = calc_seq_key(dbname.c_str(), seq_name.c_str());    \
  if (lock_sequence(thd, dbname.c_str(), seq_name.c_str(), true)) { \
    return true;                                                    \
  }

// NOTE: If create succeed, the sequence is "OPENED"(SEQ_STATUS_UNREADABLE)
bool Sql_cmd_create_sequence::execute(THD *thd) {
  DBUG_TRACE;

  if (m_seq_option->validate_create_options(thd)) return true;

  PROC_NAME_AND_LOCK

  bool table_view_is_exists = false;
  if (dd::table_exists(thd->dd_client(), dbname.c_str(), seq_name.c_str(),
                       &table_view_is_exists)) {
    return true;
  } else {
    if (table_view_is_exists) {
      my_error(ER_GDB_CREATE_SEQUENCE, MYF(0),
               "duplicated table/view name is already exists!");
      return true;
    }
  }

  auto version = get_and_inc_version();  // for re-prepare
  auto seq_entity = std::make_shared<Gdb_sequence_entity>(
      db, seq_name, m_seq_option, version);
  if (seq_entity == nullptr) {
    LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG, "malloc memory for sequence error");
    return true;
  }

  // Check for existing cache entries with same name
  mysql_rwlock_rdlock(&THR_LOCK_sequences);
  const auto it = sequences_cache->find(seq_key);
  if (it != sequences_cache->end()) {
    mysql_rwlock_unlock(&THR_LOCK_sequences);
    /* do nothing is the sequence is already exists */
    HA_CREATE_INFO create_info(*thd->lex->create_info);
    std::string exists_info("sequence '");
    exists_info += seq_name;
    exists_info += "' already exists";
    if (create_info.options & HA_LEX_CREATE_IF_NOT_EXISTS) {
      push_warning(thd, Sql_condition::SL_NOTE, ER_GDB_CREATE_SEQUENCE,
                   exists_info.c_str());
      my_ok(thd, 1);
      return false;
    }

    my_error(ER_GDB_CREATE_SEQUENCE, MYF(0), exists_info.c_str());
    return true;
  }

  if (check_access(thd, CREATE_ACL, db_lex.str, nullptr, nullptr, true,
                   false) ||
      lock_and_open_table(thd)) {
    // if failed, automatically call my_error() in function
    mysql_rwlock_unlock(&THR_LOCK_sequences);
    return true;
  }

  int error;
  {
    table->use_all_columns();
    empty_record(table);

    /* set the field that's the PK to the value we're looking for */
    table->field[SEQUENCE_FIELD_DB]->store(dbname.c_str(), dbname.size(),
                                           system_charset_info);
    table->field[SEQUENCE_FIELD_NAME]->store(seq_name.c_str(), seq_name.size(),
                                             system_charset_info);
    table->field[SEQUENCE_FIELD_START_WITH]->store_decimal(
        &m_seq_option->start_with);
    table->field[SEQUENCE_FIELD_MINVALUE]->store_decimal(
        &m_seq_option->min_value);
    table->field[SEQUENCE_FIELD_MAXVALUE]->store_decimal(
        &m_seq_option->max_value);
    table->field[SEQUENCE_FIELD_INCREMENT]->store_decimal(
        &m_seq_option->increment);
    table->field[SEQUENCE_FIELD_CYCLE_FLAG]->store(m_seq_option->cycle_flag);
    table->field[SEQUENCE_FIELD_CACHE_NUM]->store(m_seq_option->cache_num);
    table->field[SEQUENCE_FIELD_ORDER_FALG]->store(m_seq_option->order_flag);

    /* write/insert the new sequence */
    Disable_binlog_guard binlog_guard(thd);
    if ((error = table->file->ha_write_row(table->record[0]))) {
      if (error == ER_DUP_ENTRY || error == ER_DUP_KEY)
        my_error(ER_GDB_CREATE_SEQUENCE, MYF(0),
                 "sequence metadata already exists!");
      table->file->print_error(error, MYF(0));
    }

    if (error)
      ;  // fall through
    else if (!seq_slave_mode(thd) &&
             seq_entity->create_persist_record(gdb_seqval_invalid)) {
      error = seq_entity->get_persist_handler_errno();
    } else {
      /* insert the sequence into the cache */
      mysql_rwlock_unlock(&THR_LOCK_sequences);
      mysql_rwlock_wrlock(&THR_LOCK_sequences);
      sequences_cache->emplace(seq_key, seq_entity);
    }
  }

  mysql_rwlock_unlock(&THR_LOCK_sequences);
  if (error || write_bin_log(thd, false, thd->query().str, thd->query().length))
    trans_rollback_stmt(thd);
  else
    trans_commit_stmt(thd);
  close_mysql_tables(thd);

  if (error == 0 && !thd->killed) my_ok(thd, 1);
  return error != 0 || thd->killed;
}

bool Sql_cmd_alter_sequence::execute(THD *thd) {
  DBUG_TRACE;

  PROC_NAME_AND_LOCK

  mysql_rwlock_rdlock(&THR_LOCK_sequences);
  const auto it = sequences_cache->find(seq_key);
  if (it == sequences_cache->end()) {
    mysql_rwlock_unlock(&THR_LOCK_sequences);
    my_error(ER_GDB_ALTER_SEQUENCE, MYF(0), "sequence not exists!");
    return true;
  }

  /* open sequence if is closed */
  if (!it->second->is_opened() && it->second->open_sequence()) {
    mysql_rwlock_unlock(&THR_LOCK_sequences);
    return true;
  }

  /* validate altered param */
  auto seq_options = it->second->get_options();
  if (m_seq_option->validate_alter_options(thd, seq_options)) {
    mysql_rwlock_unlock(&THR_LOCK_sequences);
    return true;
  }

  /* need reallocate in following situation. */
  bool reallocate = false;
  {
    auto &old = seq_options;
    auto &now = *m_seq_option;
    if (seq_slave_mode(thd) || old.increment != now.increment ||
        old.cache_num > now.cache_num || (!old.order_flag && now.order_flag))
      reallocate = true;
  }

  int error = 0;
  /* query currval from persist record if not slave */
  if (!seq_slave_mode(thd)) {
    my_decimal currval;
    if (it->second->get_persist_currval(currval)) {
      my_error(ER_GDB_ALTER_SEQUENCE, MYF(0), "get persist currval failed");
      error = 1;
    } else if (m_seq_option->validate_alter_currval(currval)) {
      error = 1;
    }

    // check is update minvalue > current value
    if (error == 0 &&
        m_seq_option->option_bitmap & Sequence_option::OPTION_TYPE_MINVALUE) {
      if (my_decimal_cmp(&m_seq_option->min_value, &it->second->get_currval()) >
          0) {
        my_error(ER_GDB_ALTER_SEQUENCE, MYF(0),
                 "alter MINVALUE can't greater than \"currval\" ");
        error = 1;
      }
    }
  }

  if (error ||
      check_access(thd, ALTER_ACL, db_lex.str, nullptr, nullptr, true, false) ||
      lock_and_open_table(thd)) {
    mysql_rwlock_unlock(&THR_LOCK_sequences);
    it->second->release_record_lock(false);
    return true;
  }

  // alter sequence
  {
    table->use_all_columns();
    table->field[SEQUENCE_FIELD_DB]->store(dbname.c_str(), dbname.size(),
                                           system_charset_info);
    table->field[SEQUENCE_FIELD_NAME]->store(seq_name.c_str(), seq_name.size(),
                                             system_charset_info);

    uchar key_buf[GREATDB_SEQUENCE_MAX_KEY_LEN] = {0};
    assert(GREATDB_SEQUENCE_MAX_KEY_LEN >= table->key_info->key_length);
    key_copy(key_buf, table->record[0], table->key_info,
             table->key_info->key_length);

    error = table->file->ha_index_read_idx_map(
        table->record[0], GREATDB_SEQUENCES_TABLE_KEY_PRIMARY, key_buf,
        HA_WHOLE_KEY, HA_READ_KEY_EXACT);
    if (error) {
      table->file->print_error(error, MYF(0));
    }

    if (!error) {
      Disable_binlog_guard binlog_guard(thd);

      /* so we can update since the record exists in the table */
      store_record(table, record[1]);

      store_altered_sequence_attr(table, m_seq_option);

      error = table->file->ha_update_row(table->record[1], table->record[0]);
      if (0 == error) {
        /* update attributes of cache sequence */
        it->second->update_cache_sequence_attributes(m_seq_option);
        /* if need reallocate, set cached numbers to 0. */
        if (reallocate) it->second->evict_cached_numbers();
        auto version = get_and_inc_version();  // for re-prepare
        it->second->set_metadata_version(version);
      } else if (HA_ERR_RECORD_IS_THE_SAME == error) {
        error = 0;
      } else {
        table->file->print_error(error, MYF(0));
      }
    }
  }

  it->second->release_record_lock(false);
  mysql_rwlock_unlock(&THR_LOCK_sequences);

  if (error || write_bin_log(thd, false, thd->query().str, thd->query().length))
    trans_rollback_stmt(thd);
  else
    trans_commit_stmt(thd);
  close_mysql_tables(thd);

  if (error == 0 && !thd->killed) my_ok(thd, 1);
  return error != 0 || thd->killed;
}

bool Sql_cmd_drop_sequence::execute(THD *thd) {
  DBUG_TRACE;

  PROC_NAME_AND_LOCK

  mysql_rwlock_rdlock(&THR_LOCK_sequences);
  const auto it = sequences_cache->find(seq_key);
  if (it == sequences_cache->end()) {
    mysql_rwlock_unlock(&THR_LOCK_sequences);
    std::string drop_info("sequence '");
    drop_info += seq_key;
    drop_info += "' not exists";
    if (thd->lex->drop_if_exists) {
      push_warning(thd, Sql_condition::SL_NOTE, ER_GDB_DROP_SEQUENCE,
                   drop_info.c_str());
      my_ok(thd, 1);
      return false;
    }

    my_error(ER_GDB_DROP_SEQUENCE, MYF(0), drop_info.c_str());
    return true;
  }

  /* open sequence if is closed */
  if (!it->second->is_opened() && it->second->open_sequence()) {
    mysql_rwlock_unlock(&THR_LOCK_sequences);
    return true;
  }

  if (check_access(thd, DROP_ACL, db_lex.str, nullptr, nullptr, true, false) ||
      lock_and_open_table(thd)) {
    // if failed, automatically call my_error() in function
    mysql_rwlock_unlock(&THR_LOCK_sequences);
    return true;
  }

  int error;
  {
    table->use_all_columns();
    table->field[SEQUENCE_FIELD_DB]->store(dbname.c_str(), dbname.size(),
                                           system_charset_info);
    table->field[SEQUENCE_FIELD_NAME]->store(seq_name.c_str(), seq_name.size(),
                                             system_charset_info);
    uchar key_buf[GREATDB_SEQUENCE_MAX_KEY_LEN];
    assert(GREATDB_SEQUENCE_MAX_KEY_LEN >= table->key_info->key_length);
    key_copy(key_buf, table->record[0], table->key_info,
             table->key_info->key_length);

    error = table->file->ha_index_read_idx_map(
        table->record[0], GREATDB_SEQUENCES_TABLE_KEY_PRIMARY, key_buf,
        HA_WHOLE_KEY, HA_READ_KEY_EXACT);
    if (error) {
      table->file->print_error(error, MYF(0));
    }

    if (error)
      ;  // fall through
    else if (!seq_slave_mode(thd) &&
             it->second->delete_persist_record(true /*auto commit*/)) {
      error = it->second->get_persist_handler_errno();
    } else {
      Disable_binlog_guard binlog_guard(thd);
      // Delete from table
      if ((error = table->file->ha_delete_row(table->record[0])))
        table->file->print_error(error, MYF(0));
      else {
        // Remove from cache
        mysql_rwlock_unlock(&THR_LOCK_sequences);
        mysql_rwlock_wrlock(&THR_LOCK_sequences);
        it->second->close_seq();
        sequences_cache->erase(seq_key);
      }
    }
  }

  mysql_rwlock_unlock(&THR_LOCK_sequences);

  if (error || write_bin_log(thd, false, thd->query().str, thd->query().length))
    trans_rollback_stmt(thd);
  else
    trans_commit_stmt(thd);
  close_mysql_tables(thd);

  if (error == 0 && !thd->killed) my_ok(thd, 1);
  return error != 0 || thd->killed;
}

uint64_t has_sequence_def(THD *thd, const char *db, const char *seq_name) {
  DBUG_TRACE;
  assert(db != nullptr && strlen(db) > 0);

  if (sequences_cache == nullptr) return false;  // starting up
  if (seq_name == nullptr || strlen(seq_name) == 0)
    return false;  // no seq_name
  auto seq_name_string = normalize_seq_name(seq_name);
  auto seq_key = calc_seq_key(db, seq_name_string.c_str());

  uint64_t res = 0;
  if (lock_sequence(thd, db, seq_name, false)) return res;

  mysql_rwlock_rdlock(&THR_LOCK_sequences);
  auto itr = sequences_cache->find(seq_key);
  if (itr != sequences_cache->end()) res = itr->second->metadata_version();
  mysql_rwlock_unlock(&THR_LOCK_sequences);
  return res;
}
std::shared_ptr<Gdb_sequence_entity> get_sequence_def(const char *db,
                                                      const char *name) {
  DBUG_TRACE;
  /* db and name already checked by "has_sequence_def()" */
  assert(db != nullptr && strlen(db) > 0);
  assert(name != nullptr && strlen(name) > 0);
  auto seq_name_string = normalize_seq_name(name);
  auto seq_key = calc_seq_key(db, seq_name_string.c_str());
  std::shared_ptr<Gdb_sequence_entity> res(nullptr);

  mysql_rwlock_rdlock(&THR_LOCK_sequences);
  auto itr = sequences_cache->find(seq_key);
  if (itr != sequences_cache->end()) res = itr->second;
  mysql_rwlock_unlock(&THR_LOCK_sequences);
  if (res == nullptr) {
    my_error(ER_GDB_READ_SEQUENCE, MYF(0),
             "sequence cache entity not found, maybe dropped!");
  }
  return res;
}

std::shared_ptr<Gdb_sequence_entity> find_sequence_def(THD *thd, const char *db,
                                                       const char *seq_name) {
  DBUG_TRACE;

  assert(db != nullptr && strlen(db) > 0);
  if (sequences_cache == nullptr) return nullptr;  // starting up
  if (seq_name == nullptr || strlen(seq_name) == 0)
    return nullptr;  // no seq_name
  auto seq_name_string = normalize_seq_name(seq_name);
  auto seq_key = calc_seq_key(db, seq_name_string.c_str());

  if (lock_sequence(thd, db, seq_name, false)) return nullptr;

  std::shared_ptr<Gdb_sequence_entity> res(nullptr);
  mysql_rwlock_rdlock(&THR_LOCK_sequences);
  auto itr = sequences_cache->find(seq_key);
  if (itr != sequences_cache->end()) res = itr->second;
  mysql_rwlock_unlock(&THR_LOCK_sequences);
  return res;
}

static bool lock_sequence_low(THD *thd, const char *db, const char *name,
                              bool ddl) {
  DBUG_TRACE;
  MDL_request_list mdl_requests;
  MDL_request global_request;
  MDL_request backup_request;
  MDL_request schema_request;
  MDL_request mdl_request;

  enum_mdl_type mdl_type = ddl ? MDL_EXCLUSIVE : MDL_SHARED;
  enum_mdl_duration mdl_duration = ddl ? MDL_TRANSACTION : MDL_STATEMENT;
  MDL_REQUEST_INIT(&mdl_request, MDL_key::TABLE, db, name, mdl_type,
                   mdl_duration);
  mdl_requests.push_front(&mdl_request);
  MDL_REQUEST_INIT(&schema_request, MDL_key::SCHEMA, db, "",
                   MDL_INTENTION_EXCLUSIVE, mdl_duration);
  mdl_requests.push_front(&schema_request);
  if (ddl) {
    MDL_REQUEST_INIT(&backup_request, MDL_key::BACKUP_LOCK, "", "",
                     MDL_INTENTION_EXCLUSIVE, MDL_STATEMENT);
    mdl_requests.push_front(&backup_request);
    MDL_REQUEST_INIT(&global_request, MDL_key::GLOBAL, "", "",
                     MDL_INTENTION_EXCLUSIVE, MDL_STATEMENT);
    mdl_requests.push_front(&global_request);
  }
  return thd->mdl_context.acquire_locks(&mdl_requests,
                                        thd->variables.lock_wait_timeout);
}
/* TODO: acquire backup_request and backup_lock_request for backup ? */
bool lock_sequence(THD *thd, const char *db, const char *name, bool ddl) {
  DBUG_TRACE;
  /* db and name already checked by "find_sequence_def()" */
  assert(db != nullptr && strlen(db) > 0);
  assert(name != nullptr && strlen(name) > 0);
  auto seq_name_string = normalize_seq_name(name);

  auto ret = lock_sequence_low(thd, db, seq_name_string.c_str(), ddl);
#ifndef NDEBUG
  if (!ret) {
    DBUG_SIGNAL_WAIT_FOR(thd, "greatdb_sequence_hold_mdl", "reach_wait_sync",
                         "end_wait_sync");
  }
#endif
  return ret;
}
static bool append_create_sequence_str(String &buffer,
                                       Gdb_sequence_entity *seq_def) {
  DBUG_TRACE;
  assert(seq_def != nullptr);
  auto seq_option = seq_def->get_options();
  char val_str_buf[128];
  String val_str(val_str_buf, sizeof(val_str_buf), &my_charset_bin);
  buffer.append(STRING_WITH_LEN("CREATE SEQUENCE "));

  /* sequence name */
  append_ident(&buffer, seq_def->name().c_str(), seq_def->name().size(),
               IDENT_QUOTE_CHAR);

  /* start with */
  my_decimal curval;
  if (seq_def->do_refresh_start_with_option(curval)) return true;
  buffer.append(STRING_WITH_LEN(" START WITH "));
  my_decimal2string(E_DEC_FATAL_ERROR, &curval, &val_str);
  buffer.append(val_str);

  /* minvalue */
  buffer.append(STRING_WITH_LEN(" MINVALUE "));
  my_decimal2string(E_DEC_FATAL_ERROR, &seq_option.min_value, &val_str);
  buffer.append(val_str);

  /* maxvalue */
  buffer.append(STRING_WITH_LEN(" MAXVALUE "));
  my_decimal2string(E_DEC_FATAL_ERROR, &seq_option.max_value, &val_str);
  buffer.append(val_str);

  /* increment */
  buffer.append(STRING_WITH_LEN(" INCREMENT BY "));
  my_decimal2string(E_DEC_FATAL_ERROR, &seq_option.increment, &val_str);
  buffer.append(val_str);

  /* cycle flag */
  if (seq_option.cycle_flag)
    buffer.append(STRING_WITH_LEN(" CYCLE"));
  else
    buffer.append(STRING_WITH_LEN(" NOCYCLE"));

  /* cache xx / nocache */
  if (seq_option.cache_num == 0) {
    buffer.append(STRING_WITH_LEN(" NOCACHE"));
  } else {
    buffer.append(STRING_WITH_LEN(" CACHE "));
    buffer.append_ulonglong(seq_option.cache_num);
  }

  /* order flag */
  if (seq_option.order_flag)
    buffer.append(STRING_WITH_LEN(" ORDER"));
  else
    buffer.append(STRING_WITH_LEN(" NOORDER"));

  return false;
}
bool build_sequence_create_str(String &buffer, const char *db,
                               const char *seq_name) {
  DBUG_TRACE;
  buffer.length(0);
  auto seq_def = get_sequence_def(db, seq_name);
  if (seq_def == nullptr) {
    my_error(ER_GDB_READ_SEQUENCE, MYF(0), "sequence not exists!");
    return true;
  }
  return append_create_sequence_str(buffer, seq_def.get());
}

bool get_migrate_sequences(
    std::map<std::shared_ptr<Gdb_sequence_entity>, uint64_t> &migrate_seqs,
    need_migrate_callback_t func, std::vector<uint64_t> &shard_ids) {
  DBUG_TRACE;
  assert(func != nullptr);
  assert(sequences_cache != nullptr);
  migrate_seqs.clear();

  mysql_rwlock_rdlock(&THR_LOCK_sequences);
  for (auto &entity : *sequences_cache) {
    uint64_t dst_shard_id = 0;
    if (func(entity.second.get(), dst_shard_id, shard_ids)) {
      migrate_seqs[entity.second] = dst_shard_id;
    }
  }
  mysql_rwlock_unlock(&THR_LOCK_sequences);
  return false;
}

static std::string build_insert_sql(Gdb_sequence_entity *seq,
                                    const my_decimal &cur) {
  std::string sql("INSERT INTO /*sequence_create*/ mysql.");
  sql += GREATDB_SEQUENCES_PERSIST_TABLE;
  sql += " (db,name,`currval`) VALUES (";
  sql += gdb_string_add_quote(seq->db(), VALUE_QUOTE_CHAR) + ",";
  sql += gdb_string_add_quote(seq->name(), VALUE_QUOTE_CHAR) + ",";

  char val_str_buf[128];
  String val_str(val_str_buf, sizeof(val_str_buf), &my_charset_bin);
  my_decimal2string(E_DEC_FATAL_ERROR, &cur, &val_str);
  sql += std::string(val_str.ptr(), val_str.length());
  sql += ")";
  return sql;
}
static std::string build_delete_sql(Gdb_sequence_entity *seq) {
  std::string sql("DELETE FROM /*sequence_drop*/ mysql.");
  sql += GREATDB_SEQUENCES_PERSIST_TABLE;
  sql += " WHERE db=";
  sql += gdb_string_add_quote(seq->db(), VALUE_QUOTE_CHAR);
  sql += " AND name=";
  sql += gdb_string_add_quote(seq->name(), VALUE_QUOTE_CHAR);
  return sql;
}
static std::string build_query_sql(Gdb_sequence_entity *seq, bool lock) {
  std::string sql("SELECT `currval` FROM /*sequence_query*/ mysql.");
  sql += GREATDB_SEQUENCES_PERSIST_TABLE;
  sql += " WHERE db=";
  sql += gdb_string_add_quote(seq->db(), VALUE_QUOTE_CHAR);
  sql += " AND name=";
  sql += gdb_string_add_quote(seq->name(), VALUE_QUOTE_CHAR);
  if (lock) sql += " FOR UPDATE";
  return sql;
}
static std::string build_update_sql(Gdb_sequence_entity *seq,
                                    const my_decimal &val) {
  std::string sql("UPDATE /*sequence_query*/ mysql.");
  sql += GREATDB_SEQUENCES_PERSIST_TABLE;
  sql += " SET `currval`=";

  char val_str_buf[128];
  String val_str(val_str_buf, sizeof(val_str_buf), &my_charset_bin);
  my_decimal2string(E_DEC_FATAL_ERROR, &val, &val_str);
  sql += std::string(val_str.ptr(), val_str.length());

  sql += " WHERE db=";
  sql += gdb_string_add_quote(seq->db(), VALUE_QUOTE_CHAR);
  sql += " AND name=";
  sql += gdb_string_add_quote(seq->name(), VALUE_QUOTE_CHAR);
  return sql;
}

///////////// start of sequence persist handler class ///////////
Gdb_seq_persist_front_end::~Gdb_seq_persist_front_end() {}
bool Gdb_seq_persist_front_end::create_persist_record(const my_decimal &cur) {
  DBUG_TRACE;
  auto sql = build_insert_sql(m_seq, cur);
  if (m_cmd_service.execute_sql(sql)) {
    auto &cb_data = m_cmd_service.get_cb_data();
    m_errno = cb_data.error_no();
    m_errmsg = cb_data.error_msg();
    return true;
  }
  if (m_cmd_service.execute_sql(STRING_WITH_LEN("COMMIT"))) {
    m_cmd_service.close_session();
    return true;
  }
  return false;
}
bool Gdb_seq_persist_front_end::delete_persist_record() {
  DBUG_TRACE;
  auto sql = build_delete_sql(m_seq);
  if (m_cmd_service.execute_sql(sql)) {
    auto &cb_data = m_cmd_service.get_cb_data();
    m_errno = cb_data.error_no();
    m_errmsg = cb_data.error_msg();
    return true;
  }
  if (m_cmd_service.execute_sql(STRING_WITH_LEN("COMMIT"))) {
    m_cmd_service.close_session();
    return true;
  }
  return false;
}
/* NOTE: If lock=true and read record succeed, need release rec_lock
   and end transaction later */
bool Gdb_seq_persist_front_end::read_persist_record(bool lock,
                                                    my_decimal &val) {
  DBUG_TRACE;
  auto sql = build_query_sql(m_seq, lock);
  if ((lock && m_cmd_service.execute_sql(STRING_WITH_LEN("BEGIN"))) ||
      m_cmd_service.execute_sql(sql)) {
    auto &cb_data = m_cmd_service.get_cb_data();
    m_errno = cb_data.error_no();
    m_errmsg = cb_data.error_msg();
    return true;
  }
  auto &cb_data = m_cmd_service.get_cb_data();
  assert(!cb_data.is_error());
  if (cb_data.rows() == 0) {
    m_errno = -1;
    m_errmsg = "persist record of sequence [";
    m_errmsg += m_seq->db() + "." + m_seq->name() + "] not found!";
    if (lock) m_cmd_service.execute_sql(STRING_WITH_LEN("ROLLBACK"));
    return true;
  }

  std::string val_str = cb_data.get_value(0, 0);
  int ret = str2my_decimal(E_DEC_FATAL_ERROR, val_str.c_str(), val_str.size(),
                           &my_charset_bin, &val);
  if (ret) {
    m_errno = -2;
    m_errmsg = "read sequence decimal column error for sequence [";
    m_errmsg += m_seq->db() + "." + m_seq->name() + "]";
  }
  return ret;
}
/* will automatically release records lock */
bool Gdb_seq_persist_front_end::update_persist_record(
    const my_decimal &currval) {
  DBUG_TRACE;
  auto sql = build_update_sql(m_seq, currval);
  if (m_cmd_service.execute_sql(sql)) {
    auto &cb_data = m_cmd_service.get_cb_data();
    m_errno = cb_data.error_no();
    m_errmsg = cb_data.error_msg();
    release_rec_lock(false);
    return true;
  } else
    return release_rec_lock(true);
}
bool Gdb_seq_persist_front_end::release_rec_lock(bool must_commit) {
  DBUG_TRACE;
  if (must_commit) {
    if (m_cmd_service.execute_sql(STRING_WITH_LEN("COMMIT"))) {
      m_cmd_service.close_session();
      return true;
    }
  } else {
    if (m_cmd_service.execute_sql(STRING_WITH_LEN("ROLLBACK")))
      m_cmd_service.close_session();
  }
  return false;
}

bool Gdb_seq_persist_front_end::close() {
  DBUG_TRACE;
  m_cmd_service.close_session();
  return false;
}
