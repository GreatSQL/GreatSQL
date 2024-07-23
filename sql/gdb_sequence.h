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

#ifndef GDB_SEQUENCE_INCLUDED
#define GDB_SEQUENCE_INCLUDED

#include <stddef.h>

#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

// #include "lex_string.h"
#include "my_sqlcommand.h"
#include "mysql/components/services/bits/mysql_rwlock_bits.h"
#include "sql/sequence_option.h"
#include "sql/sql_cmd.h"  // Sql_cmd

class THD;
struct TABLE;
class Gdb_seq_persist_handler;
class Gdb_sequence_entity;

using sequences_cache_map =
    std::unordered_map<std::string, std::shared_ptr<Gdb_sequence_entity>>;
#ifdef HAVE_PSI_INTERFACE
#endif  // HAVE_PSI_INTERFAC

const sequences_cache_map *global_sequences_cache();

/* cache handlers */
bool sequences_init(bool dont_read_server_table);
bool sequences_load(THD *thd);
void sequences_close();
void sequences_free(bool end = false);

class Gdb_sequences_lock_guard {
 public:
  Gdb_sequences_lock_guard(const Gdb_sequences_lock_guard &) = delete;
  Gdb_sequences_lock_guard(bool readonly = false);
  ~Gdb_sequences_lock_guard();
};

class Gdb_sequence_entity {
 private:
  static std::atomic<uint64> volatile_id_generator;

 public:
  enum seq_status {
    SEQ_STATUS_CLOSED = 0,
    SEQ_STATUS_UNREADABLE,  // "OPENED", but can't read value
    SEQ_STATUS_READY,
  };
  /* used for load to cache (used for parser only) */
  Gdb_sequence_entity(const std::string &db, const std::string &name,
                      const Sequence_option *seq_option)
      : m_volatile_id(volatile_id_generator.fetch_add(1)),
        m_db(db),
        m_name(name),
        m_status(SEQ_STATUS_CLOSED),  // init as closed
        m_cached_num(0),
        m_persist_handler(nullptr),
        m_metadata_version(0) {
    m_cur = gdb_seqval_invalid;
    m_last_persist_val = gdb_seqval_invalid;
    m_option = *seq_option;
  }
  /* used for create new */
  Gdb_sequence_entity(const std::string &db, const std::string &name,
                      const Sequence_option *seq_option,
                      uint64_t metadata_version)
      : m_volatile_id(volatile_id_generator.fetch_add(1)),
        m_db(db),
        m_name(name),
        m_status(SEQ_STATUS_UNREADABLE),  // init as opened, but unreadable
        m_cached_num(0),
        m_persist_handler(nullptr),
        m_metadata_version(metadata_version) {
    assert(seq_option != nullptr);
    m_cur = gdb_seqval_invalid;
    m_last_persist_val = gdb_seqval_invalid;
    m_option = *seq_option;
    setup_persist_handler();
  }

  ~Gdb_sequence_entity();

  inline std::string key() {
    // same value with calc_seq_key
    return m_db + "." + m_name;
  }
  inline std::string db() { return m_db; }
  inline std::string name() { return m_name; }
  inline Sequence_option get_options() { return m_option; }
  bool refresh_start_with();
  inline void set_currval(const my_decimal &decimal) { m_cur = decimal; }
  const inline my_decimal &get_currval() const { return m_cur; }
  inline uint64_t metadata_version() { return m_metadata_version; }
  inline void set_metadata_version(uint64_t version) {
    m_metadata_version = version;
  }
  inline void evict_cached_numbers() { m_cached_num = 0; }  // force reallocate

  /* read value from sequence
       @read_next [in]  false:read currval, true:read nextval
       @val [in/out]    output the value that read from sequence
   */
  bool read_val(bool read_next, my_decimal &val);
  /* used for show sequence, calc the "start_with" */
  bool do_refresh_start_with_option(my_decimal &val);
  /* insert records into persist table when create sequence */
  bool create_persist_record(const my_decimal &cur);
  /* delete records from persist table when drop sequence */
  bool delete_persist_record(bool need_commit);
  int get_persist_handler_errno();
  void close_seq();  // close/release session attached on sequence entity

  bool get_persist_currval(my_decimal &currval);
  bool release_record_lock(bool must_commit);

  /* update cache increment by/minvalue/maxvalue/... vlaue to newest */
  void update_cache_sequence_attributes(const Sequence_option *new_option);

  bool is_opened() { return m_status.load() > SEQ_STATUS_CLOSED; }

  bool open_sequence();

 private:
  bool setup_persist_handler();

  /* load sequence currval from persist table */
  bool init_sequence();

  bool open_and_init();

  /* allocate numbers from persist table, also do boundary checking */
  bool allocate_numbers(uint desires, bool &start_or_restart);

 public:
  inline uint64 volatile_id() const { return m_volatile_id; }

 private:
  uint64 m_volatile_id;
  std::mutex m_mutex;
  std::string m_db;
  std::string m_name;  // normalized seq_name
  my_decimal m_cur;
  my_decimal m_last_persist_val;  // used for calc "start_with" in show seq.
  std::atomic<seq_status> m_status;
  std::atomic<uint> m_cached_num;
  Sequence_option m_option;
  Gdb_seq_persist_handler *m_persist_handler;
  std::atomic<uint64_t> m_metadata_version;

  friend void sequences_close();
};

std::string normalize_seq_name(const char *seq_name);
/* calc sequence's key in cache
   NOTE: need call normalize_seq_name() before it */
std::string calc_seq_key(const char *db_name, const char *seq_name);

/* used for parser, check whether a Item_field refers to a sequence.
   @return  metadata version */
uint64_t has_sequence_def(THD *thd, const char *db, const char *seq_name);

/*
  used for parser, check whether a Item_field refers to a sequence.
  @return   Gdb_sequence_entity*
*/
std::shared_ptr<Gdb_sequence_entity> find_sequence_def(THD *thd, const char *db,
                                                       const char *seq_name);

/* acquire MDL lock on sequence, called in "setup_sequence_func()" */
bool lock_sequence(THD *thd, const char *db, const char *name, bool ddl);
std::shared_ptr<Gdb_sequence_entity> get_sequence_def(const char *db,
                                                      const char *name);
/* check and build create sequence string, used for "show create sequence" */
bool build_sequence_create_str(String &buffer, const char *db,
                               const char *seq_name);

/* get sequence map that need migrate backend persist record */
typedef bool (*need_migrate_callback_t)(Gdb_sequence_entity *entity,
                                        uint64_t &dst_shard_id,
                                        std::vector<uint64_t> &shard_ids);
bool get_migrate_sequences(
    std::map<std::shared_ptr<Gdb_sequence_entity>, uint64_t> &migrate_seqs,
    need_migrate_callback_t func, std::vector<uint64_t> &shard_ids);

class Sql_cmd_common_sequence : public Sql_cmd {
 protected:
  TABLE *table;

  /* allocated by memroot, no need free */
  const LEX_CSTRING *m_db;  // If without database name this values is ""
  const LEX_CSTRING *m_seq_name;

  Sql_cmd_common_sequence() = delete;
  Sql_cmd_common_sequence(const LEX_CSTRING *db, const LEX_CSTRING *seq_name)
      : table(nullptr), m_db(db), m_seq_name(seq_name) {}

  ~Sql_cmd_common_sequence() override {}

  /**
     open the mysql.greatdb_sequences table.
  */
  bool lock_and_open_table(THD *thd);
};

class Sql_cmd_create_sequence final : public Sql_cmd_common_sequence {
 public:
  Sql_cmd_create_sequence(const LEX_CSTRING *db, const LEX_CSTRING *seq_name,
                          Sequence_option *seq_option)
      : Sql_cmd_common_sequence(db, seq_name), m_seq_option(seq_option) {}

  enum_sql_command sql_command_code() const override {
    return SQLCOM_CREATE_SEQUENCE;
  }

  bool execute(THD *thd) override;

 private:
  Sequence_option *m_seq_option;
};

class Sql_cmd_alter_sequence final : public Sql_cmd_common_sequence {
 public:
  Sql_cmd_alter_sequence(const LEX_CSTRING *db, const LEX_CSTRING *seq_name,
                         Sequence_option *seq_option)
      : Sql_cmd_common_sequence(db, seq_name), m_seq_option(seq_option) {}

  enum_sql_command sql_command_code() const override {
    return SQLCOM_ALTER_SEQUENCE;
  }

  bool execute(THD *thd) override;

 private:
  Sequence_option *m_seq_option;
};

class Sql_cmd_drop_sequence final : public Sql_cmd_common_sequence {
 public:
  Sql_cmd_drop_sequence(const LEX_CSTRING *db, const LEX_CSTRING *seq_name)
      : Sql_cmd_common_sequence(db, seq_name) {}

  enum_sql_command sql_command_code() const override {
    return SQLCOM_DROP_SEQUENCE;
  }

  bool execute(THD *thd) override;
};

#endif /* GDB_SEQUENCE_INCLUDED */
