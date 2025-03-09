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

#include "sql/sql_synonym.h"
#include <atomic>

#include "dd/types/abstract_table.h"
#include "my_alloc.h"
#include "my_base.h"
#include "my_compiler.h"
#include "my_dbug.h"
#include "my_loglevel.h"
#include "my_sqlcommand.h"
#include "mysql/components/service.h"
#include "mysql/components/services/bits/mysql_rwlock_bits.h"
#include "mysql/components/services/bits/psi_rwlock_bits.h"
#include "mysql/components/services/log_builtins.h"
#include "mysql/components/services/mysql_admin_session.h"
#include "mysql/psi/mysql_rwlock.h"
#include "mysqld_error.h"
#include "sql/dd/dd_table.h"
#include "sql/gdb_common.h"
#include "sql/key.h"
#include "sql/lock.h"
#include "sql/mdl.h"
#include "sql/sql_backup_lock.h"
#include "sql/sql_base.h"
#include "sql/sql_class.h"
#include "sql/sql_executor.h"
#include "sql/sql_lex.h"
#include "sql/sql_table.h"
#include "sql/strfunc.h"
#include "sql/table.h"
#include "sql/thd_raii.h"
#include "sql/transaction.h"
#include "sql_show.h"

#define DB_OBJECT_SYNONYMS_TABLE "db_object_synonyms"

#define DB_OBJECT_SYNONYMS_TABLE_KEY_PRIMARY 0
#define DB_OBJECT_SYNONYMS_MAX_KEY_LEN 512 + 4  // 2col*64*4(utf8mb4) + paddings

#define SYNONYM_TARGET_TYPE_TABLE "TABLE"
#define SYNONYM_TARGET_TYPE_SYNONYM "SYNONYM"
#define PUBLIC_SYNONYM_SCHEMA ""

enum enum_synonyms_table_field {
  DB_OBJECT_SYNONYMS_FIELD_SCHEMA = 0,
  DB_OBJECT_SYNONYMS_FIELD_NAME,
  DB_OBJECT_SYNONYMS_FIELD_TARGET_TYPE,
  DB_OBJECT_SYNONYMS_FIELD_TARGET_SCHEMA,
  DB_OBJECT_SYNONYMS_FIELD_TARGET_NAME,
  DB_OBJECT_SYNONYMS_FIELD_DB_LINK,
};

static inline bool _is_empty_str(const char *str) {
  return str == nullptr || strlen(str) == 0;
}

static std::string _fix_name(const char *name) {
  DBUG_ENTER("_fix_name");

  if (name == nullptr) {
    DBUG_RETURN("");
  }
  if (lower_case_table_names) {
    char lowercase_name[FN_REFLEN + 1];
    // TODO(hongnan): memory leak
    strcpy(lowercase_name, name);
    my_casedn_str(system_charset_info, lowercase_name);
    DBUG_RETURN(lowercase_name);
  } else {
    DBUG_RETURN(name);
  }
}  // std::string _fix_name(const char *name)

static bool _lock_synonym(THD *thd, const char *db, const char *seq_name,
                          bool ddl) {
  DBUG_ENTER("_lock_synonym");

  bool is_empty_db = _is_empty_str(db);
  std::string fixed_name = _fix_name(seq_name);
  std::string fixed_db = _fix_name(db);

  MDL_request_list mdl_requests;
  MDL_request global_request;
  MDL_request backup_request;
  MDL_request schema_request;
  MDL_request mdl_request;

  enum_mdl_type mdl_type =
      ddl ? MDL_EXCLUSIVE : MDL_SHARED /*NOTE: diff with lock_sequence*/;

  MDL_REQUEST_INIT(&mdl_request, MDL_key::TABLE, fixed_db.c_str(),
                   fixed_name.c_str(), mdl_type, MDL_TRANSACTION);
  mdl_requests.push_front(&mdl_request);
  if (!is_empty_db) {
    MDL_REQUEST_INIT(&schema_request, MDL_key::SCHEMA, fixed_db.c_str(),
                     PUBLIC_SYNONYM_SCHEMA, MDL_INTENTION_EXCLUSIVE,
                     MDL_TRANSACTION);
    mdl_requests.push_front(&schema_request);
  }
  if (ddl) {
    MDL_REQUEST_INIT(&backup_request, MDL_key::BACKUP_LOCK, "", "",
                     MDL_INTENTION_EXCLUSIVE, MDL_STATEMENT);
    mdl_requests.push_front(&backup_request);
    MDL_REQUEST_INIT(&global_request, MDL_key::GLOBAL, "", "",
                     MDL_INTENTION_EXCLUSIVE, MDL_STATEMENT);
    mdl_requests.push_front(&global_request);
  }
  if (thd->mdl_context.acquire_locks(&mdl_requests,
                                     thd->variables.lock_wait_timeout)) {
    DBUG_RETURN(true);
  } else {
    DBUG_RETURN(false);
  }
}  // bool _lock_synonym(THD *thd, const char *db, const char *seq_name, bool
   // ddl)

class _Mdl_checkpoint {
 public:
  _Mdl_checkpoint(THD *thd) : _thd(thd) {
    DBUG_ENTER("_Mdl_checkpoint::_Mdl_checkpoint");
    this->_mdl_savepoint = thd->mdl_context.mdl_savepoint();
    DBUG_VOID_RETURN;
  }
  ~_Mdl_checkpoint() { this->rollback(); }

 public:
  bool lock(const char *db, const char *obj) {
    DBUG_ENTER("_Mdl_checkpoint::lock");
    bool ret = false;

    if (object_mdl_req.ticket) {
      this->rollback();
    }

    auto lock_wait = this->_thd->variables.lock_wait_timeout;

    MDL_REQUEST_INIT(&this->object_mdl_req, MDL_key::enum_mdl_namespace::TABLE,
                     db, obj, enum_mdl_type::MDL_SHARED,
                     enum_mdl_duration::MDL_STATEMENT);

    Open_table_context ot_ctx(this->_thd,
                              0 /* we don't need any specific flags*/);
    MDL_deadlock_handler mdl_deadlock_handler(&ot_ctx);
    this->_thd->push_internal_handler(&mdl_deadlock_handler);
    this->_thd->mdl_context.set_force_dml_deadlock_weight(
        ot_ctx.can_back_off());
    ret =
        this->_thd->mdl_context.acquire_lock(&this->object_mdl_req, lock_wait);
    this->_thd->mdl_context.set_force_dml_deadlock_weight(false);
    this->_thd->pop_internal_handler();
    if (ret) {
      this->rollback();
    }

    DBUG_RETURN(ret);
  }

 private:
  void rollback() {
    if (_thd != nullptr) {
      this->_thd->mdl_context.rollback_to_savepoint(this->_mdl_savepoint);
    }
  }

 private:
  THD *_thd;
  MDL_request object_mdl_req;
  MDL_savepoint _mdl_savepoint;
};

db_object_synonyms_cache_map Db_object_synonyms_cache::_cache;

#ifdef HAVE_PSI_INTERFACE
mysql_rwlock_t Db_object_synonyms_cache::_THR_LOCK_synonyms_cache;
PSI_rwlock_key Db_object_synonyms_cache::_key_rwlock_THR_LOCK_synonyms_cache;
PSI_rwlock_info Db_object_synonyms_cache::_synonyms_cache_psi_info[] = {
    {&Db_object_synonyms_cache::_key_rwlock_THR_LOCK_synonyms_cache,
     "db_object_synonyms_cache", PSI_FLAG_SINGLETON, 0, PSI_DOCUMENT_ME},
};
#endif

bool Db_object_synonyms_cache::free(bool end) {
  DBUG_ENTER("Db_object_synonyms_cache::free");

  Db_object_synonyms_cache::_cache.clear();

  if (end) {
    mysql_rwlock_destroy(&Db_object_synonyms_cache::_THR_LOCK_synonyms_cache);
  }

  DBUG_RETURN(false);
}  // void Db_object_synonyms_cache::free(bool end)

bool Db_object_synonyms_cache::init(bool dont_read_server_table) {
  DBUG_ENTER("Db_object_synonyms_cache::init");
  bool ret = false;

#ifdef HAVE_PSI_INTERFACE
  /**
   * @brief Initialize synonyms cache PSI key
   */
  {
    const char *category = "sql";
    int count;

    count = static_cast<int>(
        array_elements(Db_object_synonyms_cache::_synonyms_cache_psi_info));
    mysql_rwlock_register(
        category, Db_object_synonyms_cache::_synonyms_cache_psi_info, count);
  }
#endif  // HAVE_PSI_INTERFACE

  if (mysql_rwlock_init(
          Db_object_synonyms_cache::_key_rwlock_THR_LOCK_synonyms_cache,
          &Db_object_synonyms_cache::_THR_LOCK_synonyms_cache)) {
    DBUG_RETURN(true);
  }

  if (!dont_read_server_table) {
    THD *thd;
    if ((thd = new THD)) {
      thd->thread_stack = (char *)&thd;
      thd->store_globals();

      /**
       * @brief Load synonyms
       */
      {
        Db_object_synonyms_cache::_Lock_guard lock_guard(/*readonly=*/false);

        Table_ref tables("mysql", DB_OBJECT_SYNONYMS_TABLE, TL_READ);
        if (!open_trans_system_tables_for_read(thd, &tables)) {
          // Load synonyms from the table
          _cache.clear();

          {
            unique_ptr_destroy_only<RowIterator> iter = init_table_iterator(
                thd, tables.table, /* ignore_not_found_rows */ false,
                /* count_examined_rows */ false);
            if (iter) {
              while (!(iter->Read())) {
                if ((ret = Db_object_synonyms_cache::load_from_table(
                         tables.table))) {
                  LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
                         "load synonym from table failed.");
                  break;
                }
              }
            }
          }
          close_trans_system_tables(thd);
        } else {
          if (thd->get_stmt_da()->is_error()) {
            LogErr(ERROR_LEVEL, ER_CANT_OPEN_AND_LOCK_PRIVILEGE_TABLES,
                   DB_OBJECT_SYNONYMS_TABLE);
          }
          Db_object_synonyms_cache::free();
        }
      }

      delete thd;
    }
    DBUG_RETURN(ret);
  } else {
    DBUG_RETURN(false);
  }

}  // bool Db_object_synonyms_cache::init(bool dont_read_server_table)

bool Db_object_synonyms_cache::create(
    THD *thd, const std::string &db, const std::string &name,
    std::shared_ptr<Db_object_synonym_entity> entity, bool replace) {
  DBUG_ENTER("Db_object_synonyms_cache::create");
  std::string key = Db_object_synonym_entity::make_key(db, name);

  Db_object_synonyms_cache::_Lock_guard lock_guard(/*readonly=*/false);

  const auto it = _cache.find(key);
  if (it != _cache.end()) {
    if (!replace) {
      my_error(ER_SYNONYM_ALREADY_EXISTS, MYF(0), key.c_str());
      DBUG_RETURN(true);
    }
  }

  /**
   * @brief Write to table
   */
  int error = 0;
  {
    TABLE *table = Db_object_synonyms_cache::lock_and_open_table(thd);
    if (!table) {
      DBUG_RETURN(true);
    }

    table->use_all_columns();
    empty_record(table);

    table->field[DB_OBJECT_SYNONYMS_FIELD_SCHEMA]->store(
        entity->schema().c_str(), entity->schema().length(),
        system_charset_info);
    table->field[DB_OBJECT_SYNONYMS_FIELD_NAME]->store(
        entity->name().c_str(), entity->name().length(), system_charset_info);
    table->field[DB_OBJECT_SYNONYMS_FIELD_TARGET_TYPE]->store(
        entity->target_type().c_str(), entity->target_type().length(),
        system_charset_info);
    table->field[DB_OBJECT_SYNONYMS_FIELD_TARGET_SCHEMA]->store(
        entity->target_schema().c_str(), entity->target_schema().length(),
        system_charset_info);
    table->field[DB_OBJECT_SYNONYMS_FIELD_TARGET_NAME]->store(
        entity->target_name().c_str(), entity->target_name().length(),
        system_charset_info);
    table->field[DB_OBJECT_SYNONYMS_FIELD_DB_LINK]->store(
        entity->dblink().c_str(), entity->dblink().length(),
        system_charset_info);

    Disable_binlog_guard binlog_guard(thd);
    if ((error = table->file->ha_write_row(table->record[0]))) {
      if (error == ER_DUP_ENTRY || error == ER_DUP_KEY) {
        my_error(ER_SYNONYM_ALREADY_EXISTS, MYF(0), key.c_str());
        DBUG_RETURN(true);
      }

      table->file->print_error(error, MYF(0));
    }

  }  // Write to table

  if (error || (error = write_bin_log(thd, false, thd->query().str,
                                      thd->query().length))) {
    trans_rollback_stmt(thd);
  } else {
    trans_commit_stmt(thd);
    if (it != _cache.end()) {
      _cache.erase(it);
    }
    _cache.emplace(key, entity);
  }
  close_mysql_tables(thd);

  if (error == 0 && !thd->killed) {
    my_ok(thd, 1);
    DBUG_RETURN(false);
  } else {
    DBUG_RETURN(true);
  }
}  // bool Db_object_synonyms_cache::create(const std::string &db, const
   // std::string &name, std::shared_ptr<Db_object_synonym_entity> entity)

bool Db_object_synonyms_cache::alter(
    THD *thd [[maybe_unused]], const std::string &db, const std::string &name,
    std::shared_ptr<Db_object_synonym_entity> entity) {
  DBUG_ENTER("Db_object_synonyms_cache::alter");
  std::string key = Db_object_synonym_entity::make_key(db, name);

  Db_object_synonyms_cache::_Lock_guard lock_guard(/*readonly=*/false);
  _cache[key] = entity;

  DBUG_RETURN(false);
}  // bool Db_object_synonyms_cache::alter(const std::string &key,
   // std::shared_ptr<Db_object_synonym_entity> entity)

const std::shared_ptr<Db_object_synonym_entity> Db_object_synonyms_cache::find(
    THD *thd [[maybe_unused]], const std::string &db, const std::string &name) {
  DBUG_ENTER("Db_object_synonyms_cache::find");
  std::string key = Db_object_synonym_entity::make_key(db, name);

  Db_object_synonyms_cache::_Lock_guard lock_guard(/*readonly=*/true);
  const auto it = _cache.find(key);
  if (it != _cache.end()) {
    thd->lex->set_has_synonym();
    DBUG_RETURN(it->second);
  } else {
    DBUG_RETURN(nullptr);
  }
}  // bool Db_object_synonyms_cache::find(const std::string &key)

bool Db_object_synonyms_cache::drop(THD *thd [[maybe_unused]],
                                    const std::string &db,
                                    const std::string &name) {
  DBUG_ENTER("Db_object_synonyms_cache::drop");
  std::string key = Db_object_synonym_entity::make_key(db, name);

  Db_object_synonyms_cache::_Lock_guard lock_guard(/*readonly=*/false);

  auto it = _cache.find(key);
  if (it == _cache.end()) {
    std::string synm = gdb_string_add_quote(db, IDENT_QUOTE_CHAR) + "." +
                       gdb_string_add_quote(name, IDENT_QUOTE_CHAR);
    my_error(ER_SYNONYM_DOES_NOT_EXIST, MYF(0), synm.c_str());
    DBUG_RETURN(true);
  }

  /**
   * @brief Delete from table
   */
  int error = 0;
  {
    TABLE *table = Db_object_synonyms_cache::lock_and_open_table(thd);
    if (!table) {
      DBUG_RETURN(true);
    }

    table->use_all_columns();
    table->field[DB_OBJECT_SYNONYMS_FIELD_SCHEMA]->store(
        db.c_str(), db.length(), system_charset_info);
    table->field[DB_OBJECT_SYNONYMS_FIELD_NAME]->store(
        name.c_str(), name.length(), system_charset_info);
    uchar key_buf[DB_OBJECT_SYNONYMS_MAX_KEY_LEN];
    assert(DB_OBJECT_SYNONYMS_MAX_KEY_LEN >= table->key_info->key_length);
    key_copy(key_buf, table->record[0], table->key_info,
             table->key_info->key_length);

    error = table->file->ha_index_read_idx_map(
        table->record[0], DB_OBJECT_SYNONYMS_TABLE_KEY_PRIMARY, key_buf,
        HA_WHOLE_KEY, HA_READ_KEY_EXACT);
    if (error) {
      table->file->print_error(error, MYF(0));
    }

    Disable_binlog_guard binlog_guard(thd);
    if ((error = table->file->ha_delete_row(table->record[0]))) {
      table->file->print_error(error, MYF(0));
    }
  }  // Delete from table

  if (error || (error = write_bin_log(thd, false, thd->query().str,
                                      thd->query().length))) {
    trans_rollback_stmt(thd);
  } else {
    trans_commit_stmt(thd);
    _cache.erase(it);
  }

  close_mysql_tables(thd);

  if (error == 0 && !thd->killed) {
    my_ok(thd, 1);
    DBUG_RETURN(false);
  } else {
    DBUG_RETURN(true);
  }
}  // bool Db_object_synonyms_cache::drop(const std::string &key)

bool Db_object_synonyms_cache::load_from_table(TABLE *table) {
  DBUG_ENTER("Db_object_synonyms_cache::load_from_table");

  char *ptr;

  MEM_ROOT mem{PSI_NOT_INSTRUMENTED, 512};
  table->use_all_columns();

  ptr = get_field(&mem, table->field[DB_OBJECT_SYNONYMS_FIELD_SCHEMA]);
  std::string db = ptr ? std::string(ptr) : PUBLIC_SYNONYM_SCHEMA;
  ptr = get_field(&mem, table->field[DB_OBJECT_SYNONYMS_FIELD_NAME]);
  assert(ptr);
  std::string name = std::string(ptr);
  ptr = get_field(&mem, table->field[DB_OBJECT_SYNONYMS_FIELD_TARGET_TYPE]);
  assert(ptr);
  std::string target_type = std::string(ptr);
  ptr = get_field(&mem, table->field[DB_OBJECT_SYNONYMS_FIELD_TARGET_SCHEMA]);
  std::string target_schema = ptr ? std::string(ptr) : PUBLIC_SYNONYM_SCHEMA;
  ptr = get_field(&mem, table->field[DB_OBJECT_SYNONYMS_FIELD_TARGET_NAME]);
  assert(ptr);
  std::string target_name = std::string(ptr);
  ptr = get_field(&mem, table->field[DB_OBJECT_SYNONYMS_FIELD_DB_LINK]);
  std::string db_link = ptr ? std::string(ptr) : "";

  auto synonym_entity = std::make_shared<Db_object_synonym_entity>(
      db, name, target_schema, target_name, target_type, db_link);

  if (!synonym_entity) {
    LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG, "malloc for synonym entity failed.");
    DBUG_RETURN(true);
  }

  _cache.emplace(synonym_entity->key(), synonym_entity);

  DBUG_RETURN(false);
}  // bool Db_object_synonyms_cache::load_from_table(TABLE *table)

TABLE *Db_object_synonyms_cache::lock_and_open_table(THD *thd) {
  DBUG_ENTER("Db_object_synonyms_cache::lock_and_open_table");

  if (acquire_shared_backup_lock(thd, thd->variables.lock_wait_timeout)) {
    DBUG_RETURN(nullptr);
  }

  Table_ref tables("mysql", DB_OBJECT_SYNONYMS_TABLE, TL_WRITE);
  TABLE *table = open_ltable(thd, &tables, TL_WRITE, MYSQL_LOCK_IGNORE_TIMEOUT);
  if (!table) {
    DBUG_RETURN(nullptr);
  }

  DBUG_RETURN(table);
}  // bool Db_object_synonyms_cache::lock_and_open_table(THD *thd)

Sql_cmd_create_synonym::Sql_cmd_create_synonym(Table_ident *synonym_ident,
                                               bool or_replace, bool is_public,
                                               Table_ident *target)
    : m_ident(synonym_ident),
      m_or_replace(or_replace),
      m_target(target),
      m_is_public(is_public) {}

bool Sql_cmd_create_synonym::prepare(THD *thd) {
  DBUG_ENTER("Sql_cmd_create_synonym::prepare");

  /**
   * @name Check if synonym name is valid
   * @{ */
  if (this->m_is_public) {
    // Public synonyms must not have a db name
    if (!_is_empty_str(this->m_ident->db.str)) {
      my_error(ER_SYNONYM_INVALID_PUBLIC_SYNONYM_NAME, MYF(0),
               this->m_ident->db.str, this->m_ident->table.str);
      DBUG_RETURN(true);
    }
  } else {
    // Private synonyms must have a db name
    if (_is_empty_str(this->m_ident->db.str)) {
      if (thd->db().str != nullptr) {
        this->m_ident->db = thd->db();
      } else {
        my_error(ER_NO_DB_ERROR, MYF(0));
        DBUG_RETURN(true);
      }
    }
  }

  std::string fixed_target_name = _fix_name(this->m_target->table.str);
  std::string fixed_target_db = _fix_name(this->m_target->db.str);
  std::shared_ptr<Db_object_synonym_entity> synonym = nullptr;
  synonym =
      Db_object_synonyms_cache::find(thd, fixed_target_db, fixed_target_name);
  if (!synonym) {
    synonym = Db_object_synonyms_cache::find(thd, PUBLIC_SYNONYM_SCHEMA,
                                             fixed_target_name);
    if (!synonym) {
      // target is not a synonym, use current db as target db
      if (_is_empty_str(this->m_target->db.str)) {
        if (thd->db().str != nullptr) {
          this->m_target->db = thd->db();
        } else {
          my_error(ER_NO_DB_ERROR, MYF(0));
          DBUG_RETURN(true);
        }
      }
    }
  }

  if (my_strcasecmp(system_charset_info, m_ident->table.str,
                    m_target->table.str) == 0) {
    if (!this->m_is_public &&
        my_strcasecmp(system_charset_info, m_ident->db.str, m_target->db.str) ==
            0) {
      my_error(ER_SYNONYM_RECURSIVE, MYF(0), this->m_ident->db.str,
               this->m_ident->table.str);
      DBUG_RETURN(true);
    }
  }
  /**  @} Check if synonym name is valid */

  /**
   * @name Check privileges
   * @{ */
  /**  @} Check privileges */

  this->set_prepared();
  DBUG_RETURN(false);
}  // bool Sql_cmd_create_synonym::prepare(THD *thd)

bool Sql_cmd_create_synonym::execute(THD *thd) {
  DBUG_ENTER("Sql_cmd_create_synonym::execute");
  if (this->prepare(thd)) {
    DBUG_RETURN(true);
  }

  std::string name = _fix_name(this->m_ident->table.str);
  std::string db = _fix_name(this->m_ident->db.str);
  std::string target_name = _fix_name(this->m_target->table.str);
  std::string target_db = _fix_name(this->m_target->db.str);
  std::string dblink;
  if (!_is_empty_str(this->m_ident->dblink.str)) {
    my_error(ER_SYNONYM_UNSUPPORTED_OPERATION, MYF(0),
             "Creating synonym on dblink");
    DBUG_RETURN(true);
  }
  if (!_is_empty_str(this->m_target->dblink.str)) {
    my_error(ER_SYNONYM_UNSUPPORTED_OPERATION, MYF(0),
             "Creating synonym for target");
    DBUG_RETURN(true);

    // TODO(hongnan): dblink should be resolved (create new temp table), or
    // another session won't be able to use the synonym
    dblink =
        std::string(this->m_target->dblink.str, this->m_target->dblink.length);
    // target_name = target_name.substr(0, target_name.rfind('@'));
    dblink = _fix_name(dblink.c_str());

    target_db = "";
    // dblink = target_name;
  }

  if (_lock_synonym(thd, this->m_ident->db.str, this->m_ident->table.str,
                    true)) {
    DBUG_RETURN(true);
  }

  bool physical_table_view_exists = false;
  if (dd::table_exists(thd->dd_client(), db.c_str(), name.c_str(),
                       &physical_table_view_exists)) {
    DBUG_RETURN(true);
  } else {
    if (physical_table_view_exists) {
      my_error(ER_SYNONYM_DUPLICATE_NAME, MYF(0), db.c_str(), name.c_str());
      DBUG_RETURN(true);
    } else {
      /* Loopup temporary tables */
      for (TABLE *table = thd->temporary_tables; table; table = table->next) {
        if (my_strcasecmp(system_charset_info, table->s->table_name.str,
                          name.c_str()) == 0) {
          my_error(ER_SYNONYM_DUPLICATE_NAME, MYF(0), db.c_str(), name.c_str());
          DBUG_RETURN(true);
        }
      }
    }
  }

  std::string target_type = SYNONYM_TARGET_TYPE_TABLE;
  if (Db_object_synonyms_cache::find(thd, target_db, target_name)) {
    target_type = SYNONYM_TARGET_TYPE_SYNONYM;
  }
  auto synonym_entity = std::make_shared<Db_object_synonym_entity>(
      db, name, target_db, target_name, target_type, dblink);
  if (!synonym_entity) {
    LogErr(ERROR_LEVEL, ER_LOG_PRINTF_MSG, "malloc memory failed for synonym");
    DBUG_RETURN(true);
  }

  if (Db_object_synonyms_cache::create(thd, db, name, synonym_entity,
                                       this->m_or_replace)) {
    DBUG_RETURN(true);
  }

  DBUG_RETURN(false);
}  // bool Sql_cmd_create_synonym::execute(THD *thd)

Sql_cmd_drop_synonym::Sql_cmd_drop_synonym(Table_ident *synonym_ident,
                                           bool is_public)
    : m_ident(synonym_ident), m_is_public(is_public) {}

bool Sql_cmd_drop_synonym::prepare(THD *thd) {
  DBUG_ENTER("Sql_cmd_drop_synonym::prepare");

  if (this->m_is_public) {
    if (!_is_empty_str(this->m_ident->db.str)) {
      my_error(ER_SYNONYM_INVALID_PUBLIC_SYNONYM_NAME, MYF(0),
               this->m_ident->db.str, this->m_ident->table.str);
      DBUG_RETURN(true);
    }
  } else {
    if (_is_empty_str(this->m_ident->db.str)) {
      if (thd->db().str != nullptr) {
        this->m_ident->db = thd->db();
      } else {
        my_error(ER_NO_DB_ERROR, MYF(0));
        DBUG_RETURN(true);
      }
    }
  }

  this->set_prepared();
  DBUG_RETURN(false);

}  // bool Sql_cmd_drop_synonym::prepare(THD *thd)

bool Sql_cmd_drop_synonym::execute(THD *thd) {
  DBUG_ENTER("Sql_cmd_drop_synonym::execute");

  if (this->prepare(thd)) {
    DBUG_RETURN(true);
  }

  std::string name = _fix_name(this->m_ident->table.str);
  std::string db = _fix_name(this->m_ident->db.str);

  if (_lock_synonym(thd, db.c_str(), name.c_str(), true)) {
    DBUG_RETURN(true);
  }

  if (Db_object_synonyms_cache::drop(thd, db, name)) {
    DBUG_RETURN(true);
  }

  DBUG_RETURN(false);
}  // bool Sql_cmd_drop_synonym::execute(THD *thd)

Sql_cmd_alter_synonym::Sql_cmd_alter_synonym(Table_ident *synonym_ident,
                                             bool is_public)
    : m_ident(synonym_ident), m_is_public(is_public) {}

bool Sql_cmd_alter_synonym::prepare(THD *thd [[maybe_unused]]) {
  DBUG_ENTER("Sql_cmd_alter_synonym::prepare");
  my_error(ER_SYNONYM_UNSUPPORTED_OPERATION, MYF(0), "ALTER");
  DBUG_RETURN(true);
}

bool Sql_cmd_alter_synonym::execute(THD *thd [[maybe_unused]]) {
  DBUG_ENTER("Sql_cmd_alter_synonym::execute");
  my_error(ER_SYNONYM_UNSUPPORTED_OPERATION, MYF(0), "ALTER");
  DBUG_RETURN(true);
}

Db_object_synonyms_cache::_Lock_guard::_Lock_guard(bool readonly) {
  if (readonly) {
    mysql_rwlock_rdlock(&Db_object_synonyms_cache::_THR_LOCK_synonyms_cache);
  } else {
    mysql_rwlock_wrlock(&Db_object_synonyms_cache::_THR_LOCK_synonyms_cache);
  }
}  // Db_objct_synonyms_cache_lock_guard::Db_objct_synonyms_cache_lock_guard()

Db_object_synonyms_cache::_Lock_guard::~_Lock_guard() {
  mysql_rwlock_unlock(&Db_object_synonyms_cache::_THR_LOCK_synonyms_cache);
}  // Db_objct_synonyms_cache_lock_guard::~Db_objct_synonyms_cache_lock_guard()

bool skip_synm_trans_for_command(THD *thd, bool is_prepare) {
  if (opt_initialize || !synonym_translation_enabled) {
    return true;  // skip translate when initialize or switch=off
  }

  switch (thd->lex->sql_command) {
    case SQLCOM_SELECT:
    case SQLCOM_UPDATE:
    case SQLCOM_UPDATE_MULTI:
    case SQLCOM_INSERT:
    case SQLCOM_INSERT_SELECT:
    case SQLCOM_DELETE:
    case SQLCOM_TRUNCATE:
    case SQLCOM_SHOW_KEYS:
    case SQLCOM_SHOW_CREATE:
    case SQLCOM_LOAD:
    case SQLCOM_REPLACE:
    case SQLCOM_REPLACE_SELECT:
    case SQLCOM_OPTIMIZE:
    case SQLCOM_CHECK:
    case SQLCOM_FLUSH:
    case SQLCOM_ANALYZE:
    case SQLCOM_PREPARE:
    case SQLCOM_DEALLOCATE_PREPARE:
    case SQLCOM_LOCK_TABLES:
    case SQLCOM_UNLOCK_TABLES:
      break;
    case SQLCOM_EXECUTE:
    case SQLCOM_EXECUTE_IMMEDIATE: {
      if (!is_prepare) return true;  // only translate in prepare stage
    } break;
    default:
      return true;
      break;
  }
  return false;
}
static bool skip_synm_trans_for_tb(Table_ref *tr) {
  std::string name = _fix_name(tr->db);
  std::string db = _fix_name(tr->table_name);

  if (tr->is_derived()) {
    return true;
  }
  if (!_is_empty_str(tr->dblink_name)) {
    return true;
  }

  auto skip_schema_table{[&tr](const char *schema,
                               const char *table = nullptr) {
    if (my_strcasecmp(system_charset_info, schema, tr->db) == 0) {
      if (table) {
        return my_strcasecmp(system_charset_info, table, tr->table_name) == 0;
      } else {
        return true;
      }
    } else {
      return false;
    }
  }};

  return skip_schema_table("", "json_table") ||
         skip_schema_table("", "role_graph") ||
         skip_schema_table("information_schema") ||
         skip_schema_table("mysql") ||
         skip_schema_table("performance_schema") || skip_schema_table("sys") ||
         skip_schema_table("ndbinfo") || skip_schema_table("sys_mac") ||
         skip_schema_table("mtr", "error_log");
}

enum_synm_trans_result translate_synonym_if_any(THD *thd, Table_ref *tr) {
  DBUG_ENTER("translate_synonym_if_any");
  assert(thd != nullptr && tr != nullptr);
  enum_synm_trans_result res = enum_synm_trans_result::SYNM_TRANS_NONE;
  _Mdl_checkpoint mdl_guard(thd);

  // 1. check if skip translation
  if (tr->db_length == 0 && tr->table_name_length == 0) {
    DBUG_RETURN(res);  // other commands without table, such as: show, set.
  }
  if (skip_synm_trans_for_tb(tr)) {
    DBUG_RETURN(res);
  }

  // 2.prepare db and obj names, init vars
  enum_mdl_type tr_mdl_type = tr->mdl_request.type;
  MDL_key::enum_mdl_namespace tr_mdl_namespace =
      tr->mdl_request.key.mdl_namespace();
  enum_mdl_duration tr_mdl_duration = tr->mdl_request.duration;
  std::string orig_db = _fix_name(tr->db);
  std::string orig_table = _fix_name(tr->table_name);
  Table_ident tmp_target(to_lex_cstring(orig_db.c_str()),
                         to_lex_cstring(orig_table.c_str()));

  std::shared_ptr<Db_object_synonym_entity> synonym = nullptr;
  std::shared_ptr<Db_object_synonym_entity> last_synonym = nullptr;
  do {
    // 3.1 check if is synonym
    const char *db = tmp_target.db.str;
    synonym = Db_object_synonyms_cache::find(thd, db, tmp_target.table.str);
    if (last_synonym == nullptr && !tr->is_fqtn && synonym == nullptr) {
      synonym = Db_object_synonyms_cache::find(thd, PUBLIC_SYNONYM_SCHEMA,
                                               tmp_target.table.str);
    }
    if (synonym != nullptr) {
      last_synonym = synonym;
      tmp_target.db = to_lex_cstring(synonym->target_schema().c_str());
      tmp_target.table = to_lex_cstring(synonym->target_name().c_str());
      continue;
    }

    // 3.2 check if is tmp table
    for (TABLE *table = thd->temporary_tables; table; table = table->next) {
      if (my_strcasecmp(system_charset_info, table->s->table_name.str,
                        tmp_target.table.str) == 0) {
        if (last_synonym) {
          DBUG_RETURN(enum_synm_trans_result::SYNM_TRANS_OK);
        } else {
          DBUG_RETURN(enum_synm_trans_result::SYNM_TRANS_NONE);
        }
      }
    }
    // 3.3 check if is or view or tmp_tab.
    tmp_target.db = to_lex_cstring(db);
    if (!thd->mdl_context.owns_equal_or_stronger_lock(
            MDL_key::TABLE, tmp_target.db.str, tmp_target.table.str,
            MDL_SHARED)) {
      if (mdl_guard.lock(tmp_target.db.str, tmp_target.table.str)) {
        DBUG_RETURN(enum_synm_trans_result::SYNM_TRANS_INVALID);
      }
    }
    bool physical_table_view_exists = false;
    if (dd::table_exists(thd->dd_client(), tmp_target.db.str,
                         tmp_target.table.str, &physical_table_view_exists)) {
      DBUG_RETURN(enum_synm_trans_result::SYNM_TRANS_INVALID);
    }

    if (last_synonym) {
      if (physical_table_view_exists) {
        tr->db = tmp_target.db.str;
        tr->db_length = tmp_target.db.length;
        tr->table_name = tmp_target.table.str;
        tr->table_name_length = tmp_target.table.length;
        if (tr->mdl_request.key.length()) {
          MDL_REQUEST_INIT(&tr->mdl_request, tr_mdl_namespace, tr->db,
                           tr->table_name, tr_mdl_type, tr_mdl_duration);
        }
        DBUG_RETURN(enum_synm_trans_result::SYNM_TRANS_OK);  // base table/view
      } else {
        my_error(ER_SYNONYM_TRANS_INVALID, MYF(0), orig_db.c_str(),
                 orig_table.c_str(), last_synonym->key().c_str());
        DBUG_RETURN(
            enum_synm_trans_result::SYNM_TRANS_INVALID);  // base table/view
      }
    } else {
      DBUG_RETURN(enum_synm_trans_result::SYNM_TRANS_NONE);  // base table/view
    }

  } while (1);

  DBUG_RETURN(res);
}

bool build_synonym_create_str(THD *thd, String &buffer, const char *db,
                              const char *synonym, bool is_public) {
  DBUG_ENTER("build_synonym_create_str");

  buffer.length(0);

  if (!is_public) {
    if (_is_empty_str(db)) {
      db = thd->db().str;
    }
  } else {
    if (db != nullptr) {
      if (strlen(db) != 0) {
        my_error(ER_SYNONYM_INVALID_PUBLIC_SYNONYM_NAME, MYF(0), db, synonym);
        DBUG_RETURN(true);
      }
    } else {
      db = PUBLIC_SYNONYM_SCHEMA;
    }
  }

  const std::string fixed_db = _fix_name(db);
  const std::string fixed_synonym = _fix_name(synonym);

  std::shared_ptr<Db_object_synonym_entity> entity = nullptr;

  if (!is_public) {
    entity = Db_object_synonyms_cache::find(thd, fixed_db, fixed_synonym);
  } else {
    entity = Db_object_synonyms_cache::find(thd, PUBLIC_SYNONYM_SCHEMA,
                                            fixed_synonym);
  }
  if (!entity) {
    my_error(ER_SYNONYM_DOES_NOT_EXIST, MYF(0), fixed_synonym.c_str());
    DBUG_RETURN(true);
  }

  buffer.append(STRING_WITH_LEN("CREATE "));
  if (entity->schema().empty()) {
    buffer.append(STRING_WITH_LEN("PUBLIC "));
  }
  buffer.append(STRING_WITH_LEN("SYNONYM "));

  /* name */
  if (!entity->schema().empty()) {
    append_identifier(&buffer, entity->schema().c_str(),
                      entity->schema().length());
    buffer.append(STRING_WITH_LEN("."));
  }
  append_identifier(&buffer, entity->name().c_str(), entity->name().length());

  /* FOR */
  buffer.append(STRING_WITH_LEN(" FOR "));

  /* target */
  if (!entity->target_schema().empty()) {
    append_identifier(&buffer, entity->target_schema().c_str(),
                      entity->target_schema().length());
    buffer.append(STRING_WITH_LEN("."));
  }
  append_identifier(&buffer, entity->target_name().c_str(),
                    entity->target_name().length());

  DBUG_RETURN(false);
}  // bool build_synonym_create_str(String &buffer, const char *db, const char
   // *synonym)
