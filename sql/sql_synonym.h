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

#ifndef SQL_SYNONYM_INCLUDED
#define SQL_SYNONYM_INCLUDED

#include "mysql/components/services/bits/mysql_rwlock_bits.h"
#include "mysql/components/services/bits/psi_bits.h"
#include "mysql/components/services/bits/psi_rwlock_bits.h"
#include "sql/server_component/gdb_cmd_service.h"
#include "sql/sql_cmd_ddl.h"
#include "sql/table.h"

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

class Db_object_synonym_entity;
class Table_ident;
class THD;

using db_object_synonyms_cache_map =
    std::unordered_map<std::string, std::shared_ptr<Db_object_synonym_entity>>;

enum enum_synm_trans_result {
  SYNM_TRANS_NONE,
  SYNM_TRANS_OK,
  SYNM_TRANS_INVALID,
};  // enum enum_synm_trans_result

class Db_object_synonyms_cache {
 private:
  class _Lock_guard {
   public:
    _Lock_guard(const _Lock_guard &) = delete;
    _Lock_guard(bool readonly = false);
    ~_Lock_guard();
  };  // class Db_objct_synonyms_cache__Lock_guard

 public:
  static const db_object_synonyms_cache_map *get_cache();
  static bool load(THD *thd);
  static bool close();
  static bool free(bool end = false);
  static bool init(bool dont_read_server_table);

  static bool create(THD *thd, const std::string &db, const std::string &name,
                     std::shared_ptr<Db_object_synonym_entity> entity,
                     bool replace = false);
  static bool alter(THD *thd, const std::string &db, const std::string &name,
                    std::shared_ptr<Db_object_synonym_entity> entity);
  static const std::shared_ptr<Db_object_synonym_entity> find(
      THD *thd, const std::string &db, const std::string &name);
  static bool drop(THD *thd, const std::string &db, const std::string &name);

 private:
  static bool load_from_table(TABLE *table);
  static TABLE *lock_and_open_table(THD *thd);

 private:
  static db_object_synonyms_cache_map _cache;

  static mysql_rwlock_t _THR_LOCK_synonyms_cache;
  static PSI_rwlock_key _key_rwlock_THR_LOCK_synonyms_cache;
  static PSI_rwlock_info _synonyms_cache_psi_info[];

 private:
  friend class Sql_cmd_alter_synonym;
  friend class Sql_cmd_create_synonym;
  friend class Sql_cmd_drop_synonym;
};  // class Db_object_synonyms_cache

class Db_object_synonym_entity {
 public:
  static inline std::string make_key(const std::string &schema,
                                     const std::string &name) {
    if (schema.empty()) {
      return name;
    } else {
      return schema + "." + name;
    }
  }

 public:
  Db_object_synonym_entity() = delete;
  Db_object_synonym_entity(const std::string &schema, const std::string &name,
                           const std::string &target_schema,
                           const std::string &target_name,
                           const std::string &target_type,
                           const std::string &dblink)
      : _schema(schema),
        _name(name),
        _target_schema(target_schema),
        _target_name(target_name),
        _target_type(target_type),
        _dblink(dblink) {}

 public:
  inline const std::string &schema() const { return this->_schema; }
  inline const std::string &name() const { return this->_name; }
  inline const std::string &target_schema() const {
    return this->_target_schema;
  }
  inline const std::string &target_name() const { return this->_target_name; }
  inline const std::string &target_type() const { return this->_target_type; }
  inline const std::string &dblink() const { return this->_dblink; }

 public:
  std::string key() const {
    std::string key = make_key(this->_schema, this->_name);
    return key;
  }

  int get_errno() const { return this->_errno; }

 private:
  std::string _schema;
  std::string _name;
  std::string _target_schema;
  std::string _target_name;
  std::string _target_type;
  std::string _dblink;

  int _errno = 0;

 private:
  friend class Sql_cmd_alter_synonym;
  friend class Sql_cmd_create_synonym;
  friend class Sql_cmd_drop_synonym;
};  // class Db_object_synonym_entity

class Sql_cmd_create_synonym final : public Sql_cmd_ddl {
 private:
  Table_ident *m_ident;
  bool m_or_replace;
  Table_ident *m_target;
  bool m_is_public;

 public:
  Sql_cmd_create_synonym(Table_ident *synonym_ident, bool or_replace,
                         bool is_public, Table_ident *target);

  enum_sql_command sql_command_code() const override {
    return enum_sql_command::SQLCOM_CREATE_SYNONYM;
  }

  bool prepare(THD *thd) override;

  bool execute(THD *thd) override;
};  // class Sql_cmd_create_synonym

class Sql_cmd_drop_synonym final : public Sql_cmd_ddl {
 private:
  Table_ident *m_ident;
  bool m_is_public;

 public:
  Sql_cmd_drop_synonym(Table_ident *synonym_ident, bool is_public = false);

 public:
  enum_sql_command sql_command_code() const override {
    return enum_sql_command::SQLCOM_DROP_SYNONYM;
  }

  bool prepare(THD *thd) override;

  bool execute(THD *thd) override;
};  // class Sql_cmd_drop_synonym

class Sql_cmd_alter_synonym final : public Sql_cmd_ddl {
 private:
  Table_ident *m_ident;
  bool m_is_public;

 public:
  Sql_cmd_alter_synonym(Table_ident *synonym_ident, bool is_public = false);

 public:
  enum_sql_command sql_command_code() const override {
    return enum_sql_command::SQLCOM_ALTER_SYNONYM;
  }

  bool prepare(THD *thd) override;

  bool execute(THD *thd) override;
};  // class Sql_cmd_alter_synonym

bool skip_synm_trans_for_command(THD *thd, bool is_prepare);
enum_synm_trans_result translate_synonym_if_any(THD *thd, Table_ref *tr);

bool build_synonym_create_str(THD *thd, String &buffer, const char *db,
                              const char *synonym, bool is_public);

#endif  // SQL_SYNONYM_INCLUDED
