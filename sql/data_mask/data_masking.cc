/* Copyright (c) 2024, GreatDB Software Co., Ltd.

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

#include "sql/data_mask/data_masking.h"
#include <strings.h>
#include <boost/algorithm/string.hpp>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <string>
#include <vector>

#include "map_helpers.h"
#include "my_loglevel.h"
#include "mysql/components/services/log_builtins.h"
#include "mysqld_error.h"
#include "sql/auth/auth_acls.h"  // SUPER_ACL
#include "sql/error_handler.h"   // Internal_error_handler
#include "sql/item.h"
#include "sql/item_strfunc.h"
#include "sql/mysqld.h"          // global_system_variables
#include "sql/psi_memory_key.h"  // key_memory_data_mask
#include "sql/sql_base.h"        // open_and_lock_tables
#include "sql/sql_class.h"
#include "sql/sql_executor.h"
#include "sql/sql_plugin.h"  // my_plugin_lock_by_name
#include "sql/sql_plugin_ref.h"
#include "sql/sql_system_table_check.h"
#include "sql/sql_table.h"
#include "sql/system_variables.h"
#include "sql/table.h"  // TABLE

namespace data_masking {

const auto block_size = 8192;
const auto tmp_alloc_block_size = 1024;

static MEM_ROOT global_masking_memory;
static MEM_ROOT masking_memx;

const char *DATA_MASK_DB = {"sys_masking"};

enum DATA_MASK_TABLES_E {
  LABEL_TABLE = 0,
  POLICY_TABLE,
  POLICY_LABEL_TABLE,
  POLICY_USER_TABLE,
  TABLE_END
};

const char *DATA_MASK_TABLE_NAMES[] = {"masking_label", "masking_policy",
                                       "masking_policy_labels",
                                       "masking_policy_users"};

static_assert((sizeof(DATA_MASK_TABLE_NAMES) /
               sizeof(DATA_MASK_TABLE_NAMES[0])) ==
                  /*DATA_MASK_TABLES_E::TABLE_END*/ 4,
              "data masking table name");

namespace masking_label_table {
enum MASKING_LABEL_E {
  LABEL_ID = 0,
  LABEL_NAME,
  DB_NAME,
  TABLE_NAME,
  FIELD_NAME,
  CREATE_TIME,
  FIELD_COUNT
};
}  // namespace masking_label_table

static const TABLE_FIELD_TYPE
    lable_table_def[masking_label_table::FIELD_COUNT] = {
        {{STRING_WITH_LEN("label_id")}, {STRING_WITH_LEN("int")}, {nullptr, 0}},
        {{STRING_WITH_LEN("label_name")},
         {STRING_WITH_LEN("varchar(255)")},
         {nullptr, 0}},
        {{STRING_WITH_LEN("db_name")},
         {STRING_WITH_LEN("varchar(64)")},
         {nullptr, 0}},
        {{STRING_WITH_LEN("table_name")},
         {STRING_WITH_LEN("varchar(64)")},
         {nullptr, 0}},
        {{STRING_WITH_LEN("field_name")},
         {STRING_WITH_LEN("varchar(64)")},
         {nullptr, 0}},
        {{STRING_WITH_LEN("create_time")},
         {STRING_WITH_LEN("timestamp")},
         {nullptr, 0}},
};

namespace policy_table {
enum MASKING_POLICY_E {
  MASK_NAME = 0,
  MASKACTION,
  OPTIONAL,
  POLENABLED,
  CREATE_TIME,
  UPDATE_TIME,
  FIELD_COUNT
};
}

static const TABLE_FIELD_TYPE policy_table_def[policy_table::FIELD_COUNT] = {
    {{STRING_WITH_LEN("polname")},
     {STRING_WITH_LEN("varchar(255)")},
     {nullptr, 0}},
    {{STRING_WITH_LEN("maskaction")},
     {STRING_WITH_LEN("enum('maskall','mask_inside')")},
     {nullptr, 0}},
    {{STRING_WITH_LEN("optinal")},
     {STRING_WITH_LEN("varchar(255)")},
     {nullptr, 0}},
    {{STRING_WITH_LEN("polenabled")}, {STRING_WITH_LEN("int")}, {nullptr, 0}},
    {{STRING_WITH_LEN("create_time")},
     {STRING_WITH_LEN("timestamp")},
     {nullptr, 0}},
    {{STRING_WITH_LEN("update_time")},
     {STRING_WITH_LEN("timestamp")},
     {nullptr, 0}},
};

namespace masking_policy_labels {

enum MASKING_POLICY_LABELS_E {
  POLNAME = 0,
  LABEL_NAME,
  CREATE_TIME,
  FIELD_COUNT
};
}  // namespace masking_policy_labels

static const TABLE_FIELD_TYPE
    policy_labels_table_def[masking_policy_labels::FIELD_COUNT] = {
        {{STRING_WITH_LEN("polname")},
         {STRING_WITH_LEN("varchar(255)")},
         {nullptr, 0}},
        {{STRING_WITH_LEN("label_name")},
         {STRING_WITH_LEN("varchar(255)")},
         {nullptr, 0}},
        {{STRING_WITH_LEN("create_time")},
         {STRING_WITH_LEN("timestamp")},
         {nullptr, 0}},
};

namespace masking_policy_users {
enum MASKING_POLICY_USERS_E {
  POLNAME = 0,
  USER_NAME,
  CREATE_TIME,
  FIELD_COUNT
};
}  // namespace masking_policy_users

static const TABLE_FIELD_TYPE
    policy_users_table_def[masking_policy_users::FIELD_COUNT] = {
        {{STRING_WITH_LEN("polname")},
         {STRING_WITH_LEN("varchar(255)")},
         {nullptr, 0}},
        {{STRING_WITH_LEN("user_name")},
         {STRING_WITH_LEN("varchar(255)")},
         {nullptr, 0}},
        {{STRING_WITH_LEN("create_time")},
         {STRING_WITH_LEN("timestamp")},
         {nullptr, 0}},
};

const TABLE_FIELD_DEF mask_table_def[TABLE_END] = {
    {masking_label_table::FIELD_COUNT, lable_table_def},
    {policy_table::FIELD_COUNT, policy_table_def},
    {masking_policy_labels::FIELD_COUNT, policy_labels_table_def},
    {masking_policy_users::FIELD_COUNT, policy_users_table_def},
};

// field->label->masking->user_filter

class DbTableField {
 public:
  /// @see Item_ident

  /// @param db
  /// @param table
  /// @param field
  DbTableField(const char *db, const char *table, const char *field)
      : db_(db), table_(table), field_(field) {}

  bool operator==(const DbTableField &other) const {
    return db_ == other.db_ && table_ == other.table_ && field_ == other.field_;
  }
  bool invailid() {
    if (db_.empty() || table_.empty() || field_.empty()) {
      return true;
    }
    my_casedn_str(system_charset_info, db_.data());
    my_casedn_str(system_charset_info, table_.data());
    my_casedn_str(system_charset_info, field_.data());
    return false;
  }

  std::string db_;
  std::string table_;
  std::string field_;
};

struct DbTableFieldHash {
  size_t operator()(const DbTableField &key) const {
    return std::hash<std::string>()(key.db_) ^
           std::hash<std::string>()(key.table_) ^
           std::hash<std::string>()(key.field_);
  }
};

struct DbTableFieldEqual {
  bool operator()(const DbTableField &lhs, const DbTableField &rhs) const {
    return my_strcasecmp(system_charset_info, lhs.db_.c_str(),
                         rhs.db_.c_str()) == 0 &&
           my_strcasecmp(system_charset_info, lhs.table_.c_str(),
                         rhs.table_.c_str()) == 0 &&
           my_strcasecmp(system_charset_info, lhs.field_.c_str(),
                         rhs.field_.c_str()) == 0;
  }
};

typedef malloc_unordered_set<std::string> UserFilterList;

enum mask_func_type { maskall_e = 1, mask_inside_e };

class MaskPolicy {
 public:
  MaskPolicy(std::string name, bool e)
      : Policy_name(name),
        Filter_user_list(key_memory_data_mask),
        enabled(e),
        start(-1),
        end(-1) {}
  std::string Policy_name;
  longlong Action_func;
  UserFilterList Filter_user_list;
  bool enabled;
  unique_ptr_destroy_only<String> mask_char;
  size_t start;
  size_t end;
};

class LabelPolicy {
 public:
  LabelPolicy(std::string name) : Label_name(name), Policy(nullptr) {}
  std::string Label_name;
  MaskPolicy *Policy;

  ~LabelPolicy() {}
};

class FieldLabel {
 public:
  FieldLabel(const char *db, const char *tb, const char *field,
             std::string name)
      : Field_key(db, tb, field), Label_name(name), Label(nullptr) {}
  ~FieldLabel() {}
  DbTableField Field_key;
  std::string Label_name;
  LabelPolicy *Label;
};

typedef malloc_unordered_map<std::string, unique_ptr_destroy_only<MaskPolicy>>
    Data_Mask_Policy;
typedef malloc_unordered_map<std::string, unique_ptr_destroy_only<LabelPolicy>>
    Data_Label;
typedef malloc_unordered_map<DbTableField, unique_ptr_destroy_only<FieldLabel>,
                             DbTableFieldHash, DbTableFieldEqual>
    Data_Mask_Field_Label;

std::unique_ptr<Data_Mask_Field_Label> g_mask_field_map;
std::unique_ptr<Data_Mask_Policy> g_mask_policy_map;
std::unique_ptr<Data_Label> g_mask_label_map;

/*  see MAC_CACHE_LOCK_TIMEOUT*/
static const uint CACHE_LOCK_TIMEOUT = 900UL;
static const MDL_key MASKING_CACHE_KEY(MDL_key::MASKING_CACHE, "", "");

class CACHE_error_handler : public Internal_error_handler {
 public:
  /**
    Handle an error condition

    @param [in] thd           THD handle
    @param [in] sql_errno     Error raised by MDL subsystem
    @param [in] sqlstate      SQL state. Unused.
    @param [in] level         Severity level. Unused.
    @param [in] msg           Message string. Unused.
  */

  bool handle_condition(THD *thd MY_ATTRIBUTE((unused)), uint sql_errno,
                        const char *sqlstate MY_ATTRIBUTE((unused)),
                        Sql_condition::enum_severity_level *level
                            MY_ATTRIBUTE((unused)),
                        const char *msg MY_ATTRIBUTE((unused))) override {
    return (sql_errno == ER_LOCK_DEADLOCK ||
            sql_errno == ER_LOCK_WAIT_TIMEOUT ||
            sql_errno == ER_QUERY_INTERRUPTED || sql_errno == ER_QUERY_TIMEOUT);
  }
};

/*
  MDL_release_locks_visitor subclass to release MDL for MASKING_CACHE.
*/
class Release_masking_cache_locks : public MDL_release_locks_visitor {
 public:
  /**
    Lock releaser.
    Check details of given key and see it is of type MASKING_CACHE
    and if key name it matches with m_partition. If so, release it.

    @param [in] ticket      MDL Ticket returned by MDL subsystem

    @returns Whether ticket matches our criteria or not
      @retval true  Ticket matches
      @retval false Ticket does not match
  */

  bool release(MDL_ticket *ticket) override {
    return ticket->get_key()->mdl_namespace() == MDL_key::MASKING_CACHE;
  }
};

/**
  Enum for specifying lock type over Acl cache
*/

enum class Mask_cache_lock_mode { READ_MODE = 1, WRITE_MODE };

/**
  Lock guard for Data_Mask Cache.
  Destructor automatically releases the lock.
*/

class Mask_cache_lock_guard {
 public:
  Mask_cache_lock_guard(THD *thd, Mask_cache_lock_mode mode)
      : m_thd(thd), m_mode(mode), m_locked(false) {}

  /**
    Mask_cache_lock_guard destructor.

    Release lock(s) if taken
  */
  ~Mask_cache_lock_guard() { unlock(); }

  bool lock(bool raise_error = true) {
    assert(!m_locked);

    if (already_locked()) return true;

    /* If we do not have MDL, we should not be holding LOCK_open */
    mysql_mutex_assert_not_owner(&LOCK_open);

    MDL_request lock_request;
    MDL_REQUEST_INIT_BY_KEY(
        &lock_request, &MASKING_CACHE_KEY,
        m_mode == Mask_cache_lock_mode::READ_MODE ? MDL_SHARED : MDL_EXCLUSIVE,
        MDL_EXPLICIT);
    CACHE_error_handler handler;
    m_thd->push_internal_handler(&handler);
    m_locked =
        !m_thd->mdl_context.acquire_lock(&lock_request, CACHE_LOCK_TIMEOUT);
    m_thd->pop_internal_handler();

    if (!m_locked && raise_error)
      my_error(ER_CANNOT_LOCK_USER_MANAGEMENT_CACHES, MYF(0));

    return m_locked;
  }
  void unlock() {
    if (m_locked) {
      Release_masking_cache_locks lock_visitor;
      m_thd->mdl_context.release_locks(&lock_visitor);
      m_locked = false;
    }
  }

 private:
  bool already_locked() {
    return m_thd->mdl_context.owns_equal_or_stronger_lock(
        MDL_key::MASKING_CACHE, "", "",
        m_mode == Mask_cache_lock_mode::READ_MODE ? MDL_SHARED : MDL_EXCLUSIVE);
  }

 private:
  /** Handle to THD object */
  THD *m_thd;
  /** Lock mode */
  Mask_cache_lock_mode m_mode;
  /** Lock status */
  bool m_locked;
};

/**
  Small helper function which allows to determine if error which caused
  failure to open and lock privilege tables should not be reported to error
  log (because this is expected or temporary condition).
*/

bool is_expected_or_transient_error(THD *thd) {
  return !thd->get_stmt_da()->is_error() ||  // Interrupted/no error condition.
         thd->get_stmt_da()->mysql_errno() == ER_TABLE_NOT_LOCKED ||
         thd->get_stmt_da()->mysql_errno() == ER_LOCK_DEADLOCK;
}

class mask_data_table_intact : public System_table_intact {
 public:
  mask_data_table_intact(THD *c_thd, enum loglevel log_level = ERROR_LEVEL)
      : System_table_intact(c_thd, log_level) {}

  /**
    Checks whether an   table is intact.

    Works in conjunction with @ref  masktable and
    Table_check_intact::check()

    @param table Table to check.
    @param DATA_MASK_TABLES_E  Table "id"

    @retval  false  OK
    @retval  true   There was an error.
  */
  bool check(Table_ref *table, DATA_MASK_TABLES_E t) {
    return Table_check_intact::check(thd(), table[t].table,
                                     &(mask_table_def[t]));
  }
};

///

static void init_tables_for_open(Table_ref *tables) {
  for (auto i = 0; i < DATA_MASK_TABLES_E::TABLE_END; i++) {
    tables[i] = {DATA_MASK_DB, DATA_MASK_TABLE_NAMES[i], TL_WRITE,
                 MDL_SHARED_NO_READ_WRITE};
    tables[i].open_type = OT_BASE_ONLY;
    if (i < (DATA_MASK_TABLES_E::TABLE_END - 1))
      tables[i].next_local = tables[i].next_global = tables + i + 1;
  }
}

static bool check_policy_config(THD *thd, Table_ref *tables) {
  mask_data_table_intact table_intact(thd);
  if (table_intact.check(tables, DATA_MASK_TABLES_E::LABEL_TABLE)) return true;
  if (table_intact.check(tables, DATA_MASK_TABLES_E::POLICY_TABLE)) return true;
  if (table_intact.check(tables, DATA_MASK_TABLES_E::POLICY_LABEL_TABLE))
    return true;
  if (table_intact.check(tables, DATA_MASK_TABLES_E::POLICY_USER_TABLE))
    return true;
  return false;
}

char *get_field_cassdn(MEM_ROOT *mem, Field *field) {
  char buff[MAX_FIELD_WIDTH], *to;
  String str(buff, sizeof(buff), &my_charset_bin);
  size_t length;

  field->val_str(&str);
  length = str.length();
  if (!length || !(to = (char *)mem->Alloc(length + 1))) return NullS;
  memcpy(to, str.ptr(), length);
  to[length] = 0;
  my_casedn_str(system_charset_info, to);
  return to;
}

static bool mask_policy_table(TABLE *table) {
  auto name =
      get_field_cassdn(&masking_memx, table->field[policy_table::MASK_NAME]);
  if (!name) {
    LogErr(WARNING_LEVEL, ER_DATA_MASKING,
           " check policy table polname has empty string");
    return false;
  }
  auto e = table->field[policy_table::POLENABLED]->val_int();
  auto pol = make_unique_destroy_only<MaskPolicy>(&global_masking_memory, name,
                                                  e == 0 ? false : true);
  if (!pol) return true;
  /// start with 1
  pol->Action_func = table->field[policy_table::MASKACTION]->val_int();

  if (pol->Action_func == mask_func_type::mask_inside_e) {
    pol->start = 0;
    pol->end = INT32_MAX;
  }
  pol->mask_char.reset();

  if (!table->field[policy_table::OPTIONAL]->is_null()) {
    char buff[MAX_FIELD_WIDTH] = {0};
    String str(buff, sizeof(buff), &my_charset_bin);
    size_t length;
    table->field[policy_table::OPTIONAL]->val_str(&str);
    length = str.length();
    if (length != 0) {
      if (pol->Action_func == mask_func_type::maskall_e) {
        if (str.numchars() > 0) {
          length = str.charpos(1);
          length = str.length() < length ? str.length() : length;

          pol->mask_char =
              make_unique_destroy_only<String>(&global_masking_memory, length);
          if (!pol->mask_char) return true;
          pol->mask_char->copy(str.ptr(), length, str.charset());
        }
      } else {
        std::vector<std::string> res;
        boost::algorithm::split(res, str.c_ptr(), boost::is_any_of(","));
        int start, end;
        int err = 0;

        for (size_t i = 0; i < res.size(); i++) {
          err = 0;
          switch (i) {
            case 0: {
              start = my_strntoll(&my_charset_bin,
                                  pointer_cast<const char *>(res[i].data()),
                                  res[i].size(), 10, NULL, &err);
              if (err) {
                start = 0;
              }
              if (start < 0) start = 0;
              pol->start = start;
            } break;
            case 1: {
              end = my_strntoll(&my_charset_bin,
                                pointer_cast<const char *>(res[i].data()),
                                res[i].size(), 10, NULL, &err);
              if (err) {
                end = INT32_MAX;
              }
              if (end < 0) {
                end = INT32_MAX;
              }
              pol->end = end;
            } break;
            case 2:
              if (res[i].size() != 0) {
                length =
                    my_charpos(table->field[policy_table::OPTIONAL]->charset(),
                               res[i].data(), res[i].data() + res[i].size(), 1);

                length = length > res[i].size() ? res[i].size() : length;
                pol->mask_char = make_unique_destroy_only<String>(
                    &global_masking_memory, length);
                if (!pol->mask_char) return true;
                pol->mask_char->copy(
                    res[i].data(), length,
                    table->field[policy_table::OPTIONAL]->charset());
              }
              break;
            default:
              err = 1;
              break;
          }
          if (err) {
            std::string msg(" POLICY HAS INVALID OPTIONAL");
            msg = pol->Policy_name + msg;
            LogErr(WARNING_LEVEL, ER_DATA_MASKING, msg.c_str());
          }
        }
      }
    }
  }
  g_mask_policy_map->emplace(pol->Policy_name, move(pol));
  return false;
}

static bool policy_label_table(TABLE *table) {
  auto label = get_field_cassdn(
      &masking_memx, table->field[masking_policy_labels::LABEL_NAME]);
  auto polname = get_field_cassdn(&masking_memx,
                                  table->field[masking_policy_labels::POLNAME]);

  if (!label || !polname) {
    LogErr(WARNING_LEVEL, ER_DATA_MASKING,
           " check policy_label table has empty string");
    return false;
  }

  std::string polkey(polname);
  auto pitr = g_mask_policy_map->find(polkey);
  if (pitr == g_mask_policy_map->end()) {
    std::string errmsg("masking_policy_labels can't not found polname: ");
    errmsg += polkey;
    LogErr(WARNING_LEVEL, ER_DATA_MASKING, errmsg.c_str());
    //
    return false;
  }
  // skip
  if (!pitr->second->enabled) return false;
  //  if not found should delete the field map
  std::string key(label);
  auto litr = g_mask_label_map->find(key);
  if (litr != g_mask_label_map->end()) {
    litr->second->Policy = pitr->second.get();
  }
  return false;
}

static bool check_unused() {
  for (auto itr = g_mask_policy_map->begin();
       itr != g_mask_policy_map->end();) {
    if (!itr->second->enabled) {
      itr = g_mask_policy_map->erase(itr);
    } else {
      itr++;
    }
  }

  std::set<std::string> del_list;
  for (auto itr = g_mask_label_map->begin(); itr != g_mask_label_map->end();) {
    if (itr->second->Policy == nullptr) {
      del_list.insert(itr->first);
      std::string msg = itr->second->Label_name + " label not find any policy ";
      itr = g_mask_label_map->erase(itr);
      LogErr(INFORMATION_LEVEL, ER_DATA_MASKING, msg.c_str());
    } else {
      itr++;
    }
  }
  for (auto itr = g_mask_field_map->begin(); itr != g_mask_field_map->end();) {
    if (del_list.find(itr->second->Label_name) != del_list.end()) {
      itr = g_mask_field_map->erase(itr);
    } else {
      itr++;
    }
  }
  return false;
}

static bool policy_user_table(TABLE *table) {
  auto polname = get_field_cassdn(&global_masking_memory,
                                  table->field[masking_policy_users::POLNAME]);
  auto username = get_field_cassdn(
      &global_masking_memory, table->field[masking_policy_users::USER_NAME]);

  if (!polname || !username) {
    LogErr(WARNING_LEVEL, ER_DATA_MASKING,
           " check policy_label table has empty string");
    return false;
  }

  std::string key(polname);
  auto itr = g_mask_policy_map->find(key);
  if (itr != g_mask_policy_map->end()) {
    itr->second->Filter_user_list.emplace(username);
  } else {
    std::string errmsg("masking_policy_users can't not found polname: ");
    errmsg += key;
    LogErr(WARNING_LEVEL, ER_DATA_MASKING, errmsg.c_str());
    return false;
  }
  return false;
}

static bool mask_label_table(TABLE *table) {
  auto name = get_field_cassdn(&masking_memx,
                               table->field[masking_label_table::LABEL_NAME]);
  auto db = get_field_cassdn(&masking_memx,
                             table->field[masking_label_table::DB_NAME]);
  auto tb = get_field_cassdn(&masking_memx,
                             table->field[masking_label_table::TABLE_NAME]);
  auto f = get_field_cassdn(&masking_memx,
                            table->field[masking_label_table::FIELD_NAME]);
  if (!db || !tb || !f || !name) {
    LogErr(WARNING_LEVEL, ER_DATA_MASKING, "check mask_label empty value");
    return false;
  }

  auto field_label = make_unique_destroy_only<FieldLabel>(
      &global_masking_memory, db, tb, f, name);
  if (!field_label) return true;

  auto label_itr = g_mask_label_map->find(name);
  if (label_itr != g_mask_label_map->end()) {
    field_label->Label = label_itr->second.get();
  } else {
    auto labelpolicy =
        make_unique_destroy_only<LabelPolicy>(&global_masking_memory, name);
    if (!labelpolicy) return true;
    field_label->Label = labelpolicy.get();
    g_mask_label_map->emplace(name, move(labelpolicy));
  }
  g_mask_field_map->emplace(field_label->Field_key, move(field_label));

  return false;
}

static bool load_table(THD *thd, TABLE *table,
                       std::function<bool(TABLE *)> read_func) {
  assert(table);
  int read_rec = 0;
  unique_ptr_destroy_only<RowIterator> iterator;
  iterator = init_table_iterator(thd, table,
                                 /*ignore_not_found_rows=*/false,
                                 /*count_examined_rows=*/false);
  if (iterator == nullptr) return true;
  table->use_all_columns();
  while (!(read_rec = iterator->Read())) {
    if (read_func) {
      if (read_func(table)) return true;
    }
  }
  iterator.reset();
  return read_rec > 0 ? true : false;
}

bool reload(THD *thd, bool mdl_locked) {
  if (!enable_data_masking) return false;
  bool res = true;
  Table_ref tables[DATA_MASK_TABLES_E::TABLE_END];
  uint flags = mdl_locked
                   ? MYSQL_OPEN_HAS_MDL_LOCK | MYSQL_LOCK_IGNORE_TIMEOUT |
                         MYSQL_OPEN_IGNORE_FLUSH
                   : MYSQL_LOCK_IGNORE_TIMEOUT;
  init_tables_for_open(tables);
  if (open_and_lock_tables(thd, tables, flags)) {
    if (!is_expected_or_transient_error(thd)) {
      LogErr(ERROR_LEVEL, ER_DATA_MASKING, thd->get_stmt_da()->message_text());
    }
    goto error_end;
  }

  if (check_policy_config(thd, tables)) goto error_end;

  // flush all policy
  {
    Mask_cache_lock_guard cache_lock(thd, Mask_cache_lock_mode::WRITE_MODE);
    if (!cache_lock.lock()) goto error_end;

    sql_mode_t old_sql_mode = thd->variables.sql_mode;
    thd->variables.sql_mode &= ~MODE_PAD_CHAR_TO_FULL_LENGTH;
    MEM_ROOT old_mem = std::move(global_masking_memory);
    init_sql_alloc(key_memory_data_mask, &global_masking_memory, block_size);
    init_sql_alloc(key_memory_data_mask, &masking_memx, tmp_alloc_block_size);

    // backup
    std::unique_ptr<Data_Mask_Field_Label> old_field_map(
        new Data_Mask_Field_Label(key_memory_data_mask));
    std::unique_ptr<Data_Mask_Policy> old_policy_map(
        new Data_Mask_Policy(key_memory_data_mask));
    std::unique_ptr<Data_Label> old_label_map(
        new Data_Label(key_memory_data_mask));
    g_mask_field_map.swap(old_field_map);
    g_mask_policy_map.swap(old_policy_map);
    g_mask_label_map.swap(old_label_map);
    /*
    1. create label
        field_map
        label_map
    2. create policy
        bind mask function
    3. policy bind label
       rasie error if label not find in policy (if not use fk)
       and check if label not bind policy and delete field_map
    4. check label and apply policy
        raise error if this if not find policy (if not use fk)
    */

    if (load_table(thd, tables[DATA_MASK_TABLES_E::LABEL_TABLE].table,
                   mask_label_table) ||
        load_table(thd, tables[DATA_MASK_TABLES_E::POLICY_TABLE].table,
                   mask_policy_table) ||
        load_table(thd, tables[DATA_MASK_TABLES_E::POLICY_LABEL_TABLE].table,
                   policy_label_table) ||
        load_table(thd, tables[DATA_MASK_TABLES_E::POLICY_USER_TABLE].table,
                   policy_user_table) ||
        check_unused()) {
      g_mask_field_map.swap(old_field_map);
      g_mask_policy_map.swap(old_policy_map);
      g_mask_label_map.swap(old_label_map);
      global_masking_memory.Clear();
      global_masking_memory = std::move(old_mem);
    } else {
      res = false;
    }
    masking_memx.Clear();
    thd->variables.sql_mode = old_sql_mode;
  }

error_end:
  if (!mdl_locked)
    commit_and_close_mysql_tables(thd);
  else
    close_thread_tables(thd);

  return res;
}

bool init() {
  if (!enable_data_masking) return false;
  THD *thd;
  bool return_val;
  DBUG_TRACE;

  if (!(thd = new THD)) return true; /* purecov: deadcode */
  thd->thread_stack = (char *)&thd;
  thd->store_globals();

  return_val = reload(thd, false);
  if (return_val && thd->get_stmt_da()->is_error())
    LogErr(ERROR_LEVEL, ER_DATA_MASKING, thd->get_stmt_da()->message_text());
  thd->release_resources();
  delete thd;

  return return_val;
}

void free(void) {
  g_mask_field_map.reset();
  g_mask_policy_map.reset();
  g_mask_label_map.reset();
  global_masking_memory.Clear();
}

bool apply_in_mask_policy(THD *thd, Query_block *select, DbTableField &key,
                          Item **res) {
  auto sctx = thd->security_context();

  assert(g_mask_field_map);
  assert(g_mask_label_map);
  assert(g_mask_policy_map);
  auto itr = g_mask_field_map->find(key);
  if (itr != g_mask_field_map->end()) {
    if (itr->second && itr->second->Label && itr->second->Label->Policy) {
      auto policy = itr->second->Label->Policy;
      if (!policy->enabled) return false;

      std::string user_key(sctx->priv_user().str, sctx->priv_user().length);
      user_key.push_back('@');
      user_key.append(sctx->priv_host().str, sctx->priv_host().length);
      my_casedn_str(system_charset_info, user_key.data());

      if (policy->Filter_user_list.find(user_key) !=
          policy->Filter_user_list.end())
        return false;
      Item *mask_func;
      Item *it = *res;
      if (policy->Action_func == mask_func_type::maskall_e) {
        mask_func =
            new (thd->mem_root) Item_func_maskall(it, policy->mask_char.get());
      } else {
        mask_func = new (thd->mem_root) Item_func_mask_inside(
            it, policy->start, policy->end, policy->mask_char.get());
      }
      if (!mask_func || mask_func->fix_fields(thd, &mask_func)) return true;

      mask_func->item_name = it->item_name;
      // create item_mask_ref -> real item
      //        item_mask_ref link mask_function
      //        in use for other select
      //        only use in send result field
      it->hidden = true;
      auto mask_ref = select->add_hidden_item(mask_func);
      auto select_item_ref = select->add_hidden_item(it);
      Item *ref = new (thd->mem_root) Item_data_mask_with_ref(
          &select->context, select_item_ref, it->item_name.ptr(), mask_ref);
      if (!ref) return true;

      thd->change_item_tree(res, ref);
      *res = ref;
    }
  }
  return false;
}

bool check_and_replace_mask_data(THD *thd, Query_block *select) {
  if (!enable_data_masking) return false;
  /// super ignore super
  if (thd->security_context()->master_access() & SUPER_ACL) {
    return false;
  }

  Mask_cache_lock_guard cache_lock(thd, Mask_cache_lock_mode::READ_MODE);
  if (!cache_lock.lock()) return true;

  if (!g_mask_field_map || !g_mask_label_map || !g_mask_policy_map) {
    /// TODO auto reload
    LogErr(WARNING_LEVEL, ER_DATA_MASKING, "Need to run FLUSH PRIVILEGES once");
    return false;
  }

  auto old = select->fields.size();
  for (size_t i = 0; i < select->fields.size(); i++) {
    Item *it = select->fields[i];
    if (it->hidden) continue;
    if (it->type() != Item::FIELD_ITEM) continue;
    Item_field *fi = down_cast<Item_field *>(it);
    if (!fi) continue;

    if (fi->original_db_name() && fi->original_table_name() &&
        fi->original_field_name()) {
      DbTableField key(fi->original_db_name(), fi->original_table_name(),
                       fi->original_field_name());
      if (key.invailid()) continue;

      old = select->fields.size();
      if (apply_in_mask_policy(thd, select, key, &it)) {
        return true;
      }

      if (old != select->fields.size()) {
        assert((select->fields.size() - old) == 2);
        select->fields[i + 2] = it;
        it->update_used_tables();
        select->select_list_tables |= it->used_tables();
        i += (select->fields.size() - old);
      }
    }
  }
  return false;
}

}  // namespace data_masking
