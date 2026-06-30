/* Copyright (c) 2026, GreatDB Software Co., Ltd.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have either included with
   the program or referenced in the documentation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#ifndef SQL_DELTA_INCLUDED
#define SQL_DELTA_INCLUDED

#include "sql/dd/string_type.h"
#include "sql/item_strfunc.h"
#include "sql/sql_insert.h"
#include "sql/sql_select.h"

#define HIDDEN_IMV_PREFIX "{_hidden_imv_"
#define MLOG_PREFIX "mlog$_"

extern const char *mlog_id_column_name;
extern const char *mlog_op_column_name;
extern const char *mlog_ref_column_name;

namespace greatdb {
/**
 * @brief used by drop mlog binlog write
 *   suffix prefix mlog$_
 *
 *   mlog$_t1 => t1
 *
 * @param thd
 * @param to [output]
 * @param mlog
 *
 * @return true
 * @return false
 */
bool append_table_name_by_mlog(const THD *thd, String *to,
                               const Table_ref *mlog);

/**
 * @brief parser mlog ref json
 *
 *
 * @param table_ref
 * @param func(db, table, ref , args )
 * @param args
 *
 * @return true
 * @return false
 */
template <typename S, typename Func, typename... Args>
bool string_to_mlog_status(S table_ref, Func func, Args &&...args) {
  JsonParseDefaultErrorHandler parse_handler("string_to_mlog_status", 0);

  auto dom_ptr = Json_dom::parse(table_ref.c_str(), table_ref.length(),
                                 parse_handler, JsonDepthErrorHandler);

  if (dom_ptr && dom_ptr->json_type() == enum_json_type::J_ARRAY) {
    auto jarry = down_cast<Json_array *>(dom_ptr.get());

    for (auto obj = jarry->begin(); obj != jarry->end(); obj++) {
      auto obj_ptr = down_cast<Json_object *>(obj->get());
      auto table_val = down_cast<Json_string *>(obj_ptr->get("table"));
      auto db_val = down_cast<Json_string *>(obj_ptr->get("db"));

      auto ref_val = down_cast<Json_int *>(obj_ptr->get("ref"));
      if (func(db_val->value(), table_val->value(), ref_val->value(),
               std::forward<Args>(args)...)) {
        return true;
      }
    }
  } else {
    my_error(ER_INVALID_MATERIALIZED_VIEW, MYF(0), "mlog ref is invalid",
             table_ref.c_str());
    return true;
  }
  return false;
}

class Mview_mlogs {
  typedef mem_root_unordered_map<std::pair<dd::String_type, dd::String_type>,
                                 std::pair<uint64_t, Table_ref *>>
      mlogMap;

  THD *thd;
  // key: db.table
  // value:  ref_offect.  table_ref
  mlogMap m_mlogs;

  /// m_lock or thd->extra_lock
  MYSQL_LOCK *m_plock{nullptr};

 public:
  Table_ref *mlog_array;
  Mview_mlogs(THD *thd)
      : thd(thd), m_mlogs(thd->mem_root), mlog_array(nullptr) {}

  ~Mview_mlogs() {
    unlock_mlog_tables(thd);
    cleanup();
  }

  void cleanup() {
    // set all table_ref to nullptr
    for (auto tr = mlog_array; tr != nullptr; tr = tr->next_global) {
      tr->table = nullptr;
    }
    mlog_array = nullptr;
    m_mlogs.clear();
  }

  void add(const dd::String_type &db, const dd::String_type &table,
           uint64_t offset);

  static bool add_mlog(const std::string &db, const std::string &table,
                       uint64_t offset, Mview_mlogs *mv) {
    mv->add(dd::String_type(db.c_str(), db.length()),
            dd::String_type(table.c_str(), table.length()), offset);
    return false;
  }

  /**
   * @brief
   *  check mlog in mlogs
   * @param db
   * @param mlog
   *
   * @return true not find
   * @return false  find
   */
  bool find(dd::String_type &db, dd::String_type &mlog) {
    return (m_mlogs.find(typename mlogMap::key_type(db, mlog)) ==
            m_mlogs.end());
  }
  /**
   * @brief
   * lock table_ref->table
   * use for update mlog ref value
   *
   * extra_lock is nullptr set m_plock
   * else merge extra_lock and m_plock set nullptr
   *
   *
   * @param thd
   * @param extra_lock
   *
   * @return true
   * @return false
   */
  bool lock_mlog_tables(THD *thd, MYSQL_LOCK **extra_lock);

  /***
   * unlock m_plock is not nullptr
   */
  bool unlock_mlog_tables(THD *thd);

  /**
   * @brief m_mlogs to json string
   *
   * @param thd
   * @param str
   *
   */
  void mlogs_to_string(THD *thd, LEX_STRING *str);

  /**
   * @brief check m_mlogs
   *    mlog has alread remove remove this from m_mlogs
   *
   * @param thd
   *
   * @return true
   * @return false
   */
  bool invalidator_remove(THD *thd);

  /**
   * @brief open tables m_mlogs
   *   mlog open failed will break
   *
   * @param thd
   *
   * @return true
   * @return false
   */
  bool add_tables_ref(THD *thd);

  /**
   * @brief update mlog metadata ref value
   *
   * @param thd
   * @param create_or_drop
   *
   * @return true
   * @return false
   */
  bool updateMetaDataRefValue(THD *thd, bool create_or_drop = true);

  /**
   * @brief update mlog table ref value
   *
   * mlogs second.second should open table and lock before
   *
   * @param thd
   * @param create_or_drop
   *
   *
   * @return true
   * @return false
   */
  bool updateRefValue(THD *thd, bool create_or_drop = true);

  size_t get_mlogs_size() { return m_mlogs.size(); }
};

class Query_result_materialized_view_create : public Query_result_create {
 public:
  Mview_mlogs flush_info;
  Query_result_materialized_view_create(THD *thd, Table_ref *create_table_arg,
                                        mem_root_deque<Item *> *fields,
                                        enum_duplicates duplic,
                                        Table_ref *select_tables_arg)
      : Query_result_create(create_table_arg, fields, duplic,
                            select_tables_arg),
        flush_info(thd) {}
  ~Query_result_materialized_view_create() {}

  bool send_eof(THD *thd) override;
  bool start_execution(THD *thd) override;
  // bool create_table_for_query_block(THD *thd) override;

  bool derived_column_rename(THD *thd, LEX *lex);

  auto get_materialized_view_info() { return create_info->m_mv_info; }

  void cleanup() override;

 private:
  bool binlog_show_create_materialized_view(THD *thd);
};

Item *add_count_func(THD *thd, Item_sum *it_sum);
Item *add_count0_func(THD *thd);

Item *add_sum_func(THD *thd, Item_sum *it_sum);

bool check_fields_to_rewrite_aggr_func(
    THD *thd, Item *it, Query_block *qb,
    mem_root_unordered_map<std::string_view, Item *> *hidden_items);

bool populate_materialized_view_and_update_table(THD *thd, LEX *lex);
bool store_materialized_view_create_info(THD *thd, Table_ref *table_list,
                                         String *packet,
                                         HA_CREATE_INFO *create_info_arg,
                                         bool show_database,
                                         bool for_show_create_stmt);
bool store_materialized_view_log_create_info(THD *thd, Table_ref *table_list,
                                             String *packet);
}  // namespace greatdb

namespace dd {
bool fill_create_mlog_trigger(THD *thd, const char *mlog_name, Table *src_table,
                              Trigger *dd_trig_obj);
}
typedef std::pair<std::pair<dd::String_type, dd::String_type>, dd::String_type>
    invalid_mlog_trigger_type;

typedef std::vector<std::pair<dd::String_type, dd::String_type>>
    invalid_imv_ref_mlog_type;

bool invalid_mlog_trigger(THD *thd, invalid_mlog_trigger_type *itr);
bool push_mlog_src_table_and_trigger_mdl_request_to_list(
    THD *thd, const char *schema_name, const char *table_name,
    const char *trigger_name, MDL_request_list *mdl_request_list);
bool push_mlog_mdl_request_to_list(THD *thd, const char *schema_name,
                                   const char *table_name,
                                   MDL_request_list *mdl_request_list);

bool mark_referencing_materialized_views_invalid(
    THD *thd, invalid_imv_ref_mlog_type &invalid_imv_ref_mlog);
bool prepare_materialized_view_tables_list(THD *thd, const char *schema_name,
                                           const char *mlog_name,
                                           bool skip_same_db,
                                           invalid_imv_ref_mlog_type *result);
bool rm_table_lock_mlogs(THD *thd, Table_ref *tables);

class Item_func_snowflow_id final : public Item_int_func {
  typedef Item_int_func super;

  // packed <ts:48 | seq:16>, updated via CAS so concurrent callers cannot
  // share the same (ts, seq) and collide on the mlog primary key.
  static std::atomic<uint64_t> last_ts_seq;
  static std::atomic<int64_t> logical_offset;
  uint16_t node_mix{0};
  ulonglong snapshot_position{0};

 public:
  Item_func_snowflow_id(const POS &pos) : Item_int_func(pos) {}
  Item_func_snowflow_id() : Item_int_func() {}
  bool do_itemize(Parse_context *pc, Item **res) override;
  bool resolve_type(THD *thd) override;
  const char *func_name() const override { return "snowflow_id"; }

  bool check_partition_func_processor(uchar *) override { return false; }
  bool check_function_as_value_generator(uchar *checker_args) override {
    Check_function_as_value_generator_parameters *func_arg =
        pointer_cast<Check_function_as_value_generator_parameters *>(
            checker_args);
    func_arg->banned_function_name = func_name();
    return ((func_arg->source == VGS_GENERATED_COLUMN) ||
            (func_arg->source == VGS_CHECK_CONSTRAINT));
  }
  table_map get_initial_pseudo_tables() const override {
    return RAND_TABLE_BIT;
  }
  longlong val_int() override;
};

class Item_dbms_mview_delta_imv : public Item_str_func {
  String buffer;
  bool trace;

 public:
  Item_dbms_mview_delta_imv(const POS &pos, Item *a, Item *b)
      : Item_str_func(pos, a, b), trace(false) {}

  Item_dbms_mview_delta_imv(const POS &pos, Item *a, Item *b, Item *c)
      : Item_str_func(pos, a, b, c), trace(true) {}
  //
  enum Functype functype() const override { return DD_INTERNAL_FUNC; }
  bool resolve_type(THD *) override {
    set_data_type_string(MAX_BLOB_WIDTH, system_charset_info);
    set_nullable(true);
    return false;
  }
  String *val_str(String *str) override;
  const char *func_name() const override { return "dbms_mview_delta_imv"; }
};

class Item_dbms_mview_refresh : public Item_str_func {
 public:
  Item_dbms_mview_refresh(const POS &pos, Item *a, Item *b, Item *c, Item *d)
      : Item_str_func(pos, a, b, c, d) {}
  enum Functype functype() const override { return DD_INTERNAL_FUNC; }

  bool resolve_type(THD *) override {
    set_data_type_string(MAX_BLOB_WIDTH, system_charset_info);
    set_nullable(true);
    return false;
  }

  String *val_str(String *str) override;
  const char *func_name() const override { return "dbms_mview_refresh"; }
};

class Item_dbms_mview_purge_mlog : public Item_int_func {
 public:
  Item_dbms_mview_purge_mlog(const POS &pos, Item *a, Item *b)
      : Item_int_func(pos, a, b) {}
  enum Functype functype() const override { return DD_INTERNAL_FUNC; }

  bool resolve_type(THD *thd) override {
    if (param_type_is_default(thd, 0, 1)) return true;
    return false;
  }
  longlong val_int() override;
  const char *func_name() const override { return "dbms_mview_purge_mlog"; }
};

class Item_dbms_mview_update_mlog : public Item_int_func {
 public:
  Item_dbms_mview_update_mlog(const POS &pos, Item *a, Item *b, Item *c)
      : Item_int_func(pos, a, b, c) {}
  enum Functype functype() const override { return DD_INTERNAL_FUNC; }

  bool resolve_type(THD *thd) override {
    if (param_type_is_default(thd, 0, 1)) return true;
    return false;
  }
  longlong val_int() override;
  const char *func_name() const override { return "dbms_mview_update_mlog"; }
};

#endif
