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

#include "sql/sql_delta.h"

#include <charconv>
#include "base64.h"  // base64_encode_max_arg_length

#include "include/sql_string.h"
#include "map_helpers.h"
#include "scope_guard.h"
#include "sql/dd/cache/dictionary_client.h"  // dd::cache::Dictionary_client
#include "sql/dd/dd.h"                       // dd::get_dictionary
#include "sql/dd/dictionary.h"               // dd::Dictionary
#include "sql/dd/dictionary.h"
#include "sql/dd/properties.h"
#include "sql/dd/string_type.h"
#include "sql/dd/types/table.h"
#include "sql/dd_sql_view.h"  // update_referencing_views_metadata
#include "sql/debug_sync.h"   // DEBUG_SYNC
#include "sql/derror.h"
#include "sql/derror.h"  // ER_THD
#include "sql/lock.h"
#include "sql/mem_root_array.h"
#include "sql/sp_cache.h"  // sp_cache_invalidate
#include "sql/sql_class.h"
#include "sql/sql_error.h"
#include "sql/sql_executor.h"
#include "sql/sql_gipk.h"  // table_has_generated_invisible_primary_key
#include "sql/sql_handler.h"
#include "sql/sql_lex.h"
#include "sql/sql_opt_exec_shared.h"
#include "sql/sql_optimizer.h"
#include "sql/sql_select.h"
#include "sql/sql_table.h"  // Foreign_key_parents_invalidator
#include "sql/sql_view.h"
#include "sql/thd_raii.h"
#include "sql/transaction.h"  // trans_commit_stmt
#include "sql/transaction_info.h"
// TODO: repalce c++20 format or fmt/
#include <boost/algorithm/string.hpp>
#include <vector>

// generator
// 1. id pk  <-- duplicate name
// 2. op int
// 3. ref  int means table use
//  add prefix @_sys_ means
//
const char *mlog_id_column_name = "{id}";
const char *mlog_op_column_name = "{op}";
const char *mlog_ref_column_name = "{ref}";

#define HIDDEN_IMV_PK_COLUMN HIDDEN_IMV_PREFIX "pk_%s.%s}"
#define HIDDEN_IMV_SUM_COLUMN HIDDEN_IMV_PREFIX "sum_%s}"
#define HIDDEN_IMV_COUNT_COLUMN HIDDEN_IMV_PREFIX "count_%s}"
#define DENTIFIER_CHAR "\""
#define DENTIFIER_CHAR_SEQ '"'
template <typename String, typename INPUT>
void append_quoted(
    String &target, const INPUT &s,
    typename String::value_type identifier_char =
        static_cast<typename String::value_type>(DENTIFIER_CHAR_SEQ)) {
  target += identifier_char;
  target += s;
  target += identifier_char;
}

// check_trigger_table_mdl
bool push_mlog_src_table_and_trigger_mdl_request_to_list(
    THD *thd, const char *schema_name, const char *table_name,
    const char *trigger_name, MDL_request_list *mdl_request_list) {
  MDL_request *mdl_request = new (thd->mem_root) MDL_request;
  if (mdl_request == nullptr) return true;

  MDL_REQUEST_INIT(mdl_request, MDL_key::TABLE, schema_name, table_name,
                   MDL_EXCLUSIVE, MDL_TRANSACTION);
  mdl_request_list->push_front(mdl_request);

  MDL_key mdl_key;
  dd::Trigger::create_mdl_key(schema_name, trigger_name, &mdl_key);

  mdl_request = new (thd->mem_root) MDL_request;
  if (mdl_request == nullptr) return true;

  MDL_REQUEST_INIT_BY_KEY(mdl_request, &mdl_key, MDL_EXCLUSIVE,
                          MDL_TRANSACTION);

  mdl_request_list->push_front(mdl_request);

  mdl_request = new (thd->mem_root) MDL_request;
  if (mdl_request == nullptr) return true;

  MDL_REQUEST_INIT(mdl_request, MDL_key::SCHEMA, schema_name, "",
                   MDL_INTENTION_EXCLUSIVE, MDL_STATEMENT);

  mdl_request_list->push_front(mdl_request);

  return false;
}

bool push_mlog_mdl_request_to_list(THD *thd, const char *schema_name,
                                   const char *table_name,
                                   MDL_request_list *mdl_request_list) {
  MDL_request *mdl_request = new (thd->mem_root) MDL_request;
  if (mdl_request == nullptr) return true;

  MDL_REQUEST_INIT(mdl_request, MDL_key::TABLE, schema_name, table_name,
                   MDL_EXCLUSIVE, MDL_TRANSACTION);
  mdl_request_list->push_front(mdl_request);

  mdl_request = new (thd->mem_root) MDL_request;
  if (mdl_request == nullptr) return true;

  MDL_REQUEST_INIT(mdl_request, MDL_key::SCHEMA, schema_name, "",
                   MDL_INTENTION_EXCLUSIVE, MDL_STATEMENT);

  mdl_request_list->push_front(mdl_request);
  return false;
}

bool invalid_mlog_trigger(THD *thd, invalid_mlog_trigger_type *itr) {
  DBUG_TRACE;
  MDL_request_list mdl_requests;

  auto &found_trigger_table = itr->first;

  if (push_mlog_src_table_and_trigger_mdl_request_to_list(
          thd, found_trigger_table.first.c_str(),
          found_trigger_table.second.c_str(), itr->second.c_str(),
          &mdl_requests))
    return true;

  if (!mdl_requests.is_empty() &&
      thd->mdl_context.acquire_locks(&mdl_requests,
                                     thd->variables.lock_wait_timeout))
    return true;

  mysql_ha_flush_table(thd, found_trigger_table.first.c_str(),
                       found_trigger_table.second.c_str());
  close_all_tables_for_name(thd, found_trigger_table.first.c_str(),
                            found_trigger_table.second.c_str(), false);

  const dd::cache::Dictionary_client::Auto_releaser releaser(thd->dd_client());

  dd::Table *dd_table = nullptr;
  if (thd->dd_client()->acquire_for_modification(
          found_trigger_table.first, found_trigger_table.second, &dd_table))
    return true;
  assert(dd_table != nullptr);

  const dd::Trigger *dd_trig_obj = dd_table->get_trigger(itr->second.c_str());
  if (dd_trig_obj) {
    dd_table->drop_trigger(dd_trig_obj);
    if (thd->dd_client()->update(dd_table)) {
      return true;
    }

    sp_cache_invalidate();
  } else {
    push_warning_printf(thd, Sql_condition::SL_WARNING, ER_TRG_DOES_NOT_EXIST,
                        "%s", ER_THD(thd, ER_TRG_DOES_NOT_EXIST));
  }

  return false;
}

/**
 * @brief get all ref mlogs imv
 *
 * @param thd
 * @param db
 * @param skip_same_db
 * @param result
 *
 * @return true
 * @return false
 */
bool prepare_materialized_view_tables_list(THD *thd, const char *schema_name,
                                           const char *mlog_name,
                                           bool skip_same_db,
                                           invalid_imv_ref_mlog_type *result) {
  // default include mlog
  auto check_is_mlog = [&](const dd::String_type &table_ref) -> bool {
    typedef std::vector<std::pair<const std::string, const std::string>>
        mlog_list;
    mlog_list mlogs;
    if (greatdb::string_to_mlog_status(
            table_ref,
            [](const std::string &db, const std::string &mlog, uint64_t,
               mlog_list *list) {
              list->push_back(std::make_pair(db, mlog));
              return false;
            },
            &mlogs)) {
      //  fetch all raise error
      return false;
    }

    for (auto ml : mlogs) {
      if (ml.first.compare(schema_name) == 0 &&
          ml.second.compare(mlog_name) == 0) {
        return true;
      }
    }
    return false;
  };

  dd::cache::Dictionary_client::Auto_releaser releaser(thd->dd_client());

  if (thd->dd_client()->find_all_imv(check_is_mlog, result)) {
    return true;
  }

  // raise err string_to_mlog_status
  if (thd->is_error()) {
    return true;
  }

  //
  if (skip_same_db) {
    for (auto itr = result->begin(); itr != result->end();) {
      if (itr->first.compare(schema_name) == 0) {
        itr = result->erase(itr);
      } else {
        itr++;
      }
    }
  }

  return false;
}

/**
 * @brief
 *
 * @param thd
 * @param skip_same_db
 * @param invalid_imv_ref_mlog_type
 *
 * @return true
 * @return false
 */
bool mark_referencing_materialized_views_invalid(
    THD *thd, invalid_imv_ref_mlog_type &invalid_imv_ref_mlog) {
  MDL_request_list mdl_requests;

  for (auto table : invalid_imv_ref_mlog) {
    if (push_mlog_mdl_request_to_list(thd, table.first.c_str(),
                                      table.second.c_str(), &mdl_requests)) {
      return true;
    }
  }
  if (!mdl_requests.is_empty() &&
      thd->mdl_context.acquire_locks(&mdl_requests,
                                     thd->variables.lock_wait_timeout))
    return true;

  const dd::cache::Dictionary_client::Auto_releaser releaser(thd->dd_client());
  dd::Table *mv_def = nullptr;

  for (auto itr : invalid_imv_ref_mlog) {
    if (thd->dd_client()->acquire_for_modification(itr.first, itr.second,
                                                   &mv_def)) {
      return true;
    }
    if (mv_def) {
      auto mv = mv_def->get_mv_info();
      if (mv) {
        // change to comp mode
        if (mv->flush_mode() != 0) {
          mv->set_error_msg("mlog invalid");
        }
        mv->set_build_clause(0);

        mv->set_last_updated(thd->query_start_timeval_trunc(2));
        if (thd->dd_client()->update(mv_def)) return true;
        mysql_ha_flush_table(thd, itr.first.c_str(), itr.second.c_str());
        close_all_tables_for_name(thd, itr.first.c_str(), itr.second.c_str(),
                                  false);
      }
    }
  }
  return false;
}

/**
 * @brief drop base table need drop mlog
 *
 * need lock mlog
 *
 * @param thd
 * @param tables
 *
 * @return true
 * @return false
 */
bool rm_table_lock_mlogs(THD *thd, Table_ref *tables) {
  DBUG_TRACE;
  MEM_ROOT mdl_reqs_root(key_memory_rm_db_mdl_reqs_root, MEM_ROOT_BLOCK_SIZE);
  MDL_request_list mdl_requests;

  for (Table_ref *table = tables; table != nullptr; table = table->next_local) {
    if (table->open_type != OT_BASE_ONLY && is_temporary_table(table)) continue;

    const dd::cache::Dictionary_client::Auto_releaser releaser(
        thd->dd_client());

    const dd::Abstract_table *abstract_table_def = nullptr;
    if (thd->dd_client()->acquire(table->db, table->table_name,
                                  &abstract_table_def))
      return true;

    if (abstract_table_def == nullptr ||
        abstract_table_def->type() != dd::enum_table_type::BASE_TABLE)
      continue;

    const dd::Table *table_def =
        dynamic_cast<const dd::Table *>(abstract_table_def);
    assert(table_def != nullptr);

    MEM_ROOT *save_thd_mem_root = thd->mem_root;
    auto restore_thd_mem_root =
        create_scope_guard([&]() { thd->mem_root = save_thd_mem_root; });
    thd->mem_root = &mdl_reqs_root;

    // normal table
    if (table_def->has_mlog()) {
      if (push_mlog_mdl_request_to_list(thd, table_def->mlog_db_name().c_str(),
                                        table_def->mlog_name().c_str(),
                                        &mdl_requests)) {
        return true;
      }
    }
  }

  // Acquire MDL lock on all the check constraint names.
  if (!mdl_requests.is_empty() &&
      thd->mdl_context.acquire_locks(&mdl_requests,
                                     thd->variables.lock_wait_timeout))
    return true;

  return false;
}

namespace dd {

/**
 * @brief
 *
 *  fill source table a trigger
 *
 *  trigger case when dml
 *
 * @param thd
 * @param mlog_name
 * @param src_table
 * @param dd_trig_obj
 *
 * @return true
 * @return false
 */
bool fill_create_mlog_trigger(THD *thd, const char *mlog_name, Table *src_table,
                              Trigger *dd_trig_obj) {
  char trigger_name[NAME_CHAR_LEN] = {0};
  auto trigger_name_len =
      snprintf(trigger_name, NAME_CHAR_LEN, "imv_mlog_trigger_%s",
               src_table->name().c_str());
  if (trigger_name_len >= NAME_CHAR_LEN) {
    // silent truncation would let two source tables sharing a long prefix
    // collide on the trigger name; reject up front.
    my_error(ER_TOO_LONG_IDENT, MYF(0), src_table->name().c_str());
    return true;
  }

  dd_trig_obj->set_name(String_type(trigger_name, trigger_name_len));
  // set user
  // use for sync not dump so only use once
  if (thd->lex->definer) {
    auto definer = thd->lex->definer;
    dd_trig_obj->set_definer(
        String_type(definer->user.str, definer->user.length),
        String_type(definer->host.str, definer->host.length));
  } else {
    if (thd->rli_slave == nullptr) {
      const Security_context *sctx = thd->security_context();
      assert(sctx);
      dd_trig_obj->set_definer(
          String_type(sctx->priv_user().str, sctx->priv_user().length),
          String_type(sctx->priv_host().str, sctx->priv_host().length));
    } else {
      // not definer
      // mysql.session is a guaranteed system account on every replica
      dd_trig_obj->set_definer(String_type(STRING_WITH_LEN("mysql.session")),
                               String_type(STRING_WITH_LEN("localhost")));
    }
  }

  dd_trig_obj->set_event_type(
      Trigger::enum_event_type::ET_INSERT_UPDATE_DELETE);
  dd_trig_obj->set_action_timing(Trigger::enum_action_timing::AT_AFTER);

  // UTF8 define

  /**
   *
    begin
case when inserting then
insert  into `mlog$t1`(id, op, x ,y ,z) values (0, 'i' , new.x,  new.y , new.z)
; WHEN updating THEN insert  into `mlog$t1`(id, op, x , y, z) values (0, 'd' ,
old.x  , old.y, old.z ); insert  into `mlog$t1`(id, op, x , y, z) values (0, 'i'
, new.x,  new.y , new.z ); when deleting then insert  into `mlog$t1`(id, op, x,
y,z) values (0, 'd' , old.x  , old.y, old.z ); END CASE ;
   END

   */
  dd::String_type sql;

  sql += " insert  into ";
  append_quoted(sql, mlog_name);
  sql += " (";
  append_quoted(sql, mlog_id_column_name);
  sql += ',';
  append_quoted(sql, mlog_op_column_name);

  // --- 收集列名 & 构建 new/old 部分 ---
  dd::String_type new_part;
  dd::String_type old_part;

  for (auto col : *src_table->columns()) {
    if (col->is_virtual()) continue;
    auto hidden = col->hidden();
    if (hidden != dd::Column::enum_hidden_type::HT_VISIBLE &&
        hidden != dd::Column::enum_hidden_type::HT_HIDDEN_USER)
      continue;

    // Append to column list
    sql += ',';
    append_quoted(sql, col->name());

    // Build new and old parts
    new_part += ", new.";
    append_quoted(new_part, col->name());

    old_part += ", old.";
    append_quoted(old_part, col->name());
  }

  sql += ") values ( snowflow_id(), ";

  dd::String_type final_sql;
  final_sql.reserve(sql.size() + new_part.size() + old_part.size() + 100);

  // WHEN inserting
  final_sql += "begin case when inserting then ";
  final_sql += sql;
  final_sql += " 1";
  final_sql += new_part;
  final_sql += ");";

  // WHEN updating
  final_sql += " WHEN updating THEN ";
  final_sql += sql;
  final_sql += " -1";
  final_sql += old_part;
  final_sql += ");";
  final_sql += sql;
  final_sql += " 1";
  final_sql += new_part;
  final_sql += ");";

  // WHEN deleting
  final_sql += " when deleting then ";
  final_sql += sql;
  final_sql += " -1";
  final_sql += old_part;
  final_sql += ");";

  final_sql += " END CASE ;END";

  dd_trig_obj->set_action_statement(final_sql);
  dd_trig_obj->set_action_statement_utf8(final_sql);

  dd_trig_obj->set_sql_mode(thd->variables.sql_mode);
  assert(thd->variables.sql_mode & MODE_ORACLE);

  dd_trig_obj->set_client_collation_id(my_charset_utf8mb4_0900_ai_ci.number);
  dd_trig_obj->set_connection_collation_id(
      my_charset_utf8mb4_0900_ai_ci.number);
  dd_trig_obj->set_schema_collation_id(my_charset_utf8mb4_0900_ai_ci.number);
  dd_trig_obj->options().set("status",
                             (ulong)Trigger::enum_trigger_status::ES_ENABLED);

  return false;
}

}  // namespace dd

namespace greatdb {

using string = std::string;
using QueryArray = std::vector<string>;
using ColumnNameArray =
    mem_root_deque<dd::String_type>;  //  std::vector< dd::String_type>;

using ColumnNameMap = mem_root_unordered_map<dd::String_type, dd::String_type>;

using TableColumnNameMap =
    mem_root_unordered_map<string, std::unique_ptr<ColumnNameMap>>;

/**
 *  args name rename to
 *
 *   count_base64(item_name)
 *
 *  NOTE:
 *    in parse tree node, item is not fixed
 *    Item_Fields print is will without db, table name
 *    if user not fill full name
 *
 * item_name:
 *    avg('x') ==>  count_x
 *                  sum_x
 *
 * item has fixed:
 *                 count_db.tb.x
 *                 sum_db.tb.x
 *
 */
bool aggr_func_name_alias(THD *thd, LEX_STRING *alias, Item *args0,
                          bool count = true) {
  assert(!(args0 == nullptr || (alias == nullptr)));
  if (args0 == nullptr || (alias == nullptr)) return true;

  char hidden_name[NAME_CHAR_LEN] = {0};

  auto format = count ? HIDDEN_IMV_COUNT_COLUMN : HIDDEN_IMV_SUM_COLUMN;

  char *args0_alias = nullptr;

  if (!args0->item_name.is_autogenerated() && args0->item_name.length() > 0) {
    args0_alias = const_cast<char *>(args0->item_name.ptr());
  } else {
    String item_name_str;
    args0->print(thd, &item_name_str, QT_ORDINARY);
    auto b64_length = base64_needed_encoded_length(item_name_str.length());
    args0_alias = (char *)thd->alloc(b64_length);
    if (!args0_alias) return true;
    base64_encode(item_name_str.ptr(), item_name_str.length(), args0_alias);
  }

  auto name_len = snprintf(hidden_name, NAME_CHAR_LEN, format, args0_alias);
  if (name_len < 0) {
    my_error(ER_INVALID_MATERIALIZED_VIEW, MYF(0), args0_alias,
             "name print failed");
    return true;
  }
  if (name_len >= NAME_CHAR_LEN) {
    name_len = NAME_CHAR_LEN - 1;
  }
  auto alias_str = thd->strmake(hidden_name, name_len);
  if (!alias_str) {
    return true;
  }

  alias->str = alias_str;
  alias->length = name_len;

  return false;
}

/**
 * @brief add Item  COUNT(0)
 *
 * @param thd
 *
 * @return Item*  , nullptr on failure
 */
Item *add_count0_func(THD *thd) {
  // add_hidden_count(0)
  Item *i0 = new (thd->mem_root) Item_int_0(POS());
  if (!i0) return nullptr;
  Item *count_func = new (thd->mem_root) Item_sum_count(POS(), i0, nullptr);
  if (!count_func) return nullptr;

  char hidden_name[NAME_CHAR_LEN] = {0};
  auto name_len =
      snprintf(hidden_name, NAME_CHAR_LEN, HIDDEN_IMV_COUNT_COLUMN, "0");
  if (name_len >= NAME_CHAR_LEN) {
    name_len = NAME_CHAR_LEN - 1;
  }

  count_func->item_name.copy(hidden_name, name_len, system_charset_info, false);

  return count_func;
}

/**
 * @brief add count func as alias
 *
 *   it_sum: avg(x)
 *
 *   return:   count(x) as 'base64(db.table.x)'
 *
 * @param thd
 * @param qb
 * @param it_sum
 *
 * @return Item* , nullptr on failure
 */
Item *add_count_func(THD *thd, Query_block *qb, Item_sum *it_sum) {
  // name is is_autogenerated if item  name is nullptr
  LEX_STRING alias_count;

  if (aggr_func_name_alias(thd, &alias_count, it_sum->arguments()[0])) {
    return nullptr;
  }

  auto arg0 =
      new (thd->mem_root) Item_ref(&qb->context, &(it_sum->arguments()[0]),
                                   (it_sum->arguments()[0])->item_name.ptr());
  //
  if (arg0 == nullptr) {
    my_error(ER_INVALID_MATERIALIZED_VIEW, MYF(0), "aggregate function",
             "count args not support");
    return nullptr;
  }
  Item *count_func = new (thd->mem_root) Item_sum_count(POS(), arg0, nullptr);

  count_func->item_name.copy(alias_count.str, alias_count.length,
                             system_charset_info, true);
  return count_func;
}

/**
 * @brief add sum func as alais
 *
 *  it_sum: avg(x)
 *     return:  sum(x) as 'base64(db.table.x)'
 *
 * @param thd
 * @param qb
 * @param it_sum
 *
 * @return Item*
 */
Item *add_sum_func(THD *thd, Query_block *qb, Item_sum *it_sum) {
  LEX_STRING alias_sum;

  if (aggr_func_name_alias(thd, &alias_sum, it_sum->arguments()[0], false))
    return nullptr;

  auto arg0 =
      new (thd->mem_root) Item_ref(&qb->context, &(it_sum->arguments()[0]),
                                   (it_sum->arguments()[0])->item_name.ptr());
  if (arg0 == nullptr) {
    my_error(ER_INVALID_MATERIALIZED_VIEW, MYF(0), "aggregate function",
             "sum args not support");
    return nullptr;
  }

  Item *sum_func =
      new (thd->mem_root) Item_sum_sum(POS(), arg0, false, nullptr);
  if (sum_func == nullptr) return nullptr;

  sum_func->item_name.copy(alias_sum.str, alias_sum.length, system_charset_info,
                           true);
  return sum_func;
}

/**
 * @brief
 *
 *
 * 1.  avg(x) =>  sum(x) , count(x)
 *
 *   avg need use   sum/count visile field to use fast mode
 *
 *
 * 2.
 *  Item_agg_ref
 *
 *  query agg and use func use spilt func
 *                 func(agg_func(x)  )
 *                 sum(x) + 1
 *
 * after fix query fields:
 *
 * hidden              visvisibleile
 *  agg_func(x)               Item_plus(Item_agg_ref, 1 )
 *
 *   sum(x)                   ref[0] + 1
 *
 *
 * in this case will shoud add  sum visile field to use fast mode
 *
 * @param thd
 * @param it
 * @param qb
 * @param hidden_items
 *
 * @return true
 * @return false
 */
bool check_fields_to_rewrite_aggr_func(
    THD *thd, Item *it, Query_block *qb,
    mem_root_unordered_map<std::string_view, Item *> *hidden_items) {
  assert(it->has_aggregation());

  if (WalkItem(it, enum_walk::PREFIX, [&](Item *item) {
        if (item->type() == Item::SUM_FUNC_ITEM) {
          auto sum_it = down_cast<Item_sum *>(item);
          switch (sum_it->sum_func()) {
            case Item_sum::AVG_FUNC: {
              auto sum_func = greatdb::add_sum_func(thd, qb, sum_it);
              if (sum_func == nullptr) return true;

              auto count_func = greatdb::add_count_func(thd, qb, sum_it);
              if (count_func == nullptr) return true;

              hidden_items->insert(std::make_pair(
                  std::string_view(count_func->item_name.ptr(),
                                   count_func->item_name.length()),
                  count_func));

              hidden_items->insert(
                  std::make_pair(std::string_view(sum_func->item_name.ptr(),
                                                  sum_func->item_name.length()),
                                 sum_func));

              break;
            }

            case Item_sum::SUM_FUNC: {
              if (sum_it->item_name.ptr() == nullptr) {
                auto sum_func = greatdb::add_sum_func(thd, qb, sum_it);
                if (sum_func == nullptr) return true;
                hidden_items->insert(std::make_pair(
                    std::string_view(sum_func->item_name.ptr(),
                                     sum_func->item_name.length()),
                    sum_func));
              }

              auto count_func = greatdb::add_count_func(thd, qb, sum_it);
              if (count_func == nullptr) return true;

              hidden_items->insert(std::make_pair(
                  std::string_view(count_func->item_name.ptr(),
                                   count_func->item_name.length()),
                  count_func));

              break;
            }

            case Item_sum::COUNT_FUNC: {
              if (sum_it->item_name.ptr() == nullptr) {
                auto count_func = greatdb::add_count_func(thd, qb, sum_it);
                if (count_func == nullptr) return true;

                hidden_items->insert(std::make_pair(
                    std::string_view(count_func->item_name.ptr(),
                                     count_func->item_name.length()),
                    count_func));
              }
              break;
            }

            default:
              my_error(ER_INVALID_MATERIALIZED_VIEW, MYF(0),
                       sum_it->func_name(), "not support");
              return true;
          }
        }
        return false;
      })) {
    return true;
  }

  return false;
}

class CheckVisitor : public Select_lex_visitor {
  THD *thd;

 public:
  mem_root_unordered_map<
      Table_ref *,
      std::unique_ptr<mem_root_unordered_map<Field *, Item_name_string *>>>
      table_pk_columns;
  // mgr need primary key
  List<Key_part_spec> key_parts;

  CheckVisitor(THD *thd) : thd(thd), table_pk_columns(thd->mem_root) {}

  // to  field= item_name
  bool add_hidden_primary_key_map(LEX_STRING *pk_obj) {
    if (table_pk_columns.empty()) {
      return false;
    }

    //   { table: { field: item_name , ...}, ...  ,   }
    auto obj = create_dom_ptr<Json_object>();

    for (auto &tb_itr : table_pk_columns) {
      auto field_obj = create_dom_ptr<Json_object>();

      for (auto itr : *tb_itr.second) {
        field_obj->add_alias(string(itr.first->field_name),
                             create_dom_ptr<Json_string>(itr.second->ptr(),
                                                         itr.second->length()));
      }

      obj->add_alias(
          string(tb_itr.first->table_name, tb_itr.first->table_name_length),
          std::move(field_obj));
    }
    Json_wrapper w(std::move(obj));
    String ss;
    w.to_string(&ss, false, "add_hidden_primary_key", [] {});
    pk_obj->str = thd->strmake(ss.ptr(), ss.length());
    pk_obj->length = ss.length();
    return false;
  }

  bool transform_func(Query_block *qb) {
    /// find table
    if (!qb->agg_func_used()) {
      return add_hidden_primary_key(qb);
    }

    return false;
  }

 protected:
  bool get_all_table_primary(Query_block *qb) {
    for (auto tr = qb->get_table_list(); tr != nullptr; tr = tr->next_global) {
      auto pk_columns =
          std::make_unique<mem_root_unordered_map<Field *, Item_name_string *>>(
              thd->mem_root);

      assert(tr->table);

      if (tr->table->s->primary_key != MAX_KEY) {
        KEY *pk = &tr->table->s->key_info[tr->table->s->primary_key];

        for (uint i = 0; i < pk->user_defined_key_parts; ++i) {
          pk_columns->insert(std::make_pair(
              tr->table->field[pk->key_part[i].fieldnr - 1], nullptr));
        }

      } else {
        // table without primary key
        my_error(ER_INVALID_MATERIALIZED_VIEW, MYF(0), tr->table_name,
                 "primary key not find");
        return true;
      }

      table_pk_columns[tr] = std::move(pk_columns);
    }
    return false;
  }

  Item_name_string *find_field_in_query_block_func(Field *field,
                                                   Query_block *qb) {
    for (Item *item : qb->visible_fields()) {
      if (item->type() == Item::FIELD_ITEM) {
        Item_field *field_item = down_cast<Item_field *>(item);
        if (field_item->fixed) {
          if (field_item->field == field) {
            return &field_item->item_name;
          }
        }
      }
    }

    return nullptr;
  }

  bool add_hidden_primary_key(Query_block *qb) {
    if (get_all_table_primary(qb)) return true;

    char hidden_name_pk[NAME_CHAR_LEN] = {0};
    for (auto tb_itr = table_pk_columns.begin();
         tb_itr != table_pk_columns.end(); tb_itr++) {
      for (auto itr = tb_itr->second->begin(); itr != tb_itr->second->end();
           itr++) {
        auto item_name = find_field_in_query_block_func(itr->first, qb);
        if (item_name == nullptr) {
          // without select pk columns
          Item *fi = new (thd->mem_root) Item_field(itr->first);
          if (!fi) return true;
          if (!fi->fixed && fi->fix_fields(thd, &fi)) {
            return true;
          }
          // real table field
          auto pk_len = snprintf(
              hidden_name_pk, NAME_CHAR_LEN, HIDDEN_IMV_PK_COLUMN,
              itr->first->table->s->table_name.str, itr->first->field_name);
          if (pk_len >= NAME_CHAR_LEN) {
            pk_len = NAME_CHAR_LEN - 1;
          }

          auto alias = thd->strmake(hidden_name_pk, pk_len);
          if (!alias) {
            return true;
          }
          fi->item_name.copy(alias, pk_len, system_charset_info, false);
          qb->add_item_to_list(fi);

          itr->second = &fi->item_name;
        } else {
          itr->second = item_name;
        }
        // create primary key
        Key_part_spec *key_part_spec = new (thd->mem_root) Key_part_spec(
            {itr->second->ptr(), itr->second->length()}, 0, ORDER_ASC);
        if (key_part_spec == nullptr || key_parts.push_back(key_part_spec)) {
          return true;  // OOM
        }
      }
    }
    return false;
  }

  /**
   * check view
   *
   *  see visible_fields
   *
   */
  bool visit_item(Item *it) override {
    if (it->used_tables() & PSEUDO_TABLE_BITS) {
      //
      String str;
      it->print(thd, &str, QT_ORDINARY);

      my_error(ER_INVALID_MATERIALIZED_VIEW, MYF(0), "pseudo column",
               str.c_ptr());
      return true;
    }

    if (it->type() == Item::REF_ITEM) {
      it = down_cast<Item_ref *>(it)->ref_item();
    }

    if (it->type() == Item::SUBQUERY_ITEM) {
      auto sub_it = down_cast<Item_subselect *>(it);
      if (sub_it == nullptr) return true;

      if (sub_it->subquery_type() == Item_subselect::SCALAR_SUBQUERY) {
        my_error(ER_INVALID_MATERIALIZED_VIEW, MYF(0), "subquery",
                 "not support");
        return true;
      }

    } else if (it->type() == Item::SUM_FUNC_ITEM) {
      auto it_sum = down_cast<Item_sum *>(it);
      if (it_sum == nullptr) return true;
      if (it_sum->window()) {
        my_error(ER_INVALID_MATERIALIZED_VIEW, MYF(0),
                 "window function not support", it_sum->func_name());
        return true;
      }

      switch (it_sum->sum_func()) {
        case Item_sum::AVG_FUNC:
        case Item_sum::SUM_FUNC:
        case Item_sum::COUNT_FUNC:
          break;
        default:
          my_error(ER_INVALID_MATERIALIZED_VIEW, MYF(0), it_sum->func_name(),
                   " aggr function not support");
          return true;
          break;
      }
    }
    return false;
  }

  bool visit_union(Query_expression *qe) override {
    if (qe->m_with_clause) {
      my_error(ER_INVALID_MATERIALIZED_VIEW, MYF(0), "common table expression",
               "not support");
      return true;
    }

    if (qe->has_any_limit()) {
      my_error(ER_INVALID_MATERIALIZED_VIEW, MYF(0), "limit / offset",
               "not support");
      return true;
    }

    if (!qe->is_simple()) {
      my_error(ER_INVALID_MATERIALIZED_VIEW, MYF(0), "UNION/INTERSECT/EXCEPT",
               "not support");

      return true;
    }

    return false;
  }

  bool visit_query_block(Query_block *qb) override {
    if (qb->is_distinct()) {
      my_error(ER_INVALID_MATERIALIZED_VIEW, MYF(0), "distinct", "not support");
      return true;
    }

    if (qb->is_ordered()) {
      my_error(ER_INVALID_MATERIALIZED_VIEW, MYF(0), "order by", "not support");
      return true;
    }

    if (qb->olap != UNSPECIFIED_OLAP_TYPE) {
      my_error(ER_INVALID_MATERIALIZED_VIEW, MYF(0),
               GroupByModifierString(qb->olap), "not support");
      return true;
    }

    if (qb->having_cond()) {
      my_error(ER_INVALID_MATERIALIZED_VIEW, MYF(0), "having", "not support");

      return true;
    }
    if (qb->connect_by_cond()) {
      my_error(ER_INVALID_MATERIALIZED_VIEW, MYF(0), "connect by",
               "not support");
      return true;
    }

    if (qb->m_table_list.elements != 1) {
      my_error(ER_NOT_SUPPORTED_YET, MYF(0),
               "materialized view muti-tables / dual table");
      return true;
    }

    for (auto tr = qb->get_table_list(); tr != nullptr; tr = tr->next_global) {
      if (tr->is_view_or_derived()) {
        my_error(ER_NOT_SUPPORTED_YET, MYF(0),
                 "materialized view derived table or view ");
        return true;
      }
      if (!tr->table) {
        my_error(ER_INVALID_MATERIALIZED_VIEW, MYF(0), tr->table_name,
                 " table_ref can't open table");
        return true;
      }
    }
    if (qb->is_grouped()) {
      for (ORDER *grp = qb->group_list.first; grp; grp = grp->next) {
        {
          uint counter = 0;
          enum_resolution_type resolution;
          Item **select_item = nullptr;
          if (!find_item_in_list(thd, *grp->item, qb->get_fields_list(),
                                 &select_item, &counter, &resolution)) {
            if (select_item == nullptr) {
              // not find in select fields

              my_error(ER_INVALID_MATERIALIZED_VIEW, MYF(0),
                       (*grp->item)->item_name.ptr(),
                       "group by not find in select list");
              return true;
            }
            Key_part_spec *key_part_spec = new (thd->mem_root)
                Key_part_spec({(*select_item)->item_name.ptr(),
                               (*select_item)->item_name.length()},
                              0, ORDER_ASC);
            if (key_part_spec == nullptr ||
                key_parts.push_back(key_part_spec)) {
              return true;  // OOM
            }
          }
        }  // for
      }
    }
    return false;
  }
};  // CheckVisitor

/**
 * @brief
 *   update mlog refvalue
 *      *    full scan mlog
 *  likeSQL
 *    see @@delta_updat_ref
 *
 *
 * @param thd
 * @param table
 * @param offset
 *
 * @return true
 * @return false
 */
bool UpdateMlogRef(THD *thd, TABLE *table, uint64_t offset, bool add = true) {
  unique_ptr_destroy_only<RowIterator> iterator =
      init_table_iterator(thd, table,
                          /*ignore_not_found_rows=*/false,
                          /*count_examined_rows=*/false);

  if (iterator == nullptr) return true;
  table->use_all_columns();
  myf error_flags = MYF(0); /**< Flag for fatal errors */
  for (;;) {
    int error = iterator->Read();
    if (error != 0) return error > 0 ? true : false;
    if (thd->killed) {
      thd->send_kill_message();
      return 1;
    }
    ///// update ref value
    /// mlog_ref_column_name
    store_record(table, record[1]);
    Field *f = table->field[2];
    auto ref_value = f->val_int();

    if (add) {
      assert(!((ref_value >> offset) & 1ULL));
      ref_value |= (1ULL << offset);
    } else {
      ref_value &= ~(1ULL << offset);
    }
    f->set_notnull();
    type_conversion_status cs;
    cs = f->store(ref_value, false);
    if (cs != TYPE_OK) {
      my_error(ER_INVALID_ON_UPDATE, MYF(0), table->alias);
      return true;
    }
    error = table->file->ha_update_row(table->record[1], table->record[0]);

    if (error != 0) {
      if (error == HA_ERR_RECORD_IS_THE_SAME)
        error = 0;
      else {
        if (table->file->is_fatal_error(error)) error_flags |= ME_FATALERROR;

        table->file->print_error(error, error_flags);

        // The error can have been downgraded to warning by IGNORE.
        if (thd->is_error()) return true;
      }
    }

    ///////////////
  }

  return false;
}

template <typename ELEM, typename Func>
bool read_db_table_ref(const ELEM &el, Func func) {
  return func(el.first.first, el.first.second, el.second.first);
}

template <typename Container, typename Func>
bool foreach_mlog_status(const Container &c, Func f) {
  for (const auto &elem : c) {
    if (read_db_table_ref(elem, f)) return true;
  }
  return false;
}

bool UpdateMlogRefValue(THD *thd, const dd::String_type &db,
                        const dd::String_type &table, ulonglong ref_offset,
                        bool add_ref_value, bool flush) {
  const dd::cache::Dictionary_client::Auto_releaser releaser(thd->dd_client());
  dd::Table *mlog_table_def = nullptr;
  if (thd->dd_client()->acquire_for_modification(db, table, &mlog_table_def))
    return true;
  if (mlog_table_def) {
    auto mlog_info = mlog_table_def->get_mlog_info();
    if (mlog_info) {
      auto ref = mlog_info->ref();

      if (add_ref_value) {
        ref |= (1ULL << ref_offset);
      } else {
        ref &= ~(1ULL << ref_offset);
      }
      mlog_info->set_ref(ref);
      mlog_info->set_last_purged(thd->query_start_timeval_trunc(2));

      if (thd->dd_client()->update(mlog_table_def)) return true;

      if (flush) {
        mysql_ha_flush_table(thd, db.c_str(), table.c_str());
        close_all_tables_for_name(thd, db.c_str(), table.c_str(), false);
      }

      return false;
    }
  }
  my_error(ER_BAD_TABLE_ERROR, MYF(0), table.c_str());
  return true;
}

/**
 * @brief update mlog metadata ref value
 *
 * @param thd
 * @param mlog_status
 * @param flush
 * @param func
 *
 * @return true
 * @return false
 */
template <typename Container>
bool UpdateMlogsRefValue(THD *thd, Container &mlog_status, bool flush,
                         bool add_ref_value) {
  if (mlog_status.empty()) {
    return false;
  }

  MDL_request_list mdl_requests;
  /// mdl lock
  if (foreach_mlog_status(
          mlog_status,
          [thd, &mdl_requests](const dd::String_type &db,
                               const dd::String_type &table, const ulonglong) {
            return push_mlog_mdl_request_to_list(thd, db.c_str(), table.c_str(),
                                                 &mdl_requests);
          })) {
    return true;
  }

  if (!mdl_requests.is_empty() &&
      thd->mdl_context.acquire_locks(&mdl_requests,
                                     thd->variables.lock_wait_timeout))
    return true;

  if (foreach_mlog_status(mlog_status, [thd, add_ref_value, flush](
                                           const dd::String_type &db,
                                           const dd::String_type &table,
                                           const ulonglong ref) {
        return UpdateMlogRefValue(thd, db, table, ref, add_ref_value, flush);
      })) {
    return true;
  }

  return false;
}

void Mview_mlogs::add(const dd::String_type &db, const dd::String_type &table,
                      uint64_t offset) {
  m_mlogs.insert(typename mlogMap::value_type(
      typename mlogMap::key_type(db, table),

      typename mlogMap::mapped_type(offset, nullptr)));
}

void Mview_mlogs::mlogs_to_string(THD *thd, LEX_STRING *str) {
  Json_array_ptr array = create_dom_ptr<Json_array>();

  for (const auto &itr : m_mlogs) {
    auto obj = create_dom_ptr<Json_object>();

    obj->add_alias("db", create_dom_ptr<Json_string>(itr.first.first));
    obj->add_alias("table", create_dom_ptr<Json_string>(itr.first.second));
    obj->add_alias("ref", create_dom_ptr<Json_uint>(itr.second.first));
    array->append_alias(obj.release());
  }

  Json_wrapper w(std::move(array));
  String ss;
  w.to_string(&ss, false, "mlog_status_to_string", [] {});

  str->str = thd->strmake(ss.ptr(), ss.length());
  str->length = ss.length();
}

bool Mview_mlogs::invalidator_remove(THD *thd) {
  // need mdl lock before , and lock @
  MDL_request_list mdl_requests;
  for (auto itr = m_mlogs.begin(); itr != m_mlogs.end(); itr++) {
    if (push_mlog_mdl_request_to_list(thd, itr->first.first.c_str(),
                                      itr->first.second.c_str(), &mdl_requests))
      return true;
  }
  if (!mdl_requests.is_empty() &&
      thd->mdl_context.acquire_locks(&mdl_requests,
                                     thd->variables.lock_wait_timeout))
    return true;

  const dd::Table *table_def = nullptr;
  const dd::cache::Dictionary_client::Auto_releaser releaser(thd->dd_client());

  for (auto itr = m_mlogs.begin(); itr != m_mlogs.end();) {
    if (thd->dd_client()->acquire(itr->first.first, itr->first.second,
                                  &table_def)) {
      return true;
    }

    if (!table_def) {
      // mlog  may be has drop
      itr = m_mlogs.erase(itr);
    } else {
      itr++;
    }
  }

  return false;
}

bool Mview_mlogs::add_tables_ref(THD *thd) {
  // mlog

  auto prev = mlog_array = nullptr;
  for (auto &itr : m_mlogs) {
    auto db = &itr.first.first;
    auto tb = &itr.first.second;

    auto tab_ref =
        new (thd->mem_root) Table_ref(db->c_str(), db->length(), tb->c_str(),
                                      tb->length(), tb->c_str(), TL_WRITE);

    if (tab_ref == nullptr) return true;
    itr.second.second = tab_ref;

    if (mlog_array == nullptr) {
      mlog_array = prev = tab_ref;
    } else {
      prev->next_local = tab_ref;
      prev->next_global = tab_ref;
      prev = tab_ref;
    }
  }

  if (mlog_array) {
    uint table_counter = 0;
    if (open_tables(thd, &mlog_array, &table_counter, 0)) return true;
    auto mlog_ref = mlog_array;
    while (mlog_ref) {
      if (!mlog_ref->table) {
        assert(0);
        my_error(ER_BAD_TABLE_ERROR, MYF(0), mlog_ref->table_name);
        return true;
      }

      assert(mlog_ref->table->s->fields > 2);
      assert(strcmp(mlog_ref->table->field[2]->field_name,
                    mlog_ref_column_name) == 0);

      bitmap_set_bit(mlog_ref->table->read_set, 2);
      bitmap_set_bit(mlog_ref->table->write_set, 2);
      mlog_ref = mlog_ref->next_global;
    }
  }

  return false;
}

bool Mview_mlogs::lock_mlog_tables(THD *thd, MYSQL_LOCK **extra_lock) {
  if (mlog_array) {
    TABLE **start, **ptr;
    ptr = start = (TABLE **)thd->alloc(sizeof(TABLE *) * get_mlogs_size());
    Table_ref *mlog_ptr = mlog_array;
    while (mlog_ptr) {
      *(ptr++) = mlog_ptr->table;
      mlog_ptr = mlog_ptr->next_global;
    }

    MYSQL_LOCK *mlog_lock =
        mysql_lock_tables(thd, start, (uint)(ptr - start), 0);

    if (mlog_lock == nullptr) {
      return true;
    }

    if (extra_lock && *extra_lock) {
      // need merge lock
      *extra_lock = mysql_lock_merge(*extra_lock, mlog_lock);
      m_plock = nullptr;
    } else {
      m_plock = mlog_lock;
    }
  }
  return false;
}

bool Mview_mlogs::unlock_mlog_tables(THD *thd) {
  if (m_plock) {
    mysql_unlock_tables(thd, m_plock);
    m_plock = nullptr;
  }
  return false;
}

bool Mview_mlogs::updateMetaDataRefValue(THD *thd, bool create_or_drop) {
  return UpdateMlogsRefValue(thd, m_mlogs, mlog_array == nullptr,
                             create_or_drop);
}

bool Mview_mlogs::updateRefValue(THD *thd, bool create_or_drop) {
  if (mlog_array == nullptr) return false;

  // slave disable update
  if (thd->rli_slave == nullptr) {
    for (const auto &el : m_mlogs) {
      // without open table
      assert(el.second.second);
      if (UpdateMlogRef(thd, el.second.second->table, el.second.first,
                        create_or_drop))
        return true;
    }
  }
  return false;
}

bool store_materialized_view_log_create_info(THD *thd, Table_ref *table_list,
                                             String *packet) {
  const char *alias;

  packet->append(STRING_WITH_LEN("CREATE "));

  packet->append(STRING_WITH_LEN("DEFINER = "));
  if (thd->lex && thd->lex->definer) {
    auto definer = thd->lex->definer;
    packet->append(definer->user.str, definer->user.length);
    packet->append('@');
    packet->append(definer->host.str, definer->host.length);
  } else {
    if (thd->rli_slave == nullptr) {
      const Security_context *sctx = thd->security_context();
      assert(sctx);
      packet->append(sctx->priv_user().str, sctx->priv_user().length);
      packet->append('@');
      packet->append(sctx->priv_host().str, sctx->priv_host().length);
    }
  }

  packet->append(STRING_WITH_LEN(" MATERIALIZED VIEW LOG ON "));

  if (table_list->schema_table)
    alias = table_list->schema_table->table_name;
  else {
    if (lower_case_table_names == 2)
      alias = table_list->alias;
    else {
      alias = table_list->table_name;
    }
  }

  if (!thd->db().str || strcmp(table_list->db, thd->db().str)) {
    append_identifier(thd, packet, table_list->db, strlen(table_list->db));
    packet->append(STRING_WITH_LEN("."));
  }

  append_identifier(thd, packet, alias, strlen(alias));

  return false;
}

/**
 * @brief generator string for binlog sync
 *
 *
 * @param thd
 * @param table_list
 * @param packet
 * @param create_info_arg [MAYBE NULL]
 * @param show_database
 * @param for_show_create_stmt
 *
 * @return true
 * @return false
 */
bool store_materialized_view_create_info(THD *thd, Table_ref *table_list,
                                         String *packet,
                                         HA_CREATE_INFO *create_info_arg,
                                         bool show_database,
                                         bool for_show_create_stmt) {
  const char *alias;
  TABLE *table = table_list->table;
  TABLE_SHARE *share = table->s;

  packet->append(STRING_WITH_LEN("CREATE"));

  if (!for_show_create_stmt) {
    packet->append(STRING_WITH_LEN(" DEFINER = "));
    if (thd->lex && thd->lex->definer) {
      auto definer = thd->lex->definer;
      packet->append(definer->user.str, definer->user.length);
      packet->append('@');
      packet->append(definer->host.str, definer->host.length);

    } else {
      const Security_context *sctx = thd->security_context();
      assert(sctx);
      packet->append(sctx->priv_user().str, sctx->priv_user().length);
      packet->append('@');
      packet->append(sctx->priv_host().str, sctx->priv_host().length);
    }
  }

  packet->append(STRING_WITH_LEN(" MATERIALIZED VIEW "));

  if (create_info_arg &&
      (create_info_arg->options & HA_LEX_CREATE_IF_NOT_EXISTS)) {
    packet->append(STRING_WITH_LEN("IF NOT EXISTS "));
  }

  if (table_list->schema_table)
    alias = table_list->schema_table->table_name;
  else {
    if (lower_case_table_names == 2)
      alias = table->alias;
    else {
      alias = share->table_name.str;
    }
  }
  const LEX_CSTRING *const db =
      table_list->schema_table ? &INFORMATION_SCHEMA_NAME : &table->s->db;
  if (show_database) {
    if (!thd->db().str || strcmp(db->str, thd->db().str)) {
      append_identifier(thd, packet, db->str, db->length);
      packet->append(STRING_WITH_LEN("."));
    }
  }
  append_identifier(thd, packet, alias, strlen(alias));

  packet->append(STRING_WITH_LEN(" ( "));

  const bool skip_gipk = table_has_generated_invisible_primary_key(table);

  Field **first_field = table->field;
  Field **ptr, *field;
  /*
    Generated invisible primary key column is placed at the first position.
    So skip first column when skip_gipk is set.
  */
  assert(!table_has_generated_invisible_primary_key(table) ||
         is_generated_invisible_primary_key_column_name(
             (*first_field)->field_name));
  if (skip_gipk) first_field++;

  for (ptr = first_field; (field = *ptr); ptr++) {
    // Skip hidden system fields.
    if (field->is_hidden_by_system()) continue;

    if (strncmp(field->field_name, STRING_WITH_LEN(HIDDEN_IMV_PREFIX)) == 0) {
      continue;
    }

    if (ptr != first_field) packet->append(STRING_WITH_LEN(",\n"));

    packet->append(STRING_WITH_LEN("  "));
    append_identifier(thd, packet, field->field_name,
                      strlen(field->field_name));
    packet->append(' ');
  }
  packet->append(STRING_WITH_LEN(" ) BUILD"));

  if (!share->mview_info) {
    my_error(ER_BAD_TABLE_ERROR, MYF(0), alias);
    return true;
  }

  if (share->mview_info->build_clasue == 0) {
    packet->append(STRING_WITH_LEN(" DEFERRED"));
  } else {
    packet->append(STRING_WITH_LEN(" IMMEDIATE"));
  }

  packet->append(STRING_WITH_LEN(" REFRESH"));

  if (share->mview_info->flush_mode == 0) {
    packet->append(STRING_WITH_LEN(" COMPLETE"));
  } else {
    packet->append(STRING_WITH_LEN(" FAST"));
  }

  packet->append(STRING_WITH_LEN(" ON DEMAND"));

  if (skip_gipk) {
    // use for mgr or dump
    // redo if thd has set gipk
    packet->append(STRING_WITH_LEN(" WITH ROWID"));
  }

  if (!for_show_create_stmt && thd->rli_slave == nullptr && create_info_arg) {
    packet->append(STRING_WITH_LEN(" ATTRIBUTE "));
    append_unescaped(packet, create_info_arg->m_mv_info->table_ref.str,
                     create_info_arg->m_mv_info->table_ref.length);
  }

  packet->append(STRING_WITH_LEN(" AS "));

  packet->append(share->mview_info->create_view_define.str,
                 share->mview_info->create_view_define.length);
  return false;
}

/**
 * @brief
 *
 * 1. use derived_column_names
 *
 *    (a, b ,c )     select x , y , z from t1
 *
 *   select x as  a, y  as b , z as c from t1
 *
 *
 *  NOTE: HIDDEN_IMV_PREFIX use for imv rewrite
 *         add user hidden column prefix
 *
 *        pk  as HIDDEN_IMV_PK_COLUMN
 *
 *
 * 2. if mv use  fast
 *     get all mlog
 *     set new ref val
 *     check query can use fast mode
 *
 * 3. if immediate
 *
 *    set mlog table read_set / write_set
 *
 * @param thd
 * @param lex
 *
 * @return true
 * @return false
 */
bool Query_result_materialized_view_create::derived_column_rename(THD *thd,
                                                                  LEX *lex) {
  assert(create_info && create_info->m_mv_info);

  const mem_root_deque<Item *> &unit_items =
      *lex->unit->get_unit_column_types();

  /* rename to mv colname  */
  if (select_tables->derived_column_names() &&
      select_tables->derived_column_names()->size()) {
    const Create_col_name_list &tmp_table_col_names =
        *select_tables->derived_column_names();
    {
      if (check_duplicate_names(&tmp_table_col_names, lex->query_block->fields,
                                true))
        return true;

      uint fieldnr = 0;

      for (Item *item : VisibleFields(unit_items)) {
        const char *s = item->item_name.ptr();
        size_t l = item->item_name.length();

        LEX_CSTRING &other_name =
            const_cast<LEX_CSTRING &>(tmp_table_col_names[fieldnr]);

        // should rename this
        if (strncmp(other_name.str, STRING_WITH_LEN(HIDDEN_IMV_PREFIX)) != 0) {
          item->item_name.set(other_name.str, other_name.length);
        } else {
          if (item->type() == Item::SUM_FUNC_ITEM) {
            auto sum_it = down_cast<Item_sum *>(item);
            LEX_STRING new_name;
            if (sum_it->sum_func() == Item_sum::SUM_FUNC) {
              if (aggr_func_name_alias(thd, &new_name, sum_it->arguments()[0],
                                       false))
                return true;
            } else if (sum_it->sum_func() == Item_sum::COUNT_FUNC) {
              if (aggr_func_name_alias(thd, &new_name, sum_it->arguments()[0]))
                return true;
            }

            item->item_name.set(new_name.str, new_name.length);
          }
        }

        other_name.str = s;
        other_name.length = l;
        fieldnr++;
      }
    }
  } else {
    // has HIDDEN_IMV_PREFIX column need to rewrite

    for (Item *item : VisibleFields(unit_items)) {
      const char *s = item->item_name.ptr();
      if (strncmp(s, STRING_WITH_LEN(HIDDEN_IMV_PREFIX)) == 0) {
        if (item->type() == Item::SUM_FUNC_ITEM) {
          auto sum_it = down_cast<Item_sum *>(item);
          LEX_STRING new_name;
          if (sum_it->sum_func() == Item_sum::SUM_FUNC) {
            if (aggr_func_name_alias(thd, &new_name, sum_it->arguments()[0],
                                     false))
              return true;
          } else if (sum_it->sum_func() == Item_sum::COUNT_FUNC) {
            if (aggr_func_name_alias(thd, &new_name, sum_it->arguments()[0]))
              return true;
          }

          item->item_name.set(new_name.str, new_name.length);
        }
      }
    }
  }

  // delta check query
  // table mlog check
  // init
  // lock all query table  add for update query
  if (create_info->m_mv_info->flush_mode != 0) {
    std::vector<dd::String_type> parent_dbs, parent_names, children_dbs,
        children_names;
    std::vector<uint64_t> refs;
    for (const Table_ref *table = select_tables; table != nullptr;
         table = table->next_global) {
      parent_dbs.push_back(dd::String_type(table->db, table->db_length));
      parent_names.push_back(
          dd::String_type(table->table_name, table->table_name_length));

      /// get primary key info for generoator imv SQL
    }
    dd::cache::Dictionary_client::Auto_releaser releaser(thd->dd_client());

    if (thd->dd_client()->find_all_mlog(&parent_dbs, &parent_names,
                                        &children_dbs, &children_names,
                                        &refs)) {
      my_error(ER_INVALID_MATERIALIZED_VIEW, MYF(0), parent_names[0].c_str(),
               "find mlog failed");
      return true;
    }

    if (create_info->m_mv_info->table_ref.length == 0) {
      // find all mlog

      // mlog each ref offset value
      for (size_t i = 0; i < children_names.size(); i++) {
        if (refs[i] == UINT64_MAX) {
          my_error(ER_INVALID_MATERIALIZED_VIEW, MYF(0), "mlog",
                   "reference max");
          return true;
        }
        auto ref_offset = __builtin_ffsll(~refs[i]) - 1;

        flush_info.add(children_dbs[i], children_names[i], ref_offset);
      }

      flush_info.mlogs_to_string(thd, &create_info->m_mv_info->table_ref);
    } else {
      // sync
      if (string_to_mlog_status(
              string(create_info->m_mv_info->table_ref.str,
                     create_info->m_mv_info->table_ref.length),
              greatdb::Mview_mlogs::add_mlog, &flush_info)) {
        return true;
      }
      // check is real mlog exits
      for (size_t i = 0; i < flush_info.get_mlogs_size(); i++) {
        if (flush_info.find(children_dbs[i], children_names[i])) {
          my_error(ER_INVALID_MATERIALIZED_VIEW, MYF(0), "mlog",
                   "ATTRIBUTE mlog not match");
          return true;
        }
      }
    }

    // check limit and generator imv sql

    greatdb::CheckVisitor check_query(thd);
    if (lex->unit->accept(&check_query)) {
      return true;
    }

    if (thd->is_error()) {
      return true;
    }

    /***
     *  generator SQL
     */
    if (check_query.transform_func(lex->unit->first_query_block())) {
      return true;
    }
    /*
     * generator hidden primary key map
     */
    if (check_query.add_hidden_primary_key_map(
            &create_info->m_mv_info->derived_columns)) {
      return true;
    }
    /**
     * generator primary key
     */
    if (!check_query.key_parts.is_empty()) {
      Key_spec *key = new (thd->mem_root) Key_spec(
          thd->mem_root, KEYTYPE_PRIMARY, NULL_CSTR, &default_key_create_info,
          false, true, check_query.key_parts);
      if (key == nullptr || alter_info->key_list.push_back(key))
        return true;  // OOM
    }

    String query_view_str;
    lex->unit->print(
        thd, &query_view_str,
        enum_query_type(QT_TO_SYSTEM_CHARSET | QT_NO_DATA_EXPANSION));
    query_view_str.append('\0');
    String query_view_str_utf8;
    uint dummy_err;
    if (query_view_str_utf8.copy(query_view_str.ptr(), query_view_str.length(),
                                 query_view_str.charset(),
                                 &my_charset_utf8mb4_general_ci, &dummy_err)) {
      return true;
    }

    auto utf8_view =
        thd->strmake(query_view_str_utf8.ptr(), query_view_str_utf8.length());
    create_info->m_mv_info->create_view_query_block.str = utf8_view;

    create_info->m_mv_info->create_view_query_block.length =
        query_view_str_utf8.length();

    /// immedaite
    if (create_info->m_mv_info->build_clasue == 1) {
      // open table use for update
      if (flush_info.add_tables_ref(thd)) return true;
    }
  }

  // mgr need hidden pk so forced create primary key in other mode
  if (create_info->m_mv_info->with_row_id != 0 &&
      is_candidate_table_for_invisible_primary_key_generation(create_info,
                                                              alter_info)) {
    if (validate_and_generate_invisible_primary_key(thd, alter_info)) {
      return true;
    }
  }

  return false;
}

/***
 *  create materialized view without insert data by slave or binlog apply
 *   like Query_result_create::binlog_show_create_table
 *
 *   create table with materialized_view without data , "SQL" string
 *
 *
 */
bool Query_result_materialized_view_create::
    binlog_show_create_materialized_view(THD *thd) {
  DBUG_TRACE;

  Table_ref *save_next_global = create_table->next_global;
  create_table->next_global = select_tables;
  const int error = thd->decide_logging_format(create_table);
  create_table->next_global = save_next_global;

  if (error) return error;

  create_table->table->set_binlog_drop_if_temp(
      !thd->is_current_stmt_binlog_disabled() &&
      !thd->is_current_stmt_binlog_format_row());

  if (!thd->is_current_stmt_binlog_format_row() || table->s->tmp_table)
    return 0;

  /*
    Note 1: In RBR mode, we generate a CREATE TABLE statement for the
    created table by calling store_create_info() (behaves as SHOW
    CREATE TABLE). The 'CREATE TABLE' event will be put in the
    binlog statement cache with an Anonymous_gtid_log_event, and
    any subsequent events (e.g., table-map events and rows event)
    will be put in the binlog transaction cache with an
    Anonymous_gtid_log_event. So that the 'CREATE...SELECT'
    statement is logged as:
      Anonymous_gtid_log_event
      CREATE TABLE event
      Anonymous_gtid_log_event
      BEGIN
      rows event
      COMMIT

    We write the CREATE TABLE statement here and not in prepare()
    since there potentially are sub-selects or accesses to information
    schema that will do a close_thread_tables(), destroying the
    statement transaction cache.
  */

  char buf[2048];
  String query(buf, sizeof(buf), system_charset_info);
  int result;
  Table_ref tmp_table_list(table);

  query.length(0);  // Have to zero it since constructor doesn't

  // replace to materialized_view
  result = store_materialized_view_create_info(thd, &tmp_table_list, &query,
                                               create_info,
                                               /* show_database */ true,
                                               /* SHOW CREATE TABLE */ false);

  assert(result == 0); /* store_create_info() always return 0 */

  if (mysql_bin_log.is_open()) {
    DEBUG_SYNC(thd, "create_select_before_write_create_event");
    /*
      Binary log layer has special code to handle rollback of CREATE TABLE
      SELECT in RBR mode - it truncates statement cache in this case.

      If SE is transactional and supports atomic DDL, we log the Query_log
      event into transactional cache and do not flush it immediately.
    */
    int errcode = query_error_code(thd, thd->killed == THD::NOT_KILLED);

    bool is_trans = false;
    bool direct = true;
    if (get_default_handlerton(thd, thd->lex->create_info->db_type)->flags &
        HTON_SUPPORTS_ATOMIC_DDL) {
      is_trans = true;
      direct = false;
    }
    result = thd->binlog_query(THD::STMT_QUERY_TYPE, query.ptr(),
                               query.length(), is_trans, direct,
                               /* suppress_use */ false, errcode);
    DEBUG_SYNC(thd, "create_select_after_write_create_event");
  }
  return result;
}

/**
 * @brief
 *
 *  1. mlog will open table
 *
 *  2. m_plock need to merged
 *    (if plog has merged  can't flush dd before Query_result free)
 *
 *  3. query lock and  default value for select
 *
 * @param thd
 *
 * @return true
 * @return false
 */
bool Query_result_materialized_view_create::start_execution(THD *thd) {
  DBUG_TRACE;

  MYSQL_LOCK *extra_lock = nullptr;

  table->reginfo.lock_type = TL_WRITE;

  /*
    mysql_lock_tables() below should never fail with request to reopen table
    since it won't wait for the table lock (we have exclusive metadata lock on
    the table) and thus can't get aborted.
  */
  if (!(extra_lock = mysql_lock_tables(thd, &table, 1, 0)) ||
      binlog_show_create_materialized_view(thd)) {
    if (extra_lock) {
      mysql_unlock_tables(thd, extra_lock);
      extra_lock = nullptr;
    }
    return true;
  }
  if (extra_lock) {
    assert(m_plock == nullptr);

    if (create_info->options & HA_LEX_CREATE_TMP_TABLE) {
      m_plock = &m_lock;
    } else
      m_plock = &thd->extra_lock;

    *m_plock = extra_lock;
  }

  // immdeiate update
  if (flush_info.mlog_array != nullptr) {
    // lock mlog

    if (flush_info.lock_mlog_tables(thd, m_plock)) {
      return true;
    }
  }

  /* Mark all fields that are given values */
  for (Field **f = table_fields; *f != nullptr; f++) {
    bitmap_set_bit(table->write_set, (*f)->field_index());
    bitmap_set_bit(table->fields_set_during_insert, (*f)->field_index());
  }

  // Set up an empty bitmap of function defaults
  if (info.add_function_default_columns(table, table->write_set)) return true;

  if (info.add_function_default_columns(table, table->fields_set_during_insert))
    return true;

  table->next_number_field = table->found_next_number_field;

  restore_record(table, s->default_values);  // Get empty record
  thd->num_truncated_fields = 0;

  const enum_duplicates duplicate_handling = info.get_duplicate_handling();

  if (thd->lex->is_ignore() || duplicate_handling != DUP_ERROR)
    table->file->ha_extra(HA_EXTRA_IGNORE_DUP_KEY);
  if (duplicate_handling == DUP_REPLACE &&
      (!table->triggers || !table->triggers->has_delete_triggers()))
    table->file->ha_extra(HA_EXTRA_WRITE_CAN_REPLACE);
  if (duplicate_handling == DUP_UPDATE)
    table->file->ha_extra(HA_EXTRA_INSERT_WITH_UPDATE);
  if (thd->locked_tables_mode <= LTM_LOCK_TABLES) {
    table->file->ha_start_bulk_insert((ha_rows)0);
    bulk_insert_started = true;
  }

  const enum_check_fields save_check_for_truncated_fields =
      thd->check_for_truncated_fields;
  thd->check_for_truncated_fields = CHECK_FIELD_WARN;

  if (check_that_all_fields_are_given_values(thd, table, table_list))
    return true;

  thd->check_for_truncated_fields = save_check_for_truncated_fields;

  table->mark_columns_needed_for_insert(thd);
  return false;
}

/**
 *  Query_result_materialized_view_create copy from Query_result_create
 *
 *   update all record  mlog ref valuew after create table select
 *
 */
bool Query_result_materialized_view_create::send_eof(THD *thd) {
  // update mlog ref field value
  flush_info.updateRefValue(thd, true);

  bool error = false;

  // update all mlog tables ref values to new ref
  // means has already consume if each record ref values = metadata.ref
  //
  if (flush_info.updateMetaDataRefValue(thd, true)) error = true;

  {
    Uncommitted_tables_guard uncommitted_tables(thd);

    /*
      We can rollback target table creation by dropping it even for SEs which
      don't support atomic DDL. So there is no need to commit changes to
      metadata of dependent views below.
      Moreover, doing these intermediate commits can be harmful as in RBR mode
      they will flush CREATE TABLE event and row events to the binary log
      which, in case of later error, will create discrepancy with rollback of
      statement by target table removal.
      Such intermediate commits also wipe out transaction's unsafe-to-rollback
      flags which leads to broken assertions in
      Query_result_insert::send_eof().
    */
    if (!error)
      error = update_referencing_views_metadata(thd, create_table, false,
                                                &uncommitted_tables);
  }
  DBUG_EXECUTE_IF("crash_before_create_select_insert", DBUG_SUICIDE(););

  if (!error) error = Query_result_insert::send_eof(thd);

  if (error)
    abort_result_set(thd);
  else {
    DBUG_EXECUTE_IF("crash_after_create_select_insert", DBUG_SUICIDE(););

    // update ref mlog ref values
    error = false;
    //
    if (error) {
      abort_result_set(thd);
    } else {
      if (create_info->m_transactional_ddl) {
        thd->m_transactional_ddl.init(
            create_table->db, create_table->table_name, create_info->db_type);
      }

      /*
        Do an implicit commit at end of statement for non-temporary tables.
        This can fail in which case rollback will be done automatically.
        For storage engines supporting atomic DDL this will revert table
        creation in SE, data-dictionary and binlog changes.
        For other storage engines we might end-up with partially consistent
        state between data-dictionary, SE, data in table and binary log.
        However this should be extremely rare.
      */
      if (!table->s->tmp_table) {
        thd->get_stmt_da()->set_overwrite_status(true);
        error = trans_commit_stmt(thd) || create_info->m_transactional_ddl
                    ? false
                    : trans_commit_implicit(thd);
        thd->get_stmt_da()->set_overwrite_status(false);
      }

      // lock by start execute
      if (!error && m_plock) {
        mysql_unlock_tables(thd, *m_plock);
        *m_plock = nullptr;
        m_plock = nullptr;
      }

      if (!error && m_post_ddl_ht && !create_info->m_transactional_ddl) {
        m_post_ddl_ht->post_ddl(thd);
      }
    }
  }
  return error;
}

void Query_result_materialized_view_create::cleanup() {
  Query_result_create::cleanup();
  flush_info.cleanup();
}

/**
 * @brief like populate_table
 *
 *  1.  query transform and add new column or name
 *
 *  2. create table use query_reuslt
 *
 *  3. lock table
 *
 *  4. if table execute immediate
 *        execute query
 *           update base mlog ref value
 *  5. not immediate
 *       only create imv table without execute query
 *
 *
 * @param thd
 * @param lex
 *
 * @return true
 * @return false
 */
bool populate_materialized_view_and_update_table(THD *thd, LEX *lex) {
  Query_expression *const unit = lex->unit;

  if (lex->set_var_list.elements && resolve_var_assignments(thd, lex))
    return true;

  lex->set_exec_started();

  auto query_result =
      down_cast<Query_result_materialized_view_create *>(unit->query_result());
  if (query_result == nullptr) return true;

  if (query_result->derived_column_rename(thd, lex)) {
    return true;
  }

  auto mv = query_result->get_materialized_view_info();

  /*
    Table creation may perform an intermediate commit and must therefore
    be performed before locking the tables in the query expression.
  */
  if (query_result->create_table_for_query_block(thd)) return true;

  if (lock_tables(thd, lex->query_tables, lex->table_count, 0)) return true;

  if (mv->build_clasue == 1 && thd->rli_slave == nullptr) {
    if (unit->optimize(thd, nullptr, true, /*finalize_access_paths=*/true))
      return true;

    // Calculate the current statement cost.
    accumulate_statement_cost(lex);

    // Perform secondary engine optimizations, if needed.
    if (optimize_secondary_engine(thd)) return true;

    if (unit->execute(thd)) return true;

    notify_plugins_after_select(thd, lex->m_sql_cmd);
  } else {
    // lock mlog table and query table
    // send_eof  will update mlog metadata
    // send_eof  depends on start executeion
    if (query_result->start_execution(thd)) return true;

    if (query_result->send_eof(thd)) return true;
  }

  return false;
}

//////////////////////////////////////////////////////////////////////////////////////////

/**

  apply delta
  setup0:

   copy delta mlog;


  setup1:
     merge mlog

      1. first insert

          CREATE TEMPORARY TABLE `delta_mlog$t1_insert` AS
          WITH final_inserts AS (
              SELECT x, MAX(id) as insert_id
              FROM mlog$t1
              WHERE op = 1
              GROUP BY x
              HAVING NOT EXISTS (
                  SELECT 1 FROM mlog$t1 d
                  WHERE d.x = mlog$t1.x
                    AND d.op = -1
                    AND d.id > MAX(mlog$_t1.id)
              )
          )
          SELECT
             m.*
          FROM mlog$_t1 m
          JOIN final_inserts fi ON m.id = fi.insert_id ;


      2. last delete


        CREATE TEMPORARY TABLE `delta_mlog$t1_delete` AS
        WITH keyinfo AS (
          SELECT
            id,
            MIN(CASE WHEN op = 1 THEN id END) OVER (PARTITION BY x) AS
  first_insert_id, MIN(CASE WHEN op = -1 THEN id END) OVER (PARTITION BY x) AS
  first_delete_id FROM mlog$t1
        )
        SELECT m.*
        FROM mlog$_t1 m
        JOIN keyinfo k USING (id)
        WHERE m.op = -1
          AND k.id = k.first_delete_id
          AND (k.first_insert_id IS NULL OR k.first_insert_id > k.id);


  setup2:

      check query has agg func ?

      query replace temp table if not

      example:

      create view v1
          select x from t1 where y > 20;

      1.  delete query

          DELETE FROM "test"."mv2" ml
          inner join  "test"."delta_mlog$_t1_delete" d
          ON ml.x = d.x

      2.  insert select

          insert into v1 (x)   select x from delta_mlog$t1_insert where y > 20

  setup3:

      query has group by ?

      use update agg func

      example:
      create view 2 as select avg(x) from t1

            create view mvx as select avg(z)/sum(y) from t1;
         =>  select avg(z)/sum(y) , hidden_count(z) hidden_sum(z),
                   hidden_sum(y) ,hidden_count(y) from t1;



       update v2  as imv join (
          select sum(y) from
          `delta_mlog$t1_delta`
      ) as delta_delete
      set v2.sum` =  v2.sum - delta_delete.sum
          v2.count =  v2.count - delta_delete.count


       update v2  as imv join (
          select sum(y) from
          `delta_mlog$t1_delta`
      ) as delta_insert
      set v2.sum` =  v2.sum + delta_insert.sum
          v2.count =  v2.count + delta_insert.count


      update v2 set  v2.avg(x) = v2.sum / v2.count


  setup4:

      agg+ group by


      1.  update delete delta


          with delta_delete as (
              select count(z) as count_z ,
                    sum(z) as sum_z,
                    x from  `delta_mlog$t1_delete` group by x
          )
          update v3    join  `delta_delete`  on   delta_delete.x = v3.x    set
  v3.count_z = v3.count_z -   delta_delete.count_z , v3.sum_z = v3.sum_z -
  delta_delete.count_z  ;

      2.  remove if group is zero

        delete from v3 where v3.count_z = 0;


      3.  update insert delta

      WITH delta_insert AS (
          SELECT
              x,
              COUNT(z) AS count_z,
              SUM(z)   AS sum_z
          FROM delta_mlog$t1_insert
          GROUP BY x
      )
      UPDATE v3
      JOIN delta_insert i ON v3.x = i.x
      SET
          v3.count_z = v3.count_z + i.count_z,
          v3.sum_z   = v3.sum_z   + i.sum_z,

      4. insert new group

        insert INTO v3 (x, count_z, sum_z)
        WITH delta_insert AS (
            SELECT
                x,
                COUNT(z) AS count_z,
                SUM(z)   AS sum_z
            FROM `delta_mlog$t1_insert`
            GROUP BY x
        )
        SELECT i.x, i.count_z, i.sum_z
        FROM delta_insert i
        WHERE NOT EXISTS (
            SELECT 1 FROM v3 v WHERE v.x = i.x
        );


      5. update v2 set  v2.avg(x) = v2.sum / v2.count

   setup5:

     update mlog ref ++


 */
template <typename String, typename Container>
String join_table_column_list(
    const Container &columns, const String &alias = String{},
    typename String::value_type identifier_char =
        static_cast<typename String::value_type>(DENTIFIER_CHAR_SEQ)) {
  if (columns.empty()) {
    return String{};
  }

  String result;
  auto it = columns.begin();
  auto end = columns.end();

  // Helper lambda to append a quoted column, optionally qualified with alias
  auto append_qualified = [&](const typename Container::value_type &col_name) {
    if (!alias.empty()) {
      append_quoted(result, alias, identifier_char);
      result += static_cast<typename String::value_type>('.');
    }
    append_quoted(result, col_name, identifier_char);
  };

  // First column
  append_qualified(*it);
  ++it;

  // Remaining columns
  const typename String::value_type sep[] = {
      static_cast<typename String::value_type>(','),
      static_cast<typename String::value_type>(' ')};

  for (; it != end; ++it) {
    result.append(sep, sep + 2);  // append ", "
    append_qualified(*it);
  }

  return result;
}

template <typename Container, typename NameMap, typename String>
String join_equal_cond_list(
    const Container &columns, const NameMap *column_map,
    const String &tableA_alias = String{},
    const String &tableB_alias = String{}, bool use_map = true,
    typename String::value_type identifier_char =
        static_cast<typename String::value_type>(DENTIFIER_CHAR_SEQ)) {
  if (columns.empty()) {
    return String{};
  }

  if (use_map && (column_map == nullptr || column_map->empty())) {
    use_map = false;
  }

  String result;
  auto it = columns.begin();
  auto end = columns.end();

  auto append_qualified_ref = [&](const String &alias, const auto &col_name) {
    if (!alias.empty()) {
      result += alias;
      result += '.';
    }
    append_quoted(result, col_name, identifier_char);
  };

  auto get_right_col = [&](const auto &left_col) {
    if (use_map && column_map) {
      auto found = column_map->find(left_col);
      if (found != column_map->end()) {
        return found->second;
      }
    }
    return left_col;
  };

  // First condition
  append_qualified_ref(tableA_alias, *it);
  result += " = ";
  append_qualified_ref(tableB_alias, get_right_col(*it));

  ++it;
  for (; it != end; ++it) {
    result += " and ";
    append_qualified_ref(tableA_alias, *it);
    result += " = ";
    append_qualified_ref(tableB_alias, get_right_col(*it));
  }

  return result;
}

class MlogInfo {
 public:
  ColumnNameArray pk_column_name;
  string mlog_name;
  ulonglong ref_value{0};

  ColumnNameMap *column_map;
  ulonglong delta_delete_count{0};
  ulonglong delta_insert_count{0};

  MlogInfo(THD *thd, string mlog, ulonglong ref, ColumnNameMap *name_map)
      : pk_column_name(thd->mem_root),
        mlog_name(mlog),
        ref_value(ref),
        column_map(name_map),
        delta_delete_count(0),
        delta_insert_count(0) {}

  string get_pk_list(const string &table_alias) {
    return join_table_column_list(pk_column_name, table_alias);
  }

  string get_pk_compare(string tableA_alias, string tableB_alias,
                        bool use_map = true) {
    return join_equal_cond_list(pk_column_name, column_map, tableA_alias,
                                tableB_alias, use_map);
  }
};
using MlogMap = std::map<std::pair<string, string>, std::unique_ptr<MlogInfo>>;

/**
 * @brief only use print interface to print Item string
 *  aggr func used hidden fields
 *
 *  expr:  func(Item_print_rewrite)
 *                   Item_sum        hidden_imv_sum
 *                   Item_count     hidden_imv_count
 *                   Item_avg   ==>   hidden_imv_sum  / hidden_imv_count
 *
 *    avg item print to replace   sum / count
 *
 *
 */
class Item_print_rewrite : public Item_func {
  Item_sum *ref;

 public:
  Item_print_rewrite(Item_sum *it) : ref(it) {}

  Item_print_rewrite(const Item_print_rewrite &) = delete;
  Item_print_rewrite &operator=(const Item_print_rewrite &) = delete;

  enum Type type() const override { return FIELD_ITEM; }  // or FUNC_ITEM

  /// just user for print
  void print(const THD *thd, String *str,
             enum_query_type query_type) const override {
    if (ref->sum_func() == Item_sum::AVG_FUNC) {
      LEX_STRING sum_item_alias;
      if (aggr_func_name_alias(current_thd, &sum_item_alias,
                               ref->arguments()[0], false)) {
        return;
      }
      LEX_STRING count_item_alias;

      if (aggr_func_name_alias(current_thd, &count_item_alias,
                               ref->arguments()[0]))
        return;

      append_identifier(thd, str, sum_item_alias.str, sum_item_alias.length);

      str->append(STRING_WITH_LEN(" / "));

      append_identifier(thd, str, count_item_alias.str,
                        count_item_alias.length);

    } else if (ref->sum_func() == Item_sum::SUM_FUNC) {
      LEX_STRING sum_item_alias;
      if (aggr_func_name_alias(current_thd, &sum_item_alias,
                               ref->arguments()[0], false)) {
        return;
      }
      append_identifier(thd, str, sum_item_alias.str, sum_item_alias.length);

    } else if (ref->sum_func() == Item_sum::COUNT_FUNC) {
      LEX_STRING count_item_alias;
      if (aggr_func_name_alias(current_thd, &count_item_alias,
                               ref->arguments()[0])) {
        return;
      }
      append_identifier(thd, str, count_item_alias.str,
                        count_item_alias.length);
    } else {
      ref->print(thd, str, query_type);
    }
  }
  const char *func_name() const override { return "print_rewrite"; }

  bool fix_fields(THD *, Item **) override {
    fixed = true;
    return false;
  }

  double val_real() override {
    assert(false);
    my_error(ER_INTERNAL_ERROR, MYF(0),
             "Item_print_rewrite: val_real() called illegally");
    null_value = true;
    return 0.0;
  }

  longlong val_int() override {
    assert(false);
    my_error(ER_INTERNAL_ERROR, MYF(0),
             "Item_print_rewrite: val_int() called illegally");
    null_value = true;
    return 0;
  }

  String *val_str(String *) override {
    assert(false);
    my_error(ER_INTERNAL_ERROR, MYF(0),
             "Item_print_rewrite: val_str() called illegally");
    null_value = true;
    return nullptr;
  }

  my_decimal *val_decimal(my_decimal *) override {
    assert(false);
    my_error(ER_INTERNAL_ERROR, MYF(0),
             "Item_print_rewrite: val_decimal() called illegally");
    null_value = true;
    return nullptr;
  }
  bool get_date(MYSQL_TIME *, my_time_flags_t) override {
    assert(false);
    my_error(ER_INTERNAL_ERROR, MYF(0),
             "Item_print_rewrite: get_date() called illegally");
    return true;  // error
  }

  bool get_time(MYSQL_TIME *) override {
    assert(false);
    my_error(ER_INTERNAL_ERROR, MYF(0),
             "Item_print_rewrite: get_time() called illegally");
    return true;
  }

  enum Item_result result_type() const override { return INVALID_RESULT; }
};

/**
 *  rewrite query
 *
 *   1. agg func avg need rewrite to sum and count
 *
 *   2. table name rewrite   --> delta_temp name as alias   alias (src_table
 * or alias )
 *
 *   3. field ref
 *
 */

/// @brief
class RewriteVisitor : public Select_lex_visitor {
  THD *m_thd;
  MlogMap *mlog_info;
  String *db_name;
  String *imv_name;
  //    "db"."tb"
  string db_imv_name;

  mem_root_deque<string> sum_funcs;
  mem_root_unordered_map<string, string> scalar_funcs;
  // simple query save all columns
  mem_root_deque<string> group_by_fields;

 public:
  String rewrite_imv_define;
  RewriteVisitor(THD *thd, String *db, String *imv, MlogMap *info)
      : m_thd(thd),
        mlog_info(info),
        db_name(db),
        imv_name(imv),
        sum_funcs(thd->mem_root),
        scalar_funcs(thd->mem_root),
        group_by_fields(thd->mem_root) {
    append_quoted(db_imv_name, db->c_ptr_quick(), DENTIFIER_CHAR_SEQ);
    db_imv_name += '.';
    append_quoted(db_imv_name, imv->c_ptr_quick(), DENTIFIER_CHAR_SEQ);
  }

  bool is_simple() {
    if (is_group_by || has_count || has_avg || has_sum) {
      return false;
    }
    return true;
  }

  bool has_group_by() { return is_group_by; }

  //
  // transform expr
  //   avg =  sum / count
  //
  bool transform_aggr(Item *it, string *str) {
    // change to print

    auto new_it = TransformItem(it, [&](Item *item) -> Item * {
      if (item->type() == Item::SUM_FUNC_ITEM) {
        auto rewrite = new (m_thd->mem_root)
            Item_print_rewrite(down_cast<Item_sum *>(item));
        if (rewrite == nullptr) return nullptr;
        return rewrite;
      }
      return item;
    });

    if (new_it == nullptr) {
      return true;
    }

    String item_name_str;
    new_it->print(m_thd, &item_name_str, QT_ORDINARY);

    *str = string(item_name_str.c_ptr(), item_name_str.length());

    return false;
  }

  // transform
  bool transform_func(LEX *lex) {
    auto qb = lex->unit->first_query_block();
    // Note: is only a parser ast tree without resolver
    assert(qb->outer_query_block() == nullptr);
    if (qb) {
      if (!is_simple()) {
        for (auto it = qb->fields.begin(); it != qb->fields.end();) {
          Item *expr = *it;

          if (expr->hidden) {
            ++it;
            continue;
          }

          //  scalar_funcs:
          //       any func:
          //
          //       imv.field = func(_hidden_) to update

          assert(!expr->item_name.is_autogenerated());
          auto item_name =
              string(expr->item_name.ptr(), expr->item_name.length());

          switch (expr->type()) {
            case Item::SUM_FUNC_ITEM: {
              auto ftype = down_cast<Item_sum *>(expr)->sum_func();

              if (ftype == Item_sum::AVG_FUNC) {
                string update_expr;
                if (transform_aggr(expr, &update_expr)) {
                  return true;
                }

                scalar_funcs[item_name] = update_expr;

                it = qb->fields.erase(it);
              } else {
                sum_funcs.push_back(item_name);
                ++it;
              }
            } break;
            case Item::FIELD_ITEM:
              // use for deleta count zero value
              group_by_fields.push_back(item_name);
              ++it;
              break;
            default:
              // expr ? or const
              if (expr->has_aggregation()) {
                // this item need to repalce hidden item name expr to update
                // this value
                string update_expr;
                if (transform_aggr(expr, &update_expr)) {
                  return true;
                }

                scalar_funcs[item_name] = update_expr;

                it = qb->fields.erase(it);

              } else {
                // ignore  const item or const expr
                // func(group by key())
                ++it;
              }
              //

              break;
          }
        }
      } else {
        // can't use imv define all columns
        // note this has primary key my_row_id use for mgr
        // and not need insert
        for (auto it = qb->fields.begin(); it != qb->fields.end(); it++) {
          Item *expr = *it;
          if (expr->hidden) {
            continue;
          }
          auto item_name =
              string(expr->item_name.ptr(), expr->item_name.length());
          group_by_fields.push_back(item_name);
        }
      }
    }

    rewrite_imv_define.length(0);
    qb->print(m_thd, &rewrite_imv_define, enum_query_type::QT_ONLY_QB_NAME);

    return false;
  }

  bool simple_query(QueryArray *result) {
    std::string delete_sql;
    //
    delete_sql.reserve(256 + mlog_info->size() * 150);

    // DELETE imv FROM imv
    delete_sql = "delete ";
    delete_sql += db_imv_name;
    delete_sql += " from ";
    delete_sql += db_imv_name;

    int alias_num = 0;
    for (auto itr = mlog_info->begin(); itr != mlog_info->end();
         ++itr, ++alias_num) {
      std::string alias_name = "d" + std::to_string(alias_num);
      auto cond = itr->second->get_pk_compare(alias_name, db_imv_name);
      // inner join `schema`.`delta_mlog_delete` AS d0 ON ...
      delete_sql += " inner join ";
      append_quoted(delete_sql, itr->first.first);
      delete_sql += '.';
      append_quoted(delete_sql, "delta_" + itr->second->mlog_name + "_delete");
      delete_sql += " AS ";
      delete_sql += alias_name;
      delete_sql += " ON ";
      delete_sql += cond;
    }
    result->push_back(std::move(delete_sql));

    string insert_sql("insert into ");

    insert_sql += db_imv_name;
    auto column_define = join_table_column_list(group_by_fields, string());
    insert_sql += " ( " + column_define + " ) " + delta_define_sql();
    result->push_back(insert_sql);

    return false;
  }

  std::string delta_define_sql(bool insert = true) {
    std::string define_sql(rewrite_imv_define.c_ptr(),
                           rewrite_imv_define.length());
    define_sql.reserve(define_sql.size() + mlog_info->size() * 80);

    for (auto itr = mlog_info->begin(); itr != mlog_info->end(); ++itr) {
      const auto &base_name = itr->second->mlog_name;
      std::string placeholder;
      append_quoted(placeholder, "{delta_" + base_name + "}");
      std::string replacement;
      if (insert) {
        append_quoted(replacement, "delta_" + base_name + "_insert");
      } else {
        append_quoted(replacement, "delta_" + base_name + "_delete");
      }
      boost::replace_all(define_sql, placeholder, replacement);
    }

    return define_sql;
  }

  bool update_delta_with_group_by(QueryArray *result) {
    ulonglong insert_count = 0;
    ulonglong delete_count = 0;

    for (auto itr = mlog_info->begin(); itr != mlog_info->end(); ++itr) {
      insert_count += itr->second->delta_insert_count;
      delete_count += itr->second->delta_delete_count;
    }
    std::map<string, string> empty;
    if (delete_count != 0) {
      string delete_sql;
      delete_sql.reserve(512);
      delete_sql.append(" UPDATE ");
      delete_sql.append(db_imv_name);
      delete_sql.append("AS imv JOIN ( ");
      delete_sql.append(delta_define_sql(false));

      delete_sql.append(" ) AS delta_delete  on ");
      /// group by key

      delete_sql += join_equal_cond_list(group_by_fields, &empty, string("imv"),
                                         string("delta_delete"), false);

      delete_sql.append(" SET imv.");

      auto it = sum_funcs.begin();
      // First element
      append_quoted(delete_sql, *it);

      delete_sql.append("= IFNULL(imv.");

      append_quoted(delete_sql, *it);
      delete_sql.append(" ,0) - IFNULL(delta_delete.");
      append_quoted(delete_sql, *it);
      delete_sql.append(" ,0)");

      for (++it; it != sum_funcs.end(); ++it) {
        delete_sql.append(",  imv.");
        append_quoted(delete_sql, *it);

        delete_sql.append("= IFNULL(imv.");
        append_quoted(delete_sql, *it);
        delete_sql.append(",0) - IFNULL(delta_delete.");
        append_quoted(delete_sql, *it);
        delete_sql.append(" ,0)");
      }
      result->push_back(delete_sql);

      // remove group is zero
      string delete_zero_sql;

      delete_zero_sql.append(" delete from  ");
      delete_zero_sql.append(db_imv_name);
      // count 0 base64
      delete_zero_sql.append(" where \"" HIDDEN_IMV_PREFIX
                             "count_MA==}\" = 0 ");
      //
      result->push_back(delete_zero_sql);
    }
    if (insert_count != 0) {
      string insert_sql;
      insert_sql.reserve(512);

      auto new_delta_define = delta_define_sql();

      insert_sql.append(" UPDATE ");
      insert_sql.append(db_imv_name);
      insert_sql.append(" AS imv JOIN  (");
      insert_sql.append(new_delta_define);
      insert_sql.append(") AS  delta_insert ON ");

      auto primary_key_cond =
          join_equal_cond_list(group_by_fields, &empty, string("imv"),
                               string("delta_insert"), false);
      insert_sql.append(primary_key_cond);

      insert_sql.append("SET imv.");
      auto it = sum_funcs.begin();
      append_quoted(insert_sql, *it);
      insert_sql.append(" =  IFNULL(imv.");

      append_quoted(insert_sql, *it);
      insert_sql.append(",0) + IFNULL(delta_insert.");
      append_quoted(insert_sql, *it);
      insert_sql.append(" ,0)");
      for (++it; it != sum_funcs.end(); ++it) {
        insert_sql.append(", imv.");
        append_quoted(insert_sql, *it);
        insert_sql.append(" = IFNULL(imv.");
        append_quoted(insert_sql, *it);
        insert_sql.append(",0) + IFNULL(delta_insert.");
        append_quoted(insert_sql, *it);
        insert_sql.append(" ,0)");
      }
      result->push_back(insert_sql);

      string insert_new_group_sql;

      insert_new_group_sql.reserve(512);

      insert_new_group_sql.append("insert into ");
      insert_new_group_sql.append(db_imv_name);

      auto column_define = join_table_column_list(group_by_fields, string());

      column_define += " , " + join_table_column_list(sum_funcs, string());

      insert_new_group_sql += " ( " + column_define + " )";

      insert_new_group_sql.append(" select ");

      auto delta_column_define =
          join_table_column_list(group_by_fields, string("delta_insert"));

      delta_column_define +=
          " , " + join_table_column_list(sum_funcs, string("delta_insert"));

      insert_new_group_sql += delta_column_define + " from ( " +
                              new_delta_define + " ) as delta_insert ";

      insert_new_group_sql.append(" where not exists ( select 1 from ");
      insert_new_group_sql.append(db_imv_name);
      insert_new_group_sql.append(" as imv  where  ");
      insert_new_group_sql.append(primary_key_cond);
      insert_new_group_sql.append(" ) ;");
      result->push_back(insert_new_group_sql);
    }

    // update expr

    if (!scalar_funcs.empty() && (delete_count + insert_count) != 0) {
      std::string update_sql;
      update_sql.reserve(256);

      update_sql.append("UPDATE ");
      update_sql.append(db_imv_name);
      update_sql.append(" SET ");

      auto itr = scalar_funcs.begin();

      append_quoted(update_sql, itr->first);

      update_sql.append(" = ");
      update_sql.append(itr->second);

      for (++itr; itr != scalar_funcs.end(); itr++) {
        update_sql.append(" , ");
        append_quoted(update_sql, itr->first);

        update_sql.append(" = ");
        update_sql.append(itr->second);
      }
      result->push_back(update_sql);
    }

    return false;
  }

  /**
   * @brief update_delta_without_group_by
   *
   *   generator sql with out group by
   *
   * @param result
   *
   * @return true
   * @return false
   */
  bool update_delta_without_group_by(QueryArray *result) {
    assert(!sum_funcs.empty());

    if (sum_funcs.empty()) {
      return true;
    }

    ulonglong insert_count = 0;
    ulonglong delete_count = 0;

    for (auto itr = mlog_info->begin(); itr != mlog_info->end(); ++itr) {
      insert_count += itr->second->delta_insert_count;
      delete_count += itr->second->delta_delete_count;
    }

    if (delete_count != 0) {
      string delete_sql;
      delete_sql.reserve(512);
      delete_sql.append(" UPDATE ");
      delete_sql.append(db_imv_name);
      delete_sql.append("AS imv JOIN ( ");
      delete_sql.append(delta_define_sql(false));

      delete_sql.append(" ) AS delta_delete SET imv.");

      auto it = sum_funcs.begin();
      // First element
      append_quoted(delete_sql, *it);

      delete_sql.append("= IFNULL(imv.");

      append_quoted(delete_sql, *it);
      delete_sql.append(" ,0) - IFNULL(delta_delete.");
      append_quoted(delete_sql, *it);
      delete_sql.append(" ,0)");

      for (++it; it != sum_funcs.end(); ++it) {
        delete_sql.append(",  imv.");
        append_quoted(delete_sql, *it);

        delete_sql.append("= IFNULL(imv.");
        append_quoted(delete_sql, *it);
        delete_sql.append(",0) - IFNULL(delta_delete.");
        append_quoted(delete_sql, *it);
        delete_sql.append(" ,0)");
      }
      result->push_back(delete_sql);
    }

    if (insert_count != 0) {
      string insert_sql;
      insert_sql.reserve(512);

      insert_sql.append(" UPDATE ");
      insert_sql.append(db_imv_name);
      insert_sql.append(" AS imv JOIN  (");
      insert_sql.append(delta_define_sql());
      insert_sql.append(") AS  delta_insert SET imv.");
      auto it = sum_funcs.begin();
      append_quoted(insert_sql, *it);
      insert_sql.append(" =  IFNULL(imv.");

      append_quoted(insert_sql, *it);
      insert_sql.append(",0) + IFNULL(delta_insert.");
      append_quoted(insert_sql, *it);
      insert_sql.append(" ,0)");
      for (++it; it != sum_funcs.end(); ++it) {
        insert_sql.append(", imv.");
        append_quoted(insert_sql, *it);
        insert_sql.append(" = IFNULL(imv.");
        append_quoted(insert_sql, *it);
        insert_sql.append(",0) + IFNULL(delta_insert.");
        append_quoted(insert_sql, *it);
        insert_sql.append(" ,0)");
      }
      result->push_back(insert_sql);
    }

    // update expr

    if (!scalar_funcs.empty() && (delete_count + insert_count) != 0) {
      std::string update_sql;
      update_sql.reserve(256);

      update_sql.append("UPDATE ");
      update_sql.append(db_imv_name);
      update_sql.append(" SET ");

      auto itr = scalar_funcs.begin();

      append_quoted(update_sql, itr->first);
      update_sql.append(" = ");
      update_sql.append(itr->second);

      for (++itr; itr != scalar_funcs.end(); itr++) {
        update_sql.append(" , ");
        append_quoted(update_sql, itr->first);
        update_sql.append(" = ");
        update_sql.append(itr->second);
      }
      result->push_back(update_sql);
    }
    return false;
  }

 protected:
  bool visit_item(Item *it) override {
    if (it->type() == Item::SUM_FUNC_ITEM) {
      auto it_sum = down_cast<Item_sum *>(it);
      if (it_sum == nullptr) return true;
      switch (it_sum->sum_func()) {
        case Item_sum::COUNT_FUNC: {
          has_count = true;
          break;
        }

        case Item_sum::SUM_FUNC: {
          has_sum = true;
          break;
        }
        case Item_sum::AVG_FUNC: {
          has_avg = true;
          // transform to  SUM/COUNT to print

          return false;
        }
        default:
          return true;
      }
    }
    return false;
  }

  bool visit_query_block(Query_block *qb) override {
    // qb->get_table_list();
    if (qb->is_grouped()) {
      is_group_by = true;
    }

    if (qb->has_tables()) {
      for (Table_ref *tbl = qb->get_table_list(); tbl; tbl = tbl->next_global) {
        // is_derived
        if (tbl->table_name == nullptr) {
          continue;
        }
        if (tbl->db == nullptr) {
          tbl->db = db_name->c_ptr();
          tbl->db_length = db_name->length();
        }

        auto key =
            std::make_pair(string{tbl->db, tbl->db_length},
                           string{tbl->table_name, tbl->table_name_length});
        auto iter = mlog_info->find(key);

        if (iter != mlog_info->end()) {
          // just replace to <delta_mlog> use for delete and insert to replace
          char delta_mlog[NAME_CHAR_LEN] = {0};

          auto delta_mlog_len =
              snprintf(delta_mlog, NAME_CHAR_LEN, "{delta_%s}",
                       iter->second->mlog_name.c_str());
          tbl->table_name_length = delta_mlog_len >= NAME_CHAR_LEN
                                       ? NAME_CHAR_LEN - 1
                                       : delta_mlog_len;
          tbl->table_name = m_thd->strmake(delta_mlog, tbl->table_name_length);
        }
      }
    }

    return false;
  }

  bool is_group_by{false};
  bool has_count{false};
  bool has_avg{false};
  bool has_sum{false};
};

bool parser_mv_define(THD *thd, const char *text, size_t length,
                      RewriteVisitor *visitor) {
  // A parsed view requires its own LEX object
  LEX *const old_lex = thd->lex;
  LEX *const view_lex = (LEX *)new (thd->mem_root) st_lex_local;
  if (!view_lex) return true;

  auto grd = create_scope_guard([&]() {
    if (thd->lex != old_lex) {
      lex_end(thd->lex);   // Terminate processing of view LEX
      thd->lex = old_lex;  // Needed for prepare_security
    }
  });
  thd->lex = view_lex;
  if (lex_start(thd)) {
    thd->lex = old_lex;
    return true;
  }

  Parser_state parser_state;

  if (parser_state.init(thd, text, length)) {
    return true;
  }
  Parser_state *old = thd->m_parser_state;
  thd->m_parser_state = &parser_state;

  // parser_state.m_lip.stmt_prepare_mode = old->m_lip.stmt_prepare_mode;
  parser_state.m_lip.multi_statements = false;  // A safety measure.
  parser_state.m_lip.m_digest = nullptr;

  bool result = thd->sql_parser();
  thd->m_parser_state = old;
  if (result) {
    return true;
  }

  if (view_lex->unit->accept(visitor)) {
    return true;
  }
  if (visitor->transform_func(view_lex)) {
    return true;
  }

  return false;
}

std::string_view extract_suffix(std::string_view table_name,
                                std::string_view prefix) {
  if (table_name.starts_with(prefix)) {
    return table_name.substr(prefix.length());
  }
  return table_name;
}

bool append_table_name_by_mlog(const THD *thd, String *to,
                               const Table_ref *mlog) {
  auto table = extract_suffix(
      std::string_view(mlog->table_name, mlog->table_name_length), MLOG_PREFIX);
  // not c style
  append_identifier(thd, to, table.data(), strlen(table.data()),
                    system_charset_info, thd->charset());

  return false;
}

/**
 * @brief Get the mlog name object
 *
 * @param thd
 * @param db_name
 * @param table_name
 * @param mlog_db [out]
 * @param mlog_name [out]
 *
 * @return true
 * @return false
 */
bool get_mlog_name(THD *thd, const string &db_name, const string &table_name,
                   dd::String_type &mlog_db, dd::String_type &mlog_name) {
  dd::cache::Dictionary_client::Auto_releaser releaser(thd->dd_client());
  const dd::Table *table = nullptr;
  if (thd->dd_client()->acquire(db_name.c_str(), table_name.data(), &table))
    return true;

  if (table->has_mlog()) {
    mlog_db = table->mlog_db_name();

    mlog_name = table->mlog_name();
  } else {
    my_error(ER_BAD_TABLE_ERROR, MYF(0), table_name.data());
    return true;
  }
  return false;
}

/**
 * @brief Get the table primary object
 *
 *  get table primary key define
 *

 * @param db_name
 * @param table_name
 * @param ref
 * @param thd
 * @param pk
 * @param table_column_map
 *
 * @return true
 * @return false
 */
bool get_table_primary(const string &db_name, const string &mlog_name,
                       ulonglong ref, THD *thd, MlogMap *pk,
                       TableColumnNameMap *table_column_map) {
  auto tb_name = extract_suffix(mlog_name, MLOG_PREFIX);
  MDL_request_list mdl_requests;
  if (push_mlog_mdl_request_to_list(thd, db_name.c_str(), tb_name.data(),
                                    &mdl_requests)) {
    return true;
  }

  if (!mdl_requests.is_empty() &&
      thd->mdl_context.acquire_locks(&mdl_requests,
                                     thd->variables.lock_wait_timeout))
    return true;

  dd::cache::Dictionary_client::Auto_releaser releaser(thd->dd_client());
  const dd::Table *mlog_ref_table = nullptr;
  if (thd->dd_client()->acquire(db_name.c_str(), tb_name.data(),
                                &mlog_ref_table))
    return true;
  if (mlog_ref_table == nullptr) {
    my_error(ER_BAD_TABLE_ERROR, MYF(0), tb_name.data());
    return true;
  }

  bool first_pk = true;

  ColumnNameMap *column_map_ptr = nullptr;
  if (table_column_map && !table_column_map->empty()) {
    auto itr = table_column_map->find(tb_name.data());
    if (itr != table_column_map->end()) {
      column_map_ptr = itr->second.get();
    }
  }

  auto mInfo = std::make_unique<MlogInfo>(thd, mlog_name, ref, column_map_ptr);

  for (auto col : mlog_ref_table->columns()) {
    if (dd::Column::CK_PRIMARY == col->column_key()) {
      if (first_pk) {
        first_pk = false;
      }

      mInfo->pk_column_name.push_back(col->name());
    }
  }

  if (first_pk) {
    //
    my_error(ER_TABLE_NO_PRIMARY_KEY, MYF(0), tb_name.data());
    return true;
  }

  pk->insert(std::make_pair(
      std::make_pair(db_name, string{tb_name.data(), tb_name.length()}),
      std::move(mInfo)));

  return false;
}

bool delta_data_generator(MlogMap &mlog_info, QueryArray *result) {
  char delta_mlog_insert[] =
      R"(CREATE TEMPORARY TABLE "{db_name}"."delta_{mlog}_insert" AS WITH final_inserts AS (SELECT {pk}, max("{id}") AS insert_id FROM "{db_name}"."{mlog}" ml WHERE	"{op}" = 1 GROUP BY	{pk} HAVING	NOT EXISTS (	SELECT		1	FROM	"{db_name}"."{mlog}" d	WHERE		{pk_cond}		AND d."{op}" = -1		AND d."{id}" > max(ml."{id}"))) SELECT	m.* FROM	"{db_name}"."{mlog}" m JOIN final_inserts fi ON	m."{id}" = fi.insert_id  and ( IFNULL(m."{ref}", 0) & (1<< {ref_value})) = 0 ;)";

  char delta_mlog_delete[] =
      R"(CREATE TEMPORARY TABLE "{db_name}"."delta_{mlog}_delete" AS WITH t AS (SELECT	ml.*,	MIN(CASE WHEN "{op}" = 1 THEN "{id}" END) OVER (PARTITION BY  {pk}) AS first_insert_id,	MIN(CASE WHEN "{op}" = -1 THEN "{id}" END) OVER (PARTITION BY {pk}) AS first_delete_id FROM	"{db_name}"."{mlog}" ml where ( IFNULL(ml."{ref}", 0) & (1 << {ref_value})) = 0 )  SELECT	t.* FROM	t WHERE	"{op}" =  -1	AND "{id}" = first_delete_id	AND (first_insert_id IS NULL		OR first_insert_id > "{id}" ) ;)";

  for (auto itr = mlog_info.begin(); itr != mlog_info.end(); itr++) {
    // itr->second->mlog_name
    string mlog_insert_sql(delta_mlog_insert);
    string mlog_deleta_sql(delta_mlog_delete);
    boost::replace_all(mlog_insert_sql, "{db_name}", itr->first.first);
    boost::replace_all(mlog_deleta_sql, "{db_name}", itr->first.first);
    boost::replace_all(mlog_insert_sql, "{mlog}", itr->second->mlog_name);
    boost::replace_all(mlog_deleta_sql, "{mlog}", itr->second->mlog_name);

    auto compare_cond = itr->second->get_pk_compare("ml", "d", false);

    boost::replace_all(mlog_insert_sql, "{pk_cond}", compare_cond);
    auto pk_list = itr->second->get_pk_list("ml");

    boost::replace_all(mlog_insert_sql, "{pk}", pk_list);
    boost::replace_all(mlog_deleta_sql, "{pk}", pk_list);

    boost::replace_all(mlog_insert_sql, "{ref_value}",
                       std::to_string(itr->second->ref_value));
    boost::replace_all(mlog_deleta_sql, "{ref_value}",
                       std::to_string(itr->second->ref_value));

    result->push_back(mlog_insert_sql);
    result->push_back(mlog_deleta_sql);
  }

  return false;
}

bool check_mlog_has_delta_data(Gdb_cmd_service &cmd_service,
                               MlogMap &mlog_info) {
  char delta_delete_count[] =
      R"(select count(*) from  "{db_name}"."delta_{mlog}_delete" ;)";
  char delta_insert_count[] =
      R"(select count(*) from  "{db_name}"."delta_{mlog}_insert" ;)";
  for (auto itr = mlog_info.begin(); itr != mlog_info.end(); itr++) {
    string mlog_insert_sql(delta_insert_count);
    string mlog_deleta_sql(delta_delete_count);

    boost::replace_all(mlog_insert_sql, "{db_name}", itr->first.first);
    boost::replace_all(mlog_deleta_sql, "{db_name}", itr->first.first);
    boost::replace_all(mlog_insert_sql, "{mlog}", itr->second->mlog_name);
    boost::replace_all(mlog_deleta_sql, "{mlog}", itr->second->mlog_name);

    auto &cb_data = cmd_service.get_cb_data();

    if (cmd_service.execute_sql(mlog_insert_sql) || cb_data.is_error()) {
      my_error(ER_MATERIALIZED_VIEW_REFLUSH, MYF(0),
               cb_data.error_msg().c_str());
      return true;
    }

    assert(cb_data.rows() == 1);

    itr->second->delta_insert_count = std::stoull(cb_data.get_value(0, 0));

    if (cmd_service.execute_sql(mlog_deleta_sql) || cb_data.is_error()) {
      my_error(ER_MATERIALIZED_VIEW_REFLUSH, MYF(0),
               cb_data.error_msg().c_str());
      return true;
    }

    assert(cb_data.rows() == 1);

    itr->second->delta_delete_count = std::stoull(cb_data.get_value(0, 0));
  }
  return false;
}

/**
 *  see  @UpdateMlogRef
 *  the same behavier
 *
 *  this is SQL
 *
 *
 *  UpdateMlogRef: real iterator
 *
 */
bool delta_updat_ref(MlogMap &mlog_info, QueryArray *result) {
  // update mlog$_t1 set "{ref}" = (IFNULL("{ref}", 0) | 1<< 0);
  string update_sql(
      R"(update "{mlog_db}"."{mlog}" set "{ref}" = (IFNULL("{ref}", 0) | 1<<  {ref_value}) ;)");

  for (auto itr = mlog_info.begin(); itr != mlog_info.end(); itr++) {
    auto update_ref =
        boost::replace_all_copy(update_sql, "{mlog}", itr->second->mlog_name);

    boost::replace_all(update_ref, "{mlog_db}", itr->first.first);
    boost::replace_all(update_ref, "{ref_value}",
                       std::to_string(itr->second->ref_value));
    result->push_back(update_ref);
  }

  return false;
}

/**
 * @brief check is fast and  flush
 *
 * @param db_name
 * @param imv_name
 *
 * @return true
 * @return false
 */
bool delta_imv(String *db_name, String *imv_name, bool trace, String *result) {
  auto thd = current_thd;
  String view_define;
  TableColumnNameMap imv_pk_map(thd->mem_root);
  MlogMap mlogs;

  // ref table primary keys

  {
    // Acquire exclusive lock on it before dropping.
    MDL_request mdl_request;
    MDL_REQUEST_INIT(&mdl_request, MDL_key::TABLE, db_name->c_ptr(),
                     imv_name->c_ptr(), MDL_SHARED, MDL_TRANSACTION);
    if (thd->mdl_context.acquire_lock(&mdl_request,
                                      thd->variables.lock_wait_timeout)) {
      return true;
    }

    // 1. get mv define open dd to read mv define
    dd::cache::Dictionary_client::Auto_releaser releaser(thd->dd_client());
    const dd::Table *imv = nullptr;
    if (thd->dd_client()->acquire(db_name->c_ptr(), imv_name->c_ptr(), &imv))
      return true;

    if (!imv->is_materialized_view()) {
      my_error(ER_WRONG_TABLE_NAME, MYF(0), imv_name->c_ptr_safe());
      return true;
    }
    auto mv_info = imv->get_mv_info();

    view_define.copy(mv_info->definition_view().c_str(),
                     mv_info->definition_view().length(),
                     &my_charset_utf8mb4_general_ci);

    // see @CheckVisitor::add_hidden_primary_key define
    // get
    JsonParseDefaultErrorHandler parse_handler("", 0);
    // only has simple query need pk map
    if (!mv_info->columns().empty()) {
      auto dom_ptr = Json_dom::parse(mv_info->columns().c_str(),
                                     mv_info->columns().length(), parse_handler,
                                     JsonDepthErrorHandler);
      if (dom_ptr->json_type() == enum_json_type::J_OBJECT) {
        auto table_obj = down_cast<Json_object *>(dom_ptr.get());
        for (auto &itr : *table_obj) {
          auto colNameMap = std::make_unique<ColumnNameMap>(thd->mem_root);
          auto column_obj = down_cast<Json_object *>(itr.second.get());
          for (auto &field_itr : *column_obj) {
            auto field_name = down_cast<Json_string *>(field_itr.second.get());
            colNameMap->insert(
                std::make_pair(field_itr.first, field_name->value()));
          }
          imv_pk_map.emplace(std::make_pair(itr.first, std::move(colNameMap)));
        }
      } else {
        my_error(ER_INVALID_MATERIALIZED_VIEW, MYF(0),
                 mv_info->columns().c_str(), " imv column is invalid");
        return true;
      }
    }

    if (string_to_mlog_status(mv_info->table_ref(), get_table_primary, thd,
                              &mlogs, &imv_pk_map)) {
      return true;
    }
  }
  // delta_mlog temp tables;

  // parse view

  RewriteVisitor rewriter(thd, db_name, imv_name, &mlogs);

  if (parser_mv_define(thd, view_define.c_ptr(), view_define.length(),
                       &rewriter))
    return true;

  QueryArray sql_str;

  sql_str.push_back("set sql_mode=oracle;");
  sql_str.push_back("START TRANSACTION;");

  if (delta_data_generator(mlogs, &sql_str)) {
    return true;
  }

  Gdb_cmd_service cmd_service;

  auto &cb_data = cmd_service.get_cb_data();

  if (cmd_service.execute_sqls(sql_str) || cb_data.is_error()) {
    my_error(ER_MATERIALIZED_VIEW_REFLUSH, MYF(0), cb_data.error_msg().c_str());
    return true;
  }

  if (trace)
    for (auto &sql : sql_str) result->append(sql.c_str(), sql.length());

  sql_str.clear();
  // generator

  if (rewriter.is_simple()) {
    //
    if (rewriter.simple_query(&sql_str)) {
      return true;
    }

  } else {
    /// check has delta data null value
    if (check_mlog_has_delta_data(cmd_service, mlogs)) {
      return true;
    }

    if (rewriter.has_group_by()) {
      if (rewriter.update_delta_with_group_by(&sql_str)) {
        return true;
      }
    } else {
      if (rewriter.update_delta_without_group_by(&sql_str)) {
        return true;
      }
    }
  }

  // update ref values
  if (delta_updat_ref(mlogs, &sql_str)) {
    return true;
  }

  sql_str.push_back("COMMIT;");

  if (cmd_service.execute_sqls(sql_str) || cb_data.is_error()) {
    my_error(ER_MATERIALIZED_VIEW_REFLUSH, MYF(0), cb_data.error_msg().c_str());
    return true;
  }

  if (trace)
    for (auto &sql : sql_str) result->append(sql.c_str(), sql.length());

  return false;
}

}  // namespace greatdb

bool Item_func_snowflow_id::do_itemize(Parse_context *pc, Item **res) {
  if (skip_itemize(res)) return false;
  if (super::do_itemize(pc, res)) return true;
  pc->thd->lex->set_stmt_unsafe(LEX::BINLOG_STMT_UNSAFE_SYSTEM_FUNCTION);
  pc->thd->lex->safe_to_cache_query = false;
  return false;
}

bool Item_func_snowflow_id::resolve_type(THD *thd) {
  if (!thd->is_current_stmt_binlog_disabled()) {
    if (mysql_bin_log.is_open()) {
      snapshot_position = mysql_bin_log.get_binlog_end_pos();
    }
  }

  // server_id is uint32; (server_id >> 32) is UB, drop it.
  node_mix = (server_id ^ (server_id >> 16)) & 0x3FF;  // 10-bit node

  unsigned_flag = true;
  return false;
}

std::atomic<uint64_t> Item_func_snowflow_id::last_ts_seq(0);
std::atomic<int64_t> Item_func_snowflow_id::logical_offset(0);

/**
 * @brief snowflowid
 *
 *   snapshot_position = mysql_bin_log.get_binlog_end_pos();
 *
 *   node_mix = ((server_id >> 32) ^ server_id ^ (server_id >> 16)) &   0x3FF;
 * // 10-bit node
 *
 *  // 36-bit timestamp
 *  // 15-bit seq
 *  // 10-bit node_mix
 *  // 3-bit snapshot_position
 *
 *
 * @return longlong
 */

longlong Item_func_snowflow_id::val_int() {
  constexpr uint64_t SEQ_MASK =
      0xFFFFULL;                          // 16-bit headroom in the packed word
  constexpr uint16_t SEQ_LIMIT = 0x7FFF;  // id encodes only 15 bits of seq

  uint64_t new_ts;
  uint16_t new_seq;

  while (true) {
    uint64_t now_ms = my_micro_time() / 1000;
    int64_t offset = logical_offset.load(std::memory_order_relaxed);

    uint64_t packed = last_ts_seq.load(std::memory_order_relaxed);
    uint64_t prev_ts = packed >> 16;
    uint16_t prev_seq = static_cast<uint16_t>(packed & SEQ_MASK);

    // Wall clock went backwards relative to what we already issued: bump the
    // logical offset to cover the gap and retry.
    if ((int64_t)now_ms + offset < (int64_t)prev_ts) {
      logical_offset.fetch_add(prev_ts - (now_ms + offset),
                               std::memory_order_relaxed);
      continue;
    }

    uint64_t adjusted_ts = now_ms + offset;
    if (adjusted_ts == prev_ts) {
      if (prev_seq >= SEQ_LIMIT) {
        // seq exhausted in this ms; yield and retry next ms (do not CAS)
        continue;
      }
      new_ts = prev_ts;
      new_seq = prev_seq + 1;
    } else {
      new_ts = adjusted_ts;
      new_seq = 0;
    }

    uint64_t new_packed = (new_ts << 16) | new_seq;
    if (last_ts_seq.compare_exchange_weak(packed, new_packed,
                                          std::memory_order_relaxed))
      break;  // CAS succeeded: (new_ts, new_seq) is uniquely owned by us
  }

  uint64_t id =
      ((new_ts & 0xFFFFFFFFFULL) << (15 + 10 + 3))  // 36-bit timestamp
      | ((uint64_t)(new_seq & 0x7FFF) << (10 + 3))  // 15-bit seq
      | ((uint64_t)(node_mix & 0x3FF) << 3)         // 10-bit node_mix
      | (snapshot_position & 0x7);                  // 3-bit snapshot_position
  return (longlong)id;
}

String *Item_dbms_mview_delta_imv::val_str(String *str) {
  null_value = true;
  auto db = args[0]->val_str(str);
  if (!db) {
    my_error(ER_BAD_DB_ERROR, MYF(0), "args0");
    return nullptr;
  }

  StringBuffer<MAX_ALIAS_NAME> tb_buffer;
  auto tb = args[1]->val_str(&tb_buffer);
  if (!tb) {
    //
    my_error(ER_BAD_TABLE_ERROR, MYF(0), "args1");
    return nullptr;
  }

  if (greatdb::delta_imv(db, tb, trace, &buffer)) {
    return nullptr;
  }

  null_value = false;

  if (trace) {
    return &buffer;
  }
  str->length(0);
  str->append("flush success ");

  str->append(db->c_ptr_quick(), db->length());
  str->append(".");
  str->append(tb->c_ptr_quick(), tb->length());

  return str;
}

bool release_build_clause(THD *thd, dd::String_type db, dd::String_type table) {
  MDL_request_list mdl_requests;
  if (push_mlog_mdl_request_to_list(thd, db.c_str(), table.c_str(),
                                    &mdl_requests))
    return true;

  if (!mdl_requests.is_empty() &&
      thd->mdl_context.acquire_locks(&mdl_requests,
                                     thd->variables.lock_wait_timeout))
    return true;

  const dd::cache::Dictionary_client::Auto_releaser releaser(thd->dd_client());
  dd::Table *mv_def = nullptr;
  if (thd->dd_client()->acquire_for_modification(db, table, &mv_def)) {
    return true;
  }
  if (mv_def) {
    auto mv = mv_def->get_mv_info();
    if (mv) {
      mv->set_build_clause(1);

      mv->set_last_updated(thd->query_start_timeval_trunc(2));
      if (thd->dd_client()->update(mv_def)) return true;
      mysql_ha_flush_table(thd, db.c_str(), table.c_str());
      close_all_tables_for_name(thd, db.c_str(), table.c_str(), false);
    }
  }

  return false;
}

String *Item_dbms_mview_refresh::val_str(String *str) {
  null_value = true;
  int i = 0;
  auto db = args[i++]->val_str(str);
  if (!db) {
    my_error(ER_MATERIALIZED_VIEW_REFLUSH, MYF(0), "args0");
    return nullptr;
  }

  auto table = args[i++]->val_str(str);
  if (!table) {
    my_error(ER_MATERIALIZED_VIEW_REFLUSH, MYF(0), "args1");
    return nullptr;
  }

  auto table_col = args[i++]->val_str(str);
  if (!table_col) {
    my_error(ER_MATERIALIZED_VIEW_REFLUSH, MYF(0), "args2");
    return nullptr;
  }

  auto view_def = args[i++]->val_str(str);
  if (!view_def) {
    my_error(ER_MATERIALIZED_VIEW_REFLUSH, MYF(0), "args3");
    return nullptr;
  }

  Gdb_cmd_service cmd_service;

  auto &cb_data = cmd_service.get_cb_data();

  greatdb::QueryArray sql_str;

  sql_str.push_back("set sql_mode=oracle;");
  std::string use_db("use ");
  use_db.append(db->c_ptr(), db->length());

  std::string truncate("delete from ");

  truncate.append(table->c_ptr(), table->length());

  std::string insert("insert into ");
  insert.append(table_col->c_ptr(), table_col->length());
  insert.append(" ");
  insert.append(view_def->c_ptr(), view_def->length());

  sql_str.push_back(use_db);
  sql_str.push_back("begin work;");
  sql_str.push_back(truncate);
  sql_str.push_back(insert);
  sql_str.push_back("commit;");
  if (cmd_service.execute_sqls(sql_str) || cb_data.is_error()) {
    my_error(ER_MATERIALIZED_VIEW_REFLUSH, MYF(0), cb_data.error_msg().c_str());
    return nullptr;
  }

  //
  null_value = false;
  str->length(0);
  str->append("flush success ");
  str->append(db->c_ptr_quick(), db->length());
  str->append(".");
  str->append(table->c_ptr(), table->length());

  return str;
}

longlong Item_dbms_mview_purge_mlog::val_int() {
  null_value = true;
  int i = 0;
  StringBuffer<NAME_CHAR_LEN> dbstr;
  auto db = args[i++]->val_str(&dbstr);
  if (!db) {
    my_error(ER_WRONG_ARGUMENTS, MYF(0), "database");
    return 1;
  }

  StringBuffer<NAME_CHAR_LEN> tbstr;
  auto table = args[i++]->val_str(&tbstr);
  if (!table) {
    my_error(ER_WRONG_ARGUMENTS, MYF(0), "mlog");
    return 1;
  }

  Gdb_cmd_service cmd_service;

  auto &cb_data = cmd_service.get_cb_data();
  char sql_str[2048] = {0};

  // check is mlog
  auto sql_str_len =
      snprintf(sql_str, sizeof(sql_str),
               "select count(*) from information_schema.materialized_view_logs "
               "where MLOG_DB ='%s' and MLOG_NAME = '%s' ",
               db->c_ptr_safe(), table->c_ptr_safe());
  if (sql_str_len < 0 || ((size_t)sql_str_len) >= sizeof(sql_str)) {
    my_error(ER_MATERIALIZED_VIEW_REFLUSH, MYF(0), sql_str);
    return 1;
  }

  if (cmd_service.execute_sql(sql_str, sql_str_len) || cb_data.is_error()) {
    my_error(ER_MATERIALIZED_VIEW_REFLUSH, MYF(0), cb_data.error_msg().c_str());
    return 1;
  }

  auto find = std::stoull(cb_data.get_value(0, 0));
  if (find != 1) {
    my_error(ER_WRONG_ARGUMENTS, MYF(0), "mlog");
    return true;
  }

  // delete from test.mlog$_t1 ml where ml."{ref}" = (select ref  from
  // information_schema.materialized_view_logs where MLOG_DB ='test' and
  // MLOG_NAME = 'mlog$_t1' );

  sql_str_len =
      snprintf(sql_str, sizeof(sql_str),
               "delete from  `%s`.`%s` where `%s` = "
               " (select ref  from information_schema.materialized_view_logs "
               "where MLOG_DB ='%s' and MLOG_NAME = '%s' )",
               db->c_ptr_safe(), table->c_ptr_safe(), mlog_ref_column_name,
               db->c_ptr_safe(), table->c_ptr_safe());
  if (sql_str_len < 0 || ((size_t)sql_str_len) >= sizeof(sql_str)) {
    my_error(ER_MATERIALIZED_VIEW_REFLUSH, MYF(0), sql_str);
    return 1;
  }

  if (cmd_service.execute_sql(sql_str, sql_str_len) || cb_data.is_error()) {
    my_error(ER_MATERIALIZED_VIEW_REFLUSH, MYF(0), cb_data.error_msg().c_str());
    return 1;
  }
  null_value = false;
  return 0;
}

longlong Item_dbms_mview_update_mlog::val_int() {
  null_value = true;
  int i = 0;
  StringBuffer<NAME_CHAR_LEN> dbstr;
  auto db = args[i++]->val_str(&dbstr);
  if (!db) {
    my_error(ER_WRONG_ARGUMENTS, MYF(0), "database");
    return 1;
  }

  StringBuffer<NAME_CHAR_LEN> tbstr;
  auto table = args[i++]->val_str(&tbstr);
  if (!table) {
    my_error(ER_MATERIALIZED_VIEW_REFLUSH, MYF(0), "mlog");
    return 1;
  }

  auto ref_val = args[i++]->val_int();

  Gdb_cmd_service cmd_service;

  auto &cb_data = cmd_service.get_cb_data();
  char sql_str[2048] = {0};

  // check is mlog
  auto sql_str_len =
      snprintf(sql_str, sizeof(sql_str),
               "select count(*) from information_schema.materialized_view_logs "
               "where MLOG_DB ='%s' and MLOG_NAME = '%s' ",
               db->c_ptr_safe(), table->c_ptr_safe());
  if (sql_str_len < 0 || ((size_t)sql_str_len) >= sizeof(sql_str)) {
    my_error(ER_MATERIALIZED_VIEW_REFLUSH, MYF(0), sql_str);
    return 1;
  }

  if (cmd_service.execute_sql(sql_str, sql_str_len) || cb_data.is_error()) {
    my_error(ER_MATERIALIZED_VIEW_REFLUSH, MYF(0), cb_data.error_msg().c_str());
    return 1;
  }

  auto find = std::stoull(cb_data.get_value(0, 0));
  if (find != 1) {
    my_error(ER_WRONG_ARGUMENTS, MYF(0), "mlog");
    return true;
  }

  sql_str_len =
      snprintf(sql_str, sizeof(sql_str),
               "update `%s`.`%s` set `%s` = (IFNULL(`%s`, 0) | 1<<  %lld) ",
               db->c_ptr_safe(), table->c_ptr_safe(), mlog_ref_column_name,
               mlog_ref_column_name, ref_val);
  if (sql_str_len < 0 || ((size_t)sql_str_len) >= sizeof(sql_str)) {
    my_error(ER_MATERIALIZED_VIEW_REFLUSH, MYF(0), sql_str);
    return 1;
  }
  if (cmd_service.execute_sql(sql_str, sql_str_len) || cb_data.is_error()) {
    my_error(ER_MATERIALIZED_VIEW_REFLUSH, MYF(0), cb_data.error_msg().c_str());
    return 1;
  }

  null_value = false;
  return 0;
}
static_assert(8707 == ER_MATERIALIZED_VIEW_REFLUSH,
              "should update dbms_mview RAISE_APPLICATION_ERROR error number");
