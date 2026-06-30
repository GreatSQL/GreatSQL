/* Copyright (c) 2026, GreatDB Software Co., Ltd.

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

#include "explain_query_serializer.h"
#include <string>
#include <vector>
#include "plan_baseline.h"
#include "sha2.h"
#include "sql-common/json_path.h"  // Json_path
#include "sql/join_optimizer/explain_access_path.h"
#include "sql/join_optimizer/print_utils.h"
#include "sql/opt_explain.h"
#include "sql/opt_explain_traditional.h"
#include "sql/range_optimizer/group_index_skip_scan_plan.h"
#include "sql/range_optimizer/index_skip_scan_plan.h"
#include "sql/range_optimizer/range_optimizer.h"
#include "sql/sql_lex.h"

using std::string;
using std::unique_ptr;
using std::vector;

void ExplainPrintTreeNode(const Json_dom *json, int level,
                          std::string *explain);
static std::unique_ptr<Json_object> gdb_ExplainAccessPath(
    const AccessPath *path, const AccessPath *materialized_path, JOIN *join,
    bool is_root_of_join, unique_ptr<Json_object> root_obj = nullptr);

inline static double GetJSONDouble(const Json_object *obj, const char *key) {
  return down_cast<const Json_double *>(obj->get(key))->value();
}

/// Convenience function to add a json field.
template <class T, class JsonObjectPtr, class... Args>
static bool AddMemberToObject(const JsonObjectPtr &obj, const char *alias,
                              Args &&...ctor_args) {
  return obj->add_alias(
      alias, create_dom_ptr<T, Args...>(std::forward<Args>(ctor_args)...));
}

template <class T, class... Args>
static bool AddElementToArray(const unique_ptr<Json_array> &array,
                              Args &&...ctor_args) {
  return array->append_alias(
      create_dom_ptr<T, Args...>(std::forward<Args>(ctor_args)...));
}

/// Convert Json object to string.
static std::string ExplainJsonToString(Json_object *json) {
  std::string explain;
  ExplainPrintTreeNode(json, 0, &explain);
  if (explain.empty()) return "";
  return explain;
}

std::string print_explain_query(THD *, Query_expression *unit,
                                double *last_cost, ulonglong *rows) {
  std::string result;
  JOIN *join = nullptr;
  std::unique_ptr<Json_object> json;
  AccessPath *path = (unit != nullptr ? unit->root_access_path() : nullptr);

  if (path == nullptr) goto error;

  if (unit != nullptr && !unit->is_union())
    join = unit->first_query_block()->join;

  json = gdb_ExplainAccessPath(path, nullptr, join, /*is_root_of_join=*/true);
  if (json == nullptr) goto error;

  // 1.get access_path_serialization
  // result = get_explain_query(thd, thd, unit);
  result = ExplainJsonToString(json.get());
  if (result.empty()) goto error;

  // 2. get cost
  if (json->get("estimated_total_cost") != nullptr)
    *last_cost = GetJSONDouble(json.get(), "estimated_total_cost");

  // 3. get rows
  if (json->get("estimated_rows") != nullptr)
    *rows = GetJSONDouble(json.get(), "estimated_rows");
  return result;

error:
  result = "<not executable by iterator executor>";
  *last_cost = 0;
  *rows = 0;
  return result;
}

static LEX_CSTRING QUERY_REWRITE_SCHEMA_NAME = {
    STRING_WITH_LEN("query_rewrite")};
static LEX_CSTRING SYS_SCHEMA_NAME = {STRING_WITH_LEN("sys")};
static LEX_CSTRING SYS_MAC_SCHEMA_NAME = {STRING_WITH_LEN("sys_mac")};

bool check_if_all_db_is_system_schema(THD *thd) {
  int count_db = 0;
  int count = 0;
  for (Table_ref *table = thd->lex->query_tables;
       table && table != thd->lex->first_not_own_table();
       table = table->next_global) {
    if (!table->db || strlen(table->db) == 0) continue;
    if (strcasecmp(table->db, INFORMATION_SCHEMA_NAME.str) == 0 ||
        strcasecmp(table->db, PERFORMANCE_SCHEMA_DB_NAME.str) == 0 ||
        strcasecmp(table->db, MYSQL_SCHEMA_NAME.str) == 0 ||
        strcasecmp(table->db, AUDIT_LOG_DB.str) == 0 ||
        strcasecmp(table->db, QUERY_REWRITE_SCHEMA_NAME.str) == 0 ||
        strcasecmp(table->db, SYS_SCHEMA_NAME.str) == 0 ||
        strcasecmp(table->db, SYS_MAC_SCHEMA_NAME.str) == 0 ||
        strcasecmp(table->db, plan_baseline_database) == 0)
      count_db++;
    count++;
  }
  if (count_db == count) return true;
  return false;
}

bool parse(THD *thd, String *statement_string) {
  // The lexer can't handle non-zero-length strings starting with NUL and we
  // can't return NULL for them because this function is declared
  // nonnullable.
  if (statement_string->length() > 0 && (*statement_string)[0] == '\0')
    statement_string->length(0);

  const CHARSET_INFO *cs = statement_string->charset();
  thd->variables.character_set_client = cs;
  thd->update_charset();

  Parser_state ps;

  // The lexer needs null-terminated strings, despite boasting the below
  // interface. Hence the use of c_ptr_safe().
  if (ps.init(thd, statement_string->c_ptr_safe(), statement_string->length()))
    return true;

  ps.m_lip.multi_statements = false;
  ps.m_lip.m_digest = thd->m_digest;
  ps.m_lip.m_digest->m_digest_storage.m_charset_number = cs->number;

  thd->m_parser_state = &ps;

  Diagnostics_area tmp_da(false);
  thd->push_diagnostics_area(&tmp_da);
  {
    if (thd->sql_parser()) {
      thd->pop_diagnostics_area();
      return true;
    }
  }
  thd->pop_diagnostics_area();
  return false;
}

class Thd_parse_modifier {
 public:
  Thd_parse_modifier(THD *thd, uchar *token_buffer)
      : m_thd(thd),
        m_arena(&m_mem_root, Query_arena::STMT_REGULAR_EXECUTION),
        m_backed_up_lex(thd->lex),
        m_saved_parser_state(thd->m_parser_state),
        m_saved_digest(thd->m_digest),
        m_cs(thd->variables.character_set_client) {
    thd->m_digest = &m_digest_state;
    m_digest_state.reset(token_buffer, get_max_digest_length());
    m_arena.set_query_arena(*thd);
    thd->lex = &m_lex;
    lex_start(thd);
  }

  ~Thd_parse_modifier() {
    lex_end(&m_lex);
    m_thd->lex = m_backed_up_lex;
    m_thd->set_query_arena(m_arena);
    m_thd->m_parser_state = m_saved_parser_state;
    m_thd->m_digest = m_saved_digest;
    m_thd->variables.character_set_client = m_cs;
    m_thd->update_charset();
  }

 private:
  THD *m_thd;
  MEM_ROOT m_mem_root;
  Query_arena m_arena;
  LEX *m_backed_up_lex;
  LEX m_lex;
  sql_digest_state m_digest_state;
  Parser_state *m_saved_parser_state;
  sql_digest_state *m_saved_digest;
  const CHARSET_INFO *m_cs;
};

// get digest from select in sp
static bool get_digest_text(THD *thd, String *buf) {
  String statement_string(thd->query().str, thd->query().length,
                          system_charset_info);
  uchar m_token_buffer[1024];
  Thd_parse_modifier thd_mod(thd, m_token_buffer);
  if (parse(thd, &statement_string)) return true;
  compute_digest_text(&thd->m_digest->m_digest_storage, buf);
  return false;
}

// get digest hash from select in sp
static bool get_digest_hash(THD *thd, String *buf) {
  String statement_string(thd->query().str, thd->query().length,
                          system_charset_info);
  uchar digest[DIGEST_HASH_SIZE];
  {
    uchar m_token_buffer[1024];
    Thd_parse_modifier thd_mod(thd, m_token_buffer);
    if (parse(thd, &statement_string)) return true;
    compute_digest_hash(&thd->m_digest->m_digest_storage, digest);
  }
  if (buf->reserve(DIGEST_HASH_TO_STRING_LENGTH)) return true;
  buf->length(DIGEST_HASH_TO_STRING_LENGTH);
  DIGEST_HASH_TO_STRING(digest, buf->c_ptr_quick());
  return false;
}

bool delete_old_data_from_map() {
  if (plan_hash_map.empty() || map_id_list.empty()) return false;
  auto it = map_id_list.front();
  for (auto tt = plan_hash_map.begin(); tt != plan_hash_map.end(); tt++) {
    if (tt->second->id == it) plan_hash_map.erase(tt);
  }
  for (auto tt = plan_sql_map.begin(); tt != plan_sql_map.end(); tt++) {
    if (tt->second->id == it) plan_sql_map.erase(tt);
  }
  map_id_list.pop_front();
  return false;
}

// serialize explian access path
bool gdb_plan_baseline_collect_explain_impl(THD *thd, Query_expression *unit) {
  Mutex_guard_plan guard(lock_plan_hash_map, lock_plan_sql_map,
                         lock_map_id_list);
  // delete old data from map
  while (map_id_list.size() >= global_plan_baseline_var.max_rows_count) {
    if (delete_old_data_from_map()) return true;
  }

  double last_cost = 0.0;
  ulonglong rows = 0;

  // 0. get db_name
  auto db_name = std::make_shared<std::string>();
  /*It should save all db name in select sql
  e.g:
  use db;
  select * from t1
  and
  use db1;
  select * from t1;
  they have defferent db name but same digest*/
  std::vector<const char *> db_name_list;
  std::unordered_set<const char *> seen;
  for (Table_ref *table = thd->lex->query_tables;
       table && table != thd->lex->first_not_own_table();
       table = table->next_global) {
    if (table->db && strlen(table->db)) {
      if (seen.find(table->db) == seen.end()) {
        seen.insert(table->db);
        db_name_list.push_back(table->db);
      }
    }
  }
  uint count_table = 0;
  for (int i = 0; i < (int)db_name_list.size(); i++) {
    if (count_table) db_name->append(",");
    db_name->append(db_name_list[i]);
    count_table++;
  }

  // 1. get query_sql
  auto query_sql = std::make_shared<std::string>();
  query_sql->assign(db_name->c_str(), db_name->length());
  query_sql->append("#");
  query_sql->append(thd->query().str, thd->query().length);

  // 2. get digest_hash
  auto sql_digest_hash = std::make_shared<std::string>();
  // for select in sp, it doesn't have digest, so get digest first
  char m_digest[DIGEST_HASH_TO_STRING_LENGTH + 1];
  if (!thd->m_digest->m_digest_storage.m_token_array) {
    String digest_hash;
    if (get_digest_hash(thd, &digest_hash)) return true;
    sql_digest_hash->assign(digest_hash.c_ptr_quick(),
                            DIGEST_HASH_TO_STRING_LENGTH);
  } else {
    DIGEST_HASH_TO_STRING(thd->m_digest->m_digest_storage.m_hash, m_digest);
    sql_digest_hash->assign(m_digest, DIGEST_HASH_TO_STRING_LENGTH);
  }

  // 3. get digest_text
  String digest_text_tmp;
  auto digest_text = std::make_shared<std::string>();
  // for sselect in sp, it doesn't have digest, so get digest first
  if (!thd->m_digest->m_digest_storage.m_token_array) {
    if (get_digest_text(thd, &digest_text_tmp)) return true;
  } else {
    compute_digest_text(&thd->m_digest->m_digest_storage, &digest_text_tmp);
  }
  digest_text->assign(digest_text_tmp.c_ptr_safe(), digest_text_tmp.length());

  // 4.get access_path_serialization,cost,rows
  std::string result = print_explain_query(thd, unit, &last_cost, &rows);
  auto result_explain = std::make_shared<std::string>();
  result_explain->assign(result);

  // 5. get plan_name
  unsigned char plan_name_tmp[SHA256_DIGEST_LENGTH];
  auto plan_name = std::make_shared<std::string>();
  (void)SHA_EVP256(pointer_cast<const unsigned char *>(result.data()),
                   result.size(), plan_name_tmp);
  char m_plan_digest[DIGEST_HASH_TO_STRING_LENGTH + 1];
  DIGEST_HASH_TO_STRING(plan_name_tmp, m_plan_digest);
  plan_name->assign(m_plan_digest, DIGEST_HASH_TO_STRING_LENGTH);

  // 6. insert into map
  auto key_name = std::make_shared<std::string>();
  key_name->assign(db_name->c_str(), db_name->length());
  key_name->append("#");
  key_name->append(sql_digest_hash->c_str(), sql_digest_hash->length());
  key_name->append("#");
  key_name->append(plan_name->c_str(), plan_name->length());

  auto it = plan_hash_map.find(key_name);
  ulonglong exist_id = explain_count;
  bool found = false;
  if (it == plan_hash_map.end()) {
    auto explain_result = new explain_query_result(digest_text, result_explain,
                                                   last_cost, explain_count);
    if (!explain_result) return true;
    plan_hash_map.emplace(key_name,
                          unique_ptr<explain_query_result>(explain_result));
    map_id_list.push_back(explain_count);
    explain_count++;
  } else {
    // choose min cost as result
    if (last_cost < it->second->cost) {
      it->second->cost = last_cost;
      found = true;
    }
    exist_id = it->second->id;
  }

  if (plan_sql_map.find(query_sql) == plan_sql_map.end()) {
    auto sql_result =
        new explain_query_sql(sql_digest_hash, exist_id, last_cost, rows);
    if (!sql_result) return true;
    plan_sql_map.emplace(query_sql, unique_ptr<explain_query_sql>(sql_result));
  } else if (found) {
    auto it_sql = plan_sql_map.find(query_sql);
    it_sql->second->cost = last_cost;
    it_sql->second->id = exist_id;
    it_sql->second->rows = rows;
  }
  return false;
}

///////////////////////////////////////////////////////////////////////////
// serialize explain, delete condition and rows
///////////////////////////////////////////////////////////////////////////
static bool SetIndexInfoInObject(string *str,
                                 const char *json_index_access_type,
                                 const char *prefix, const TABLE &table,
                                 const KEY &key, const char *index_access_type,
                                 const string lookup_condition,
                                 const string *ranges_text,
                                 unique_ptr<Json_array> range_arr, bool reverse,
                                 Item *pushed_idx_cond, Json_object *obj) {
  string idx_cond_str = "";
  string covering_index =
      string(IsCoveringIndexScan(key, table) ? "Covering index " : "Index ");
  bool error = false;

  if (prefix) covering_index[0] = tolower(covering_index[0]);

  *str += (prefix ? string(prefix) + " " : "") + covering_index +
          index_access_type +  // lookup/scan/search
          " on " + table.alias + " using " + key.name +
          (!lookup_condition.empty() ? " ( lookup_condition )" : "") +
          (ranges_text != nullptr ? " over ranges_text : ranges" : "") +
          (reverse ? " (reverse)" : "") +
          (pushed_idx_cond ? ", with index condition: idx_cond_str" : "");
  *str += table.file->explain_extra();

  error |= AddMemberToObject<Json_string>(obj, "access_type", "index");
  error |= AddMemberToObject<Json_string>(obj, "index_access_type",
                                          json_index_access_type);
  error |= AddMemberToObject<Json_boolean>(obj, "covering",
                                           IsCoveringIndexScan(key, table));
  error |= AddTableInfoToObject(obj, &table);
  error |= AddMemberToObject<Json_string>(obj, "index_name", key.name);
  if (!lookup_condition.empty())
    error |= AddMemberToObject<Json_string>(obj, "lookup_condition",
                                            lookup_condition);
  if (range_arr) error |= obj->add_alias("ranges", std::move(range_arr));
  if (reverse) error |= AddMemberToObject<Json_boolean>(obj, "reverse", true);
  if (pushed_idx_cond)
    error |= AddMemberToObject<Json_string>(obj, "pushed_index_condition",
                                            idx_cond_str);
  if (!table.file->explain_extra().empty())
    error |= AddMemberToObject<Json_string>(obj, "message",
                                            table.file->explain_extra());

  return error;
}

static bool PrintRanges(const QUICK_RANGE *const *, unsigned num_ranges,
                        const KEY_PART_INFO *, bool,
                        const std::unique_ptr<Json_array> &range_array,
                        string *ranges_out) {
  string range, shortened_range;
  for (unsigned range_idx = 0; range_idx < num_ranges; ++range_idx) {
    if (range_idx == 2 && num_ranges > 3) {
      char str[256];
      snprintf(str, sizeof(str), " OR (%u more)", num_ranges - 2);
      // Save the shortened version for TREE format.
      shortened_range = range + str;
    }
    if (range_idx > 0) range += " OR ";

    range += "( condition )";
  }
  if (AddElementToArray<Json_string>(range_array, range)) return true;
  *ranges_out = (shortened_range.empty() ? range : shortened_range);
  return false;
}

static bool AddChildrenToObject(Json_object *obj, vector<ExplainChild> children,
                                JOIN *parent_join, bool parent_is_root_of_join,
                                string alias) {
  if (children.empty()) return false;

  unique_ptr<Json_array> children_json(new (std::nothrow) Json_array());
  if (children_json == nullptr) return true;

  for (ExplainChild &child : children) {
    JOIN *subjoin = child.join != nullptr ? child.join : parent_join;
    bool child_is_root_of_join =
        subjoin != parent_join || parent_is_root_of_join;

    unique_ptr<Json_object> child_obj =
        gdb_ExplainAccessPath(child.path, nullptr, subjoin,
                              child_is_root_of_join, std::move(child.obj));
    if (child_obj == nullptr) return true;
    if (!child.description.empty()) {
      if (AddMemberToObject<Json_string>(child_obj, "heading",
                                         child.description))
        return true;
    }
    if (children_json->append_alias(std::move(child_obj))) return true;
  }

  return obj->add_alias(alias, std::move(children_json));
}

static bool ExplainIndexSkipScanAccessPath(Json_object *obj,
                                           const AccessPath *path,
                                           JOIN *join [[maybe_unused]],
                                           string *description) {
  const TABLE &table = *path->index_skip_scan().table;
  const KEY &key_info = table.key_info[path->index_skip_scan().index];
  string ranges;
  IndexSkipScanParameters *param = path->index_skip_scan().param;

  // Print out any equality ranges.
  bool first = true;
  std::unique_ptr<Json_array> range_arr(new (std::nothrow) Json_array());
  if (range_arr == nullptr) return true;
  for (unsigned key_part_idx = 0; key_part_idx < param->eq_prefix_key_parts;
       ++key_part_idx) {
    if (!first) {
      ranges += ", ";
    }
    first = false;

    string range = param->index_info->key_part[key_part_idx].field->field_name;
    string range_short_text;
    Bounds_checked_array<unsigned char *> prefixes =
        param->eq_prefixes[key_part_idx].eq_key_prefixes;
    if (prefixes.size() == 1) {
      range += " = ";
      String out;
      out.append(" key ");
      range += to_string(out);
    } else {
      range += " IN (";
      for (unsigned i = 0; i < prefixes.size(); ++i) {
        if (i == 2 && prefixes.size() > 3) {
          range_short_text =
              range + StringPrintf(", (%zu more))", prefixes.size() - 2);
        }
        if (i != 0) {
          range += ", ";
        }
        String out;
        out.append(" key ");
        range += to_string(out);
      }
      range += ")";
    }
    if (AddElementToArray<Json_string>(range_arr, range)) return true;
    // For IN clause above, we have made range_short_text; so use that if it's
    // available, rather than the full string stored in 'range'.
    ranges += (range_short_text.empty() ? range : range_short_text);
  }

  // Then the ranges.
  if (!first) {
    ranges += ", ";
  }
  String out;
  out.append("");
  ranges += to_string(out);
  if (AddElementToArray<Json_string>(range_arr, to_string(out))) return true;

  // NOTE: Currently, index skip scan is always covering, but there's no
  // good reason why we cannot fix this limitation in the future.
  return SetIndexInfoInObject(
      description, "index_skip_scan", nullptr, table, key_info, "skip scan",
      /*lookup condition*/ "", &ranges, std::move(range_arr), /*reverse*/ false,
      /*push_condition*/ nullptr, obj);
}

static bool ExplainGroupIndexSkipScanAccessPath(Json_object *obj,
                                                const AccessPath *path,
                                                JOIN *join [[maybe_unused]],
                                                string *description) {
  const TABLE &table = *path->group_index_skip_scan().table;
  const KEY &key_info = table.key_info[path->group_index_skip_scan().index];
  GroupIndexSkipScanParameters *param = path->group_index_skip_scan().param;
  string ranges;
  bool error = false;
  unique_ptr<Json_array> range_arr(new (std::nothrow) Json_array());
  if (range_arr == nullptr) return true;

  // Print out prefix ranges, if any.
  if (!param->prefix_ranges.empty()) {
    error |= PrintRanges(param->prefix_ranges.data(),
                         param->prefix_ranges.size(), key_info.key_part,
                         /*single_part_only=*/false, range_arr, &ranges);
  }

  // Print out the ranges on the MIN/MAX keypart, if we have them.
  // (We don't print infix ranges, because they seem to be in an unusual
  // format.)
  if (!param->min_max_ranges.empty()) {
    if (!param->prefix_ranges.empty()) {
      ranges += ", ";
    }
    error |= PrintRanges(param->min_max_ranges.data(),
                         param->min_max_ranges.size(), param->min_max_arg_part,
                         /*single_part_only=*/true, range_arr, &ranges);
  }

  // NOTE: Currently, group index skip scan is always covering, but there's no
  // good reason why we cannot fix this limitation in the future.
  error |= SetIndexInfoInObject(
      description, "group_index_skip_scan", nullptr, table, key_info,
      (param->min_max_arg_part ? "skip scan for grouping"
                               : "skip scan for deduplication"),
      /*lookup condition*/ "", (!ranges.empty() ? &ranges : nullptr),
      std::move(range_arr),
      /*reverse*/ false, /*push_condition*/ nullptr, obj);

  return error;
}

static std::unique_ptr<Json_object> AssignParentPath(
    AccessPath *table_path, const AccessPath *materialized_path,
    std::unique_ptr<Json_object> materialized_obj, JOIN *join) {
  // We don't want to include the SELECT subquery list in the parent path;
  // Let them get printed in the actual root node. So is_root_of_join=false.
  std::unique_ptr<Json_object> table_obj = gdb_ExplainAccessPath(
      table_path, materialized_path, join, /*is_root_of_join=*/false);
  if (table_obj == nullptr) return nullptr;

  /* Get the bottommost object from the new object tree. */
  Json_object *bottom_obj = table_obj.get();
  while (bottom_obj->get("inputs") != nullptr) {
    Json_dom *children = bottom_obj->get("inputs");
    assert(children->json_type() == enum_json_type::J_ARRAY);
    Json_array *children_array = down_cast<Json_array *>(children);
    bottom_obj = down_cast<Json_object *>((*children_array)[0]);
  }

  /* Place the input object as a child of the bottom-most object */
  std::unique_ptr<Json_array> children(new (std::nothrow) Json_array());
  if (children == nullptr ||
      children->append_alias(std::move(materialized_obj)))
    return nullptr;
  if (bottom_obj->add_alias("inputs", std::move(children))) return nullptr;

  return table_obj;
}

static std::unique_ptr<Json_object> ExplainMaterializeAccessPath(
    const AccessPath *path, JOIN *join, std::unique_ptr<Json_object> ret_obj,
    vector<ExplainChild> *children, bool explain_analyze) {
  Json_object *obj = ret_obj.get();
  bool error = false;
  MaterializePathParameters *param = path->materialize().param;

  /*
    There may be multiple references to a CTE, but we should only print the
    plan once.
  */
  const bool explain_cte_now = param->cte != nullptr && [&]() {
    if (explain_analyze) {
      /*
        Find the temporary table for which the CTE was materialized, if there
        is one.
      */
      if (path->iterator == nullptr ||
          path->iterator->GetProfiler()->GetNumInitCalls() == 0) {
        // If the CTE was never materialized, print it at the first reference.
        return param->table == param->cte->tmp_tables[0]->table &&
               std::none_of(param->cte->tmp_tables.cbegin(),
                            param->cte->tmp_tables.cend(),
                            [](const Table_ref *tab) {
                              return tab->table->materialized;
                            });
      } else {
        // The CTE was materialized here, print it now with cost data.
        return true;
      }
    } else {
      // If we do not want cost data, print the plan at the first reference.
      return param->table == param->cte->tmp_tables[0]->table;
    }
  }();

  const bool is_set_operation = param->m_operands.size() > 1;
  string str;
  const bool doing_dedup = MaterializeIsDoingDeduplication(param->table);
  if (param->cte != nullptr) {
    error |= AddMemberToObject<Json_boolean>(obj, "cte", true);
    if (param->cte->recursive) {
      error |= AddMemberToObject<Json_boolean>(obj, "recursive", true);
      str = "Materialize recursive CTE " + to_string(param->cte->name);
    } else {
      if (is_set_operation) {
        str = "Materialize union CTE " + to_string(param->cte->name);
        error |= AddMemberToObject<Json_boolean>(obj, "union", true);
      } else {
        str = "Materialize CTE " + to_string(param->cte->name);
      }
      if (param->cte->tmp_tables.size() > 1) {
        str += " if needed";
        if (!explain_cte_now) {
          // See children().
          str += " (query plan printed elsewhere)";
        }
      }
    }
  } else if (is_set_operation) {
    if (param->table->is_union_or_table()) {
      if (doing_dedup) {
        str = "Union materialize";
      } else {
        str = "Union all materialize";
      }
      error |= AddMemberToObject<Json_boolean>(obj, "union", true);
    } else {
      if (param->table->is_except()) {
        if (param->table->is_distinct()) {
          str = "Except materialize";
        } else {
          str = "Except all materialize";
        }
        error |= AddMemberToObject<Json_boolean>(obj, "except", true);
      } else {
        if (param->table->is_distinct()) {
          str = "Intersect materialize";
        } else {
          str = "Intersect all materialize";
        }
        error |= AddMemberToObject<Json_boolean>(obj, "intersect", true);
      }
    }
  } else if (param->rematerialize) {
    error |= AddMemberToObject<Json_boolean>(obj, "temp_table", true);
    str = "Temporary table";
  } else {
    str = "Materialize";
  }
  const bool union_dedup = param->table->is_union_or_table() && doing_dedup;
  if (union_dedup ||
      (!param->table->is_union_or_table() && param->table->is_distinct())) {
    error |= AddMemberToObject<Json_boolean>(obj, "deduplication", true);
    str += " with deduplication";
  }  // else: do not print deduplication for intersect, except

  if (param->invalidators != nullptr) {
    std::unique_ptr<Json_array> cache_invalidators(new (std::nothrow)
                                                       Json_array());
    if (cache_invalidators == nullptr) return nullptr;
    bool first = true;
    str += " (invalidate on row from ";
    for (const AccessPath *invalidator : *param->invalidators) {
      if (!first) {
        str += "; ";
      }

      first = false;
      str += invalidator->cache_invalidator().name;
      error |= AddElementToArray<Json_string>(
          cache_invalidators, invalidator->cache_invalidator().name);
    }
    str += ")";
    error |=
        obj->add_alias("cache_invalidators", std::move(cache_invalidators));
  }

  error |= AddMemberToObject<Json_string>(obj, "operation", str);

  /* Move the Materialize to the bottom of its table path, and return a new
   * object for this table path.
   */
  ret_obj = AssignParentPath(path->materialize().table_path, path,
                             std::move(ret_obj), join);

  // Children.

  // If a CTE is referenced multiple times, only bother printing its query plan
  // once, instead of repeating it over and over again.
  //
  // TODO(sgunders): Consider printing CTE query plans on the top level of the
  // query block instead?
  if (param->cte != nullptr && !explain_cte_now) {
    return (error ? nullptr : std::move(ret_obj));
  }

  char heading[256] = "";

  if (param->limit_rows != HA_POS_ERROR) {
    // We call this “Limit table size” as opposed to “Limit”, to be able
    // to distinguish between the two in EXPLAIN when debugging.
    if (MaterializeIsDoingDeduplication(param->table)) {
      snprintf(heading, sizeof(heading),
               "Limit table size: limit_rows unique row(s)");
    } else {
      snprintf(heading, sizeof(heading), "Limit table size: limit_rows row(s)");
    }
  }

  // We don't list the table iterator as an explicit child; we mark it in
  // our description instead. (Anything else would look confusingly much
  // like a join.)
  for (const MaterializePathParameters::Operand &operand : param->m_operands) {
    string this_heading = heading;

    if (operand.disable_deduplication_by_hash_field) {
      if (this_heading.empty()) {
        this_heading = "Disable deduplication";
      } else {
        this_heading += ", disable deduplication";
      }
    }
    if (!param->table->is_union_or_table() &&
        (param->table->is_except() && param->table->is_distinct()) &&
        operand.m_operand_idx > 0 &&
        (operand.m_operand_idx < operand.m_first_distinct)) {
      if (this_heading.empty()) {
        this_heading = "Disable deduplication";
      } else {
        this_heading += ", disable deduplication";
      }
    }

    if (operand.is_recursive_reference) {
      if (this_heading.empty()) {
        this_heading = "Repeat until convergence";
      } else {
        this_heading += ", repeat until convergence";
      }
    }

    children->push_back({operand.subquery_path, this_heading, operand.join});
  }

  return (error ? nullptr : std::move(ret_obj));
}

static std::unique_ptr<Json_object> SetObjectMembers(
    std::unique_ptr<Json_object> ret_obj, const AccessPath *path,
    const AccessPath *materialized_path, JOIN *join,
    vector<ExplainChild> *children) {
  bool error = false;
  string description;

  // The obj to be returned might get changed when processing some of the
  // paths. So keep a handle to the original object, in case we later add any
  // more fields.
  Json_object *obj = ret_obj.get();

  /* Get path-specific info, including the description string */
  switch (path->type) {
    case AccessPath::TABLE_SCAN: {
      const TABLE &table = *path->table_scan().table;
      description += string("Table scan on ") + table.alias;
      description += table.file->explain_extra();

      error |= AddTableInfoToObject(obj, &table);
      error |= AddMemberToObject<Json_string>(obj, "access_type", "table");
      if (!table.file->explain_extra().empty())
        error |= AddMemberToObject<Json_string>(obj, "message",
                                                table.file->explain_extra());
      error |= AddChildrenFromPushedCondition(table, children);
      break;
    }
    case AccessPath::SAMPLE_SCAN: {
      const TABLE &table = *path->sample_scan().table;
      description += string("Sample scan on ") + table.alias;
      description += table.file->explain_extra();

      error |= AddMemberToObject<Json_string>(obj, "table_name", table.alias);
      error |= AddMemberToObject<Json_string>(obj, "access_type", "table");
      error |= AddChildrenFromPushedCondition(table, children);

      error |= AddMemberToObject<Json_string>(
          obj, "sampling_type",
          SamplingTypeToString(table.pos_in_table_list->get_sampling_type()));
      error |= AddMemberToObject<Json_double>(
          obj, "percentage",
          table.pos_in_table_list->get_sampling_percentage());

      break;
    }
    case AccessPath::INDEX_SCAN: {
      const TABLE &table = *path->index_scan().table;
      assert(table.file->pushed_idx_cond == nullptr);

      const KEY &key = table.key_info[path->index_scan().idx];
      error |= SetIndexInfoInObject(&description, "index_scan", nullptr, table,
                                    key, "scan",
                                    /*lookup condition*/ "", /*range*/ nullptr,
                                    nullptr, path->index_scan().reverse,
                                    /*push_condition*/ nullptr, obj);
      error |= AddChildrenFromPushedCondition(table, children);
      break;
    }
    case AccessPath::INDEX_DISTANCE_SCAN: {
      const TABLE &table = *path->index_distance_scan().table;
      assert(table.file->pushed_idx_cond == nullptr);

      const KEY &key = table.key_info[path->index_distance_scan().idx];
      error |= SetIndexInfoInObject(&description, "index_distance_scan",
                                    nullptr, table, key, "distance scan",
                                    /*lookup condition*/ "", /*range*/ nullptr,
                                    nullptr, false,
                                    /*push_condition*/ nullptr, obj);
      error |= AddChildrenFromPushedCondition(table, children);
      break;
    }
    case AccessPath::REF: {
      const TABLE &table = *path->ref().table;
      const KEY &key = table.key_info[path->ref().ref->key];
      error |= SetIndexInfoInObject(
          &description, "index_lookup", nullptr, table, key, "lookup",
          RefToString(*path->ref().ref, key, /*include_nulls=*/false),
          /*ranges=*/nullptr, nullptr, path->ref().reverse,
          table.file->pushed_idx_cond, obj);
      error |= AddChildrenFromPushedCondition(table, children);
      break;
    }
    case AccessPath::REF_OR_NULL: {
      const TABLE &table = *path->ref_or_null().table;
      const KEY &key = table.key_info[path->ref_or_null().ref->key];
      error |= SetIndexInfoInObject(
          &description, "index_lookup", nullptr, table, key, "lookup",
          RefToString(*path->ref_or_null().ref, key, /*include_nulls=*/true),
          /*ranges=*/nullptr, nullptr, false, table.file->pushed_idx_cond, obj);
      error |= AddChildrenFromPushedCondition(table, children);
      break;
    }
    case AccessPath::EQ_REF: {
      const TABLE &table = *path->eq_ref().table;
      const KEY &key = table.key_info[path->eq_ref().ref->key];
      error |= SetIndexInfoInObject(
          &description, "index_lookup", "Single-row", table, key, "lookup",
          RefToString(*path->eq_ref().ref, key, /*include_nulls=*/false),
          /*ranges=*/nullptr, nullptr, false, table.file->pushed_idx_cond, obj);
      error |= AddChildrenFromPushedCondition(table, children);
      break;
    }
    case AccessPath::PUSHED_JOIN_REF: {
      const TABLE &table = *path->pushed_join_ref().table;
      assert(table.file->pushed_idx_cond == nullptr);
      const KEY &key = table.key_info[path->pushed_join_ref().ref->key];
      error |= SetIndexInfoInObject(
          &description, "pushed_join_ref",
          path->pushed_join_ref().is_unique ? "Single-row" : nullptr, table,
          key, "lookup",
          RefToString(*path->pushed_join_ref().ref, key,
                      /*include_nulls=*/false),
          /*ranges=*/nullptr, nullptr,
          /*reverse=*/false, nullptr, obj);
      break;
    }
    case AccessPath::FULL_TEXT_SEARCH: {
      const TABLE &table = *path->full_text_search().table;
      assert(table.file->pushed_idx_cond == nullptr);
      const KEY &key = table.key_info[path->full_text_search().ref->key];
      error |= SetIndexInfoInObject(
          &description, "full_text_search", "Full-text", table, key, "search",
          RefToString(*path->full_text_search().ref, key,
                      /*include_nulls=*/false),
          /*ranges=*/nullptr, nullptr,
          /*reverse=*/false, nullptr, obj);
      break;
    }
    case AccessPath::CONST_TABLE: {
      const TABLE &table = *path->const_table().table;
      assert(table.file->pushed_idx_cond == nullptr);
      assert(table.file->pushed_cond == nullptr);
      description = string("Constant row from ") + table.alias;
      error |=
          AddMemberToObject<Json_string>(obj, "access_type", "constant_row");
      error |= AddTableInfoToObject(obj, &table);
      break;
    }
    case AccessPath::MRR: {
      const TABLE &table = *path->mrr().table;
      const KEY &key = table.key_info[path->mrr().ref->key];
      error |= SetIndexInfoInObject(
          &description, "multi_range_read", "Multi-range", table, key, "lookup",
          RefToString(*path->mrr().ref, key, /*include_nulls=*/false),
          /*ranges=*/nullptr, nullptr, false, table.file->pushed_idx_cond, obj);
      error |= AddChildrenFromPushedCondition(table, children);
      break;
    }
    case AccessPath::FOLLOW_TAIL:
      description =
          string("Scan new records on ") + path->follow_tail().table->alias;
      error |= AddMemberToObject<Json_string>(obj, "access_type",
                                              "scan_new_records");
      error |= AddTableInfoToObject(obj, path->follow_tail().table);
      error |=
          AddChildrenFromPushedCondition(*path->follow_tail().table, children);
      break;
    case AccessPath::INDEX_RANGE_SCAN: {
      const auto &param = path->index_range_scan();
      const TABLE &table = *param.used_key_part[0].field->table;
      const KEY &key_info = table.key_info[param.index];

      unique_ptr<Json_array> range_arr(new (std::nothrow) Json_array());
      if (range_arr == nullptr) return nullptr;
      string ranges;
      error |= PrintRanges(param.ranges, param.num_ranges, key_info.key_part,
                           /*single_part_only=*/false, range_arr, &ranges);
      error |= SetIndexInfoInObject(
          &description, "index_range_scan", nullptr, table, key_info,
          "range scan", /*lookup condition*/ "", &ranges, std::move(range_arr),
          path->index_range_scan().reverse, table.file->pushed_idx_cond, obj);

      error |= AddChildrenFromPushedCondition(table, children);
      break;
    }
    case AccessPath::INDEX_MERGE: {
      const auto &param = path->index_merge();
      error |=
          AddMemberToObject<Json_string>(obj, "access_type", "index_merge");
      description = "Sort-deduplicate by row ID";
      for (AccessPath *child : *path->index_merge().children) {
        if (param.allow_clustered_primary_key_scan &&
            param.table->file->primary_key_is_clustered() &&
            child->index_range_scan().index == param.table->s->primary_key) {
          children->push_back(
              {child, "Clustered primary key (scanned separately)"});
        } else {
          children->push_back({child});
        }
      }
      break;
    }
    case AccessPath::ROWID_INTERSECTION: {
      error |= AddMemberToObject<Json_string>(obj, "access_type",
                                              "rowid_intersection");
      description = "Intersect rows sorted by row ID";
      for (AccessPath *child : *path->rowid_intersection().children) {
        children->push_back({child});
      }
      break;
    }
    case AccessPath::ROWID_UNION: {
      error |=
          AddMemberToObject<Json_string>(obj, "access_type", "rowid_union");
      description = "Deduplicate rows sorted by row ID";
      for (AccessPath *child : *path->rowid_union().children) {
        children->push_back({child});
      }
      break;
    }
    case AccessPath::INDEX_SKIP_SCAN: {
      error |= ExplainIndexSkipScanAccessPath(obj, path, join, &description);
      break;
    }
    case AccessPath::GROUP_INDEX_SKIP_SCAN: {
      error |=
          ExplainGroupIndexSkipScanAccessPath(obj, path, join, &description);
      break;
    }
    case AccessPath::DYNAMIC_INDEX_RANGE_SCAN: {
      const TABLE &table = *path->dynamic_index_range_scan().table;
      description += string("Index range scan on ") + table.alias +
                     " (re-planned for each iteration)";
      if (table.file->pushed_idx_cond != nullptr) {
        description += ", with index condition";
      }
      description += table.file->explain_extra();
      error |= AddMemberToObject<Json_string>(obj, "access_type", "index");
      error |= AddMemberToObject<Json_string>(obj, "index_access_type",
                                              "dynamic_index_range_scan");
      error |= AddTableInfoToObject(obj, &table);
      if (table.file->pushed_idx_cond != nullptr) {
        error |= AddMemberToObject<Json_string>(obj, "pushed_index_condition",
                                                "index_condition");
      }
      if (!table.file->explain_extra().empty()) {
        error |= AddMemberToObject<Json_string>(obj, "message",
                                                table.file->explain_extra());
      }
      error |= AddChildrenFromPushedCondition(table, children);
      break;
    }
    case AccessPath::TABLE_VALUE_CONSTRUCTOR:
    case AccessPath::FAKE_SINGLE_ROW:
      error |= AddMemberToObject<Json_string>(obj, "access_type",
                                              "rows_fetched_before_execution");
      description = "Rows fetched before execution";
      break;
    case AccessPath::ZERO_ROWS:
      error |= AddMemberToObject<Json_string>(obj, "access_type", "zero_rows");
      error |= AddMemberToObject<Json_string>(obj, "zero_rows_cause",
                                              path->zero_rows().cause);
      description = string("Zero rows (") + path->zero_rows().cause + ")";
      // The child is not printed as part of the iterator tree.
      break;
    case AccessPath::ZERO_ROWS_AGGREGATED:
      error |= AddMemberToObject<Json_string>(obj, "access_type",
                                              "zero_rows_aggregated");
      error |= AddMemberToObject<Json_string>(
          obj, "zero_rows_cause", path->zero_rows_aggregated().cause);
      description = string("Zero input rows (") +
                    path->zero_rows_aggregated().cause +
                    "), aggregated into one output row";
      break;
    case AccessPath::MATERIALIZED_TABLE_FUNCTION:
      error |= AddMemberToObject<Json_string>(obj, "access_type",
                                              "materialized_table_function");
      description = "Materialize table function";
      break;
    case AccessPath::UNQUALIFIED_COUNT:
      error |= AddMemberToObject<Json_string>(obj, "access_type", "count_rows");
      error |= AddTableInfoToObject(obj, join->qep_tab->table());
      description = "Count rows in " + string(join->qep_tab->table()->alias);
      break;
    case AccessPath::NESTED_LOOP_JOIN: {
      string join_type = JoinTypeToString(path->nested_loop_join().join_type);
      error |= AddMemberToObject<Json_string>(obj, "access_type", "join");
      error |= AddMemberToObject<Json_string>(obj, "join_type", join_type);
      error |=
          AddMemberToObject<Json_string>(obj, "join_algorithm", "nested_loop");
      description = "Nested loop " + join_type;
      children->push_back({path->nested_loop_join().outer});
      children->push_back({path->nested_loop_join().inner});
      break;
    }
    case AccessPath::NESTED_LOOP_SEMIJOIN_WITH_DUPLICATE_REMOVAL:
      // No json fields since this path is not supported in hypergraph
      description =
          string("Nested loop semijoin with duplicate removal on ") +
          path->nested_loop_semijoin_with_duplicate_removal().key->name;
      children->push_back(
          {path->nested_loop_semijoin_with_duplicate_removal().outer});
      children->push_back(
          {path->nested_loop_semijoin_with_duplicate_removal().inner});
      break;
    case AccessPath::BKA_JOIN: {
      string join_type = JoinTypeToString(path->bka_join().join_type);
      error |= AddMemberToObject<Json_string>(obj, "access_type", "join");
      error |= AddMemberToObject<Json_string>(obj, "join_type", join_type);
      error |= AddMemberToObject<Json_string>(obj, "join_algorithm",
                                              "batch_key_access");
      description = "Batched key access " + join_type;
      children->push_back({path->bka_join().outer, "Batch input rows"});
      children->push_back({path->bka_join().inner});
      break;
    }
    case AccessPath::HASH_JOIN: {
      const JoinPredicate *predicate = path->hash_join().join_predicate;
      RelationalExpression::Type type = path->hash_join().rewrite_semi_to_inner
                                            ? RelationalExpression::INNER_JOIN
                                            : predicate->expr->type;
      THD *const thd = current_thd;

      string json_join_type;
      description = HashJoinTypeToString(type, &json_join_type);

      unique_ptr<Json_array> hash_condition(new (std::nothrow) Json_array());
      if (hash_condition == nullptr) return nullptr;

      vector<HashJoinCondition> equijoin_conditions;
      equijoin_conditions.reserve(predicate->expr->equijoin_conditions.size());
      for (Item_eq_base *cond : predicate->expr->equijoin_conditions) {
        equijoin_conditions.emplace_back(cond, thd->mem_root);
      }
      if (equijoin_conditions.empty()) {
        description.append(" (no condition)");
      } else {
        bool first = true;
        for (const HashJoinCondition &hj_cond : equijoin_conditions) {
          if (!first) {
            description.push_back(',');
          }
          first = false;
          string condition_str;
          if (!hj_cond.store_full_sort_key()) {
            condition_str =
                "(<hash>( left_condition "
                ")=<hash>( right_condition ))";
          } else {
            condition_str = "condition";
          }
          error |=
              AddElementToArray<Json_string>(hash_condition, condition_str);
          description.append(" " + condition_str);
        }
      }
      error |= obj->add_alias("hash_condition", std::move(hash_condition));

      const Mem_root_array<Item *> *extra_join_conditions =
          GetExtraHashJoinConditions(
              thd->mem_root, thd->lex->using_hypergraph_optimizer(),
              equijoin_conditions, predicate->expr->join_conditions);
      if (extra_join_conditions == nullptr) return nullptr;

      unique_ptr<Json_array> extra_condition(new (std::nothrow) Json_array());
      if (extra_condition == nullptr) return nullptr;
      bool first = true;
      for (uint i = 0; i < extra_join_conditions->size(); i++) {
        if (first) {
          description.append(", extra conditions: ");
          first = false;
        } else {
          description += " and ";
        }
        string condition_str = "condition_str";
        description += condition_str;
        error |= AddElementToArray<Json_string>(extra_condition, condition_str);
      }
      if (extra_condition->size() > 0)
        error |= obj->add_alias("extra_condition", std::move(extra_condition));

      error |= AddMemberToObject<Json_string>(obj, "access_type", "join");
      error |= AddMemberToObject<Json_string>(obj, "join_type", json_join_type);
      error |= AddMemberToObject<Json_string>(obj, "join_algorithm", "hash");
      children->push_back({path->hash_join().outer});
      children->push_back({path->hash_join().inner, "Hash"});

      const RelationalExpression *join_predicate =
          path->hash_join().join_predicate->expr;
      for (Item_eq_base *cond : join_predicate->equijoin_conditions) {
        AddSubqueryPaths(cond, "condition", children);
      }
      for (Item *cond : join_predicate->join_conditions) {
        AddSubqueryPaths(cond, "extra conditions", children);
      }

      break;
    }
    case AccessPath::FILTER: {
      error |= AddMemberToObject<Json_string>(obj, "access_type", "filter");
      string filter = "filter_condition";
      error |= AddMemberToObject<Json_string>(obj, "condition", filter);
      description = "Filter: " + filter;
      children->push_back({path->filter().child});
      AddSubqueryPaths(path->filter().condition, "condition", children);
      break;
    }
    case AccessPath::SORT: {
      error |= AddMemberToObject<Json_string>(obj, "access_type", "sort");
      if (path->sort().force_sort_rowids) {
        description = "Sort row IDs";
        error |= AddMemberToObject<Json_boolean>(obj, "row_ids", true);
      } else {
        description = "Sort";
      }
      if (path->sort().remove_duplicates) {
        description += " with duplicate removal: ";
        error |=
            AddMemberToObject<Json_boolean>(obj, "duplicate_removal", true);
      } else {
        description += ": ";
      }

      unique_ptr<Json_array> sort_fields(new (std::nothrow) Json_array());
      if (sort_fields == nullptr) return nullptr;
      for (ORDER *order = path->sort().order; order != nullptr;
           order = order->next) {
        if (order != path->sort().order) {
          description += ", ";
        }

        // We usually want to print the item_name if it's set, so that we get
        // the alias instead of the full expression when there is an alias. If
        // it is a field reference, we prefer ItemToString() because item_name
        // in Item_field doesn't include the table name.
        string sort_field;
        if (const Item *item = *order->item;
            item->item_name.is_set() && item->type() != Item::FIELD_ITEM) {
          sort_field = item->item_name.ptr();
        } else {
          sort_field = ItemToString(item);
        }
        if (order->direction == ORDER_DESC) {
          sort_field += " DESC";
        }
        description += sort_field;
        error |= AddElementToArray<Json_string>(sort_fields, sort_field);
      }
      error |= obj->add_alias("sort_fields", std::move(sort_fields));

      if (const ha_rows limit = path->sort().limit; limit != HA_POS_ERROR) {
        char buf[256];
        error |= AddMemberToObject<Json_int>(obj, "per_chunk_limit", limit);
        snprintf(buf, sizeof(buf), ", limit input to limit row(s) per chunk");
        description += buf;
      }
      children->push_back({path->sort().child});
      break;
    }
    case AccessPath::AGGREGATE: {
      string ret;
      error |= AddMemberToObject<Json_string>(obj, "access_type", "aggregate");
      if (join->grouped || join->group_optimized_away) {
        error |= AddMemberToObject<Json_boolean>(obj, "group_by", true);
        if (*join->sum_funcs == nullptr) {
          description = "Group (no aggregates)";
        } else if (path->aggregate().olap == ROLLUP_TYPE) {
          error |= AddMemberToObject<Json_boolean>(obj, "rollup", true);
          description = "Group aggregate with rollup: ";
        } else if (path->aggregate().olap == CUBE_TYPE) {
          error |= AddMemberToObject<Json_boolean>(obj, "cube", true);
          description = "Group aggregate with cube: ";
        } else {
          description = "Group aggregate: ";
        }
      } else {
        description = "Aggregate: ";
      }

      unique_ptr<Json_array> funcs(new (std::nothrow) Json_array());
      if (funcs == nullptr) return nullptr;
      bool first = true;
      for (Item_sum **item = join->sum_funcs; *item != nullptr; ++item) {
        if (first) {
          first = false;
        } else {
          description += ", ";
        }
        string func = "item";
        description += func;
        error |= AddElementToArray<Json_string>(funcs, func);
      }

      // If there are no aggs, still let this field print a "" rather than
      // omit this field.
      error |= obj->add_alias("functions", std::move(funcs));

      children->push_back({path->aggregate().child});
      break;
    }
    case AccessPath::TEMPTABLE_AGGREGATE: {
      error |= AddMemberToObject<Json_string>(obj, "access_type",
                                              "temp_table_aggregate");
      ret_obj = AssignParentPath(path->temptable_aggregate().table_path,
                                 nullptr, std::move(ret_obj), join);
      if (ret_obj == nullptr) return nullptr;
      description = "Aggregate using temporary table";
      children->push_back({path->temptable_aggregate().subquery_path});
      break;
    }
    case AccessPath::CONNECT_BY_SCAN: {
      error |= AddMemberToObject<Json_string>(obj, "access_type", "connect_by");

      auto param = &path->connect_by_scan();

      description = "connect by scan:";
      if (param->connect_by_param->nocycle) {
        description += "(nocycle)";
      }
      if (param->connect_by_param->ref) {
        auto cache_table = param->connect_by_param->cache->tab;
        auto key = cache_table->key_info + 1;
        error |= SetIndexInfoInObject(
            &description, "index_lookup", nullptr, *cache_table,
            *(cache_table->key_info + 1), "lookup",
            RefToString(*param->connect_by_param->ref, *key,
                        /*include_nulls=*/false),
            /*ranges=*/nullptr, nullptr, true,
            cache_table->file->pushed_idx_cond, obj);
        if (param->connect_by_param->connect_by_cond) {
          description += " and filter with: ";
        }
      }
      if (param->connect_by_param->connect_by_cond) {
        description += "condition";
      }

      if (param->connect_by_param->start_with_cond) {
        description += string(" start with: condition");
      }
      children->push_back({param->src_path});
      if (param->connect_by_param->start_with_cond) {
        AddSubqueryPaths(param->connect_by_param->start_with_cond, "start_with",
                         children);
      }
    } break;
    case AccessPath::LIMIT_OFFSET: {
      error |= AddMemberToObject<Json_string>(obj, "access_type", "limit");
      char buf[256] = {0};
      auto limit_cnt = &path->limit_offset();
      if ((limit_cnt->percent_value > 0) || limit_cnt->is_with_ties) {
        if (limit_cnt->offset == 0) {
          if (limit_cnt->percent_value > 0) {
            snprintf(buf, sizeof(buf),
                     "Fetch next percent_value percent row(s) %s",
                     limit_cnt->is_with_ties ? "with ties" : "only");
          } else {
            snprintf(buf, sizeof(buf), "Fetch next limit row(s) %s",
                     limit_cnt->is_with_ties ? "with ties" : "only");
          }
        } else {
          if (limit_cnt->percent_value > 0) {
            snprintf(
                buf, sizeof(buf),
                "Offset offset rows Fetch next percent_value percent row(s) %s",
                limit_cnt->is_with_ties ? "with ties" : "only");
          } else {
            snprintf(buf, sizeof(buf),
                     "Offset offset rows Fetch next limit row(s) %s",
                     limit_cnt->is_with_ties ? "with ties" : "");
          }
        }
      } else {
        if (limit_cnt->offset == 0) {
          snprintf(buf, sizeof(buf), "Limit: limit row(s)");
        } else if (limit_cnt->limit == HA_POS_ERROR) {
          snprintf(buf, sizeof(buf), "Offset: offset row(s)");
        } else {
          snprintf(buf, sizeof(buf), "Limit/Offset: percent row(s)");
        }
      }
      error |=
          AddMemberToObject<Json_int>(obj, "limit", path->limit_offset().limit);
      error |= AddMemberToObject<Json_int>(obj, "limit_offset",
                                           path->limit_offset().offset);
      if (limit_cnt->count_all_rows) {
        error |= AddMemberToObject<Json_boolean>(obj, "count_all_rows", true);
        description =
            string(buf) + " (no early end due to SQL_CALC_FOUND_ROWS)";
      } else {
        description = buf;
      }
      children->push_back({path->limit_offset().child});
      break;
    }
    case AccessPath::STREAM:
      error |= AddMemberToObject<Json_string>(obj, "access_type", "stream");
      description = "Stream results";
      children->push_back({path->stream().child});
      break;
    case AccessPath::MATERIALIZE:
      error |=
          AddMemberToObject<Json_string>(obj, "access_type", "materialize");
      ret_obj =
          ExplainMaterializeAccessPath(path, join, std::move(ret_obj), children,
                                       current_thd->lex->is_explain_analyze);
      if (ret_obj == nullptr) return nullptr;
      break;
    case AccessPath::MATERIALIZE_INFORMATION_SCHEMA_TABLE: {
      ret_obj = AssignParentPath(
          path->materialize_information_schema_table().table_path, nullptr,
          std::move(ret_obj), join);
      if (ret_obj == nullptr) return nullptr;
      const TABLE *table =
          path->materialize_information_schema_table().table_list->table;
      error |= AddTableInfoToObject(obj, table);
      error |= AddMemberToObject<Json_string>(obj, "access_type",
                                              "materialize_information_schema");
      description = "Fill information schema table " + string(table->alias);
      break;
    }
    case AccessPath::APPEND:
      error |= AddMemberToObject<Json_string>(obj, "access_type", "append");
      description = "Append";
      for (const AppendPathParameters &child : *path->append().children) {
        children->push_back({child.path, "", child.join});
      }
      break;
    case AccessPath::WINDOW: {
      Window *const window = path->window().window;
      if (path->window().needs_buffering) {
        error |= AddMemberToObject<Json_boolean>(obj, "buffering", true);
        if (window->optimizable_row_aggregates() ||
            window->optimizable_range_aggregates() ||
            window->static_aggregates()) {
          description = "Window aggregate with buffering: ";
        } else {
          error |= AddMemberToObject<Json_boolean>(obj, "multi_pass", true);
          description = "Window multi-pass aggregate with buffering: ";
        }
      } else {
        description = "Window aggregate: ";
      }

      unique_ptr<Json_array> funcs(new (std::nothrow) Json_array());
      if (funcs == nullptr) return nullptr;
      bool first = true;
      for (uint i = 0; i < window->functions().size(); i++) {
        if (!first) {
          description += ", ";
        }
        string func_str = "sum_func";
        description += func_str;
        error |= AddElementToArray<Json_string>(funcs, func_str);
        first = false;
      }
      error |= obj->add_alias("functions", std::move(funcs));
      error |= AddMemberToObject<Json_string>(obj, "access_type", "window");
      children->push_back({path->window().child});
      // temp_table_param may be nullptr for secondary engine,
      // see ExplainWindowForExternalExecutor in hypergraph_optimizer-t.cc.
      if (path->window().temp_table_param != nullptr) {
        for (const Func_ptr &func :
             *path->window().temp_table_param->items_to_copy) {
          AddSubqueryPaths(func.func(), "projection", children);
        }
      }

      break;
    }
    case AccessPath::WEEDOUT: {
      SJ_TMP_TABLE *sj = path->weedout().weedout_table;
      unique_ptr<Json_array> tables(new (std::nothrow) Json_array());
      if (tables == nullptr) return nullptr;

      description = "Remove duplicate ";
      if (sj->tabs_end == sj->tabs + 1) {  // Only one table.
        description += sj->tabs->qep_tab->table()->alias;
        error |= AddElementToArray<Json_string>(
            tables, sj->tabs->qep_tab->table()->alias);
      } else {
        description += "(";
        for (SJ_TMP_TABLE_TAB *tab = sj->tabs; tab != sj->tabs_end; ++tab) {
          if (tab != sj->tabs) {
            description += ", ";
          }
          description += tab->qep_tab->table()->alias;
          error |= AddElementToArray<Json_string>(tables,
                                                  tab->qep_tab->table()->alias);
        }
        description += ")";
      }
      description += " rows using temporary table (weedout)";
      error |= obj->add_alias("tables", std::move(tables));
      error |= AddMemberToObject<Json_string>(obj, "access_type", "weedout");
      children->push_back({path->weedout().child});
      break;
    }
    case AccessPath::REMOVE_DUPLICATES: {
      description = "Remove duplicates from input grouped on ";
      unique_ptr<Json_array> group_items(new (std::nothrow) Json_array());
      if (group_items == nullptr) return nullptr;
      for (int i = 0; i < path->remove_duplicates().group_items_size; ++i) {
        string group_item = "group_items";
        if (i != 0) {
          description += ", ";
        }
        description += group_item;
        error |= AddElementToArray<Json_string>(group_items, group_item);
      }
      error |= AddMemberToObject<Json_string>(obj, "access_type",
                                              "remove_duplicates_from_groups");
      error |= obj->add_alias("group_items", std::move(group_items));
      children->push_back({path->remove_duplicates().child});
      break;
    }
    case AccessPath::REMOVE_DUPLICATES_ON_INDEX: {
      const char *keyname = path->remove_duplicates_on_index().key->name;
      description = string("Remove duplicates from input sorted on ") + keyname;
      error |= AddMemberToObject<Json_string>(obj, "access_type",
                                              "remove_duplicates_on_index");
      error |= AddMemberToObject<Json_string>(obj, "index_name", keyname);
      children->push_back({path->remove_duplicates_on_index().child});
      break;
    }
    case AccessPath::ALTERNATIVE: {
      const TABLE &table =
          *path->alternative().table_scan_path->table_scan().table;
      const Index_lookup *ref = path->alternative().used_ref;
      const KEY &key = table.key_info[ref->key];

      int num_applicable_cond_guards = 0;
      for (unsigned key_part_idx = 0; key_part_idx < ref->key_parts;
           ++key_part_idx) {
        if (ref->cond_guards[key_part_idx] != nullptr) {
          ++num_applicable_cond_guards;
        }
      }

      description = "Alternative plans for IN subquery: Index lookup unless ";
      if (num_applicable_cond_guards > 1) {
        description += " any of (";
      }
      bool first = true;
      for (unsigned key_part_idx = 0; key_part_idx < ref->key_parts;
           ++key_part_idx) {
        if (ref->cond_guards[key_part_idx] != nullptr) {
          if (!first) {
            description += ", ";
          }
          first = false;
          description += key.key_part[key_part_idx].field->field_name;
        }
      }
      if (num_applicable_cond_guards > 1) {
        description += ")";
      }
      description += " IS NULL";
      error |= AddMemberToObject<Json_string>(
          obj, "access_type", "alternative_plans_for_in_subquery");
      children->push_back({path->alternative().child});
      children->push_back({path->alternative().table_scan_path});
      break;
    }
    case AccessPath::CACHE_INVALIDATOR:
      description = string("Invalidate materialized tables (row from ") +
                    path->cache_invalidator().name + ")";
      error |= AddMemberToObject<Json_string>(obj, "access_type",
                                              "invalidate_materialized_tables");
      error |= AddMemberToObject<Json_string>(obj, "table_name",
                                              path->cache_invalidator().name);
      children->push_back({path->cache_invalidator().child});
      break;
    case AccessPath::DELETE_ROWS: {
      error |=
          AddMemberToObject<Json_string>(obj, "access_type", "delete_rows");
      string tables;
      for (Table_ref *t = join->query_block->leaf_tables; t != nullptr;
           t = t->next_leaf) {
        if (Overlaps(t->map(), path->delete_rows().tables_to_delete_from)) {
          if (!tables.empty()) {
            tables.append(", ");
          }
          tables.append(t->alias);
          if (Overlaps(t->map(), path->delete_rows().immediate_tables)) {
            tables.append(" (immediate)");
          } else {
            tables.append(" (buffered)");
          }
        }
      }
      error |= AddMemberToObject<Json_string>(obj, "tables", tables);
      description = string("Delete from ") + tables;
      children->push_back({path->delete_rows().child});
      break;
    }
    case AccessPath::UPDATE_ROWS: {
      string tables;
      for (Table_ref *t = join->query_block->leaf_tables; t != nullptr;
           t = t->next_leaf) {
        if (Overlaps(t->map(), path->update_rows().tables_to_update)) {
          if (!tables.empty()) {
            tables.append(", ");
          }
          tables.append(t->alias);
          if (Overlaps(t->map(), path->update_rows().immediate_tables)) {
            tables.append(" (immediate)");
          } else {
            tables.append(" (buffered)");
          }
        }
      }
      description = string("Update ") + tables;
      children->push_back({path->update_rows().child});
      break;
    }
    case AccessPath::COUNTER: {
      description = string(path->counter().counter->Description());
      children->push_back({path->counter().child});
      break;
    }
#ifdef HAVE_QUERY_PLAN_PLUGIN
    case AccessPath::QUERY_PLAN_EXECUTE: {
      /*auto &param = path->query_plan_execute();
      error |= AddMemberToObject<Json_string>(obj, "access_type",
                                              "query_plan_plugin_execution");
      error |= explain_additional_query_plan(
          description, children, param.native_path, param.native_outlist);*/
      break;
    }
#endif
  }
#ifdef HAVE_QUERY_PLAN_PLUGIN
  if (path->type != AccessPath::QUERY_PLAN_EXECUTE)
#endif
    // Append the various costs.
    error |= AddPathCosts(path, materialized_path, obj,
                          current_thd->lex->is_explain_analyze);

  // Empty description means the object already has the description set above.
  if (!description.empty()) {
    // Create JSON objects for description strings.
    error |= AddMemberToObject<Json_string>(obj, "operation", description);
  }

  return (error ? nullptr : std::move(ret_obj));
}

static std::unique_ptr<Json_object> gdb_ExplainAccessPath(
    const AccessPath *path, const AccessPath *materialized_path, JOIN *join,
    bool is_root_of_join, unique_ptr<Json_object> root_obj) {
  bool error = false;
  vector<ExplainChild> children;

  if (root_obj == nullptr) {
    root_obj = create_dom_ptr<Json_object>();
    if (root_obj == nullptr) return nullptr;
  }

  // Keep a handle to the original object.
  Json_object *original_object = root_obj.get();

  // This should not happen, but some unit tests have shown to cause null child
  // paths to be present in the AccessPath tree.
  if (path == nullptr) {
    if (AddMemberToObject<Json_string>(root_obj, "operation",
                                       "<not executable by iterator executor>"))
      return nullptr;
    return root_obj;
  }

  root_obj = SetObjectMembers(std::move(root_obj), path, materialized_path,
                              join, &children);
  if (root_obj == nullptr) return nullptr;

  // If we are crossing into a different query block, but there's a streaming
  // or materialization node in the way, don't count it as the root; we want
  // any SELECT printouts to be on the actual root node.
  // TODO(sgunders): This gives the wrong result if a query block ends in a
  // materialization.
  bool delayed_root_of_join = false;
  if (path->type == AccessPath::STREAM ||
      path->type == AccessPath::MATERIALIZE) {
    delayed_root_of_join = is_root_of_join;
    is_root_of_join = false;
  }

  // If we know that the join will return zero rows, we don't bother
  // optimizing any subqueries in the SELECT list, but end optimization
  // early (see Query_block::optimize()). If so, don't attempt to print
  // them either, as they have no query plan.
  if (is_root_of_join && path->type != AccessPath::ZERO_ROWS) {
    vector<ExplainChild> children_from_select;
    if (GetAccessPathsFromSelectList(join, &children_from_select))
      return nullptr;

    // Return 'true' if 'children' contains an object with the same 'path'
    // as 'sel_child'.
    const auto in_children = [&children](const ExplainChild &sel_child) {
      return std::any_of(children.cbegin(), children.cend(),
                         [&sel_child](const ExplainChild &child) {
                           return sel_child.path == child.path;
                         });
    };

    // Remove objects from children_from_select where 'children' has
    // an object with the same 'path', so that we do not print the same path
    // twice.
    children_from_select.erase(
        std::remove_if(children_from_select.begin(), children_from_select.end(),
                       in_children),
        children_from_select.end());

    if (AddChildrenToObject(
            original_object, std::move(children_from_select), join,
            /*is_root_of_join*/ true, "inputs_from_select_list"))
      return nullptr;
  }

  if (AddChildrenToObject(original_object, std::move(children), join,
                          delayed_root_of_join, "inputs")) {
    return nullptr;
  }

  if (error == 0)
    return root_obj;
  else
    return nullptr;
}

///////////////////////////////////////////////////////////////////////////
// print json to string
///////////////////////////////////////////////////////////////////////////
/*
  The out param 'child_token_digest' will have something like :
  ",[child1_desc:]0xchild1,[child2_desc:]0xchild2,....."
*/
static void AppendChildren(const Json_dom *children, int level,
                           std::string *explain, std::string *) {
  if (children == nullptr) {
    return;
  }
  assert(children->json_type() == enum_json_type::J_ARRAY);
  for (const Json_dom_ptr &child : *down_cast<const Json_array *>(children)) {
    if (child->json_type() == enum_json_type::J_OBJECT &&
        down_cast<const Json_object *>(child.get())->get("heading") !=
            nullptr) {
      std::string heading =
          down_cast<Json_string *>(
              down_cast<const Json_object *>(child.get())->get("heading"))
              ->value();

      if (level) explain->append(" ");
      explain->append(std::to_string(level));
      explain->append(".");
      explain->append(heading);
      ExplainPrintTreeNode(child.get(), level + 1, explain);
    } else {
      ExplainPrintTreeNode(child.get(), level, explain);
    }
  }
}

void ExplainPrintTreeNode(const Json_dom *json, int level,
                          std::string *explain) {
  std::string children_explain;
  std::string children_digest;

  if (level) explain->append(" ");
  explain->append(std::to_string(level));
  explain->append(".");

  if (json == nullptr || json->json_type() == enum_json_type::J_NULL) {
    explain->append("<not executable by iterator executor>\n");
    return;
  }

  const Json_object *obj = down_cast<const Json_object *>(json);

  AppendChildren(obj->get("inputs"), level + 1, &children_explain,
                 &children_digest);
  AppendChildren(obj->get("inputs_from_select_list"), level, &children_explain,
                 &children_digest);

  assert(obj->get("operation")->json_type() == enum_json_type::J_STRING);
  // serialize obj, delete condition and rows info
  *explain += down_cast<Json_string *>(obj->get("operation"))->value();

  *explain += children_explain;
}
