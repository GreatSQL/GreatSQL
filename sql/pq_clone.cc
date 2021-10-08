/* Copyright (c) 2020, Oracle and/or its affiliates. All rights reserved.
   Copyright (c) 2021, Huawei Technologies Co., Ltd.
   Copyright (c) 2021, GreatDB Software Co., Ltd.

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

#include "sql/pq_clone.h"
#include "include/my_dbug.h"
#include "include/mysql/psi/mysql_thread.h"
#include "sql/mysqld.h"
#include "sql/opt_range.h"
#include "sql/sql_base.h"
#include "sql/sql_lex.h"
#include "sql/sql_opt_exec_shared.h"
#include "sql/sql_optimizer.h"
#include "sql/sql_parallel.h"
#include "sql/sql_resolver.h"
#include "sql/system_variables.h"

class COND_CMP;
bool propagate_cond_constants(THD *thd, I_List<COND_CMP> *save_list,
                              Item *and_father, Item *cond);

bool POSITION::pq_copy(THD *thd, POSITION *orig) {
  rows_fetched = orig->rows_fetched;
  read_cost = orig->read_cost;
  filter_effect = orig->filter_effect;
  prefix_rowcount = orig->prefix_rowcount;
  prefix_cost = orig->prefix_cost;
  table = orig->table;
  if (orig->key)
    key = orig->key->pq_clone(thd);
  else
    key = nullptr;
  ref_depend_map = orig->ref_depend_map;
  use_join_buffer = orig->use_join_buffer;
  sj_strategy = orig->sj_strategy;
  n_sj_tables = orig->n_sj_tables;
  dups_producing_tables = orig->dups_producing_tables;
  first_loosescan_table = orig->first_loosescan_table;
  loosescan_need_tables = orig->loosescan_need_tables;
  loosescan_key = orig->loosescan_key;
  loosescan_parts = orig->loosescan_parts;
  first_firstmatch_table = orig->first_firstmatch_table;
  first_firstmatch_rtbl = orig->first_firstmatch_rtbl;
  firstmatch_need_tables = orig->firstmatch_need_tables;
  first_dupsweedout_table = orig->first_dupsweedout_table;
  dupsweedout_tables = orig->dupsweedout_tables;
  sjm_scan_last_inner = orig->sjm_scan_last_inner;
  sjm_scan_need_tables = orig->sjm_scan_need_tables;

  return false;
}

bool QEP_TAB::pq_copy(THD *thd, QEP_TAB *orig) {
  set_type(orig->type());
  set_index(orig->index());
  set_first_inner(orig->first_inner());
  set_last_inner(orig->last_inner());
  set_first_sj_inner(orig->first_sj_inner());
  set_last_sj_inner(orig->last_sj_inner());
  keys().merge(orig->keys());
  m_reversed_access = orig->m_reversed_access;
  do_parallel_scan = orig->do_parallel_scan;
  cache_idx_cond = orig->cache_idx_cond;

  POSITION *position = new (thd->pq_mem_root) POSITION;
  if (!position || position->pq_copy(thd, orig->position())) return true;

  set_position(position);
  if (orig->pq_cond) {
    JOIN *join = this->join();
    if (!join) return true;
    pq_cond = orig->pq_cond->pq_clone(join->thd, join->query_block);
    if (!pq_cond) return true;
  }

  return false;
}

bool TABLE::pq_copy(THD *thd, void *select_arg, TABLE *orig) {
  Query_block *select = static_cast<Query_block *>(select_arg);
  possible_quick_keys = orig->possible_quick_keys;
  covering_keys = orig->covering_keys;
  key_read = orig->key_read;
  const_table = orig->const_table;
  nullable = orig->nullable;
  null_row = orig->null_row;
  m_cost_model = orig->m_cost_model;
  memcpy(record[0], orig->record[0], orig->s->rec_buff_length);

  reginfo = orig->reginfo;

  file->pushed_idx_cond_keyno = orig->file->pushed_idx_cond_keyno;
  Item *index_pushdown = orig->file->pushed_idx_cond;
  // needs deep copy
  file->pushed_idx_cond =
      index_pushdown ? index_pushdown->pq_clone(thd, select) : nullptr;
  Item *copy_index_pushdown = file->pushed_idx_cond;
  if ((index_pushdown && !copy_index_pushdown) ||
      (copy_index_pushdown &&
       copy_index_pushdown->fix_fields(thd, &copy_index_pushdown))) {
    return true;
  }

  return false;
}

/*
 * copy table_ref info.
 *
 * @retval:
 *    false if copy successfully, and otherwise true.
 */
bool TABLE_REF::pq_copy(JOIN *join, TABLE_REF *ref, QEP_TAB *qep_tab) {
  THD *thd = join->thd;
  key_parts = ref->key_parts;
  key_length = ref->key_length;
  key_err = ref->key_err;
  key = ref->key;
  null_rejecting = ref->null_rejecting;
  depend_map = ref->depend_map;
  use_count = ref->use_count;
  disable_cache = ref->disable_cache;

  if (!(key_buff = (uchar *)thd->mem_calloc(2 * ALIGN_SIZE(key_length))) ||
      !(key_copy = (store_key **)thd->mem_calloc(
            (sizeof(store_key *) * (key_parts)))) ||
      !(items = (Item **)thd->mem_calloc(sizeof(Item *) * key_parts)) ||
      !(cond_guards = (bool **)thd->mem_calloc(sizeof(uint *) * key_parts)))
    return true;

  if (ref->null_ref_key != nullptr) null_ref_key = key_buff;

  key_buff2 = key_buff + ALIGN_SIZE(key_length);
  memcpy(key_buff, ref->key_buff, key_length);
  memcpy(key_buff2, ref->key_buff2, key_length);
  uchar *key_buff_tmp = key_buff;

  for (uint i = 0; i < key_parts; i++) {
    items[i] = ref->items[i]->pq_clone(thd, join->query_block);
    assert(DBUG_EVALUATE_IF("skip_pq_clone_check", true, false) || items[i]);
    if (!items[i]->fixed) items[i]->fix_fields(thd, &items[i]);

    if (qep_tab->table()->key_info) {
      KEY *const keyinfo = qep_tab->table()->key_info + key;
      bool maybe_null = keyinfo->key_part[i].null_bit;
      qep_tab->position()->key->val = items[i];
      qep_tab->position()->key->used_tables =
          qep_tab->position()->key->val->used_tables();
      if (ref->key_copy[i] != nullptr) {
        key_copy[i] = get_store_key(
            thd, qep_tab->position()->key->val,
            qep_tab->position()->key->used_tables, join->const_table_map,
            &keyinfo->key_part[i], key_buff_tmp, maybe_null);
      }

      key_buff_tmp += keyinfo->key_part[i].store_length;
    }

    if (!key_copy[i]) {
      key_copy[i] = ref->key_copy[i];
    }
    cond_guards[i] = ref->cond_guards[i];
  }
  return false;
}

/**
 * duplicate qep_tabs in JOIN
 *
 * @join : target JOIN
 * @orig : origin JOIN
 * @setup : setup qep_tab object
 *
 */
bool pq_dup_tabs(JOIN *join, JOIN *orig, bool setup MY_ATTRIBUTE((unused))) {
  Item *m_having_cond = nullptr;

  join->const_tables = orig->const_tables;
  join->primary_tables = orig->primary_tables;
  Query_block *select = join->query_block;

  // phase 1. Create qep_tab and qep_tab->qs;
  QEP_shared *qs = new (join->thd->pq_mem_root) QEP_shared[join->tables + 1];
  if (!qs) goto err;
  join->qep_tab0 = new (join->thd->pq_mem_root) QEP_TAB[join->tables + 1];
  if (!join->qep_tab0) goto err;
  join->qep_tab = join->qep_tab0;

  for (uint i = 0; i < join->tables; i++) {
    join->qep_tab[i].set_qs(&qs[i]);
    join->qep_tab[i].set_join(join);
    join->qep_tab[i].set_idx(i);
    join->qep_tab[i].op_type = orig->qep_tab[i].op_type;
    join->qep_tab[i].table_ref = orig->qep_tab[i].table_ref;
    join->qep_tab[i].using_dynamic_range = orig->qep_tab[i].using_dynamic_range;
  }

  for (uint i = 0; i < join->primary_tables; i++) {
    QEP_TAB *tab = &join->qep_tab[i];
    QEP_TAB *orig_tab = &orig->qep_tab[i];

    // phase 3. Set tables to qep_tab according to db/table name
    tab->pq_copy(join->thd, orig_tab);
    TABLE *tb = orig_tab->table();
    tab->table_name = new (join->thd->pq_mem_root)
        LEX_CSTRING{tb->s->table_name.str, tb->s->table_name.length};

    tab->db = new (join->thd->pq_mem_root)
        LEX_CSTRING{tb->s->db.str, tb->s->db.length};
    if (!tab->table_name || !tab->db) goto err;

    /*
     * note: currently, setup is true.
     * Because duplicate qep_tabs in JOIN need fix_field to convert item to
     * field.
     */
    assert(select->leaf_tables);
    LEX_CSTRING *db = tab->db;
    LEX_CSTRING *table_name = tab->table_name;
    const char *table_ref_alias = tab->table_ref->alias;

    // setup physic table object
    for (TABLE_LIST *tl = select->leaf_tables; tl != nullptr;
         tl = tl->next_leaf) {
      if (!strncmp(db->str, tl->db, db->length) &&
          strlen(tl->db) == db->length &&
          !strncmp(table_name->str, tl->table_name, table_name->length) &&
          strlen(tl->table_name) == table_name->length &&
          !strncmp(table_ref_alias, tl->alias, strlen(table_ref_alias)) &&
          strlen(table_ref_alias) == strlen(tl->alias)) {
        bitmap_copy(tl->table->read_set, tab->table_ref->table->read_set);
        bitmap_copy(tl->table->write_set, tab->table_ref->table->write_set);
        tab->set_table(tl->table);
        tab->table_ref = tl;
        break;
      }
    }

    // phase 4. Copy table properties from leader
    if (tab->ref().pq_copy(join, &orig_tab->ref(), tab)) goto err;

    if (orig_tab->table()) {
      tab->table()->pq_copy(join->thd, (void *)select, orig_tab->table());
      tab->set_keyread_optim();
    }

    // phase 2. clone conditions in qep_tab
    Item *condition = orig_tab->condition();
    if ((condition != nullptr) && i < orig->primary_tables) {
      Item *cond = condition->pq_clone(join->thd, select);
      assert(DBUG_EVALUATE_IF("skip_pq_clone_check", true, false) || cond);
      if (!cond) goto err;
      if (cond->fix_fields(join->thd, &cond)) {
        goto err;
      }
      tab->set_condition(cond);
      tab->set_condition_optim();
    }

    // phase 2. clone cache_idx_cond in qep_tab
    Item *cache_idx_cond = orig_tab->cache_idx_cond;
    if ((cache_idx_cond != nullptr) && i < orig->primary_tables) {
      Item *cond = cache_idx_cond->pq_clone(join->thd, select);
      assert(DBUG_EVALUATE_IF("skip_pq_clone_check", true, false) || cond);
      if (!cond) goto err;
      if (cond->fix_fields(join->thd, &cond)) {
        goto err;
      }
      tab->cache_idx_cond = cond;
    }

    // phase 5. setup pq condition for index push down
    if ((tab->has_pq_cond && !tab->pq_cond) ||
        (tab->pq_cond && tab->pq_cond->fix_fields(join->thd, &tab->pq_cond)) ||
        DBUG_EVALUATE_IF("pq_clone_error2", true, false)) {
      sql_print_warning("[Parallel query]: ICP condition pushdown failed");
      goto err;
    }

    // phase 6. copy quick select
    MEM_ROOT *saved_mem_root = join->thd->mem_root;
    if (orig_tab->quick()) {
      tab->set_quick(orig_tab->quick()->pq_clone(join->thd, tab->table()));
      assert(DBUG_EVALUATE_IF("pq_clone_error1", true, false) || tab->quick());
      if (!tab->quick()) goto err;
      tab->set_quick_optim();
    }
    join->thd->mem_root = saved_mem_root;
  }

  // phase 7. Copy having condition
  m_having_cond = select->having_cond();
  if (m_having_cond) {
    assert(m_having_cond->is_bool_func());
    join->thd->where = "having clause";
    select->having_fix_field = true;
    select->resolve_place = Query_block::RESOLVE_HAVING;
    if (!m_having_cond->fixed &&
        (m_having_cond->fix_fields(join->thd, &m_having_cond) ||
         m_having_cond->check_cols(1)))
      goto err;

    select->having_fix_field = false;
    select->resolve_place = Query_block::RESOLVE_NONE;
  }
  return false;

err:
  return true;
}

/*
 * clone order structure
 */
ORDER *pq_dup_order(THD *thd, Query_block *select, ORDER *orig) {
  ORDER *order = new (thd->pq_mem_root) ORDER();
  if (!order) return nullptr;
  order->next = nullptr;
  order->item_initial = orig->item_initial->pq_clone(thd, select);
  assert(DBUG_EVALUATE_IF("skip_pq_clone_check", true, false) ||
         order->item_initial);
  if (!order->item_initial) return nullptr;
  order->item = &order->item_initial;
  order->direction = orig->direction;
  order->in_field_list = orig->in_field_list;
  order->used_alias = orig->used_alias;
  order->field_in_tmp_table = nullptr;
  order->buff = nullptr;
  order->used = 0;
  order->depend_map = 0;
  order->is_position = orig->is_position;
  order->is_explicit = orig->is_explicit;

  return order;
}

Query_block *pq_dup_select(THD *thd, Query_block *orig) {
  Item *new_item;
  Item *where, *having;
  ORDER *group, *group_new;
  ORDER *order, *order_new;
  Query_block *select = nullptr;
  SQL_I_List<ORDER> orig_list;

  LEX *lex = new (thd->pq_mem_root) LEX();
  if (!lex) goto err;
  lex->reset();
  lex->result = orig->parent_lex->result;
  lex->sql_command = orig->parent_lex->sql_command;
  lex->explain_format = orig->parent_lex->explain_format;
  lex->is_explain_analyze = orig->parent_lex->is_explain_analyze;
  thd->lex = lex;
  lex->thd = thd;
  thd->query_plan.set_query_plan(SQLCOM_SELECT, lex, false);

  select = lex->new_query(nullptr);
  if (!select || DBUG_EVALUATE_IF("dup_select_abort1", true, false)) goto err;

  select->renumber(thd->lex);
  select->with_sum_func = orig->with_sum_func;
  select->n_child_sum_items = orig->n_child_sum_items;
  select->n_sum_items = orig->n_sum_items;
  select->select_n_having_items = orig->select_n_having_items;
  select->select_n_where_fields = orig->select_n_where_fields;
  select->m_active_options = orig->m_active_options;
  lex->set_current_query_block(select);
  lex->unit = select->master_query_expression();
  thd->lex->query_block = select;

  // phase 1. clone tables and open/lock them
  for (TABLE_LIST *tbl_list = orig->table_list.first; tbl_list != nullptr;
       tbl_list = tbl_list->next_local) {
    LEX_CSTRING *db_name =
        new (thd->pq_mem_root) LEX_CSTRING{tbl_list->db, tbl_list->db_length};
    if (db_name == nullptr) {
      goto err;
    }
    LEX_CSTRING *tbl_name = new (thd->pq_mem_root)
        LEX_CSTRING{tbl_list->table_name, tbl_list->table_name_length};
    if (tbl_name == nullptr) {
      goto err;
    }
    Table_ident *tbl_ident =
        new (thd->pq_mem_root) Table_ident(*db_name, *tbl_name);
    if (!tbl_ident ||
        !select->add_table_to_list(thd, tbl_ident, tbl_list->alias, 0))
      goto err;
  }

  assert(select->context.query_block == select);
  select->context.table_list = select->context.first_name_resolution_table =
      select->table_list.first;

  // phase 1. open tables and lock them
  if (open_tables_for_query(thd, thd->lex->query_tables, 0) ||
      select->setup_tables(thd, select->get_table_list(), false) ||
      lock_tables(thd, thd->lex->query_tables, thd->lex->table_count, 0)) {
    goto err;
  }

  // phase 1. copy table->nullable
  // before setup_fields, propagate_nullability will change table->nullable,
  // which may affect item->maybe_null, so we copy it here.
  // see in Query_block:: prepare
  for (TABLE_LIST *tl = orig->table_list.first; tl != nullptr;
       tl = tl->next_local) {
    for (TABLE_LIST *tbl_list = select->table_list.first; tbl_list != nullptr;
         tbl_list = tbl_list->next_local) {
      const char *db = tbl_list->db;
      const char *table_name = tbl_list->table_name;
      const char *alias = tbl_list->alias;

      if (!strncmp(db, tl->db, strlen(db)) && strlen(tl->db) == strlen(db) &&
          !strncmp(table_name, tl->table_name, strlen(table_name)) &&
          strlen(tl->table_name) == strlen(table_name) &&
          !strncmp(alias, tl->alias, strlen(alias)) &&
          strlen(tl->alias) == strlen(alias)) {
        if (tl->table->is_nullable()) {
          tbl_list->table->set_nullable();
        }
        break;
      }
    }
  }

  // phase 2. clone select fields list
  for (Item *item : orig->fields) {
    if (item->hidden) {
      continue;
    }
    new_item = item->pq_clone(thd, select);
    assert(DBUG_EVALUATE_IF("skip_pq_clone_check", true, false) || new_item);
    if (!new_item) goto err;

    select->fields.push_back(new_item);
  }

  // phase 3. duplicate group list
  /*
   * for template Query_block, we use leader's saved_group_list_ptrs to
   * restore original group_list, and then copy it to template. For
   * worker's Query_block, we directly use template's info to generate
   * its group_list.
   */
  if (orig->saved_group_list_ptrs) {
    restore_list(orig->saved_group_list_ptrs, orig_list);
    assert(orig_list.elements == orig->group_list.elements);
  } else {  // the case of template Query_block
    orig_list = orig->group_list;
  }

  // duplicate group list
  if (orig_list.elements) {
    for (group = orig_list.first; group; group = group->next) {
      group_new = pq_dup_order(thd, select, group);
      if (!group_new) goto err;

      select->group_list.link_in_list(group_new, &group_new->next);
    }
  }

  if (orig->saved_order_list_ptrs) {
    restore_list(orig->saved_order_list_ptrs, orig_list);
    assert(orig_list.elements == orig->order_list.elements);
  } else {  // the case of template Query_block
    orig_list = orig->order_list;
  }

  // duplicate order list
  if (orig_list.elements) {
    for (order = orig_list.first; order; order = order->next) {
      order_new = pq_dup_order(thd, select, order);
      if (!order_new) goto err;

      select->order_list.link_in_list(order_new, &order_new->next);
    }
  }

  /** mianly used for optimized_group_by */
  if (select->group_list.elements)
    select->fix_prepare_information_for_order(thd, &select->group_list,
                                              &select->saved_group_list_ptrs);
  if (select->order_list.elements)
    select->fix_prepare_information_for_order(thd, &select->order_list,
                                              &select->saved_order_list_ptrs);

  if (select->setup_base_ref_items(thd) ||
      DBUG_EVALUATE_IF("dup_select_abort2", true, false))
    goto err;

  thd->mark_used_columns = MARK_COLUMNS_READ;

  // phase 5. duplicate where cond
  if (orig->where_cond()) {
    where = orig->where_cond()->pq_clone(thd, select);
    assert(DBUG_EVALUATE_IF("skip_pq_clone_check", true, false) || where);
    if (!where) goto err;
    select->set_where_cond(where);
  } else
    select->set_where_cond(nullptr);

  // phase 6. duplicate having cond
  if (orig->having_cond()) {
    having = orig->having_cond()->pq_clone(thd, select);
    assert(DBUG_EVALUATE_IF("skip_pq_clone_check", true, false) || having);
    if (!having) goto err;
    select->set_having_cond(having);
  } else
    select->set_having_cond(nullptr);

  // phase 7: allow local set functions in HAVING and ORDER BY
  lex->allow_sum_func |= (nesting_map)1 << (nesting_map)select->nest_level;
  select->set_query_result(lex->result);
  return select;

err:
  return nullptr;
}

/**
 * resolve query block, setup tables list, fields list, group list\order list
 *
 * @select: query block
 *
 */
static bool pq_select_prepare(THD *thd, Query_block *select,
                              mem_root_deque<Item *> &orig_all_fields) {
  // Setup.1 setup all fields
  // select->all_fields = select->fields_list;
  int all_fields_count = select->fields.size();
  thd->mark_used_columns = MARK_COLUMNS_READ;
  ulong want_privilege = 0;
  if (setup_fields(thd, want_privilege, true, true, false, nullptr,
                   &select->fields, select->base_ref_items, true))
    return true;
  // Setup.2 setup GROUP BY clause
  // int all_fields_count = select->all_fields.elements;
  if (select->group_list.elements && select->setup_group(thd)) return true;
  select->hidden_group_field_count = select->fields.size() - all_fields_count;

  // Setup.3 setup ORDER BY clause
  if (select->order_list.elements &&
      setup_order(thd, select->base_ref_items, select->table_list.first,
                  &select->fields, select->order_list.first))

    return true;

  select->hidden_order_field_count = select->fields.size() - all_fields_count;

  // Setup.4: check item's property */
  if (select->fields.size() != orig_all_fields.size()) return true;

  Item *orig_item = nullptr;
  uint i = 0;
  for (Item *item : select->fields) {
    orig_item = orig_all_fields[i];
    if (!item || (item->type() != orig_item->type())) return true;
    i++;
  }

  return false;
}

JOIN *pq_make_join(THD *thd, JOIN *join) {
  JOIN *pq_join = nullptr;
  Query_block *select = pq_dup_select(thd, join->query_block);
  if (!select || pq_select_prepare(thd, select, join->query_block->fields)) {
    goto err;
  }
  thd->lex->unit->set_prepared();
  pq_join = new (thd->pq_mem_root) JOIN(thd, select);
  if (!pq_join || DBUG_EVALUATE_IF("dup_join_abort", true, false)) {
    goto err;
  }
  pq_join->pq_copy_from(join);
  /**
   * limit cannot push down to worker, for the cases:
   *   (1) with aggregation
   *   (2) with sorting after optimized-group-by
   */
  if (join->query_expression()->select_limit_cnt) {
    if (join->query_block->with_sum_func ||  // c1
        (join->pq_rebuilt_group &&           // c2
         join->pq_last_sort_idx >= (int)join->primary_tables)) {
      pq_join->m_select_limit = HA_POS_ERROR;  // no limit
      pq_join->query_expression()->select_limit_cnt = HA_POS_ERROR;
    }
  }
  return pq_join;

err:
  return nullptr;
}

bool System_variables::pq_copy_from(struct System_variables orig) {
  pseudo_thread_id = orig.pseudo_thread_id;
  sql_mode = orig.sql_mode;
  collation_connection = orig.collation_connection;
  div_precincrement = orig.div_precincrement;
  time_zone = orig.time_zone;
  big_tables = orig.big_tables;
  lc_time_names = orig.lc_time_names;
  my_aes_mode = orig.my_aes_mode;
  transaction_isolation = orig.transaction_isolation;
  option_bits = orig.option_bits;
  explicit_defaults_for_timestamp = orig.explicit_defaults_for_timestamp;
  sortbuff_size = orig.sortbuff_size;
  return false;
}

bool System_status_var::pq_merge_status(struct System_status_var worker) {
  filesort_range_count += worker.filesort_range_count;
  filesort_rows += worker.filesort_rows;
  filesort_scan_count += worker.filesort_scan_count;

  ha_read_first_count += worker.ha_read_first_count;
  ha_read_last_count += worker.ha_read_last_count;
  ha_read_key_count += worker.ha_read_key_count;
  ha_read_next_count += worker.ha_read_next_count;
  ha_read_prev_count += worker.ha_read_prev_count;
  ha_read_rnd_count += worker.ha_read_rnd_count;
  ha_read_rnd_next_count += worker.ha_read_rnd_next_count;
  return false;
}

bool THD::pq_copy_from(THD *thd) {
  variables.pq_copy_from(thd->variables);
  start_time = thd->start_time;
  user_time = thd->user_time;
  m_query_string = thd->m_query_string;
  tx_isolation = thd->tx_isolation;
  tx_read_only = thd->tx_read_only;
  parallel_exec = thd->parallel_exec;
  pq_dop = thd->pq_dop;
  arg_of_last_insert_id_function = thd->arg_of_last_insert_id_function;
  first_successful_insert_id_in_prev_stmt =
      thd->first_successful_insert_id_in_prev_stmt;
  first_successful_insert_id_in_prev_stmt_for_binlog =
      thd->first_successful_insert_id_in_prev_stmt_for_binlog;
  first_successful_insert_id_in_cur_stmt =
      thd->first_successful_insert_id_in_cur_stmt;
  stmt_depends_on_first_successful_insert_id_in_prev_stmt =
      thd->stmt_depends_on_first_successful_insert_id_in_prev_stmt;
  return false;
}

bool THD::pq_merge_status(THD *thd) {
  status_var.pq_merge_status(thd->status_var);
  current_found_rows += thd->current_found_rows;
  pq_current_found_rows = thd->current_found_rows;
  m_examined_row_count += thd->m_examined_row_count;
  return false;
}

bool THD::pq_status_reset() {
  current_found_rows = 0;
  m_examined_row_count = 0;
  return false;
}
