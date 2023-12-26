/* Copyright (c) 2023, GreatDB Software Co., Ltd.

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

/**
  @file

  @brief
  Implementation of transform (+) to outer join stage

*/

#include "outerjoin_transformer.h"

#include "my_dbug.h"
#include "sql/item.h"
#include "sql/item_sum.h"
#include "sql/nested_join.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"
#include "sql/sql_parse.h"
#include "sql/sql_resolver.h"

// map<string, vector<string>>  or map<string, vector<Item*> >
template <class T1>
void addPairToListMap(T1 key, Item *valueContext,
                      std::map<T1, List<Item>> &objectMap) {
  typename std::map<T1, List<Item>>::iterator mit = objectMap.find(key);
  if (mit == objectMap.end()) {
    List<Item> vt;
    vt.push_back(valueContext);
    objectMap.insert(std::pair<T1, List<Item>>(key, vt));
  } else {
    mit->second.push_back(valueContext);
  }
}

void addPairToSetMap(uint key, uint value,
                     std::map<uint, std::set<uint>> &objectMap) {
  std::map<uint, std::set<uint>>::iterator mit = objectMap.find(key);
  if (mit == objectMap.end()) {
    std::set<uint> values;
    values.insert(value);
    objectMap.insert(std::pair<uint, std::set<uint>>(key, values));
  } else {
    mit->second.insert(value);
  }
}

Item *create_condition(List<Item> &join_conds) {
  assert(!join_conds.is_empty());
  Item *cond = nullptr;
  if (join_conds.elements == 1) {
    cond = join_conds.head();
  } else {
    // construct an AND'ed condition
    cond = new Item_cond_and(join_conds);
    if (cond) {
      cond->update_used_tables();
    }
  }
  return cond;
}

void set_table_outer_info(Table_ref *outer_table, Table_ref *inner_table,
                          Table_ref *join_nest, THD *thd) {
  if (!join_nest) {
    return;
  }
  join_nest->nested_join->m_tables.clear();
  join_nest->nested_join->m_tables.push_front(outer_table);
  join_nest->nested_join->m_tables.push_front(inner_table);
  outer_table->embedding = join_nest;
  inner_table->embedding = join_nest;
  outer_table->join_list = &join_nest->nested_join->m_tables;
  inner_table->join_list = &join_nest->nested_join->m_tables;

  join_nest->join_list =
      new (thd->mem_root) mem_root_deque<Table_ref *>(thd->mem_root);

  join_nest->join_list->push_back(join_nest);
}

void get_used_tables(Table_ref *tl, table_map &used_tables) {
  if (tl->nested_join) {
    for (Table_ref *item : tl->nested_join->m_tables) {
      get_used_tables(item, used_tables);
    }
  } else {
    used_tables |= tl->map();
  }
}

Table_ref *get_prev_join(std::set<Table_ref *> &nest_join_list, Table_ref *tl) {
  Table_ref *result = nullptr;
  std::set<Table_ref *>::iterator it;
  for (it = nest_join_list.begin(); it != nest_join_list.end();) {
    table_map tm = 0;
    get_used_tables(*it, tm);
    if (tm & tl->map()) {
      result = *it;
      it = nest_join_list.erase(it);
      break;
    } else {
      it++;
    }
  }
  return result;
}

bool collect_joined_key(Item *item, Query_block *select_lex,
                        List<Item> &outer_joined_keys) {
  if (!item) {
    return false;
  }
  List<Item> item_fields_or_refs;
  Item::Collect_item_fields_or_view_refs info{&item_fields_or_refs, select_lex,
                                              true};
  /**
   * walk_option: PREFIX | POSTFIX is needed when item is aggregation funcs.
   * select count(*), sum(age(+)) from t1. stopped_at_item of item walker needs
   *to set and reset.
   **/
  if (item->walk(&Item::collect_item_field_or_view_ref_processor,
                 enum_walk::PREFIX | enum_walk::POSTFIX,
                 pointer_cast<uchar *>(&info))) {
    return true;  // error
  }
  List_iterator<Item> lfi(item_fields_or_refs);
  Item *lf;

  while ((lf = lfi++)) {
    Item_ident *item_ident = dynamic_cast<Item_ident *>(lf);
    if (item_ident->outer_joined) {
      outer_joined_keys.push_back(lf);
    }
  }
  return false;
}

bool Outerjoin_transformer::check_single_outer_join_cond(Item *item) {
  List<Item> item_fields_or_refs;
  Item::Collect_item_fields_or_view_refs info{&item_fields_or_refs,
                                              m_select_lex, true};
  if (item->walk(&Item::collect_item_field_or_view_ref_processor,
                 enum_walk::PREFIX | enum_walk::POSTFIX,
                 pointer_cast<uchar *>(&info))) {
    return true;
  }
  // rownum or where exists(subquery).
  if (has_rownum_func(item, current_thd) ||
      (item_fields_or_refs.elements == 0)) {
    m_outer_info.where_conds.push_back(item);
    return false;
  }

  List_iterator<Item> lfi(item_fields_or_refs);
  Item *lf;
  // key is inner table no. value is fields list used in this cond belong to it.
  std::map<uint, List<Item>> joined_tables;
  std::vector<uint> o_tables;           // tables no of fields with no "+".
  std::vector<Item *> outer_ref_item;   // parent select fields.
  std::vector<Item *> plus_fields;      // fields with "+"
  std::vector<Item *> non_plus_fields;  // fields no "+"
  while ((lf = lfi++)) {
    Item_ident *item_ident = dynamic_cast<Item_ident *>(lf);
    if (item_ident->depended_from) {
      outer_ref_item.emplace_back(lf);
    }
    Table_ref *tl = item_ident->cached_table;
    /**
     * SELECT * FROM t11 UNION SELECT * FROM t11
     * ORDER BY(SELECT a FROM t22 WHERE b=12);
     * b is output alias of union set. the cached_table of it is empty.
     * */
    if (!tl) {
      m_outer_info.where_conds.push_back(item);
      return false;
    }
    assert(tl);
    uint table_no = tl->tableno();
    std::map<uint, Table_ref *> &table_no_map = m_outer_info.table_no_map;
    std::map<uint, Table_ref *>::iterator finder = table_no_map.find(table_no);
    if (finder == table_no_map.end()) {
      table_no_map.insert(std::pair<uint, Table_ref *>(table_no, tl));
    }
    if (item_ident->outer_joined) {
      addPairToListMap<uint>(table_no, lf, joined_tables);
      plus_fields.emplace_back(lf);

    } else {
      std::vector<uint>::iterator it =
          std::find(o_tables.begin(), o_tables.end(), table_no);
      if (it == o_tables.end()) {
        o_tables.emplace_back(table_no);
      }
      non_plus_fields.emplace_back(lf);
    }
  }

  Item_subselect::Collect_subq_info subqueries(m_select_lex);
  item->walk(&Item::collect_subqueries, enum_walk::PREFIX,
             pointer_cast<uchar *>(&subqueries));

  if (!joined_tables.empty()) {
    // select * from t1 where t1.ida(+) = (subquery);
    if (!subqueries.list.empty()) {
      my_error(ER_OUTER_JOIN_INVALID, MYF(0),
               "a column may not be outer-joined to a subquery");
      return true;
    }

    Item_func *func = down_cast<Item_func *>(item);
    Item_func::Functype functype = func->functype();
    // select * from t1 where ida(+) in (age, 2, ...);
    if (functype == Item_func::IN_FUNC) {
      my_error(ER_OUTER_JOIN_INVALID, MYF(0),
               "outer join operator (+) not allowed in operand of OR or IN");
      return true;
    }
    /**
     * select * from t1, t2, t3 where t1.ida(+) = t2.ida and t1.age(+) <t3.age.
     * t1 is outer joined by t2 and t3 is invalid.
     **/
    if (o_tables.size() > 1) {
      my_error(ER_OUTER_JOIN_INVALID, MYF(0),
               "a table may be outer joined to at most one other table");
      return true;
    }
    // select * from t1, t2, t3 where t1.ida(+) = t2.ida(+) + t3.age.
    if (joined_tables.size() > 1) {
      my_error(ER_OUTER_JOIN_INVALID, MYF(0),
               "a predicate may reference only one outer-joined table");
      return true;
    }
    if (!outer_ref_item.empty()) {
      /**
       *  select * from t1, t2 where t1.ida in (select ida from t2
       * where age(+) = t1.age); <age(+) = t1.age> match this case.
       **/
      m_outer_info.where_conds.push_back(item);
      return false;
    }
  }

  if (plus_fields.empty()) {
    // select * from t1, t2 where t1.age(+) < t2.age and t2.name = 'cc';
    // select * from t1, t2 where t1.age(+) < t2.age and t1.name = 'cc';
    // select * from t1,t2 where t1.c1=t2.c1(+) and t2.c1 is null;
    m_outer_info.where_conds.push_back(item);
    return false;
  }
  if (non_plus_fields.empty()) {
    for (std::vector<Item *>::iterator vit = plus_fields.begin();
         vit != plus_fields.end(); vit++) {
#ifndef NDEBUG
      Table_ref *inner_table = dynamic_cast<Item_ident *>(*vit)->cached_table;
#endif
      assert(inner_table);

      /**
       * sql1: select * from t1, t2 where t1.ida <3 and t2.age(+)> 5;
       * sql2: select * from t1, t2 where t2.age(+)> 5 and t2.ida(+) = t1.ida;
       * sql1:<t2.age(+) > 5> is a where cond. add it to join_cond_map, t1 and
              t2 do inner join because of empty inner_to_outer_map.
       * sql2:<t2.age(+) > 5> is a left join condition.
       * */
      Table_ref *joined_table =
          dynamic_cast<Item_ident *>(plus_fields[0])->cached_table;
      addPairToListMap<Table_ref *>(joined_table, item,
                                    m_outer_info.join_cond_map);
      return false;
    }
  }

  assert(o_tables.size() == 1);
  assert(joined_tables.size() == 1);
  for (std::map<uint, List<Item>>::iterator mit1 = joined_tables.begin();
       mit1 != joined_tables.end(); mit1++) {
    std::vector<uint> &inner_tables = m_outer_info.inner_tables;
    std::vector<uint>::iterator ir =
        std::find(inner_tables.begin(), inner_tables.end(), mit1->first);
    if (ir == inner_tables.end()) {
      m_outer_info.inner_tables.emplace_back(mit1->first);
    }
    for (std::vector<uint>::iterator sit = o_tables.begin();
         sit != o_tables.end(); sit++) {
      std::vector<uint> &outer_tables = m_outer_info.outer_tables;
      std::vector<uint>::iterator it =
          std::find(outer_tables.begin(), outer_tables.end(), *sit);
      if (it == outer_tables.end()) {
        outer_tables.emplace_back(*sit);
      }

      addPairToSetMap(mit1->first, *sit, m_outer_info.inner_to_outer_map);

      addPairToSetMap(*sit, mit1->first, m_outer_info.outer_to_inner_map);
    }
  }

  if (!plus_fields.empty()) {
    Table_ref *joined_table =
        dynamic_cast<Item_ident *>(plus_fields[0])->cached_table;
    std::map<Table_ref *, Table_ref *> &outer_joined_map =
        m_outer_info.outer_joined_map;
    assert(!non_plus_fields.empty());
    Table_ref *outer_table =
        dynamic_cast<Item_ident *>(non_plus_fields[0])->cached_table;
    outer_joined_map.insert(
        std::pair<Table_ref *, Table_ref *>(joined_table, outer_table));
    addPairToListMap<Table_ref *>(joined_table, item,
                                  m_outer_info.join_cond_map);
  }
  return false;
}

bool Outerjoin_transformer::collect_outer_join_info(THD *thd, Item *item) {
  if (item->type() == Item::COND_ITEM) {
    Item_cond *cond = (Item_cond *)item;
    if (cond->functype() == Item_func::COND_AND_FUNC) {
      List_iterator<Item> li(*cond->argument_list());
      Item *boolean_term;
      while ((boolean_term = li++)) {
        if (collect_outer_join_info(thd, boolean_term)) {
          return true;
        }
      }
    } else {
      List<Item> item_fields_or_refs;
      Item::Collect_item_fields_or_view_refs info{&item_fields_or_refs,
                                                  m_select_lex, true};
      if (item->walk(&Item::collect_item_field_or_view_ref_processor,
                     enum_walk::POSTFIX, pointer_cast<uchar *>(&info))) {
        return true;
      }
      List_iterator<Item> lfi(item_fields_or_refs);
      Item *lf;
      while ((lf = lfi++)) {
        Item_ident *item_ident = dynamic_cast<Item_ident *>(lf);
        // select * from t1, t2,..where t1.ida(+) = t2.ida or ....;
        if (item_ident->outer_joined) {
          my_error(
              ER_OUTER_JOIN_INVALID, MYF(0),
              "outer join operator (+) not allowed in operand of OR or IN");
          return true;
        }
      }
      if (check_single_outer_join_cond(item)) {
        return true;
      }
    }
  } else {
    if (check_single_outer_join_cond(item)) {
      return true;
    }
  }
  return false;
}

bool Outerjoin_transformer::check_outer_join(THD *thd, Item *item) {
  /**
   * check if item_field with '+' sign occur in select list or order list.
   * it does not include groupby/hanvin/order/and other expression now.
   * it is only select list.
   */
  List<Item> outer_joined_keys;
  for (Item *select_item : m_select_lex->visible_fields()) {
    collect_joined_key(select_item, m_select_lex, outer_joined_keys);
    Item_sum::Collect_grouped_aggregate_info aggregates(m_select_lex);
    if (select_item->walk(&Item::collect_grouped_aggregates, enum_walk::POSTFIX,
                          pointer_cast<uchar *>(&aggregates))) {
      return true;
    }
    for (Item_sum *agg : aggregates.list) {
      collect_joined_key(agg->get_arg(0), m_select_lex, outer_joined_keys);
    }
  }

  if (outer_joined_keys.elements > 0) {
    my_error(ER_OUTER_JOIN_INVALID, MYF(0),
             "outer join operator (+) is not allowed here");
    return true;
  }

  // check fields with '+' sign and came from parent select lex.
  outer_joined_keys.clear();
  collect_joined_key(item, m_select_lex, outer_joined_keys);
  collect_joined_key(m_select_lex->having_cond(), m_select_lex,
                     outer_joined_keys);

  /**
   * check group by list: if contains fields referenced from parent select lex.
   * select * from t1
     where t1.ida in (select count(*) from t2 group by t1.age(+));
   * check order by list: It makes no sense, order by is no sense in subquery.
   **/
  for (ORDER *group = m_select_lex->group_list.first; group;
       group = group->next) {
    collect_joined_key(*group->item, m_select_lex, outer_joined_keys);
  }

  // there are no fields with "+" sign, do nothing.
  if (outer_joined_keys.elements == 0) {
    return false;
  }

  List_iterator<Item> lfi(outer_joined_keys);
  Item *lf;

  while ((lf = lfi++)) {
    Item_ident *item_ident = dynamic_cast<Item_ident *>(lf);
    if (item_ident->depended_from) {
      my_error(ER_OUTER_JOIN_INVALID, MYF(0),
               "an outer join cannot be specified on a correlation column");
      return true;
    }
  }

  if (!item) {
    return false;
  }

  OUTER_INFO outer_info;
  if (collect_outer_join_info(thd, item)) {
    return true;
  }

  std::map<uint, std::set<uint>>::iterator mit;
  for (mit = m_outer_info.inner_to_outer_map.begin();
       mit != m_outer_info.inner_to_outer_map.end(); mit++) {
    std::set<uint> &values = mit->second;
    if (values.size() > 1) {
      my_error(ER_OUTER_JOIN_INVALID, MYF(0),
               "a table may be outer joined to at most one other table");
      return true;
    }
  }

  const std::vector<uint> &inner = m_outer_info.inner_tables;
  for (std::vector<uint>::const_iterator it1 = inner.begin();
       it1 != inner.end(); it1++) {
    std::map<uint, std::set<uint>>::const_iterator cit1 =
        m_outer_info.inner_to_outer_map.find(*it1);
    assert(cit1 != m_outer_info.inner_to_outer_map.end());
    uint inner_no = cit1->first;
    const std::set<uint> &outers = cit1->second;
    assert(outers.size() == 1);
    std::map<uint, std::set<uint>>::const_iterator cit2 =
        m_outer_info.inner_to_outer_map.find(*(outers.begin()));
    if (cit2 != m_outer_info.inner_to_outer_map.end()) {
      const std::set<uint> &ref_outers = cit2->second;
      std::set<uint>::const_iterator cit3 = ref_outers.find(inner_no);
      if (cit3 != ref_outers.end()) {
        my_error(ER_OUTER_JOIN_INVALID, MYF(0),
                 "two tables cannot be outer-joined to each other");
        return true;
      }
    }
  }
  return false;
}

bool Outerjoin_transformer::setup_outer_join(THD *thd, Item *&where_cond) {
  // check legality of outer join signed by "+".
  if (this->check_outer_join(thd, where_cond)) {
    return true;
  }
  // no outer join info: <select * from t1 where t1.ida(+) < 5>;
  if (m_outer_info.inner_to_outer_map.empty()) {
    return false;
  }
  // key is inner table, value is outer table;
  std::map<Table_ref *, Table_ref *> &outer_joined_map =
      m_outer_info.outer_joined_map;
  // key is inner table, value is join_cond;
  std::map<Table_ref *, List<Item>> &join_cond_map = m_outer_info.join_cond_map;

  Table_ref *emb_tbl_nest = nullptr;
  mem_root_deque<Table_ref *> *m_table_nest = &m_select_lex->m_table_nest;
  mem_root_deque<Table_ref *> emb_join_list = m_select_lex->m_table_nest;
  Table_ref *cur_join = nullptr;

  int i = 0;
  std::set<Table_ref *> join_tables;
  std::set<Table_ref *> nest_joins;
  Table_ref *outer_table = nullptr;
  std::map<Table_ref *, List<Item>>::iterator mit1;
  std::map<Table_ref *, Table_ref *>::iterator mit2;
  std::vector<uint> joined_tables = m_outer_info.inner_tables;
  for (std::vector<uint>::iterator it = joined_tables.begin();
       it != joined_tables.end(); it++) {
    std::map<uint, Table_ref *>::iterator finder =
        m_outer_info.table_no_map.find(*it);
    assert(finder != m_outer_info.table_no_map.end());
    Table_ref *tl = finder->second;
    mit1 = join_cond_map.find(tl);
    assert(mit1 != join_cond_map.end());
    Table_ref *inner_table = mit1->first;
    List<Item> join_conds = mit1->second;
    Item *on = create_condition(join_conds);

    mit2 = outer_joined_map.find(mit1->first);
    if (mit2 != outer_joined_map.end()) {
      join_tables.insert(mit2->second);
      join_tables.insert(inner_table);
      std::string alias_str("outer_join@");
      alias_str += std::to_string(i);
      // use mem from thd->mem_root
      char *alias = new (thd->mem_root) char[alias_str.size() + 1];
      strcpy(alias, alias_str.c_str());
      if (i == 0) {
        outer_table = mit2->second;
        inner_table->outer_join = true;
        add_join_on(inner_table, on);
        cur_join = Table_ref::new_nested_join(
            thd->mem_root, alias, emb_tbl_nest, &emb_join_list, m_select_lex);
        nest_joins.insert(cur_join);
      } else {
        /**
         * 1.inner table occur in previous outer join
         * 2.outer table occur in previous outer join
         * 3.it is a new nested join;
         */
        Table_ref *tmp = mit2->second;
        Table_ref *prev_join = get_prev_join(nest_joins, tmp);
        if (!prev_join) {
          prev_join = get_prev_join(nest_joins, inner_table);
          if (prev_join) {
            outer_table = tmp;
            inner_table = prev_join;
          } else {
            outer_table = tmp;
          }
        } else {
          outer_table = prev_join;
        }
        inner_table->outer_join = true;
        add_join_on(inner_table, on);

        cur_join = Table_ref::new_nested_join(
            thd->mem_root, alias, emb_tbl_nest, &emb_join_list, m_select_lex);
        nest_joins.insert(cur_join);
      }
      if (cur_join) {
        set_table_outer_info(outer_table, inner_table, cur_join, thd);
      }
      i++;
    }
  }
  for (auto it = m_table_nest->begin(); it != m_table_nest->end();) {
    auto sit = join_tables.find(*it);
    if (sit != join_tables.end()) {
      it = m_table_nest->erase(it);
    } else {
      it++;
    }
  }
  for (std::set<Table_ref *>::iterator it = nest_joins.begin();
       it != nest_joins.end(); it++) {
    m_table_nest->push_back(*it);
  }

  if (m_outer_info.where_conds.elements == 0) {
    where_cond = nullptr;
  } else {
    where_cond = create_condition(m_outer_info.where_conds);
  }

  propagate_nullability(m_table_nest, false);

  set_nullable(thd);

  return false;
}

void Outerjoin_transformer::set_nullable(THD *thd) {
  mem_root_deque<Item_field *> all_item_fields(thd->mem_root);
  for (auto &item : m_select_lex->fields) {
    item->walk(&Item::collect_item_field_processor, enum_walk::POSTFIX,
               (uchar *)&all_item_fields);
  }

  for (auto &i_item : all_item_fields) {
    Item_field *i_field = dynamic_cast<Item_field *>(i_item);
    if (i_field->field->table->is_nullable()) {
      i_field->set_nullable(true);
    }
  }
}
