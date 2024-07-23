/* Copyright (c) 2023, 2024, GreatDB Software Co., Ltd. All rights reserved.

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
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/
#ifndef CONNECT_BY_ITERATOR_H_
#define CONNECT_BY_ITERATOR_H_

#include <assert.h>

#include "my_inttypes.h"
#include "sql/item.h"
#include "sql/item_func.h"
#include "sql/item_sum.h"
#include "sql/sql_tmp_table.h"
#include "sql/table.h"
#include "sql/temp_table_param.h"
class JOIN;

bool SetupConnectByTmp(THD *thd, JOIN *join,
                       mem_root_deque<Item *> *curr_fields,
                       Temp_table_param &tmp_table_param);

class TempTableParam {
 public:
  explicit TempTableParam() {}
  TABLE *tab{nullptr};
  Temp_table_param *param{nullptr};
  bool Destory();
  ~TempTableParam() { Destory(); }
};

class TempStore : public TempTableParam {
 public:
  TempStore(THD *thd, Item **el, size_t size)
      : fields(thd->mem_root), ref_items(el, size) {}
  bool Destory();
  ~TempStore() { Destory(); }
  mem_root_deque<Item *> fields;
  Ref_item_array ref_items;
};

struct ConnectbyInfo {
  longlong currLevel{1};
  longlong isCycle{0};
  longlong isLeaf{1};
};

class Connect_by_param : public ConnectbyInfo {
 public:
  Connect_by_param(THD *thd, Item *start_with, Item *connect_by,
                   bool nocycle_arg)
      : start_with_cond(start_with),
        connect_by_cond(connect_by),
        nocycle(nocycle_arg),
        connect_by_func_list(thd->mem_root),
        ref(nullptr),
        start_rownum_counter(nullptr),
        connect_by_rownum_it(nullptr) {}
  ~Connect_by_param() {
    if (ref) {
      destroy(ref);
      ref = nullptr;
    }
  }

  Item *start_with_cond;
  Item *connect_by_cond;
  bool nocycle;

  mem_root_unordered_set<Item_connect_by_func *> connect_by_func_list;

  /**
   *
    if connect by condition has prior col = col , ref is index loopup.

    To obtain the non-recursive preorder ,
    this need to compare the results in reverse order of connect_by_cond.
    However, the iterator can only access the data in forward order.
    if ref == nullptr
      1. cache the results obtained in the forward order save into result,
      2. the use an index  in reverse order insert to a stack
    else
      1. cache save source_iter result  and create index for ref
      2. insert into stack if ref match

  */
  Index_lookup *ref;
  unique_ptr_destroy_only<TempStore> cache;
  unique_ptr_destroy_only<TempStore> stack;
  /**
    check is cycle
  */
  unique_ptr_destroy_only<TempTableParam> history;
  RownumCounter *start_rownum_counter;
  Item *connect_by_rownum_it;
};

namespace connect_by_iterator {
unique_ptr_destroy_only<RowIterator> CreateIterator(
    THD *thd, JOIN *join, unique_ptr_destroy_only<RowIterator> src_iter,
    Connect_by_param *connect_by_param);
}
#endif
