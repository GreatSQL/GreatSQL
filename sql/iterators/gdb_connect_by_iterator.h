/* Copyright (c) 2023, GreatDB Software Co., Ltd. All rights reserved.

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

bool SetupConnectByTmp(THD *thd, JOIN *join, uint qep_tab_n,
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
  TempStore(THD *thd, size_t size)
      : fields(thd->mem_root),
        ref_items(Ref_item_array::Alloc(thd->mem_root, size)) {}
  bool Destory();
  ~TempStore() { Destory(); }
  mem_root_deque<Item *> fields;
  Ref_item_array ref_items;
};

class Connect_by_param : public Collect_leaf_cycle_info {
 public:
  Connect_by_param(THD *thd, Item *start_with, Item *connect_by,
                   bool nocycle_arg)
      : Collect_leaf_cycle_info(thd),
        start_with_cond(start_with),
        connect_by_cond(connect_by),
        nocycle(nocycle_arg),
        connect_by_func_list(thd->mem_root),
        ref(nullptr),
        start_rownum_it(nullptr),
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
  mem_root_deque<Item_connect_by_func *> connect_by_func_list;
  Item_connect_by_func_level *level_it;  // result
  /**
    To obtain the non-recursive preorder ,
    this need to compare the results in reverse order of connect_by_cond.
    However, the iterator can only access the data in forward order.

    1. cache the results obtained in the forward order save into result,
    2. the use an index  in reverse order insert to a stack
  */
  unique_ptr_destroy_only<TempStore> cache;
  unique_ptr_destroy_only<TempStore> stack;
  /**
    check is cycle
  */
  unique_ptr_destroy_only<TempTableParam> history;

  Index_lookup *ref;

  // rownum
  Item_func_rownum *start_rownum_it;
  Item *connect_by_rownum_it;
};

namespace connect_by_iterator {
RowIterator *CreateIterator(THD *thd, JOIN *join, TABLE *table,
                            Temp_table_param *temp_table_param,
                            unique_ptr_destroy_only<RowIterator> src_iter,
                            unique_ptr_destroy_only<RowIterator> res_iter,
                            Connect_by_param *connect_by_param, int ref_slice);
}
#endif
