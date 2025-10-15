/* Copyright (c) 2020, 2024, Oracle and/or its affiliates.
   Copyright (c) 2023, 2025, GreatDB Software Co., Ltd.

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

#ifndef SQL_PARSE_TREE_WINDOW_INCLUDED
#define SQL_PARSE_TREE_WINDOW_INCLUDED

#include "sql/parse_tree_node_base.h"
#include "sql/sql_list.h"
#include "sql/window.h"

class Item_string;
class PT_frame;
class PT_order_list;

/**
  Parse tree node for a window; just a shallow wrapper for
  class Window, q.v.
*/
class PT_window : public Parse_tree_node, public Window {
  typedef Parse_tree_node super;

 public:
  PT_window(const POS &pos, PT_order_list *partition_by,
            PT_order_list *order_by, PT_frame *frame)
      : super(pos), Window(partition_by, order_by, frame) {}

  PT_window(const POS &pos, PT_order_list *partition_by,
            PT_order_list *order_by, PT_frame *frame, Item_string *inherit)
      : super(pos), Window(partition_by, order_by, frame, inherit) {}

  PT_window(const POS &pos, Item_string *name) : super(pos), Window(name) {}

  PT_window(const POS &pos, int keep_dir_arg, PT_order_list *partition_by,
            PT_order_list *order_by, PT_frame *frame)
      : super(pos), Window(partition_by, order_by, frame) {
    this->keep_dir = keep_dir_arg;
  }

  PT_window(const POS &pos, PT_frame *frame, bool needs_frame_buffering, bool needs_peerset,
            bool needs_last_peer_in_frame, bool needs_partition_cardinality,
            bool row_optimizable, bool range_optimizable,
            bool static_aggregates, bool opt_first_row, bool opt_last_row,
            bool last, int keep_dir_arg)
      : super(pos), Window(nullptr, nullptr, frame) {
    m_needs_frame_buffering = needs_frame_buffering;
    m_needs_peerset = needs_peerset;
    m_needs_last_peer_in_frame = needs_last_peer_in_frame;
    m_needs_partition_cardinality = needs_partition_cardinality;
    m_row_optimizable = row_optimizable;
    m_range_optimizable = range_optimizable;
    m_static_aggregates = static_aggregates;
    m_opt_first_row = opt_first_row;
    m_opt_last_row = opt_last_row;
    m_last = last;
    keep_dir = keep_dir_arg;
  }

  bool do_contextualize(Parse_context *pc) override;

 protected:
  void add_json_info(Json_object *obj) override;
};

/**
  Parse tree node for a list of window definitions corresponding
  to a \<window clause\> in SQL 2003.
*/
class PT_window_list : public Parse_tree_node {
  typedef Parse_tree_node super;

 public:
  List<Window> m_windows;
  explicit PT_window_list(const POS &pos) : super(pos) {}

  bool do_contextualize(Parse_context *pc) override;

  bool push_back(PT_window *w) { return m_windows.push_back(w); }
};

#endif  // SQL_PARSE_TREE_WINDOW_INCLUDED
