/* Copyright (c) 2023, GreatDB Software Co., Ltd.
   All rights reserved.

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

#ifndef SQL_SEQUENCE_OPTION_INCLUDED
#define SQL_SEQUENCE_OPTION_INCLUDED

#include "include/m_string.h"
#include "m_ctype.h"
#include "sql/my_decimal.h"

#define GDB_SEQUENCE_DEFAULT_CACHENUM 20

extern my_decimal gdb_seqval_pstv_min;
extern my_decimal gdb_seqval_pstv_max;
extern my_decimal gdb_seqval_ngtv_min;
extern my_decimal gdb_seqval_ngtv_max;
extern my_decimal gdb_seqval_invalid;
extern void init_sequence_global_var();
inline bool is_seqval_invalid(const my_decimal &val) {
  return (my_decimal_cmp(&val, &gdb_seqval_invalid) <= 0 ||
          my_decimal_cmp(&val, &gdb_seqval_pstv_max) > 0);
}
class THD;
class Sequence_option {
 public:
  enum enum_option_type {
    OPTION_TYPE_START = 1,
    OPTION_TYPE_MINVALUE = 2,
    OPTION_TYPE_NOMINVALUE = 4,
    OPTION_TYPE_MAXVALUE = 8,
    OPTION_TYPE_NOMAXVALUE = 16,
    OPTION_TYPE_INCREMENT = 32,
    OPTION_TYPE_CYCLEFLAG = 64,
    OPTION_TYPE_CACHENUM = 128,
    OPTION_TYPE_ORDERFLAG = 256,
  };
  Sequence_option() { reset(); }
  void reset() {
    start_with = gdb_seqval_pstv_min;  // 1
    min_value = gdb_seqval_pstv_min;   // 1
    max_value = gdb_seqval_pstv_max;   // max
    increment = gdb_seqval_pstv_min;   // 1
    cycle_flag = false;
    cache_num = GDB_SEQUENCE_DEFAULT_CACHENUM;
    order_flag = false;
    option_bitmap = 0;
  }

  int validate_create_options(THD *thd);
  int validate_alter_options(THD *thd, const Sequence_option &old_option);
  int validate_alter_currval(const my_decimal &currval);

 public:
  my_decimal start_with;
  my_decimal min_value;
  my_decimal max_value;
  my_decimal increment;
  bool cycle_flag{false};
  uint cache_num{GDB_SEQUENCE_DEFAULT_CACHENUM};
  bool order_flag{false};
  uint option_bitmap{0};
};

#endif
