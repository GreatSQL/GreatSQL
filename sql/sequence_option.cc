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

#include "sequence_option.h"

#include "my_dbug.h"
#include "my_sys.h"
#include "mysqld_error.h"
#include "sql/derror.h"
#include "sql/sql_class.h"

#define GDB_SEQUENCE_PSTV_MINVALUE_STR "1"
#define GDB_SEQUENCE_PSTV_MAXVALUE_STR "9999999999999999999999999999"
#define GDB_SEQUENCE_NGTV_MINVALUE_STR "-999999999999999999999999999"
#define GDB_SEQUENCE_NGTV_MAXVALUE_STR "-1"
#define GDB_SEQUENCE_INVALID_VALUE_STR "-1000000000000000000000000000"
// global decimal vars, for easy use.
my_decimal gdb_seqval_pstv_min;
my_decimal gdb_seqval_pstv_max;
my_decimal gdb_seqval_ngtv_min;
my_decimal gdb_seqval_ngtv_max;
my_decimal gdb_seqval_invalid;

static inline void str2decimal_nothrow(const char *str, uint length,
                                       my_decimal &decimal) {
  const char *end = str + length;
  str2my_decimal(E_DEC_FATAL_ERROR, str, &decimal, &end);
}

void init_sequence_global_var() {
  str2decimal_nothrow(STRING_WITH_LEN(GDB_SEQUENCE_PSTV_MINVALUE_STR),
                      gdb_seqval_pstv_min);
  str2decimal_nothrow(STRING_WITH_LEN(GDB_SEQUENCE_PSTV_MAXVALUE_STR),
                      gdb_seqval_pstv_max);
  str2decimal_nothrow(STRING_WITH_LEN(GDB_SEQUENCE_NGTV_MINVALUE_STR),
                      gdb_seqval_ngtv_min);
  str2decimal_nothrow(STRING_WITH_LEN(GDB_SEQUENCE_NGTV_MAXVALUE_STR),
                      gdb_seqval_ngtv_max);
  str2decimal_nothrow(STRING_WITH_LEN(GDB_SEQUENCE_INVALID_VALUE_STR),
                      gdb_seqval_invalid);
}

#define PRINT_TRUNCATE_WARNING(decimal, is_max)                           \
  do {                                                                    \
    char val_str_buf[128];                                                \
    String val_str(val_str_buf, sizeof(val_str_buf), &my_charset_bin);    \
    my_decimal2string(E_DEC_FATAL_ERROR, &decimal, &val_str);             \
    val_str.append('\0');                                                 \
    val_str.chop();                                                       \
    const char *item_name = is_max ? "MAXVALUE" : "MINVALUE";             \
    push_warning_printf(                                                  \
        thd, Sql_condition::SL_WARNING, ER_TRUNCATED_WRONG_VALUE,         \
        ER_THD(thd, ER_TRUNCATED_WRONG_VALUE), item_name, val_str.ptr()); \
  } while (0)

/* CEIL ( (MAXVALUE - MINVALUE) / ABS (INCREMENT) ) */
static bool check_cache_le_one_cycle(uint cache_num,
                                     const my_decimal &increment,
                                     const my_decimal &max_value,
                                     const my_decimal &min_value) {
  my_decimal difference, cache_decimal, div_res, ceil_res,
      increment_abs = increment;
  if (increment.sign()) my_decimal_neg(&increment_abs);
  int2my_decimal(E_DEC_FATAL_ERROR, cache_num, true, &cache_decimal);
  my_decimal_sub(E_DEC_FATAL_ERROR, &difference, &max_value, &min_value);
  my_decimal_div(E_DEC_FATAL_ERROR, &div_res, &difference, &increment_abs, 1);
  my_decimal_ceiling(E_DEC_FATAL_ERROR, &div_res, &ceil_res);
  if (my_decimal_cmp(&ceil_res, &cache_decimal) < 0) return false;
  return true;
}

int Sequence_option::validate_create_options(THD *thd) {
  DBUG_TRACE;
  if (my_decimal_is_zero(&increment)) {
    my_error(ER_GDB_CREATE_SEQUENCE, MYF(0), "INCREMENT can not be zero!");
    return -1;
  }

  // 1. set "min/max" value according "increment", if not specify
  if (increment.sign()) {  // descending
    if (cycle_flag && !(option_bitmap & OPTION_TYPE_MINVALUE)) {
      my_error(ER_GDB_CREATE_SEQUENCE, MYF(0),
               "descending sequences that CYCLE must specify MINVALUE!");
      return -2;
    }

    if (!(option_bitmap & OPTION_TYPE_MINVALUE) ||
        (option_bitmap & OPTION_TYPE_NOMINVALUE)) {
      min_value = gdb_seqval_ngtv_min;
    }
    if (!(option_bitmap & OPTION_TYPE_MAXVALUE) ||
        (option_bitmap & OPTION_TYPE_NOMAXVALUE)) {
      max_value = gdb_seqval_ngtv_max;
    }
  } else {  // ascending
    if (cycle_flag && !(option_bitmap & OPTION_TYPE_MAXVALUE)) {
      my_error(ER_GDB_CREATE_SEQUENCE, MYF(0),
               "ascending sequences that CYCLE must specify MAXVALUE!");
      return -2;
    }

    if (!(option_bitmap & OPTION_TYPE_MINVALUE) ||
        (option_bitmap & OPTION_TYPE_NOMINVALUE)) {
      min_value = gdb_seqval_pstv_min;
    }
    if (!(option_bitmap & OPTION_TYPE_MAXVALUE) ||
        (option_bitmap & OPTION_TYPE_NOMAXVALUE)) {
      max_value = gdb_seqval_pstv_max;
    }
  }
  // 1.1 truncate min/max value if exceed valid boundary
  if (my_decimal_cmp(&min_value, &gdb_seqval_ngtv_min) < 0) {
    PRINT_TRUNCATE_WARNING(min_value, false);
    min_value = gdb_seqval_ngtv_min;
  }
  if (my_decimal_cmp(&max_value, &gdb_seqval_pstv_max) > 0) {
    PRINT_TRUNCATE_WARNING(max_value, true);
    max_value = gdb_seqval_pstv_max;
  }
  // 1.2 validate max > min
  if (my_decimal_cmp(&min_value, &max_value) >= 0) {
    my_error(ER_GDB_CREATE_SEQUENCE, MYF(0),
             "MAXVALUE should greater than MINVALUE!");
    return -3;
  }

  // 2. set "start" according "increment", if not specify
  if (!(option_bitmap & OPTION_TYPE_START)) {
    if (increment.sign())
      start_with = max_value;
    else
      start_with = min_value;
  }
  // 2.1 validate "start" between "min" and "max"
  if (my_decimal_cmp(&start_with, &max_value) > 0 ||
      my_decimal_cmp(&start_with, &min_value) < 0) {
    my_error(ER_GDB_CREATE_SEQUENCE, MYF(0),
             "START WITH should between MINVALUE and MAXVALUE!");
    return -4;
  }

  // 3. check "increment" < "max - min"
  my_decimal range, increment_abs = increment;
  my_decimal_sub(E_DEC_FATAL_ERROR, &range, &max_value, &min_value);
  if (increment.sign()) my_decimal_neg(&increment_abs);
  if (my_decimal_cmp(&range, &increment_abs) < 0) {
    my_error(ER_GDB_CREATE_SEQUENCE, MYF(0),
             "INCREMENT should not greater than \"MAXVALUE - MINVALUE\"!");
    return -5;
  }

  // 4. check cache less than one cycle
  if (cycle_flag &&
      !check_cache_le_one_cycle(cache_num, increment, max_value, min_value)) {
    my_error(ER_GDB_CREATE_SEQUENCE, MYF(0),
             "number to CACHE must be less than one cycle!");
    return -6;
  }

  return 0;
}

int Sequence_option::validate_alter_options(THD *thd,
                                            const Sequence_option &old_option) {
  DBUG_TRACE;

  /* copy old option params for alter*/
  if (!(option_bitmap & (OPTION_TYPE_MINVALUE | OPTION_TYPE_NOMINVALUE)))
    min_value = old_option.min_value;
  if (!(option_bitmap & (OPTION_TYPE_MAXVALUE | OPTION_TYPE_NOMAXVALUE)))
    max_value = old_option.max_value;
  if (!(option_bitmap & OPTION_TYPE_INCREMENT))
    increment = old_option.increment;
  if (!(option_bitmap & OPTION_TYPE_CYCLEFLAG))
    cycle_flag = old_option.cycle_flag;
  if (!(option_bitmap & OPTION_TYPE_CACHENUM)) cache_num = old_option.cache_num;
  if (!(option_bitmap & OPTION_TYPE_ORDERFLAG))
    order_flag = old_option.order_flag;

  if (my_decimal_is_zero(&increment)) {
    my_error(ER_GDB_ALTER_SEQUENCE, MYF(0), "INCREMENT can not be zero!");
    return -1;
  }
  if (option_bitmap & OPTION_TYPE_START) {
    my_error(ER_GDB_ALTER_SEQUENCE, MYF(0), "can not modify START WITH");
    return -2;
  }

  // 1. reset "min/max" accrding "increment"
  if (increment.sign()) {
    if (option_bitmap & OPTION_TYPE_NOMINVALUE) {
      min_value = gdb_seqval_ngtv_min;
    }
    if (option_bitmap & OPTION_TYPE_NOMAXVALUE) {
      max_value = gdb_seqval_ngtv_max;
    }
  } else {
    if (option_bitmap & OPTION_TYPE_NOMINVALUE) {
      min_value = gdb_seqval_pstv_min;
    }
    if (option_bitmap & OPTION_TYPE_NOMAXVALUE) {
      max_value = gdb_seqval_pstv_max;
    }
  }
  // 1.1 truncate min/max value if exceed valid boundary
  if (my_decimal_cmp(&min_value, &gdb_seqval_ngtv_min) < 0) {
    PRINT_TRUNCATE_WARNING(min_value, false);
    min_value = gdb_seqval_ngtv_min;
  }
  if (my_decimal_cmp(&max_value, &gdb_seqval_pstv_max) > 0) {
    PRINT_TRUNCATE_WARNING(max_value, false);
    max_value = gdb_seqval_pstv_max;
  }
  // 1.2 validate max > min
  if (my_decimal_cmp(&min_value, &max_value) >= 0) {
    my_error(ER_GDB_ALTER_SEQUENCE, MYF(0),
             "MAXVALUE should greater than MINVALUE!");
    return -3;
  }

  // 2. check "increment" < "max - min"
  my_decimal range, increment_abs = increment;
  my_decimal_sub(E_DEC_FATAL_ERROR, &range, &max_value, &min_value);
  if (increment.sign()) my_decimal_neg(&increment_abs);
  if (my_decimal_cmp(&range, &increment_abs) < 0) {
    my_error(ER_GDB_ALTER_SEQUENCE, MYF(0),
             "INCREMENT should not greater than \"MAXVALUE - MINVALUE\"!");
    return -5;
  }
  start_with = old_option.start_with;  // copy start_with

  // 3. check cache less than one cycle
  if (cycle_flag &&
      !check_cache_le_one_cycle(cache_num, increment, max_value, min_value)) {
    my_error(ER_GDB_ALTER_SEQUENCE, MYF(0),
             "number to CACHE must be less than one cycle!");
    return -6;
  }

  return 0;
}

int Sequence_option::validate_alter_currval(const my_decimal &currval) {
  DBUG_TRACE;

  my_decimal res;
  /* altered sequence not queried yet. use original "start" */
  if (is_seqval_invalid(currval)) {
    res = start_with;
  } else {
    res = currval;
  }

  if (my_decimal_cmp(&res, &max_value) > 0 ||
      my_decimal_cmp(&res, &min_value) < 0) {
    my_error(ER_GDB_ALTER_SEQUENCE, MYF(0),
             "\"currval\" should between MINVALUE and MAXVALUE!");
    return -1;
  }
  start_with = res;
  return 0;
}
