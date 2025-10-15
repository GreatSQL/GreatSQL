/* Copyright (c) 2000, 2024, Oracle and/or its affiliates.
   Copyright (c) 2025, GreatDB Software Co., Ltd.

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
#include "item_gtid_func.h"

#include <algorithm>

#include "include/scope_guard.h"  //Scope_guard
#include "sql/derror.h"           // ER_THD
#include "sql/rpl_mi.h"           // Master_info
#include "sql/rpl_msr.h"          // channel_map
#include "sql/sql_class.h"        // THD
#include "sql/sql_lex.h"

using std::max;

bool Item_wait_for_executed_gtid_set::do_itemize(Parse_context *pc,
                                                 Item **res) {
  if (skip_itemize(res)) return false;
  if (super::do_itemize(pc, res)) return true;
  /*
    It is unsafe because the return value depends on timing. If the timeout
    happens, the return value is different from the one in which the function
    returns with success.
  */
  pc->thd->lex->set_stmt_unsafe(LEX::BINLOG_STMT_UNSAFE_SYSTEM_FUNCTION);
  pc->thd->lex->safe_to_cache_query = false;
  return false;
}

/**
  Wait until the given gtid_set is found in the executed gtid_set independent
  of the slave threads.
*/
longlong Item_wait_for_executed_gtid_set::val_int() {
  DBUG_TRACE;
  assert(fixed);
  THD *thd = current_thd;

  String *gtid_text = args[0]->val_str(&value);
  if (gtid_text == nullptr) {
    /*
      Usually, an argument that is NULL causes an SQL function to return NULL,
      however since this is a function with side-effects, a NULL value is
      treated as an error.
    */
    if (!thd->is_error()) {
      my_error(ER_MALFORMED_GTID_SET_SPECIFICATION, MYF(0), "NULL");
    }
    return error_int();
  }

  double timeout = 0;
  if (arg_count > 1) {
    timeout = args[1]->val_real();
    if (args[1]->null_value || timeout < 0) {
      if (!thd->is_error()) {
        my_error(ER_WRONG_ARGUMENTS, MYF(0), "WAIT_FOR_EXECUTED_GTID_SET.");
      }
      return error_int();
    }
  }

  // Waiting for a GTID in a slave thread could cause the slave to
  // hang/deadlock.
  // @todo: Return error instead of NULL
  if (thd->slave_thread) {
    return error_int();
  }

  Gtid_set wait_for_gtid_set(global_tsid_map, nullptr);

  const Checkable_rwlock::Guard global_tsid_lock_guard(
      *global_tsid_lock, Checkable_rwlock::READ_LOCK);
  if (global_gtid_mode.get() == Gtid_mode::OFF) {
    my_error(ER_GTID_MODE_OFF, MYF(0), "use WAIT_FOR_EXECUTED_GTID_SET");
    return error_int();
  }
  gtid_state->begin_gtid_wait();
  Scope_guard x([&] { gtid_state->end_gtid_wait(); });
  if (wait_for_gtid_set.add_gtid_text(gtid_text->c_ptr_safe()) !=
      RETURN_STATUS_OK) {
    // Error has already been generated.
    return error_int();
  }

  // Cannot wait for a GTID that the thread owns since that would
  // immediately deadlock.
  if (thd->owned_gtid.sidno > 0 &&
      wait_for_gtid_set.contains_gtid(thd->owned_gtid)) {
    char buf[Gtid::MAX_TEXT_LENGTH + 1];
    thd->owned_gtid.to_string(global_tsid_map, buf);
    my_error(ER_CANT_WAIT_FOR_EXECUTED_GTID_SET_WHILE_OWNING_A_GTID, MYF(0),
             buf);
    return error_int();
  }

  bool result = gtid_state->wait_for_gtid_set(thd, &wait_for_gtid_set, timeout);

  null_value = false;
  return result;
}

/**
  Return 1 if both arguments are Gtid_sets and the first is a subset
  of the second.  Generate an error if any of the arguments is not a
  Gtid_set.
*/
longlong Item_func_gtid_subset::val_int() {
  DBUG_TRACE;
  assert(fixed);

  // Evaluate strings without lock
  String *string1 = args[0]->val_str(&buf1);
  if (string1 == nullptr) {
    return error_int();
  }
  String *string2 = args[1]->val_str(&buf2);
  if (string2 == nullptr) {
    return error_int();
  }

  const char *charp1 = string1->c_ptr_safe();
  assert(charp1 != nullptr);
  const char *charp2 = string2->c_ptr_safe();
  assert(charp2 != nullptr);
  int ret = 1;
  enum_return_status status;

  Tsid_map tsid_map(nullptr /*no rwlock*/);
  // compute sets while holding locks
  const Gtid_set sub_set(&tsid_map, charp1, &status);
  if (status == RETURN_STATUS_OK) {
    const Gtid_set super_set(&tsid_map, charp2, &status);
    if (status == RETURN_STATUS_OK) {
      ret = sub_set.is_subset(&super_set) ? 1 : 0;
    }
  }

  null_value = false;
  return ret;
}

bool Item_func_gtid_subtract::resolve_type(THD *thd) {
  if (param_type_is_default(thd, 0, -1)) return true;

  collation.set(default_charset(), DERIVATION_COERCIBLE, MY_REPERTOIRE_ASCII);
  /*
    In the worst case, the string grows after subtraction. This
    happens when a GTID in args[0] is split by a GTID in args[1],
    e.g., UUID:1-6 minus UUID:3-4 becomes UUID:1-2,5-6.  The worst
    case is UUID:1-100 minus UUID:9, where the two characters ":9" in
    args[1] yield the five characters "-8,10" in the result.
  */
  set_data_type_string(
      args[0]->max_length +
      max<ulonglong>(args[1]->max_length - mysql::gtid::Uuid::TEXT_LENGTH, 0) *
          5 / 2);
  return false;
}

String *Item_func_gtid_subtract::val_str_ascii(String *str) {
  DBUG_TRACE;
  assert(fixed);

  String *str1 = args[0]->val_str_ascii(&buf1);
  if (str1 == nullptr) {
    return error_str();
  }
  String *str2 = args[1]->val_str_ascii(&buf2);
  if (str2 == nullptr) {
    return error_str();
  }

  const char *charp1 = str1->c_ptr_safe();
  assert(charp1 != nullptr);
  const char *charp2 = str2->c_ptr_safe();
  assert(charp2 != nullptr);

  enum_return_status status;

  Tsid_map tsid_map(nullptr /*no rwlock*/);
  // compute sets while holding locks
  Gtid_set set1(&tsid_map, charp1, &status);
  if (status == RETURN_STATUS_OK) {
    const Gtid_set set2(&tsid_map, charp2, &status);
    size_t length;
    // subtract, save result, return result
    if (status == RETURN_STATUS_OK) {
      set1.remove_gtid_set(&set2);
      if (!str->mem_realloc((length = set1.get_string_length()) + 1)) {
        set1.to_string(str->ptr());
        str->length(length);
        null_value = false;
        return str;
      }
    }
  }
  return error_str();
}

bool Item_func_gtid_sub::resolve_type(THD *thd) {
  if (param_type_is_default(thd, 0, -1)) return true;

  collation.set(default_charset(), DERIVATION_COERCIBLE, MY_REPERTOIRE_ASCII);
  /*
    In the worst case, the string grows after subtraction. This
    happens when a GTID in args[0] is split by a GTID in args[1],
    e.g., UUID:1-6 minus UUID:3-4 becomes UUID:1-2,5-6.  The worst
    case is UUID:1-100 minus UUID:9, where the two characters ":9" in
    args[1] yield the five characters "-8,10" in the result.
    Need add gtid_count, 2^63 - 1 = 9223372036854775807, and " | " need 22
    length.
  */
  set_data_type_string(
      22 + args[0]->max_length +
      max<ulonglong>(args[1]->max_length - mysql::gtid::Uuid::TEXT_LENGTH, 0) *
          5 / 2);
  return false;
}

/**
  Use GTID_SUB(gtid_set1, gtid_set2) :
  Return NULL if the gtid_set1 is not a subset of gtid_set2.
  Else return GTID_SUM(GTID_SUBTRACT(gtid_set2, gtid_set1)) |
  GTID_SUBTRACT(gtid_set2, gtid_set1).
  Generate an error if any of the arguments is not a Gtid_set.
*/
String *Item_func_gtid_sub::val_str_ascii(String *str) {
  DBUG_TRACE;
  assert(fixed);

  String *str1 = args[0]->val_str_ascii(&buf1);
  if (str1 == nullptr) {
    return error_str();
  }
  String *str2 = args[1]->val_str_ascii(&buf2);
  if (str2 == nullptr) {
    return error_str();
  }

  const char *charp1 = str1->c_ptr_safe();
  assert(charp1 != nullptr);
  const char *charp2 = str2->c_ptr_safe();
  assert(charp2 != nullptr);
  null_value = false;
  enum_return_status status;

  Tsid_map tsid_map(nullptr /*no rwlock*/);
  // compute sets while holding locks
  Gtid_set set1(&tsid_map, charp1, &status);
  if (status == RETURN_STATUS_OK) {
    Gtid_set set2(&tsid_map, charp2, &status);
    size_t length;
    // subtract, save result, return result
    if (status == RETURN_STATUS_OK) {
      if (!set1.is_subset(&set2)) {
        null_value = true;
        return error_str();
      }
      set2.remove_gtid_set(&set1);
      String gtid_count;
      gtid_count.set(set2.get_all_gtid_count(), default_charset());
      gtid_count.append(STRING_WITH_LEN(" ~ "));
      // result: sum_gtid_count | gtid_set, gtid_set, ...'\0'
      if (!str->mem_realloc(
              (length = gtid_count.length() + set2.get_string_length()) + 1)) {
        str->replace(0, 0, gtid_count);
        set2.to_string(str->ptr() + gtid_count.length());
        str->length(length);
        return str;
      }
    }
  }
  return error_str();
}

/**
  Return the count of all the GTIDs. Generate an error if the argument is not a
  Gtid_set.
*/
longlong Item_func_gtid_count::val_int() {
  DBUG_TRACE;
  assert(fixed);

  // Evaluate strings without lock
  String *string1 = args[0]->val_str(&buf1);
  if (string1 == nullptr) {
    return error_int();
  }

  const char *charp1 = string1->c_ptr_safe();
  assert(charp1 != nullptr);
  enum_return_status status;
  null_value = false;

  Tsid_map tsid_map(nullptr /*no rwlock*/);
  // compute sets while holding locks
  const Gtid_set sub_set(&tsid_map, charp1, &status);
  if (status == RETURN_STATUS_OK) return sub_set.get_all_gtid_count();
  return error_int();
}

/*
  Find the binlog file where given gtid
  if second param true, will extra print position and trx length
*/
String *Item_func_gtid_pos_in_binlog::val_str_ascii(String *str) {
  DBUG_TRACE;
  assert(fixed);
  char binlog_file[FN_REFLEN];

  String *str1 = args[0]->val_str_ascii(&file_pos);
  find_more_info = args[1]->val_bool();
  if (str1 == nullptr) {
    return error_str();
  }
  const char *charp1 = str1->c_ptr_safe();
  assert(charp1 != nullptr);

  null_value = false;
  enum_return_status status;
  Tsid_map tsid_map(nullptr /*no rwlock*/);
  Gtid_set gtid_set(&tsid_map, charp1, &status);

  if ((status == RETURN_STATUS_OK) && (gtid_set.get_all_gtid_count() == 1)) {
    Gtid_set g_executed_gtids(&tsid_map);
    global_tsid_lock->wrlock();
    g_executed_gtids.add_gtid_set(gtid_state->get_executed_gtids());
    global_tsid_lock->unlock();
    if (!gtid_set.is_subset(&g_executed_gtids)) {
      null_value = true;
      return error_str();
    }

    if (!mysql_bin_log.find_log_name_by_gtid_set(binlog_file, &gtid_set)) {
      str->copy(binlog_file, strlen(binlog_file), &my_charset_bin);
      if (find_more_info) {
        my_off_t pos;
        ulonglong trx_length;
        if (!mysql_bin_log.find_pos_from_binary_log(pos, trx_length,
                                                    binlog_file, &gtid_set)) {
          str->append(POSITION_NAME, strlen(POSITION_NAME));
          str->append_ulonglong(pos);
          str->append(TRANSACTION_LENGTH, strlen(TRANSACTION_LENGTH));
          str->append_ulonglong(trx_length);
        } else {
          null_value = true;
          return error_str();
        }
      }
      null_value = false;
      return str;
    }
  }

  return error_str();
}

bool Item_func_gtid_pos_in_binlog::resolve_type(THD *thd) {
  if (param_type_is_default(thd, 0, -1)) return true;
  collation.set(default_charset(), DERIVATION_COERCIBLE, MY_REPERTOIRE_ASCII);

  /*
    binlog file path max FN_REFLEN, and position ulonglong
    and transaction length ulongulong, and strlen(POSITION_NAME)
    and strlen(TRANSACTION_LENGTH）
  */
  set_data_type_string(uint32(FN_REFLEN + 20 + 20 + strlen(POSITION_NAME) +
                              strlen(TRANSACTION_LENGTH)));
  return false;
}
