#ifndef ITEM_FUNC_INCLUDED
#define ITEM_FUNC_INCLUDED

/* Copyright (c) 2000, 2022, Oracle and/or its affiliates. All rights reserved.
   Copyright (c) 2023, 2025, GreatDB Software Co., Ltd.

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

#include <sys/types.h>

#include <climits>
#include <cmath>  // isfinite
#include <cstddef>
#include <functional>

#include "decimal.h"
#include "field_types.h"
#include "ft_global.h"
#include "lex_string.h"
#include "m_ctype.h"
#include "my_alloc.h"
#include "my_base.h"
#include "my_compiler.h"
#include "my_dbug.h"
#include "my_inttypes.h"
#include "my_pointer_arithmetic.h"
#include "my_table_map.h"
#include "my_thread_local.h"
#include "my_time.h"
#include "mysql/service_mysql_alloc.h"
#include "mysql/udf_registration_types.h"
#include "mysql_com.h"
#include "mysql_time.h"
#include "mysqld_error.h"
#include "sql/enum_query_type.h"
#include "sql/field.h"
#include "sql/handler.h"
#include "sql/item.h"            // Item_result_field
#include "sql/my_decimal.h"      // str2my_decimal
#include "sql/parse_location.h"  // POS
#include "sql/set_var.h"         // enum_var_type
#include "sql/sql_const.h"
#include "sql/sql_locale.h"
#include "sql/sql_tmp_table.h"  // close_tmp_table
#include "sql/sql_udf.h"        // udf_handler
#include "sql/table.h"
#include "sql/thr_malloc.h"
#include "sql_string.h"
#include "template_utils.h"

class MY_LOCALE;
class Json_wrapper;
class PT_item_list;
class Protocol;
class Query_block;
class THD;
class sp_rcontext;
struct MY_BITMAP;
struct Parse_context;
class Gdb_sequence_entity;
class sp_variable;

template <class T>
class List;
enum class enum_udt_table_of_type;

/* Function items used by mysql */

extern bool reject_geometry_args(uint arg_count, Item **args,
                                 Item_result_field *me);
extern THD *thd_get_current_thd();
void unsupported_json_comparison(size_t arg_count, Item **args,
                                 const char *msg);
class Cursor_ref {
 protected:
  LEX_CSTRING m_cursor_name;
  uint m_cursor_offset;
  sp_variable *m_cursor_spv;
  class sp_cursor *get_open_cursor_or_error();
  Cursor_ref(LEX_CSTRING name, uint offset, sp_variable *cursor_spv)
      : m_cursor_name(name),
        m_cursor_offset(offset),
        m_cursor_spv(cursor_spv) {}
  void print_func(String *str, const char *func_name) const;
};

void report_conversion_error(const CHARSET_INFO *to_cs, const char *from,
                             size_t from_length, const CHARSET_INFO *from_cs);

bool simplify_string_args(THD *thd, const DTCollation &c, Item **items,
                          uint nitems);

String *eval_string_arg(const CHARSET_INFO *to_cs, Item *arg, String *buffer);
class Item_func_get_system_var;
class Item_func_get_user_var;

class Item_func : public Item_result_field {
 protected:
  /**
     Array of pointers to arguments. If there are max 2 arguments, this array
     is often just m_embedded_arguments; otherwise it's explicitly allocated in
     the constructor.
  */
  Item **args;

 private:
  Item *m_embedded_arguments[2];

  /// Allocates space for the given number of arguments, if needed. Uses
  /// #m_embedded_arguments if it's big enough.
  bool alloc_args(MEM_ROOT *mem_root, unsigned num_args) {
    if (num_args <= array_elements(m_embedded_arguments)) {
      args = m_embedded_arguments;
    } else {
      args = mem_root->ArrayAlloc<Item *>(num_args);
      if (args == nullptr) {
        // OOM
        arg_count = 0;
        return true;
      }
    }
    arg_count = num_args;
    return false;
  }

 public:
  uint arg_count;  ///< How many arguments in 'args'
  /// Changes argument and maintains any necessary invariants.
  virtual void set_arg_resolve(THD *, uint i, Item *arg) { args[i] = arg; }
  virtual uint argument_count() const { return arg_count; }
  inline Item **arguments() const {
    return (argument_count() > 0) ? args : nullptr;
  }

 protected:
  /*
    These decide of types of arguments which are prepared-statement
    parameters.
  */
  bool param_type_uses_non_param(THD *thd,
                                 enum_field_types def = MYSQL_TYPE_VARCHAR);
  bool param_type_is_default(THD *thd, uint start, uint end, uint step,
                             enum_field_types def);
  bool param_type_is_default(THD *thd, uint start, uint end,
                             enum_field_types def = MYSQL_TYPE_VARCHAR) {
    return param_type_is_default(thd, start, end, 1, def);
  }
  bool param_type_is_rejected(uint start, uint end);

  /**
    Affects how to determine that NULL argument implies a NULL function return.
    Default behaviour in this class is:
    - if true, any NULL argument means the function returns NULL.
    - if false, no such assumption is made and not_null_tables_cache is thus
      set to 0.
    null_on_null is true for all Item_func derived classes, except Item_func_sp,
    all CASE derived functions and a few other functions.
    RETURNS NULL ON NULL INPUT can be implemented for stored functions by
    modifying this member in class Item_func_sp.
  */
  bool null_on_null{true};
  /*
    Allowed numbers of columns in result (usually 1, which means scalar value)
    0 means get this number from first argument
  */
  uint allowed_arg_cols{1};
  /// Value used in calculation of result of used_tables()
  table_map used_tables_cache{0};
  /// Value used in calculation of result of not_null_tables()
  table_map not_null_tables_cache{0};

 public:
  /*
    When updating Functype with new spatial functions,
    is_spatial_operator() should also be updated.

    DD_INTERNAL_FUNC:
      Some of the internal functions introduced for the INFORMATION_SCHEMA views
      opens data-dictionary tables. DD_INTERNAL_FUNC is used for the such type
      of functions.
  */
  enum Functype {
    UNKNOWN_FUNC,
    EQ_FUNC,
    EQUAL_FUNC,
    NE_FUNC,
    LT_FUNC,
    LE_FUNC,
    GE_FUNC,
    GT_FUNC,
    FT_FUNC,
    MATCH_FUNC,
    LIKE_FUNC,
    ISNULL_FUNC,
    ISNOTNULL_FUNC,
    ISTRUTH_FUNC,
    COND_AND_FUNC,
    COND_OR_FUNC,
    XOR_FUNC,
    BETWEEN,
    IN_FUNC,
    MULT_EQUAL_FUNC,
    INTERVAL_FUNC,
    ISNOTNULLTEST_FUNC,
    SP_EQUALS_FUNC,
    SP_DISJOINT_FUNC,
    SP_INTERSECTS_FUNC,
    SP_TOUCHES_FUNC,
    SP_CROSSES_FUNC,
    SP_WITHIN_FUNC,
    SP_CONTAINS_FUNC,
    SP_COVEREDBY_FUNC,
    SP_COVERS_FUNC,
    SP_OVERLAPS_FUNC,
    SP_STARTPOINT,
    SP_ENDPOINT,
    SP_EXTERIORRING,
    SP_POINTN,
    SP_GEOMETRYN,
    SP_INTERIORRINGN,
    NOT_FUNC,
    NOT_ALL_FUNC,
    NOW_FUNC,
    FROM_DAYS_FUNC,
    TRIG_COND_FUNC,
    SUSERVAR_FUNC,
    GUSERVAR_FUNC,
    COLLATE_FUNC,
    EXTRACT_FUNC,
    TYPECAST_FUNC,
    FUNC_SP,
    UDF_FUNC,
    NEG_FUNC,
    GSYSVAR_FUNC,
    GROUPING_FUNC,
    ROLLUP_GROUP_ITEM_FUNC,
    TABLE_FUNC,
    DD_INTERNAL_FUNC,
    PLUS_FUNC,
    MINUS_FUNC,
    MUL_FUNC,
    DIV_FUNC,
    CEILING_FUNC,
    ROUND_FUNC,
    TRUNCATE_FUNC,
    SQRT_FUNC,
    ABS_FUNC,
    POW_FUNC,
    SIGN_FUNC,
    FLOOR_FUNC,
    LOG_FUNC,
    LN_FUNC,
    LOG10_FUNC,
    SIN_FUNC,
    TAN_FUNC,
    COS_FUNC,
    COT_FUNC,
    DEGREES_FUNC,
    RADIANS_FUNC,
    EXP_FUNC,
    ASIN_FUNC,
    ATAN_FUNC,
    ACOS_FUNC,
    MOD_FUNC,
    IF_FUNC,
    NULLIF_FUNC,
    CASE_FUNC,
    YEAR_FUNC,
    YEARWEEK_FUNC,
    MAKEDATE_FUNC,
    MONTH_FUNC,
    MONTHNAME_FUNC,
    DAY_FUNC,
    DAYNAME_FUNC,
    TO_DAYS_FUNC,
    TO_SECONDS_FUNC,
    DATE_FUNC,
    HOUR_FUNC,
    MINUTE_FUNC,
    SECOND_FUNC,
    MICROSECOND_FUNC,
    DAYOFYEAR_FUNC,
    ADDTIME_FUNC,
    QUARTER_FUNC,
    WEEK_FUNC,
    WEEKDAY_FUNC,
    DATEADD_FUNC,
    FROM_UNIXTIME_FUNC,
    CONVERT_TZ_FUNC,
    LAST_DAY_FUNC,
    LNNVL_FUNC,
    UNIX_TIMESTAMP_FUNC,
    TIME_TO_SEC_FUNC,
    TIMESTAMPDIFF_FUNC,
    DATETIME_LITERAL,
    GREATEST_FUNC,
    COALESCE_FUNC,
    LEAST_FUNC,
    JSON_CONTAINS,
    JSON_OVERLAPS,
    JSON_UNQUOTE_FUNC,
    MEMBER_OF_FUNC,
    STRCMP_FUNC,
    TRUE_FUNC,
    FALSE_FUNC,
    UDT_FUNC,
    UDT_TABLE_FUNC,
    ROWNUM_FUNC,
    SEQUENCE_FUNC,
    CONNECT_BY_FUNC,
    JSON_FUNC,
    XML_FUNC,
    DATE_LITERAL_FUNC,
    TIME_LITERAL_FUNC,
    CURDATE_FUNC,
    SUBSTR_FUNC,
    CONCAT_FUNC,
    CONCAT_WS_FUNC,
    LOWER_FUNC,
    UPPER_FUNC,
    LOG2_FUNC,
    INSTR_FUNC,
    LOCATE_FUNC,
    LEFT_FUNC,
    RIGHT_FUNC,
    REPEAT_FUNC,
    LPAD_FUNC,
    RPAD_FUNC,
    LTRIM_FUNC,
    RTRIM_FUNC,
    TRIM_FUNC,
    REPLACE_FUNC,
    REVERSE_FUNC,
    ASCII_FUNC,
    BIT_LENGTH_FUNC,
    LENGTH_FUNC,
    CHAR_LENGTH_FUNC,
    TRANSLATE_FUNC,
    RAND_FUNC,
    DATE_FORMAT_FUNC,
  };
  enum optimize_type {
    OPTIMIZE_NONE,
    OPTIMIZE_KEY,
    OPTIMIZE_OP,
    OPTIMIZE_NULL,
    OPTIMIZE_EQUAL
  };
  enum Type type() const override { return FUNC_ITEM; }
  virtual enum Functype functype() const { return UNKNOWN_FUNC; }
  Item_func() : args(m_embedded_arguments), arg_count(0) {}

  explicit Item_func(const POS &pos)
      : Item_result_field(pos), args(m_embedded_arguments), arg_count(0) {}

  Item_func(Item *a) : args(m_embedded_arguments), arg_count(1) {
    args[0] = a;
    set_accum_properties(a);
  }
  Item_func(const POS &pos, Item *a)
      : Item_result_field(pos), args(m_embedded_arguments), arg_count(1) {
    args[0] = a;
  }

  Item_func(Item *a, Item *b) : args(m_embedded_arguments), arg_count(2) {
    args[0] = a;
    args[1] = b;
    m_accum_properties = 0;
    add_accum_properties(a);
    add_accum_properties(b);
  }
  Item_func(const POS &pos, Item *a, Item *b)
      : Item_result_field(pos), args(m_embedded_arguments), arg_count(2) {
    args[0] = a;
    args[1] = b;
  }

  Item_func(Item *a, Item *b, Item *c) {
    if (alloc_args(*THR_MALLOC, 3)) return;
    args[0] = a;
    args[1] = b;
    args[2] = c;
    m_accum_properties = 0;
    add_accum_properties(a);
    add_accum_properties(b);
    add_accum_properties(c);
  }

  Item_func(const POS &pos, Item *a, Item *b, Item *c)
      : Item_result_field(pos) {
    if (alloc_args(*THR_MALLOC, 3)) return;
    args[0] = a;
    args[1] = b;
    args[2] = c;
  }

  Item_func(Item *a, Item *b, Item *c, Item *d) {
    if (alloc_args(*THR_MALLOC, 4)) return;
    args[0] = a;
    args[1] = b;
    args[2] = c;
    args[3] = d;
    m_accum_properties = 0;
    add_accum_properties(a);
    add_accum_properties(b);
    add_accum_properties(c);
    add_accum_properties(d);
  }

  Item_func(const POS &pos, Item *a, Item *b, Item *c, Item *d)
      : Item_result_field(pos) {
    if (alloc_args(*THR_MALLOC, 4)) return;
    args[0] = a;
    args[1] = b;
    args[2] = c;
    args[3] = d;
  }
  Item_func(Item *a, Item *b, Item *c, Item *d, Item *e) {
    if (alloc_args(*THR_MALLOC, 5)) return;
    args[0] = a;
    args[1] = b;
    args[2] = c;
    args[3] = d;
    args[4] = e;
    m_accum_properties = 0;
    add_accum_properties(a);
    add_accum_properties(b);
    add_accum_properties(c);
    add_accum_properties(d);
    add_accum_properties(e);
  }
  Item_func(const POS &pos, Item *a, Item *b, Item *c, Item *d, Item *e)
      : Item_result_field(pos) {
    if (alloc_args(*THR_MALLOC, 5)) return;
    args[0] = a;
    args[1] = b;
    args[2] = c;
    args[3] = d;
    args[4] = e;
  }
  Item_func(Item *a, Item *b, Item *c, Item *d, Item *e, Item *f) {
    if (alloc_args(*THR_MALLOC, 6)) return;
    args[0] = a;
    args[1] = b;
    args[2] = c;
    args[3] = d;
    args[4] = e;
    args[5] = f;
    m_accum_properties = 0;
    add_accum_properties(a);
    add_accum_properties(b);
    add_accum_properties(c);
    add_accum_properties(d);
    add_accum_properties(e);
    add_accum_properties(f);
  }
  Item_func(const POS &pos, Item *a, Item *b, Item *c, Item *d, Item *e,
            Item *f)
      : Item_result_field(pos) {
    if (alloc_args(*THR_MALLOC, 6)) return;
    args[0] = a;
    args[1] = b;
    args[2] = c;
    args[3] = d;
    args[4] = e;
    args[5] = f;
  }
  explicit Item_func(mem_root_deque<Item *> *list) {
    set_arguments(list, false);
  }

  Item_func(const POS &pos, PT_item_list *opt_list);

  // Constructor used for Item_cond_and/or (see Item comment)
  Item_func(THD *thd, const Item_func *item);

  /// Get the i'th argument of the function that this object represents.
  virtual Item *get_arg(uint i) { return args[i]; }

  /// Get the i'th argument of the function that this object represents.
  virtual const Item *get_arg(uint i) const { return args[i]; }
  virtual Item *set_arg(THD *, uint, Item *) {
    assert(0);
    return nullptr;
  }

  bool itemize(Parse_context *pc, Item **res) override;

  bool fix_fields(THD *, Item **ref) override;
  virtual bool refix_fields() { return false; }
  bool fix_func_arg(THD *, Item **arg);
  void fix_after_pullout(Query_block *parent_query_block,
                         Query_block *removed_query_block) override;
  /**
    Resolve type of function after all arguments have had their data types
    resolved. Called from resolve_type() when no dynamic parameters
    are used and from propagate_type() otherwise.
  */
  virtual bool resolve_type_inner(THD *) {
    assert(false);
    return false;
  }
  bool propagate_type(THD *thd, const Type_properties &type) override;
  /**
     Returns the pseudo tables depended upon in order to evaluate this
     function expression. The default implementation returns the empty
     set.
  */
  virtual table_map get_initial_pseudo_tables() const { return 0; }
  table_map used_tables() const override { return used_tables_cache; }
  table_map not_null_tables() const override { return not_null_tables_cache; }
  void update_used_tables() override;
  void set_used_tables(table_map map) { used_tables_cache = map; }
  bool eq(const Item *item, bool binary_cmp) const override;
  virtual optimize_type select_optimize(const THD *) { return OPTIMIZE_NONE; }
  virtual bool have_rev_func() const { return false; }
  virtual Item *key_item() const { return args[0]; }
  /**
    Copy arguments from list to args array

    @param list           function argument list
    @param context_free   true: for use in context-independent
                          constructors (Item_func(POS,...)) i.e. for use
                          in the parser
    @return true on OOM, false otherwise
  */
  bool set_arguments(mem_root_deque<Item *> *list, bool context_free);
  void split_sum_func(THD *thd, Ref_item_array ref_item_array,
                      mem_root_deque<Item *> *fields) override;
  void print(const THD *thd, String *str,
             enum_query_type query_type) const override;
  void print_op(const THD *thd, String *str, enum_query_type query_type) const;
  virtual void print_op_extra(const THD *, String *, enum_query_type) const {}
  void print_args(const THD *thd, String *str, uint from,
                  enum_query_type query_type) const;
  virtual void fix_num_length_and_dec();
  virtual bool is_deprecated() const { return false; }
  bool get_arg0_date(MYSQL_TIME *ltime, my_time_flags_t fuzzy_date) {
    return (null_value = args[0]->get_date(ltime, fuzzy_date));
  }
  inline bool get_arg0_time(MYSQL_TIME *ltime) {
    return (null_value = args[0]->get_time(ltime));
  }
  bool is_null() override { return update_null_value() || null_value; }
  void signal_divide_by_null();
  void signal_invalid_argument_for_log();
  friend class udf_handler;
  Field *tmp_table_field(TABLE *t_arg) override;
  Item *get_tmp_table_item(THD *thd) override;

  my_decimal *val_decimal(my_decimal *) override;

  bool agg_arg_charsets(DTCollation &c, Item **items, uint nitems, uint flags,
                        int item_sep) {
    return agg_item_charsets(c, func_name(), items, nitems, flags, item_sep,
                             false);
  }
  /*
    Aggregate arguments for string result, e.g: CONCAT(a,b)
    - convert to @@character_set_connection if all arguments are numbers
    - allow DERIVATION_NONE
  */
  bool agg_arg_charsets_for_string_result(DTCollation &c, Item **items,
                                          uint nitems, int item_sep = 1) {
    return agg_item_charsets_for_string_result(c, func_name(), items, nitems,
                                               item_sep);
  }
  /*
    Aggregate arguments for comparison, e.g: a=b, a LIKE b, a RLIKE b
    - don't convert to @@character_set_connection if all arguments are numbers
    - don't allow DERIVATION_NONE
  */
  bool agg_arg_charsets_for_comparison(DTCollation &c, Item **items,
                                       uint nitems, int item_sep = 1) {
    return agg_item_charsets_for_comparison(c, func_name(), items, nitems,
                                            item_sep);
  }

  bool walk(Item_processor processor, enum_walk walk, uchar *arg) override;
  Item *transform(Item_transformer transformer, uchar *arg) override;
  Item *compile(Item_analyzer analyzer, uchar **arg_p,
                Item_transformer transformer, uchar *arg_t) override;
  void traverse_cond(Cond_traverser traverser, void *arg,
                     traverse_order order) override;
  bool collect_item_func_processor(uchar *) override;

  /// argument used by walk method collect_session_vars(for parallel load)
  struct Collect_session_vars {
    std::vector<Item_func_get_system_var *> sys_vars;
    std::vector<Item_func_get_user_var *> user_vars;
  };

  /**
     Throw an error if the input double number is not finite, i.e. is either
     +/-INF or NAN.
  */
  inline double check_float_overflow(double value) {
    return std::isfinite(value) ? value : raise_float_overflow();
  }
  /**
    Throw an error if the input BIGINT value represented by the
    (longlong value, bool unsigned flag) pair cannot be returned by the
    function, i.e. is not compatible with this Item's unsigned_flag.
  */
  inline longlong check_integer_overflow(longlong value, bool val_unsigned) {
    if ((unsigned_flag && !val_unsigned && value < 0) ||
        (!unsigned_flag && val_unsigned &&
         (ulonglong)value > (ulonglong)LLONG_MAX))
      return raise_integer_overflow();
    return value;
  }
  /**
     Throw an error if the error code of a DECIMAL operation is E_DEC_OVERFLOW.
  */
  inline int check_decimal_overflow(int error) {
    return (error == E_DEC_OVERFLOW) ? raise_decimal_overflow() : error;
  }

  bool has_timestamp_args() {
    assert(fixed);
    for (uint i = 0; i < arg_count; i++) {
      if (args[i]->type() == Item::FIELD_ITEM &&
          args[i]->data_type() == MYSQL_TYPE_TIMESTAMP)
        return true;
    }
    return false;
  }

  bool has_date_args() {
    assert(fixed);
    for (uint i = 0; i < arg_count; i++) {
      if (args[i]->type() == Item::FIELD_ITEM &&
          (args[i]->data_type() == MYSQL_TYPE_DATE ||
           args[i]->data_type() == MYSQL_TYPE_DATETIME))
        return true;
    }
    return false;
  }

  bool has_time_args() {
    assert(fixed);
    for (uint i = 0; i < arg_count; i++) {
      if (args[i]->type() == Item::FIELD_ITEM &&
          (args[i]->data_type() == MYSQL_TYPE_TIME ||
           args[i]->data_type() == MYSQL_TYPE_DATETIME))
        return true;
    }
    return false;
  }

  bool has_datetime_args() {
    assert(fixed);
    for (uint i = 0; i < arg_count; i++) {
      if (args[i]->type() == Item::FIELD_ITEM &&
          args[i]->data_type() == MYSQL_TYPE_DATETIME)
        return true;
    }
    return false;
  }

  /*
    We assume the result of any function that has a TIMESTAMP argument to be
    timezone-dependent, since a TIMESTAMP value in both numeric and string
    contexts is interpreted according to the current timezone.
    The only exception is UNIX_TIMESTAMP() which returns the internal
    representation of a TIMESTAMP argument verbatim, and thus does not depend on
    the timezone.
   */
  bool check_valid_arguments_processor(uchar *) override {
    return has_timestamp_args();
  }

  Item *gc_subst_transformer(uchar *arg) override;

  bool resolve_type(THD *thd) override {
    // By default, pick PS-param's type from other arguments, or VARCHAR
    return param_type_uses_non_param(thd);
  }

  /**
    Whether an arg of a JSON function can be cached to avoid repetitive
    string->JSON conversion. This function returns true only for those args,
    which are the source of JSON data. JSON path args are cached independently
    and for them this function returns false. Same as for all other type of
    args.

    @param arg  the arg to cache

    @retval true   arg can be cached
    @retval false  otherwise
  */
  virtual enum_const_item_cache can_cache_json_arg(Item *arg [[maybe_unused]]) {
    return CACHE_NONE;
  }

  /// Whether this Item is an equi-join condition. If this Item is a compound
  /// item (i.e. multiple condition AND'ed together), it will only return true
  /// if the Item contains only equi-join conditions AND'ed together. This is
  /// used to determine whether the condition can be used as a join condition
  /// for hash join (join conditions in hash join must be equi-join conditions),
  /// or if it should be placed as a filter after the join.
  virtual bool contains_only_equi_join_condition() const { return false; }

  bool ensure_multi_equality_fields_are_available_walker(uchar *);

  /// used for Item_func_udt_table and Item_func_udt_single_type_table
  bool check_record_table_index(int index_length);

 protected:
  /**
    Whether or not an item should contribute to the filtering effect
    (@see get_filtering_effect()). First it verifies that table
    requirements are satisfied as follows:

     1) The item must refer to a field in 'filter_for_table' in some
        way. This reference may be indirect through any number of
        intermediate items. For example, this item may be an
        Item_cond_and which refers to an Item_func_eq which refers to
        the field.
     2) The item must not refer to other tables than those already
        read and the table in 'filter_for_table'

    Then it contines to other properties as follows:

    Item_funcs represent "<operand1> OP <operand2> [OP ...]". If the
    Item_func is to contribute to the filtering effect, then

    1) one of the operands must be a field from 'filter_for_table' that is not
       in 'fields_to_ignore', and
    2) depending on the Item_func type filtering effect is calculated
       for, one or all [1] of the other operand(s) must be an available
       value, i.e.:
       - a constant, or
       - a constant subquery, or
       - a field value read from a table in 'read_tables', or
       - a second field in 'filter_for_table', or
       - a function that only refers to constants or tables in
         'read_tables', or
       - special case: an implicit value like NULL in the case of
         "field IS NULL". Such Item_funcs have arg_count==1.

    [1] "At least one" for multiple equality (X = Y = Z = ...), "all"
    for the rest (e.g. BETWEEN)

    @param read_tables       Tables earlier in the join sequence.
                             Predicates for table 'filter_for_table' that
                             rely on values from these tables can be part of
                             the filter effect.
    @param filter_for_table  The table we are calculating filter effect for
    @param fields_to_ignore Columns that should be ignored.


    @return Item_field that participates in the predicate if none of the
            requirements are broken, NULL otherwise

    @note: This function only applies to items doing comparison, i.e.
    boolean predicates. Unfortunately, some of those items do not
    inherit from Item_bool_func so the member function has to be
    placed in Item_func.
  */
  const Item_field *contributes_to_filter(
      table_map read_tables, table_map filter_for_table,
      const MY_BITMAP *fields_to_ignore) const;
  /**
    Named parameters are allowed in a parameter list

    The syntax to name parameters in a function call is as follow:
    <code>foo(expr AS named, expr named, expr AS "named", expr "named")</code>
    where "AS" is optional.
    Only UDF function support that syntax.

    @return true if the function item can have named parameters
  */
  virtual bool may_have_named_parameters() const { return false; }
  bool is_non_const_over_literals(uchar *) override { return false; }

  bool check_function_as_value_generator(uchar *checker_args) override {
    if (is_deprecated()) {
      Check_function_as_value_generator_parameters *func_arg =
          pointer_cast<Check_function_as_value_generator_parameters *>(
              checker_args);
      func_arg->banned_function_name = func_name();
      return true;
    }
    return false;
  }
  bool is_valid_for_pushdown(uchar *arg) override;
  bool check_column_in_window_functions(uchar *arg) override;
  bool check_column_in_group_by(uchar *arg) override;

  longlong val_int_from_real();
  bool pq_copy_from(THD *thd, Query_block *select, Item *item) override;
};

class Item_real_func : public Item_func {
 public:
  Item_real_func() : Item_func() { set_data_type_double(); }
  explicit Item_real_func(const POS &pos) : Item_func(pos) {
    set_data_type_double();
  }

  Item_real_func(Item *a) : Item_func(a) { set_data_type_double(); }
  Item_real_func(const POS &pos, Item *a) : Item_func(pos, a) {
    set_data_type_double();
  }

  Item_real_func(Item *a, Item *b) : Item_func(a, b) { set_data_type_double(); }

  Item_real_func(const POS &pos, Item *a, Item *b) : Item_func(pos, a, b) {
    set_data_type_double();
  }

  explicit Item_real_func(mem_root_deque<Item *> *list) : Item_func(list) {
    set_data_type_double();
  }

  Item_real_func(const POS &pos, PT_item_list *list) : Item_func(pos, list) {
    set_data_type_double();
  }

  String *val_str(String *str) override;
  my_decimal *val_decimal(my_decimal *decimal_value) override;
  longlong val_int() override {
    assert(fixed);
    return llrint_with_overflow_check(val_real());
  }
  bool get_date(MYSQL_TIME *ltime, my_time_flags_t fuzzydate) override {
    return get_date_from_real(ltime, fuzzydate);
  }
  bool get_time(MYSQL_TIME *ltime) override {
    return get_time_from_real(ltime);
  }
  enum Item_result result_type() const override { return REAL_RESULT; }
};

class Item_func_numhybrid : public Item_func {
 protected:
  Item_result hybrid_type;

 public:
  Item_func_numhybrid(Item *a) : Item_func(a), hybrid_type(REAL_RESULT) {
    collation.set_numeric();
  }
  Item_func_numhybrid(const POS &pos, Item *a)
      : Item_func(pos, a), hybrid_type(REAL_RESULT) {
    collation.set_numeric();
  }

  Item_func_numhybrid(Item *a, Item *b)
      : Item_func(a, b), hybrid_type(REAL_RESULT) {
    collation.set_numeric();
  }
  Item_func_numhybrid(const POS &pos, Item *a, Item *b)
      : Item_func(pos, a, b), hybrid_type(REAL_RESULT) {
    collation.set_numeric();
  }

  explicit Item_func_numhybrid(mem_root_deque<Item *> *list)
      : Item_func(list), hybrid_type(REAL_RESULT) {
    collation.set_numeric();
  }
  Item_func_numhybrid(const POS &pos, PT_item_list *list)
      : Item_func(pos, list), hybrid_type(REAL_RESULT) {
    collation.set_numeric();
  }

  enum Item_result result_type() const override { return hybrid_type; }
  enum_field_types default_data_type() const override {
    return MYSQL_TYPE_DOUBLE;
  }
  bool resolve_type(THD *thd) override;
  bool resolve_type_inner(THD *thd) override;
  void fix_num_length_and_dec() override;
  virtual void set_numeric_type() = 0;  // To be called from resolve_type()

  double val_real() override;
  longlong val_int() override;
  my_decimal *val_decimal(my_decimal *) override;
  String *val_str(String *str) override;
  bool get_date(MYSQL_TIME *ltime, my_time_flags_t fuzzydate) override;
  bool get_time(MYSQL_TIME *ltime) override;
  /**
     @brief Performs the operation that this functions implements when the
     result type is INT.

     @return The result of the operation.
  */
  virtual longlong int_op() = 0;

  /**
     @brief Performs the operation that this functions implements when the
     result type is REAL.

     @return The result of the operation.
  */
  virtual double real_op() = 0;

  /**
     @brief Performs the operation that this functions implements when the
     result type is DECIMAL.

     @param decimal_value A pointer where the DECIMAL value will be allocated.
     @return
       - 0 If the result is NULL
       - The same pointer it was given, with the area initialized to the
         result of the operation.
  */
  virtual my_decimal *decimal_op(my_decimal *decimal_value) = 0;

  /**
     @brief Performs the operation that this functions implements when the
     result type is a string type.

     @return The result of the operation.
  */
  virtual String *str_op(String *) = 0;
  /**
     @brief Performs the operation that this functions implements when the
     result type is MYSQL_TYPE_DATE or MYSQL_TYPE_DATETIME.

     @return The result of the operation.
  */
  virtual bool date_op(MYSQL_TIME *ltime, my_time_flags_t fuzzydate) = 0;
  virtual bool time_op(MYSQL_TIME *ltime) = 0;
  bool is_null() override { return update_null_value() || null_value; }
};

/* function where type of result detected by first argument */
class Item_func_num1 : public Item_func_numhybrid {
 public:
  Item_func_num1(Item *a) : Item_func_numhybrid(a) {}
  Item_func_num1(const POS &pos, Item *a) : Item_func_numhybrid(pos, a) {}

  Item_func_num1(Item *a, Item *b) : Item_func_numhybrid(a, b) {}
  Item_func_num1(const POS &pos, Item *a, Item *b)
      : Item_func_numhybrid(pos, a, b) {}

  void fix_num_length_and_dec() override;
  void set_numeric_type() override;
  String *str_op(String *) override {
    assert(0);
    return nullptr;
  }
  bool date_op(MYSQL_TIME *, my_time_flags_t) override {
    assert(0);
    return false;
  }
  bool time_op(MYSQL_TIME *) override {
    assert(0);
    return false;
  }
};

/* Base class for operations like '+', '-', '*' */
class Item_num_op : public Item_func_numhybrid {
 protected:
  bool resolve_type_inner(THD *thd) override;

 public:
  Item_num_op(Item *a, Item *b) : Item_func_numhybrid(a, b) {}
  Item_num_op(const POS &pos, Item *a, Item *b)
      : Item_func_numhybrid(pos, a, b) {}

  virtual void result_precision() = 0;

  void print(const THD *thd, String *str,
             enum_query_type query_type) const override {
    print_op(thd, str, query_type);
  }

  void set_numeric_type() override;
  String *str_op(String *) override {
    assert(0);
    return nullptr;
  }
  bool date_op(MYSQL_TIME *, my_time_flags_t) override {
    assert(0);
    return false;
  }
  bool time_op(MYSQL_TIME *) override {
    assert(0);
    return false;
  }
};

class Item_int_func : public Item_func {
 public:
  Item_int_func() : Item_func() { set_data_type_longlong(); }
  explicit Item_int_func(const POS &pos) : Item_func(pos) {
    set_data_type_longlong();
  }

  Item_int_func(Item *a) : Item_func(a) { set_data_type_longlong(); }
  Item_int_func(const POS &pos, Item *a) : Item_func(pos, a) {
    set_data_type_longlong();
  }

  Item_int_func(Item *a, Item *b) : Item_func(a, b) {
    set_data_type_longlong();
  }
  Item_int_func(const POS &pos, Item *a, Item *b) : Item_func(pos, a, b) {
    set_data_type_longlong();
  }

  Item_int_func(Item *a, Item *b, Item *c) : Item_func(a, b, c) {
    set_data_type_longlong();
  }
  Item_int_func(const POS &pos, Item *a, Item *b, Item *c)
      : Item_func(pos, a, b, c) {
    set_data_type_longlong();
  }

  Item_int_func(Item *a, Item *b, Item *c, Item *d) : Item_func(a, b, c, d) {
    set_data_type_longlong();
  }
  Item_int_func(const POS &pos, Item *a, Item *b, Item *c, Item *d)
      : Item_func(pos, a, b, c, d) {
    set_data_type_longlong();
  }

  Item_int_func(const POS &pos, Item *a, Item *b, Item *c, Item *d, Item *e)
      : Item_func(pos, a, b, c, d, e) {
    set_data_type_longlong();
  }

  explicit Item_int_func(mem_root_deque<Item *> *list) : Item_func(list) {
    set_data_type_longlong();
  }
  Item_int_func(const POS &pos, PT_item_list *opt_list)
      : Item_func(pos, opt_list) {
    set_data_type_longlong();
  }

  Item_int_func(THD *thd, Item_int_func *item) : Item_func(thd, item) {
    set_data_type_longlong();
  }
  double val_real() override;
  String *val_str(String *str) override;
  bool get_date(MYSQL_TIME *ltime, my_time_flags_t fuzzydate) override {
    return get_date_from_int(ltime, fuzzydate);
  }
  bool get_time(MYSQL_TIME *ltime) override { return get_time_from_int(ltime); }
  enum Item_result result_type() const override { return INT_RESULT; }
  /*
    Concerning PS-param types,
    resolve_type(THD *) is not overridden here, as experience shows that for
    most child classes of this class, VARCHAR is the best default
  */
};

class Item_func_connection_id final : public Item_int_func {
  typedef Item_int_func super;

 public:
  Item_func_connection_id(const POS &pos) : Item_int_func(pos) {}

  table_map get_initial_pseudo_tables() const override {
    return INNER_TABLE_BIT;
  }
  bool itemize(Parse_context *pc, Item **res) override;
  const char *func_name() const override { return "connection_id"; }
  bool resolve_type(THD *thd) override;
  bool fix_fields(THD *thd, Item **ref) override;
  longlong val_int() override;
  bool check_function_as_value_generator(uchar *checker_args) override {
    Check_function_as_value_generator_parameters *func_arg =
        pointer_cast<Check_function_as_value_generator_parameters *>(
            checker_args);
    func_arg->banned_function_name = func_name();
    return ((func_arg->source == VGS_GENERATED_COLUMN) ||
            (func_arg->source == VGS_CHECK_CONSTRAINT));
  }

  Item *pq_clone(THD *thd, Query_block *select) override;
  bool pq_copy_from(THD *thd, Query_block *select, Item *item) override;
};

class Item_typecast_signed final : public Item_int_func {
 public:
  Item_typecast_signed(const POS &pos, Item *a) : Item_int_func(pos, a) {
    unsigned_flag = false;
  }
  const char *func_name() const override { return "cast_as_signed"; }
  longlong val_int() override;
  bool resolve_type(THD *thd) override;
  void print(const THD *thd, String *str,
             enum_query_type query_type) const override;
  enum Functype functype() const override { return TYPECAST_FUNC; }
  Item *pq_clone(THD *thd, Query_block *select) override;
};

class Item_typecast_unsigned final : public Item_int_func {
 public:
  Item_typecast_unsigned(const POS &pos, Item *a) : Item_int_func(pos, a) {
    unsigned_flag = true;
  }
  const char *func_name() const override { return "cast_as_unsigned"; }
  longlong val_int() override;
  bool resolve_type(THD *thd) override;
  void print(const THD *thd, String *str,
             enum_query_type query_type) const override;
  enum Functype functype() const override { return TYPECAST_FUNC; }
  Item *pq_clone(THD *thd, Query_block *select) override;
};

class Item_typeto_format_number final : public Item_func {
  bool fixed_length;  // True if item  format string has been resolved
  const MY_LOCALE *locale;
  int64_t actual_precision;
  typedef struct {
    bool zero_return_empty;  // B zero fill ' ',len format_length
    uint16 is_minus;         // first +,-,< null " "
    uint16 is_dollar;        // todo cny or CNY
    uint16 is_EEEE;          // ronam, hex,EEEE, tmE, tm9
    uint16 proint;           // proint
    uint16 is_g;             // G FMT
    uint16 fillzero;         // append 0 , n^10

    int fmt_inter_len;
    uint16 fmt_inter[DECIMAL_MAX_FIELD_SIZE];  // oracle_num str max is 64+1
    int fmt_decimal_len;                       // append right 0
    uint16 fmt_decimal[DECIMAL_MAX_FIELD_SIZE];

    int intg;              //  value len
    int fixed_prec;        // append '0'  intg like 009
    int fixed_prec_end;    // append '0'  like .00
    size_t format_length;  // format string length fill '#' if overflow
    uint16 FM;             // trim
  } NUMDesc;

  /*
   When FMT format models is parsed, use uint16 integers to
    represent the format models and store in fmt_array.
  */

  NUMDesc *num_format;

  // parser FORMAT NUMBER
  bool generate_number_format_array(const String *format, uint *fmt_len,
                                    uint *dec);
  bool make_format_number_oracle(my_decimal *num, String *str);

  my_decimal *format_arg_string_to_number(String *format_string,
                                          my_decimal *dec);

  bool check_format(String *format_string);

 public:
  Item_typeto_format_number(const POS &pos, Item *a, Item *b)
      : Item_func(pos, a, b),
        fixed_length(false),
        locale(nullptr),
        num_format(nullptr) {}
  String *val_str(String *str) override;
  double val_real() override;
  longlong val_int() override;
  bool get_date(MYSQL_TIME *ltime, my_time_flags_t fuzzydate) override {
    return get_date_from_decimal(ltime, fuzzydate);
  }
  bool get_time(MYSQL_TIME *ltime) override {
    return get_time_from_decimal(ltime);
  }
  my_decimal *val_decimal(my_decimal *) override;
  enum Item_result result_type() const override { return DECIMAL_RESULT; }
  const char *func_name() const override { return "to_number"; }
  enum Functype functype() const override { return TYPECAST_FUNC; }
  bool resolve_type(THD *thd) override;
};

class Item_typeto_number final : public Item_func {
 public:
  Item_typeto_number(const POS &pos, Item *a, int len, int dec)
      : Item_func(pos, a) {
    set_data_type_decimal(len, dec);
  }
  String *val_str(String *str) override;
  double val_real() override;
  longlong val_int() override;
  bool get_date(MYSQL_TIME *ltime, my_time_flags_t fuzzydate) override {
    return get_date_from_decimal(ltime, fuzzydate);
  }
  bool get_time(MYSQL_TIME *ltime) override {
    return get_time_from_decimal(ltime);
  }
  my_decimal *val_decimal(my_decimal *) override;
  enum Item_result result_type() const override { return DECIMAL_RESULT; }
  bool resolve_type(THD *thd) override {
    set_nullable(true);
    if (args[0]->propagate_type(thd, MYSQL_TYPE_NEWDECIMAL, false, true))
      return true;
    return false;
  }
  const char *func_name() const override { return "to_number"; }
  enum Functype functype() const override { return TYPECAST_FUNC; }
  void print(const THD *thd, String *str,
             enum_query_type query_type) const override;

 private:
  bool from_str = false;
  int check_input_str_precision(String *res);
};

class Item_typecast_decimal final : public Item_func {
 public:
  Item_typecast_decimal(const POS &pos, Item *a, int len, int dec)
      : Item_func(pos, a), pq_precision(len) {
    set_data_type_decimal(len, dec);
  }
  int pq_precision;
  String *val_str(String *str) override;
  double val_real() override;
  longlong val_int() override;
  bool get_date(MYSQL_TIME *ltime, my_time_flags_t fuzzydate) override {
    return get_date_from_decimal(ltime, fuzzydate);
  }
  bool get_time(MYSQL_TIME *ltime) override {
    return get_time_from_decimal(ltime);
  }
  my_decimal *val_decimal(my_decimal *) override;
  enum Item_result result_type() const override { return DECIMAL_RESULT; }
  bool resolve_type(THD *thd) override {
    if (args[0]->propagate_type(thd, MYSQL_TYPE_NEWDECIMAL, false, true))
      return true;
    return false;
  }
  const char *func_name() const override { return "cast_as_decimal"; }
  enum Functype functype() const override { return TYPECAST_FUNC; }
  void print(const THD *thd, String *str,
             enum_query_type query_type) const override;
  Item *pq_clone(THD *thd, Query_block *select) override;
};

/**
  Class used to implement CAST to floating-point data types.
*/
class Item_typecast_real final : public Item_func {
 public:
  Item_typecast_real(const POS &pos, Item *a, bool as_double)
      : Item_func(pos, a) {
    if (as_double)
      set_data_type_double();
    else
      set_data_type_float();
  }
  Item_typecast_real(Item *a) : Item_func(a) { set_data_type_double(); }
  String *val_str(String *str) override;
  double val_real() override;
  longlong val_int() override { return val_int_from_real(); }
  bool get_date(MYSQL_TIME *ltime, my_time_flags_t fuzzydate) override;
  bool get_time(MYSQL_TIME *ltime) override;
  my_decimal *val_decimal(my_decimal *decimal_value) override;
  enum Item_result result_type() const override { return REAL_RESULT; }
  bool resolve_type(THD *thd) override {
    return args[0]->propagate_type(thd, MYSQL_TYPE_DOUBLE, false, true);
  }
  const char *func_name() const override { return "cast_as_real"; }
  enum Functype functype() const override { return TYPECAST_FUNC; }
  void print(const THD *thd, String *str,
             enum_query_type query_type) const override;
  Item *pq_clone(THD *thd, Query_block *select) override;
};

class Item_func_additive_op : public Item_num_op {
 public:
  Item_func_additive_op(Item *a, Item *b) : Item_num_op(a, b) {}
  Item_func_additive_op(const POS &pos, Item *a, Item *b)
      : Item_num_op(pos, a, b) {}

  void result_precision() override;
  bool check_partition_func_processor(uchar *) override { return false; }
  bool check_function_as_value_generator(uchar *) override { return false; }
};

class Item_func_plus final : public Item_func_additive_op {
  uint datetype_offset;

 public:
  Item_func_plus(Item *a, Item *b)
      : Item_func_additive_op(a, b), datetype_offset(0) {}
  Item_func_plus(const POS &pos, Item *a, Item *b)
      : Item_func_additive_op(pos, a, b), datetype_offset(0) {}

  const char *func_name() const override { return "+"; }

  // SUPPRESS_UBSAN: signed integer overflow
  longlong int_op() override SUPPRESS_UBSAN;

  double real_op() override;
  my_decimal *decimal_op(my_decimal *) override;
  bool date_op(MYSQL_TIME *, my_time_flags_t) override;
  enum Functype functype() const override { return PLUS_FUNC; }

 protected:
  bool resolve_type_inner(THD *thd) override;
  Item *pq_clone(THD *thd, Query_block *select) override;
};

class Item_func_minus final : public Item_func_additive_op {
  uint datetype_count;

 public:
  Item_func_minus(Item *a, Item *b)
      : Item_func_additive_op(a, b), datetype_count(0) {}
  Item_func_minus(const POS &pos, Item *a, Item *b)
      : Item_func_additive_op(pos, a, b), datetype_count(0) {}

  const char *func_name() const override { return "-"; }

  // SUPPRESS_UBSAN: signed integer overflow
  longlong int_op() override SUPPRESS_UBSAN;

  double real_op() override;
  my_decimal *decimal_op(my_decimal *) override;
  bool date_op(MYSQL_TIME *, my_time_flags_t) override;
  bool resolve_type(THD *thd) override;
  enum Functype functype() const override { return MINUS_FUNC; }
  String *val_str(String *str) override;

 protected:
  bool resolve_type_inner(THD *thd) override;
  Item *pq_clone(THD *thd, Query_block *select) override;
};

class Item_func_mul final : public Item_num_op {
 public:
  Item_func_mul(Item *a, Item *b) : Item_num_op(a, b) {}
  Item_func_mul(const POS &pos, Item *a, Item *b) : Item_num_op(pos, a, b) {}

  const char *func_name() const override { return "*"; }
  longlong int_op() override;
  double real_op() override;
  my_decimal *decimal_op(my_decimal *) override;
  void result_precision() override;
  bool check_partition_func_processor(uchar *) override { return false; }
  bool check_function_as_value_generator(uchar *) override { return false; }
  enum Functype functype() const override { return MUL_FUNC; }
  Item *pq_clone(THD *thd, Query_block *select) override;
};

class Item_func_div_base : public Item_num_op {
 public:
  Item_func_div_base(const POS &pos, Item *a, Item *b)
      : Item_num_op(pos, a, b) {}
  Item_func_div_base(Item *a, Item *b) : Item_num_op(a, b) {}
  longlong int_op() override;
  double real_op() override;
  my_decimal *decimal_op(my_decimal *) override;
  enum Functype functype() const override { return DIV_FUNC; }

 protected:
  uint m_prec_increment;
};

class Item_func_div final : public Item_func_div_base {
 public:
  Item_func_div(const POS &pos, Item *a, Item *b)
      : Item_func_div_base(pos, a, b) {}
  const char *func_name() const override { return "/"; }
  bool resolve_type(THD *thd) override;
  void result_precision() override;
  enum Functype functype() const override { return DIV_FUNC; }
  Item *pq_clone(THD *thd, Query_block *select) override;
};

class Item_func_div_int final : public Item_func_div_base {
 public:
  Item_func_div_int(Item *a, Item *b) : Item_func_div_base(a, b) {}
  Item_func_div_int(const POS &pos, Item *a, Item *b)
      : Item_func_div_base(pos, a, b) {}
  const char *func_name() const override { return "DIV"; }
  enum_field_types default_data_type() const override {
    return MYSQL_TYPE_LONGLONG;
  }
  bool resolve_type(THD *thd) override;
  void result_precision() override;
  void set_numeric_type() override;
  bool check_partition_func_processor(uchar *) override { return false; }
  bool check_function_as_value_generator(uchar *) override { return false; }
  Item *pq_clone(THD *thd, Query_block *select) override;
};

class Item_func_mod final : public Item_num_op {
 public:
  Item_func_mod(Item *a, Item *b) : Item_num_op(a, b) {}
  Item_func_mod(const POS &pos, Item *a, Item *b) : Item_num_op(pos, a, b) {}

  longlong int_op() override;
  double real_op() override;
  my_decimal *decimal_op(my_decimal *) override;
  const char *func_name() const override { return "%"; }
  void result_precision() override;
  bool resolve_type(THD *thd) override;
  bool check_partition_func_processor(uchar *) override { return false; }
  bool check_function_as_value_generator(uchar *) override { return false; }
  enum Functype functype() const override { return MOD_FUNC; }
  Item *pq_clone(THD *thd, Query_block *select) override;
};

class Item_func_neg final : public Item_func_num1 {
 public:
  Item_func_neg(Item *a) : Item_func_num1(a) {}
  Item_func_neg(const POS &pos, Item *a) : Item_func_num1(pos, a) {}

  double real_op() override;
  longlong int_op() override;
  my_decimal *decimal_op(my_decimal *) override;
  const char *func_name() const override { return "-"; }
  enum Functype functype() const override { return NEG_FUNC; }
  bool resolve_type(THD *thd) override;
  void fix_num_length_and_dec() override;
  bool check_partition_func_processor(uchar *) override { return false; }
  bool check_function_as_value_generator(uchar *) override { return false; }
  Item *pq_clone(THD *thd, Query_block *select) override;
};

class Item_func_abs final : public Item_func_num1 {
 public:
  Item_func_abs(const POS &pos, Item *a) : Item_func_num1(pos, a) {}
  double real_op() override;
  longlong int_op() override;
  my_decimal *decimal_op(my_decimal *) override;
  const char *func_name() const override { return "abs"; }
  bool resolve_type(THD *) override;
  bool check_partition_func_processor(uchar *) override { return false; }
  bool check_function_as_value_generator(uchar *) override { return false; }
  enum Functype functype() const override { return ABS_FUNC; }
  Item *pq_clone(THD *thd, Query_block *select) override;
};

// A class to handle logarithmic and trigonometric functions

class Item_dec_func : public Item_real_func {
 public:
  Item_dec_func(Item *a) : Item_real_func(a) {}
  Item_dec_func(const POS &pos, Item *a) : Item_real_func(pos, a) {}

  Item_dec_func(const POS &pos, Item *a, Item *b) : Item_real_func(pos, a, b) {}
  bool resolve_type(THD *thd) override;
};

class Item_func_exp final : public Item_dec_func {
 public:
  Item_func_exp(const POS &pos, Item *a) : Item_dec_func(pos, a) {}
  double val_real() override;
  const char *func_name() const override { return "exp"; }
  enum Functype functype() const override { return EXP_FUNC; }
  Item *pq_clone(THD *thd, Query_block *select) override;
};

class Item_func_ln final : public Item_dec_func {
 public:
  Item_func_ln(const POS &pos, Item *a) : Item_dec_func(pos, a) {}
  double val_real() override;
  const char *func_name() const override { return "ln"; }
  enum Functype functype() const override { return LN_FUNC; }
  Item *pq_clone(THD *thd, Query_block *select) override;
};

class Item_func_log final : public Item_dec_func {
 public:
  Item_func_log(const POS &pos, Item *a) : Item_dec_func(pos, a) {}
  Item_func_log(const POS &pos, Item *a, Item *b) : Item_dec_func(pos, a, b) {}
  double val_real() override;
  const char *func_name() const override { return "log"; }
  enum Functype functype() const override { return LOG_FUNC; }
  Item *pq_clone(THD *thd, Query_block *select) override;
};

class Item_func_log2 final : public Item_dec_func {
 public:
  Item_func_log2(const POS &pos, Item *a) : Item_dec_func(pos, a) {}
  double val_real() override;
  const char *func_name() const override { return "log2"; }
  enum Functype functype() const override { return LOG2_FUNC; }
  Item *pq_clone(THD *thd, Query_block *select) override;
};

class Item_func_log10 final : public Item_dec_func {
 public:
  Item_func_log10(const POS &pos, Item *a) : Item_dec_func(pos, a) {}
  double val_real() override;
  const char *func_name() const override { return "log10"; }
  enum Functype functype() const override { return LOG10_FUNC; }
  Item *pq_clone(THD *thd, Query_block *select) override;
};

class Item_func_sqrt final : public Item_dec_func {
 public:
  Item_func_sqrt(const POS &pos, Item *a) : Item_dec_func(pos, a) {}
  double val_real() override;
  const char *func_name() const override { return "sqrt"; }
  enum Functype functype() const override { return SQRT_FUNC; }
  Item *pq_clone(THD *thd, Query_block *select) override;
};

class Item_func_pow final : public Item_dec_func {
 public:
  Item_func_pow(const POS &pos, Item *a, Item *b) : Item_dec_func(pos, a, b) {}
  double val_real() override;
  const char *func_name() const override { return "pow"; }
  enum Functype functype() const override { return POW_FUNC; }
  Item *pq_clone(THD *thd, Query_block *select) override;
};

class Item_func_acos final : public Item_dec_func {
 public:
  Item_func_acos(const POS &pos, Item *a) : Item_dec_func(pos, a) {}
  double val_real() override;
  const char *func_name() const override { return "acos"; }
  enum Functype functype() const override { return ACOS_FUNC; }
  Item *pq_clone(THD *thd, Query_block *select) override;
};

class Item_func_asin final : public Item_dec_func {
 public:
  Item_func_asin(const POS &pos, Item *a) : Item_dec_func(pos, a) {}
  double val_real() override;
  const char *func_name() const override { return "asin"; }
  enum Functype functype() const override { return ASIN_FUNC; }
  Item *pq_clone(THD *thd, Query_block *select) override;
};

class Item_func_atan final : public Item_dec_func {
 public:
  Item_func_atan(const POS &pos, Item *a) : Item_dec_func(pos, a) {}
  Item_func_atan(const POS &pos, Item *a, Item *b) : Item_dec_func(pos, a, b) {}
  double val_real() override;
  const char *func_name() const override { return "atan"; }
  enum Functype functype() const override { return ATAN_FUNC; }
  Item *pq_clone(THD *thd, Query_block *select) override;
};

class Item_func_cos final : public Item_dec_func {
 public:
  Item_func_cos(const POS &pos, Item *a) : Item_dec_func(pos, a) {}
  double val_real() override;
  const char *func_name() const override { return "cos"; }
  enum Functype functype() const override { return COS_FUNC; }
  Item *pq_clone(THD *thd, Query_block *select) override;
};

class Item_func_sin final : public Item_dec_func {
 public:
  Item_func_sin(const POS &pos, Item *a) : Item_dec_func(pos, a) {}
  double val_real() override;
  const char *func_name() const override { return "sin"; }
  enum Functype functype() const override { return SIN_FUNC; }
  Item *pq_clone(THD *thd, Query_block *select) override;
};

class Item_func_tan final : public Item_dec_func {
 public:
  Item_func_tan(const POS &pos, Item *a) : Item_dec_func(pos, a) {}
  double val_real() override;
  const char *func_name() const override { return "tan"; }
  enum Functype functype() const override { return TAN_FUNC; }
  Item *pq_clone(THD *thd, Query_block *select) override;
};

class Item_func_cot final : public Item_dec_func {
 public:
  Item_func_cot(const POS &pos, Item *a) : Item_dec_func(pos, a) {}
  double val_real() override;
  const char *func_name() const override { return "cot"; }
  enum Functype functype() const override { return COT_FUNC; }
  Item *pq_clone(THD *thd, Query_block *select) override;
};

class Item_func_int_val : public Item_func_num1 {
 public:
  Item_func_int_val(Item *a) : Item_func_num1(a) {}
  Item_func_int_val(const POS &pos, Item *a) : Item_func_num1(pos, a) {}
  bool resolve_type_inner(THD *thd) override;
};

class Item_func_ceiling final : public Item_func_int_val {
 public:
  Item_func_ceiling(Item *a) : Item_func_int_val(a) {}
  Item_func_ceiling(const POS &pos, Item *a) : Item_func_int_val(pos, a) {}
  const char *func_name() const override { return "ceiling"; }
  longlong int_op() override;
  double real_op() override;
  my_decimal *decimal_op(my_decimal *) override;
  bool check_partition_func_processor(uchar *) override { return false; }
  bool check_function_as_value_generator(uchar *) override { return false; }
  enum Functype functype() const override { return CEILING_FUNC; }
  Item *pq_clone(THD *thd, Query_block *select) override;
};

class Item_func_floor final : public Item_func_int_val {
 public:
  Item_func_floor(Item *a) : Item_func_int_val(a) {}
  Item_func_floor(const POS &pos, Item *a) : Item_func_int_val(pos, a) {}
  const char *func_name() const override { return "floor"; }
  longlong int_op() override;
  double real_op() override;
  my_decimal *decimal_op(my_decimal *) override;
  bool check_partition_func_processor(uchar *) override { return false; }
  bool check_function_as_value_generator(uchar *) override { return false; }
  enum Functype functype() const override { return FLOOR_FUNC; }
  Item *pq_clone(THD *thd, Query_block *select) override;
};

/* This handles round and truncate */

class Item_func_round : public Item_func_num1 {
  typedef Item_func_num1 super;
  bool truncate;

 protected:
  uint16 unit_type;
  bool trunc_date;

 public:
  Item_func_round(Item *a, Item *b, bool trunc_arg)
      : Item_func_num1(a, b),
        truncate(trunc_arg),
        unit_type(FMT_DDD),
        trunc_date(false) {}
  Item_func_round(const POS &pos, Item *a, Item *b, bool trunc_arg)
      : Item_func_num1(pos, a, b),
        truncate(trunc_arg),
        unit_type(FMT_DDD),
        trunc_date(false) {}

  const char *func_name() const override {
    return truncate ? "truncate" : "round";
  }
  double real_op() override;
  longlong int_op() override;
  my_decimal *decimal_op(my_decimal *) override;
  bool resolve_type(THD *) override;
  enum Functype functype() const override {
    return truncate ? TRUNCATE_FUNC : ROUND_FUNC;
  }
  Item *pq_clone(THD *thd, Query_block *select) override;
  String *val_str(String *str) override {
    if (trunc_date) return val_string_from_datetime(str);
    check_trunc_number_params();
    return (null_value ? nullptr : super::val_str(str));
  }

  double val_real() override {
    if (trunc_date) return val_real_from_decimal();
    check_trunc_number_params();
    return (null_value ? 0.0 : real_op());
  }

  longlong val_int() override {
    if (trunc_date) return val_int_from_datetime();
    check_trunc_number_params();
    return (null_value ? 0 : super::val_int());
  }

  my_decimal *val_decimal(my_decimal *decimal_value) override {
    if (trunc_date) return val_decimal_from_date(decimal_value);
    check_trunc_number_params();
    return (null_value ? nullptr : decimal_op(decimal_value));
  }

 private:
  void check_trunc_number_params();
  longlong get_decimal_place();
};

/* This handles oracle round */

class Item_func_ora_round : public Item_func_round {
  typedef Item_func_round super;

 public:
  Item_func_ora_round(Item *a, Item *b, bool trunc_arg)
      : Item_func_round(a, b, trunc_arg) {
    set_nullable(true);
    set_has_notsupported_func_true();
  }
  Item_func_ora_round(const POS &pos, Item *a, Item *b, bool trunc_arg)
      : Item_func_round(pos, a, b, trunc_arg) {
    set_nullable(true);
    set_has_notsupported_func_true();
  }

  const char *func_name() const override { return "round"; }
  enum Functype functype() const override { return ROUND_FUNC; }
  bool resolve_type(THD *thd) override;
  bool get_date(MYSQL_TIME *ltime, my_time_flags_t fuzzy_date) override;
};

class Item_func_rand final : public Item_real_func {
  typedef Item_real_func super;

  rand_struct *m_rand{nullptr};
  bool first_eval{true};  // true if val_real() is called 1st time
 public:
  Item_func_rand(const POS &pos, Item *a) : Item_real_func(pos, a) {}
  explicit Item_func_rand(const POS &pos) : Item_real_func(pos) {}

  bool itemize(Parse_context *pc, Item **res) override;
  double val_real() override;
  const char *func_name() const override { return "rand"; }
  /**
    This function is non-deterministic and hence depends on the
    'RAND' pseudo-table.

    @retval RAND_TABLE_BIT
  */
  table_map get_initial_pseudo_tables() const override {
    return RAND_TABLE_BIT;
  }
  bool fix_fields(THD *thd, Item **ref) override;
  bool resolve_type(THD *thd) override;
  void cleanup() override {
    first_eval = true;
    Item_real_func::cleanup();
  }
  bool check_function_as_value_generator(uchar *checker_args) override {
    Check_function_as_value_generator_parameters *func_arg =
        pointer_cast<Check_function_as_value_generator_parameters *>(
            checker_args);
    func_arg->banned_function_name = func_name();
    return ((func_arg->source == VGS_GENERATED_COLUMN) ||
            (func_arg->source == VGS_CHECK_CONSTRAINT));
  }

  Item *pq_clone(THD *thd, Query_block *select) override;
  enum Functype functype() const override { return RAND_FUNC; }

 private:
  void seed_random(Item *val);
};

class Item_func_sign final : public Item_int_func {
 public:
  Item_func_sign(const POS &pos, Item *a) : Item_int_func(pos, a) {}
  const char *func_name() const override { return "sign"; }
  enum Functype functype() const override { return SIGN_FUNC; }
  longlong val_int() override;
  bool resolve_type(THD *thd) override;
};

// Common base class for the DEGREES and RADIANS functions.
class Item_func_units : public Item_real_func {
  double mul, add;

 protected:
  Item_func_units(const POS &pos, Item *a, double mul_arg, double add_arg)
      : Item_real_func(pos, a), mul(mul_arg), add(add_arg) {}

 public:
  double val_real() override;
  bool resolve_type(THD *thd) override;
};

class Item_func_degrees final : public Item_func_units {
 public:
  Item_func_degrees(const POS &pos, Item *a)
      : Item_func_units(pos, a, 180.0 / M_PI, 0.0) {}
  const char *func_name() const override { return "degrees"; }
  enum Functype functype() const override { return DEGREES_FUNC; }
};

class Item_func_radians final : public Item_func_units {
 public:
  Item_func_radians(const POS &pos, Item *a)
      : Item_func_units(pos, a, M_PI / 180.0, 0.0) {}
  const char *func_name() const override { return "radians"; }
  enum Functype functype() const override { return RADIANS_FUNC; }
};
class Item_func_min_max : public Item_func_numhybrid {
 public:
  Item_func_min_max(const POS &pos, PT_item_list *opt_list, bool is_least_func)
      : Item_func_numhybrid(pos, opt_list),
        m_is_least_func(is_least_func),
        temporal_item(nullptr) {}

  longlong val_int() override;
  double val_real() override;
  my_decimal *val_decimal(my_decimal *) override;
  longlong int_op() override;
  double real_op() override;
  my_decimal *decimal_op(my_decimal *) override;
  String *str_op(String *) override;
  bool date_op(MYSQL_TIME *ltime, my_time_flags_t fuzzydate) override;
  bool time_op(MYSQL_TIME *ltime) override;
  enum_field_types default_data_type() const override {
    return MYSQL_TYPE_VARCHAR;
  }
  bool resolve_type(THD *thd) override;
  bool resolve_type_inner(THD *thd) override;
  void set_numeric_type() override {}
  enum Item_result result_type() const override { return hybrid_type; }

  /**
    Make CAST(LEAST_OR_GREATEST(datetime_expr, varchar_expr))
    return a number in format YYMMDDhhmmss.
  */
  enum Item_result cast_to_int_type() const override {
    return compare_as_dates() ? INT_RESULT : result_type();
  }

  /// Returns true if arguments to this function should be compared as dates.
  bool compare_as_dates() const;

  /// Returns true if at least one of the arguments was of temporal type.
  bool has_temporal_arg() const { return temporal_item; }

 private:
  /// True if LEAST function, false if GREATEST.
  const bool m_is_least_func;
  String m_string_buf;
  /*
    Used for determining whether one of the arguments is of temporal type and
    for converting arguments to a common output format if arguments are
    compared as dates and result type is character string. For example,
    LEAST('95-05-05', date '10-10-10') should return '1995-05-05', not
    '95-05-05'.
  */
  Item *temporal_item;
  /**
    Compare arguments as datetime values.

    @param value Pointer to which the datetime value of the winning argument
    is written.

    @return true if error, false otherwise.
  */
  bool cmp_datetimes(longlong *value);

  /**
    Compare arguments as time values.

    @param value Pointer to which the time value of the winning argument is
    written.

    @return true if error, false otherwise.
  */
  bool cmp_times(longlong *value);
};

class Item_func_min final : public Item_func_min_max {
 public:
  Item_func_min(const POS &pos, PT_item_list *opt_list)
      : Item_func_min_max(pos, opt_list, true) {}
  const char *func_name() const override { return "least"; }
  enum Functype functype() const override { return LEAST_FUNC; }
  Item *pq_clone(THD *thd, Query_block *select) override;
};

class Item_func_max final : public Item_func_min_max {
 public:
  Item_func_max(const POS &pos, PT_item_list *opt_list)
      : Item_func_min_max(pos, opt_list, false) {}
  const char *func_name() const override { return "greatest"; }
  enum Functype functype() const override { return GREATEST_FUNC; }
  Item *pq_clone(THD *thd, Query_block *select) override;
};

/**
  A wrapper Item that normally returns its parameter, but becomes NULL when
  processing rows for rollup. Rollup is implemented by AggregateIterator, and
  works by means of hierarchical levels -- 0 is the “grand totals” phase, 1 is
  where only one group level is active, and so on. E.g., for a query with GROUP
  BY a,b, the rows will look like this:

     a     b      rollup level
     1     1      2
     1     2      2
     1     NULL   1
     2     1      2
     2     NULL   1
     NULL  NULL   0

  Each rollup group item has a minimum level for when it becomes NULL. In the
  example above, a would have minimum level 0 and b would have minimum level 1.
  For simplicity, the JOIN carries a list of all rollup group items, and they
  are being given the current rollup level when it changes. A rollup level of
  INT_MAX essentially always disables rollup, which is useful when there are
  leftover group items in places that are not relevant for rollup
  (e.g., sometimes resolving can leave rollup wrappers in place for temporary
  tables that are created before grouping, which should then effectively be
  disabled).
 */
class Item_rollup_group_item final : public Item_func {
 public:
  Item_rollup_group_item(int min_rollup_level, Item *inner_item)
      : Item_func(inner_item), m_min_rollup_level(min_rollup_level) {
    item_name = inner_item->item_name;
    set_data_type_from_item(inner_item);
    // We're going to replace inner_item in the SELECT list, so copy its hidden
    // status. (We could have done this in the caller, but it fits naturally in
    // with all the other copying done here.)
    hidden = inner_item->hidden;
    set_nullable(true);
    set_rollup_expr();
  }
  double val_real() override;
  longlong val_int() override;
  String *val_str(String *str) override;
  my_decimal *val_decimal(my_decimal *dec) override;
  bool val_json(Json_wrapper *result) override;
  bool get_date(MYSQL_TIME *ltime, my_time_flags_t fuzzydate) override;
  bool get_time(MYSQL_TIME *ltime) override;
  const char *func_name() const override { return "rollup_group_item"; }
  table_map used_tables() const override {
    /*
      If underlying item is non-constant, return its used_tables value.
      Otherwise, ensure it is non-constant by adding RAND_TABLE_BIT.
    */
    return args[0]->const_for_execution()
               ? (args[0]->used_tables() | RAND_TABLE_BIT)
               : args[0]->used_tables();
  }
  Item_result result_type() const override { return args[0]->result_type(); }
  bool resolve_type(THD *) override {
    // needn't handle dynamic parameter as its const_item() is false.
    set_data_type_from_item(args[0]);

    // The item could be a NULL constant.
    null_value = args[0]->is_null();
    return false;
  }
  Item *inner_item() { return args[0]; }
  const Item *inner_item() const { return args[0]; }
  bool rollup_null() const {
    return m_current_rollup_level <= m_min_rollup_level;
  }
  enum Functype functype() const override { return ROLLUP_GROUP_ITEM_FUNC; }
  void print(const THD *thd, String *str,
             enum_query_type query_type) const override;
  bool eq(const Item *item, bool binary_cmp) const override;

  // Used by AggregateIterator.
  void set_current_rollup_level(int level) { m_current_rollup_level = level; }

  // Used when cloning the item only.
  int min_rollup_level() const { return m_min_rollup_level; }

 private:
  const int m_min_rollup_level;
  int m_current_rollup_level = INT_MAX;
};

class Item_func_lengthb : public Item_int_func {
  String value;

 public:
  Item_func_lengthb(const POS &pos, Item *a) : Item_int_func(pos, a) {}
  longlong val_int() override;
  const char *func_name() const override { return "lengthb"; }
  bool resolve_type(THD *thd) override {
    if (param_type_is_default(thd, 0, 1)) return true;
    max_length = 10;
    return false;
  }
  Item *pq_clone(THD *thd, Query_block *select) override;
};

class Item_func_length : public Item_int_func {
  String value;
  bool oracle_mode_flag = false;

 public:
  Item_func_length(const POS &pos, Item *a) : Item_int_func(pos, a) {}
  /* NOTE: it is suggested to override val_int() when oracle mode is requested
   */
  longlong val_int() override;
  const char *func_name() const override { return "length"; }
  bool resolve_type(THD *thd) override;

  Item *pq_clone(THD *thd, Query_block *select) override;
  enum Functype functype() const override { return LENGTH_FUNC; }
};

class Item_func_bit_length final : public Item_func_length {
 public:
  Item_func_bit_length(const POS &pos, Item *a) : Item_func_length(pos, a) {}
  longlong val_int() override {
    assert(fixed);
    return Item_func_length::val_int() * 8;
  }
  const char *func_name() const override { return "bit_length"; }
  Item *pq_clone(THD *thd, Query_block *select) override;
  enum Functype functype() const override { return BIT_LENGTH_FUNC; }
};

class Item_func_char_length final : public Item_int_func {
  String value;

 public:
  Item_func_char_length(Item *a) : Item_int_func(a) {}
  Item_func_char_length(const POS &pos, Item *a) : Item_int_func(pos, a) {}
  longlong val_int() override;
  const char *func_name() const override { return "char_length"; }
  bool resolve_type(THD *thd) override {
    max_length = 10;
    return Item_int_func::resolve_type(thd);
  }

  Item *pq_clone(THD *thd, Query_block *select) override;
  bool pq_copy_from(THD *thd, Query_block *select, Item *item) override;
  enum Functype functype() const override { return CHAR_LENGTH_FUNC; }
};

class Item_func_coercibility final : public Item_int_func {
 public:
  Item_func_coercibility(const POS &pos, Item *a) : Item_int_func(pos, a) {
    null_on_null = false;
  }
  longlong val_int() override;
  const char *func_name() const override { return "coercibility"; }
  bool resolve_type(THD *thd) override {
    max_length = 10;
    set_nullable(false);
    return Item_int_func::resolve_type(thd);
  }

  Item *pq_clone(THD *thd, Query_block *select) override;
};

class Item_func_locate : public Item_int_func {
  String value1, value2;
  DTCollation cmp_collation;

 public:
  Item_func_locate(Item *a, Item *b) : Item_int_func(a, b) {}
  Item_func_locate(const POS &pos, Item *a, Item *b)
      : Item_int_func(pos, a, b) {}
  Item_func_locate(const POS &pos, Item *a, Item *b, Item *c)
      : Item_int_func(pos, a, b, c) {}

  const char *func_name() const override { return "locate"; }
  longlong val_int() override;
  bool resolve_type(THD *thd) override;
  void print(const THD *thd, String *str,
             enum_query_type query_type) const override;
  Item *pq_clone(THD *thd, Query_block *select) override;
  bool pq_copy_from(THD *thd, Query_block *select, Item *item) override;
  enum Functype functype() const override { return LOCATE_FUNC; }
};

class Item_func_instr final : public Item_func_locate {
 public:
  Item_func_instr(const POS &pos, Item *a, Item *b)
      : Item_func_locate(pos, a, b) {}

  const char *func_name() const override { return "instr"; }
  Item *pq_clone(THD *thd, Query_block *select) override;
  enum Functype functype() const override { return INSTR_FUNC; }
};

class Item_func_instrb final : public Item_int_func {
  String value1, value2;
  DTCollation cmp_collation;

 public:
  Item_func_instrb(const POS &pos, Item *a, Item *b)
      : Item_int_func(pos, a, b) {}
  Item_func_instrb(const POS &pos, Item *a, Item *b, Item *c)
      : Item_int_func(pos, a, b, c) {}
  Item_func_instrb(const POS &pos, Item *a, Item *b, Item *c, Item *d)
      : Item_int_func(pos, a, b, c, d) {}

  const char *func_name() const override { return "instrb"; }
  longlong val_int() override;
  bool resolve_type(THD *thd) override;
  void print(const THD *thd, String *str,
             enum_query_type query_type) const override;
};

class Item_func_ora_instr final : public Item_int_func {
  String value1, value2;
  DTCollation cmp_collation;

 public:
  Item_func_ora_instr(const POS &pos, Item *a, Item *b)
      : Item_int_func(pos, a, b) {}
  Item_func_ora_instr(const POS &pos, Item *a, Item *b, Item *c, Item *d)
      : Item_int_func(pos, a, b, c, d) {}

  const char *func_name() const override { return "instr"; }
  longlong val_int() override;
  bool resolve_type(THD *thd) override;
  void print(const THD *thd, String *str,
             enum_query_type query_type) const override;
};

class Item_func_validate_password_strength final : public Item_int_func {
 public:
  Item_func_validate_password_strength(const POS &pos, Item *a)
      : Item_int_func(pos, a) {}
  longlong val_int() override;
  const char *func_name() const override {
    return "validate_password_strength";
  }
  bool resolve_type(THD *thd) override {
    max_length = 10;
    set_nullable(true);
    return Item_int_func::resolve_type(thd);
  }
};

class Item_func_field final : public Item_int_func {
  String value, tmp;
  Item_result cmp_type;

 public:
  Item_func_field(const POS &pos, PT_item_list *opt_list)
      : Item_int_func(pos, opt_list) {}
  longlong val_int() override;
  const char *func_name() const override { return "field"; }
  bool resolve_type(THD *thd) override;
  Item *pq_clone(THD *thd, Query_block *select) override;
};

class Item_func_ascii final : public Item_int_func {
  String value;

 public:
  Item_func_ascii(const POS &pos, Item *a) : Item_int_func(pos, a) {}
  longlong val_int() override;
  const char *func_name() const override { return "ascii"; }
  bool resolve_type(THD *thd) override {
    max_length = 3;
    return Item_int_func::resolve_type(thd);
  }
  Item *pq_clone(THD *thd, Query_block *select) override;
  enum Functype functype() const override { return ASCII_FUNC; }
};

class Item_func_ord final : public Item_int_func {
  String value;

 public:
  Item_func_ord(const POS &pos, Item *a) : Item_int_func(pos, a) {}
  longlong val_int() override;
  const char *func_name() const override { return "ord"; }
  Item *pq_clone(THD *thd, Query_block *select) override;
};

class Item_func_find_in_set final : public Item_int_func {
  String value, value2;
  /*
    if m_enum_value is non-zero, it indicates the index of the value of
    argument 0 in the set in argument 1, given that argument 0 is
    a constant value and argument 1 is a field of type SET.
  */
  uint m_enum_value{0};
  DTCollation cmp_collation;

 public:
  Item_func_find_in_set(const POS &pos, Item *a, Item *b)
      : Item_int_func(pos, a, b) {}
  longlong val_int() override;
  const char *func_name() const override { return "find_in_set"; }
  bool resolve_type(THD *) override;
  const CHARSET_INFO *compare_collation() const override {
    return cmp_collation.collation;
  }
  Item *pq_clone(THD *thd, Query_block *select) override;
};

/* Base class for all bit functions: '~', '|', '^', '&', '>>', '<<' */

class Item_func_bit : public Item_func {
 protected:
  /// Stores the Item's result type. Can only be INT_RESULT or STRING_RESULT
  Item_result hybrid_type;
  /// Buffer storing the determined value
  String tmp_value;
  /**
     @returns true if the second argument should be of binary type for the
     result to be of binary type.
  */
  virtual bool binary_result_requires_binary_second_arg() const = 0;

 public:
  Item_func_bit(const POS &pos, Item *a, Item *b) : Item_func(pos, a, b) {}
  Item_func_bit(const POS &pos, Item *a) : Item_func(pos, a) {}

  bool resolve_type(THD *) override;
  enum Item_result result_type() const override { return hybrid_type; }

  longlong val_int() override;
  String *val_str(String *str) override;
  double val_real() override;
  my_decimal *val_decimal(my_decimal *decimal_value) override;

  void print(const THD *thd, String *str,
             enum_query_type query_type) const override {
    print_op(thd, str, query_type);
  }
  bool get_date(MYSQL_TIME *ltime, my_time_flags_t fuzzydate) override {
    if (hybrid_type == INT_RESULT)
      return get_date_from_int(ltime, fuzzydate);
    else
      return get_date_from_string(ltime, fuzzydate);
  }
  bool get_time(MYSQL_TIME *ltime) override {
    if (hybrid_type == INT_RESULT)
      return get_time_from_int(ltime);
    else
      return get_time_from_string(ltime);
  }

  bool pq_copy_from(THD *thd, Query_block *select, Item *item) override;

 private:
  /**
    @brief Performs the operation on integers to produce a result of type
    INT_RESULT.
    @return The result of the operation.
   */
  virtual longlong int_op() = 0;

  /**
    @brief Performs the operation on binary strings to produce a result of
    type STRING_RESULT.
    @return The result of the operation.
  */
  virtual String *str_op(String *) = 0;
};

/**
  Base class for all the bit functions that work with two binary
  arguments: '&', '|', '^'.
*/

class Item_func_bit_two_param : public Item_func_bit {
 protected:
  bool binary_result_requires_binary_second_arg() const override {
    return true;
  }
  template <class Char_func, class Int_func>
  String *eval_str_op(String *, Char_func char_func, Int_func int_func);
  template <class Int_func>
  longlong eval_int_op(Int_func int_func);

 public:
  Item_func_bit_two_param(const POS &pos, Item *a, Item *b)
      : Item_func_bit(pos, a, b) {}
};

class Item_func_bit_or final : public Item_func_bit_two_param {
 public:
  Item_func_bit_or(const POS &pos, Item *a, Item *b)
      : Item_func_bit_two_param(pos, a, b) {}
  const char *func_name() const override { return "|"; }
  Item *pq_clone(THD *thd, Query_block *select) override;

 private:
  longlong int_op() override { return eval_int_op(std::bit_or<ulonglong>()); }
  String *str_op(String *str) override {
    return eval_str_op(str, std::bit_or<char>(), std::bit_or<ulonglong>());
  }
};

class Item_func_bit_and final : public Item_func_bit_two_param {
 public:
  Item_func_bit_and(const POS &pos, Item *a, Item *b)
      : Item_func_bit_two_param(pos, a, b) {}
  const char *func_name() const override { return "&"; }
  Item *pq_clone(THD *thd, Query_block *select) override;

 private:
  longlong int_op() override { return eval_int_op(std::bit_and<ulonglong>()); }
  String *str_op(String *str) override {
    return eval_str_op(str, std::bit_and<char>(), std::bit_and<ulonglong>());
  }
};

class Item_func_bit_xor final : public Item_func_bit_two_param {
 public:
  Item_func_bit_xor(const POS &pos, Item *a, Item *b)
      : Item_func_bit_two_param(pos, a, b) {}
  const char *func_name() const override { return "^"; }
  Item *pq_clone(THD *thd, Query_block *select) override;

 private:
  longlong int_op() override { return eval_int_op(std::bit_xor<ulonglong>()); }
  String *str_op(String *str) override {
    return eval_str_op(str, std::bit_xor<char>(), std::bit_xor<ulonglong>());
  }
};

class Item_func_bit_count final : public Item_int_func {
 public:
  Item_func_bit_count(const POS &pos, Item *a) : Item_int_func(pos, a) {}
  longlong val_int() override;
  const char *func_name() const override { return "bit_count"; }
  bool resolve_type(THD *thd) override {
    // Default: binary string; reprepare if integer
    if (args[0]->data_type() == MYSQL_TYPE_INVALID &&
        args[0]->propagate_type(
            thd, Type_properties(MYSQL_TYPE_VARCHAR, &my_charset_bin)))
      return true;
    max_length = MAX_BIGINT_WIDTH + 1;
    return false;
  }
  Item *pq_clone(THD *thd, Query_block *select) override;
};

class Item_func_shift : public Item_func_bit {
 protected:
  bool binary_result_requires_binary_second_arg() const override {
    return false;
  }
  template <bool to_left>
  longlong eval_int_op();
  template <bool to_left>
  String *eval_str_op(String *str);

 public:
  Item_func_shift(const POS &pos, Item *a, Item *b)
      : Item_func_bit(pos, a, b) {}
};

class Item_func_shift_left final : public Item_func_shift {
 public:
  Item_func_shift_left(const POS &pos, Item *a, Item *b)
      : Item_func_shift(pos, a, b) {}
  const char *func_name() const override { return "<<"; }
  Item *pq_clone(THD *thd, Query_block *select) override;

 private:
  longlong int_op() override { return eval_int_op<true>(); }
  String *str_op(String *str) override { return eval_str_op<true>(str); }
};

class Item_func_shift_right final : public Item_func_shift {
 public:
  Item_func_shift_right(const POS &pos, Item *a, Item *b)
      : Item_func_shift(pos, a, b) {}
  const char *func_name() const override { return ">>"; }
  Item *pq_clone(THD *thd, Query_block *select) override;

 private:
  longlong int_op() override { return eval_int_op<false>(); }
  String *str_op(String *str) override { return eval_str_op<false>(str); }
};

class Item_func_bit_neg final : public Item_func_bit {
 protected:
  bool binary_result_requires_binary_second_arg() const override {
    return false;
  }

 public:
  Item_func_bit_neg(const POS &pos, Item *a) : Item_func_bit(pos, a) {}
  const char *func_name() const override { return "~"; }
  void print(const THD *thd, String *str,
             enum_query_type query_type) const override {
    Item_func::print(thd, str, query_type);
  }
  Item *pq_clone(THD *thd, Query_block *select) override;

 private:
  longlong int_op() override;
  String *str_op(String *str) override;
};

class Item_func_last_insert_id final : public Item_int_func {
  typedef Item_int_func super;

 public:
  Item_func_last_insert_id() : Item_int_func() {}
  explicit Item_func_last_insert_id(const POS &pos) : Item_int_func(pos) {}
  Item_func_last_insert_id(const POS &pos, Item *a) : Item_int_func(pos, a) {}

  bool itemize(Parse_context *pc, Item **res) override;
  longlong val_int() override;
  const char *func_name() const override { return "last_insert_id"; }
  bool resolve_type(THD *thd) override {
    if (param_type_is_default(thd, 0, 1, MYSQL_TYPE_LONGLONG)) return true;
    unsigned_flag = true;
    return false;
  }
  bool check_function_as_value_generator(uchar *checker_args) override {
    Check_function_as_value_generator_parameters *func_arg =
        pointer_cast<Check_function_as_value_generator_parameters *>(
            checker_args);
    func_arg->banned_function_name = func_name();
    return true;
  }

  Item *pq_clone(THD *thd, Query_block *select) override;
};

class Item_func_benchmark final : public Item_int_func {
  typedef Item_int_func super;

 public:
  Item_func_benchmark(const POS &pos, Item *count_expr, Item *expr)
      : Item_int_func(pos, count_expr, expr) {}

  /// Ensure that "benchmark()" is never optimized away
  table_map get_initial_pseudo_tables() const override {
    return RAND_TABLE_BIT;
  }

  bool itemize(Parse_context *pc, Item **res) override;
  longlong val_int() override;
  const char *func_name() const override { return "benchmark"; }
  bool resolve_type(THD *thd) override {
    if (param_type_is_default(thd, 0, 1, MYSQL_TYPE_LONGLONG)) return true;
    if (param_type_is_default(thd, 1, 2)) return true;
    max_length = 1;
    set_nullable(true);
    return false;
  }
  void print(const THD *thd, String *str,
             enum_query_type query_type) const override;
  bool check_function_as_value_generator(uchar *checker_args) override {
    Check_function_as_value_generator_parameters *func_arg =
        pointer_cast<Check_function_as_value_generator_parameters *>(
            checker_args);
    func_arg->banned_function_name = func_name();
    return true;
  }

  Item *pq_clone(THD *thd, Query_block *select) override;
};

void item_func_sleep_init();
void item_func_sleep_free();

class Item_func_sleep final : public Item_int_func {
  typedef Item_int_func super;

 public:
  Item_func_sleep(const POS &pos, Item *a) : Item_int_func(pos, a) {}

  bool itemize(Parse_context *pc, Item **res) override;
  const char *func_name() const override { return "sleep"; }
  /**
    This function is non-deterministic and hence depends on the
    'RAND' pseudo-table.

    @retval RAND_TABLE_BIT
  */
  table_map get_initial_pseudo_tables() const override {
    return RAND_TABLE_BIT;
  }
  bool check_function_as_value_generator(uchar *checker_args) override {
    Check_function_as_value_generator_parameters *func_arg =
        pointer_cast<Check_function_as_value_generator_parameters *>(
            checker_args);
    func_arg->banned_function_name = func_name();
    return true;
  }
  bool resolve_type(THD *thd) override {
    if (param_type_is_default(thd, 0, 1, MYSQL_TYPE_DOUBLE)) return true;
    return Item_int_func::resolve_type(thd);
  }
  longlong val_int() override;
  Item *pq_clone(THD *thd, Query_block *select) override;
};

class Item_udf_func : public Item_func {
  typedef Item_func super;

 protected:
  udf_handler udf;
  bool is_expensive_processor(uchar *) override { return true; }

 public:
  Item_udf_func(const POS &pos, udf_func *udf_arg, PT_item_list *opt_list)
      : Item_func(pos, opt_list), udf(udf_arg) {
    null_on_null = false;
  }
  ~Item_udf_func() override = default;

  bool itemize(Parse_context *pc, Item **res) override;
  const char *func_name() const override { return udf.name(); }
  enum Functype functype() const override { return UDF_FUNC; }
  bool fix_fields(THD *thd, Item **ref) override;
  void update_used_tables() override {
    /*
      TODO: Make a member in UDF_INIT and return if a UDF is deterministic or
      not.
      Currently UDF_INIT has a member (const_item) that is an in/out
      parameter to the init() call.
      The code in udf_handler::fix_fields also duplicates the arguments
      handling code in Item_func::fix_fields().

      The lack of information if a UDF is deterministic makes writing
      a correct update_used_tables() for UDFs impossible.
      One solution to this would be :
       - Add a is_deterministic member of UDF_INIT
       - (optionally) deprecate the const_item member of UDF_INIT
       - Take away the duplicate code from udf_handler::fix_fields() and
         make Item_udf_func call Item_func::fix_fields() to process its
         arguments as for any other function.
       - Store the deterministic flag returned by <udf>_init into the
       udf_handler.
       - Don't implement Item_udf_func::fix_fields, implement
       Item_udf_func::resolve_type() instead (similar to non-UDF functions).
       - Override Item_func::update_used_tables to call
       Item_func::update_used_tables() and add a RAND_TABLE_BIT to the
       result of Item_func::update_used_tables() if the UDF is
       non-deterministic.
       - (optionally) rename RAND_TABLE_BIT to NONDETERMINISTIC_BIT to
       better describe its usage.

      The above would require a change of the UDF API.
      Until that change is done here's how the current code works:
      We call Item_func::update_used_tables() only when we know that
      the function depends on real non-const tables and is deterministic.
      This can be done only because we know that the optimizer will
      call update_used_tables() only when there's possibly a new const
      table. So update_used_tables() can only make a Item_func more
      constant than it is currently.
      That's why we don't need to do anything if a function is guaranteed
      to return non-constant (it's non-deterministic) or is already a
      const.
    */
    if ((used_tables_cache & ~PSEUDO_TABLE_BITS) &&
        !(used_tables_cache & RAND_TABLE_BIT))
      Item_func::update_used_tables();

    not_null_tables_cache = 0;
    assert(!null_on_null);  // no need to update not_null_tables_cache
  }
  void cleanup() override;
  Item_result result_type() const override { return udf.result_type(); }
  bool is_expensive() override { return true; }
  void print(const THD *thd, String *str,
             enum_query_type query_type) const override;

  bool check_function_as_value_generator(uchar *checker_args) override {
    Check_function_as_value_generator_parameters *func_arg =
        pointer_cast<Check_function_as_value_generator_parameters *>(
            checker_args);
    func_arg->banned_function_name = func_name();
    return true;
  }

 protected:
  bool may_have_named_parameters() const override { return true; }
};

class Item_func_udf_float final : public Item_udf_func {
 public:
  Item_func_udf_float(const POS &pos, udf_func *udf_arg, PT_item_list *opt_list)
      : Item_udf_func(pos, udf_arg, opt_list) {}
  longlong val_int() override {
    assert(fixed == 1);
    return (longlong)rint(Item_func_udf_float::val_real());
  }
  my_decimal *val_decimal(my_decimal *dec_buf) override {
    double res = val_real();
    if (null_value) return nullptr;
    double2my_decimal(E_DEC_FATAL_ERROR, res, dec_buf);
    return dec_buf;
  }
  double val_real() override;
  String *val_str(String *str) override;
  bool get_date(MYSQL_TIME *ltime, my_time_flags_t fuzzydate) override {
    return get_date_from_real(ltime, fuzzydate);
  }
  bool get_time(MYSQL_TIME *ltime) override {
    return get_time_from_real(ltime);
  }
  bool resolve_type(THD *) override {
    set_data_type_double();
    fix_num_length_and_dec();  // @todo - needed?
    return false;
  }
};

class Item_func_udf_int final : public Item_udf_func {
 public:
  Item_func_udf_int(const POS &pos, udf_func *udf_arg, PT_item_list *opt_list)
      : Item_udf_func(pos, udf_arg, opt_list) {}
  longlong val_int() override;
  double val_real() override {
    return static_cast<double>(Item_func_udf_int::val_int());
  }
  String *val_str(String *str) override;
  bool get_date(MYSQL_TIME *ltime, my_time_flags_t fuzzydate) override {
    return get_date_from_int(ltime, fuzzydate);
  }
  bool get_time(MYSQL_TIME *ltime) override { return get_time_from_int(ltime); }
  enum Item_result result_type() const override { return INT_RESULT; }
  bool resolve_type(THD *) override {
    set_data_type_longlong();
    return false;
  }
};

class Item_func_udf_decimal : public Item_udf_func {
 public:
  Item_func_udf_decimal(const POS &pos, udf_func *udf_arg,
                        PT_item_list *opt_list)
      : Item_udf_func(pos, udf_arg, opt_list) {}
  longlong val_int() override;
  double val_real() override;
  my_decimal *val_decimal(my_decimal *) override;
  String *val_str(String *str) override;
  bool get_date(MYSQL_TIME *ltime, my_time_flags_t fuzzydate) override {
    return get_date_from_decimal(ltime, fuzzydate);
  }
  bool get_time(MYSQL_TIME *ltime) override {
    return get_time_from_decimal(ltime);
  }
  enum Item_result result_type() const override { return DECIMAL_RESULT; }
  bool resolve_type(THD *thd) override;
};

class Item_func_udf_str : public Item_udf_func {
 public:
  Item_func_udf_str(const POS &pos, udf_func *udf_arg, PT_item_list *opt_list)
      : Item_udf_func(pos, udf_arg, opt_list) {}

  String *val_str(String *) override;
  double val_real() override {
    int err_not_used;
    const char *end_not_used;
    String *res;
    res = val_str(&str_value);
    return res ? my_strntod(res->charset(), res->ptr(), res->length(),
                            &end_not_used, &err_not_used)
               : 0.0;
  }
  longlong val_int() override {
    int err_not_used;
    String *res;
    res = val_str(&str_value);
    return res ? my_strntoll(res->charset(), res->ptr(), res->length(), 10,
                             nullptr, &err_not_used)
               : (longlong)0;
  }
  my_decimal *val_decimal(my_decimal *dec_buf) override {
    String *res = val_str(&str_value);
    if (!res) return nullptr;
    str2my_decimal(E_DEC_FATAL_ERROR, res->ptr(), res->length(), res->charset(),
                   dec_buf);
    return dec_buf;
  }
  bool get_date(MYSQL_TIME *ltime, my_time_flags_t fuzzydate) override {
    return get_date_from_string(ltime, fuzzydate);
  }
  bool get_time(MYSQL_TIME *ltime) override {
    return get_time_from_string(ltime);
  }
  enum Item_result result_type() const override { return STRING_RESULT; }
  bool resolve_type(THD *thd) override;
};

void mysql_ull_cleanup(THD *thd);
void mysql_ull_set_explicit_lock_duration(THD *thd);

class Item_func_get_lock final : public Item_int_func {
  typedef Item_int_func super;

  String value;

 public:
  Item_func_get_lock(const POS &pos, Item *a, Item *b)
      : Item_int_func(pos, a, b) {}

  bool itemize(Parse_context *pc, Item **res) override;
  longlong val_int() override;
  const char *func_name() const override { return "get_lock"; }
  bool resolve_type(THD *thd) override {
    if (param_type_is_default(thd, 0, 1)) return true;
    if (param_type_is_default(thd, 1, 2, MYSQL_TYPE_LONGLONG)) return true;
    max_length = 1;
    set_nullable(true);
    return false;
  }
  bool is_non_const_over_literals(uchar *) override { return true; }
  bool check_function_as_value_generator(uchar *checker_args) override {
    Check_function_as_value_generator_parameters *func_arg =
        pointer_cast<Check_function_as_value_generator_parameters *>(
            checker_args);
    func_arg->banned_function_name = func_name();
    return true;
  }
  uint decimal_precision() const override { return max_length; }
  Item *pq_clone(THD *thd, Query_block *select) override;
};

class Item_func_release_lock final : public Item_int_func {
  typedef Item_int_func super;

  String value;

 public:
  Item_func_release_lock(const POS &pos, Item *a) : Item_int_func(pos, a) {
    set_has_notsupported_func_true();
  }
  bool itemize(Parse_context *pc, Item **res) override;

  longlong val_int() override;
  const char *func_name() const override { return "release_lock"; }
  bool resolve_type(THD *thd) override {
    if (param_type_is_default(thd, 0, 1)) return true;
    max_length = 1;
    set_nullable(true);
    return false;
  }
  bool is_non_const_over_literals(uchar *) override { return true; }
  bool check_function_as_value_generator(uchar *checker_args) override {
    Check_function_as_value_generator_parameters *func_arg =
        pointer_cast<Check_function_as_value_generator_parameters *>(
            checker_args);
    func_arg->banned_function_name = func_name();
    return true;
  }
};

class Item_func_release_all_locks final : public Item_int_func {
  typedef Item_int_func super;

 public:
  explicit Item_func_release_all_locks(const POS &pos) : Item_int_func(pos) {}
  bool itemize(Parse_context *pc, Item **res) override;

  longlong val_int() override;
  const char *func_name() const override { return "release_all_locks"; }
  bool resolve_type(THD *) override {
    unsigned_flag = true;
    return false;
  }
  bool is_non_const_over_literals(uchar *) override { return true; }
  bool check_function_as_value_generator(uchar *checker_args) override {
    Check_function_as_value_generator_parameters *func_arg =
        pointer_cast<Check_function_as_value_generator_parameters *>(
            checker_args);
    func_arg->banned_function_name = func_name();
    return true;
  }
};

/* replication functions */

class Item_source_pos_wait : public Item_int_func {
  typedef Item_int_func super;
  String value;

 public:
  Item_source_pos_wait(const POS &pos, Item *a, Item *b)
      : Item_int_func(pos, a, b) {}
  Item_source_pos_wait(const POS &pos, Item *a, Item *b, Item *c)
      : Item_int_func(pos, a, b, c) {}
  Item_source_pos_wait(const POS &pos, Item *a, Item *b, Item *c, Item *d)
      : Item_int_func(pos, a, b, c, d) {}

  bool itemize(Parse_context *pc, Item **res) override;
  longlong val_int() override;
  const char *func_name() const override { return "source_pos_wait"; }
  bool resolve_type(THD *thd) override {
    if (param_type_is_default(thd, 0, 1)) return true;
    if (param_type_is_default(thd, 1, 3, MYSQL_TYPE_LONGLONG)) return true;
    if (param_type_is_default(thd, 3, 4)) return true;
    max_length = 21;
    set_nullable(true);
    return false;
  }
  bool check_function_as_value_generator(uchar *checker_args) override {
    Check_function_as_value_generator_parameters *func_arg =
        pointer_cast<Check_function_as_value_generator_parameters *>(
            checker_args);
    func_arg->banned_function_name = func_name();
    return true;
  }
};

class Item_master_pos_wait : public Item_source_pos_wait {
 public:
  Item_master_pos_wait(const POS &pos, Item *a, Item *b)
      : Item_source_pos_wait(pos, a, b) {}
  Item_master_pos_wait(const POS &pos, Item *a, Item *b, Item *c)
      : Item_source_pos_wait(pos, a, b, c) {}
  Item_master_pos_wait(const POS &pos, Item *a, Item *b, Item *c, Item *d)
      : Item_source_pos_wait(pos, a, b, c, d) {}
  longlong val_int() override;
};

/**
 Internal functions used by INFORMATION_SCHEMA implementation to check
 if user have access to given database/table/column.
*/

class Item_func_can_access_database : public Item_int_func {
 public:
  Item_func_can_access_database(const POS &pos, Item *a)
      : Item_int_func(pos, a) {}
  longlong val_int() override;
  const char *func_name() const override { return "can_access_database"; }
  bool resolve_type(THD *) override {
    set_nullable(true);
    return false;
  }
};

class Item_func_can_access_table : public Item_int_func {
 public:
  Item_func_can_access_table(const POS &pos, Item *a, Item *b)
      : Item_int_func(pos, a, b) {}
  longlong val_int() override;
  const char *func_name() const override { return "can_access_table"; }
  bool resolve_type(THD *) override {
    set_nullable(true);
    return false;
  }
  Item *pq_clone(THD *thd, Query_block *select) override;
};

class Item_func_can_access_user : public Item_int_func {
 public:
  Item_func_can_access_user(const POS &pos, Item *a, Item *b)
      : Item_int_func(pos, a, b) {}
  longlong val_int() override;
  const char *func_name() const override { return "can_access_user"; }
  bool resolve_type(THD *) override {
    set_nullable(true);
    return false;
  }
};

class Item_func_can_access_trigger : public Item_int_func {
 public:
  Item_func_can_access_trigger(const POS &pos, Item *a, Item *b)
      : Item_int_func(pos, a, b) {}
  longlong val_int() override;
  const char *func_name() const override { return "can_access_trigger"; }
  bool resolve_type(THD *) override {
    max_length = 4;
    set_nullable(true);
    return false;
  }
};

class Item_func_can_access_routine : public Item_int_func {
 public:
  Item_func_can_access_routine(const POS &pos, PT_item_list *list)
      : Item_int_func(pos, list) {}
  longlong val_int() override;
  const char *func_name() const override { return "can_access_routine"; }
  bool resolve_type(THD *) override {
    max_length = 4;
    set_nullable(true);
    return false;
  }
};

class Item_func_can_access_event : public Item_int_func {
 public:
  Item_func_can_access_event(const POS &pos, Item *a) : Item_int_func(pos, a) {}
  longlong val_int() override;
  const char *func_name() const override { return "can_access_event"; }
  bool resolve_type(THD *) override {
    set_nullable(true);
    return false;
  }
};

class Item_func_can_access_resource_group : public Item_int_func {
 public:
  Item_func_can_access_resource_group(const POS &pos, Item *a)
      : Item_int_func(pos, a) {}
  longlong val_int() override;
  const char *func_name() const override { return "can_access_resource_group"; }
  bool resolve_type(THD *) override {
    max_length = 1;  // Function can return 0 or 1.
    set_nullable(true);
    return false;
  }
};

class Item_func_can_access_view : public Item_int_func {
 public:
  Item_func_can_access_view(const POS &pos, Item *a, Item *b, Item *c, Item *d)
      : Item_int_func(pos, a, b, c, d) {}
  longlong val_int() override;
  const char *func_name() const override { return "can_access_view"; }
  bool resolve_type(THD *) override {
    set_nullable(true);
    return false;
  }
};

class Item_func_can_access_column : public Item_int_func {
 public:
  Item_func_can_access_column(const POS &pos, Item *a, Item *b, Item *c)
      : Item_int_func(pos, a, b, c) {}
  longlong val_int() override;
  const char *func_name() const override { return "can_access_column"; }
  bool resolve_type(THD *) override {
    set_nullable(true);
    return false;
  }
};

class Item_func_is_visible_dd_object : public Item_int_func {
 public:
  Item_func_is_visible_dd_object(const POS &pos, Item *a)
      : Item_int_func(pos, a) {}
  Item_func_is_visible_dd_object(const POS &pos, Item *a, Item *b)
      : Item_int_func(pos, a, b) {}
  Item_func_is_visible_dd_object(const POS &pos, Item *a, Item *b, Item *c)
      : Item_int_func(pos, a, b, c) {}
  longlong val_int() override;
  const char *func_name() const override { return "is_visible_dd_object"; }
  bool resolve_type(THD *) override {
    max_length = 1;
    set_nullable(true);
    return false;
  }

  Item *pq_clone(THD *thd, Query_block *select) override;
};

class Item_func_internal_table_rows : public Item_int_func {
 public:
  Item_func_internal_table_rows(const POS &pos, PT_item_list *list)
      : Item_int_func(pos, list) {}
  enum Functype functype() const override { return DD_INTERNAL_FUNC; }
  longlong val_int() override;
  const char *func_name() const override { return "internal_table_rows"; }
  bool resolve_type(THD *) override {
    set_nullable(true);
    unsigned_flag = true;
    null_on_null = false;
    return false;
  }

  Item *pq_clone(THD *thd, Query_block *select) override;
};

class Item_func_internal_avg_row_length : public Item_int_func {
 public:
  Item_func_internal_avg_row_length(const POS &pos, PT_item_list *list)
      : Item_int_func(pos, list) {}
  enum Functype functype() const override { return DD_INTERNAL_FUNC; }
  longlong val_int() override;
  const char *func_name() const override { return "internal_avg_row_length"; }
  bool resolve_type(THD *) override {
    set_nullable(true);
    unsigned_flag = true;
    null_on_null = false;
    return false;
  }

  Item *pq_clone(THD *thd, Query_block *select) override;
};

class Item_func_internal_data_length : public Item_int_func {
 public:
  Item_func_internal_data_length(const POS &pos, PT_item_list *list)
      : Item_int_func(pos, list) {}
  enum Functype functype() const override { return DD_INTERNAL_FUNC; }
  longlong val_int() override;
  const char *func_name() const override { return "internal_data_length"; }
  bool resolve_type(THD *) override {
    set_nullable(true);
    unsigned_flag = true;
    null_on_null = false;
    return false;
  }

  Item *pq_clone(THD *thd, Query_block *select) override;
};

class Item_func_internal_max_data_length : public Item_int_func {
 public:
  Item_func_internal_max_data_length(const POS &pos, PT_item_list *list)
      : Item_int_func(pos, list) {}
  enum Functype functype() const override { return DD_INTERNAL_FUNC; }
  longlong val_int() override;
  const char *func_name() const override { return "internal_max_data_length"; }
  bool resolve_type(THD *) override {
    set_nullable(true);
    unsigned_flag = true;
    null_on_null = false;
    return false;
  }

  Item *pq_clone(THD *thd, Query_block *select) override;
};

class Item_func_internal_index_length : public Item_int_func {
 public:
  Item_func_internal_index_length(const POS &pos, PT_item_list *list)
      : Item_int_func(pos, list) {}
  enum Functype functype() const override { return DD_INTERNAL_FUNC; }
  longlong val_int() override;
  const char *func_name() const override { return "internal_index_length"; }
  bool resolve_type(THD *) override {
    set_nullable(true);
    unsigned_flag = true;
    null_on_null = false;
    return false;
  }

  Item *pq_clone(THD *thd, Query_block *select) override;
};

class Item_func_internal_data_free : public Item_int_func {
 public:
  Item_func_internal_data_free(const POS &pos, PT_item_list *list)
      : Item_int_func(pos, list) {}
  enum Functype functype() const override { return DD_INTERNAL_FUNC; }
  longlong val_int() override;
  const char *func_name() const override { return "internal_data_free"; }
  bool resolve_type(THD *) override {
    set_nullable(true);
    unsigned_flag = true;
    null_on_null = false;
    return false;
  }

  Item *pq_clone(THD *thd, Query_block *select) override;
};

class Item_func_internal_auto_increment : public Item_int_func {
 public:
  Item_func_internal_auto_increment(const POS &pos, PT_item_list *list)
      : Item_int_func(pos, list) {}
  enum Functype functype() const override { return DD_INTERNAL_FUNC; }
  longlong val_int() override;
  const char *func_name() const override { return "internal_auto_increment"; }
  bool resolve_type(THD *) override {
    set_nullable(true);
    unsigned_flag = true;
    null_on_null = false;
    return false;
  }

  Item *pq_clone(THD *thd, Query_block *select) override;
};

class Item_func_internal_checksum : public Item_int_func {
 public:
  Item_func_internal_checksum(const POS &pos, PT_item_list *list)
      : Item_int_func(pos, list) {}
  enum Functype functype() const override { return DD_INTERNAL_FUNC; }
  longlong val_int() override;
  const char *func_name() const override { return "internal_checksum"; }
  bool resolve_type(THD *) override {
    set_nullable(true);
    null_on_null = false;
    return false;
  }

  Item *pq_clone(THD *thd, Query_block *select) override;
};

class Item_func_internal_keys_disabled : public Item_int_func {
 public:
  Item_func_internal_keys_disabled(const POS &pos, Item *a)
      : Item_int_func(pos, a) {}
  longlong val_int() override;
  const char *func_name() const override { return "internal_keys_disabled"; }
  bool resolve_type(THD *) override {
    set_nullable(false);
    null_on_null = false;
    return false;
  }
};

class Item_func_internal_index_column_cardinality : public Item_int_func {
 public:
  Item_func_internal_index_column_cardinality(const POS &pos,
                                              PT_item_list *list)
      : Item_int_func(pos, list) {}
  enum Functype functype() const override { return DD_INTERNAL_FUNC; }
  longlong val_int() override;
  const char *func_name() const override {
    return "internal_index_column_cardinality";
  }
  bool resolve_type(THD *) override {
    set_nullable(true);
    null_on_null = false;
    return false;
  }
};

class Item_func_internal_dd_char_length final : public Item_int_func {
 public:
  Item_func_internal_dd_char_length(const POS &pos, Item *a, Item *b, Item *c,
                                    Item *d)
      : Item_int_func(pos, a, b, c, d) {}
  longlong val_int() override;
  const char *func_name() const override { return "internal_dd_char_length"; }
  bool resolve_type(THD *) override {
    set_nullable(true);
    null_on_null = false;
    return false;
  }
};

class Item_func_internal_get_view_warning_or_error final
    : public Item_int_func {
 public:
  Item_func_internal_get_view_warning_or_error(const POS &pos,
                                               PT_item_list *list)
      : Item_int_func(pos, list) {}
  longlong val_int() override;
  const char *func_name() const override {
    return "internal_get_view_warning_or_error";
  }
  bool resolve_type(THD *) override {
    max_length = 1;
    set_nullable(false);
    null_on_null = false;
    return false;
  }
};

class Item_func_get_dd_index_sub_part_length final : public Item_int_func {
 public:
  Item_func_get_dd_index_sub_part_length(const POS &pos, PT_item_list *list)
      : Item_int_func(pos, list) {}
  longlong val_int() override;
  bool resolve_type(THD *) override {
    set_nullable(true);
    null_on_null = false;
    return false;
  }
  const char *func_name() const override {
    return "get_dd_index_sub_part_length";
  }
};

class Item_func_internal_tablespace_id : public Item_int_func {
 public:
  Item_func_internal_tablespace_id(const POS &pos, Item *a, Item *b, Item *c,
                                   Item *d)
      : Item_int_func(pos, a, b, c, d) {}
  enum Functype functype() const override { return DD_INTERNAL_FUNC; }
  longlong val_int() override;
  const char *func_name() const override { return "internal_tablespace_id"; }
  bool resolve_type(THD *) override {
    set_nullable(true);
    null_on_null = false;
    return false;
  }
};

class Item_func_internal_tablespace_logfile_group_number
    : public Item_int_func {
 public:
  Item_func_internal_tablespace_logfile_group_number(const POS &pos, Item *a,
                                                     Item *b, Item *c, Item *d)
      : Item_int_func(pos, a, b, c, d) {}

  enum Functype functype() const override { return DD_INTERNAL_FUNC; }
  longlong val_int() override;

  const char *func_name() const override {
    return "internal_tablespace_logfile_group_number";
  }

  bool resolve_type(THD *) override {
    set_nullable(true);
    null_on_null = false;
    return false;
  }
};

class Item_func_internal_tablespace_free_extents : public Item_int_func {
 public:
  Item_func_internal_tablespace_free_extents(const POS &pos, Item *a, Item *b,
                                             Item *c, Item *d)
      : Item_int_func(pos, a, b, c, d) {}

  enum Functype functype() const override { return DD_INTERNAL_FUNC; }
  longlong val_int() override;

  const char *func_name() const override {
    return "internal_tablespace_free_extents";
  }

  bool resolve_type(THD *) override {
    set_nullable(true);
    null_on_null = false;
    return false;
  }
};

class Item_func_internal_tablespace_total_extents : public Item_int_func {
 public:
  Item_func_internal_tablespace_total_extents(const POS &pos, Item *a, Item *b,
                                              Item *c, Item *d)
      : Item_int_func(pos, a, b, c, d) {}

  enum Functype functype() const override { return DD_INTERNAL_FUNC; }
  longlong val_int() override;

  const char *func_name() const override {
    return "internal_tablespace_total_extents";
  }

  bool resolve_type(THD *) override {
    set_nullable(true);
    null_on_null = false;
    return false;
  }
};

class Item_func_internal_tablespace_extent_size : public Item_int_func {
 public:
  Item_func_internal_tablespace_extent_size(const POS &pos, Item *a, Item *b,
                                            Item *c, Item *d)
      : Item_int_func(pos, a, b, c, d) {}

  enum Functype functype() const override { return DD_INTERNAL_FUNC; }
  longlong val_int() override;

  const char *func_name() const override {
    return "internal_tablespace_extent_size";
  }

  bool resolve_type(THD *) override {
    set_nullable(true);
    null_on_null = false;
    return false;
  }
};

class Item_func_internal_tablespace_initial_size : public Item_int_func {
 public:
  Item_func_internal_tablespace_initial_size(const POS &pos, Item *a, Item *b,
                                             Item *c, Item *d)
      : Item_int_func(pos, a, b, c, d) {}

  enum Functype functype() const override { return DD_INTERNAL_FUNC; }
  longlong val_int() override;

  const char *func_name() const override {
    return "internal_tablespace_initial_size";
  }

  bool resolve_type(THD *) override {
    set_nullable(true);
    null_on_null = false;
    return false;
  }
};

class Item_func_internal_tablespace_maximum_size : public Item_int_func {
 public:
  Item_func_internal_tablespace_maximum_size(const POS &pos, Item *a, Item *b,
                                             Item *c, Item *d)
      : Item_int_func(pos, a, b, c, d) {}

  enum Functype functype() const override { return DD_INTERNAL_FUNC; }
  longlong val_int() override;

  const char *func_name() const override {
    return "internal_tablespace_maximum_size";
  }

  bool resolve_type(THD *) override {
    set_nullable(true);
    null_on_null = false;
    return false;
  }
};

class Item_func_internal_tablespace_autoextend_size : public Item_int_func {
 public:
  Item_func_internal_tablespace_autoextend_size(const POS &pos, Item *a,
                                                Item *b, Item *c, Item *d)
      : Item_int_func(pos, a, b, c, d) {}

  enum Functype functype() const override { return DD_INTERNAL_FUNC; }
  longlong val_int() override;

  const char *func_name() const override {
    return "internal_tablespace_autoextend_size";
  }

  bool resolve_type(THD *) override {
    set_nullable(true);
    null_on_null = false;
    return false;
  }
};

class Item_func_internal_tablespace_version : public Item_int_func {
 public:
  Item_func_internal_tablespace_version(const POS &pos, Item *a, Item *b,
                                        Item *c, Item *d)
      : Item_int_func(pos, a, b, c, d) {}

  enum Functype functype() const override { return DD_INTERNAL_FUNC; }
  longlong val_int() override;

  const char *func_name() const override {
    return "internal_tablespace_version";
  }

  bool resolve_type(THD *) override {
    set_nullable(true);
    null_on_null = false;
    return false;
  }
};

class Item_func_internal_tablespace_data_free : public Item_int_func {
 public:
  Item_func_internal_tablespace_data_free(const POS &pos, Item *a, Item *b,
                                          Item *c, Item *d)
      : Item_int_func(pos, a, b, c, d) {}

  enum Functype functype() const override { return DD_INTERNAL_FUNC; }
  longlong val_int() override;

  const char *func_name() const override {
    return "internal_tablespace_data_free";
  }

  bool resolve_type(THD *) override {
    set_nullable(true);
    null_on_null = false;
    return false;
  }
};

/**
  Common class for:
    Item_func_get_system_var
    Item_func_get_user_var
    Item_func_set_user_var
*/
class Item_var_func : public Item_func {
 public:
  Item_var_func() : Item_func() {}
  explicit Item_var_func(const POS &pos) : Item_func(pos) {}

  Item_var_func(THD *thd, Item_var_func *item) : Item_func(thd, item) {}

  Item_var_func(Item *a) : Item_func(a) {}
  Item_var_func(const POS &pos, Item *a) : Item_func(pos, a) {}

  bool get_date(MYSQL_TIME *ltime, my_time_flags_t fuzzydate) override {
    return get_date_from_non_temporal(ltime, fuzzydate);
  }
  bool get_time(MYSQL_TIME *ltime) override {
    return get_time_from_non_temporal(ltime);
  }
  bool check_function_as_value_generator(uchar *checker_args) override {
    Check_function_as_value_generator_parameters *func_arg =
        pointer_cast<Check_function_as_value_generator_parameters *>(
            checker_args);
    func_arg->err_code = (func_arg->source == VGS_CHECK_CONSTRAINT)
                             ? ER_CHECK_CONSTRAINT_VARIABLES
                             : ER_DEFAULT_VAL_GENERATED_VARIABLES;
    return true;
  }
};

/* Handling of user definable variables */

// this is needed for user_vars hash
class user_var_entry {
  void reset_value() {
    m_ptr = nullptr;
    m_length = 0;
  }
  void set_value(char *value, size_t length) {
    m_ptr = value;
    m_length = length;
  }

  /**
    Position inside a user_var_entry where small values are stored:
    double values, longlong values and string values with length
    up to extra_size (should be 8 bytes on all platforms).
    String values with length longer than 8 are stored in a separate
    memory buffer, which is allocated when needed using the method realloc().
  */
  char *internal_buffer_ptr() {
    return pointer_cast<char *>(this) + ALIGN_SIZE(sizeof(user_var_entry));
  }

  /**
    Position inside a user_var_entry where a null-terminates array
    of characters representing the variable name is stored.
  */
  char *name_ptr() { return internal_buffer_ptr() + extra_size; }

  /**
    Initialize m_ptr to the internal buffer (if the value is small enough),
    or allocate a separate buffer.
    @param length - length of the value to be stored.
  */
  bool mem_realloc(size_t length);

  /**
    Check if m_ptr points to an external buffer previously allocated by
    realloc().
    @retval true  - an external buffer is allocated.
    @retval false - m_ptr is null, or points to the internal buffer.
  */
  bool alloced() { return m_ptr && m_ptr != internal_buffer_ptr(); }

  /**
    Free the external value buffer, if it's allocated.
  */
  void free_value() {
    if (alloced()) my_free(m_ptr);
  }

  /**
    Copy the array of characters from the given name into the internal
    name buffer and initialize entry_name to point to it.
  */
  void copy_name(const Simple_cstring &name) {
    name.strcpy(name_ptr());
    entry_name = Name_string(name_ptr(), name.length());
  }

  /**
    Initialize all members

    @param thd    Current session.
    @param name    Name of the user_var_entry instance.
    @param cs      charset information of the user_var_entry instance.
  */
  void init(THD *thd, const Simple_cstring &name, const CHARSET_INFO *cs);

  /**
    Store a value of the given type into a user_var_entry instance.
    @param from    Value
    @param length  Size of the value
    @param type    type
    @retval        false on success
    @retval        true on memory allocation error
  */
  bool store(const void *from, size_t length, Item_result type);

  /**
    Assert the user variable is locked.
    This is debug code only.
    The thread LOCK_thd_data mutex protects:
    - the thd->user_vars hash itself
    - the values in the user variable itself.
    The protection is required for monitoring,
    as a different thread can inspect this session
    user variables, on a live session.
  */
  void assert_locked() const;

  static const size_t extra_size = sizeof(double);
  char *m_ptr;         ///< Value
  size_t m_length;     ///< Value length
  Item_result m_type;  ///< Value type
  THD *m_owner;
  /**
    Set to the id of the most recent query that has used the variable.
    Used in binlogging: When set, there is no need to add a reference to this
    variable to the binlog. Imagine it is this:

        INSERT INTO t SELECT @a:=10, @a:=@a+1.

    Then we have a Item_func_get_user_var (because of the `@a+1`) so we
    think we have to write the value of `@a` to the binlog. But before that,
    we have a Item_func_set_user_var to create `@a` (`@a:=10`), in this we mark
    the variable as "already logged" so that it won't be logged
    by Item_func_get_user_var (because that's not necessary).
  */
  query_id_t m_used_query_id;

 public:
  user_var_entry() = default; /* Remove gcc warning */

  THD *owner_session() const { return m_owner; }

  Simple_cstring entry_name;  // Variable name
  DTCollation collation;      // Collation with attributes
  bool unsigned_flag;         // true if unsigned, false if signed

  /**
    Set value to user variable.

    @param ptr            pointer to buffer with new value
    @param length         length of new value
    @param type           type of new value
    @param cs             charset info for new value
    @param dv             derivation for new value
    @param unsigned_arg   indicates if a value of type INT_RESULT is unsigned

    @note Sets error and fatal error if allocation fails.

    @retval
      false   success
    @retval
      true    failure
   */
  bool store(const void *ptr, size_t length, Item_result type,
             const CHARSET_INFO *cs, Derivation dv, bool unsigned_arg);
  /**
    Set type of to the given value.
    @param type  Data type.
  */
  void set_type(Item_result type) {
    assert_locked();
    m_type = type;
  }
  /**
    Set value to NULL
    @param type  Data type.
  */

  void set_null_value(Item_result type) {
    assert_locked();
    free_value();
    reset_value();
    m_type = type;
  }

  void set_used_query_id(query_id_t query_id) { m_used_query_id = query_id; }
  query_id_t used_query_id() const { return m_used_query_id; }

  /**
    Allocates and initializes a user variable instance.

    @param thd    Current session.
    @param name   Name of the variable.
    @param cs     Charset of the variable.

    @return Address of the allocated and initialized user_var_entry instance.
    @retval NULL On allocation error.
  */
  static user_var_entry *create(THD *thd, const Name_string &name,
                                const CHARSET_INFO *cs);

  /**
    Free all memory used by a user_var_entry instance
    previously created by create().
  */
  void destroy() {
    assert_locked();
    free_value();   // Free the external value buffer
    my_free(this);  // Free the instance itself
  }

  void lock();
  void unlock();

  /* Routines to access the value and its type */
  const char *ptr() const { return m_ptr; }
  size_t length() const { return m_length; }
  Item_result type() const { return m_type; }
  /* Item-alike routines to access the value */
  double val_real(bool *null_value) const;
  longlong val_int(bool *null_value) const;
  String *val_str(bool *null_value, String *str, uint decimals) const;
  my_decimal *val_decimal(bool *null_value, my_decimal *result) const;
};

/**
  This class is used to implement operations like
  SET \@variable or \@variable:= expression.
*/

class Item_func_set_user_var : public Item_var_func {
  enum Item_result cached_result_type;
  user_var_entry *entry{nullptr};
  String value;
  my_decimal decimal_buff;
  bool null_item;
  union {
    longlong vint;
    double vreal;
    String *vstr;
    my_decimal *vdec;
  } save_result;

 public:
  Name_string name;  // keep it public

  Item_func_set_user_var(Name_string a, Item *b) : Item_var_func(b), name(a) {}
  Item_func_set_user_var(const POS &pos, Name_string a, Item *b)
      : Item_var_func(pos, b), name(a) {}

  Item_func_set_user_var(THD *thd, Item_func_set_user_var *item)
      : Item_var_func(thd, item),
        cached_result_type(item->cached_result_type),
        entry(item->entry),
        value(item->value),
        decimal_buff(item->decimal_buff),
        null_item(item->null_item),
        save_result(item->save_result),
        name(item->name) {}
  enum Functype functype() const override { return SUSERVAR_FUNC; }
  double val_real() override;
  longlong val_int() override;
  String *val_str(String *str) override;
  my_decimal *val_decimal(my_decimal *) override;
  bool update_hash(const void *ptr, uint length, enum Item_result type,
                   const CHARSET_INFO *cs, Derivation dv, bool unsigned_arg);
  bool send(Protocol *protocol, String *str_arg) override;
  void make_field(Send_field *tmp_field) override;
  bool check(bool use_result_field);
  void save_item_result(Item *item);
  bool update();
  enum Item_result result_type() const override { return cached_result_type; }
  bool fix_fields(THD *thd, Item **ref) override;
  bool resolve_type(THD *) override;
  void print(const THD *thd, String *str,
             enum_query_type query_type) const override;
  void print_assignment(const THD *thd, String *str,
                        enum_query_type query_type) const;
  const char *func_name() const override { return "set_user_var"; }

  type_conversion_status save_in_field(Field *field, bool no_conversions,
                                       bool can_use_result_field);

  void save_org_in_field(Field *field) override {
    save_in_field(field, true, false);
  }

  bool set_entry(THD *thd, bool create_if_not_exists);
  void cleanup() override;

 protected:
  type_conversion_status save_in_field_inner(Field *field,
                                             bool no_conversions) override {
    return save_in_field(field, no_conversions, true);
  }
};

class Item_func_get_user_var : public Item_var_func,
                               private Settable_routine_parameter {
  user_var_entry *var_entry;
  Item_result m_cached_result_type;

 public:
  Name_string name;  // keep it public

  Item_func_get_user_var(Name_string a)
      : Item_var_func(), m_cached_result_type(STRING_RESULT), name(a) {}
  Item_func_get_user_var(const POS &pos, Name_string a)
      : Item_var_func(pos), m_cached_result_type(STRING_RESULT), name(a) {}

  enum Functype functype() const override { return GUSERVAR_FUNC; }
  double val_real() override;
  longlong val_int() override;
  my_decimal *val_decimal(my_decimal *) override;
  String *val_str(String *str) override;
  const CHARSET_INFO *charset_for_protocol() override;
  bool resolve_type(THD *) override;
  bool propagate_type(THD *thd, const Type_properties &type) override;
  void cleanup() override;
  void update_used_tables() override {}  // Keep existing used tables
  void print(const THD *thd, String *str,
             enum_query_type query_type) const override;
  enum Item_result result_type() const override;
  /*
    We must always return variables as strings to guard against selects of type
    select @t1:=1,@t1,@t:="hello",@t from foo where (@t1:= t2.b)
  */
  const char *func_name() const override { return "get_user_var"; }
  bool is_non_const_over_literals(uchar *) override { return true; }
  bool eq(const Item *item, bool binary_cmp) const override;

  bool collect_session_vars(uchar *arg) override {
    auto collection = pointer_cast<Collect_session_vars *>(arg);
    for (auto var_ptr : collection->user_vars) {
      if (name.eq(var_ptr->name)) return 0;  // rm duplicate
    }
    collection->user_vars.push_back(this);
    return false;
  }

 private:
  bool set_value(THD *thd, sp_rcontext *ctx, Item **it) override;

 public:
  Settable_routine_parameter *get_settable_routine_parameter() override {
    return this;
  }

  Item *pq_clone(THD *thd, Query_block *select) override;
  bool pq_copy_from(THD *thd, Query_block *select, Item *item) override;
};

/*
  This item represents user variable used as out parameter (e.g in LOAD DATA),
  and it is supposed to be used only for this purprose. So it is simplified
  a lot. Actually you should never obtain its value.

  The only two reasons for this thing being an Item is possibility to store it
  in const mem_root_deque<Item> and desire to place this code somewhere near
  other functions working with user variables.
*/
class Item_user_var_as_out_param : public Item {
  user_var_entry *entry;

 public:
  Name_string name;  // keep it public(used for parallel load)
  Item_user_var_as_out_param(const POS &pos, Name_string a)
      : Item(pos), name(a) {
    item_name.copy(a);
  }
  /* We should return something different from FIELD_ITEM here */
  enum Type type() const override { return STRING_ITEM; }
  double val_real() override;
  longlong val_int() override;
  String *val_str(String *str) override;
  my_decimal *val_decimal(my_decimal *decimal_buffer) override;
  bool get_date(MYSQL_TIME *, my_time_flags_t) override {
    assert(0);
    return true;
  }
  bool get_time(MYSQL_TIME *) override {
    assert(0);
    return true;
  }

  /* fix_fields() binds variable name with its entry structure */
  bool fix_fields(THD *thd, Item **ref) override;
  void print(const THD *thd, String *str,
             enum_query_type query_type) const override;
  void set_null_value(const CHARSET_INFO *cs);
  void set_value(const char *str, size_t length, const CHARSET_INFO *cs);
};

/* A system variable */

#define GET_SYS_VAR_CACHE_LONG 1
#define GET_SYS_VAR_CACHE_DOUBLE 2
#define GET_SYS_VAR_CACHE_STRING 4

/** Class to log audit event MYSQL_AUDIT_GLOBAL_VARIABLE_GET. */
class Audit_global_variable_get_event {
 public:
  Audit_global_variable_get_event(THD *thd, Item_func_get_system_var *item,
                                  uchar cache_type);
  ~Audit_global_variable_get_event();

 private:
  // Thread handle.
  THD *m_thd;

  // Item_func_get_system_var instance.
  Item_func_get_system_var *m_item;

  /*
    Value conversion type.
    Depending on the value conversion type GET_SYS_VAR_CACHE_* is stored in this
    member while creating the object. While converting value if there are any
    intermediate conversions in the same query then this member is used to avoid
    auditing more than once.
  */
  uchar m_val_type;

  /*
    To indicate event auditing is required or not. Event is not audited if
      * scope of the variable is *not* GLOBAL.
      * or the event is already audited for global variable for the same query.
  */
  bool m_audit_event;
};

class Item_func_get_system_var final : public Item_var_func {
  const enum_var_type var_scope;
  longlong cached_llval;
  double cached_dval;
  String cached_strval;
  bool cached_null_value;
  query_id_t used_query_id;
  uchar cache_present;
  const System_variable_tracker var_tracker;

  template <typename T>
  longlong get_sys_var_safe(THD *thd, sys_var *var);

  friend class Audit_global_variable_get_event;

 public:
  Item_func_get_system_var(const System_variable_tracker &,
                           enum_var_type scope);
  enum Functype functype() const override { return GSYSVAR_FUNC; }
  table_map get_initial_pseudo_tables() const override {
    return INNER_TABLE_BIT;
  }
  bool resolve_type(THD *) override;
  void print(const THD *thd, String *str,
             enum_query_type query_type) const override;
  /* Add for GreatDB: Since print() use 'item_name' which is nullptr, the
     print() function can't output system_var name. So add a new function.
     Already report a bug to MySQL, see: Bug #104435 */
  void print_var_name(const THD *thd, String *str, enum_query_type query_type);
  bool is_non_const_over_literals(uchar *) override { return true; }
  enum Item_result result_type() const override {
    assert(fixed);
    return type_to_result(data_type());
  }
  double val_real() override;
  longlong val_int() override;
  String *val_str(String *) override;
  my_decimal *val_decimal(my_decimal *dec_buf) override {
    return val_decimal_from_real(dec_buf);
  }
  /* TODO: fix to support views */
  const char *func_name() const override { return "get_system_var"; }
  bool eq(const Item *item, bool binary_cmp) const override;
  bool is_valid_for_pushdown(uchar *arg [[maybe_unused]]) override {
    // Expressions which have system variables cannot be pushed as of
    // now because Item_func_get_system_var::print does not print the
    // original expression which leads to an incorrect clone.
    return true;
  }

  bool collect_session_vars(uchar *arg) override {
    auto collection = pointer_cast<Collect_session_vars *>(arg);
    /* skip global-only sys vars */
    if (var_scope == OPT_DEFAULT || var_scope == OPT_SESSION) {
      for (auto var_ptr : collection->sys_vars) {
        if (!strcasecmp(var_tracker.get_var_name(),
                        var_ptr->var_tracker.get_var_name()))
          return 0;  // rm duplicate
      }
      collection->sys_vars.push_back(this);
    }
    return false;
  }

  void cleanup() override;
  bool bind(THD *thd);

  Item *pq_clone(THD *thd, Query_block *select) override;
  bool pq_copy_from(THD *thd, Query_block *select, Item *item) override;
};

class JOIN;

class Item_func_match final : public Item_real_func {
  typedef Item_real_func super;

 public:
  Item *against;
  uint key, flags;
  /// True if we are doing a full-text index scan with this MATCH function as a
  /// predicate, and the score can be retrieved with get_relevance(). If it is
  /// false, the score of the document must be retrieved with find_relevance().
  bool score_from_index_scan{false};
  DTCollation cmp_collation;
  FT_INFO *ft_handler;
  Table_ref *table_ref;
  /**
     Master item means that if identical items are present in the
     statement, they use the same FT handler. FT handler is initialized
     only for master item and slave items just use it. FT hints initialized
     for master only, slave items HINTS are not accessed.
  */
  Item_func_match *master;
  Item *concat_ws;      // Item_func_concat_ws
  String value;         // value of concat_ws
  String search_value;  // key_item()'s value converted to cmp_collation

  /**
    Constructor for Item_func_match class.

    @param pos         Position of token in the parser.
    @param a           List of arguments.
    @param against_arg Expression to match against.
    @param b           FT Flags.
  */
  Item_func_match(const POS &pos, PT_item_list *a, Item *against_arg, uint b)
      : Item_real_func(pos, a),
        against(against_arg),
        key(0),
        flags(b),
        ft_handler(nullptr),
        table_ref(nullptr),
        master(nullptr),
        concat_ws(nullptr),
        hints(nullptr),
        simple_expression(false),
        used_in_where_only(false) {
    null_on_null = false;
  }

  bool itemize(Parse_context *pc, Item **res) override;

  void cleanup() override {
    DBUG_TRACE;
    Item_real_func::cleanup();
    if (master == nullptr && ft_handler != nullptr) {
      ft_handler->please->close_search(ft_handler);
    }
    score_from_index_scan = false;
    ft_handler = nullptr;
    concat_ws = nullptr;
    return;
  }
  Item *key_item() const override { return against; }
  enum Functype functype() const override { return FT_FUNC; }
  const char *func_name() const override { return "match"; }
  bool fix_fields(THD *thd, Item **ref) override;
  bool eq(const Item *, bool binary_cmp) const override;
  /* The following should be safe, even if we compare doubles */
  longlong val_int() override {
    assert(fixed);
    return val_real() != 0.0;
  }
  double val_real() override;
  void print(const THD *thd, String *str,
             enum_query_type query_type) const override;

  bool fix_index(const THD *thd);
  bool init_search(THD *thd);
  bool check_function_as_value_generator(uchar *checker_args) override {
    Check_function_as_value_generator_parameters *func_arg =
        pointer_cast<Check_function_as_value_generator_parameters *>(
            checker_args);
    func_arg->banned_function_name = func_name();
    return true;
  }

  /**
     Get number of matching rows from FT handler.

     @note Requires that FT handler supports the extended API

     @return Number of matching rows in result
   */
  ulonglong get_count() {
    assert(ft_handler);
    assert(table_ref->table->file->ha_table_flags() & HA_CAN_FULLTEXT_EXT);

    return ((FT_INFO_EXT *)ft_handler)
        ->could_you->count_matches((FT_INFO_EXT *)ft_handler);
  }

  /**
     Check whether FT result is ordered on rank

     @return true if result is ordered
     @return false otherwise
   */
  bool ordered_result() {
    assert(!master);
    if (hints->get_flags() & FT_SORTED) return true;

    if ((table_ref->table->file->ha_table_flags() & HA_CAN_FULLTEXT_EXT) == 0)
      return false;

    assert(ft_handler);
    return ((FT_INFO_EXT *)ft_handler)->could_you->get_flags() &
           FTS_ORDERED_RESULT;
  }

  /**
     Check whether FT result contains the document ID

     @return true if document ID is available
     @return false otherwise
   */
  bool docid_in_result() {
    assert(ft_handler);

    if ((table_ref->table->file->ha_table_flags() & HA_CAN_FULLTEXT_EXT) == 0)
      return false;

    return ((FT_INFO_EXT *)ft_handler)->could_you->get_flags() &
           FTS_DOCID_IN_RESULT;
  }

  float get_filtering_effect(THD *thd, table_map filter_for_table,
                             table_map read_tables,
                             const MY_BITMAP *fields_to_ignore,
                             double rows_in_table) override;

  /**
     Returns master MATCH function.

     @return pointer to master MATCH function.
  */
  Item_func_match *get_master() {
    if (master) return master->get_master();
    return this;
  }

  /**
     Set master MATCH function and adjust used_in_where_only value.

     @param item item for which master should be set.
  */
  void set_master(Item_func_match *item) {
    used_in_where_only &= item->used_in_where_only;
    item->master = this;
  }

  /**
     Returns pointer to Ft_hints object belonging to master MATCH function.

     @return pointer to Ft_hints object
  */
  Ft_hints *get_hints() {
    assert(!master);
    return hints;
  }

  /**
     Set comparison operation type and and value for master MATCH function.

     @param type   comparison operation type
     @param value_arg  comparison operation value
  */
  void set_hints_op(enum ft_operation type, double value_arg) {
    assert(!master);
    hints->set_hint_op(type, value_arg);
  }

  /**
     Set FT hints.
  */
  void set_hints(JOIN *join, uint ft_flag, ha_rows ft_limit, bool no_cond);

  /**
     Check if ranking is not needed.

     @return true if ranking is not needed
     @return false otherwise
  */
  bool can_skip_ranking() {
    assert(!master);
    return (!(hints->get_flags() & FT_SORTED) &&  // FT_SORTED is no set
            used_in_where_only &&                 // MATCH result is not used
                                                  // in expression
            hints->get_op_type() == FT_OP_NO);    // MATCH is single function
  }

  /**
     Set flag that the function is a simple expression.

     @param val true if the function is a simple expression, false otherwise
  */
  void set_simple_expression(bool val) {
    assert(!master);
    simple_expression = val;
  }

  /**
     Check if this MATCH function is a simple expression in WHERE condition.

     @return true if simple expression
     @return false otherwise
  */
  bool is_simple_expression() {
    assert(!master);
    return simple_expression;
  }

 private:
  /**
     Fulltext index hints, initialized for master MATCH function only.
  */
  Ft_hints *hints;
  /**
     Flag is true when MATCH function is used as a simple expression in
     WHERE condition, i.e. there is no AND/OR combinations, just simple
     MATCH function or [MATCH, rank] comparison operation.
  */
  bool simple_expression;
  /**
     true if MATCH function is used in WHERE condition only.
     Used to determine what hints can be used for FT handler.
     Note that only master MATCH function has valid value.
     it's ok since only master function is involved in the hint processing.
  */
  bool used_in_where_only;
  /**
     Check whether storage engine for given table,
     allows FTS Boolean search on non-indexed columns.

     @todo A flag should be added to the extended fulltext API so that
           it may be checked whether search on non-indexed columns are
           supported. Currently, it is not possible to check for such a
           flag since @c this->ft_handler is not yet set when this function is
           called.  The current hack is to assume that search on non-indexed
           columns are supported for engines that does not support the extended
           fulltext API (e.g., MyISAM), while it is not supported for other
           engines (e.g., InnoDB)

     @param tr Table for which storage engine to check

     @retval true if BOOLEAN search on non-indexed columns is supported
     @retval false otherwise
   */
  bool allows_search_on_non_indexed_columns(const TABLE *tr) {
    // Only Boolean search may support non_indexed columns
    if (!(flags & FT_BOOL)) return false;

    assert(tr && tr->file);

    // Assume that if extended fulltext API is not supported,
    // non-indexed columns are allowed.  This will be true for MyISAM.
    if ((tr->file->ha_table_flags() & HA_CAN_FULLTEXT_EXT) == 0) return true;

    return false;
  }
};

/**
  A visitor that calls the specified function on every non-aggregated full-text
  search function (Item_func_match) it encounters when it is used in a
  PREFIX+POSTFIX walk with WalkItem(). It skips every item that is wrapped in an
  aggregate function, and also every item wrapped in a reference, as the items
  behind the reference are already handled elsewhere (in another query block or
  in another element of the SELECT list).
 */
class NonAggregatedFullTextSearchVisitor : private Item_tree_walker {
 public:
  explicit NonAggregatedFullTextSearchVisitor(
      std::function<bool(Item_func_match *)> func);
  bool operator()(Item *item);

 private:
  std::function<bool(Item_func_match *)> m_func;
};

class Item_func_is_free_lock final : public Item_int_func {
  typedef Item_int_func super;

  String value;

 public:
  Item_func_is_free_lock(const POS &pos, Item *a) : Item_int_func(pos, a) {
    set_has_notsupported_func_true();
  }

  bool itemize(Parse_context *pc, Item **res) override;
  longlong val_int() override;
  const char *func_name() const override { return "is_free_lock"; }
  bool resolve_type(THD *thd) override {
    if (param_type_is_default(thd, 0, 1)) return true;
    max_length = 1;
    set_nullable(true);
    return false;
  }
  bool is_non_const_over_literals(uchar *) override { return true; }
  bool check_function_as_value_generator(uchar *checker_args) override {
    Check_function_as_value_generator_parameters *func_arg =
        pointer_cast<Check_function_as_value_generator_parameters *>(
            checker_args);
    func_arg->banned_function_name = func_name();
    return true;
  }
};

class Item_func_is_used_lock final : public Item_int_func {
  typedef Item_int_func super;

  String value;

 public:
  Item_func_is_used_lock(const POS &pos, Item *a) : Item_int_func(pos, a) {
    set_has_notsupported_func_true();
  }

  bool itemize(Parse_context *pc, Item **res) override;
  longlong val_int() override;
  const char *func_name() const override { return "is_used_lock"; }
  bool resolve_type(THD *thd) override {
    if (param_type_is_default(thd, 0, 1)) return true;
    unsigned_flag = true;
    set_nullable(true);
    return false;
  }
  bool is_non_const_over_literals(uchar *) override { return true; }
  bool check_function_as_value_generator(uchar *checker_args) override {
    Check_function_as_value_generator_parameters *func_arg =
        pointer_cast<Check_function_as_value_generator_parameters *>(
            checker_args);
    func_arg->banned_function_name = func_name();
    return true;
  }
};

class Item_func_row_count final : public Item_int_func {
  typedef Item_int_func super;

 public:
  explicit Item_func_row_count(const POS &pos) : Item_int_func(pos) {}

  bool itemize(Parse_context *pc, Item **res) override;

  longlong val_int() override;
  const char *func_name() const override { return "row_count"; }
  bool resolve_type(THD *) override {
    set_nullable(false);
    return false;
  }
  bool check_function_as_value_generator(uchar *checker_args) override {
    Check_function_as_value_generator_parameters *func_arg =
        pointer_cast<Check_function_as_value_generator_parameters *>(
            checker_args);
    func_arg->banned_function_name = func_name();
    return true;
  }
};

/*
*  collection type count
*
   declaration creates a collection type,
   specifically an associative array or index-by table
*  'type val is table of '
*  example:
*   select val.count;
*
*   for i in 1 .. val.count
*       stmt ;
*/
class Item_func_table_count final : public Item_int_func {
  uint m_spv_idx;

 public:
  explicit Item_func_table_count(const POS &pos, uint spv_idx)
      : Item_int_func(pos), m_spv_idx(spv_idx) {}

  bool resolve_type(THD *thd) override;
  longlong val_int() override;
  const char *func_name() const override { return "table_count"; }
  void print(const THD *, String *str, enum_query_type) const override {
    str->append("count");
  }
};

class Item_func_udt_constructor : public Item_func {
  typedef Item_func super;
  bool m_use_explicit_name{false};

 public:
  LEX_STRING m_db_name;
  LEX_STRING m_udt_name;
  List<Create_field> *m_field_def_list{nullptr};
  ulong m_reclength;
  sql_mode_t m_sql_mode;
  TABLE *m_table{nullptr};
  Name_resolution_context *context{nullptr};

 public:
  Item_func_udt_constructor(const POS &pos, const LEX_STRING &db_name,
                            const LEX_STRING &udt_name, bool use_explicit_name,
                            PT_item_list *item_list,
                            List<Create_field> *field_def, ulong reclength,
                            sql_mode_t sql_mode);

  ~Item_func_udt_constructor() override {}

  TABLE *get_udt_table() override { return m_table; }
  enum Item_result result_type() const override { return ROW_RESULT; }
  const char *func_name() const override { return m_udt_name.str; }
  enum Functype functype() const override { return UDT_FUNC; }
  table_map get_initial_pseudo_tables() const override {
    return RAND_TABLE_BIT;
  }

  void cleanup() override {
    if (m_table) {
      free_udt_tmp_table(m_table);
      m_table = nullptr;
    }
    Item_func::cleanup();
  }

  bool itemize(Parse_context *pc, Item **res) override;
  bool fix_fields(THD *thd, Item **ref) override;
  bool refix_fields() override;
  bool is_null() override { return false; }
  double val_real() override { return 1.0; }

  longlong val_int() override { return 1; }

  bool get_date(MYSQL_TIME *, my_time_flags_t) override {
    assert(0);
    return true;
  }

  bool get_time(MYSQL_TIME *) override {
    assert(0);
    return true;
  }

  String *val_str(String *val) override;
  TABLE *val_udt() override;
  bool resolve_type(THD *thd) override;
  void make_field(Send_field *field) override;
  LEX_STRING get_udt_db_name() const override { return m_db_name; }
  LEX_STRING get_udt_name() const override { return m_udt_name; }
  bool udt_table_store_to_table(TABLE *table_to) override;
  type_conversion_status udt_table_store_to_field(Field *to,
                                                  bool no_conversions);
  /// for create table as select udt();
  Field *tmp_table_field(TABLE *t_arg) override;
  bool create_table_for_udt_column(TABLE *table,
                                   List<Create_field> *field_list);
};

class Item_func_udt_collection : public Item_func_udt_constructor {
  typedef Item_func_udt_constructor super;

 protected:
  LEX_CSTRING m_nested_table_udt;
  enum_udt_table_of_type m_table_type;
  ulonglong m_varray_size_limit;
  bool m_has_assignment_list;
  int m_index_length;

 public:
  Item_func_udt_collection(const POS &pos, const LEX_STRING &db_name,
                           const LEX_STRING &udt_name,
                           enum_udt_table_of_type table_type,
                           ulonglong varray_limit, PT_item_list *item_list,
                           List<Create_field> *field_def, ulong reclength,
                           sql_mode_t sql_mode, int index_length = 0,
                           LEX_CSTRING nested_table_udt = NULL_CSTR);

  TABLE *val_udt() override;
  String *val_str(String *) override;
  List<Create_field> *get_field_create_field_list() override {
    return m_field_def_list;
  }
  bool has_assignment_list() override { return m_has_assignment_list; }
  Field *tmp_table_field(TABLE *) override {
    my_error(ER_NOT_SUPPORTED_YET, MYF(0),
             "create table as select udt_table()");
    return nullptr;
  }
  const CHARSET_INFO *charset_for_protocol() override {
    return system_charset_info;
  }
};

class Item_func_udt_table final : public Item_func_udt_collection {
  typedef Item_func_udt_collection super;

 public:
  Item_func_udt_table(const POS &pos, const LEX_STRING &db_name,
                      const LEX_STRING &udt_name,
                      const LEX_CSTRING nested_table_udt, ulong reclength,
                      enum_udt_table_of_type table_type, ulonglong varray_limit,
                      PT_item_list *item_list, List<Create_field> *field_def,
                      int index_length = 0);

  ~Item_func_udt_table() override {}

  enum Functype functype() const override { return UDT_TABLE_FUNC; }
  bool itemize(Parse_context *pc, Item **res) override;
  bool fix_fields(THD *thd, Item **ref) override;
  bool refix_fields() override;
  void make_field(Send_field *field) override;
};

class Item_func_udt_single_type_table final : public Item_func_udt_collection {
  typedef Item_func_udt_collection super;

 public:
  Item_func_udt_single_type_table(
      const POS &pos, const LEX_STRING &db_name, const LEX_STRING &udt_name,
      enum_udt_table_of_type table_type, ulonglong varray_limit,
      PT_item_list *item_list, List<Create_field> *field_def, ulong reclength,
      sql_mode_t sql_mode, int index_length = 0);

  ~Item_func_udt_single_type_table() override {}

  enum Functype functype() const override { return UDT_TABLE_FUNC; }
  bool itemize(Parse_context *pc, Item **res) override;
  bool fix_fields(THD *thd, Item **ref) override;
  void make_field(Send_field *field) override;
  bool udt_table_store_to_table(TABLE *table_to) override;
};

class Item_func_cursor final : public Item_func {
  typedef Item_func super;
  const LEX_STRING m_cursor_name;

 public:
  explicit Item_func_cursor(const POS &pos, const LEX_STRING cursor_name,
                            PT_item_list *item_list)
      : Item_func(pos, item_list), m_cursor_name(cursor_name) {}

  LEX_STRING get_cursor_name() { return m_cursor_name; }
  const char *func_name() const override { return "func_cursor"; }
  bool resolve_type_inner(THD *) override {
    my_error(ER_NOT_SUPPORTED_YET, MYF(0), "use static cursor as a variable");
    return true;
  }
  bool resolve_type(THD *) override {
    set_nullable(false);
    return false;
  }
  double val_real() override {
    assert(0);
    return 0.0;
  }

  longlong val_int() override {
    assert(0);
    return 0;
  }

  bool get_date(MYSQL_TIME *, my_time_flags_t) override {
    assert(0);
    return true;
  }

  bool get_time(MYSQL_TIME *) override {
    assert(0);
    return true;
  }

  String *val_str(String *) override {
    assert(0);
    return nullptr;
  }
};

/*
 *
 * Stored FUNCTIONs
 *
 */

class sp_head;
class sp_name;
class sp_signature;

class Item_func_sp final : public Item_func {
  typedef Item_func super;

  /**
    Backup and restore the list of parameters in the package function
  */
  class Backup_args_defer {
   public:
    Backup_args_defer(Item_func_sp *item);
    ~Backup_args_defer();

    Backup_args_defer(const Backup_args_defer &) = delete;
    Backup_args_defer &operator=(const Backup_args_defer &) = delete;

   private:
    Item **backup_args{nullptr};
    const Item_func_sp *outer{nullptr};
  };

 private:
  Name_resolution_context *context{nullptr};
  /// The name of the stored function
  sp_name *m_name{nullptr};
  LEX_CSTRING m_save_db;
  LEX_STRING m_save_fn_name;
  bool m_save_explicit_name;

  /// The name of the stored function
  sp_name *m_pkg_name{nullptr};
  /*
    routine signature which is set in itemize() and used in
    invoking sp_setup_routine().
  */
  sp_signature *m_sig{nullptr};
  /// Pointer to actual function instance (null when not resolved or executing)
  sp_head *m_sp{nullptr};
  /// The result field of the concrete stored function.
  Field *sp_result_field{nullptr};
  /// true when function execution is deterministic
  bool m_deterministic{false};
  bool m_is_refcursor{false};

  bool execute();
  bool execute_impl(THD *thd);
  bool init_result_field(THD *thd);
  bool resolve_udt_type(THD *thd);

 protected:
  bool is_expensive_processor(uchar *) override { return true; }
  /**
    It is used to judge and convert the non-binary string value of the RAW type
    parameter passed to the function in PACKAGE according to the conversion
    rules of oracle which is caused by the fact that there is no RAW type in
    MySQL.

    Note: In the future, if the RAW type is compatible, this function should be
    discarded
  */
  bool is_raw_params_invalid();

 public:
  Item_func_sp(const POS &pos, const LEX_STRING &db_name,
               const LEX_STRING &fn_name, bool use_explicit_name,
               PT_item_list *opt_list);
  ~Item_func_sp() override;
  bool itemize(Parse_context *pc, Item **res) override;
  /**
    Must not be called before the procedure is resolved,
    i.e. @c init_result_field().
  */
  table_map get_initial_pseudo_tables() const override;
  void update_used_tables() override;
  void fix_after_pullout(Query_block *parent_query_block,
                         Query_block *removed_query_block) override;
  void cleanup() override;

  const char *func_name() const override;

  Field *tmp_table_field(TABLE *t_arg) override;

  void make_field(Send_field *tmp_field) override;

  Item_result result_type() const override;

  longlong val_int() override;
  double val_real() override;
  bool get_date(MYSQL_TIME *ltime, my_time_flags_t fuzzydate) override;
  bool get_time(MYSQL_TIME *ltime) override;
  my_decimal *val_decimal(my_decimal *dec_buf) override;
  String *val_str(String *str) override;
  bool val_json(Json_wrapper *result) override;
  TABLE *val_udt() override;
  TABLE *get_udt_table() override { return sp_result_field->m_udt_table; }

  bool change_context_processor(uchar *arg) override {
    context = reinterpret_cast<Item_ident::Change_context *>(arg)->m_context;
    return false;
  }

  bool sp_check_access(THD *thd);
  enum Functype functype() const override { return FUNC_SP; }

  bool fix_fields(THD *thd, Item **ref) override;
  bool resolve_type(THD *thd) override;

  bool is_expensive() override { return true; }

  inline Field *get_sp_result_field() { return sp_result_field; }
  bool check_function_as_value_generator(uchar *checker_args) override {
    Check_function_as_value_generator_parameters *func_arg =
        pointer_cast<Check_function_as_value_generator_parameters *>(
            checker_args);
    func_arg->banned_function_name = func_name();
    return true;
  }

  void set_pkg_db(LEX_CSTRING &db_name);
  sp_name *get_sp_name() { return m_name; }
  LEX_STRING get_udt_db_name() const override {
    if (sp_result_field == nullptr) return NULL_STR;
    return to_lex_string(to_lex_cstring(sp_result_field->get_udt_db_name()));
  }
  LEX_STRING get_udt_name() const override {
    if (sp_result_field == nullptr) return NULL_STR;
    return sp_result_field->udt_name();
  }
  bool udt_table_store_to_table(TABLE *table_to) override;
  bool is_ora_refcursor() const override { return m_is_refcursor; }
  void cleanup_for_refcursor() override {
    if (is_ora_type() || is_ora_refcursor()) {
      destroy(sp_result_field);
      sp_result_field = nullptr;
    }
  }
};

class Item_func_found_rows final : public Item_int_func {
  typedef Item_int_func super;

 public:
  explicit Item_func_found_rows(const POS &pos) : Item_int_func(pos) {}

  bool itemize(Parse_context *pc, Item **res) override;
  longlong val_int() override;
  const char *func_name() const override { return "found_rows"; }
  bool resolve_type(THD *) override {
    set_nullable(false);
    return false;
  }
  bool check_function_as_value_generator(uchar *checker_args) override {
    Check_function_as_value_generator_parameters *func_arg =
        pointer_cast<Check_function_as_value_generator_parameters *>(
            checker_args);
    func_arg->banned_function_name = func_name();
    return true;
  }

  Item *pq_clone(THD *thd, Query_block *select) override;
};

void uuid_short_init();

class Item_func_uuid_short final : public Item_int_func {
  typedef Item_int_func super;

 public:
  Item_func_uuid_short(const POS &pos) : Item_int_func(pos) {}

  bool itemize(Parse_context *pc, Item **res) override;
  const char *func_name() const override { return "uuid_short"; }
  longlong val_int() override;
  bool resolve_type(THD *) override {
    unsigned_flag = true;
    return false;
  }
  bool check_partition_func_processor(uchar *) override { return false; }
  bool check_function_as_value_generator(uchar *checker_args) override {
    Check_function_as_value_generator_parameters *func_arg =
        pointer_cast<Check_function_as_value_generator_parameters *>(
            checker_args);
    func_arg->banned_function_name = func_name();
    return ((func_arg->source == VGS_GENERATED_COLUMN) ||
            (func_arg->source == VGS_CHECK_CONSTRAINT));
  }
};

class Item_func_version final : public Item_static_string_func {
  typedef Item_static_string_func super;

 public:
  explicit Item_func_version(const POS &pos);

  bool itemize(Parse_context *pc, Item **res) override;

  Item *pq_clone(THD *thd, Query_block *select) override;
};

class Item_func_vsize : public Item_int_func {
  String value;

 public:
  Item_func_vsize(const POS &pos, Item *a) : Item_int_func(pos, a) {}
  longlong val_int() override;
  const char *func_name() const override { return "vsize"; }
  bool resolve_type(THD *thd) override {
    if (param_type_is_default(thd, 0, 1)) return true;
    max_length = 10;
    return false;
  }
};

/**
  Internal function used by INFORMATION_SCHEMA implementation to check
  if a role is a mandatory role.
*/

class Item_func_internal_is_mandatory_role : public Item_int_func {
 public:
  Item_func_internal_is_mandatory_role(const POS &pos, Item *a, Item *b)
      : Item_int_func(pos, a, b) {}
  longlong val_int() override;
  const char *func_name() const override {
    return "internal_is_mandatory_role";
  }
  enum Functype functype() const override { return DD_INTERNAL_FUNC; }
  bool resolve_type(THD *) override {
    set_nullable(true);
    return false;
  }
};

class SequenceEntity {
 public:
  SequenceEntity(Gdb_sequence_entity *entity_def, const char *db,
                 const char *table, uint64_t version)
      : entity(entity_def),
        db_name(db),
        table_name(table),
        sequence_metadata_version(version),
        read_next(false),
        init(false),
        executed(false) {
    my_decimal_set_zero(&curval);
  }

  bool check_sequence_version(THD *thd);
  bool check_and_refix(THD *thd);
  bool Reset();
  bool Read_val();

  void print(const THD *, String *str, enum_query_type query_type) const;

  Gdb_sequence_entity *entity;
  const char *db_name;
  const char *table_name;
  uint64_t sequence_metadata_version;
  my_decimal curval;  // current value for row read
  bool read_next;
  bool init;
  bool executed;

  void Clear() {
    init = false;
    executed = false;
  }
};

class Item_func_sequence : public Item_func {
  my_decimal curval;  // just copy each item value
  SequenceEntity *m_seq_def;
  bool read_next;

 public:
  Item_func_sequence(const POS &pos, SequenceEntity *seq_def, bool next)
      : Item_func(pos), m_seq_def(seq_def), read_next(next) {
    my_decimal_set_zero(&curval);
  }

  const char *sequence_name() const {
    assert(m_seq_def);
    return m_seq_def->table_name;
  }
  // TODO
  // bool check_column_privileges(THD *thd);

  String *val_str(String *str) override;
  double val_real() override;
  longlong val_int() override;

  bool get_date(MYSQL_TIME *ltime, my_time_flags_t) override {
    assert(0);
    set_zero_time(ltime, MYSQL_TIMESTAMP_DATETIME);
    return false;
  }
  bool get_time(MYSQL_TIME *ltime) override {
    assert(0);
    set_zero_time(ltime, MYSQL_TIMESTAMP_TIME);
    return false;
  }

  my_decimal *val_decimal(my_decimal *) override;

  const CHARSET_INFO *charset_for_protocol() override {
    return &my_charset_numeric;
  }

  enum Functype functype() const override { return SEQUENCE_FUNC; }
  table_map get_initial_pseudo_tables() const override {
    return RAND_TABLE_BIT;
  }

  // force return decimal value
  enum Item_result result_type() const override { return DECIMAL_RESULT; }
  bool resolve_type(THD *thd) override;

  const char *func_name() const override {
    return read_next ? "nextval" : "currval";
  }

  void print(const THD *, String *str, enum_query_type) const override;

  void update_used_tables() override;
};

/**
  Internal function used by INFORMATION_SCHEMA implementation to check
  if a role is enabled.
*/

class Item_func_internal_is_enabled_role : public Item_int_func {
 public:
  Item_func_internal_is_enabled_role(const POS &pos, Item *a, Item *b)
      : Item_int_func(pos, a, b) {}
  longlong val_int() override;
  const char *func_name() const override { return "internal_is_enabled_role"; }
  enum Functype functype() const override { return DD_INTERNAL_FUNC; }
  bool resolve_type(THD *) override {
    set_nullable(true);
    return false;
  }
};

class CounterItem {
 public:
  virtual ~CounterItem() {}
  virtual bool Reset() = 0;
  virtual bool Incr() = 0;
  // for explain
  virtual const char *Description() = 0;

  bool BeforeRead{false};
};

class RownumCounter : public CounterItem {
  longlong num;

 public:
  RownumCounter() : num(0) { BeforeRead = true; }

  bool Reset() override {
    num = 0;
    return false;
  }
  bool Incr() override {
    num++;
    return false;
  }

  const char *Description() override { return "COUNTER ROWNUM"; }

  longlong *GetValue() { return &num; }
};

class SequenceCounter : public CounterItem {
  THD *m_thd;

 public:
  SequenceCounter(THD *thd) : m_thd(thd), sequence_funcs(thd->mem_root) {}

  bool Reset() override;

  bool Incr() override;

  const char *Description() override { return "SEQUENCE"; }

  bool check_sequence_version(THD *thd);
  bool check_and_refix(THD *thd);

  // for sp/func
  void Clear() {
    for (auto &seq : sequence_funcs) seq->Clear();
  }
  mem_root_deque<SequenceEntity *> sequence_funcs;
};

class Item_func_rownum : public Item_int_func {
  typedef Item_int_func super;

 public:
  explicit Item_func_rownum(const POS &pos) : Item_int_func(pos), ref(nullptr) {
    set_has_notsupported_func_true();
    item_name = NAME_STRING("rownum");
  }

  longlong val_int() override;
  bool itemize(Parse_context *pc, Item **res) override;

  const char *func_name() const override { return "rownum"; }

  enum Functype functype() const override { return ROWNUM_FUNC; }

  bool resolve_type(THD *thd) override;

  bool fix_fields(THD *thd, Item **res) override;

  enum Item_result result_type() const override { return INT_RESULT; }

  table_map get_initial_pseudo_tables() const override {
    return RAND_TABLE_BIT;
  }
  void print(const THD *, String *str, enum_query_type) const override {
    str->append(func_name());
  }
 private:
  longlong *ref;
};

class Item_func_lnnvl_advisor {
 public:
  static bool is_valid_expr(Item *expr);
  static bool is_lnnvl_expr(Item *expr);
};

class Item_func_lnnvl : public Item_int_func {
 private:
  bool validate_cond(THD *thd);

 public:
  explicit Item_func_lnnvl(const POS &pos, Item *a) : Item_int_func(pos, a) {}

  bool itemize(Parse_context *pc, Item **res) override;
  bool fix_fields(THD *thd, Item **ref) override;
  bool resolve_type(THD *thd) override;
  longlong val_int() override;
  enum Item_result result_type() const override { return INT_RESULT; }
  const char *func_name() const override { return "lnnvl"; }
  enum_field_types default_data_type() const override {
    return MYSQL_TYPE_NULL;
  }
  bool is_bool_func() const override { return true; }
  enum Functype functype() const override { return LNNVL_FUNC; }
  Item *pq_clone(THD *thd, Query_block *select) override;
};
class Item_func_dbmsotpt_putline : public Item_int_func {
 public:
  explicit Item_func_dbmsotpt_putline(const POS &pos, Item *a)
      : Item_int_func(pos, a) {}
  longlong val_int() override;
  const char *func_name() const override { return "dbmsotpt_putline"; }
};

class Item_func_dbmsotpt_setbuf : public Item_int_func {
 public:
  Item_func_dbmsotpt_setbuf(const POS &pos, Item *a) : Item_int_func(pos, a) {}
  longlong val_int() override;
  const char *func_name() const override { return "dbmsotpt_setbuf"; }
};

class Item_func_dbmsotpt_getbuffercount : public Item_int_func {
 public:
  explicit Item_func_dbmsotpt_getbuffercount(const POS &pos)
      : Item_int_func(pos) {}
  longlong val_int() override;
  const char *func_name() const override { return "dbmsotpt_getbuffercount"; }
};

/**
  Create new Item_func_get_system_var object

  @param pc     Parse context

  @param scope  Scope of the variable (GLOBAL, SESSION, PERSISTENT ...)

  @param prefix Empty LEX_CSTRING{} or the left hand side of the composite
                variable name, e.g.:
                * component name of the component-registered variable
                * name of MyISAM Multiple Key Cache.

  @param suffix Name of the variable (if prefix is empty) or the right
                hand side of the composite variable name, e.g.:
                * name of the component-registered variable
                * property name of MyISAM Multiple Key Cache variable.

  @param unsafe_for_replication force writing this system variable to binlog
                (if not written yet)

  @returns new item on success, otherwise nullptr
*/
Item *get_system_variable(Parse_context *pc, enum_var_type scope,
                          const LEX_CSTRING &prefix, const LEX_CSTRING &suffix,
                          bool unsafe_for_replication);

inline Item *get_system_variable(Parse_context *pc, enum_var_type scope,
                                 const LEX_CSTRING &trivial_name,
                                 bool unsafe_for_replication) {
  return get_system_variable(pc, scope, {}, trivial_name,
                             unsafe_for_replication);
}

class Item_func_cursor_rowcount : public Item_int_func, public Cursor_ref {
 public:
  Item_func_cursor_rowcount(const POS &pos, LEX_CSTRING name, uint offset,
                            sp_variable *cursor_spv)
      : Item_int_func(pos), Cursor_ref(name, offset, cursor_spv) {}
  const char *func_name() const override { return "%ROWCOUNT"; }
  longlong val_int() override;
};

class Item_func_implicit_cursor_rowcount : public Item_int_func {
 public:
  Item_func_implicit_cursor_rowcount(const POS &pos) : Item_int_func(pos) {}
  const char *func_name() const override { return "SQL%ROWCOUNT"; }
  longlong val_int() override;
};

extern bool check_reserved_words(const char *name);
extern enum_field_types agg_field_type(Item **items, uint nitems);
double my_double_round(double value, longlong dec, bool dec_unsigned,
                       bool truncate);
bool eval_const_cond(THD *thd, Item *cond, bool *value);
Item_field *get_gc_for_expr(const Item *func, Field *fld, Item_result type,
                            Field **found = nullptr);

void retrieve_tablespace_statistics(THD *thd, Item **args, bool *null_value);

bool is_function_of_type(const Item *item, Item_func::Functype type);

extern bool volatile mqh_used;

/// Checks if "item" is a function of the specified type.
bool is_function_of_type(const Item *item, Item_func::Functype type);

/// Checks if "item" contains a function of the specified type.
bool contains_function_of_type(Item *item, Item_func::Functype type);

#endif /* ITEM_FUNC_INCLUDED */
