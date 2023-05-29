/*
   Copyright (c) 2000, 2022, Oracle and/or its affiliates. All rights reserved.
   Copyright (c) 2023, GreatDB Software Co., Ltd.

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
  This file defines all time functions
*/

#include "sql/item_timefunc.h"

#include "my_config.h"
#ifdef HAVE_SYS_TIME_H
#include <sys/time.h>
#endif

#include <algorithm>
#include <climits>
#include <cstdio>
#include <cstdlib>
#include <cstring>

#include "decimal.h"
#include "lex_string.h"
#include "m_string.h"
#include "my_compiler.h"
#include "my_dbug.h"
#include "my_sys.h"
#include "my_systime.h"  // my_micro_time
#include "mysql_com.h"
#include "mysqld_error.h"
#include "sql/current_thd.h"
#include "sql/dd/info_schema/table_stats.h"
#include "sql/dd/object_id.h"  // dd::Object_id
#include "sql/derror.h"        // ER_THD
#include "sql/my_decimal.h"
#include "sql/parse_tree_node_base.h"  // Parse_context
#include "sql/sql_class.h"             // THD
#include "sql/sql_error.h"
#include "sql/sql_lex.h"
#include "sql/sql_locale.h"  // my_locale_en_US
#include "sql/sql_time.h"    // make_truncated_value_warning
#include "sql/strfunc.h"     // check_word
#include "sql/system_variables.h"
#include "sql/table.h"
#include "sql/tztime.h"  // Time_zone
#include "template_utils.h"
#include "typelib.h"

using std::max;
using std::min;

/**
  Check and adjust a time value with a warning.

  @param ltime    Time variable.
  @param decimals Precision.
  @retval         True on error, false of success.
*/
static bool adjust_time_range_with_warn(MYSQL_TIME *ltime, uint8 decimals) {
  /* Fatally bad value should not come here */
  if (check_time_range_quick(*ltime)) {
    int warning = 0;
    if (make_truncated_value_warning(current_thd, Sql_condition::SL_WARNING,
                                     ErrConvString(ltime, decimals),
                                     MYSQL_TIMESTAMP_TIME, NullS))
      return true;
    adjust_time_range(ltime, &warning);
  }
  return false;
}

/*
  Convert seconds to MYSQL_TIME value with overflow checking.

  SYNOPSIS:
    sec_to_time()
    seconds          number of seconds
    ltime            output MYSQL_TIME value

  DESCRIPTION
    If the 'seconds' argument is inside MYSQL_TIME data range, convert it to a
    corresponding value.
    Otherwise, truncate the resulting value to the nearest endpoint.
    Note: Truncation in this context means setting the result to the MAX/MIN
          value of TIME type if value is outside the allowed range.
          If the number of decimals exceeds what is supported, the value
          is rounded to the supported number of decimals.

  RETURN
    1                if the value was truncated during conversion
    0                otherwise
*/

static bool sec_to_time(lldiv_t seconds, MYSQL_TIME *ltime) {
  int warning = 0;

  set_zero_time(ltime, MYSQL_TIMESTAMP_TIME);

  if (seconds.quot < 0 || seconds.rem < 0) {
    ltime->neg = true;
    seconds.quot = -seconds.quot;
    seconds.rem = -seconds.rem;
  }

  if (seconds.quot > TIME_MAX_VALUE_SECONDS) {
    set_max_hhmmss(ltime);
    return true;
  }

  ltime->hour = (uint)(seconds.quot / 3600);
  uint sec = (uint)(seconds.quot % 3600);
  ltime->minute = sec / 60;
  ltime->second = sec % 60;
  time_add_nanoseconds_adjust_frac(ltime, static_cast<uint>(seconds.rem),
                                   &warning,
                                   current_thd->is_fsp_truncate_mode());

  adjust_time_range(ltime, &warning);
  return warning ? true : false;
}

/** Array of known date_time formats */
static constexpr const Known_date_time_format known_date_time_formats[6] = {
    {"USA", "%m.%d.%Y", "%Y-%m-%d %H.%i.%s", "%h:%i:%s %p"},
    {"JIS", "%Y-%m-%d", "%Y-%m-%d %H:%i:%s", "%H:%i:%s"},
    {"ISO", "%Y-%m-%d", "%Y-%m-%d %H:%i:%s", "%H:%i:%s"},
    {"EUR", "%d.%m.%Y", "%Y-%m-%d %H.%i.%s", "%H.%i.%s"},
    {"INTERNAL", "%Y%m%d", "%Y%m%d%H%i%s", "%H%i%s"},
    {nullptr, nullptr, nullptr, nullptr}};

/*
  Date formats corresponding to compound %r and %T conversion specifiers
*/
static const Date_time_format time_ampm_format = {{0}, {"%I:%i:%S %p", 11}};
static const Date_time_format time_24hrs_format = {{0}, {"%H:%i:%S", 8}};

static bool is_oracle_compat_func(const char *func_name) {
  return !strcmp(func_name, "to_date") || !strcmp(func_name, "to_timestamp");
}

static int get_rr_year(const char *nptr, const char **endptr, int *error,
                       MYSQL_TIME current_time) {
  int ret_year{0};
  int rr_digits = (int)my_strtoll10(nptr, endptr, error);
  int cur_centory = current_time.year / 100;
  int cur_year = current_time.year % 100;

  if (rr_digits > 99) return rr_digits;
  if (cur_year <= 49) {
    if (rr_digits <= 49)
      ret_year = cur_centory * 100 + rr_digits;
    else
      ret_year = (cur_centory - 1) * 100 + rr_digits;
  } else {
    if (rr_digits <= 49)
      ret_year = (cur_centory + 1) * 100 + rr_digits;
    else
      ret_year = cur_centory * 100 + rr_digits;
  }
  return ret_year;
}

typedef enum {
  FMT_BASE = 128,
  FMT_AD,
  FMT_Ad,
  FMT_ad,
  FMT_AD_DOT,
  FMT_ad_DOT,
  FMT_AM,
  FMT_Am,
  FMT_am,
  FMT_AM_DOT,
  FMT_am_DOT,
  FMT_BC,
  FMT_Bc,
  FMT_bc,
  FMT_BC_DOT,
  FMT_bc_DOT,
  FMT_CC,
  FMT_SCC,
  FMT_D,
  FMT_DAY,
  FMT_Day,
  FMT_day,
  FMT_DD,
  FMT_DDD,
  FMT_DL,
  FMT_DS,
  FMT_DY,
  FMT_Dy,
  FMT_dy,
  FMT_E,
  FMT_EE,
  FMT_FF,
  FMT_FM,
  FMT_FX,
  FMT_HH,
  FMT_HH12,
  FMT_HH24,
  FMT_IW,
  FMT_I,
  FMT_IY,
  FMT_IYY,
  FMT_IYYY,
  FMT_J,
  FMT_MI,
  FMT_MM,
  FMT_MON,
  FMT_Mon,
  FMT_mon,
  FMT_MONTH,
  FMT_Month,
  FMT_month,
  FMT_PM,
  FMT_Pm,
  FMT_pm,
  FMT_PM_DOT,
  FMT_pm_DOT,
  FMT_Q,
  FMT_rm,
  FMT_Rm,
  FMT_RM,
  FMT_RR,
  FMT_RRRR,
  FMT_SP,
  FMT_SS,
  FMT_SSSSS,
  FMT_th,
  FMT_TH,
  FMT_TS,
  FMT_TZD,
  FMT_TZH,
  FMT_TZM,
  FMT_TZR,
  FMT_W,
  FMT_WW,
  FMT_X,
  FMT_Y,
  FMT_YY,
  FMT_YYY,
  FMT_YYYY,
  FMT_YYYY_COMMA,
  FMT_year,
  FMT_Year,
  FMT_YEAR,
  FMT_SYYYY,
  FMT_SYEAR,
  FMT_BASE_END
} FORMAT_ORACLE_DATE_TYPE_E;

/**
  Extract datetime value to MYSQL_TIME struct from string value
  according to format string.

  @param format		date/time format specification
  @param val			String to decode
  @param length		Length of string
  @param l_time		Store result here
  @param cached_timestamp_type  It uses to get an appropriate warning
                                in the case when the value is truncated.
  @param sub_pattern_end    if non-zero then we are parsing string which
                            should correspond compound specifier (like %T or
                            %r) and this parameter is pointer to place where
                            pointer to end of string matching this specifier
                            should be stored.
  @param date_time_type "time" or "datetime", used for the error/warning msg
  @param func_name function name of the caller

  @note
    Possibility to parse strings matching to patterns equivalent to compound
    specifiers is mainly intended for use from inside of this function in
    order to understand %T and %r conversion specifiers, so number of
    conversion specifiers that can be used in such sub-patterns is limited.
    Also most of checks are skipped in this case.

  @note
    If one adds new format specifiers to this function he should also
    consider adding them to Item_func_str_to_date::fix_from_format().

  @retval
    0	ok
  @retval
    1	error
*/

static bool extract_date_time(const Date_time_format *format, const char *val,
                              size_t length, MYSQL_TIME *l_time,
                              enum_mysql_timestamp_type cached_timestamp_type,
                              const char **sub_pattern_end,
                              const char *date_time_type,
                              const char *func_name) {
  int weekday = INT_MIN, yearday = INT_MIN, daypart = 0;
  int week_number = -1;
  int error = 0;
  int strict_week_number_year = -1;
  int frac_part;
  bool usa_time = false;
  bool sunday_first_n_first_week_non_iso = false;
  bool strict_week_number = false;
  bool strict_week_number_year_type = false;
  const char *val_begin = val;
  const char *val_end = val + length;
  const char *ptr = format->format.str;
  const char *end = ptr + format->format.length;
  const CHARSET_INFO *cs = &my_charset_bin;
  bool extract_year = false;
  bool extract_month = false;
  bool extract_day = false;
  bool extract_hour = false;
  bool extract_minute = false;
  bool extract_second = false;
  uint8 day_of_week_1_7 = UINT_MAX8;
  uint16 fm = UINT_MAX16;
  uint32 second_of_day = UINT_MAX32, julian_day = UINT_MAX32;
  bool isdeli = false, is_fill_mode = false, is_format_exact = false;
  bool is_oracle_compat = is_oracle_compat_func(func_name);

  DBUG_TRACE;

  // get current time to post-process ltime.
  MYSQL_TIME_cache tm;
  MYSQL_TIME current_time;
  memset(&current_time, 0, sizeof(current_time));
  tm.set_datetime(current_thd->query_start_timeval_trunc(0), 0,
                  current_thd->time_zone());
  tm.get_date(&current_time, 0);

  if (!sub_pattern_end) memset(l_time, 0, sizeof(*l_time));

  auto format_exact_enable = [&is_fill_mode, &is_format_exact]() {
    return (is_format_exact && !is_fill_mode);
  };

  /**
    In oracle mode, continue parsing the format even after extracting to the end
    of date string
  */
  for (; ptr != end && (val != val_end || is_oracle_compat); ptr++) {
    isdeli = true;
    if (ptr + 1 != end) {
      if (is_oracle_compat) {
        fm = (*(const uint8 *)(ptr + 1) << 8) | *(const uint8 *)ptr;
        ptr += 1;
        if (fm >= FMT_BASE) {
          isdeli = false;
          switch (fm) {
            case FMT_FX:
              is_format_exact = true;
              continue;
            case FMT_FM:
              is_fill_mode = true;
              continue;
            case FMT_th:
            case FMT_TH:
              continue;
            case FMT_X:
              break;
            default:
              /**
                The non-delimited format must not match the delimiter of the
                date string
              */
              if (my_ispunct(cs, *val)) goto err;
              break;
          }
        }
      } else {
        fm = *ptr;
        if ('%' == fm) {
          fm = *++ptr;
          isdeli = false;
        }
      }
    } else {
      fm = *(const uint8 *)ptr;
    }

    /* Skip pre-space between each argument */
    if (!is_format_exact &&
        (val += cs->cset->scan(cs, val, val_end, MY_SEQ_SPACES)) >= val_end &&
        !is_oracle_compat)
      break;

    if (!isdeli) {
      int val_len;
      const char *tmp;

      error = 0;

      val_len = (uint)(val_end - val);
      switch (fm) {
          /* Year */
        case FMT_Y:
          if (extract_year) goto err;
          tmp = val + min(1, val_len);
          l_time->year = (int)my_strtoll10(val, &tmp, &error) +
                         (current_time.year / 10) * 10;
          val = tmp;
          extract_year = true;
          break;
        case FMT_YY:
          if (extract_year) goto err;
          tmp = val + min(2, val_len);
          l_time->year = (int)my_strtoll10(val, &tmp, &error) +
                         (current_time.year / 100) * 100;
          if (format_exact_enable() && tmp - val != 2) goto err;
          val = tmp;
          extract_year = true;
          break;
        case FMT_RR: {
          if (extract_year) goto err;
          tmp = val + min(2, val_len);
          l_time->year = get_rr_year(val, &tmp, &error, current_time);
          if (format_exact_enable() && tmp - val != 2) goto err;
          val = tmp;
          extract_year = true;
        } break;
        case FMT_YYY:
          if (extract_year) goto err;
          tmp = val + min(3, val_len);
          l_time->year = (int)my_strtoll10(val, &tmp, &error) +
                         (current_time.year / 1000) * 1000;
          if (format_exact_enable() && tmp - val != 3) goto err;
          val = tmp;
          extract_year = true;
          break;
        case FMT_YYYY:
          tmp = val + min(4, val_len);
          l_time->year = (int)my_strtoll10(val, &tmp, &error);
          if (format_exact_enable() && tmp - val != 4) goto err;
          val = tmp;
          extract_year = true;
          break;
        case FMT_RRRR:
          if (extract_year) goto err;
          tmp = val + min(4, val_len);
          l_time->year = (int)my_strtoll10(val, &tmp, &error);
          if ((uint)(tmp - val) <= 3) {
            tmp = val + min(4, val_len);
            l_time->year = get_rr_year(val, &tmp, &error, current_time);
          }
          if (format_exact_enable() && tmp - val != 4) goto err;
          val = tmp;
          extract_year = true;
          break;
        case 'Y':
          tmp = val + min(4, val_len);
          l_time->year = (int)my_strtoll10(val, &tmp, &error);
          if ((int)(tmp - val) <= 2)
            l_time->year = year_2000_handling(l_time->year);
          val = tmp;
          extract_year = true;
          break;
        case 'y':
          tmp = val + min(2, val_len);
          l_time->year = (int)my_strtoll10(val, &tmp, &error);
          val = tmp;
          l_time->year = year_2000_handling(l_time->year);
          extract_year = true;
          break;

          /* Month */
        case 'm':
        case 'c':
        case FMT_MM:
          if (is_oracle_compat && extract_month) goto err;
          tmp = val + min(2, val_len);
          l_time->month = (int)my_strtoll10(val, &tmp, &error);
          if (format_exact_enable() && tmp - val != 2) goto err;
          val = tmp;
          extract_month = true;
          break;
        case 'M':
        case FMT_month:
        case FMT_Month:
        case FMT_MONTH:
          if (is_oracle_compat && extract_month) goto err;
          if ((l_time->month =
                   check_word(my_locale_en_US.month_names, val, val_end, &val,
                              !format_exact_enable())) <= 0) {
            if (is_oracle_compat) {
              error = MY_ERRNO_EDOM;
            } else {
              goto err;
            }
          }
          extract_month = true;
          break;
        case 'b':
        case FMT_mon:
        case FMT_Mon:
        case FMT_MON:
          if (is_oracle_compat && extract_month) goto err;
          if ((l_time->month =
                   check_word(my_locale_en_US.ab_month_names, val, val_end,
                              &val, !format_exact_enable())) <= 0) {
            if (is_oracle_compat) {
              error = MY_ERRNO_EDOM;
            } else {
              goto err;
            }
          }
          extract_month = true;
          break;
        case FMT_D:
          tmp = val + min(1, val_len);
          day_of_week_1_7 = (int)my_strtoll10(val, &tmp, &error);
          val = tmp;
          break;
          /* Day */
        case 'd':
        case 'e':
        case FMT_DD:
          tmp = val + min(2, val_len);
          l_time->day = (int)my_strtoll10(val, &tmp, &error);
          if (format_exact_enable() && tmp - val != 2) goto err;
          val = tmp;
          extract_day = true;
          break;
        case 'D':
          tmp = val + min(2, val_len);
          l_time->day = (int)my_strtoll10(val, &tmp, &error);
          /* Skip 'st, 'nd, 'th .. */
          val = tmp + min<long>((val_end - tmp), 2);
          extract_day = true;
          break;

          /* Hour */
        case 'h':
        case 'I':
        case 'l':
        case FMT_HH12:
        case FMT_HH:
          usa_time = true;
          [[fallthrough]];
        case 'k':
        case 'H':
        case FMT_HH24:
          if (is_oracle_compat && extract_hour) goto err;
          tmp = val + min(2, val_len);
          l_time->hour = (int)my_strtoll10(val, &tmp, &error);
          if (format_exact_enable() && tmp - val != 2) goto err;
          val = tmp;
          extract_hour = true;
          break;

          /* Minute */
        case 'i':
        case FMT_MI:
          tmp = val + min(2, val_len);
          l_time->minute = (int)my_strtoll10(val, &tmp, &error);
          if (format_exact_enable() && tmp - val != 2) goto err;
          val = tmp;
          extract_minute = true;
          break;

          /* Second */
        case 's':
        case 'S':
        case FMT_SS:
          tmp = val + min(2, val_len);
          l_time->second = (int)my_strtoll10(val, &tmp, &error);
          if (format_exact_enable() && tmp - val != 2) goto err;
          val = tmp;
          extract_second = true;
          break;

          /* Second part */
        case 'f':
        case FMT_FF: {
          int frac = DATETIME_MAX_DECIMALS;
          if (FMT_FF == fm) {
            if ((ptr + 2) < end) {
              uint16 next_fm =
                  (*(const uint8 *)(ptr + 2) << 8) | *(const uint8 *)(ptr + 1);
              if (next_fm > 0 && next_fm <= DATETIME_MAX_DECIMALS) {
                ptr += 2;
                frac = next_fm;
              }
            }
            tmp = (val_end - val) > frac ? val + frac : val_end;
          } else {
            tmp = val_end;
            if (tmp - val > 6) tmp = val + 6;
          }
          l_time->second_part = (int)my_strtoll10(val, &tmp, &error);
          if (format_exact_enable() && tmp - val != frac) goto err;
          frac_part = 6 - (int)(tmp - val);
          if (frac_part > 0)
            l_time->second_part *= (ulong)log_10_int[frac_part];
          val = tmp;
          break;
        }
          /* AM / PM */
        case 'p':
        case FMT_am:
        case FMT_pm:
        case FMT_Pm:
        case FMT_Am:
        case FMT_AM:
        case FMT_PM:
          if (!usa_time) goto err;
          if (val_len < 2) {
            if (is_oracle_compat) {
              error = MY_ERRNO_EDOM;
            } else {
              goto err;
            }
          } else {
            if (!my_strnncoll(&my_charset_latin1, (const uchar *)val, 2,
                              (const uchar *)"PM", 2))
              daypart = 12;
            else if (my_strnncoll(&my_charset_latin1, (const uchar *)val, 2,
                                  (const uchar *)"AM", 2))
              goto err;
            val += 2;
          }
          break;
        case FMT_am_DOT:
        case FMT_pm_DOT:
        case FMT_AM_DOT:
        case FMT_PM_DOT:
          if (!usa_time) goto err;
          if (val_len < 4) {
            if (is_oracle_compat) {
              error = MY_ERRNO_EDOM;
            } else {
              goto err;
            }
          } else {
            if (!my_strnncoll(&my_charset_latin1, (const uchar *)val, 4,
                              (const uchar *)"p.m.", 4))
              daypart = 12;
            else if (my_strnncoll(&my_charset_latin1, (const uchar *)val, 4,
                                  (const uchar *)"a.m.", 4))
              goto err;
            val += 4;
          }
          break;

          /* Exotic things */
        case 'W':
        case FMT_day:
        case FMT_Day:
        case FMT_DAY:
          if ((weekday = check_word(my_locale_en_US.day_names, val, val_end,
                                    &val)) <= 0) {
            if (is_oracle_compat) {
              error = MY_ERRNO_EDOM;
            } else {
              goto err;
            }
          }
          break;
        case 'a':
        case FMT_dy:
        case FMT_Dy:
        case FMT_DY:
          if ((weekday = check_word(my_locale_en_US.ab_day_names, val, val_end,
                                    &val)) <= 0) {
            if (is_oracle_compat) {
              error = MY_ERRNO_EDOM;
            } else {
              goto err;
            }
          }
          break;
        case 'w':
          tmp = val + 1;
          if ((weekday = (int)my_strtoll10(val, &tmp, &error)) < 0 ||
              weekday >= 7)
            goto err;
          /* We should use the same 1 - 7 scale for %w as for %W */
          if (!weekday) weekday = 7;
          val = tmp;
          break;
        case 'j':
        case FMT_DDD:
          tmp = val + min(val_len, 3);
          yearday = (int)my_strtoll10(val, &tmp, &error);
          if (format_exact_enable() && tmp - val != 3) goto err;
          val = tmp;
          break;

          /* Week numbers */
        case 'V':
        case 'U':
        case 'v':
        case 'u':
          sunday_first_n_first_week_non_iso = (fm == 'U' || fm == 'V');
          strict_week_number = (fm == 'V' || fm == 'v');
          tmp = val + min(val_len, 2);
          if ((week_number = (int)my_strtoll10(val, &tmp, &error)) < 0 ||
              (strict_week_number && !week_number) || week_number > 53)
            goto err;
          val = tmp;
          break;

          /* Year used with 'strict' %V and %v week numbers */
        case 'X':
        case 'x':
          strict_week_number_year_type = (fm == 'X');
          tmp = val + min(4, val_len);
          strict_week_number_year = (int)my_strtoll10(val, &tmp, &error);
          val = tmp;
          break;

          /* Time in AM/PM notation */
        case 'r':
          /*
            We can't just set error here, as we don't want to generate two
            warnings in case of errors
          */
          if (extract_date_time(&time_ampm_format, val, (uint)(val_end - val),
                                l_time, cached_timestamp_type, &val, "time",
                                func_name))
            return true;
          break;

          /* Time in 24-hour notation */
        case 'T':
          if (extract_date_time(&time_24hrs_format, val, (uint)(val_end - val),
                                l_time, cached_timestamp_type, &val, "time",
                                func_name))
            return true;
          break;

          /* Conversion specifiers that match classes of characters */
        case '.':
          while (val < val_end && my_ispunct(cs, *val)) val++;
          break;
        case '@':
          while (val < val_end && my_isalpha(cs, *val)) val++;
          break;
        case '#':
          while (val < val_end && my_isdigit(cs, *val)) val++;
          break;
        case FMT_J:
          tmp = val + min(7, val_len);
          julian_day = (uint)my_strtoll10(val, &tmp, &error);
          if (format_exact_enable() && tmp - val != 7) goto err;
          val = tmp;
          break;
        case FMT_rm:
        case FMT_Rm:
        case FMT_RM:
          if (extract_month) goto err;
          l_time->month = roman_numeral_to_month(&val, val_end);
          extract_month = true;
          break;
        case FMT_SSSSS:
          tmp = val + min(5, val_len);
          second_of_day = (int)my_strtoll10(val, &tmp, &error);
          if (format_exact_enable() && tmp - val != 5) goto err;
          val = tmp;
          break;
        case FMT_X:
          if (val >= val_end ||
              my_strnncoll(&my_charset_latin1, (const uchar *)val, 1,
                           (const uchar *)".", 1)) {
            goto err;
          }
          val += 1;
          break;
        case FMT_YYYY_COMMA:
          if (extract_year) goto err;
          tmp = val + min(5, val_len);
          l_time->year = (int)my_strtoll10(val, &tmp, &error);
          if (!error && tmp - val < 5 && tmp != val_end && *tmp == ',') {
            int extract_len = tmp - val;
            const char *val_tmp = tmp + 1;
            tmp = val_tmp + min(5 - extract_len, (int)(val_end - val_tmp));
            /**
              For example ('2022,-12', 'Y,YYY-MM') is allowed so no error is
              handled here
            */
            int tmp_error;
            int part = (int)my_strtoll10(val_tmp, &tmp, &tmp_error);
            if (tmp > val_tmp)
              l_time->year = l_time->year * pow(10, tmp - val_tmp) + part;
          }
          /**
            Y,YYY will read 6 characters, but in the FX format, only 5
            characters including commas are required to be read. The actual
            processing rule in FX format is to read characters in Y,YYY format
            first and then judge whether to read only 5 characters.

            eg: TO_DATE('00,2211', 'FXY,YYYMM') and TO_DATE('00,221-01',
            'FXY,YYY-MM') will fail, but TO_DATE('00,22-11', 'FXY,YYY-MM') is
            correct
          */
          if (format_exact_enable() && tmp - val != 5) goto err;
          val = tmp;
          extract_year = true;
          break;
        default:
          goto err;
      }
      if (error) {  // Error from my_strtoll10
        /**
          In oracle mode, if my_strtoll10 is returning MY_ERRNO_EDOM and val is
          at the end of date string then it's not an error, in this case the
          data extract from val is 0
        */
        if (!is_oracle_compat || val != val_end || MY_ERRNO_EDOM != error) {
          goto err;
        }
      }
    } else if (is_oracle_compat) {
      if (is_format_exact) {
        if (val >= val_end || (*val != fm)) goto err;
        val += 1;
      } else {
        /**
          Skip if val is not at the end of the date string and the current val
          is punct or a space
        */
        val +=
            (val != val_end && (my_ispunct(cs, *val) || my_isspace(cs, *val)))
                ? 1
                : 0;
      }
    } else if (!my_isspace(&my_charset_latin1, fm)) {
      if (*val != fm) goto err;
      val++;
    }
  }
  if (usa_time) {
    if (l_time->hour > 12 || l_time->hour < 1) goto err;
    l_time->hour = l_time->hour % 12 + daypart;
  }

  /*
    If we are recursively called for parsing string matching compound
    specifiers we are already done.
  */
  if (sub_pattern_end) {
    *sub_pattern_end = val;
    return false;
  }

  if (is_oracle_compat) {
    if (val != val_end || ptr != end) goto err;
    if (!extract_year) l_time->year = current_time.year;
    if (!extract_month) l_time->month = current_time.month;
    if (!extract_day) l_time->day = 1;
    if (l_time->year < 1 || l_time->year > 9999 || l_time->month < 1 ||
        l_time->day < 1 ||
        l_time->day > calc_days_in_month(l_time->year, l_time->month))
      goto err;

    /* Check whether the day of week is correct */
    if (UINT_MAX8 != day_of_week_1_7) {
      if (day_of_week_1_7 < 1 || day_of_week_1_7 > 7) {
        goto err;
      }
      if (day_of_week_1_7 !=
          (calc_weekday(calc_daynr(l_time->year, l_time->month, l_time->day),
                        true) +
           1)) {
        goto err;
      }
    }

    /* Check whether the day of year in the input string is correct */
    if (yearday != INT_MIN &&
        (yearday < 1 || yearday > (calc_daynr(l_time->year, 12, 31) -
                                   calc_daynr(l_time->year, 1, 1) + 1)))
      goto err;

    /* Check whether the day of week in the input string is correct */
    if (INT_MIN != weekday &&
        calc_weekday(calc_daynr(l_time->year, l_time->month, l_time->day), 0) +
                1 !=
            weekday) {
      goto err;
    }

    /* Handles seconds past midnight (0-86399) */
    if (second_of_day != UINT_MAX32) {
      if (second_of_day >= SECONDS_IN_24H) {
        goto err;
      }
      uint h_second_of_day = second_of_day / SECS_PER_HOUR;
      uint mi_second_of_day = second_of_day % SECS_PER_HOUR / SECS_PER_MIN;
      uint s_second_of_day = second_of_day % SECS_PER_MIN;
      if ((extract_hour && l_time->hour != h_second_of_day) ||
          (extract_minute && l_time->minute != mi_second_of_day) ||
          (extract_second && l_time->second != s_second_of_day)) {
        goto err;
      }

      l_time->hour = h_second_of_day;
      l_time->minute = mi_second_of_day;
      l_time->second = s_second_of_day;
    }

    /* Handles Julian day */
    if (julian_day != UINT_MAX32) {
      if (yearday != INT_MIN || julian_day < 1 || julian_day > 5373484) {
        goto err;
      }
      int year_julian_day, mon_julian_day, day_julian_day;
      julianday_to_date(julian_day, year_julian_day, mon_julian_day,
                        day_julian_day);
      if (year_julian_day < 1 || year_julian_day > 9999 || mon_julian_day < 1 ||
          day_julian_day < 1) {
        goto err;
      }

      if ((extract_year && l_time->year != (uint)year_julian_day) ||
          (extract_month && l_time->month != (uint)mon_julian_day) ||
          (extract_day && l_time->day != (uint)day_julian_day)) {
        goto err;
      }
      l_time->year = year_julian_day;
      l_time->month = mon_julian_day;
      l_time->day = day_julian_day;
    }
  }

  if (yearday > 0) {
    uint days;
    days = calc_daynr(l_time->year, 1, 1) + yearday - 1;
    if (days <= 0 || days > MAX_DAY_NUMBER) goto err;
    MYSQL_TIME old_time = *l_time;
    get_date_from_daynr(days, &l_time->year, &l_time->month, &l_time->day);
    if (is_oracle_compat &&
        ((extract_month && l_time->year != old_time.year) ||
         (extract_month && l_time->month != old_time.month) ||
         (extract_day && l_time->day != old_time.day))) {
      goto err;
    }
  }

  if (week_number >= 0 && INT_MIN != weekday) {
    int days;
    uint weekday_b;

    /*
      %V,%v require %X,%x resprectively,
      %U,%u should be used with %Y and not %X or %x
    */
    if ((strict_week_number &&
         (strict_week_number_year < 0 ||
          strict_week_number_year_type != sunday_first_n_first_week_non_iso)) ||
        (!strict_week_number && strict_week_number_year >= 0))
      goto err;

    /* Number of days since year 0 till 1st Jan of this year */
    days = calc_daynr(
        (strict_week_number ? strict_week_number_year : l_time->year), 1, 1);
    /* Which day of week is 1st Jan of this year */
    weekday_b = calc_weekday(days, sunday_first_n_first_week_non_iso);

    /*
      Below we are going to sum:
      1) number of days since year 0 till 1st day of 1st week of this year
      2) number of days between 1st week and our week
      3) and position of our day in the week
    */
    if (sunday_first_n_first_week_non_iso) {
      days += ((weekday_b == 0) ? 0 : 7) - weekday_b + (week_number - 1) * 7 +
              weekday % 7;
    } else {
      days += ((weekday_b <= 3) ? 0 : 7) - weekday_b + (week_number - 1) * 7 +
              (weekday - 1);
    }

    if (days <= 0 || days > MAX_DAY_NUMBER) goto err;
    get_date_from_daynr(days, &l_time->year, &l_time->month, &l_time->day);
  }

  if (l_time->month > 12 || l_time->day > 31 || l_time->hour > 23 ||
      l_time->minute > 59 || l_time->second > 59)
    goto err;

  if (val != val_end) {
    do {
      if (!my_isspace(&my_charset_latin1, *val)) {
        // TS-TODO: extract_date_time is not UCS2 safe
        if (make_truncated_value_warning(current_thd, Sql_condition::SL_WARNING,
                                         ErrConvString(val_begin, length),
                                         cached_timestamp_type, NullS))
          goto err;
        break;
      }
    } while (++val != val_end);
  }
  return false;

err : {
  char buff[128];
  strmake(buff, val_begin, min<size_t>(length, sizeof(buff) - 1));
  push_warning_printf(current_thd, Sql_condition::SL_WARNING,
                      ER_WRONG_VALUE_FOR_TYPE,
                      ER_THD(current_thd, ER_WRONG_VALUE_FOR_TYPE),
                      date_time_type, buff, func_name);
}
  return true;
}

/**
  Create a formatted date/time value in a string.
*/

bool make_date_time(Date_time_format *format, MYSQL_TIME *l_time,
                    enum_mysql_timestamp_type type, String *str) {
  char intbuff[15];
  uint hours_i;
  uint weekday;
  ulong length;
  const char *ptr, *end;
  THD *thd = current_thd;
  MY_LOCALE *locale = thd->variables.lc_time_names;

  str->length(0);

  if (l_time->neg) str->append('-');

  end = (ptr = format->format.str) + format->format.length;
  for (; ptr != end; ptr++) {
    if (*ptr != '%' || ptr + 1 == end)
      str->append(*ptr);
    else {
      switch (*++ptr) {
        case 'M':
          if (!l_time->month) return true;
          str->append(
              locale->month_names->type_names[l_time->month - 1],
              strlen(locale->month_names->type_names[l_time->month - 1]),
              system_charset_info);
          break;
        case 'b':
          if (!l_time->month) return true;
          str->append(
              locale->ab_month_names->type_names[l_time->month - 1],
              strlen(locale->ab_month_names->type_names[l_time->month - 1]),
              system_charset_info);
          break;
        case 'W':
          if (type == MYSQL_TIMESTAMP_TIME || !(l_time->month || l_time->year))
            return true;
          weekday = calc_weekday(
              calc_daynr(l_time->year, l_time->month, l_time->day), false);
          str->append(locale->day_names->type_names[weekday],
                      strlen(locale->day_names->type_names[weekday]),
                      system_charset_info);
          break;
        case 'a':
          if (type == MYSQL_TIMESTAMP_TIME || !(l_time->month || l_time->year))
            return true;
          weekday = calc_weekday(
              calc_daynr(l_time->year, l_time->month, l_time->day), false);
          str->append(locale->ab_day_names->type_names[weekday],
                      strlen(locale->ab_day_names->type_names[weekday]),
                      system_charset_info);
          break;
        case 'D':
          if (type == MYSQL_TIMESTAMP_TIME) return true;
          length = longlong10_to_str(l_time->day, intbuff, 10) - intbuff;
          str->append_with_prefill(intbuff, length, 1, '0');
          if (l_time->day >= 10 && l_time->day <= 19)
            str->append(STRING_WITH_LEN("th"));
          else {
            switch (l_time->day % 10) {
              case 1:
                str->append(STRING_WITH_LEN("st"));
                break;
              case 2:
                str->append(STRING_WITH_LEN("nd"));
                break;
              case 3:
                str->append(STRING_WITH_LEN("rd"));
                break;
              default:
                str->append(STRING_WITH_LEN("th"));
                break;
            }
          }
          break;
        case 'Y':
          length = longlong10_to_str(l_time->year, intbuff, 10) - intbuff;
          str->append_with_prefill(intbuff, length, 4, '0');
          break;
        case 'y':
          length = longlong10_to_str(l_time->year % 100, intbuff, 10) - intbuff;
          str->append_with_prefill(intbuff, length, 2, '0');
          break;
        case 'm':
          length = longlong10_to_str(l_time->month, intbuff, 10) - intbuff;
          str->append_with_prefill(intbuff, length, 2, '0');
          break;
        case 'c':
          length = longlong10_to_str(l_time->month, intbuff, 10) - intbuff;
          str->append_with_prefill(intbuff, length, 1, '0');
          break;
        case 'd':
          length = longlong10_to_str(l_time->day, intbuff, 10) - intbuff;
          str->append_with_prefill(intbuff, length, 2, '0');
          break;
        case 'e':
          length = longlong10_to_str(l_time->day, intbuff, 10) - intbuff;
          str->append_with_prefill(intbuff, length, 1, '0');
          break;
        case 'f':
          length =
              longlong10_to_str(l_time->second_part, intbuff, 10) - intbuff;
          str->append_with_prefill(intbuff, length, 6, '0');
          break;
        case 'H':
          length = longlong10_to_str(l_time->hour, intbuff, 10) - intbuff;
          str->append_with_prefill(intbuff, length, 2, '0');
          break;
        case 'h':
        case 'I':
          hours_i = (l_time->hour % 24 + 11) % 12 + 1;
          length = longlong10_to_str(hours_i, intbuff, 10) - intbuff;
          str->append_with_prefill(intbuff, length, 2, '0');
          break;
        case 'i': /* minutes */
          length = longlong10_to_str(l_time->minute, intbuff, 10) - intbuff;
          str->append_with_prefill(intbuff, length, 2, '0');
          break;
        case 'j': {
          if (type == MYSQL_TIMESTAMP_TIME) return true;

          int radix = 10;
          int diff = calc_daynr(l_time->year, l_time->month, l_time->day) -
                     calc_daynr(l_time->year, 1, 1) + 1;
          if (diff < 0) radix = -10;

          length = longlong10_to_str(diff, intbuff, radix) - intbuff;
          str->append_with_prefill(intbuff, length, 3, '0');
        } break;
        case 'k':
          length = longlong10_to_str(l_time->hour, intbuff, 10) - intbuff;
          str->append_with_prefill(intbuff, length, 1, '0');
          break;
        case 'l':
          hours_i = (l_time->hour % 24 + 11) % 12 + 1;
          length = longlong10_to_str(hours_i, intbuff, 10) - intbuff;
          str->append_with_prefill(intbuff, length, 1, '0');
          break;
        case 'p':
          hours_i = l_time->hour % 24;
          str->append(hours_i < 12 ? "AM" : "PM", 2);
          break;
        case 'r':
          length = sprintf(intbuff,
                           ((l_time->hour % 24) < 12) ? "%02d:%02d:%02d AM"
                                                      : "%02d:%02d:%02d PM",
                           (l_time->hour + 11) % 12 + 1, l_time->minute,
                           l_time->second);
          str->append(intbuff, length);
          break;
        case 'S':
        case 's':
          length = longlong10_to_str(l_time->second, intbuff, 10) - intbuff;
          str->append_with_prefill(intbuff, length, 2, '0');
          break;
        case 'T':
          length = sprintf(intbuff, "%02d:%02d:%02d", l_time->hour,
                           l_time->minute, l_time->second);
          str->append(intbuff, length);
          break;
        case 'U':
        case 'u': {
          uint year;
          if (type == MYSQL_TIMESTAMP_TIME) return true;
          length =
              longlong10_to_str(calc_week(*l_time,
                                          (*ptr) == 'U' ? WEEK_FIRST_WEEKDAY
                                                        : WEEK_MONDAY_FIRST,
                                          &year),
                                intbuff, 10) -
              intbuff;
          str->append_with_prefill(intbuff, length, 2, '0');
        } break;
        case 'v':
        case 'V': {
          uint year;
          if (type == MYSQL_TIMESTAMP_TIME) return true;
          length =
              longlong10_to_str(
                  calc_week(*l_time,
                            ((*ptr) == 'V' ? (WEEK_YEAR | WEEK_FIRST_WEEKDAY)
                                           : (WEEK_YEAR | WEEK_MONDAY_FIRST)),
                            &year),
                  intbuff, 10) -
              intbuff;
          str->append_with_prefill(intbuff, length, 2, '0');
        } break;
        case 'x':
        case 'X': {
          uint year;
          if (type == MYSQL_TIMESTAMP_TIME) return true;
          (void)calc_week(*l_time,
                          ((*ptr) == 'X' ? WEEK_YEAR | WEEK_FIRST_WEEKDAY
                                         : WEEK_YEAR | WEEK_MONDAY_FIRST),
                          &year);
          length = longlong10_to_str(year, intbuff, 10) - intbuff;
          str->append_with_prefill(intbuff, length, 4, '0');
        } break;
        case 'w':
          if (type == MYSQL_TIMESTAMP_TIME || !(l_time->month || l_time->year))
            return true;
          weekday = calc_weekday(
              calc_daynr(l_time->year, l_time->month, l_time->day), true);
          length = longlong10_to_str(weekday, intbuff, 10) - intbuff;
          str->append_with_prefill(intbuff, length, 1, '0');
          break;

        default:
          str->append(*ptr);
          break;
      }
    }
  }
  return false;
}

/**
  @details
  Get a array of positive numbers from a string object.
  Each number is separated by 1 non digit character
  Return error if there is too many numbers.
  If there is too few numbers, assume that the numbers are left out
  from the high end. This allows one to give:
  DAY_TO_SECOND as "D MM:HH:SS", "MM:HH:SS" "HH:SS" or as seconds.

  @param args            item expression which we convert to an ASCII string
  @param str_value       string buffer
  @param is_negative     set to true if interval is prefixed by '-'
  @param count           count of elements in result array
  @param values          array of results
  @param transform_msec  if value is true we suppose
                         that the last part of string value is microseconds
                         and we should transform value to six digit value.
                         For example, '1.1' -> '1.100000'
*/

static bool get_interval_info(Item *args, String *str_value, bool *is_negative,
                              uint count, ulonglong *values,
                              bool transform_msec) {
  String *res;
  if (!(res = args->val_str_ascii(str_value))) return true;

  const CHARSET_INFO *cs = res->charset();
  const char *str = res->ptr();
  const char *end = str + res->length();

  str += cs->cset->scan(cs, str, end, MY_SEQ_SPACES);
  if (str < end && *str == '-') {
    *is_negative = true;
    str++;
  }

  while (str < end && !my_isdigit(cs, *str)) str++;

  long msec_length = 0;
  for (uint i = 0; i < count; i++) {
    longlong value;
    const char *start = str;
    for (value = 0; str != end && my_isdigit(cs, *str); str++) {
      if (value > (LLONG_MAX - 10) / 10) return true;
      value = value * 10LL + (longlong)(*str - '0');
    }
    msec_length = 6 - (str - start);
    values[i] = value;
    while (str != end && !my_isdigit(cs, *str)) str++;
    if (str == end && i != count - 1) {
      i++;
      /* Change values[0...i-1] -> values[0...count-1] */
      size_t len = sizeof(*values) * i;
      memmove(reinterpret_cast<uchar *>(values + count) - len,
              reinterpret_cast<uchar *>(values + i) - len, len);
      memset(values, 0, sizeof(*values) * (count - i));
      break;
    }
  }

  if (transform_msec && msec_length > 0)
    values[count - 1] *= (long)log_10_int[msec_length];

  return (str != end);
}

/*** Abstract classes ****************************************/

bool Item_temporal_func::check_precision() {
  if (decimals > DATETIME_MAX_DECIMALS) {
    my_error(ER_TOO_BIG_PRECISION, MYF(0), (int)decimals, func_name(),
             DATETIME_MAX_DECIMALS);
    return true;
  }
  return false;
}

/**
  Appends function name with argument list or fractional seconds part
  to the String str.

  @param[in]      thd         Thread handle
  @param[in,out]  str         String to which the func_name and decimals/
                              argument list should be appended.
  @param[in]      query_type  Query type

*/

void Item_temporal_func::print(const THD *thd, String *str,
                               enum_query_type query_type) const {
  str->append(func_name());
  str->append('(');

  // When the functions have arguments specified
  if (arg_count) {
    print_args(thd, str, 0, query_type);
  } else if (decimals) {
    /*
      For temporal functions like NOW, CURTIME and SYSDATE which can specify
      fractional seconds part.
    */
    if (unsigned_flag)
      str->append_ulonglong(decimals);
    else
      str->append_longlong(decimals);
  }

  str->append(')');
}

type_conversion_status Item_temporal_hybrid_func::save_in_field_inner(
    Field *field, bool no_conversions) {
  if (data_type() == MYSQL_TYPE_TIME) return save_time_in_field(field);
  if (is_temporal_type_with_date(data_type())) return save_date_in_field(field);
  return Item_str_func::save_in_field_inner(field, no_conversions);
}

my_decimal *Item_temporal_hybrid_func::val_decimal(my_decimal *decimal_value) {
  assert(fixed == 1);
  if (data_type() == MYSQL_TYPE_TIME)
    return val_decimal_from_time(decimal_value);
  else if (data_type() == MYSQL_TYPE_DATETIME)
    return val_decimal_from_date(decimal_value);
  else {
    MYSQL_TIME ltime;
    my_time_flags_t flags = TIME_FUZZY_DATE;
    if (sql_mode & MODE_NO_ZERO_IN_DATE) flags |= TIME_NO_ZERO_IN_DATE;
    if (sql_mode & MODE_NO_ZERO_DATE) flags |= TIME_NO_ZERO_DATE;
    if (sql_mode & MODE_INVALID_DATES) flags |= TIME_INVALID_DATES;

    val_datetime(&ltime, flags);
    return null_value ? nullptr
                      : ltime.time_type == MYSQL_TIMESTAMP_TIME
                            ? time2my_decimal(&ltime, decimal_value)
                            : date2my_decimal(&ltime, decimal_value);
  }
}

bool Item_temporal_hybrid_func::get_date(MYSQL_TIME *ltime,
                                         my_time_flags_t fuzzy_date) {
  MYSQL_TIME tm;
  if (val_datetime(&tm, fuzzy_date)) {
    assert(null_value == true);
    return true;
  }
  if (data_type() == MYSQL_TYPE_TIME || tm.time_type == MYSQL_TIMESTAMP_TIME)
    time_to_datetime(current_thd, &tm, ltime);
  else
    *ltime = tm;
  return false;
}

bool Item_temporal_hybrid_func::get_time(MYSQL_TIME *ltime) {
  if (val_datetime(ltime, TIME_FUZZY_DATE)) {
    assert(null_value == true);
    return true;
  }
  if (data_type() == MYSQL_TYPE_TIME &&
      ltime->time_type != MYSQL_TIMESTAMP_TIME)
    datetime_to_time(ltime);
  return false;
}

String *Item_temporal_hybrid_func::val_str_ascii(String *str) {
  assert(fixed == 1);
  MYSQL_TIME ltime;

  if (val_datetime(&ltime, TIME_FUZZY_DATE) ||
      (null_value =
           my_TIME_to_str(&ltime, str,
                          data_type() == MYSQL_TYPE_STRING
                              ? ltime.second_part ? DATETIME_MAX_DECIMALS : 0
                              : decimals)))
    return nullptr;

  /* Check that the returned timestamp type matches to the function type */
  assert((data_type() == MYSQL_TYPE_TIME &&
          ltime.time_type == MYSQL_TIMESTAMP_TIME) ||
         (data_type() == MYSQL_TYPE_DATE &&
          ltime.time_type == MYSQL_TIMESTAMP_DATE) ||
         (data_type() == MYSQL_TYPE_DATETIME &&
          ltime.time_type == MYSQL_TIMESTAMP_DATETIME) ||
         data_type() == MYSQL_TYPE_STRING ||
         ltime.time_type == MYSQL_TIMESTAMP_NONE);
  return str;
}

longlong Item_time_func::val_time_temporal() {
  assert(fixed == 1);
  MYSQL_TIME ltime;
  return get_time(&ltime) ? 0LL : TIME_to_longlong_time_packed(ltime);
}

longlong Item_date_func::val_date_temporal() {
  assert(fixed == 1);
  MYSQL_TIME ltime;
  return get_date(&ltime, TIME_FUZZY_DATE)
             ? 0LL
             : TIME_to_longlong_date_packed(ltime);
}

longlong Item_datetime_func::val_date_temporal() {
  assert(fixed == 1);
  MYSQL_TIME ltime;
  return get_date(&ltime, TIME_FUZZY_DATE)
             ? 0LL
             : TIME_to_longlong_datetime_packed(ltime);
}

bool Item_date_literal::eq(const Item *item, bool) const {
  return item->basic_const_item() && type() == item->type() &&
         strcmp(func_name(), down_cast<const Item_func *>(item)->func_name()) ==
             0 &&
         cached_time.eq(
             down_cast<const Item_date_literal *>(item)->cached_time);
}

void Item_date_literal::print(const THD *, String *str, enum_query_type) const {
  str->append("DATE'");
  str->append(cached_time.cptr());
  str->append('\'');
}

bool Item_datetime_literal::eq(const Item *item, bool) const {
  return item->basic_const_item() && type() == item->type() &&
         strcmp(func_name(), down_cast<const Item_func *>(item)->func_name()) ==
             0 &&
         cached_time.eq(
             down_cast<const Item_datetime_literal *>(item)->cached_time);
}

void Item_datetime_literal::print(const THD *, String *str,
                                  enum_query_type) const {
  str->append("TIMESTAMP'");
  str->append(cached_time.cptr());
  str->append('\'');
}

bool Item_time_literal::eq(const Item *item, bool) const {
  return item->basic_const_item() && type() == item->type() &&
         strcmp(func_name(), down_cast<const Item_func *>(item)->func_name()) ==
             0 &&
         cached_time.eq(
             down_cast<const Item_time_literal *>(item)->cached_time);
}

void Item_time_literal::print(const THD *, String *str, enum_query_type) const {
  str->append("TIME'");
  str->append(cached_time.cptr());
  str->append('\'');
}

bool Item_func_at_time_zone::resolve_type(THD *thd) {
  if (check_type()) return true;

  if (strcmp(specifier_string(), "+00:00") != 0 &&
      (m_is_interval || strcmp(specifier_string(), "UTC") != 0)) {
    my_error(ER_UNKNOWN_TIME_ZONE, MYF(0), specifier_string());
    return true;
  }

  return set_time_zone(thd);
}

bool Item_func_at_time_zone::set_time_zone(THD *thd) {
  String s(m_specifier_string, strlen(m_specifier_string),
           &my_charset_utf8mb3_bin);
  m_tz = my_tz_find(thd, &s);
  if (m_tz == nullptr) {
    my_error(ER_UNKNOWN_TIME_ZONE, MYF(0), m_specifier_string);
    return true;
  }
  return false;
}

bool Item_func_at_time_zone::get_date(MYSQL_TIME *res, my_time_flags_t flags) {
  my_timeval tm;
  int warnings = 0;

  if (args[0]->data_type() == MYSQL_TYPE_TIMESTAMP) {
    if (args[0]->get_timeval(&tm, &warnings)) {
      null_value = true;
      return true;
    }

    m_tz->gmt_sec_to_TIME(res, tm.m_tv_sec);
    return warnings != 0;
  }

  bool is_error = args[0]->get_date(res, flags);
  null_value = args[0]->null_value;
  if (is_error || null_value) return true;
  // Datetime value is in local time zone, convert to UTC:
  if (datetime_to_timeval(res, *current_thd->time_zone(), &tm, &warnings))
    return true;  // Value is out of the supported range
  // Finally, convert the temporal value to the desired time zone:
  m_tz->gmt_sec_to_TIME(res, tm.m_tv_sec);
  return warnings != 0;
}

bool Item_func_at_time_zone::check_type() const {
  if (args[0]->data_type() == MYSQL_TYPE_TIMESTAMP) return false;
  // A NULL literal must be allowed, and it has this type.
  if (args[0]->data_type() == MYSQL_TYPE_NULL) return false;

  if (args[0]->type() == Item::FUNC_ITEM &&
      down_cast<const Item_func *>(args[0])->functype() ==
          Item_func::DATETIME_LITERAL)
    return false;

  my_error(ER_INVALID_CAST, MYF(0), "TIMESTAMP WITH TIME ZONE");
  return true;
}

bool Item_func_period_add::resolve_type(THD *thd) {
  return param_type_is_default(thd, 0, -1, MYSQL_TYPE_LONGLONG);
}

longlong Item_func_period_add::val_int() {
  assert(fixed == 1);
  longlong period = args[0]->val_int();
  longlong months = args[1]->val_int();

  if ((null_value = args[0]->null_value || args[1]->null_value))
    return 0; /* purecov: inspected */
  if (!valid_period(period)) {
    my_error(ER_WRONG_ARGUMENTS, MYF(0), func_name());
    return error_int();
  }
  return convert_month_to_period(convert_period_to_month(period) + months);
}

bool Item_func_period_diff::resolve_type(THD *thd) {
  return param_type_is_default(thd, 0, -1, MYSQL_TYPE_LONGLONG);
}

longlong Item_func_period_diff::val_int() {
  assert(fixed == 1);
  longlong period1 = args[0]->val_int();
  longlong period2 = args[1]->val_int();

  if ((null_value = args[0]->null_value || args[1]->null_value))
    return 0; /* purecov: inspected */
  if (!valid_period(period1) || !valid_period(period2)) {
    my_error(ER_WRONG_ARGUMENTS, MYF(0), func_name());
    return error_int();
  }
  return static_cast<longlong>(convert_period_to_month(period1)) -
         static_cast<longlong>(convert_period_to_month(period2));
}

bool Item_func_to_days::resolve_type(THD *thd) {
  if (param_type_is_default(thd, 0, 1, MYSQL_TYPE_DATETIME)) return true;
  // The maximum string length returned by TO_DAYS is 7, as its range is
  // [0000-01-01, 9999-12-31] -> [0, 3652424]. Set the maximum length to one
  // higher, to account for the sign, even though the function never returns
  // negative values. (Needed in order to get decimal_precision() to return a
  // correct value.)
  fix_char_length(8);
  assert(decimal_precision() == 7);
  set_nullable(true);
  return false;
}

longlong Item_func_to_days::val_int() {
  assert(fixed == 1);
  MYSQL_TIME ltime;
  if (get_arg0_date(&ltime, TIME_NO_ZERO_DATE)) return 0;
  return (longlong)calc_daynr(ltime.year, ltime.month, ltime.day);
}

longlong Item_func_to_seconds::val_int_endpoint(bool, bool *) {
  assert(fixed == 1);
  MYSQL_TIME ltime;
  longlong seconds;
  longlong days;
  int dummy; /* unused */
  if (get_arg0_date(&ltime, TIME_FUZZY_DATE)) {
    /* got NULL, leave the incl_endp intact */
    return LLONG_MIN;
  }
  seconds = ltime.hour * 3600L + ltime.minute * 60 + ltime.second;
  seconds = ltime.neg ? -seconds : seconds;
  days = (longlong)calc_daynr(ltime.year, ltime.month, ltime.day);
  seconds += days * 24L * 3600L;
  /* Set to NULL if invalid date, but keep the value */
  null_value = check_date(ltime, non_zero_date(ltime),
                          (TIME_NO_ZERO_IN_DATE | TIME_NO_ZERO_DATE), &dummy);
  /*
    Even if the evaluation return NULL, seconds is useful for pruning
  */
  return seconds;
}

bool Item_func_to_seconds::resolve_type(THD *thd) {
  if (param_type_is_default(thd, 0, 1, MYSQL_TYPE_DATETIME)) return true;
  set_nullable(true);
  return false;
}

longlong Item_func_to_seconds::val_int() {
  assert(fixed == 1);
  MYSQL_TIME ltime;
  longlong seconds;
  longlong days;
  if (get_arg0_date(&ltime, TIME_NO_ZERO_DATE)) return 0;
  seconds = ltime.hour * 3600L + ltime.minute * 60 + ltime.second;
  seconds = ltime.neg ? -seconds : seconds;
  days = (longlong)calc_daynr(ltime.year, ltime.month, ltime.day);
  return seconds + days * 24L * 3600L;
}

/*
  Get information about this Item tree monotonicity

  SYNOPSIS
    Item_func_to_days::get_monotonicity_info()

  DESCRIPTION
  Get information about monotonicity of the function represented by this item
  tree.

  RETURN
    See enum_monotonicity_info.
*/

enum_monotonicity_info Item_func_to_days::get_monotonicity_info() const {
  if (args[0]->type() == Item::FIELD_ITEM) {
    if (args[0]->data_type() == MYSQL_TYPE_DATE)
      return MONOTONIC_STRICT_INCREASING_NOT_NULL;
    if (args[0]->data_type() == MYSQL_TYPE_DATETIME)
      return MONOTONIC_INCREASING_NOT_NULL;
  }
  return NON_MONOTONIC;
}

enum_monotonicity_info Item_func_to_seconds::get_monotonicity_info() const {
  if (args[0]->type() == Item::FIELD_ITEM) {
    if (args[0]->data_type() == MYSQL_TYPE_DATE ||
        args[0]->data_type() == MYSQL_TYPE_DATETIME)
      return MONOTONIC_STRICT_INCREASING_NOT_NULL;
  }
  return NON_MONOTONIC;
}

longlong Item_func_to_days::val_int_endpoint(bool left_endp, bool *incl_endp) {
  assert(fixed == 1);
  MYSQL_TIME ltime;
  longlong res;
  int dummy; /* unused */
  if (get_arg0_date(&ltime, TIME_FUZZY_DATE)) {
    /* got NULL, leave the incl_endp intact */
    return LLONG_MIN;
  }
  res = (longlong)calc_daynr(ltime.year, ltime.month, ltime.day);
  /* Set to NULL if invalid date, but keep the value */
  null_value = check_date(ltime, non_zero_date(ltime),
                          (TIME_NO_ZERO_IN_DATE | TIME_NO_ZERO_DATE), &dummy);
  if (null_value) {
    /*
      Even if the evaluation return NULL, the calc_daynr is useful for pruning
    */
    if (args[0]->data_type() != MYSQL_TYPE_DATE) *incl_endp = true;
    return res;
  }

  if (args[0]->data_type() == MYSQL_TYPE_DATE) {
    // TO_DAYS() is strictly monotonic for dates, leave incl_endp intact
    return res;
  }

  /*
    Handle the special but practically useful case of datetime values that
    point to day bound ("strictly less" comparison stays intact):

      col < '2007-09-15 00:00:00'  -> TO_DAYS(col) <  TO_DAYS('2007-09-15')
      col > '2007-09-15 23:59:59'  -> TO_DAYS(col) >  TO_DAYS('2007-09-15')

    which is different from the general case ("strictly less" changes to
    "less or equal"):

      col < '2007-09-15 12:34:56'  -> TO_DAYS(col) <= TO_DAYS('2007-09-15')
  */
  if ((!left_endp &&
       !(ltime.hour || ltime.minute || ltime.second || ltime.second_part)) ||
      (left_endp && ltime.hour == 23 && ltime.minute == 59 &&
       ltime.second == 59))
    /* do nothing */
    ;
  else
    *incl_endp = true;
  return res;
}

bool Item_func_dayofyear::resolve_type(THD *thd) {
  if (param_type_is_default(thd, 0, 1, MYSQL_TYPE_DATETIME)) return true;
  // Returns a value in the range [1, 366], so max three digits. Add one to the
  // character length for the sign.
  fix_char_length(4);
  assert(decimal_precision() == 3);
  set_nullable(true);
  return false;
}

longlong Item_func_dayofyear::val_int() {
  assert(fixed == 1);
  MYSQL_TIME ltime;
  if (get_arg0_date(&ltime, TIME_NO_ZERO_DATE)) return 0;
  return (longlong)calc_daynr(ltime.year, ltime.month, ltime.day) -
         calc_daynr(ltime.year, 1, 1) + 1;
}

bool Item_func_dayofmonth::resolve_type(THD *thd) {
  if (param_type_is_default(thd, 0, 1, MYSQL_TYPE_DATETIME)) return true;
  // Returns a value in the range [0, 31], so max two digits. Add one to the
  // character length for the sign.
  fix_char_length(3);
  assert(decimal_precision() == 2);
  set_nullable(true);
  return false;
}

longlong Item_func_dayofmonth::val_int() {
  assert(fixed == 1);
  MYSQL_TIME ltime;
  return get_arg0_date(&ltime, TIME_FUZZY_DATE) ? 0 : (longlong)ltime.day;
}

bool Item_func_month::resolve_type(THD *thd) {
  if (param_type_is_default(thd, 0, -1, MYSQL_TYPE_DATETIME)) return true;
  // Returns a value in the range [1, 12], so max two digits. Add one to the
  // character length for the sign.
  fix_char_length(3);
  assert(decimal_precision() == 2);
  set_nullable(true);
  return false;
}

longlong Item_func_month::val_int() {
  assert(fixed == 1);
  MYSQL_TIME ltime;
  return get_arg0_date(&ltime, TIME_FUZZY_DATE) ? 0 : (longlong)ltime.month;
}

bool Item_func_monthname::resolve_type(THD *thd) {
  if (param_type_is_default(thd, 0, -1, MYSQL_TYPE_DATETIME)) return true;
  const CHARSET_INFO *cs = thd->variables.collation_connection;
  uint32 repertoire = my_charset_repertoire(cs);
  locale = thd->variables.lc_time_names;
  collation.set(cs, DERIVATION_COERCIBLE, repertoire);
  set_data_type_string(locale->max_month_name_length);
  set_nullable(true);
  return false;
}

String *Item_func_monthname::val_str(String *str) {
  assert(fixed == 1);
  const char *month_name;
  uint err;
  MYSQL_TIME ltime;

  if ((null_value = (get_arg0_date(&ltime, TIME_FUZZY_DATE) || !ltime.month)))
    return (String *)nullptr;

  month_name = locale->month_names->type_names[ltime.month - 1];
  str->copy(month_name, strlen(month_name), &my_charset_utf8mb3_bin,
            collation.collation, &err);
  return str;
}

bool Item_func_quarter::resolve_type(THD *thd) {
  if (param_type_is_default(thd, 0, -1, MYSQL_TYPE_DATETIME)) return true;
  // Always one digit [1, 4]. Add one character for the sign.
  fix_char_length(2);
  assert(decimal_precision() == 1);
  set_nullable(true);
  return false;
}

/**
  Returns the quarter of the year.
*/

longlong Item_func_quarter::val_int() {
  assert(fixed == 1);
  MYSQL_TIME ltime;
  if (get_arg0_date(&ltime, TIME_FUZZY_DATE)) return 0;
  return (longlong)((ltime.month + 2) / 3);
}

bool Item_func_hour::resolve_type(THD *thd) {
  if (param_type_is_default(thd, 0, 1, MYSQL_TYPE_DATETIME)) return true;
  // Can have up to three digits (TIME_MAX_HOUR == 838). Add one for the sign.
  fix_char_length(4);
  assert(decimal_precision() == 3);
  set_nullable(true);
  return false;
}

longlong Item_func_hour::val_int() {
  assert(fixed == 1);
  MYSQL_TIME ltime;
  return get_arg0_time(&ltime) ? 0 : ltime.hour;
}

bool Item_func_minute::resolve_type(THD *thd) {
  if (param_type_is_default(thd, 0, -1, MYSQL_TYPE_DATETIME)) return true;
  // Can have up to two digits [0, 59]. Add one for the sign.
  fix_char_length(3);
  assert(decimal_precision() == 2);
  set_nullable(true);
  return false;
}

longlong Item_func_minute::val_int() {
  assert(fixed == 1);
  MYSQL_TIME ltime;
  return get_arg0_time(&ltime) ? 0 : ltime.minute;
}

bool Item_func_second::resolve_type(THD *thd) {
  if (param_type_is_default(thd, 0, 1, MYSQL_TYPE_DATETIME)) return true;
  // Can have up to two digits [0, 59]. Add one for the sign.
  fix_char_length(3);
  assert(decimal_precision() == 2);
  set_nullable(true);
  return false;
}

/**
  Returns the second in time_exp in the range of 0 - 59.
*/
longlong Item_func_second::val_int() {
  assert(fixed == 1);
  MYSQL_TIME ltime;
  return get_arg0_time(&ltime) ? 0 : ltime.second;
}

static uint week_mode(uint mode) {
  uint week_format = (mode & 7);
  if (!(week_format & WEEK_MONDAY_FIRST)) week_format ^= WEEK_FIRST_WEEKDAY;
  return week_format;
}

bool Item_func_week::itemize(Parse_context *pc, Item **res) {
  if (skip_itemize(res)) return false;
  if (args[1] == nullptr) {
    THD *thd = pc->thd;
    args[1] = new (pc->mem_root)
        Item_int(NAME_STRING("0"), thd->variables.default_week_format, 1);
    if (args[1] == nullptr) return true;
  }
  return super::itemize(pc, res);
}

bool Item_func_week::resolve_type(THD *thd) {
  if (param_type_is_default(thd, 0, 1, MYSQL_TYPE_DATETIME)) return true;
  if (param_type_is_default(thd, 1, 2, MYSQL_TYPE_LONGLONG)) return true;
  // Can have up to two digits [0, 53] (0 when using WEEK_YEAR, otherwise [1,
  // 53]). Add one for the sign.
  fix_char_length(3);
  assert(decimal_precision() == 2);
  set_nullable(true);
  return false;
}

/**
 @verbatim
  The bits in week_format(for calc_week() function) has the following meaning:
   WEEK_MONDAY_FIRST (0)  If not set	Sunday is first day of week
                          If set	Monday is first day of week
   WEEK_YEAR (1)	  If not set	Week is in range 0-53

        Week 0 is returned for the the last week of the previous year (for
        a date at start of january) In this case one can get 53 for the
        first week of next year.  This flag ensures that the week is
        relevant for the given year. Note that this flag is only
        relevant if WEEK_JANUARY is not set.

                          If set	 Week is in range 1-53.

        In this case one may get week 53 for a date in January (when
        the week is that last week of previous year) and week 1 for a
        date in December.

  WEEK_FIRST_WEEKDAY (2)  If not set	Weeks are numbered according
                                        to ISO 8601:1988
                          If set	The week that contains the first
                                        'first-day-of-week' is week 1.

        ISO 8601:1988 means that if the week containing January 1 has
        four or more days in the new year, then it is week 1;
        Otherwise it is the last week of the previous year, and the
        next week is week 1.
 @endverbatim
*/

longlong Item_func_week::val_int() {
  assert(fixed == 1);
  uint year;
  MYSQL_TIME ltime;
  if (get_arg0_date(&ltime, TIME_NO_ZERO_DATE)) return 0;
  return (longlong)calc_week(ltime, week_mode((uint)args[1]->val_int()), &year);
}

bool Item_func_yearweek::resolve_type(THD *thd) {
  if (param_type_is_default(thd, 0, 1, MYSQL_TYPE_DATETIME)) return true;
  if (param_type_is_default(thd, 1, 2, MYSQL_TYPE_LONGLONG)) return true;
  // Returns six digits (YYYYWW). Add one character for the sign.
  fix_char_length(7);
  assert(decimal_precision() == 6);
  set_nullable(true);
  return false;
}

longlong Item_func_yearweek::val_int() {
  assert(fixed == 1);
  uint year, week;
  MYSQL_TIME ltime;
  if (get_arg0_date(&ltime, TIME_NO_ZERO_DATE)) return 0;
  week = calc_week(ltime, (week_mode((uint)args[1]->val_int()) | WEEK_YEAR),
                   &year);
  return week + year * 100;
}

bool Item_func_weekday::resolve_type(THD *thd) {
  if (param_type_is_default(thd, 0, 1, MYSQL_TYPE_DATETIME)) return true;
  // Always one digit (either [0, 6] or [1, 7], depending on whether it's called
  // as WEEKDAY or DAYOFWEEK). Add one character for the sign.
  fix_char_length(2);
  set_nullable(true);
  return false;
}

longlong Item_func_weekday::val_int() {
  assert(fixed == 1);
  MYSQL_TIME ltime;

  if (get_arg0_date(&ltime, TIME_NO_ZERO_DATE)) return 0;

  return (longlong)calc_weekday(calc_daynr(ltime.year, ltime.month, ltime.day),
                                odbc_type) +
         odbc_type;
}

bool Item_func_dayname::resolve_type(THD *thd) {
  if (param_type_is_default(thd, 0, 1, MYSQL_TYPE_DATETIME)) return true;
  const CHARSET_INFO *cs = thd->variables.collation_connection;
  uint32 repertoire = my_charset_repertoire(cs);
  locale = thd->variables.lc_time_names;
  collation.set(cs, DERIVATION_COERCIBLE, repertoire);
  set_data_type_string(locale->max_day_name_length);
  set_nullable(true);
  return false;
}

String *Item_func_dayname::val_str(String *str) {
  assert(fixed == 1);
  uint weekday = (uint)val_int();  // Always Item_func_daynr()
  const char *day_name;
  uint err;

  if (null_value) return (String *)nullptr;

  day_name = locale->day_names->type_names[weekday];
  str->copy(day_name, strlen(day_name), &my_charset_utf8mb3_bin,
            collation.collation, &err);
  return str;
}

bool Item_func_year::resolve_type(THD *thd) {
  if (param_type_is_default(thd, 0, 1, MYSQL_TYPE_DATETIME)) return true;
  fix_char_length(5); /* 9999 plus sign */
  set_nullable(true);
  return false;
}

longlong Item_func_year::val_int() {
  assert(fixed == 1);
  MYSQL_TIME ltime;
  return get_arg0_date(&ltime, TIME_FUZZY_DATE) ? 0 : (longlong)ltime.year;
}

bool Item_typecast_year::resolve_type(THD *thd) {
  if (reject_geometry_args(arg_count, args, this)) return true;
  if (args[0]->propagate_type(thd, MYSQL_TYPE_YEAR, false, true)) return true;
  assert(decimal_precision() == 4);
  set_nullable(true);
  return false;
}

longlong Item_typecast_year::val_int() {
  assert(fixed == 1);
  longlong value{0};
  THD *thd = current_thd;
  null_value = false;

  // For temporal values, the YEAR value is extracted directly
  if (args[0]->is_temporal() && args[0]->data_type() != MYSQL_TYPE_YEAR) {
    MYSQL_TIME ltime;
    if (!get_arg0_date(&ltime, TIME_FUZZY_DATE))
      value = static_cast<longlong>(ltime.year);
  } else {
    bool is_int_type = args[0]->cast_to_int_type() != STRING_RESULT;
    // For numeric data types, the int value is extracted
    if (is_int_type) {
      value = args[0]->val_int();
      null_value = args[0]->null_value;
    } else {
      // For string-based data types, attempt int value conversion
      StringBuffer<STRING_BUFFER_USUAL_SIZE> string_buffer;
      const String *string_value;
      if (!(string_value = args[0]->val_str(&string_buffer))) {
        null_value = true;
        return 0;
      }
      const CHARSET_INFO *const cs = string_value->charset();
      const char *const start = string_value->ptr();
      const char *const end_of_string = start + string_value->length();
      const char *end_of_number = end_of_string;
      int error{0};
      value = cs->cset->strtoll10(cs, start, &end_of_number, &error);
      // Report here the error as we have access to the string value
      // extracted by val_str.
      if (error != 0) {
        ErrConvString err(string_value);
        push_warning_printf(current_thd, Sql_condition::SL_WARNING,
                            ER_WRONG_VALUE, ER_THD(current_thd, ER_WRONG_VALUE),
                            "YEAR", err.ptr());
        null_value = true;
        return 0;
      }
      if (end_of_number != end_of_string) {
        ErrConvString err(string_value);
        push_warning_printf(
            thd, Sql_condition::SL_WARNING, ER_TRUNCATED_WRONG_VALUE,
            ER_THD(current_thd, ER_TRUNCATED_WRONG_VALUE), "YEAR", err.ptr());
      }
    }
    // Only for string values we replace 0 with 2000
    if (!is_int_type && value == 0) value += 2000;
    // Values in the interval (0,70) represent years in the range [2000,2070)
    if (value > 0 && value < 70) value += 2000;
    // Values in the interval [70,100) represent years in the range [1970,2000)
    if (value >= 70 && value < 100) value += 1900;
  }
  // If date extraction failed or the YEAR value is outside the allowed range
  if (value > 2155 || (value < 1901 && (value != 0))) {
    ErrConvString err(value);
    push_warning_printf(
        thd, Sql_condition::SL_WARNING, ER_TRUNCATED_WRONG_VALUE,
        ER_THD(thd, ER_TRUNCATED_WRONG_VALUE), "YEAR", err.ptr());
    null_value = true;
    return 0;
  }

  return value;
}
/*
  Get information about this Item tree monotonicity

  SYNOPSIS
    Item_func_year::get_monotonicity_info()

  DESCRIPTION
  Get information about monotonicity of the function represented by this item
  tree.

  RETURN
    See enum_monotonicity_info.
*/

enum_monotonicity_info Item_func_year::get_monotonicity_info() const {
  if (args[0]->type() == Item::FIELD_ITEM &&
      (args[0]->data_type() == MYSQL_TYPE_DATE ||
       args[0]->data_type() == MYSQL_TYPE_DATETIME))
    return MONOTONIC_INCREASING;
  return NON_MONOTONIC;
}

longlong Item_func_year::val_int_endpoint(bool left_endp, bool *incl_endp) {
  assert(fixed == 1);
  MYSQL_TIME ltime;
  if (get_arg0_date(&ltime, TIME_FUZZY_DATE)) {
    /* got NULL, leave the incl_endp intact */
    return LLONG_MIN;
  }

  /*
    Handle the special but practically useful case of datetime values that
    point to year bound ("strictly less" comparison stays intact) :

      col < '2007-01-01 00:00:00'  -> YEAR(col) <  2007

    which is different from the general case ("strictly less" changes to
    "less or equal"):

      col < '2007-09-15 23:00:00'  -> YEAR(col) <= 2007
  */
  if (!left_endp && ltime.day == 1 && ltime.month == 1 &&
      !(ltime.hour || ltime.minute || ltime.second || ltime.second_part))
    ; /* do nothing */
  else
    *incl_endp = true;
  return ltime.year;
}

longlong Item_timeval_func::val_int() {
  my_timeval tm;
  return val_timeval(&tm) ? 0 : tm.m_tv_sec;
}

my_decimal *Item_timeval_func::val_decimal(my_decimal *decimal_value) {
  my_timeval tm;
  if (val_timeval(&tm)) {
    return error_decimal(decimal_value);
  }
  return timeval2my_decimal(&tm, decimal_value);
}

double Item_timeval_func::val_real() {
  my_timeval tm;
  return val_timeval(&tm)
             ? 0
             : (double)tm.m_tv_sec + (double)tm.m_tv_usec / (double)1000000;
}

String *Item_timeval_func::val_str(String *str) {
  my_timeval tm;
  if (val_timeval(&tm) || (null_value = str->alloc(MAX_DATE_STRING_REP_LENGTH)))
    return (String *)nullptr;
  str->length(my_timeval_to_str(&tm, str->ptr(), decimals));
  str->set_charset(collation.collation);
  return str;
}

bool Item_func_unix_timestamp::itemize(Parse_context *pc, Item **res) {
  if (skip_itemize(res)) return false;
  if (super::itemize(pc, res)) return true;
  if (arg_count == 0) pc->thd->lex->safe_to_cache_query = false;
  return false;
}

/**
   @retval true  args[0] is SQL NULL, so item is set to SQL NULL
   @retval false item's value is set, to 0 if out of range
*/
bool Item_func_unix_timestamp::val_timeval(my_timeval *tm) {
  assert(fixed == 1);
  if (arg_count == 0) {
    tm->m_tv_sec = current_thd->query_start_in_secs();
    tm->m_tv_usec = 0;
    return false;  // no args: null_value is set in constructor and is always 0.
  }
  int warnings = 0;
  return (null_value = args[0]->get_timeval(tm, &warnings));
}

enum_monotonicity_info Item_func_unix_timestamp::get_monotonicity_info() const {
  if (args[0]->type() == Item::FIELD_ITEM &&
      (args[0]->data_type() == MYSQL_TYPE_TIMESTAMP))
    return MONOTONIC_INCREASING;
  return NON_MONOTONIC;
}

longlong Item_func_unix_timestamp::val_int_endpoint(bool, bool *) {
  assert(fixed == 1);
  assert(arg_count == 1 && args[0]->type() == Item::FIELD_ITEM &&
         args[0]->data_type() == MYSQL_TYPE_TIMESTAMP);
  /* Leave the incl_endp intact */
  my_timeval tm;
  return val_timeval(&tm) ? 0 : tm.m_tv_sec;
}

bool Item_func_time_to_sec::resolve_type(THD *thd) {
  if (param_type_is_default(thd, 0, 1, MYSQL_TYPE_TIME)) return true;
  fix_char_length(10);
  set_nullable(true);
  return false;
}

longlong Item_func_time_to_sec::val_int() {
  assert(fixed == 1);
  MYSQL_TIME ltime;
  longlong seconds;
  if (get_arg0_time(&ltime)) return 0;
  seconds = ltime.hour * 3600L + ltime.minute * 60 + ltime.second;
  return ltime.neg ? -seconds : seconds;
}

/**
  Convert a string to a interval value.

  To make code easy, allow interval objects without separators.
*/

bool get_interval_value(Item *args, interval_type int_type, String *str_value,
                        Interval *interval) {
  ulonglong array[5];
  longlong value = 0;

  memset(interval, 0, sizeof(*interval));
  if (int_type == INTERVAL_SECOND && args->decimals) {
    my_decimal decimal_value, *val;
    lldiv_t tmp;
    if (!(val = args->val_decimal(&decimal_value))) return true;
    if (args->null_value) return true;
    int lldiv_result = my_decimal2lldiv_t(E_DEC_FATAL_ERROR, val, &tmp);
    if (lldiv_result == E_DEC_OVERFLOW) return true;

    if (tmp.quot >= 0 && tmp.rem >= 0) {
      interval->neg = false;
      interval->second = tmp.quot;
      interval->second_part = tmp.rem / 1000;
    } else {
      interval->neg = true;
      interval->second = -tmp.quot;
      interval->second_part = -tmp.rem / 1000;
    }
    return false;
  } else if (int_type <= INTERVAL_MICROSECOND) {
    value = args->val_int();
    if (args->null_value) return true;
    /*
      Large floating-point values will be truncated to LLONG_MIN / LLONG_MAX
      LLONG_MIN cannot be negated below, so reject it here.
    */
    if (value == LLONG_MIN) return true;
    if (value < 0) {
      interval->neg = true;
      value = -value;
    }
  }

  switch (int_type) {
    case INTERVAL_YEAR:
      interval->year = (ulong)value;
      break;
    case INTERVAL_QUARTER:
      if (value >= UINT_MAX / 3) return true;
      interval->month = (ulong)(value * 3);
      break;
    case INTERVAL_MONTH:
      interval->month = (ulong)value;
      break;
    case INTERVAL_WEEK:
      if (value >= UINT_MAX / 7) return true;
      interval->day = (ulong)(value * 7);
      break;
    case INTERVAL_DAY:
      interval->day = (ulong)value;
      break;
    case INTERVAL_HOUR:
      interval->hour = (ulong)value;
      break;
    case INTERVAL_MINUTE:
      interval->minute = value;
      break;
    case INTERVAL_SECOND:
      interval->second = value;
      break;
    case INTERVAL_MICROSECOND:
      interval->second_part = value;
      break;
    case INTERVAL_YEAR_MONTH:  // Allow YEAR-MONTH YYYYYMM
      if (get_interval_info(args, str_value, &interval->neg, 2, array, false))
        return true;
      interval->year = (ulong)array[0];
      interval->month = (ulong)array[1];
      break;
    case INTERVAL_DAY_HOUR:
      if (get_interval_info(args, str_value, &interval->neg, 2, array, false))
        return true;
      interval->day = (ulong)array[0];
      interval->hour = (ulong)array[1];
      break;
    case INTERVAL_DAY_MINUTE:
      if (get_interval_info(args, str_value, &interval->neg, 3, array, false))
        return true;
      interval->day = (ulong)array[0];
      interval->hour = (ulong)array[1];
      interval->minute = array[2];
      break;
    case INTERVAL_DAY_SECOND:
      if (get_interval_info(args, str_value, &interval->neg, 4, array, false))
        return true;
      interval->day = (ulong)array[0];
      interval->hour = (ulong)array[1];
      interval->minute = array[2];
      interval->second = array[3];
      break;
    case INTERVAL_HOUR_MINUTE:
      if (get_interval_info(args, str_value, &interval->neg, 2, array, false))
        return true;
      interval->hour = (ulong)array[0];
      interval->minute = array[1];
      break;
    case INTERVAL_HOUR_SECOND:
      if (get_interval_info(args, str_value, &interval->neg, 3, array, false))
        return true;
      interval->hour = (ulong)array[0];
      interval->minute = array[1];
      interval->second = array[2];
      break;
    case INTERVAL_MINUTE_SECOND:
      if (get_interval_info(args, str_value, &interval->neg, 2, array, false))
        return true;
      interval->minute = array[0];
      interval->second = array[1];
      break;
    case INTERVAL_DAY_MICROSECOND:
      if (get_interval_info(args, str_value, &interval->neg, 5, array, true))
        return true;
      interval->day = (ulong)array[0];
      interval->hour = (ulong)array[1];
      interval->minute = array[2];
      interval->second = array[3];
      interval->second_part = array[4];
      break;
    case INTERVAL_HOUR_MICROSECOND:
      if (get_interval_info(args, str_value, &interval->neg, 4, array, true))
        return true;
      interval->hour = (ulong)array[0];
      interval->minute = array[1];
      interval->second = array[2];
      interval->second_part = array[3];
      break;
    case INTERVAL_MINUTE_MICROSECOND:
      if (get_interval_info(args, str_value, &interval->neg, 3, array, true))
        return true;
      interval->minute = array[0];
      interval->second = array[1];
      interval->second_part = array[2];
      break;
    case INTERVAL_SECOND_MICROSECOND:
      if (get_interval_info(args, str_value, &interval->neg, 2, array, true))
        return true;
      interval->second = array[0];
      interval->second_part = array[1];
      break;
    case INTERVAL_LAST: /* purecov: begin deadcode */
      assert(0);
      break; /* purecov: end */
  }
  return false;
}

bool Item_func_from_days::get_date(MYSQL_TIME *ltime,
                                   my_time_flags_t fuzzy_date) {
  longlong value = args[0]->val_int();
  if ((null_value = args[0]->null_value)) return true;
  memset(ltime, 0, sizeof(MYSQL_TIME));
  get_date_from_daynr(value, &ltime->year, &ltime->month, &ltime->day);

  if (check_datetime_range(*ltime)) {
    // Value is out of range, cannot use our printing functions to output it.
    push_warning_printf(
        current_thd, Sql_condition::SL_WARNING, ER_DATETIME_FUNCTION_OVERFLOW,
        ER_THD(current_thd, ER_DATETIME_FUNCTION_OVERFLOW), func_name());
    null_value = true;
    return true;
  }

  if ((null_value = (fuzzy_date & TIME_NO_ZERO_DATE) &&
                    (ltime->year == 0 || ltime->month == 0 || ltime->day == 0)))
    return true;

  ltime->time_type = MYSQL_TIMESTAMP_DATE;
  return false;
}

void MYSQL_TIME_cache::set_time(MYSQL_TIME *ltime, uint8 dec_arg) {
  assert(ltime->time_type == MYSQL_TIMESTAMP_TIME);
  time = *ltime;
  time_packed = TIME_to_longlong_time_packed(time);
  dec = dec_arg;
  string_length = my_TIME_to_str(time, string_buff, decimals());
}

void MYSQL_TIME_cache::set_date(MYSQL_TIME *ltime) {
  assert(ltime->time_type == MYSQL_TIMESTAMP_DATE);
  time = *ltime;
  time_packed = TIME_to_longlong_date_packed(time);
  dec = 0;
  string_length = my_TIME_to_str(time, string_buff, decimals());
}

void MYSQL_TIME_cache::set_datetime(MYSQL_TIME *ltime, uint8 dec_arg,
                                    const Time_zone *tz) {
  assert(ltime->time_type == MYSQL_TIMESTAMP_DATETIME ||
         ltime->time_type == MYSQL_TIMESTAMP_DATETIME_TZ);
  time = *ltime;
  if (convert_time_zone_displacement(tz, &time)) {
    assert(false);
  }
  time_packed = TIME_to_longlong_datetime_packed(time);

  dec = dec_arg;
  string_length = my_TIME_to_str(time, string_buff, decimals());
}

void MYSQL_TIME_cache::set_datetime(my_timeval tv, uint8 dec_arg,
                                    Time_zone *tz) {
  tz->gmt_sec_to_TIME(&time, tv);
  time_packed = TIME_to_longlong_datetime_packed(time);
  dec = dec_arg;
  string_length = my_TIME_to_str(time, string_buff, decimals());
}

void MYSQL_TIME_cache::set_date(my_timeval tv, Time_zone *tz) {
  tz->gmt_sec_to_TIME(&time, (my_time_t)tv.m_tv_sec);
  time.time_type = MYSQL_TIMESTAMP_DATE;
  /* We don't need to set second_part and neg because they are already 0 */
  time.hour = time.minute = time.second = 0;
  time_packed = TIME_to_longlong_date_packed(time);
  dec = 0;
  string_length = my_TIME_to_str(time, string_buff, decimals());
}

void MYSQL_TIME_cache::set_time(my_timeval tv, uint8 dec_arg, Time_zone *tz) {
  tz->gmt_sec_to_TIME(&time, tv);
  datetime_to_time(&time);
  time_packed = TIME_to_longlong_time_packed(time);
  dec = dec_arg;
  string_length = my_TIME_to_str(time, string_buff, decimals());
}

bool MYSQL_TIME_cache::get_date(MYSQL_TIME *ltime,
                                my_time_flags_t fuzzydate) const {
  int warnings;
  get_TIME(ltime);
  return check_date(*ltime, non_zero_date(*ltime), fuzzydate, &warnings);
}

String *MYSQL_TIME_cache::val_str(String *str) {
  str->set(string_buff, string_length, &my_charset_latin1);
  return str;
}

/* CURDATE() and UTC_DATE() */

bool Item_func_curdate::itemize(Parse_context *pc, Item **res) {
  if (skip_itemize(res)) return false;
  if (super::itemize(pc, res)) return true;
  pc->thd->lex->safe_to_cache_query = false;
  return false;
}

bool Item_func_curdate::resolve_type(THD *thd) {
  if (Item_date_func::resolve_type(thd)) return true;
  return false;
}

Time_zone *Item_func_curdate_local::time_zone() {
  return current_thd->time_zone();
}

Time_zone *Item_func_curdate_utc::time_zone() { return my_tz_UTC; }

longlong Item_func_curdate::val_date_temporal() {
  assert(fixed == 1);
  MYSQL_TIME_cache tm;
  tm.set_date(current_thd->query_start_timeval_trunc(decimals), time_zone());
  return tm.val_packed();
}

bool Item_func_curdate::get_date(MYSQL_TIME *res, my_time_flags_t) {
  assert(fixed == 1);
  MYSQL_TIME_cache tm;
  tm.set_date(current_thd->query_start_timeval_trunc(decimals), time_zone());
  return tm.get_time(res);
}

String *Item_func_curdate::val_str(String *str) {
  assert(fixed == 1);
  MYSQL_TIME_cache tm;
  tm.set_date(current_thd->query_start_timeval_trunc(decimals), time_zone());
  if (str->alloc(10)) return nullptr;

  str->set_charset(&my_charset_numeric);
  str->length(my_TIME_to_str(*tm.get_TIME_ptr(), (char *)str->ptr(), decimals));

  return str;
}

/* CURTIME() and UTC_TIME() */

bool Item_func_curtime::itemize(Parse_context *pc, Item **res) {
  if (skip_itemize(res)) return false;
  if (super::itemize(pc, res)) return true;
  pc->thd->lex->safe_to_cache_query = false;
  return false;
}

bool Item_func_curtime::resolve_type(THD *) {
  if (check_precision()) return true;

  set_data_type_time(decimals);

  /*
    Subtract 2 from MAX_TIME_WIDTH (which is 10) because:
    - there is no sign
    - hour is in the 2-digit range
  */
  max_length -= 2 * collation.collation->mbmaxlen;

  return false;
}

longlong Item_func_curtime::val_time_temporal() {
  assert(fixed == 1);
  MYSQL_TIME_cache tm;
  tm.set_time(current_thd->query_start_timeval_trunc(decimals), decimals,
              time_zone());
  return tm.val_packed();
}

bool Item_func_curtime::get_time(MYSQL_TIME *ltime) {
  assert(fixed == 1);
  MYSQL_TIME_cache tm;
  tm.set_time(current_thd->query_start_timeval_trunc(decimals), decimals,
              time_zone());
  return tm.get_time(ltime);
}

String *Item_func_curtime::val_str(String *str) {
  assert(fixed == 1);
  MYSQL_TIME_cache tm;
  tm.set_time(current_thd->query_start_timeval_trunc(decimals), decimals,
              time_zone());
  if (str->alloc(15)) return nullptr;

  str->set_charset(&my_charset_numeric);
  str->length(my_TIME_to_str(*tm.get_TIME_ptr(), (char *)str->ptr(), decimals));

  return str;
}

Time_zone *Item_func_curtime_local::time_zone() {
  return current_thd->time_zone();
}

Time_zone *Item_func_curtime_utc::time_zone() { return my_tz_UTC; }

/* NOW() and UTC_TIMESTAMP () */

bool Item_func_now::resolve_type(THD *) {
  if (check_precision()) return true;

  set_data_type_datetime(decimals);

  return false;
}

void Item_func_now_local::store_in(Field *field) {
  THD *thd = current_thd;
  const my_timeval tm = thd->query_start_timeval_trunc(field->decimals());
  field->set_notnull();
  return field->store_timestamp(&tm);
}

Time_zone *Item_func_now_local::time_zone() { return current_thd->time_zone(); }

bool Item_func_now_utc::itemize(Parse_context *pc, Item **res) {
  if (skip_itemize(res)) return false;
  if (super::itemize(pc, res)) return true;
  pc->thd->lex->safe_to_cache_query = false;
  return false;
}

Time_zone *Item_func_now_utc::time_zone() { return my_tz_UTC; }

longlong Item_func_now::val_date_temporal() {
  assert(fixed == 1);
  MYSQL_TIME_cache tm;
  tm.set_datetime(current_thd->query_start_timeval_trunc(decimals), decimals,
                  time_zone());
  return tm.val_packed();
}

bool Item_func_now::get_date(MYSQL_TIME *res, my_time_flags_t) {
  assert(fixed == 1);
  MYSQL_TIME_cache tm;
  tm.set_datetime(current_thd->query_start_timeval_trunc(decimals), decimals,
                  time_zone());
  return tm.get_time(res);
}

String *Item_func_now::val_str(String *str) {
  assert(fixed == 1);
  MYSQL_TIME_cache tm;
  tm.set_datetime(current_thd->query_start_timeval_trunc(decimals), decimals,
                  time_zone());
  if (str->alloc(26)) return nullptr;

  str->set_charset(&my_charset_numeric);
  str->length(my_TIME_to_str(*tm.get_TIME_ptr(), (char *)str->ptr(), decimals));

  return str;
}

type_conversion_status Item_func_now::save_in_field_inner(Field *to, bool) {
  to->set_notnull();
  MYSQL_TIME_cache tm;
  tm.set_datetime(current_thd->query_start_timeval_trunc(decimals), decimals,
                  time_zone());

  return to->store_time(tm.get_TIME_ptr(), decimals);
}

/**
    Converts current time in my_time_t to MYSQL_TIME representation for local
    time zone. Defines time zone (local) used for whole SYSDATE function.
*/
bool Item_func_sysdate_local::get_date(MYSQL_TIME *now_time,
                                       my_time_flags_t fuzzy_date
                                       [[maybe_unused]]) {
  THD *thd = current_thd;
  ulonglong tmp = my_micro_time();
  thd->time_zone()->gmt_sec_to_TIME(now_time, (my_time_t)(tmp / 1000000));
  if (decimals) {
    now_time->second_part = tmp % 1000000;
    my_datetime_trunc(now_time, decimals);
  }
  return false;
}

bool Item_func_sysdate_local::resolve_type(THD *) {
  if (check_precision()) return true;
  set_data_type_datetime(decimals);
  return false;
}

bool Item_func_sec_to_time::get_time(MYSQL_TIME *ltime) {
  my_decimal tmp, *val = args[0]->val_decimal(&tmp);
  lldiv_t seconds;
  if ((null_value = args[0]->null_value)) return true;
  if (my_decimal2lldiv_t(0, val, &seconds)) {
    set_max_time(ltime, val->sign());
    return make_truncated_value_warning(current_thd, Sql_condition::SL_WARNING,
                                        ErrConvString(val),
                                        MYSQL_TIMESTAMP_TIME, NullS);
  }
  if (sec_to_time(seconds, ltime))
    return make_truncated_value_warning(current_thd, Sql_condition::SL_WARNING,
                                        ErrConvString(val),
                                        MYSQL_TIMESTAMP_TIME, NullS);
  return false;
}

typedef enum {
  NUM_BASE = 128,
  NUM_COMMA,   // ,
  NUM_DEC,     // .
  NUM_DOLLAR,  // $
  NUM_0,
  NUM_9,
  NUM_B,   // fill zero
  NUM_C,   //  skip CNY
  NUM_D,   // like .
  NUM_E,   // e+
  NUM_G,   //  $
  NUM_L,   // skip
  NUM_MI,  // -
  NUM_PR,  // <>
  NUM_RN,  // skip roman
  NUM_rn,  // skip roman
  NUM_TM9,
  NUM_TME,
  NUM_S,      // +d
  NUM_S_END,  // d+
  NUM_V,      // *10^n
  NUM_x,      // hex
  NUM_X,      // hex uppercase
  /* last */
  NUM_BASE_END
} FORMAT_ORACLE_NUMBER_TYPE_E;

#define INVALID_CHARACTER(x)                                   \
  (((x) >= 'A' && (x) <= 'Z') || ((x) >= 'a' && (x) <= 'z') || \
   ((x) >= '0' && (x) <= '9') || (x) >= 127 || ((x) < 32))

/**
  Modify the quotation flag and check whether the subsequent process is skipped

  @param cftm             Character or FMT... format descriptor
  @param quotation_flag   Points to 'true' if we are inside a quoted string

  @return true  If we are inside a quoted string or if we found a '"' character
  @return false Otherwise
*/
inline bool check_quotation(uint16 cfmt, bool *quotation_flag) {
  if (cfmt == '"') {
    *quotation_flag = !*quotation_flag;
    return true;
  }
  return *quotation_flag;
}

/**
  Parse the format string, convert it to an compact array and calculate the
  length of output string

  @param format   Format string
  @param fmt_len  Function will store max length of formated date string here

*/
bool generate_date_format_array(const String *format, const MY_LOCALE *locale,
                                const CHARSET_INFO *cs, uint16 *fmt_array,
                                uint *fmt_len) {
  const char *ptr, *end;
  uint16 *tmp_fmt = fmt_array;
  uint tmp_len = 0;
  bool quotation_flag = false;
  const char *old_ptr = nullptr;
  const char *fmt_array_pre_ptr = nullptr;

  if (!format) {
    *fmt_array = 0;
    fmt_len = 0;
    return false;
  }
  ptr = format->ptr();
  end = ptr + format->length();

  if (format->length() > MAX_DATETIME_FORMAT_MODEL_LEN) {
    *fmt_len = MAX_DATETIME_FORMAT_MODEL_LEN + 1;
    return false;
  }
  for (; ptr < end; ptr++, tmp_fmt++) {
    uint ulen;
    char cfmt, next_char;

    cfmt = *ptr;
    old_ptr = ptr;
    /*
      Oracle datetime format support text in double quotation marks like
      'YYYY"abc"MM"xyz"DD', When this happens, store the text and quotation
      marks, and use the text as a separator in make_date_time_oracle.

      NOTE: the quotation mark is not print in return value. for example:
      select TO_CHAR(sysdate, 'YYYY"abc"MM"xyzDD"') will return 2021abc01xyz11
     */
    if (check_quotation(cfmt, &quotation_flag)) {
      *tmp_fmt = *ptr;
      tmp_len += 1;
      continue;
    }

    switch (cfmt) {
      case 'a':
        if (ptr + 1 >= end) goto error;
        next_char = my_toupper(system_charset_info, *(ptr + 1));
        if ('D' == next_char) {
          *tmp_fmt = FMT_ad;
          ptr += 1;
          tmp_len += 2;
        } else if ('M' == next_char) {
          *tmp_fmt = FMT_am;
          ptr += 1;
          tmp_len += 2;
        } else if ('.' == next_char && ptr + 3 < end && '.' == *(ptr + 3)) {
          if ('D' == my_toupper(system_charset_info, *(ptr + 2))) {
            *tmp_fmt = FMT_ad_DOT;
            ptr += 3;
            tmp_len += 4;
          } else if ('M' == my_toupper(system_charset_info, *(ptr + 2))) {
            *tmp_fmt = FMT_am_DOT;
            ptr += 3;
            tmp_len += 4;
          } else
            goto error;
        } else
          goto error;
        break;

      case 'A':
        if (ptr + 1 >= end) goto error;
        next_char = *(ptr + 1);
        if ('D' == next_char) {
          *tmp_fmt = FMT_AD;
          ptr += 1;
          tmp_len += 2;
        } else if ('d' == next_char) {
          *tmp_fmt = FMT_Ad;
          ptr += 1;
          tmp_len += 2;
        } else if ('m' == next_char) {
          *tmp_fmt = FMT_Am;
          ptr += 1;
          tmp_len += 2;
        } else if ('M' == next_char) {
          *tmp_fmt = FMT_AM;
          ptr += 1;
          tmp_len += 2;
        } else if ('.' == next_char && ptr + 3 < end && '.' == *(ptr + 3)) {
          if ('D' == my_toupper(system_charset_info, *(ptr + 2))) {
            *tmp_fmt = FMT_AD_DOT;
            ptr += 3;
            tmp_len += 4;
          } else if ('M' == my_toupper(system_charset_info, *(ptr + 2))) {
            *tmp_fmt = FMT_AM_DOT;
            ptr += 3;
            tmp_len += 4;
          } else
            goto error;
        } else
          goto error;
        break;
      case 'b':
        if (ptr + 1 >= end) goto error;
        next_char = my_toupper(system_charset_info, *(ptr + 1));
        if ('C' == next_char) {
          *tmp_fmt = FMT_bc;
          ptr += 1;
          tmp_len += 2;
        } else if ('.' == next_char && ptr + 3 < end && '.' == *(ptr + 3) &&
                   'C' == my_toupper(system_charset_info, *(ptr + 2))) {
          *tmp_fmt = FMT_bc_DOT;
          ptr += 3;
          tmp_len += 4;
        } else
          goto error;
        break;
      case 'B':  // BC and B.C
        if (ptr + 1 >= end) goto error;
        next_char = *(ptr + 1);
        if ('C' == next_char) {
          *tmp_fmt = FMT_BC;
          ptr += 1;
          tmp_len += 2;
        } else if (next_char == 'c') {
          *tmp_fmt = FMT_Bc;
          ptr += 1;
          tmp_len += 2;
        } else if (next_char == '.' && ptr + 3 < end && *(ptr + 3) == '.' &&
                   my_toupper(system_charset_info, *(ptr + 2)) == 'C') {
          *tmp_fmt = FMT_BC_DOT;
          ptr += 3;
          tmp_len += 4;
        } else
          goto error;
        break;
      case 'c':
      case 'C': {
        if (ptr + 1 < end) {
          char tmp1 = my_toupper(system_charset_info, *(ptr + 1));
          if ('C' == tmp1) {
            *tmp_fmt = FMT_CC;
            tmp_len += 2;
          } else {
            goto error;
          }
          ptr += 1;
        } else {
          goto error;
        }
      } break;
      case 'd': {
        if (ptr + 1 < end) {
          char tmp1 = my_toupper(system_charset_info, *(ptr + 1));
          if ('D' == tmp1) {
            if (ptr + 2 < end &&
                my_toupper(system_charset_info, *(ptr + 2)) == 'D') {
              *tmp_fmt = FMT_DDD;
              tmp_len += 3;
              ptr += 2;
            } else {
              *tmp_fmt = FMT_DD;
              tmp_len += 2;
              ptr += 1;
            }
          } else if ('Y' == tmp1) {  // DY
            *tmp_fmt = FMT_dy;
            tmp_len += 3;
            ptr += 1;
          } else if ('A' == tmp1) {
            if (ptr + 2 >= end ||
                'Y' != my_toupper(system_charset_info, *(ptr + 2))) {
              goto error;
            }
            *tmp_fmt = FMT_day;
            tmp_len += locale->max_day_name_length * cs->mbmaxlen;
            ptr += 2;
          } else {
            *tmp_fmt = FMT_D;
            tmp_len += 1;
          }
        } else {
          *tmp_fmt = FMT_D;
          tmp_len += 1;
        }
      } break;
      case 'D':  // D, DD, DDD, DY, DAY
      {
        if (ptr + 1 < end) {
          char tmp1 = *(ptr + 1);
          switch (tmp1) {
            case 'd':
            case 'D':
              if (ptr + 2 < end &&
                  my_toupper(system_charset_info, *(ptr + 2)) == 'D') {
                *tmp_fmt = FMT_DDD;
                tmp_len += 3;
                ptr += 2;
              } else {
                *tmp_fmt = FMT_DD;
                tmp_len += 2;
                ptr += 1;
              }
              break;
            case 'y':
              *tmp_fmt = FMT_Dy;
              tmp_len += 3;
              ptr += 1;
              break;
            case 'Y':
              *tmp_fmt = FMT_DY;
              tmp_len += 3;
              ptr += 1;
              break;
            case 'a':
              if (ptr + 2 == end ||
                  my_toupper(system_charset_info, *(ptr + 2)) != 'Y') {
                goto error;
              }
              *tmp_fmt = FMT_Day;
              tmp_len += locale->max_day_name_length * cs->mbmaxlen;
              ptr += 2;
              break;
            case 'A':
              if (ptr + 2 == end ||
                  my_toupper(system_charset_info, *(ptr + 2)) != 'Y') {
                goto error;
              }
              *tmp_fmt = FMT_DAY;
              tmp_len += locale->max_day_name_length * cs->mbmaxlen;
              ptr += 2;
              break;
            default:
              *tmp_fmt = FMT_D;
              tmp_len += 1;
              break;
          }
        } else {
          *tmp_fmt = FMT_D;
          tmp_len += 1;
        }
      } break;
      case 'f':
      case 'F':
        if (ptr + 1 == end) goto error;

        switch (my_toupper(system_charset_info, *(ptr + 1))) {
          case 'F':
            *tmp_fmt = FMT_FF;
            if (ptr + 2 != end) {
              uint16 i = (uint16) * (ptr + 2) - 48;
              if (i > 0 && i <= DATETIME_MAX_DECIMALS) {
                tmp_fmt++;
                *tmp_fmt = i;
                tmp_len += i;
                ptr += 2;
              } else {
                // when digit number is not specified, set it to 6
                tmp_fmt++;
                *tmp_fmt = DATETIME_MAX_DECIMALS;
                tmp_len += DATETIME_MAX_DECIMALS;
                ptr += 1;
              }
            } else {
              tmp_len += 2;
              ptr += 1;
            }
            break;
          case 'M':
            *tmp_fmt = FMT_FM;
            ptr += 1;
            break;
          case 'X':
            *tmp_fmt = FMT_FX;
            ptr += 1;
            break;
          default:
            goto error;
        }
        break;
      case 'h':
      case 'H':  // HH, HH12 or HH24
      {
        char tmp1, tmp2, tmp3;
        if (ptr + 1 >= end) goto error;
        tmp1 = my_toupper(system_charset_info, *(ptr + 1));

        if (tmp1 != 'H') goto error;

        if (ptr + 3 >= end) {
          *tmp_fmt = FMT_HH;
          ptr += 1;
        } else {
          tmp2 = *(ptr + 2);
          tmp3 = *(ptr + 3);

          if ('1' == tmp2 && '2' == tmp3) {
            *tmp_fmt = FMT_HH12;
            ptr += 3;
          } else if ('2' == tmp2 && '4' == tmp3) {
            *tmp_fmt = FMT_HH24;
            ptr += 3;
          } else {
            *tmp_fmt = FMT_HH;
            ptr += 1;
          }
        }
        tmp_len += 2;
      } break;
      case 'i':
      case 'I':
        if (ptr + 1 < end &&
            'W' == my_toupper(system_charset_info, *(ptr + 1))) {
          *tmp_fmt = FMT_IW;
          tmp_len += 2;
          ptr += 1;
        } else {
          if (ptr + 1 == end ||
              my_toupper(system_charset_info, *(ptr + 1)) != 'Y') {
            *tmp_fmt = FMT_I;
            ulen = 0;
          } else if (ptr + 2 == end ||
                     my_toupper(system_charset_info, *(ptr + 2)) != 'Y') {
            *tmp_fmt = FMT_IY;
            ulen = 1;
          } else if (ptr + 3 == end ||
                     my_toupper(system_charset_info, *(ptr + 3)) != 'Y') {
            *tmp_fmt = FMT_IYY;
            ulen = 2;
          } else {
            *tmp_fmt = FMT_IYYY;
            ulen = 3;
          }
          tmp_len += ulen + 1;
          ptr += ulen;
        }
        break;
      case 'j':
      case 'J':
        *tmp_fmt = FMT_J;
        tmp_len += 7;
        break;
      case 'm': {
        char tmp1;
        if (ptr + 1 >= end) goto error;
        tmp1 = my_toupper(system_charset_info, *(ptr + 1));
        if ('M' == tmp1) {
          *tmp_fmt = FMT_MM;
          tmp_len += 2;
          ptr += 1;
        } else if ('I' == tmp1) {
          *tmp_fmt = FMT_MI;
          tmp_len += 2;
          ptr += 1;
        } else if ('O' == tmp1) {
          if (ptr + 2 >= end ||
              'N' != my_toupper(system_charset_info, *(ptr + 2)))
            goto error;

          if (ptr + 4 >= end ||
              my_toupper(system_charset_info, *(ptr + 3)) != 'T' ||
              my_toupper(system_charset_info, *(ptr + 4)) != 'H') {
            *tmp_fmt = FMT_mon;
            tmp_len += 3;
            ptr += 2;
          } else {
            *tmp_fmt = FMT_month;
            tmp_len += (locale->max_month_name_length * cs->mbmaxlen);
            ptr += 4;
          }
        } else
          goto error;
      } break;
      case 'M': {
        char tmp1;
        if (ptr + 1 >= end) goto error;
        tmp1 = my_toupper(system_charset_info, *(ptr + 1));
        if ('M' == tmp1) {
          *tmp_fmt = FMT_MM;
          tmp_len += 2;
          ptr += 1;
        } else if ('I' == tmp1) {
          *tmp_fmt = FMT_MI;
          tmp_len += 2;
          ptr += 1;
        } else if ('O' == tmp1) {
          if (ptr + 2 >= end ||
              'N' != my_toupper(system_charset_info, *(ptr + 2)))
            goto error;

          if (ptr + 4 >= end ||
              my_toupper(system_charset_info, *(ptr + 3)) != 'T' ||
              my_toupper(system_charset_info, *(ptr + 4)) != 'H') {
            if ('o' == *(ptr + 1)) {
              *tmp_fmt = FMT_Mon;
            } else {
              *tmp_fmt = FMT_MON;
            }
            tmp_len += 3;
            ptr += 2;
          } else {
            if ('o' == *(ptr + 1)) {
              *tmp_fmt = FMT_Month;
            } else {
              *tmp_fmt = FMT_MONTH;
            }
            tmp_len += (locale->max_month_name_length * cs->mbmaxlen);
            ptr += 4;
          }
        } else
          goto error;
      } break;
      case 'p':
        if (ptr + 1 >= end) goto error;
        next_char = my_toupper(system_charset_info, *(ptr + 1));
        if ('M' == next_char) {
          *tmp_fmt = FMT_pm;
          ptr += 1;
          tmp_len += 2;
        } else if ('.' == next_char && ptr + 3 < end && '.' == *(ptr + 3) &&
                   'M' == my_toupper(system_charset_info, *(ptr + 2))) {
          *tmp_fmt = FMT_pm_DOT;
          ptr += 3;
          tmp_len += 4;
        } else
          goto error;
        break;
      case 'P':  // PM or P.M.
        if (ptr + 1 >= end) goto error;
        next_char = *(ptr + 1);
        if ('M' == next_char) {
          *tmp_fmt = FMT_PM;
          ptr += 1;
          tmp_len += 2;
        } else if ('m' == next_char) {
          *tmp_fmt = FMT_Pm;
          ptr += 1;
          tmp_len += 2;
        } else if ('.' == next_char && ptr + 3 < end && '.' == *(ptr + 3) &&
                   'M' == my_toupper(system_charset_info, *(ptr + 2))) {
          *tmp_fmt = FMT_PM_DOT;
          ptr += 3;
          tmp_len += 4;
        } else
          goto error;
        break;
      case 'q':
      case 'Q':
        *tmp_fmt = FMT_Q;
        tmp_len += 1;
        break;
      case 'r':
      case 'R':  // RM、RR or RRRR
        if (ptr + 1 == end) {
          goto error;
        }

        if ('M' == my_toupper(system_charset_info, *(ptr + 1))) {
          if ('r' == *ptr) {
            *tmp_fmt = FMT_rm;
          } else if ('m' == *(ptr + 1)) {
            *tmp_fmt = FMT_Rm;
          } else {
            *tmp_fmt = FMT_RM;
          }
          ptr += 1;
          tmp_len += 4;
        } else if ('R' == my_toupper(system_charset_info, *(ptr + 1))) {
          if (ptr + 2 == end ||
              my_toupper(system_charset_info, *(ptr + 2)) != 'R') {
            *tmp_fmt = FMT_RR;
            ulen = 2;
          } else {
            if (ptr + 3 >= end ||
                my_toupper(system_charset_info, *(ptr + 3)) != 'R') {
              goto error;
            }
            *tmp_fmt = FMT_RRRR;
            ulen = 4;
          }
          ptr += ulen - 1;
          tmp_len += ulen;
        } else {
          goto error;
        }
        break;
      case 's':
      case 'S':  // SS or SSSSS
        if ((end - ptr) > 4 &&
            'S' == my_toupper(system_charset_info, *(ptr + 1)) &&
            'S' == my_toupper(system_charset_info, *(ptr + 2)) &&
            'S' == my_toupper(system_charset_info, *(ptr + 3)) &&
            'S' == my_toupper(system_charset_info, *(ptr + 4))) {
          *tmp_fmt = FMT_SSSSS;
          tmp_len += 5;
          ptr += 4;
        } else if ((ptr + 1) < end &&
                   'S' == my_toupper(system_charset_info, *(ptr + 1))) {
          *tmp_fmt = FMT_SS;
          tmp_len += 2;
          ptr += 1;
        } else {
          goto error;
        }
        break;
      case 't':
      case 'T':
        if (ptr + 1 < end) {
          if ('H' == my_toupper(system_charset_info, *(ptr + 1))) {
            if (!fmt_array_pre_ptr) goto error;
            bool is_upper = my_isupper(&my_charset_latin1, *fmt_array_pre_ptr);
            if (is_upper) {
              if ((ptr - fmt_array_pre_ptr) == 1) {
                is_upper = ('T' == *ptr);
              } else {
                ulen = 1;
                while ((fmt_array_pre_ptr + ulen) < ptr) {
                  if (my_isalpha(&my_charset_latin1,
                                 *(fmt_array_pre_ptr + ulen))) {
                    is_upper = my_isupper(&my_charset_latin1,
                                          *(fmt_array_pre_ptr + ulen));
                    break;
                  }
                  ulen += 1;
                }
              }
            }
            *tmp_fmt = is_upper ? FMT_TH : FMT_th;
            tmp_len += 2;
            ptr += 1;
          } else {
            goto error;
          }
        } else {
          goto error;
        }
        break;
      case 'w':
      case 'W':
        if (ptr + 1 == end ||
            'W' != my_toupper(system_charset_info, *(ptr + 1))) {
          *tmp_fmt = FMT_W;
          tmp_len += 1;
        } else {
          *tmp_fmt = FMT_WW;
          tmp_len += 2;
          ptr += 1;
        }
        break;
      case 'x':
      case 'X':
        *tmp_fmt = FMT_X;
        break;
      case 'y':
      case 'Y':  // Y, YY, YYY, YYYYY or Y,YYY YEAR
        if (ptr + 3 < end &&
            'E' == my_toupper(system_charset_info, *(ptr + 1)) &&
            'A' == my_toupper(system_charset_info, *(ptr + 2)) &&
            'R' == my_toupper(system_charset_info, *(ptr + 3))) { /* YEAR */
          if ('y' == *(ptr)) {
            *tmp_fmt = FMT_year;
          } else if ('e' == *(ptr + 1)) {
            *tmp_fmt = FMT_Year;
          } else {
            *tmp_fmt = FMT_YEAR;
          }
          ulen = 4;
          /**
            (9 + 5 + 1 + 9) * 3 + 2 = 74
            - 9: the max length of the word(multiples of 10) in
            NUM_TO_WORD_MAP, eg: SEVENTEEN
            - 5: the max length of the word(less than 10) in
            NUM_TO_WORD_MAP, eg: SEVEN
            - 1: connector, eg: '-' in 'TWENTY-ONE'
            - 9: the max length of the suffix, eg: ' THOUSAND'
            - 3: mean year will be split into 3 parts, the max length of
            each part is (9 + 5 + 1 + 9)
            - 2: add two spaces as separator between 3 parts
          */
          tmp_len += 74;
        } else if (ptr + 4 < end && *(ptr + 1) == ',' &&
                   'Y' == my_toupper(system_charset_info, *(ptr + 2)) &&
                   'Y' == my_toupper(system_charset_info, *(ptr + 3)) &&
                   'Y' == my_toupper(system_charset_info,
                                     *(ptr + 4))) { /* Y,YYY */
          *tmp_fmt = FMT_YYYY_COMMA;
          ulen = 5;
          tmp_len += ulen;
        } else {
          if (ptr + 1 == end ||
              my_toupper(system_charset_info, *(ptr + 1)) != 'Y') {
            *tmp_fmt = FMT_Y;
            tmp_len += 1;
            break;
          }
          if (ptr + 2 == end ||
              my_toupper(system_charset_info, *(ptr + 2)) != 'Y') /* YY */
          {
            *tmp_fmt = FMT_YY;
            ulen = 2;
          } else {
            if (ptr + 3 < end &&
                'Y' == my_toupper(system_charset_info, *(ptr + 3))) {
              *tmp_fmt = FMT_YYYY;
              ulen = 4;
            } else {
              *tmp_fmt = FMT_YYY;
              ulen = 3;
            }
          }
          tmp_len += ulen;
        }
        ptr += ulen - 1;
        break;
      case '|':
        if (tmp_len > 0 && '"' == my_toupper(system_charset_info, *(ptr - 1))) {
          *tmp_fmt++ = *ptr;
          tmp_len++;
        }
        /*
          If only one '|' just ignore it, else append others, for example:
          TO_CHAR('2000-11-05', 'YYYY|MM||||DD') --> 200011|||05
        */
        if ((ptr + 1) == end || *(ptr + 1) != '|') {
          tmp_fmt--;
          break;
        }
        ptr++;
        do {
          *tmp_fmt++ = *ptr++;
          tmp_len++;
        } while ((ptr < end) && '|' == *ptr);
        ptr--;  // Fix ptr for above for loop
        tmp_fmt--;
        break;
      case '&':
        /* '&' with other
         *special charaters like '|'.'*' is used as separator
         */

        *tmp_fmt++ = *ptr++;
        tmp_len++;

        do {
          if (ptr == end) {
            tmp_fmt--;
            ptr--;
            break;
          }
          next_char = my_toupper(system_charset_info, *ptr);
          if (INVALID_CHARACTER(next_char) || '"' == next_char) {
            tmp_fmt--;
            ptr--;
            break;
          }
          *tmp_fmt++ = *ptr++;
          tmp_len++;
        } while (1);
        break;

      default:
        if (INVALID_CHARACTER(cfmt)) {
          goto error;
        } else {
          *tmp_fmt = cfmt;
          tmp_len++;
        }
        break;
    }

    fmt_array_pre_ptr = old_ptr;
  }
  *fmt_len = tmp_len;
  *tmp_fmt = 0;
  return true;

error:
  DBUG_PRINT("debug",
             ("parser date format: %s , offset %s", format->ptr(), ptr));

  return false;
}

inline bool append_val(int val, int size, String *str) {
  ulong len = 0;
  char intbuff[15];
  len = (ulong)(longlong10_to_str(val, intbuff, 10) - intbuff);
  return str->append_with_prefill(intbuff, len, size, '0');
}

inline bool append_fill(int val, int size, String *str) {
  char intbuff[15];
  ulong len = (ulong)(longlong10_to_str(val, intbuff, 10) - intbuff);

  len = len > (ulong)size ? (ulong)size : len;
  auto fill_len = str->length() + size;

  if (str->append(intbuff, len)) return true;
  return str->fill(fill_len, '0');
}

bool Item_func_to_char::generate_number_format_array(const String *format,
                                                     uint *fmt_len) {
  const char *ptr, *end;
  uint16 *tmp_fmt_int = num_format->fmt_inter;

  uint16 *tmp_fmt_dec = num_format->fmt_decimal;

  if (!format) {
    fmt_len = 0;
    return false;
  }

  ptr = format->ptr();
  end = ptr + format->length();

  if (format->length() > DECIMAL_MAX_FIELD_SIZE) {
    *fmt_len = DECIMAL_MAX_FIELD_SIZE + 1;
    return false;
  }
  // locale->decimal_point;
  //
  char cfmt;
  for (; ptr < end; ptr++) {
    cfmt = my_toupper(system_charset_info, *ptr);

    switch (cfmt) {
      case 'F':  // skip FM
        if (tmp_fmt_int != num_format->fmt_inter) goto error;
        if (my_toupper(system_charset_info, *(ptr + 1)) != 'M') goto error;
        num_format->FM = true;
        ptr++;
        break;
      case '.':  // NUM_DEC
        if (num_format->proint) goto error;
        num_format->proint = NUM_DEC;

        break;
      // case 'L': // china:￥
      // case 'C':  CHINA: CNY
      case '$':  //
        if (num_format->is_dollar) goto error;
        num_format->is_dollar = NUM_DOLLAR;
        break;

      case ',':  // NUM_COMMA: //
        if (tmp_fmt_int == num_format->fmt_inter) goto error;
        if (num_format->proint) goto error;
        *tmp_fmt_int = NUM_COMMA;
        tmp_fmt_int++;

        break;
      case '0':  // NUM_0:

        if (num_format->proint) {
          *tmp_fmt_dec = NUM_0;
          tmp_fmt_dec++;
          // last .0 offset
          num_format->fixed_prec_end = tmp_fmt_dec - num_format->fmt_decimal;
        } else {
          *tmp_fmt_int = NUM_0;
          tmp_fmt_int++;
          num_format->fixed_prec++;
          num_format->intg++;
        }

        break;
      case '9':  // NUM_9:
        if (num_format->proint) {
          *tmp_fmt_dec = NUM_9;
          tmp_fmt_dec++;

        } else {
          *tmp_fmt_int = NUM_9;
          tmp_fmt_int++;
          if (num_format->fixed_prec) {
            num_format->fixed_prec++;
          }
          num_format->intg++;
        }

        break;

      case 'B':
        if (num_format->zero_return_empty) goto error;
        num_format->zero_return_empty = true;

        break;

      case 'D':  // like .
        if (num_format->proint) goto error;
        num_format->proint = NUM_D;

        break;

      case 'E':  // ‘EEEE’
        if (tmp_fmt_int == num_format->fmt_inter ||
            my_toupper(system_charset_info, *(ptr + 1)) != 'E' ||
            my_toupper(system_charset_info, *(ptr + 2)) != 'E' ||
            my_toupper(system_charset_info, *(ptr + 3)) != 'E' ||
            ptr + 4 != end)
          goto error;

        num_format->is_EEEE = NUM_E;
        ptr += 3;
        break;
      case 'G':  // like ,
        if (tmp_fmt_int == num_format->fmt_inter || num_format->proint)
          goto error;

        *tmp_fmt_int = NUM_G;
        tmp_fmt_int++;

        break;
      case 'M':
        if (ptr + 2 != end ||
            my_toupper(system_charset_info, *(ptr + 1)) != 'I' ||
            num_format->is_minus)
          goto error;
        num_format->is_minus = NUM_MI;
        ptr++;

        break;

      case 'P':  // <>
        if (ptr + 2 != end ||
            my_toupper(system_charset_info, *(ptr + 1)) != 'R' ||
            num_format->is_minus)
          goto error;
        num_format->is_minus = NUM_PR;

        ptr++;
        break;
      /*
      case 'R': //ROMAN
        if(tmp_fmt_int != num_format->fmt_inter || (ptr+2 != end) ||
         my_toupper(system_charset_info, *(ptr+1)) != 'N')
          goto error;

        if (*ptr!= 'r')
        *tmp_fmt_int = NUM_RN;
        else
        *tmp_fmt_int = NUM_rn;
        tmp_len =  15; // roman max length
        break;
      */
      case 'T':
        if (tmp_fmt_int != num_format->fmt_inter ||
            my_toupper(system_charset_info, *(ptr + 1)) != 'M')
          goto error;
        num_format->FM = true;
        ptr++;
        if ((ptr + 1) != end) {
          if (my_toupper(system_charset_info, *(ptr + 1)) == 'E') {
            num_format->is_EEEE = NUM_TME;
          } else if (my_toupper(system_charset_info, *(ptr + 1)) != '9') {
            goto error;
          } else {
            num_format->is_EEEE = NUM_TM9;
          }
          ptr++;
        } else {
          num_format->is_EEEE = NUM_TM9;
        }
        break;
      case 'S':
        // only set once
        if (num_format->is_minus) goto error;

        if (ptr == format->ptr()) {
          num_format->is_minus = NUM_S;
        } else if ((ptr + 1) == end) {
          num_format->is_minus = NUM_S_END;
        } else
          goto error;
        break;
      case 'V':
        if (num_format->proint || num_format->is_EEEE) goto error;

        for (ptr++; ptr < end; ptr++) {
          cfmt = my_toupper(system_charset_info, *ptr);
          if (cfmt != '9' && cfmt != '0') {
            ptr--;  // back loop
            break;
          }
          num_format->fillzero++;
          *tmp_fmt_int = NUM_9;
          tmp_fmt_int++;
          if (num_format->fixed_prec) {
            num_format->fixed_prec++;
          }
          // num_format->intg++;
        }
        break;
      case 'X':
        if (num_format->proint || num_format->is_minus ||
            num_format->is_dollar || num_format->is_EEEE)
          goto error;
        // xxxx only have 0
        num_format->fixed_prec = 0;
        for (uint16 *pp = num_format->fmt_inter; pp != tmp_fmt_int; pp++) {
          switch (*pp) {
            case NUM_9:
              goto error;
              break;
            case NUM_0:
              num_format->fixed_prec++;
              break;
          }
        }
        num_format->is_EEEE = NUM_X;
        num_format->intg = 0;
        for (; ptr != end; ptr++) {
          if (my_toupper(system_charset_info, *(ptr)) != 'X') {
            goto error;
          } else {
            if (*ptr == 'x') {
              num_format->is_EEEE = NUM_x;
            }
            num_format->intg++;
          }
        }

        break;
      default:
        goto error;
    }
  }
  // end to '\0'
  num_format->fmt_inter_len = tmp_fmt_int - num_format->fmt_inter;
  *tmp_fmt_int = 0;

  num_format->fmt_decimal_len = tmp_fmt_dec - num_format->fmt_decimal;

  *tmp_fmt_dec = 0;

  num_format->format_length = num_format->fmt_inter_len;
  if (num_format->is_minus == NUM_PR) {
    num_format->format_length += 2;
  } else {
    num_format->format_length += 1;  // <> len: 2
  }
  if (num_format->is_dollar != 0) {
    num_format->format_length += 1;
  }

  if (num_format->proint) {
    num_format->format_length += 1;
    if (num_format->FM) {
      num_format->format_length += num_format->fixed_prec_end;
    } else
      num_format->format_length += num_format->fmt_decimal_len;
  }

  switch (num_format->is_EEEE) {
    case NUM_x:
    case NUM_X:
      num_format->format_length += num_format->intg;
      break;
    case NUM_E:
      //   sign + inter 1， +
      num_format->format_length = 2;
      if (num_format->proint) {
        num_format->format_length += 1;
        num_format->format_length += num_format->fmt_decimal_len;
      }
      num_format->format_length += 5;

      break;
    case NUM_TME:
    case NUM_TM9:
      num_format->format_length = DECIMAL_MAX_FIELD_SIZE;
      break;
  }

  *fmt_len = num_format->format_length;
  return true;

error:
  return false;
}

/*
only datetime

https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Format-Models.html#GUID-096CA64F-1DA3-4C49-A18B-ECC7518EE56C

*/
static const char *get_th(int num, bool upper) {
  int last = num % 10;
  if (1 == (num / 10) % 10) {
    last = 0;
  }

  switch (last) {
    case 1:
      return upper ? "ST" : "st";
    case 2:
      return upper ? "ND" : "nd";
    case 3:
      return upper ? "RD" : "rd";
    default:
      return upper ? "TH" : "th";
  }
}

static const std::map<int, const char *> NUM_TO_UPPER_WORD_MAP = {
    {0, "ZERO"},     {1, "ONE"},        {2, "TWO"},       {3, "THREE"},
    {4, "FOUR"},     {5, "FIVE"},       {6, "SIX"},       {7, "SEVEN"},
    {8, "EIGHT"},    {9, "NINE"},       {10, "TEN"},      {11, "ELEVEN"},
    {12, "TWELVE"},  {13, "THIRTEEN"},  {14, "FOURTEEN"}, {15, "FIFTEEN"},
    {16, "SIXTEEN"}, {17, "SEVENTEEN"}, {18, "EIGHTEEN"}, {19, "NINETEEN"},
    {20, "TWENTY"},  {30, "THIRTY"},    {40, "FORTY"},    {50, "FIFTY"},
    {60, "SIXTY"},   {70, "SEVENTY"},   {80, "EIGHTY"},   {90, "NINETY"}};

static const std::map<int, const char *> NUM_TO_LOWER_WORD_MAP = {
    {0, "zero"},     {1, "one"},        {2, "two"},       {3, "three"},
    {4, "four"},     {5, "five"},       {6, "six"},       {7, "seven"},
    {8, "eight"},    {9, "nine"},       {10, "ten"},      {11, "eleven"},
    {12, "twelve"},  {13, "thirteen"},  {14, "fourteen"}, {15, "fifteen"},
    {16, "sixteen"}, {17, "seventeen"}, {18, "eighteen"}, {19, "nineteen"},
    {20, "twenty"},  {30, "thirty"},    {40, "forty"},    {50, "fifty"},
    {60, "sixty"},   {70, "seventy"},   {80, "eighty"},   {90, "ninety"}};

static const std::map<int, const char *> NUM_TO_INITIAL_UPPER_WORD_MAP = {
    {0, "Zero"},     {1, "One"},        {2, "Two"},       {3, "Three"},
    {4, "Four"},     {5, "Five"},       {6, "Six"},       {7, "Seven"},
    {8, "Eight"},    {9, "Nine"},       {10, "Ten"},      {11, "Eleven"},
    {12, "Twelve"},  {13, "Thirteen"},  {14, "Fourteen"}, {15, "Fifteen"},
    {16, "Sixteen"}, {17, "Seventeen"}, {18, "Eighteen"}, {19, "Nineteen"},
    {20, "Twenty"},  {30, "Thirty"},    {40, "Forty"},    {50, "Fifty"},
    {60, "Sixty"},   {70, "Seventy"},   {80, "Eighty"},   {90, "Ninety"}};

/**
  Convert year number to english representation
  Use for to_char function in YEAR format element
  @param year year number
  @param flag Represents the conversion mode
          >0 Return upper english representation
          =0 Return initial upper english representation
          <0 Return lower english representation
  @retval english representation for year number
*/
static void year_spelled_out(int32_t n_year, String *str, int flag) {
  const char *thousand = " Thousand";
  const char *hundred = " Hundred";
  const std::map<int, const char *> *num_to_word =
      &NUM_TO_INITIAL_UPPER_WORD_MAP;
  if (flag > 0) {
    num_to_word = &NUM_TO_UPPER_WORD_MAP;
    thousand = " THOUSAND";
    hundred = " HUNDRED";
  } else if (flag < 0) {
    num_to_word = &NUM_TO_LOWER_WORD_MAP;
    thousand = " thousand";
    hundred = " hundred";
  }

  if (n_year == 0) {
    str->append(num_to_word->at(n_year));
  } else {
    auto convert = [&str, &num_to_word](int part, const char *suffix,
                                        bool need_sep) {
      if (part > 0) {
        if (need_sep) {
          str->append(" ");
        }

        if (part < 20 || part % 10 == 0) {
          str->append(num_to_word->at(part));
          str->append(suffix);
        } else {
          str->append(num_to_word->at(part - part % 10));
          str->append("-");
          str->append(num_to_word->at(part % 10));
          str->append(suffix);
        }
      }
    };

    uint32_t part1, part2, part3;
    uint32_t y = std::abs(n_year);

    part1 = y % 100;
    /**
      1. Divide n_year into three parts: part3, part2, part1，eg: 2009 is 2,0,09
       a. when n_year % 100 < 10, the part2 is only one digit, eg：2,0,09
       b. when n_year % 100 >= 10, each part is two digit, eg：20，10 or 3,27,10
      2. If each part is <20 or divisible by 10, use the part directly to access
      NUM_TO_WORD_MAP to get the corresponding word string, otherwise, take the
      tenth part first and then the one-digit part, and use the '-' symbol to
      connect，eg：22 is take NUM_TO_WORD_MAP[20]-NUM_TO_WORD_MAP[2]
      3. if n_year % 100 <10，The word string "THOUSAND" needs to be appended
      after part3, otherwise the word string "HUNDRED" needs to be appended
      4. if n_year % 100 >= 10，The word string "HUNDRED" needs to be appended
      after part2, otherwise the word string will not be appended
      5. part1 does not append the word string
    */
    if (part1 < 10) {
      part3 = y / 1000 % 100;
      part2 = y / 100 % 10;

      /* handle part3 */
      convert(part3, thousand, false);
      /* handle part2 */
      convert(part2, hundred, part3 > 0);
      /* handle part1 */
      convert(part1, "", part3 > 0 || part2 > 0);
    } else {
      part3 = y / 10000 % 100;
      part2 = y / 100 % 100;
      /* handle part3 */
      convert(part3, hundred, false);
      /* handle part2 */
      convert(part2, "", part3 > 0);
      /* handle part1 */
      convert(part1, "", part3 > 0 || part2 > 0);
    }
  }
}

/**
  Use for to_char function in handle th suffix
  @param str A string to append the suffix
  @param ptr A pointer to format array
  @param num Used to calculate the suffix
  @param singal_char_fm true True if the current value of the ptr is a single
  character element, False otherwise
  @param upper_suffix true True if the current value of the ptr indicates that
  the uppercase suffix needs to be returned, False otherwise

  eg：
    if FM is 'DTH', then append uppercase suffix
    else if FM is 'DtH' or 'dTH', then append lowercase suffix

    if FM is 'DDTH' or 'DDth', then append uppercase suffix
    else if FM is 'DdTH' or 'dDTH' or 'ddTH', then append lowercase suffix
*/
static void handle_suffix(String *str, const uint16 **ptr, int num) {
  assert(str && ptr && *ptr);
  if (*ptr + 1) {
    if (FMT_th == *(*ptr + 1)) {
      str->append(get_th(num, false));
      *ptr += 1;
    } else if (FMT_TH == *(*ptr + 1)) {
      str->append(get_th(num, true));
      *ptr += 1;
    }
  }
}

bool Item_func_to_char::make_date_str_oracle(const MYSQL_TIME *l_time,
                                             String *str) {
  bool quotation_flag = false;
  const uint16 *ptr = datetime_format_array;
  uint hours_i;
  uint weekday;
  int tmp_num = 0;
  //  month max len 11 * mblen: 3 = 33;
  char buffer[34] = {0};
  String tmpStr(buffer, 34, collation.collation);
  const char *name_ptr;
  size_t name_len, char_len;

  bool fmt_fill = true;
  str->length(0);

  while (*ptr) {
    if (check_quotation(*ptr, &quotation_flag)) {
      /* don't display '"' in the result, so if it is '"', skip it */
      if (*ptr != '"') str->append(*ptr);
      ptr++;
      continue;
    }
    switch (*ptr) {
      case FMT_AM:
      case FMT_PM:
        if (l_time->hour > 11)
          str->append("PM", 2);
        else
          str->append("AM", 2);
        break;
      case FMT_Pm:
      case FMT_Am:
        if (l_time->hour > 11)
          str->append("Pm", 2);
        else
          str->append("Am", 2);
        break;
      case FMT_pm:
      case FMT_am:
        if (l_time->hour > 11)
          str->append("pm", 2);
        else
          str->append("am", 2);
        break;
      case FMT_AM_DOT:
      case FMT_PM_DOT:
        if (l_time->hour > 11)
          str->append(STRING_WITH_LEN("P.M."));
        else
          str->append(STRING_WITH_LEN("A.M."));
        break;

      case FMT_am_DOT:
      case FMT_pm_DOT:
        if (l_time->hour > 11)
          str->append(STRING_WITH_LEN("p.m."));
        else
          str->append(STRING_WITH_LEN("p.m."));
        break;
      case FMT_ad:
      case FMT_bc:
        if (l_time->year > 0)
          str->append(STRING_WITH_LEN("ad"));
        else
          str->append(STRING_WITH_LEN("bc"));
        break;
      case FMT_AD:
      case FMT_BC:
        if (l_time->year > 0)
          str->append(STRING_WITH_LEN("AD"));
        else
          str->append(STRING_WITH_LEN("BC"));
        break;
      case FMT_Ad:
      case FMT_Bc:
        if (l_time->year > 0)
          str->append(STRING_WITH_LEN("Ad"));
        else
          str->append(STRING_WITH_LEN("Bc"));
        break;
      case FMT_AD_DOT:
      case FMT_BC_DOT:
        if (l_time->year > 0)
          str->append(STRING_WITH_LEN("A.D."));
        else
          str->append(STRING_WITH_LEN("B.C."));
        break;
      case FMT_ad_DOT:
      case FMT_bc_DOT:
        if (l_time->year > 0)
          str->append(STRING_WITH_LEN("a.d."));
        else
          str->append(STRING_WITH_LEN("b.c."));
        break;
      case FMT_CC:
        if (l_time->year == 0 || l_time->year > 9900) {
          my_error(ER_STD_OUT_OF_RANGE_ERROR, MYF(0),
                   "year must be between 0001 and 9900", func_name());
          goto err_exit;
        }
        tmp_num = (l_time->year - 1) / 100 + 1;
        if (append_val(std::abs(tmp_num), (fmt_fill) ? 2 : 0, str)) {
          goto err_exit;
        }
        handle_suffix(str, &ptr, tmp_num);
        break;
      case FMT_D:
        /*
        Mode	First day of week	Range	Week 1 is the first week …
        0	Sunday	0-53	with a Sunday in this year
        1	Monday	0-53	with 4 or more days this year
        2	Sunday	1-53	with a Sunday in this year
        3	Monday	1-53	with 4 or more days this year
        4	Sunday	0-53	with 4 or more days this year
        5	Monday	0-53	with a Monday in this year
        6	Sunday	1-53	with 4 or more days this year
        7	Monday	1-53	with a Monday in this year
        */

        weekday =
            calc_weekday(calc_daynr(l_time->year, l_time->month, l_time->day),
                         !(current_thd->variables.default_week_format) % 2) +
            1;
        if (append_val(weekday, 1, str)) goto err_exit;
        handle_suffix(str, &ptr, weekday);
        break;
      case FMT_day:
      case FMT_Day:
      case FMT_DAY: {
        if (l_time->day == 0)
          str->append("00", 2);
        else {
          weekday = calc_weekday(
              calc_daynr(l_time->year, l_time->month, l_time->day), 0);
          name_ptr = locale->day_names->type_names[weekday];
          name_len = strlen(name_ptr);
          tmpStr.copy(name_ptr, name_len, collation.collation);
          tmpStr.ltrim();
          tmpStr.rtrim();
          if ((*ptr) == FMT_day) {
            my_casedn_str(collation.collation, tmpStr.ptr());
          } else if ((*ptr) == FMT_DAY) {
            my_caseup_str(collation.collation, tmpStr.ptr());
          }
          char_len = my_numchars_mb(collation.collation, tmpStr.ptr(),
                                    tmpStr.ptr() + tmpStr.length());
          if (fmt_fill) {
            if (tmpStr.fill(
                    tmpStr.length() + locale->max_day_name_length - char_len,
                    ' '))
              goto err_exit;
          }
          str->append(tmpStr);
        }
      } break;
      case FMT_DD:
        if (append_val(l_time->day, (fmt_fill) ? 2 : 0, str)) goto err_exit;
        handle_suffix(str, &ptr, l_time->day);
        break;
      case FMT_DDD:
        tmp_num = calc_daynr(l_time->year, l_time->month, l_time->day) -
                  calc_daynr(l_time->year, 1, 1) + 1;
        if (append_val(tmp_num, (fmt_fill) ? 3 : 0, str)) {
          goto err_exit;
        }
        handle_suffix(str, &ptr, tmp_num);
        break;
      case FMT_dy:
      case FMT_Dy:
      case FMT_DY:
        if (l_time->day == 0)
          str->append("00", 2);
        else {
          weekday = calc_weekday(
              calc_daynr(l_time->year, l_time->month, l_time->day), 0);
          name_ptr = locale->ab_day_names->type_names[weekday];
          name_len = strlen(name_ptr);
          tmpStr.copy(name_ptr, name_len, collation.collation);
          tmpStr.ltrim();
          tmpStr.rtrim();

          if ((*ptr) == FMT_dy) {
            my_casedn_str(collation.collation, tmpStr.ptr());
          } else if ((*ptr) == FMT_DY) {
            my_caseup_str(collation.collation, tmpStr.ptr());
          }
          if (fmt_fill) {
            if (tmpStr.fill(name_len, ' ')) goto err_exit;
          }
          str->append(tmpStr);
        }
        break;
      case FMT_FF: {
        if ((ptr + 1) != nullptr && *(ptr + 1) != 0) {
          if (append_fill(l_time->second_part, *(ptr + 1), str)) goto err_exit;
          tmp_num = *(ptr + 1);
          ptr++;
        } else {
          if (append_fill(l_time->second_part, 6, str)) goto err_exit;
          tmp_num = 6;
        }
        tmp_num = tmp_num > 1 ? ((*str)[str->length() - 2] - '0') * 10 +
                                    ((*str)[str->length() - 1] - '0')
                              : ((*str)[str->length() - 1] - '0');
        handle_suffix(str, &ptr, tmp_num);
      } break;
      case FMT_FM:
        fmt_fill = !fmt_fill;
        break;
      case FMT_FX:
        break;
      case FMT_HH:
      case FMT_HH12:
        hours_i = (l_time->hour % 24 + 11) % 12 + 1;
        if (append_val(hours_i, (fmt_fill) ? 2 : 0, str)) goto err_exit;
        handle_suffix(str, &ptr, hours_i);
        break;
      case FMT_HH24:
        if (append_val(l_time->hour, (fmt_fill) ? 2 : 0, str)) goto err_exit;
        handle_suffix(str, &ptr, l_time->hour);
        break;
      case FMT_IW:
        tmp_num = date_to_ISO_week(l_time->year, l_time->month, l_time->day);
        if (append_val(tmp_num, (fmt_fill) ? 2 : 0, str)) goto err_exit;
        handle_suffix(str, &ptr, tmp_num);
        break;
      case FMT_I:
        tmp_num =
            date_to_ISO_year(l_time->year, l_time->month, l_time->day) % 10;
        if (append_val(tmp_num, 0, str)) goto err_exit;
        handle_suffix(str, &ptr, tmp_num);
        break;
      case FMT_IY:
        tmp_num =
            date_to_ISO_year(l_time->year, l_time->month, l_time->day) % 100;
        if (append_val(tmp_num, (fmt_fill) ? 2 : 0, str)) goto err_exit;
        handle_suffix(str, &ptr, tmp_num);
        break;
      case FMT_IYY:
        tmp_num =
            date_to_ISO_year(l_time->year, l_time->month, l_time->day) % 1000;
        if (append_val(tmp_num, (fmt_fill) ? 3 : 0, str)) goto err_exit;
        handle_suffix(str, &ptr, tmp_num);
        break;
      case FMT_IYYY:
        tmp_num = date_to_ISO_year(l_time->year, l_time->month, l_time->day);
        if (append_val(tmp_num, (fmt_fill) ? 4 : 0, str)) goto err_exit;
        handle_suffix(str, &ptr, tmp_num);
        break;
      case FMT_J:
        tmp_num = date_to_julianday(l_time->year, l_time->month, l_time->day);
        if (append_val(tmp_num, (fmt_fill) ? 7 : 0, str)) goto err_exit;
        handle_suffix(str, &ptr, tmp_num);
        break;
      case FMT_MI:
        if (append_val(l_time->minute, (fmt_fill) ? 2 : 0, str)) goto err_exit;
        handle_suffix(str, &ptr, l_time->minute);
        break;
      case FMT_MM:
        if (append_val(l_time->month, (fmt_fill) ? 2 : 0, str)) goto err_exit;
        handle_suffix(str, &ptr, l_time->month);
        break;
      case FMT_mon:
      case FMT_Mon:
      case FMT_MON: {
        if (l_time->month == 0) {
          str->append("00", 2);
        } else {
          name_ptr = locale->ab_month_names->type_names[l_time->month - 1];
          name_len = strlen(name_ptr);
          tmpStr.copy(name_ptr, name_len, collation.collation);
          tmpStr.ltrim();
          tmpStr.rtrim();

          if ((*ptr) == FMT_mon) {
            my_casedn_str(collation.collation, tmpStr.ptr());
          } else if ((*ptr) == FMT_MON) {
            my_caseup_str(collation.collation, tmpStr.ptr());
          }
          if (fmt_fill) {
            if (tmpStr.fill(name_len, ' ')) goto err_exit;
          }
          str->append(tmpStr);
        }
      } break;
      case FMT_month:
      case FMT_Month:
      case FMT_MONTH: {
        if (l_time->month == 0) {
          str->append("00", 2);
        } else {
          name_ptr = locale->month_names->type_names[l_time->month - 1];
          name_len = strlen(name_ptr);
          tmpStr.copy(name_ptr, name_len, collation.collation);
          tmpStr.ltrim();
          tmpStr.rtrim();
          if ((*ptr) == FMT_month) {
            my_casedn_str(collation.collation, tmpStr.ptr());
          } else if ((*ptr) == FMT_MONTH) {
            my_caseup_str(collation.collation, tmpStr.ptr());
          }
          char_len = my_numchars_mb(collation.collation, tmpStr.ptr(),
                                    tmpStr.ptr() + tmpStr.length());

          if (fmt_fill)
            if (tmpStr.fill(
                    tmpStr.length() + locale->max_month_name_length - char_len,
                    ' '))
              goto err_exit;

          str->append(tmpStr);
        }
      } break;
      case FMT_Q:
        tmp_num = (l_time->month - 1) / 3 + 1;
        if (append_val(tmp_num, 1, str)) goto err_exit;
        handle_suffix(str, &ptr, tmp_num);
        break;
      case FMT_rm:
      case FMT_Rm:
      case FMT_RM:
        name_ptr = month_to_roman_numeral(
            (l_time->month - 1) % 12 + 1,
            *ptr == FMT_RM ? 1 : (*ptr == FMT_rm ? -1 : 0));
        name_len = strlen(name_ptr);
        tmpStr.copy(name_ptr, name_len, collation.collation);
        char_len = my_numchars_mb(collation.collation, tmpStr.ptr(),
                                  tmpStr.ptr() + tmpStr.length());
        if (fmt_fill && tmpStr.fill(tmpStr.length() + 4 - char_len, ' ')) {
          goto err_exit;
        }
        str->append(tmpStr);
        break;
      case FMT_SS:
        if (append_val(l_time->second, (fmt_fill) ? 2 : 0, str)) goto err_exit;
        handle_suffix(str, &ptr, l_time->second);
        break;
      case FMT_SSSSS:
        tmp_num = l_time->hour * SECS_PER_HOUR + l_time->minute * SECS_PER_MIN +
                  l_time->second;
        if (append_val(tmp_num, (fmt_fill) ? 5 : 0, str)) goto err_exit;
        handle_suffix(str, &ptr, tmp_num);
        break;
      case FMT_W:
        tmp_num = (l_time->day - 1) / 7 + 1;
        if (append_val(tmp_num, 0, str)) goto err_exit;
        handle_suffix(str, &ptr, tmp_num);
        break;
      case FMT_WW:
        tmp_num = ((calc_daynr(l_time->year, l_time->month, l_time->day) -
                    calc_daynr(l_time->year, 1, 1) + 1) +
                   6) /
                  7;
        if (append_val(tmp_num, (fmt_fill) ? 2 : 0, str)) goto err_exit;
        handle_suffix(str, &ptr, tmp_num);
        break;
      case FMT_X:
        str->append(".", 1);
        break;
      case FMT_Y:
        if (append_val(l_time->year % 10, 1, str)) goto err_exit;
        handle_suffix(str, &ptr, l_time->year % 10);
        break;
      case FMT_YY:
      case FMT_RR:
        if (append_val(l_time->year % 100, (fmt_fill) ? 2 : 0, str))
          goto err_exit;
        handle_suffix(str, &ptr, l_time->year % 100);
        break;
      case FMT_YYY:
        if (append_val(l_time->year % 1000, (fmt_fill) ? 3 : 0, str))
          goto err_exit;
        handle_suffix(str, &ptr, l_time->year % 1000);
        break;
      case FMT_YYYY:
      case FMT_RRRR:
        if (append_val(l_time->year, (fmt_fill) ? 4 : 0, str)) goto err_exit;
        handle_suffix(str, &ptr, l_time->year);
        break;
      case FMT_year:
      case FMT_Year:
      case FMT_YEAR:
        tmpStr.length(0);
        year_spelled_out(l_time->year, &tmpStr,
                         *ptr == FMT_YEAR ? 1 : (*ptr == FMT_year ? -1 : 0));
        str->append(tmpStr);
        break;
      case FMT_YYYY_COMMA:
        tmp_num = l_time->year / 1000;
        if (0 != tmp_num || fmt_fill) {
          if (append_val(tmp_num, 0, str)) goto err_exit;
          str->append(",");
        }
        if (append_val(l_time->year % 1000,
                       (fmt_fill || (0 != tmp_num)) ? 3 : 0, str))
          goto err_exit;
        handle_suffix(str, &ptr, l_time->year);
        break;
      case FMT_th:
      case FMT_TH:
        String *format;
        format = args[1]->val_str(&tmpStr);
        my_error(ER_STD_INVALID_ARGUMENT, MYF(0), format->c_ptr_safe(),
                 func_name());
        goto err_exit;
      default:
        str->append((char)*ptr);
    }
    ptr++;
  };
  return false;

err_exit:
  return true;
}

bool Item_func_to_char::make_num_str_oracle(my_decimal *num, String *str) {
  my_decimal rnd_dec;
  double rnd;
  char *tmp_ptr;
  int tmp_len = 0;
  char tmp_buff[DECIMAL_MAX_FIELD_SIZE * 2] = {0};
  char buffer[DECIMAL_MAX_FIELD_SIZE * 2] = {0};
  String num_str(buffer, DECIMAL_MAX_FIELD_SIZE * 2, collation.collation);
  num_str.length(0);
  str->length(0);
  bool is_zero = false;
  bool sign = false;

  tmp_len = decimal_intg(num);
  if (tmp_len == DECIMAL_MAX_FIELD_SIZE) {
    my_error(ER_STD_OVERFLOW_ERROR, MYF(0), "number", func_name());
    return true;
  }
  // to_char('aaa', '9')
  if (current_thd->get_stmt_da()->has_sql_condition(ER_TRUNCATED_WRONG_VALUE)) {
    my_error(ER_STD_INVALID_ARGUMENT, MYF(0), "args 0", func_name());
    return true;
  }

  if (decimal_is_zero(num)) is_zero = true;

  sign = num->sign();
  num->sign(false);

  if (is_zero && num_format->zero_return_empty) {
    if (num_format->FM) {
      return false;
    } else {
      if (str->fill(num_format->format_length, ' ')) return true;
      return false;
    }
  }

  int rc = E_DEC_OK;
  do {
    switch (num_format->is_EEEE) {
      case NUM_TM9:

        rc = my_decimal2string(E_DEC_FATAL_ERROR, num, &num_str);
        break;
      case NUM_E:  //
        if (!is_zero) {
          rc = decimal2double(num, &rnd);
          if (rc != E_DEC_OK) {
            break;
          }
          tmp_len = snprintf(tmp_buff, DECIMAL_MAX_FIELD_SIZE, "%.*E",
                             num_format->fmt_decimal_len, rnd);

          // remove fille zero 1.230 e+02  => 1.23 e+02
          if (num_format->FM)
            if (num_format->fmt_decimal_len > 0) {
              auto end = tmp_buff + tmp_len;
              auto pos = std::find(tmp_buff, end, 'E');
              if (pos == end) {
                rc = E_DEC_BAD_NUM;
                break;
              } else {
                auto len = end - pos;
                while (*(--pos) == '0') {
                  if (num_format->fixed_prec_end == 0 ||
                      (pos - tmp_buff - 2) > num_format->fixed_prec_end) {
                    tmp_len--;
                    memmove(pos, pos + 1, len);
                  }
                }
              }
            }
          if (num_str.copy(tmp_buff, tmp_len, collation.collation)) return true;

        } else {
          /*
          fm0EEEE    0
          fm9EEEE    0
          fm9.9EEEE  0.
          fm0.9EEEE  0.
          fm9.0EEEE  .0
          fm0.0EEEE  0.0
          */
          if (num_format->FM) {
            if (num_format->fixed_prec_end == 0) {
              if (num_format->proint) {
                num_str.set_ascii(STRING_WITH_LEN("0."));
              } else {
                num_str.set_ascii(STRING_WITH_LEN("0"));
              }

            } else {
              if (num_format->fixed_prec > 0) {
                num_str.set_ascii(STRING_WITH_LEN("0"));
              }
              num_str.append('.');
              num_str.fill(num_str.length() + num_format->fixed_prec_end, '0');
            }
          } else {
            if (num_format->fixed_prec > 0 ||
                num_format->fmt_decimal_len == 0) {
              num_str.set_ascii(STRING_WITH_LEN("0"));
            }
            if (num_format->proint) {
              num_str.append('.');
            }
            if (num_format->fmt_decimal_len > 0) {
              num_str.fill(num_str.length() + num_format->fmt_decimal_len, '0');
            }
          }
          num_str.append(STRING_WITH_LEN("E+00"));
        }

        break;
      case NUM_TME:
        rc = decimal2double(num, &rnd);
        if (rc != E_DEC_OK) {
          break;
        }
        rc = my_decimal2string(E_DEC_FATAL_ERROR, num, &num_str);
        if (rc != E_DEC_OK) {
          break;
        }
        tmp_len = static_cast<int>(num_str.length() - 1);
        // remove end zero  1.0E => 1E

        tmp_len = snprintf(tmp_buff, DECIMAL_MAX_FIELD_SIZE, "%.*E",
                           tmp_len + 1, rnd);
        {
          auto end = tmp_buff + tmp_len;
          auto pos = std::find(tmp_buff, end, 'E');
          if (pos == end) {
            rc = E_DEC_BAD_NUM;
            break;
          } else {
            auto len = end - pos;
            // remove 'zero'
            while (*(pos - 1) == '0') {
              pos--;
              tmp_len--;
              memmove(pos, pos + 1, len);
            }
            if (*(pos - 1) == '.') {
              pos--;
              tmp_len--;
              memmove(pos, pos + 1, len);
            }
          }
          num_str.copy(tmp_buff, tmp_len, collation.collation);
        }
        break;
      case NUM_x:
      case NUM_X:
        if (sign) {
          rc = E_DEC_OVERFLOW;
          break;
        }
        longlong result;
        my_decimal2int(E_DEC_FATAL_ERROR, num, unsigned_flag, &result);
        tmp_ptr = ll2str(result, tmp_buff, 16,
                         num_format->is_EEEE == NUM_X ? true : false);
        if (tmp_ptr == nullptr) return true;
        if ((tmp_ptr - tmp_buff) > num_format->intg) {
          rc = E_DEC_OVERFLOW;
          break;
        }
        tmp_len = num_format->fixed_prec
                      ? num_format->fixed_prec + num_format->intg
                      : (tmp_ptr - tmp_buff);
        if (num_str.append_with_prefill(tmp_buff, tmp_ptr - tmp_buff, tmp_len,
                                        '0'))
          return true;
        break;
      default:
        if (!is_zero) {
          uint fixed_length = num_format->fmt_decimal_len;
          /*
           tmp_len: int part len
            0.1 tmp_len= 0
            1.1 tmp_len= 1
          */
          tmp_len = decimal_intg(num);
          if (num_format->intg < tmp_len || (num_format->format_length == 1)) {
            rc = E_DEC_OVERFLOW;
            break;
          }
          if (num_format->fillzero > 0) {
            my_decimal val1, val2;
            rc = double2decimal(pow(10, num_format->fillzero), &val2);
            if (rc != E_DEC_OK) {
              break;
            }

            rc = my_decimal_mul(E_DEC_FATAL_ERROR & ~E_DEC_OVERFLOW, &val1, num,
                                &val2);
            if (rc != E_DEC_OK) {
              break;
            }
            rc = my_decimal_round(E_DEC_FATAL_ERROR, &val1, 0, false, num);
            if (rc != E_DEC_OK) {
              break;
            }
            tmp_len = decimal_intg(num);
          }

          int actual_frac = decimal_actual_fraction(num);
          // keep decimal_len
          if ((fixed_length - actual_frac) != 0) {
            // FM: fmt 0.090999  0.123 => 0.123  0.1234 =>0.1234 0.1=>0.10
            if ((num_format->fmt_decimal_len > actual_frac) && num_format->FM) {
              fixed_length = max(num_format->fixed_prec_end, actual_frac);
            }

            rc = my_decimal_round(E_DEC_FATAL_ERROR, num, fixed_length, false,
                                  &rnd_dec);
            if (rc != E_DEC_OK) {
              break;
            }

            if (num_format->FM) {
              // check 99.99 => 100.00 FM 99.99 => 100.
              actual_frac = decimal_actual_fraction(&rnd_dec);
              if ((fixed_length - actual_frac) != 0) {
                // 100.00 FM 99.00 => 100.00
                fixed_length = max(num_format->fixed_prec_end, actual_frac);
                rc = my_decimal_round(E_DEC_FATAL_ERROR, num, fixed_length,
                                      false, &rnd_dec);
                if (rc != E_DEC_OK) {
                  break;
                }
              }
            }

            num = &rnd_dec;

          } else {
            // 0.010  actual_frac:2 frac:3
            rc = my_decimal_round(E_DEC_FATAL_ERROR, num, actual_frac, false,
                                  &rnd_dec);
            if (rc != E_DEC_OK) {
              break;
            }
            num = &rnd_dec;
          }

          if (num_format->fixed_prec > tmp_len) {
            tmp_len = num_format->fixed_prec;
            rc = my_decimal2string(E_DEC_FATAL_ERROR, num,
                                   num_format->fixed_prec + fixed_length,
                                   fixed_length, &num_str);
          } else {
            rc = my_decimal2string(E_DEC_FATAL_ERROR, num, &num_str);
          }

          if (rc > E_DEC_TRUNCATED) {
            break;
          }
          rc = E_DEC_OK;
          // remove zero 0.xxx -> .xxxx
          if (num_format->fixed_prec == 0 && tmp_len == 0 &&
              num_format->fmt_decimal_len != 0 && fixed_length != 0) {
            memcpy(tmp_buff, num_str.ptr() + 1, num_str.length() - 1);

            num_str.copy(tmp_buff, num_str.length() - 1, num_str.charset());
          }

          DBUG_PRINT("debug", ("proint %d , num_str %s", num_format->proint,
                               num_str.c_ptr()));
          /**
           *  append point
           */
          if (num_format->proint != 0 && fixed_length == 0) {
            num_str.append('.');
          }

          if (num_format->proint == NUM_D) {
            num_str[tmp_len] = locale->decimal_point;
          }

          DBUG_PRINT("debug", ("proint22 %d , num_str %s", num_format->proint,
                               num_str.c_ptr()));
        } else {  // zero

          if (num_format->fixed_prec > 0) {
            num_str.fill(num_format->fixed_prec, '0');
          }

          if (num_format->fmt_decimal_len > 0) {
            size_t decimal_part_len = 0;
            if (num_format->FM) {
              if (num_format->fixed_prec_end > 0) {
                memset(tmp_buff, '0', num_format->fixed_prec_end);
                decimal_part_len = num_format->fixed_prec_end;
              }
            } else {
              memset(tmp_buff, '0', num_format->fmt_decimal_len);
              decimal_part_len = num_format->fmt_decimal_len;
            }

            if (num_str.length() == 0 && decimal_part_len == 0 &&
                num_format->fmt_inter_len > 0) {
              num_str.append('0');
            }
            tmp_len = num_str.length();
            switch (num_format->proint) {
              case NUM_DEC:
                num_str.append('.');
                break;
              case NUM_D:
                num_str.append(locale->decimal_point);
                break;
            }

            if (decimal_part_len > 0) {
              num_str.append(tmp_buff, decimal_part_len);
            }
          } else {
            if (num_format->fillzero != 0 && num_format->fixed_prec == 0) {
              num_str.fill(num_str.length() + num_format->fillzero, '0');
            }
            // default 9
            if (num_str.length() == 0 && num_format->fmt_inter_len > 0) {
              num_str.append('0');
            }
            tmp_len = num_str.length();
            switch (num_format->proint) {
              case NUM_DEC:
                num_str.append('.');
                break;
              case NUM_D:
                num_str.append(locale->decimal_point);
                break;
            }
          }
        }
        // add group ','
        if (num_format->fmt_inter_len !=
            (num_format->intg + num_format->fillzero)) {
          std::string std_tmp;
          int val_len = tmp_len;
          char *val = num_str.ptr() + val_len - 1;
          int i = val_len;
          for (int pos = num_format->fmt_inter_len - 1; (pos >= 0 && i > 0);
               pos--) {
            // lc->grouping ?
            if (num_format->fmt_inter[pos] == NUM_COMMA ||
                num_format->fmt_inter[pos] == NUM_G) {
              std_tmp.insert(std_tmp.end(), ',');
              continue;
            } else {
              std_tmp.insert(std_tmp.end(), *val);
              val--;
              i--;
            }
          }
          std::reverse(std_tmp.begin(), std_tmp.end());
          // append .dec part
          std_tmp.append(num_str.ptr() + val_len, num_str.length() - val_len);
          if (num_str.copy(std_tmp.c_str(), std_tmp.length(),
                           num_str.charset()))
            return true;
        }
    }
    if (rc != E_DEC_OK) {
      break;
    }

    String tmp(tmp_buff, DECIMAL_MAX_FIELD_SIZE * 2, collation.collation);
    if (num_format->is_dollar == NUM_DOLLAR) {
      tmp.length(0);
      tmp.append_with_prefill(num_str.ptr(), num_str.length(),
                              num_str.length() + 1, '$');
      num_str.copy(tmp);
    }
    tmp.length(0);
    if (sign) {
      switch (num_format->is_minus) {
        case NUM_MI:
        case NUM_S_END:
          if (num_str.append('-')) return true;
          break;
        case NUM_PR:
          if (num_str.append('>')) return true;
          tmp.append('<');
          tmp.append(num_str);
          num_str.copy(tmp);
          break;
        default:  // 0
          tmp.append('-');
          tmp.append(num_str);
          num_str.copy(tmp);
      }
    } else {
      switch (num_format->is_minus) {
        case NUM_MI:
          if (!num_format->FM && num_str.append(' ')) return true;
          break;
        case NUM_S_END:
          if (!num_format->FM && num_str.append('+')) return true;
          break;
        case NUM_PR:
          if (!num_format->FM && num_str.append(' ')) return true;
          tmp.append_with_prefill(num_str.ptr(), num_str.length(),
                                  num_str.length() + 1, ' ');
          num_str.copy(tmp);
          break;
        case NUM_S:
          tmp.append_with_prefill(num_str.ptr(), num_str.length(),
                                  num_str.length() + 1, '+');
          num_str.copy(tmp);
          break;
        default:
          if (!num_format->FM) {
            tmp.append_with_prefill(num_str.ptr(), num_str.length(),
                                    num_str.length() + 1, ' ');
            num_str.copy(tmp);
          }
          break;
      }
    }
    if (!num_format->FM) {
      str->append_with_prefill(num_str.c_ptr_quick(), num_str.length(),
                               num_format->format_length > num_str.length()
                                   ? num_format->format_length
                                   : num_str.length(),
                               ' ');
    } else {
      num_str.ltrim();
      num_str.rtrim();
      str->copy(num_str);
    }
    DBUG_PRINT("debug", ("res %s", str->c_ptr()));
    break;
  } while (1);
  if (rc != E_DEC_OK) {
    if (rc == E_DEC_OVERFLOW) {
      str->fill((num_format->FM ? 1 : 0) + num_format->format_length, '#');
      return false;
    } else {
      return true;
    }
  }

  return false;
}

bool Item_func_to_char::resolve_type(THD *thd) {
  uint32 char_length = 0;
  set_data_type_string(char_length);
  set_nullable(true);

  switch (args[0]->data_type()) {
    case MYSQL_TYPE_TIMESTAMP:
    case MYSQL_TYPE_DATE:
    case MYSQL_TYPE_TIME:
    case MYSQL_TYPE_DATETIME:
    case MYSQL_TYPE_YEAR:
    case MYSQL_TYPE_NEWDATE:
    case MYSQL_TYPE_TIMESTAMP2:
    case MYSQL_TYPE_DATETIME2:
    case MYSQL_TYPE_TIME2:
      is_date = true;
      datetime_format_array =
          new (thd->mem_root) uint16[MAX_DATETIME_FORMAT_MODEL_LEN + 1];
      memset(datetime_format_array, 0, MAX_DATETIME_FORMAT_MODEL_LEN + 1);
      break;
    case MYSQL_TYPE_DECIMAL:
    case MYSQL_TYPE_TINY:
    case MYSQL_TYPE_SHORT:
    case MYSQL_TYPE_LONG:
    case MYSQL_TYPE_FLOAT:
    case MYSQL_TYPE_DOUBLE:
    case MYSQL_TYPE_LONGLONG:
    case MYSQL_TYPE_INT24:
    case MYSQL_TYPE_VARCHAR:
    case MYSQL_TYPE_NEWDECIMAL:
    case MYSQL_TYPE_VAR_STRING:
    case MYSQL_TYPE_STRING:
      is_date = false;
      num_format = new (thd->mem_root) NUMDesc();
      memset(num_format, 0, sizeof(NUMDesc));
      break;
    case MYSQL_TYPE_NULL:
      return false;
    default:
      my_error(ER_INCORRECT_TYPE, MYF(9), "args 0", func_name());
      return true;
  }
  if (param_type_is_default(thd, 1, -1)) return true;

  //  Must use this_item() in case it's a local SP variable
  //  (for ->max_length and ->str_value)
  Item *arg1 = args[1]->this_item();
  const CHARSET_INFO *cs = thd->variables.collation_connection;
  uint32 repertoire = arg1->collation.repertoire;
  if (!thd->variables.lc_time_names->is_ascii)
    repertoire |= MY_REPERTOIRE_EXTENDED;
  locale = thd->variables.lc_time_names;
  collation.set(cs, arg1->collation.derivation, repertoire);

  // arg1 is const string
  if (arg1->type() == STRING_ITEM) {
    uint ulen = 0;
    String str;
    // not return error if  args[0] is null
    auto format = arg1->val_str(&str);
    if (format) {
      if (is_date) {
        if (generate_date_format_array(format, locale, collation.collation,
                                       datetime_format_array, &ulen)) {
          char_length = (uint32)(ulen * collation.collation->mbmaxlen);
        } else {
          return false;
        }
      } else {
        if (generate_number_format_array(format, &ulen)) {
          char_length = (uint32)ulen;
        } else {
          return false;
        }
      }
    } else {
      return false;
    }

    fixed_length = true;
  } else {
    fixed_length = false;
    char_length = min(min(arg1->max_char_length(), uint32(MAX_BLOB_WIDTH)) * 10,
                      uint32(MAX_BLOB_WIDTH));
  }
  set_data_type_string(char_length);
  return false;
}

String *Item_func_to_char::val_str(String *str) {
  String *format;
  StringBuffer<MAX_DATETIME_FORMAT_MODEL_LEN> format_buffer;
  size_t max_result_length = max_length;
  const MY_LOCALE *lc = locale;
  null_value = false;
  uint ulen = 0;
  null_value = false;
  // MYSQL_TYPE_NULL
  if (num_format == nullptr && datetime_format_array == nullptr) goto null_data;

  str->set_charset(collation.collation);

  // check arg0 is null
  if (is_date) {  // format datetime

    MYSQL_TIME l_time;
    if (get_arg0_date(&l_time, TIME_FUZZY_DATE | TIME_NO_ZERO_DATE)) {
      if (args[0]->null_value) goto null_data;
      my_error(ER_STD_INVALID_ARGUMENT, MYF(0), "args 0", func_name());
    }
    if (!(non_zero_date(l_time) || non_zero_time(l_time))) goto null_data;
    if (!fixed_length) {
      format = args[1]->val_str(&format_buffer);

      if (args[1]->null_value) goto null_data;

      if (!format) {
        my_error(ER_STD_INVALID_ARGUMENT, MYF(0), "args 1", func_name());
        goto null_data;
      }

      assert(datetime_format_array);

      if (!generate_date_format_array(format, lc, collation.collation,
                                      datetime_format_array, &ulen)) {
        if (ulen > MAX_DATETIME_FORMAT_MODEL_LEN) {
          my_error(ER_TOO_LONG_IDENT, MYF(0), format->c_ptr_safe());
        } else {
          my_error(ER_STD_INVALID_ARGUMENT, MYF(0), format->c_ptr_safe(),
                   func_name());
        }
        goto null_data;
      }
      max_result_length = ((size_t)ulen) * collation.collation->mbmaxlen;
    }
    if (str->is_alloced())
      if (str->alloc(max_result_length)) {
        my_error(ER_OUT_OF_RESOURCES, MYF(0));
        goto null_data;
      };
    /* Create the result string */
    str->set_charset(collation.collation);
    if (!make_date_str_oracle(&l_time, str)) return str;
  } else {
    // string or num
    if (args[0]->result_type() == STRING_RESULT) {
      String *tmp = args[0]->val_str(str);
      if (args[0]->null_value) goto null_data;
      if (tmp != nullptr && tmp->length() == 0) goto null_data;
    }
    my_decimal *num, dec_val;
    num = args[0]->val_decimal(&dec_val);
    if (args[0]->null_value) goto null_data;
    if (!num) {
      my_error(ER_STD_INVALID_ARGUMENT, MYF(0), "args 0", func_name());
      goto null_data;
    }

    if (!fixed_length) {
      format = args[1]->val_str(&format_buffer);
      if (args[1]->null_value) goto null_data;
      if (!format) {
        my_error(ER_STD_INVALID_ARGUMENT, MYF(0), "args 1", func_name());
        goto null_data;
      }
      // to_char(9, '');
      if (format->length() == 0) {
        str->append('#');
        return str;
      }
      assert(num_format);
      memset(num_format, 0, sizeof(NUMDesc));
      if (!generate_number_format_array(format, &ulen)) {
        if (ulen > DECIMAL_MAX_FIELD_SIZE) {
          my_error(ER_TOO_LONG_IDENT, MYF(0), format->c_ptr_safe());
        } else {
          my_error(ER_STD_INVALID_ARGUMENT, MYF(0), format->c_ptr_safe(),
                   func_name());
        }
        goto null_data;
      }
      max_result_length = ((size_t)ulen);
    }
    if (str->is_alloced())
      if (str->alloc(max_result_length)) {
        my_error(ER_OUT_OF_RESOURCES, MYF(0));
        goto null_data;
      };
    /* Create the result string */
    str->set_charset(collation.collation);

    if (!make_num_str_oracle(num, str)) return str;
  }
null_data:
  null_value = true;
  return nullptr;
}

bool Item_func_to_char::eq(const Item *item, bool binary_cmp) const {
  if (item->type() != FUNC_ITEM) return false;
  if (strcmp(func_name(), down_cast<const Item_func *>(item)->func_name()) != 0)
    return false;
  if (this == item) return true;
  const Item_func_to_char *item_func =
      down_cast<const Item_func_to_char *>(item);
  if (!args[0]->eq(item_func->args[0], binary_cmp)) return false;

  if (!args[1]->eq(item_func->args[1], true)) return false;

  if (arg_count > 2) {
    if (item_func->arg_count > 2) {
      if (!args[2]->eq(item_func->args[2], true)) return false;
    } else {
      return false;
    }
  }
  return true;
}

bool Item_func_date_format::resolve_type(THD *thd) {
  if (param_type_is_default(thd, 0, 1, MYSQL_TYPE_DATETIME)) return true;
  if (param_type_is_default(thd, 1, -1)) return true;
  /*
    Must use this_item() in case it's a local SP variable
    (for ->max_length and ->str_value)
  */
  Item *arg1 = args[1]->this_item();
  uint32 char_length;
  const CHARSET_INFO *cs = thd->variables.collation_connection;
  uint32 repertoire = arg1->collation.repertoire;
  if (!thd->variables.lc_time_names->is_ascii)
    repertoire |= MY_REPERTOIRE_EXTENDED;
  collation.set(cs, arg1->collation.derivation, repertoire);
  if (arg1->type() == STRING_ITEM) {  // Optimize the normal case
    fixed_length = true;
    String str;
    char_length = format_length(arg1->val_str(&str));
  } else {
    fixed_length = false;
    char_length = min(min(arg1->max_char_length(), uint32(MAX_BLOB_WIDTH)) * 10,
                      uint32(MAX_BLOB_WIDTH));
  }
  set_data_type_string(char_length);
  set_nullable(true);  // If wrong date
  return false;
}

bool Item_func_date_format::eq(const Item *item, bool binary_cmp) const {
  if (item->type() != FUNC_ITEM) return false;
  if (strcmp(func_name(), down_cast<const Item_func *>(item)->func_name()) != 0)
    return false;
  if (this == item) return true;
  const Item_func_date_format *item_func =
      down_cast<const Item_func_date_format *>(item);
  if (!ItemsAreEqual(args[0], item_func->args[0], binary_cmp)) return false;
  /*
    We must compare format string case sensitive.
    This needed because format modifiers with different case,
    for example %m and %M, have different meaning.
  */
  if (!ItemsAreEqual(args[1], item_func->args[1], /*binary_tmp=*/true))
    return false;
  return true;
}

uint Item_func_date_format::format_length(const String *format) {
  uint size = 0;
  const char *ptr = format->ptr();
  const char *end = ptr + format->length();

  for (; ptr != end; ptr++) {
    if (*ptr != '%' || ptr == end - 1)
      size++;
    else {
      switch (*++ptr) {
        case 'M':     /* month, textual */
        case 'W':     /* day (of the week), textual */
          size += 64; /* large for UTF8 locale data */
          break;
        case 'D': /* day (of the month), numeric plus english suffix */
        case 'Y': /* year, numeric, 4 digits */
        case 'x': /* Year, used with 'v' */
        case 'X': /* Year, used with 'v, where week starts with Monday' */
          size += 4;
          break;
        case 'a':     /* locale's abbreviated weekday name (Sun..Sat) */
        case 'b':     /* locale's abbreviated month name (Jan.Dec) */
          size += 32; /* large for UTF8 locale data */
          break;
        case 'j': /* day of year (001..366) */
          size += 3;
          break;
        case 'U': /* week (00..52) */
        case 'u': /* week (00..52), where week starts with Monday */
        case 'V': /* week 1..53 used with 'x' */
        case 'v': /* week 1..53 used with 'x', where week starts with Monday */
        case 'y': /* year, numeric, 2 digits */
        case 'm': /* month, numeric */
        case 'd': /* day (of the month), numeric */
        case 'h': /* hour (01..12) */
        case 'I': /* --||-- */
        case 'i': /* minutes, numeric */
        case 'l': /* hour ( 1..12) */
        case 'p': /* locale's AM or PM */
        case 'S': /* second (00..61) */
        case 's': /* seconds, numeric */
        case 'c': /* month (0..12) */
        case 'e': /* day (0..31) */
          size += 2;
          break;
        case 'k': /* hour ( 0..23) */
        case 'H': /* hour (00..23; value > 23 OK, padding always 2-digit) */
          size +=
              7; /* docs allow > 23, range depends on sizeof(unsigned int) */
          break;
        case 'r': /* time, 12-hour (hh:mm:ss [AP]M) */
          size += 11;
          break;
        case 'T': /* time, 24-hour (hh:mm:ss) */
          size += 8;
          break;
        case 'f': /* microseconds */
          size += 6;
          break;
        case 'w': /* day (of the week), numeric */
        case '%':
        default:
          size++;
          break;
      }
    }
  }
  return size;
}

String *Item_func_date_format::val_str(String *str) {
  String *format;
  MYSQL_TIME l_time;
  uint size;
  assert(fixed == 1);

  if (!is_time_format) {
    if (get_arg0_date(&l_time, TIME_FUZZY_DATE)) return nullptr;
  } else {
    if (get_arg0_time(&l_time)) return nullptr;
    l_time.year = l_time.month = l_time.day = 0;
  }

  if (!(format = args[1]->val_str(str)) || !format->length()) goto null_date;

  if (fixed_length)
    size = max_length;
  else
    size = format_length(format);

  if (size < MAX_DATE_STRING_REP_LENGTH) size = MAX_DATE_STRING_REP_LENGTH;

  // If format uses the buffer provided by 'str' then store result locally.
  if (format == str || format->uses_buffer_owned_by(str)) str = &value;
  if (str->alloc(size)) goto null_date;

  Date_time_format date_time_format;
  date_time_format.format.str = format->ptr();
  date_time_format.format.length = format->length();

  /* Create the result string */
  str->set_charset(collation.collation);
  if (!make_date_time(
          &date_time_format, &l_time,
          is_time_format ? MYSQL_TIMESTAMP_TIME : MYSQL_TIMESTAMP_DATE, str))
    return str;

null_date:
  null_value = true;
  return nullptr;
}

bool Item_func_from_unixtime::resolve_type(THD *thd) {
  if (param_type_is_default(thd, 0, 1, MYSQL_TYPE_NEWDECIMAL)) return true;
  set_data_type_datetime(min(args[0]->decimals, uint8{DATETIME_MAX_DECIMALS}));
  set_nullable(true);
  thd->time_zone_used = true;
  return false;
}

bool Item_func_from_unixtime::get_date(MYSQL_TIME *ltime,
                                       my_time_flags_t fuzzy_date
                                       [[maybe_unused]]) {
  THD *thd = current_thd;
  lldiv_t lld;
  if (decimals) {
    my_decimal *val, decimal_value;
    if (!(val = args[0]->val_decimal(&decimal_value)) || args[0]->null_value)
      return (null_value = true);
    if (0 != my_decimal2lldiv_t(E_DEC_FATAL_ERROR, val, &lld))
      return (null_value = true);
  } else {
    lld.quot = args[0]->val_int();
    lld.rem = 0;
  }

  // Return NULL for timestamps after 2038-01-19 03:14:07 UTC (32 bits OS time)
  // or after 3001-01-18 23:59:59 (64 bits OS time)
  if ((null_value = (args[0]->null_value || lld.quot > MYTIME_MAX_VALUE) ||
                    lld.quot < 0 || lld.rem < 0))
    return true;

  const bool is_end_of_epoch = (lld.quot == MYTIME_MAX_VALUE);

  thd->variables.time_zone->gmt_sec_to_TIME(ltime, (my_time_t)lld.quot);
  if (ltime->year == 0) {
    // Overflow can happen in time zones east of UTC on Dec 31
    null_value = true;
    return true;
  }
  int warnings = 0;
  ltime->second_part = decimals ? static_cast<ulong>(lld.rem / 1000) : 0;
  bool ret = propagate_datetime_overflow(
      thd, &warnings,
      datetime_add_nanoseconds_adjust_frac(ltime, lld.rem % 1000, &warnings,
                                           thd->is_fsp_truncate_mode()));
  // Disallow round-up to one second past end of epoch.
  if (decimals && is_end_of_epoch) {
    MYSQL_TIME max_ltime;
    thd->variables.time_zone->gmt_sec_to_TIME(&max_ltime, MYTIME_MAX_VALUE);
    max_ltime.second_part = 999999UL;

    const longlong max_t = TIME_to_longlong_datetime_packed(max_ltime);
    const longlong ret_t = TIME_to_longlong_datetime_packed(*ltime);
    // The first test below catches the situation with 64 bits time, the
    // second test catches it with 32 bits time
    if ((null_value =
             (warnings & MYSQL_TIME_WARN_OUT_OF_RANGE) || (ret_t > max_t)))
      return true;
  }
  return ret;
}

bool Item_func_convert_tz::resolve_type(THD *thd) {
  if (param_type_is_default(thd, 0, 1, MYSQL_TYPE_DATETIME)) return true;
  if (param_type_is_default(thd, 1, -1)) return true;
  set_data_type_datetime(args[0]->datetime_precision());
  set_nullable(true);
  return false;
}

bool Item_func_convert_tz::get_date(MYSQL_TIME *ltime,
                                    my_time_flags_t fuzzy_date
                                    [[maybe_unused]]) {
  my_time_t my_time_tmp;
  String str;
  THD *thd = current_thd;

  if (!from_tz_cached) {
    from_tz = my_tz_find(thd, args[1]->val_str_ascii(&str));
    from_tz_cached = args[1]->const_item();
  }

  if (!to_tz_cached) {
    to_tz = my_tz_find(thd, args[2]->val_str_ascii(&str));
    to_tz_cached = args[2]->const_item();
  }

  if (from_tz == nullptr || to_tz == nullptr ||
      get_arg0_date(ltime, TIME_NO_ZERO_DATE)) {
    null_value = true;
    return true;
  }

  {
    bool not_used;
    uint second_part = ltime->second_part;
    my_time_tmp = from_tz->TIME_to_gmt_sec(ltime, &not_used);
    /* my_time_tmp is guaranteed to be in the allowed range */
    if (my_time_tmp) {
      to_tz->gmt_sec_to_TIME(ltime, my_time_tmp);
      ltime->second_part = second_part;
    }
  }

  null_value = false;
  return false;
}

void Item_func_convert_tz::cleanup() {
  from_tz_cached = false;
  to_tz_cached = false;
  Item_datetime_func::cleanup();
}

bool Item_date_add_interval::resolve_type(THD *thd) {
  set_nullable(true);

  /*
    If first argument is a dynamic parameter, type DATE is assumed if the
    provided interval is a YEAR, MONTH or DAY interval, otherwise
    type DATETIME is assumed.
    If the assumed type is DATE and the user provides a DATETIME on execution,
    a reprepare will happen.
  */
  const enum_field_types assumed_type =
      m_interval_type <= INTERVAL_DAY || m_interval_type == INTERVAL_YEAR_MONTH
          ? MYSQL_TYPE_DATE
          : MYSQL_TYPE_DATETIME;

  if (param_type_is_default(thd, 0, 1, assumed_type)) return true;
  /*
    Syntax may be:
    - either DATE_ADD(x, ?): then '?' is an integer number of days
    - or DATE_ADD(x, interval ? some_query_expression): then '?' may be
    an integer, a decimal, a string in format "days hours:minutes",
    depending on m_interval_type, see
    https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#
    function_date-add
  */
  enum_field_types arg1_type;
  if (m_interval_type <= INTERVAL_MINUTE)
    arg1_type = MYSQL_TYPE_LONGLONG;
  else if (m_interval_type == INTERVAL_SECOND)  // decimals allowed
    arg1_type = MYSQL_TYPE_NEWDECIMAL;
  else if (m_interval_type == INTERVAL_MICROSECOND)
    arg1_type = MYSQL_TYPE_LONGLONG;
  else
    arg1_type = MYSQL_TYPE_VARCHAR;  // composite, as in "HOUR:MINUTE"
  if (param_type_is_default(thd, 1, 2, arg1_type)) return true;

  /*
    The result type of an Item_date_add_interval function is defined as follows:

    - If first argument is MYSQL_TYPE_DATETIME, result is MYSQL_TYPE_DATETIME.
    - If first argument is MYSQL_TYPE_DATE,
        if the interval type uses hour, minute or second,
        then type is MYSQL_TYPE_DATETIME, otherwise type is MYSQL_TYPE_DATE.
    - If first argument is MYSQL_TYPE_TIME,
        if the interval type uses year, month or days
        then type is MYSQL_TYPE_DATETIME, otherwise type is MYSQL_TYPE_TIME.
    - Otherwise the result is MYSQL_TYPE_STRING
      This is because the first argument is interpreted as a string which
      may contain a DATE, TIME or DATETIME value, but we don't know which yet.
  */
  enum_field_types arg0_data_type = args[0]->data_type();
  uint8 interval_dec = 0;
  if (m_interval_type == INTERVAL_MICROSECOND ||
      (m_interval_type >= INTERVAL_DAY_MICROSECOND &&
       m_interval_type <= INTERVAL_SECOND_MICROSECOND))
    interval_dec = DATETIME_MAX_DECIMALS;
  else if (m_interval_type == INTERVAL_SECOND && args[1]->decimals > 0)
    interval_dec = min(args[1]->decimals, uint8{DATETIME_MAX_DECIMALS});

  if (arg0_data_type == MYSQL_TYPE_DATETIME ||
      arg0_data_type == MYSQL_TYPE_TIMESTAMP) {
    uint8 dec = max<uint8>(args[0]->datetime_precision(), interval_dec);
    set_data_type_datetime(dec);
  } else if (arg0_data_type == MYSQL_TYPE_DATE) {
    if (m_interval_type <= INTERVAL_DAY ||
        m_interval_type == INTERVAL_YEAR_MONTH)
      set_data_type_date();
    else
      set_data_type_datetime(interval_dec);
  } else if (arg0_data_type == MYSQL_TYPE_TIME) {
    if ((m_interval_type >= INTERVAL_HOUR &&
         m_interval_type <= INTERVAL_MICROSECOND) ||
        (m_interval_type >= INTERVAL_HOUR_MINUTE &&
         m_interval_type <= INTERVAL_SECOND_MICROSECOND)) {
      uint8 dec = max<uint8>(args[0]->time_precision(), interval_dec);
      set_data_type_time(dec);
    } else {
      uint8 dec = max<uint8>(args[0]->datetime_precision(), interval_dec);
      set_data_type_datetime(dec);
    }
  } else {
    /* Behave as a usual string function when return type is VARCHAR. */
    set_data_type_char(MAX_DATETIME_FULL_WIDTH, default_charset());
  }
  if (value.alloc(max_length)) return true;

  return false;
}

/* Here arg[1] is a Item_interval object */
bool Item_date_add_interval::get_date_internal(MYSQL_TIME *ltime,
                                               my_time_flags_t) {
  Interval interval;

  if (args[0]->get_date(ltime, TIME_NO_ZERO_DATE)) return (null_value = true);

  if (get_interval_value(args[1], m_interval_type, &value, &interval)) {
    // Do not warn about "overflow" for NULL
    if (!args[1]->null_value) {
      push_warning_printf(
          current_thd, Sql_condition::SL_WARNING, ER_DATETIME_FUNCTION_OVERFLOW,
          ER_THD(current_thd, ER_DATETIME_FUNCTION_OVERFLOW), func_name());
    }
    return (null_value = true);
  }

  if (m_subtract) interval.neg = !interval.neg;

  /*
    Make sure we return proper time_type.
    It's important for val_str().
  */
  if (data_type() == MYSQL_TYPE_DATE &&
      ltime->time_type == MYSQL_TIMESTAMP_DATETIME)
    datetime_to_date(ltime);
  else if (data_type() == MYSQL_TYPE_DATETIME &&
           ltime->time_type == MYSQL_TIMESTAMP_DATE)
    date_to_datetime(ltime);

  if ((null_value = date_add_interval_with_warn(current_thd, ltime,
                                                m_interval_type, interval)))
    return true;
  return false;
}

bool Item_date_add_interval::get_time_internal(MYSQL_TIME *ltime) {
  Interval interval;

  null_value = args[0]->get_time(ltime) ||
               get_interval_value(args[1], m_interval_type, &value, &interval);
  if (null_value) {
    return true;
  }
  if (m_subtract) interval.neg = !interval.neg;

  assert(!check_time_range_quick(*ltime));

  longlong usec1 =
      ((((ltime->day * 24 + ltime->hour) * 60 + ltime->minute) * 60 +
        ltime->second) *
           1000000LL +
       ltime->second_part) *
      (ltime->neg ? -1 : 1);
  longlong usec2 =
      ((((interval.day * 24 + interval.hour) * 60 + interval.minute) * 60 +
        interval.second) *
           1000000LL +
       interval.second_part) *
      (interval.neg ? -1 : 1);

  // Possible overflow adding date and interval values below.
  if ((usec1 > 0 && usec2 > 0) || (usec1 < 0 && usec2 < 0)) {
    lldiv_t usec2_as_seconds;
    usec2_as_seconds.quot = usec2 / 1000000;
    usec2_as_seconds.rem = 0;
    MYSQL_TIME unused;
    if ((null_value = sec_to_time(usec2_as_seconds, &unused))) {
      push_warning_printf(
          current_thd, Sql_condition::SL_WARNING, ER_DATETIME_FUNCTION_OVERFLOW,
          ER_THD(current_thd, ER_DATETIME_FUNCTION_OVERFLOW), "time");
      return true;
    }
  }

  longlong diff = usec1 + usec2;
  lldiv_t seconds;
  seconds.quot = diff / 1000000;
  seconds.rem = diff % 1000000 * 1000; /* time->second_part= lldiv.rem / 1000 */
  if ((null_value =
           (interval.year || interval.month || sec_to_time(seconds, ltime)))) {
    push_warning_printf(
        current_thd, Sql_condition::SL_WARNING, ER_DATETIME_FUNCTION_OVERFLOW,
        ER_THD(current_thd, ER_DATETIME_FUNCTION_OVERFLOW), "time");
    return true;
  }
  return false;
}

bool Item_date_add_interval::val_datetime(MYSQL_TIME *ltime,
                                          my_time_flags_t fuzzy_date) {
  if (data_type() != MYSQL_TYPE_TIME)
    return get_date_internal(ltime, fuzzy_date | TIME_NO_ZERO_DATE);
  return get_time_internal(ltime);
}

bool Item_date_add_interval::eq(const Item *item, bool binary_cmp) const {
  if (!Item_func::eq(item, binary_cmp)) return false;
  const Item_date_add_interval *other =
      down_cast<const Item_date_add_interval *>(item);
  return m_interval_type == other->m_interval_type &&
         m_subtract == other->m_subtract;
}

/*
   'interval_names' reflects the order of the enumeration interval_type.
   See item_timefunc.h
 */

const char *interval_names[] = {"year",
                                "quarter",
                                "month",
                                "week",
                                "day",
                                "hour",
                                "minute",
                                "second",
                                "microsecond",
                                "year_month",
                                "day_hour",
                                "day_minute",
                                "day_second",
                                "hour_minute",
                                "hour_second",
                                "minute_second",
                                "day_microsecond",
                                "hour_microsecond",
                                "minute_microsecond",
                                "second_microsecond"};

void Item_date_add_interval::print(const THD *thd, String *str,
                                   enum_query_type query_type) const {
  str->append('(');
  args[0]->print(thd, str, query_type);
  str->append(m_subtract ? " - interval " : " + interval ");
  args[1]->print(thd, str, query_type);
  str->append(' ');
  str->append(interval_names[m_interval_type]);
  str->append(')');
}

void Item_extract::print(const THD *thd, String *str,
                         enum_query_type query_type) const {
  str->append(STRING_WITH_LEN("extract("));
  str->append(interval_names[int_type]);
  str->append(STRING_WITH_LEN(" from "));
  args[0]->print(thd, str, query_type);
  str->append(')');
}

bool Item_extract::resolve_type(THD *thd) {
  if (param_type_is_default(thd, 0, 1, MYSQL_TYPE_DATETIME)) return true;
  set_nullable(true);  // If wrong date
  switch (int_type) {
    case INTERVAL_YEAR:
      max_length = 5;  // YYYY + sign
      date_value = true;
      break;
    case INTERVAL_YEAR_MONTH:
      max_length = 7;  // YYYYMM + sign
      date_value = true;
      break;
    case INTERVAL_QUARTER:
      max_length = 2;
      date_value = true;
      break;
    case INTERVAL_MONTH:
      max_length = 3;  // MM + sign
      date_value = true;
      break;
    case INTERVAL_WEEK:
      max_length = 3;  // WW + sign
      date_value = true;
      break;
    case INTERVAL_DAY:
      max_length = 3;  // DD + sign
      date_value = true;
      break;
    case INTERVAL_DAY_HOUR:
      max_length = 9;
      date_value = false;
      break;
    case INTERVAL_DAY_MINUTE:
      max_length = 11;
      date_value = false;
      break;
    case INTERVAL_DAY_SECOND:
      max_length = 13;
      date_value = false;
      break;
    case INTERVAL_HOUR:
      max_length = 4;  // HHH + sign
      date_value = false;
      break;
    case INTERVAL_HOUR_MINUTE:
      max_length = 6;  // HHHMM + sign
      date_value = false;
      break;
    case INTERVAL_HOUR_SECOND:
      max_length = 8;  // HHHMMSS + sign
      date_value = false;
      break;
    case INTERVAL_MINUTE:
      max_length = 3;  // MM + sign
      date_value = false;
      break;
    case INTERVAL_MINUTE_SECOND:
      max_length = 5;  // MMSS + sign
      date_value = false;
      break;
    case INTERVAL_SECOND:
      max_length = 3;  // SS + sign
      date_value = false;
      break;
    case INTERVAL_MICROSECOND:
      max_length = 7;  // six digits + sign
      date_value = false;
      break;
    case INTERVAL_DAY_MICROSECOND:
      max_length = 20;
      date_value = false;
      break;
    case INTERVAL_HOUR_MICROSECOND:
      max_length = 14;  // HHHMMSSFFFFFF + sign
      date_value = false;
      break;
    case INTERVAL_MINUTE_MICROSECOND:
      max_length = 11;
      date_value = false;
      break;
    case INTERVAL_SECOND_MICROSECOND:
      max_length = 9;
      date_value = false;
      break;
    case INTERVAL_LAST:
      assert(0);
      break; /* purecov: deadcode */
  }
  return false;
}

longlong Item_extract::val_int() {
  assert(fixed == 1);
  MYSQL_TIME ltime;
  uint year;
  ulong week_format;
  long neg;
  if (date_value) {
    if (get_arg0_date(&ltime, TIME_FUZZY_DATE)) return 0;
    neg = 1;
  } else {
    if (get_arg0_time(&ltime)) return 0;
    neg = ltime.neg ? -1 : 1;
  }
  switch (int_type) {
    case INTERVAL_YEAR:
      return ltime.year;
    case INTERVAL_YEAR_MONTH:
      return ltime.year * 100L + ltime.month;
    case INTERVAL_QUARTER:
      return (ltime.month + 2) / 3;
    case INTERVAL_MONTH:
      return ltime.month;
    case INTERVAL_WEEK: {
      week_format = current_thd->variables.default_week_format;
      return calc_week(ltime, week_mode(week_format), &year);
    }
    case INTERVAL_DAY:
      return ltime.day;
    case INTERVAL_DAY_HOUR:
      return (long)(ltime.day * 100L + ltime.hour) * neg;
    case INTERVAL_DAY_MINUTE:
      return (long)(ltime.day * 10000L + ltime.hour * 100L + ltime.minute) *
             neg;
    case INTERVAL_DAY_SECOND:
      return ((longlong)ltime.day * 1000000L +
              (longlong)(ltime.hour * 10000L + ltime.minute * 100 +
                         ltime.second)) *
             neg;
    case INTERVAL_HOUR:
      return (long)ltime.hour * neg;
    case INTERVAL_HOUR_MINUTE:
      return (long)(ltime.hour * 100 + ltime.minute) * neg;
    case INTERVAL_HOUR_SECOND:
      return (long)(ltime.hour * 10000 + ltime.minute * 100 + ltime.second) *
             neg;
    case INTERVAL_MINUTE:
      return (long)ltime.minute * neg;
    case INTERVAL_MINUTE_SECOND:
      return (long)(ltime.minute * 100 + ltime.second) * neg;
    case INTERVAL_SECOND:
      return (long)ltime.second * neg;
    case INTERVAL_MICROSECOND:
      return (long)ltime.second_part * neg;
    case INTERVAL_DAY_MICROSECOND:
      return (((longlong)ltime.day * 1000000L + (longlong)ltime.hour * 10000L +
               ltime.minute * 100 + ltime.second) *
                  1000000L +
              ltime.second_part) *
             neg;
    case INTERVAL_HOUR_MICROSECOND:
      return (((longlong)ltime.hour * 10000L + ltime.minute * 100 +
               ltime.second) *
                  1000000L +
              ltime.second_part) *
             neg;
    case INTERVAL_MINUTE_MICROSECOND:
      return (((longlong)(ltime.minute * 100 + ltime.second)) * 1000000L +
              ltime.second_part) *
             neg;
    case INTERVAL_SECOND_MICROSECOND:
      return ((longlong)ltime.second * 1000000L + ltime.second_part) * neg;
    case INTERVAL_LAST:
      assert(0);
      break; /* purecov: deadcode */
  }
  return 0;  // Impossible
}

bool Item_extract::eq(const Item *item, bool binary_cmp) const {
  if (this == item) return true;
  if (item->type() != FUNC_ITEM ||
      functype() != down_cast<const Item_func *>(item)->functype())
    return false;

  const Item_extract *ie = down_cast<const Item_extract *>(item);
  if (ie->int_type != int_type) return false;

  if (!args[0]->eq(ie->args[0], binary_cmp)) return false;
  return true;
}

void Item_typecast_datetime::print(const THD *thd, String *str,
                                   enum_query_type query_type) const {
  str->append(STRING_WITH_LEN("cast("));
  args[0]->print(thd, str, query_type);
  str->append(STRING_WITH_LEN(" as "));
  str->append(cast_type());
  if (decimals) str->append_parenthesized(decimals);
  str->append(')');
}

bool Item_typecast_datetime::get_date(MYSQL_TIME *ltime,
                                      my_time_flags_t fuzzy_date) {
  THD *const thd = current_thd;

  my_time_flags_t flags = fuzzy_date | TIME_NO_DATE_FRAC_WARN;
  if (thd->variables.sql_mode & MODE_NO_ZERO_DATE) flags |= TIME_NO_ZERO_DATE;
  if (thd->variables.sql_mode & MODE_NO_ZERO_IN_DATE)
    flags |= TIME_NO_ZERO_IN_DATE;
  if (thd->variables.sql_mode & MODE_INVALID_DATES) flags |= TIME_INVALID_DATES;
  if (thd->is_fsp_truncate_mode()) flags |= TIME_FRAC_TRUNCATE;

  if (get_arg0_date(ltime, flags)) {
    ltime->time_type = MYSQL_TIMESTAMP_DATETIME;
    if (args[0]->null_value || m_explicit_cast) return true;
    // The implicit CAST to DATETIME returns 0-date on invalid argument
    null_value = false;
    set_zero_time(ltime, ltime->time_type);
    return false;
  }
  assert(ltime->time_type != MYSQL_TIMESTAMP_TIME);
  ltime->time_type = MYSQL_TIMESTAMP_DATETIME;  // In case it was DATE
  int warnings = 0;
  return (null_value = propagate_datetime_overflow(
              thd, &warnings,
              my_datetime_adjust_frac(ltime, decimals, &warnings,
                                      thd->is_fsp_truncate_mode())));
}

void Item_typecast_time::print(const THD *thd, String *str,
                               enum_query_type query_type) const {
  str->append(STRING_WITH_LEN("cast("));
  args[0]->print(thd, str, query_type);
  str->append(STRING_WITH_LEN(" as "));
  str->append(cast_type());
  if (decimals) str->append_parenthesized(decimals);
  str->append(')');
}

bool Item_typecast_time::get_time(MYSQL_TIME *ltime) {
  if (get_arg0_time(ltime)) return true;
  my_time_adjust_frac(ltime, decimals, current_thd->is_fsp_truncate_mode());

  /*
    For MYSQL_TIMESTAMP_TIME value we can have non-zero day part,
    which we should not lose.
  */
  if (ltime->time_type != MYSQL_TIMESTAMP_TIME) datetime_to_time(ltime);
  return false;
}

void Item_typecast_date::print(const THD *thd, String *str,
                               enum_query_type query_type) const {
  str->append(STRING_WITH_LEN("cast("));
  args[0]->print(thd, str, query_type);
  str->append(STRING_WITH_LEN(" as "));
  str->append(cast_type());
  str->append(')');
}

bool Item_typecast_date::get_date(MYSQL_TIME *ltime,
                                  my_time_flags_t fuzzy_date) {
  THD *const thd = current_thd;

  my_time_flags_t flags = fuzzy_date | TIME_NO_DATE_FRAC_WARN;
  if (thd->variables.sql_mode & MODE_NO_ZERO_DATE) flags |= TIME_NO_ZERO_DATE;
  if (thd->variables.sql_mode & MODE_NO_ZERO_IN_DATE)
    flags |= TIME_NO_ZERO_IN_DATE;
  if (thd->variables.sql_mode & MODE_INVALID_DATES) flags |= TIME_INVALID_DATES;

  if (get_arg0_date(ltime, flags)) {
    if (args[0]->null_value || m_explicit_cast) return true;
    // The implicit cast to DATE returns 0-date instead of NULL
    null_value = false;
    set_zero_time(ltime, ltime->time_type);
    return false;
  }

  ltime->hour = 0;
  ltime->minute = 0;
  ltime->second = 0;
  ltime->second_part = 0;
  ltime->time_type = MYSQL_TIMESTAMP_DATE;

  return false;
}

/**
  MAKEDATE(a,b) is a date function that creates a date value
  from a year and day value.

  NOTES:
    As arguments are integers, we can't know if the year is a 2 digit or 4 digit
  year. In this case we treat all years < 100 as 2 digit years. Ie, this is not
  safe for dates between 0000-01-01 and 0099-12-31
*/

bool Item_func_makedate::get_date(MYSQL_TIME *ltime, my_time_flags_t) {
  assert(fixed == 1);
  long daynr = (long)args[1]->val_int();
  long year = (long)args[0]->val_int();
  long days;

  if (args[0]->null_value || args[1]->null_value || year < 0 || year > 9999 ||
      daynr <= 0 || daynr > MAX_DAY_NUMBER)
    goto err;

  if (year < 100) year = year_2000_handling(year);

  days = calc_daynr(year, 1, 1) + daynr - 1;
  /* Day number from year 0 to 9999-12-31 */
  if (days >= 0 && days <= MAX_DAY_NUMBER) {
    null_value = false;
    get_date_from_daynr(days, &ltime->year, &ltime->month, &ltime->day);
    ltime->neg = false;
    ltime->hour = ltime->minute = ltime->second = ltime->second_part = 0;
    ltime->time_type = MYSQL_TIMESTAMP_DATE;
    return false;
  }

err:
  null_value = true;
  return true;
}

bool Item_func_add_time::resolve_type(THD *thd) {
  if (param_type_is_default(thd, 0, 1,
                            m_datetime ? MYSQL_TYPE_DATETIME : MYSQL_TYPE_TIME))
    return true;
  if (param_type_is_default(thd, 1, 2, MYSQL_TYPE_TIME)) return true;

  /*
    The field type for the result of an Item_func_add_time function is defined
    as follows:

    - If first argument is MYSQL_TYPE_DATETIME, MYSQL_TYPE_TIMESTAMP or
      MYSQL_TYPE_DATE, then result is MYSQL_TYPE_DATETIME
    - If first argument is MYSQL_TYPE_TIME, then result is MYSQL_TYPE_TIME
    - Result type is overridden as MYSQL_TYPE_DATETIME if m_datetime,
      meaning that this is the implementation of the two-argument
      TIMESTAMP function.
    - Otherwise the result is MYSQL_TYPE_STRING
  */
  if (args[0]->data_type() == MYSQL_TYPE_TIME && !m_datetime) {
    uint8 dec = max(args[0]->time_precision(), args[1]->time_precision());
    set_data_type_time(dec);
  } else if (args[0]->data_type() == MYSQL_TYPE_DATETIME ||
             args[0]->data_type() == MYSQL_TYPE_TIMESTAMP ||
             args[0]->data_type() == MYSQL_TYPE_DATE || m_datetime) {
    uint8 dec = max(args[0]->datetime_precision(), args[1]->time_precision());
    set_data_type_datetime(dec);
  } else {
    set_data_type_char(MAX_DATETIME_FULL_WIDTH, default_charset());
  }
  set_nullable(true);
  return false;
}

/**
  ADDTIME(t,a) and SUBTIME(t,a) are time functions that calculate a
  time/datetime value

  @param time       time or datetime_expression.
  @param fuzzy_date flags that control temporal operation.

  @returns false on success, true on error or NULL value return.
*/

bool Item_func_add_time::val_datetime(MYSQL_TIME *time,
                                      my_time_flags_t fuzzy_date) {
  assert(fixed);
  MYSQL_TIME l_time1, l_time2;
  bool is_time = false;
  long days, microseconds;
  longlong seconds;
  int l_sign = m_sign;

  null_value = false;
  if (data_type() == MYSQL_TYPE_DATETIME)  // TIMESTAMP function
  {
    if (get_arg0_date(&l_time1, fuzzy_date) || args[1]->get_time(&l_time2) ||
        l_time1.time_type == MYSQL_TIMESTAMP_TIME ||
        l_time2.time_type != MYSQL_TIMESTAMP_TIME)
      goto null_date;
  } else  // ADDTIME function
  {
    if (args[0]->get_time(&l_time1) || args[1]->get_time(&l_time2) ||
        l_time2.time_type == MYSQL_TIMESTAMP_DATETIME ||
        l_time2.time_type == MYSQL_TIMESTAMP_DATETIME_TZ)
      goto null_date;
    is_time = (l_time1.time_type == MYSQL_TIMESTAMP_TIME);
  }
  if (l_time1.neg != l_time2.neg) l_sign = -l_sign;

  memset(time, 0, sizeof(MYSQL_TIME));

  time->neg =
      calc_time_diff(l_time1, l_time2, -l_sign, &seconds, &microseconds);

  /*
    If first argument was negative and diff between arguments
    is non-zero we need to swap sign to get proper result.
  */
  if (l_time1.neg && (seconds || microseconds))
    time->neg = 1 - time->neg;  // Swap sign of result

  if (!is_time && time->neg) goto null_date;

  days = (long)(seconds / SECONDS_IN_24H);

  calc_time_from_sec(time, seconds % SECONDS_IN_24H, microseconds);

  if (!is_time) {
    get_date_from_daynr(days, &time->year, &time->month, &time->day);
    time->time_type = MYSQL_TIMESTAMP_DATETIME;

    if (check_datetime_range(*time)) {
      // Value is out of range, cannot use our printing functions to output it.
      push_warning_printf(
          current_thd, Sql_condition::SL_WARNING, ER_DATETIME_FUNCTION_OVERFLOW,
          ER_THD(current_thd, ER_DATETIME_FUNCTION_OVERFLOW), func_name());
      goto null_date;
    }

    if (time->day) return false;
    goto null_date;
  }
  time->time_type = MYSQL_TIMESTAMP_TIME;
  time->hour += days * 24;
  if (adjust_time_range_with_warn(time, 0)) goto null_date;
  return false;

null_date:
  null_value = true;
  return true;
}

void Item_func_add_time::print(const THD *thd, String *str,
                               enum_query_type query_type) const {
  if (m_datetime) {
    assert(m_sign > 0);
    str->append(STRING_WITH_LEN("timestamp("));
  } else {
    if (m_sign > 0)
      str->append(STRING_WITH_LEN("addtime("));
    else
      str->append(STRING_WITH_LEN("subtime("));
  }
  args[0]->print(thd, str, query_type);
  str->append(',');
  args[1]->print(thd, str, query_type);
  str->append(')');
}

/**
  TIMEDIFF(t,s) is a time function that calculates the
  time value between a start and end time.

  t and s: time_or_datetime_expression
  @param[out]  l_time3   Result is stored here.

  @retval   false  On success
  @retval   true   On error
*/

bool Item_func_timediff::get_time(MYSQL_TIME *l_time3) {
  assert(fixed == 1);
  longlong seconds;
  long microseconds;
  int l_sign = 1;
  MYSQL_TIME l_time1, l_time2;

  null_value = false;

  if ((args[0]->is_temporal_with_date() &&
       args[1]->data_type() == MYSQL_TYPE_TIME) ||
      (args[1]->is_temporal_with_date() &&
       args[0]->data_type() == MYSQL_TYPE_TIME))
    goto null_date;  // Incompatible types

  if (args[0]->is_temporal_with_date() || args[1]->is_temporal_with_date()) {
    if (args[0]->get_date(&l_time1, TIME_FUZZY_DATE) ||
        args[1]->get_date(&l_time2, TIME_FUZZY_DATE))
      goto null_date;
  } else {
    if (args[0]->get_time(&l_time1) || args[1]->get_time(&l_time2))
      goto null_date;
  }

  if (l_time1.time_type != l_time2.time_type)
    goto null_date;  // Incompatible types

  if (l_time1.neg != l_time2.neg) l_sign = -l_sign;

  memset(l_time3, 0, sizeof(*l_time3));

  l_time3->neg =
      calc_time_diff(l_time1, l_time2, l_sign, &seconds, &microseconds);

  /*
    For MYSQL_TIMESTAMP_TIME only:
      If first argument was negative and diff between arguments
      is non-zero we need to swap sign to get proper result.
  */
  if (l_time1.neg && (seconds || microseconds))
    l_time3->neg = 1 - l_time3->neg;  // Swap sign of result

  calc_time_from_sec(l_time3, seconds, microseconds);
  if (adjust_time_range_with_warn(l_time3, decimals)) goto null_date;
  return false;

null_date:
  return (null_value = true);
}

/**
  MAKETIME(h,m,s) is a time function that calculates a time value
  from the total number of hours, minutes, and seconds.
  Result: Time value
*/

bool Item_func_maketime::get_time(MYSQL_TIME *ltime) {
  assert(fixed == 1);
  bool overflow = false;
  longlong hour = args[0]->val_int();
  longlong minute = args[1]->val_int();
  my_decimal tmp, *sec = args[2]->val_decimal(&tmp);
  lldiv_t second;

  if ((null_value =
           (args[0]->null_value || args[1]->null_value || args[2]->null_value ||
            my_decimal2lldiv_t(E_DEC_FATAL_ERROR, sec, &second) || minute < 0 ||
            minute > 59 || second.quot < 0 || second.quot > 59 ||
            second.rem < 0)))
    return true;

  set_zero_time(ltime, MYSQL_TIMESTAMP_TIME);

  /* Check for integer overflows */
  if (hour < 0) {
    if (args[0]->unsigned_flag)
      overflow = true;
    else
      ltime->neg = true;
  }
  if (-hour > UINT_MAX || hour > UINT_MAX) overflow = true;

  if (!overflow) {
    ltime->hour = (uint)((hour < 0 ? -hour : hour));
    ltime->minute = (uint)minute;
    ltime->second = (uint)second.quot;
    int warnings = 0;
    ltime->second_part = static_cast<ulong>(second.rem / 1000);
    if (adjust_time_range_with_warn(ltime, decimals)) return true;
    time_add_nanoseconds_adjust_frac(ltime, second.rem % 1000, &warnings,
                                     current_thd->is_fsp_truncate_mode());

    if (!warnings) return false;
  }

  // Return maximum value (positive or negative)
  set_max_hhmmss(ltime);
  char
      buf[MAX_BIGINT_WIDTH /* hh */ + 6 /* :mm:ss */ + 10 /* .fffffffff */ + 1];
  char *ptr = longlong10_to_str(hour, buf, args[0]->unsigned_flag ? 10 : -10);
  int len = (int)(ptr - buf) +
            sprintf(ptr, ":%02u:%02u", (uint)minute, (uint)second.quot);
  if (second.rem) {
    /*
      Display fractional part up to nanoseconds (9 digits),
      which is the maximum precision of my_decimal2lldiv_t().
    */
    int dec = min(args[2]->decimals, uint8{9});
    len += sprintf(buf + len, ".%0*lld", dec,
                   second.rem / (ulong)log_10_int[9 - dec]);
  }
  assert(strlen(buf) < sizeof(buf));
  return make_truncated_value_warning(current_thd, Sql_condition::SL_WARNING,
                                      ErrConvString(buf, len),
                                      MYSQL_TIMESTAMP_TIME, NullS);
}

/**
  MICROSECOND(a) is a function ( extraction) that extracts the microseconds
  from a.

  a: Datetime or time value
  Result: int value
*/

longlong Item_func_microsecond::val_int() {
  assert(fixed == 1);
  MYSQL_TIME ltime;
  return get_arg0_time(&ltime) ? 0 : ltime.second_part;
}

bool Item_func_microsecond::resolve_type(THD *thd) {
  if (param_type_is_default(thd, 0, -1, MYSQL_TYPE_DATETIME)) return true;
  set_nullable(true);
  return false;
}

longlong Item_func_timestamp_diff::val_int() {
  MYSQL_TIME ltime1, ltime2;
  longlong seconds;
  long microseconds;
  long months = 0;
  int neg = 1;

  null_value = false;
  if (args[0]->get_date(&ltime1, TIME_NO_ZERO_DATE) ||
      args[1]->get_date(&ltime2, TIME_NO_ZERO_DATE))
    goto null_date;

  if (calc_time_diff(ltime2, ltime1, 1, &seconds, &microseconds)) neg = -1;

  if (int_type == INTERVAL_YEAR || int_type == INTERVAL_QUARTER ||
      int_type == INTERVAL_MONTH) {
    uint year_beg, year_end, month_beg, month_end, day_beg, day_end;
    uint years = 0;
    uint second_beg, second_end, microsecond_beg, microsecond_end;

    if (neg == -1) {
      year_beg = ltime2.year;
      year_end = ltime1.year;
      month_beg = ltime2.month;
      month_end = ltime1.month;
      day_beg = ltime2.day;
      day_end = ltime1.day;
      second_beg = ltime2.hour * 3600 + ltime2.minute * 60 + ltime2.second;
      second_end = ltime1.hour * 3600 + ltime1.minute * 60 + ltime1.second;
      microsecond_beg = ltime2.second_part;
      microsecond_end = ltime1.second_part;
    } else {
      year_beg = ltime1.year;
      year_end = ltime2.year;
      month_beg = ltime1.month;
      month_end = ltime2.month;
      day_beg = ltime1.day;
      day_end = ltime2.day;
      second_beg = ltime1.hour * 3600 + ltime1.minute * 60 + ltime1.second;
      second_end = ltime2.hour * 3600 + ltime2.minute * 60 + ltime2.second;
      microsecond_beg = ltime1.second_part;
      microsecond_end = ltime2.second_part;
    }

    /* calc years */
    years = year_end - year_beg;
    if (month_end < month_beg || (month_end == month_beg && day_end < day_beg))
      years -= 1;

    /* calc months */
    months = 12 * years;
    if (month_end < month_beg || (month_end == month_beg && day_end < day_beg))
      months += 12 - (month_beg - month_end);
    else
      months += (month_end - month_beg);

    if (day_end < day_beg)
      months -= 1;
    else if ((day_end == day_beg) &&
             ((second_end < second_beg) ||
              (second_end == second_beg && microsecond_end < microsecond_beg)))
      months -= 1;
  }

  switch (int_type) {
    case INTERVAL_YEAR:
      return months / 12 * neg;
    case INTERVAL_QUARTER:
      return months / 3 * neg;
    case INTERVAL_MONTH:
      return months * neg;
    case INTERVAL_WEEK:
      return seconds / SECONDS_IN_24H / 7L * neg;
    case INTERVAL_DAY:
      return seconds / SECONDS_IN_24H * neg;
    case INTERVAL_HOUR:
      return seconds / 3600L * neg;
    case INTERVAL_MINUTE:
      return seconds / 60L * neg;
    case INTERVAL_SECOND:
      return seconds * neg;
    case INTERVAL_MICROSECOND:
      /*
        In MySQL difference between any two valid datetime values
        in microseconds fits into longlong.
      */
      return (seconds * 1000000L + microseconds) * neg;
    default:
      break;
  }

null_date:
  null_value = true;
  return 0;
}

void Item_func_timestamp_diff::print(const THD *thd, String *str,
                                     enum_query_type query_type) const {
  str->append(func_name());
  str->append('(');

  switch (int_type) {
    case INTERVAL_YEAR:
      str->append(STRING_WITH_LEN("YEAR"));
      break;
    case INTERVAL_QUARTER:
      str->append(STRING_WITH_LEN("QUARTER"));
      break;
    case INTERVAL_MONTH:
      str->append(STRING_WITH_LEN("MONTH"));
      break;
    case INTERVAL_WEEK:
      str->append(STRING_WITH_LEN("WEEK"));
      break;
    case INTERVAL_DAY:
      str->append(STRING_WITH_LEN("DAY"));
      break;
    case INTERVAL_HOUR:
      str->append(STRING_WITH_LEN("HOUR"));
      break;
    case INTERVAL_MINUTE:
      str->append(STRING_WITH_LEN("MINUTE"));
      break;
    case INTERVAL_SECOND:
      str->append(STRING_WITH_LEN("SECOND"));
      break;
    case INTERVAL_MICROSECOND:
      str->append(STRING_WITH_LEN("MICROSECOND"));
      break;
    default:
      break;
  }

  for (uint i = 0; i < 2; i++) {
    str->append(',');
    args[i]->print(thd, str, query_type);
  }
  str->append(')');
}

String *Item_func_get_format::val_str_ascii(String *str) {
  assert(fixed == 1);
  const char *format_name;
  const Known_date_time_format *format;
  String *val = args[0]->val_str_ascii(str);
  size_t val_len;

  if ((null_value = args[0]->null_value)) return nullptr;

  val_len = val->length();
  for (format = &known_date_time_formats[0];
       (format_name = format->format_name); format++) {
    size_t format_name_len;
    format_name_len = strlen(format_name);
    if (val_len == format_name_len &&
        !my_strnncoll(&my_charset_latin1, (const uchar *)val->ptr(), val_len,
                      (const uchar *)format_name, val_len)) {
      const char *format_str = get_date_time_format_str(format, type);
      str->set(format_str, strlen(format_str), &my_charset_numeric);
      return str;
    }
  }

  null_value = true;
  return nullptr;
}

void Item_func_get_format::print(const THD *thd, String *str,
                                 enum_query_type query_type) const {
  str->append(func_name());
  str->append('(');

  switch (type) {
    case MYSQL_TIMESTAMP_DATE:
      str->append(STRING_WITH_LEN("DATE, "));
      break;
    case MYSQL_TIMESTAMP_DATETIME:
      str->append(STRING_WITH_LEN("DATETIME, "));
      break;
    case MYSQL_TIMESTAMP_TIME:
      str->append(STRING_WITH_LEN("TIME, "));
      break;
    default:
      assert(0);
  }
  args[0]->print(thd, str, query_type);
  str->append(')');
}

/**
  Set type of datetime value (DATE/TIME/...) which will be produced
  according to format string.

  @param format   format string
  @param length   length of format string

  @note
    We don't process day format's characters('D', 'd', 'e') because day
    may be a member of all date/time types.

  @note
    Format specifiers supported by this function should be in sync with
    specifiers supported by extract_date_time() function.
*/
void Item_func_str_to_date::fix_from_format(const char *format, size_t length) {
  const char *time_part_frms = "HISThiklrs";
  const char *date_part_frms = "MVUXYWabcjmvuxyw";
  bool date_part_used = false, time_part_used = false, frac_second_used = false;
  const char *val = format;
  const char *end = format + length;

  for (; val != end; val++) {
    if (*val == '%' && val + 1 != end) {
      val++;
      if (*val == 'f')
        frac_second_used = time_part_used = true;
      else if (!time_part_used && strchr(time_part_frms, *val))
        time_part_used = true;
      else if (!date_part_used && strchr(date_part_frms, *val))
        date_part_used = true;
      if (date_part_used && frac_second_used) {
        /*
          frac_second_used implies time_part_used, and thus we already
          have all types of date-time components and can end our search.
        */
        cached_timestamp_type = MYSQL_TIMESTAMP_DATETIME;
        set_data_type_datetime(DATETIME_MAX_DECIMALS);
        return;
      }
    }
  }

  /* We don't have all three types of date-time components */
  if (frac_second_used) /* TIME with microseconds */
  {
    cached_timestamp_type = MYSQL_TIMESTAMP_TIME;
    set_data_type_time(DATETIME_MAX_DECIMALS);
  } else if (time_part_used) {
    if (date_part_used) /* DATETIME, no microseconds */
    {
      cached_timestamp_type = MYSQL_TIMESTAMP_DATETIME;
      set_data_type_datetime(0);
    } else /* TIME, no microseconds */
    {
      cached_timestamp_type = MYSQL_TIMESTAMP_TIME;
      set_data_type_time(0);
    }
  } else /* DATE */
  {
    cached_timestamp_type = MYSQL_TIMESTAMP_DATE;
    set_data_type_date();
  }
}

bool Item_func_str_to_date::resolve_type(THD *thd) {
  if (param_type_is_default(thd, 0, 2)) return true;
  set_nullable(true);
  cached_timestamp_type = MYSQL_TIMESTAMP_DATETIME;
  set_data_type_datetime(DATETIME_MAX_DECIMALS);
  sql_mode = thd->variables.sql_mode &
             (MODE_NO_ZERO_DATE | MODE_NO_ZERO_IN_DATE | MODE_INVALID_DATES);
  if (args[1]->const_item() && args[1]->may_eval_const_item(thd)) {
    char format_buff[64];
    String format_str(format_buff, sizeof(format_buff), &my_charset_bin);
    String *format = args[1]->val_str(&format_str);
    if (!args[1]->null_value) fix_from_format(format->ptr(), format->length());
  }
  return false;
}

/**
  Determines whether this date should be NULL (and a warning raised) under the
  given sql_mode. Zeroes are allowed in the date if the data type is TIME.

  @param target_type The data type of the time/date.
  @param time Date and time data
  @param fuzzy_date What sql_mode dictates.
  @return Whether the result is valid or NULL.
*/
static bool date_should_be_null(enum_field_types target_type,
                                const MYSQL_TIME &time,
                                my_time_flags_t fuzzy_date) {
  return (fuzzy_date & TIME_NO_ZERO_DATE) != 0 &&
         (target_type != MYSQL_TYPE_TIME) &&
         (time.year == 0 || time.month == 0 || time.day == 0);
}

bool Item_func_str_to_date::val_datetime(MYSQL_TIME *ltime,
                                         my_time_flags_t fuzzy_date) {
  Date_time_format date_time_format;
  char val_buff[64], format_buff[64];
  String val_string(val_buff, sizeof(val_buff), &my_charset_bin), *val;
  String format_str(format_buff, sizeof(format_buff), &my_charset_bin), *format;

  if (sql_mode & MODE_NO_ZERO_IN_DATE) fuzzy_date |= TIME_NO_ZERO_IN_DATE;
  if (sql_mode & MODE_NO_ZERO_DATE) fuzzy_date |= TIME_NO_ZERO_DATE;
  if (sql_mode & MODE_INVALID_DATES) fuzzy_date |= TIME_INVALID_DATES;

  val = args[0]->val_str(&val_string);
  format = args[1]->val_str(&format_str);
  if (args[0]->null_value || args[1]->null_value) goto null_date;

  null_value = false;
  memset(ltime, 0, sizeof(*ltime));
  date_time_format.format.str = format->ptr();
  date_time_format.format.length = format->length();
  if (extract_date_time(&date_time_format, val->ptr(), val->length(), ltime,
                        cached_timestamp_type, nullptr, "datetime",
                        func_name()))
    goto null_date;
  if (date_should_be_null(data_type(), *ltime, fuzzy_date)) {
    char buff[128];
    strmake(buff, val->ptr(), min<size_t>(val->length(), sizeof(buff) - 1));
    push_warning_printf(current_thd, Sql_condition::SL_WARNING,
                        ER_WRONG_VALUE_FOR_TYPE,
                        ER_THD(current_thd, ER_WRONG_VALUE_FOR_TYPE),
                        "datetime", buff, "str_to_date");
    goto null_date;
  }
  ltime->time_type = cached_timestamp_type;
  if (cached_timestamp_type == MYSQL_TIMESTAMP_TIME && ltime->day) {
    /*
      Day part for time type can be nonzero value and so
      we should add hours from day part to hour part to
      keep valid time value.
    */
    ltime->hour += ltime->day * 24;
    ltime->day = 0;
  }
  return false;

null_date:
  null_value = true;

  return true;
}

bool Item_func_last_day::get_date(MYSQL_TIME *ltime,
                                  my_time_flags_t fuzzy_date) {
  if ((null_value = get_arg0_date(ltime, fuzzy_date))) return true;

  if (ltime->month == 0) {
    /*
      Cannot calculate last day for zero month.
      Let's print a warning and return NULL.
    */
    ltime->time_type = MYSQL_TIMESTAMP_DATE;
    ErrConvString str(ltime, 0);
    if (make_truncated_value_warning(current_thd, Sql_condition::SL_WARNING,
                                     str, MYSQL_TIMESTAMP_ERROR, NullS))
      return true;
    return (null_value = true);
  }

  uint month_idx = ltime->month - 1;
  ltime->day = days_in_month[month_idx];
  if (month_idx == 1 && calc_days_in_year(ltime->year) == 366) ltime->day = 29;
  datetime_to_date(ltime);
  return false;
}

bool Item_func_internal_update_time::resolve_type(THD *thd) {
  set_data_type_datetime(0);
  set_nullable(true);
  null_on_null = false;
  thd->time_zone_used = true;
  return false;
}

bool Item_func_internal_update_time::get_date(MYSQL_TIME *ltime,
                                              my_time_flags_t) {
  DBUG_TRACE;

  String schema_name;
  String *schema_name_ptr;
  String table_name;
  String *table_name_ptr = nullptr;
  String engine_name;
  String *engine_name_ptr = nullptr;
  String partition_name;
  String *partition_name_ptr = nullptr;
  bool skip_hidden_table = args[4]->val_int();
  String ts_se_private_data;
  String *ts_se_private_data_ptr = args[5]->val_str(&ts_se_private_data);
  ulonglong stat_data = args[6]->val_uint();
  ulonglong cached_timestamp = args[7]->val_uint();
  ulonglong unixtime = 0;

  if ((schema_name_ptr = args[0]->val_str(&schema_name)) != nullptr &&
      (table_name_ptr = args[1]->val_str(&table_name)) != nullptr &&
      (engine_name_ptr = args[2]->val_str(&engine_name)) != nullptr &&
      !is_infoschema_db(schema_name_ptr->c_ptr_safe()) && !skip_hidden_table) {
    dd::Object_id se_private_id = (dd::Object_id)args[3]->val_uint();
    THD *thd = current_thd;

    MYSQL_TIME time;
    bool not_used;
    // Convert longlong time to MYSQL_TIME format
    my_longlong_to_datetime_with_warn(stat_data, &time, MYF(0));

    // Convert MYSQL_TIME to epoc second according to local time_zone as
    // cached_timestamp value is with local time_zone
    my_time_t timestamp;
    timestamp = thd->variables.time_zone->TIME_to_gmt_sec(&time, &not_used);

    // Make sure we have safe string to access.
    schema_name_ptr->c_ptr_safe();
    table_name_ptr->c_ptr_safe();
    engine_name_ptr->c_ptr_safe();

    /*
      The same native function used by I_S.PARTITIONS is used by I_S.TABLES.
      We invoke native function with partition name only with I_S.PARTITIONS
      as a last argument. So, we check for argument count below, before
      reading partition name.
    */
    if (arg_count == 10)
      partition_name_ptr = args[9]->val_str(&partition_name);
    else if (arg_count == 9)
      partition_name_ptr = args[8]->val_str(&partition_name);

    unixtime = thd->lex->m_IS_table_stats.read_stat(
        thd, *schema_name_ptr, *table_name_ptr, *engine_name_ptr,
        (partition_name_ptr ? partition_name_ptr->c_ptr_safe() : nullptr),
        se_private_id,
        (ts_se_private_data_ptr ? ts_se_private_data_ptr->c_ptr_safe()
                                : nullptr),
        nullptr, static_cast<ulonglong>(timestamp), cached_timestamp,
        dd::info_schema::enum_table_stats_type::TABLE_UPDATE_TIME);
    if (unixtime) {
      null_value = false;
      thd->variables.time_zone->gmt_sec_to_TIME(ltime, (my_time_t)unixtime);
      return false;
    }
  }

  null_value = true;
  return true;
}

bool Item_func_internal_check_time::resolve_type(THD *thd) {
  set_data_type_datetime(0);
  set_nullable(true);
  null_on_null = false;
  thd->time_zone_used = true;
  return false;
}

bool Item_func_internal_check_time::get_date(MYSQL_TIME *ltime,
                                             my_time_flags_t) {
  DBUG_TRACE;

  String schema_name;
  String *schema_name_ptr;
  String table_name;
  String *table_name_ptr = nullptr;
  String engine_name;
  String *engine_name_ptr = nullptr;
  String partition_name;
  String *partition_name_ptr = nullptr;
  bool skip_hidden_table = args[4]->val_int();
  String ts_se_private_data;
  String *ts_se_private_data_ptr = args[5]->val_str(&ts_se_private_data);
  ulonglong stat_data = args[6]->val_uint();
  ulonglong cached_timestamp = args[7]->val_uint();
  ulonglong unixtime = 0;

  if ((schema_name_ptr = args[0]->val_str(&schema_name)) != nullptr &&
      (table_name_ptr = args[1]->val_str(&table_name)) != nullptr &&
      (engine_name_ptr = args[2]->val_str(&engine_name)) != nullptr &&
      !is_infoschema_db(schema_name_ptr->c_ptr_safe()) && !skip_hidden_table) {
    dd::Object_id se_private_id = (dd::Object_id)args[3]->val_uint();
    THD *thd = current_thd;

    MYSQL_TIME time;
    bool not_used = true;
    // Convert longlong time to MYSQL_TIME format
    if (my_longlong_to_datetime_with_warn(stat_data, &time, MYF(0))) {
      null_value = true;
      return true;
    }

    // Convert MYSQL_TIME to epoc second according to local time_zone as
    // cached_timestamp value is with local time_zone
    my_time_t timestamp =
        thd->variables.time_zone->TIME_to_gmt_sec(&time, &not_used);
    // Make sure we have safe string to access.
    schema_name_ptr->c_ptr_safe();
    table_name_ptr->c_ptr_safe();
    engine_name_ptr->c_ptr_safe();

    /*
      The same native function used by I_S.PARTITIONS is used by I_S.TABLES.
      We invoke native function with partition name only with I_S.PARTITIONS
      as a last argument. So, we check for argument count below, before
      reading partition name.
     */
    if (arg_count == 10)
      partition_name_ptr = args[9]->val_str(&partition_name);
    else if (arg_count == 9)
      partition_name_ptr = args[8]->val_str(&partition_name);

    unixtime = thd->lex->m_IS_table_stats.read_stat(
        thd, *schema_name_ptr, *table_name_ptr, *engine_name_ptr,
        (partition_name_ptr ? partition_name_ptr->c_ptr_safe() : nullptr),
        se_private_id,
        (ts_se_private_data_ptr ? ts_se_private_data_ptr->c_ptr_safe()
                                : nullptr),
        nullptr, static_cast<ulonglong>(timestamp), cached_timestamp,
        dd::info_schema::enum_table_stats_type::CHECK_TIME);

    if (unixtime) {
      null_value = false;
      thd->variables.time_zone->gmt_sec_to_TIME(ltime, (my_time_t)unixtime);
      return false;
    }
  }

  null_value = true;
  return true;
}

double Item_func_months_between::val_real() {
  MYSQL_TIME ltime1, ltime2;
  longlong seconds;
  long microseconds;
  long months = 0;
  int neg = 1;
  double ret_val = 0.0;

  uint year_beg, year_end, month_beg, month_end, day_beg, day_end;
  uint years = 0;
  int second_beg, second_end;

  null_value = false;
  if (args[0]->get_date(&ltime1, TIME_NO_ZERO_DATE) ||
      args[1]->get_date(&ltime2, TIME_NO_ZERO_DATE))
    goto null_date;

  if (calc_time_diff(ltime2, ltime1, 1, &seconds, &microseconds)) neg = -1;
  if (neg == -1) {
    year_beg = ltime2.year;
    year_end = ltime1.year;
    month_beg = ltime2.month;
    month_end = ltime1.month;
    day_beg = ltime2.day;
    day_end = ltime1.day;
    second_beg = ltime2.hour * 3600 + ltime2.minute * 60 + ltime2.second;
    second_end = ltime1.hour * 3600 + ltime1.minute * 60 + ltime1.second;
  } else {
    year_beg = ltime1.year;
    year_end = ltime2.year;
    month_beg = ltime1.month;
    month_end = ltime2.month;
    day_beg = ltime1.day;
    day_end = ltime2.day;
    second_beg = ltime1.hour * 3600 + ltime1.minute * 60 + ltime1.second;
    second_end = ltime2.hour * 3600 + ltime2.minute * 60 + ltime2.second;
  }

  /* calc years */
  years = year_end - year_beg;
  if (month_end < month_beg || (month_end == month_beg && day_end < day_beg))
    years -= 1;

  /* calc months */
  months = 12 * years;
  if (month_end < month_beg || (month_end == month_beg && day_end < day_beg))
    months += 12 - (month_beg - month_end);
  else
    months += (month_end - month_beg);

  /*  bugfix zentao #3622
   *  `months_between('2022-02-24 01:00:00','2022-02-24 03:00:00')`
   *  returns unexpected '-0'
   *  do nothing if the date part of date1 is the same as date2.
   *  */
  if (months != 0 || day_beg != day_end) {
    if (day_end == calc_days_in_month(year_end, month_end) &&
        day_beg == calc_days_in_month(year_beg, month_beg)) {
      /* if both days are end of the month respectively, ignore the day
       * difference, which result in returning full month diff other than a
       * fractional one */
      ret_val = (double)months * neg;
    } else {
      /*  bugfix zentao #3622
       *  Considering time difference between date1 and date2,
       *  as what oracle does.
       * */
      double day_frac_diff = 0.0;

      if (day_beg == day_end) {
        ret_val = (double)months * neg;
      } else if (day_end < day_beg) {
        day_frac_diff = (double)(second_beg - second_end) / (3600 * 24);
        ret_val = (double)months * neg -
                  (double)(day_beg - day_end + day_frac_diff) / 31 * neg;
      } else {
        day_frac_diff = (double)(second_end - second_beg) / (3600 * 24);
        ret_val = (double)months * neg +
                  (double)(day_end - day_beg + day_frac_diff) / 31 * neg;
      }
    }
  }

  return ret_val;

null_date:
  null_value = true;
  return 0;
}

void Item_func_months_between::print(const THD *thd, String *str,
                                     enum_query_type query_type) const {
  str->append(func_name());
  str->append('(');
  args[0]->print(thd, str, query_type);
  str->append(',');
  args[1]->print(thd, str, query_type);
  str->append(')');
}

bool Item_func_to_date::resolve_type(THD *thd) {
  if (param_type_is_default(thd, 0, 2)) return true;
  set_nullable(true);
  cached_timestamp_type = MYSQL_TIMESTAMP_DATETIME;
  set_data_type_datetime(0);

  sql_mode = thd->variables.sql_mode |
             (MODE_NO_ZERO_DATE | MODE_NO_ZERO_IN_DATE | MODE_INVALID_DATES);
  locale = thd->variables.lc_time_names;

  return false;
}

bool Item_func_to_date::val_datetime(MYSQL_TIME *ltime, my_time_flags_t) {
  assert(cached_timestamp_type == MYSQL_TIMESTAMP_DATETIME);

  Date_time_format date_time_format;
  char val_buff[64];
  String val_string(val_buff, sizeof(val_buff), &my_charset_bin), *val;

  uint16 fmt_array[MAX_DATETIME_FORMAT_MODEL_LEN + 1];
  uint ulen;

  null_value = false;
  memset(ltime, 0, sizeof(*ltime));

  String ora_format, *ora_format_val;
  ora_format_val = args[1]->val_str(&ora_format);
  if (args[1]->null_value) {
    goto null_date;
  }

  if (!generate_date_format_array(ora_format_val, locale, system_charset_info,
                                  fmt_array, &ulen)) {
    my_error(ER_WRONG_VALUE, MYF(0), "string",
             "please check the format string");
    goto null_date;
  }

  val = args[0]->val_str(&val_string);
  if (args[0]->null_value || !val || !val->length()) {
    goto null_date;
  }

  {
    auto fmt_array_end =
        std::find(std::begin(fmt_array), std::end(fmt_array), 0);
    std::unordered_set<uint16> fmt_set;
    uint fmt_count = 0;
    std::copy_if(std::begin(fmt_array), fmt_array_end,
                 std::inserter(fmt_set, fmt_set.end()),
                 [&fmt_count](uint16 fmt) {
                   if (fmt > FMT_BASE && fmt != FMT_th && fmt != FMT_TH &&
                       fmt != FMT_FM && fmt != FMT_FX && fmt != FMT_FM &&
                       fmt != FMT_FX) {
                     fmt_count++;
                     return true;
                   } else {
                     return false;
                   }
                 });
    if (fmt_count != fmt_set.size()) {
      my_error(ER_WRONG_VALUE, MYF(0), "string", "format code appears twice");
      goto null_date;
    }

    date_time_format.format.str = (char *)fmt_array;
    date_time_format.format.length =
        (fmt_array_end - std::begin(fmt_array)) * 2;
  }

  if (extract_date_time(&date_time_format, val->ptr(), val->length(), ltime,
                        cached_timestamp_type, nullptr, "datetime",
                        func_name()))
    goto null_date;

  ltime->time_type = cached_timestamp_type;
  return false;

null_date:
  null_value = true;

  return true;
}

/**

  to_timestamp::set_decimal_digits

  @param fmt_array   oracle-style time format model.

  @note
    set decimal_digit as the value after FMT_FF.
*/
void Item_func_to_timestamp::set_decimal_digits(uint16 *fmt_array) {
  uint16 *ptr = fmt_array;
  while (*ptr++) {
    if (*ptr == FMT_FF && (ptr + 1) != nullptr && *(ptr + 1) != 0) {
      decimal_digits = (*(ptr + 1) < DATETIME_MAX_DECIMALS)
                           ? *(ptr + 1)
                           : DATETIME_MAX_DECIMALS;
      return;
    }
  }
}

bool Item_func_to_timestamp::resolve_type(THD *thd) {
  if (param_type_is_default(thd, 0, 2)) return true;
  set_nullable(true);
  cached_timestamp_type = MYSQL_TIMESTAMP_DATETIME;
  set_data_type_datetime(DATETIME_MAX_DECIMALS);
  sql_mode = thd->variables.sql_mode &
             (MODE_NO_ZERO_DATE | MODE_NO_ZERO_IN_DATE | MODE_INVALID_DATES);
  locale = thd->variables.lc_time_names;

  return false;
}

bool Item_func_to_timestamp::val_datetime(MYSQL_TIME *ltime,
                                          my_time_flags_t fuzzy_date) {
  Date_time_format date_time_format;
  char val_buff[64] = {0};
  String val_string(val_buff, sizeof(val_buff), &my_charset_bin), *val;

  uint16 fmt_array[MAX_DATETIME_FORMAT_MODEL_LEN + 1] = {0};
  uint ulen;

  if (sql_mode & MODE_NO_ZERO_IN_DATE) fuzzy_date |= TIME_NO_ZERO_IN_DATE;
  if (sql_mode & MODE_NO_ZERO_DATE) fuzzy_date |= TIME_NO_ZERO_DATE;
  if (sql_mode & MODE_INVALID_DATES) fuzzy_date |= TIME_INVALID_DATES;

  null_value = false;
  memset(ltime, 0, sizeof(*ltime));

  String ora_format, *ora_format_val;
  ora_format_val = args[1]->val_str(&ora_format);
  if (args[1]->null_value) {
    goto null_date;
  }

  if (!generate_date_format_array(ora_format_val, locale, system_charset_info,
                                  fmt_array, &ulen)) {
    my_error(ER_WRONG_VALUE, MYF(0), "string",
             "please check the format string");
    goto null_date;
  }

  val = args[0]->val_str(&val_string);
  if (args[0]->null_value || !val || !val->length()) {
    goto null_date;
  }

  {
    auto fmt_array_end =
        std::find(std::begin(fmt_array), std::end(fmt_array), 0);
    std::unordered_set<uint16> fmt_set;
    uint fmt_count = 0;
    std::copy_if(std::begin(fmt_array), fmt_array_end,
                 std::inserter(fmt_set, fmt_set.end()),
                 [&fmt_count](uint16 fmt) {
                   if (fmt > FMT_BASE && fmt != FMT_th && fmt != FMT_TH &&
                       fmt != FMT_FM && fmt != FMT_FX && fmt != FMT_FM &&
                       fmt != FMT_FX) {
                     fmt_count++;
                     return true;
                   } else {
                     return false;
                   }
                 });
    if (fmt_count != fmt_set.size()) {
      my_error(ER_WRONG_VALUE, MYF(0), "string", "format code appears twice");
      goto null_date;
    }

    date_time_format.format.str = (char *)fmt_array;
    date_time_format.format.length =
        (fmt_array_end - std::begin(fmt_array)) * 2;
  }

  if (extract_date_time(&date_time_format, val->ptr(), val->length(), ltime,
                        cached_timestamp_type, nullptr, "datetime",
                        func_name()))
    goto null_date;

  set_decimal_digits(fmt_array);

  /**
    adjust ltime->second_part by setting the last
   'DATETIME_MAX_DECIMALS - decimal_digits' digits
    to 0. such as:
    .123456  --> .123450 (decimal_digits: 5)
    .123456  --> .123000 (decimal_digits: 3)
  */
  ltime->second_part -=
      ltime->second_part %
      (ulong)log_10_int[DATETIME_MAX_DECIMALS - decimal_digits];

  if (date_should_be_null(data_type(), *ltime, fuzzy_date)) {
    char buff[128];
    strmake(buff, val->ptr(), min<size_t>(val->length(), sizeof(buff) - 1));
    push_warning_printf(current_thd, Sql_condition::SL_WARNING,
                        ER_WRONG_VALUE_FOR_TYPE,
                        ER_THD(current_thd, ER_WRONG_VALUE_FOR_TYPE),
                        "datetime", buff, func_name());
    goto null_date;
  }

  ltime->time_type = cached_timestamp_type;
  return false;

null_date:
  null_value = true;

  return true;
}

bool Item_func_trunc::resolve_type(THD *thd) {
  if (is_temporal_type(args[0]->data_type())) {
    if (param_type_is_default(thd, 0, 1, MYSQL_TYPE_DATETIME)) return true;
    // trunc(date) expected, reset the datatype.
    set_data_type_datetime(0);
    trunc_date = true;
    hybrid_type = STRING_RESULT;
    return param_type_uses_non_param(thd);
  } else {
    return super::resolve_type(thd);
  }
}

/**
  Check the arguments of function trunc(number).
  prompt error if the conversion from any of the arguments to number(my_strntod)
  returns error.

*/
void Item_func_trunc::check_trunc_number_params() {
  null_value = false;
  for (uint i = 0; i < arg_count; i++) {
    if (args[i]->is_null()) {
      null_value = true;
      return;
    }
    if (args[i]->result_type() == STRING_RESULT) {
      int error;
      StringBuffer<STRING_BUFFER_USUAL_SIZE> tmp;
      const String *res = args[i]->val_str(&tmp);
      if (!res || !res->length()) {
        null_value = true;
        return;
      }
      // Try to convert the args from string to number.
      const CHARSET_INFO *cs = res->charset();
      const char *cptr = res->ptr();
      size_t length = res->length();
      const char *end = cptr + length;
      const char *endptr = cptr + length;
      my_strntod(cs, cptr, length, &endptr, &error);
      if (error || cptr == endptr ||
          (end != endptr && !check_if_only_end_space(cs, endptr, end))) {
        my_error(ER_WRONG_ARGUMENTS, MYF(0), func_name());
      }
    }
  }
}

double Item_func_trunc::real_op() {
  // use args[0]->val_str() to avoid potential data loss of values with
  // double/float types, Refer to the result of truncate, which use val_real()
  // to process double/float inputs e.g. > create table t1(c1 float, c2 int); >
  // insert  into t1 values (1.2346E+10, 123), (1.3, 2); > select truncate(c1,
  // c2) from t1;
  // +------------------+
  // | truncate(c1, c2) |
  // +------------------+
  // |      12346000384 |
  // |             1.29 |
  // +------------------+
  StringBuffer<STRING_BUFFER_USUAL_SIZE> tmp;
  const String *str_value = args[0]->val_str(&tmp);
  if (!str_value || !str_value->length()) {
    null_value = true;
    return 0.0;
  }
  const char *begin = str_value->ptr();
  const char *end = begin + str_value->length();
  longlong decimal_places = args[1]->val_int();
  my_decimal val1, val2;
  double res = 0.0;

  int rc = str2my_decimal(E_DEC_FATAL_ERROR, begin, &val1, &end);
  if (rc != E_DEC_OK) {
    const double value = args[0]->val_real();
    return my_double_round(value, decimal_places, args[1]->unsigned_flag, true);
  }

  my_decimal_round(E_DEC_FATAL_ERROR, &val1, decimal_places, true, &val2);
  my_decimal2double(E_DEC_FATAL_ERROR, &val2, &res);
  return res;
}

my_decimal *Item_func_trunc::decimal_op(my_decimal *decimal_value) {
  my_decimal val_res, val, *value = args[0]->val_decimal(&val);
  // reset decimals every time in case it is cached.
  decimals =
      args[0]->null_value ? DECIMAL_MAX_SCALE : decimal_actual_fraction(value);

  longlong dec = args[1]->val_int();
  if (dec >= 0 || args[1]->unsigned_flag)
    dec = min<ulonglong>(dec, decimals);
  else if (dec < INT_MIN)
    dec = INT_MIN;

  if (!(null_value = (args[0]->null_value || args[1]->null_value ||
                      my_decimal_round(E_DEC_FATAL_ERROR, value, (int)dec, true,
                                       decimal_value) > 1))) {
    uint8 new_decimals = decimal_actual_fraction(decimal_value);
    decimals =
        new_decimals < DECIMAL_MAX_SCALE ? new_decimals : DECIMAL_MAX_SCALE;
    return decimal_value;
  }

  return nullptr;
}
/**
  A simple wrapper for native_strncasecmp(), which adds length comparing between
  format string and pattern string.
  native_strcasecmp() used to be considered, but it is not applicable in the
  following case:

  create table t1 (date char(60), format varchar(10) not null);
  insert into t1 values
  ('2003-01-02 10:11:12', 'YEAR'),
  ('2003-01-02 02:11:12 AM', 'DAY');
  select trunc(date, format) as trunc_2 from t1;

  when dealing with 'DAY', the string buffer for format stays as 'DAYR',
  which leads to no match for any format mask and null value is returned from
  trunc().

  @param format trunc unit format
  @param pattern trunc unit type
  @retval         1 on error, 0 of success.
*/
int Item_func_trunc::strncasecmpwrap(const String *format,
                                     const char *pattern) {
  const char *ptr = format->ptr();
  size_t format_length = format->length();

  if (format_length != strlen(pattern)) return 1;
  return native_strncasecmp(ptr, pattern, format_length);
}

bool Item_func_trunc::get_trunc_unit_type(const String *format) {
  if (!format) return true;
  if (default_arg) {
    unit_type = TRUNC_DAY;
    return false;
  }

  if (strncasecmpwrap(format, "CC") == 0 ||
      strncasecmpwrap(format, "SCC") == 0) {
    unit_type = TRUNC_CENTURY;
    return false;
  } else if (strncasecmpwrap(format, "IYYY") == 0 ||
             strncasecmpwrap(format, "IYY") == 0 ||
             strncasecmpwrap(format, "IY") == 0 ||
             strncasecmpwrap(format, "I") == 0) {
    unit_type = TRUNC_ISO_YEAR;
    return false;
  } else if (strncasecmpwrap(format, "SYEAR") == 0 ||
             strncasecmpwrap(format, "SYYYY") == 0 ||
             strncasecmpwrap(format, "YYYY") == 0 ||
             strncasecmpwrap(format, "YYY") == 0 ||
             strncasecmpwrap(format, "YY") == 0 ||
             strncasecmpwrap(format, "Y") == 0 ||
             strncasecmpwrap(format, "YEAR") == 0) {
    unit_type = TRUNC_YEAR;
    return false;
  } else if (strncasecmpwrap(format, "Q") == 0) {
    unit_type = TRUNC_QUARTER;
    return false;
  } else if (strncasecmpwrap(format, "MONTH") == 0 ||
             strncasecmpwrap(format, "MON") == 0 ||
             strncasecmpwrap(format, "MM") == 0 ||
             strncasecmpwrap(format, "RM") == 0) {
    unit_type = TRUNC_MONTH;
    return false;
  } else if (strncasecmpwrap(format, "IW") == 0) {
    unit_type = TRUNC_ISO_WEEK;
    return false;
  } else if (strncasecmpwrap(format, "WW") == 0) {
    unit_type = TRUNC_WEEKDAY_YEAR;
    return false;
  } else if (strncasecmpwrap(format, "W") == 0) {
    unit_type = TRUNC_WEEKDAY_MONTH;
    return false;
  } else if (strncasecmpwrap(format, "DDD") == 0 ||
             strncasecmpwrap(format, "DD") == 0 ||
             strncasecmpwrap(format, "J") == 0) {
    unit_type = TRUNC_DAY;
    return false;
  } else if (strncasecmpwrap(format, "DAY") == 0 ||
             strncasecmpwrap(format, "DY") == 0 ||
             strncasecmpwrap(format, "D") == 0) {
    unit_type = TRUNC_DOW;
    return false;
  } else if (strncasecmpwrap(format, "HH") == 0 ||
             strncasecmpwrap(format, "HH12") == 0 ||
             strncasecmpwrap(format, "HH24") == 0) {
    unit_type = TRUNC_HOUR;
    return false;
  } else if (strncasecmpwrap(format, "MI") == 0) {
    unit_type = TRUNC_MINUTE;
    return false;
  }
  return true;
}

bool Item_func_trunc::get_date(MYSQL_TIME *ltime, my_time_flags_t fuzzy_date) {
  bool datetime_overflow = false;
  if ((null_value = args[0]->get_date(ltime, fuzzy_date))) return true;
  if (ltime->year == 0) {
    /* Only AD years are supported */
    push_warning_printf(
        current_thd, Sql_condition::SL_WARNING, ER_DATETIME_FUNCTION_OVERFLOW,
        ER_THD(current_thd, ER_DATETIME_FUNCTION_OVERFLOW), func_name());
    return (null_value = true);
  }

  char format_buff[64];
  int jd = date_to_julianday(ltime->year, ltime->month, ltime->day);
  int weekday = julianday_to_day_of_week(jd);
  uint weekday_iso = julianday_to_day_of_week(jd, 0);

  /**
   * For dates between 1582-10-05 and 1582-12-31, we should use
   * 1982-10-15 to calculate the weekday for the first day of the year.
   * And similarly, we should use 1982-10-15 to calculate the first day
   * of the month for dates between 1582-10-05 and 1582-10-31.
   */
  uint weekday_year_iso =
      (jd >= 2299161 && jd <= 2299238)
          ? julianday_to_day_of_week(date_to_julianday(1582, 10, 15), 0)
          : julianday_to_day_of_week(date_to_julianday(ltime->year, 1, 1), 0);

  uint weekday_month_iso =
      (jd >= 2299161 && jd <= 2299177)
          ? julianday_to_day_of_week(date_to_julianday(1582, 10, 15), 0)
          : julianday_to_day_of_week(
                date_to_julianday(ltime->year, ltime->month, 1), 0);

  int year_julian_day, mon_julian_day, day_julian_day;
  String format_str(format_buff, sizeof(format_buff), &my_charset_bin), *format;
  format = args[1]->val_str(&format_str);

  /* TODO: verify safety using append/chop
     It is much more elegant by using append/chop,
     then using native_strcasescmp() in function get_trunc_unit_type.
     While for now it is not sure if this implementation is safe, will
     do it at some point at the future:

     format->append("\0", 1);
     format->chop();
     ...
  */
  if (get_trunc_unit_type(format)) goto null_date;

  switch (unit_type) {
    case TRUNC_CENTURY:  // CC,SCC: First day of the centory.
      ltime->year = ((ltime->year + 99) / 100) * 100 - 99;
      ltime->month = 1;
      ltime->day = 1;
      ltime->hour = 0;
      ltime->minute = 0;
      ltime->second = 0;
      ltime->second_part = 0;
      break;
    case TRUNC_ISO_YEAR:  // IYYY,IYY,IY,I Year containing the calendar week, as
                          // defined by the ISO 8601 standard.
      // set ltime to Monday of the ISO week.
      jd = jd -
           (date_to_ISO_week(ltime->year, ltime->month, ltime->day) - 1) * 7 -
           weekday_iso;
      julianday_to_date(jd, year_julian_day, mon_julian_day, day_julian_day);
      if (year_julian_day < 1) {
        datetime_overflow = true;
        goto null_date;
      }

      ltime->year = year_julian_day;
      ltime->month = mon_julian_day;
      ltime->day = day_julian_day;
      ltime->hour = 0;
      ltime->minute = 0;
      ltime->second = 0;
      ltime->second_part = 0;
      break;
    case TRUNC_YEAR:  // SYYYY,YYYY,YEAR,SYEAR,YYY,YY,Y: Year.
      ltime->month = 1;
      ltime->day = 1;
      ltime->hour = 0;
      ltime->minute = 0;
      ltime->second = 0;
      ltime->second_part = 0;
      break;
    case TRUNC_QUARTER:  // Q: Quarter
      ltime->month = (3 * ((ltime->month - 1) / 3)) + 1;
      ltime->day = 1;
      ltime->hour = 0;
      ltime->minute = 0;
      ltime->second = 0;
      ltime->second_part = 0;
      break;
    case TRUNC_MONTH: /* MONTH,MON,MM,RM: Month */
      ltime->day = 1;
      ltime->hour = 0;
      ltime->minute = 0;
      ltime->second = 0;
      ltime->second_part = 0;
      break;
    case TRUNC_ISO_WEEK:  // IW: Same day of the week as the first day of the
                          // calendar week as defined by the ISO 8601 standard,
                          // which is Monday.
      jd = jd - weekday_iso;
      julianday_to_date(jd, year_julian_day, mon_julian_day, day_julian_day);
      if (year_julian_day < 1) {
        datetime_overflow = true;
        goto null_date;
      }

      ltime->year = year_julian_day;
      ltime->month = mon_julian_day;
      ltime->day = day_julian_day;
      ltime->hour = 0;
      ltime->minute = 0;
      ltime->second = 0;
      ltime->second_part = 0;
      break;
    case TRUNC_WEEKDAY_YEAR:  // WW: Same day of the week as the first day of
                              // the year.
      jd = jd - (weekday_iso >= weekday_year_iso
                     ? weekday_iso - weekday_year_iso
                     : 7 + weekday_iso - weekday_year_iso);
      julianday_to_date(jd, year_julian_day, mon_julian_day, day_julian_day);
      if (year_julian_day < 1) {
        datetime_overflow = true;
        goto null_date;
      }

      ltime->year = year_julian_day;
      ltime->month = mon_julian_day;
      ltime->day = day_julian_day;
      ltime->hour = 0;
      ltime->minute = 0;
      ltime->second = 0;
      ltime->second_part = 0;
      break;
    case TRUNC_WEEKDAY_MONTH:  // W: Same day of the week as the first day of
                               // the month.
      jd = jd - (weekday_iso >= weekday_month_iso
                     ? weekday_iso - weekday_month_iso
                     : 7 + weekday_iso - weekday_month_iso);
      julianday_to_date(jd, year_julian_day, mon_julian_day, day_julian_day);
      if (year_julian_day < 1) {
        datetime_overflow = true;
        goto null_date;
      }

      ltime->year = year_julian_day;
      ltime->month = mon_julian_day;
      ltime->day = day_julian_day;
      ltime->hour = 0;
      ltime->minute = 0;
      ltime->second = 0;
      ltime->second_part = 0;
      break;
    case TRUNC_DAY:  // DDD,DD,J: Day.
      ltime->hour = 0;
      ltime->minute = 0;
      ltime->second = 0;
      ltime->second_part = 0;
      break;
    case TRUNC_DOW:  // DAY,DY,D: Starting day of week.
      if (weekday > 0) {
        /* get the first day of week by using julianday_to_date(), to handle
        exceptions such as trunc('2003-01-02','day'), whose result is
        '2012-12-29'.
        */
        jd = jd - weekday;
        julianday_to_date(jd, year_julian_day, mon_julian_day, day_julian_day);
        if (year_julian_day < 1) {
          datetime_overflow = true;
          goto null_date;
        }

        ltime->year = year_julian_day;
        ltime->month = mon_julian_day;
        ltime->day = day_julian_day;
      }
      ltime->hour = 0;
      ltime->minute = 0;
      ltime->second = 0;
      ltime->second_part = 0;
      break;
    case TRUNC_HOUR:  // HH,HH12,HH24: Hour.
      ltime->minute = 0;
      ltime->second = 0;
      ltime->second_part = 0;
      break;
    case TRUNC_MINUTE:  // MI: Minute.
      ltime->second = 0;
      ltime->second_part = 0;
      break;
    default:
      goto null_date;
  }
  return false;

null_date:
  if (datetime_overflow) {
    push_warning_printf(
        current_thd, Sql_condition::SL_WARNING, ER_DATETIME_FUNCTION_OVERFLOW,
        ER_THD(current_thd, ER_DATETIME_FUNCTION_OVERFLOW), func_name());
  }
  return (null_value = true);
}

void Item_func_add_months::print(const THD *thd, String *str,
                                 enum_query_type query_type) const {
  str->append(func_name());
  str->append('(');
  args[0]->print(thd, str, query_type);
  str->append(',');
  args[1]->print(thd, str, query_type);
  str->append(')');
}

bool Item_func_add_months::resolve_type(THD *thd) {
  set_nullable(true);
  if (param_type_is_default(thd, 0, 1, MYSQL_TYPE_DATETIME)) return true;
  if (param_type_is_default(thd, 1, 2, MYSQL_TYPE_LONGLONG)) return true;
  // Always set the data type to datetime without decimal
  set_data_type_datetime(0);

  return false;
}

bool Item_func_add_months::get_month_interval(Item *intvl_arg,
                                              Interval *interval) {
  longlong month_interval = 0;
  switch (intvl_arg->result_type()) {
    /*  To avoid double/float/decimal values from being rounded,
     *  truncate such kinds of values and convert them into type longlong
     */
    case DECIMAL_RESULT: {
      my_decimal decimal_value, truncated, *val;
      val = intvl_arg->val_decimal(&decimal_value);
      if (intvl_arg->null_value) return true;
      decimal_round(val, &truncated, 0, TRUNCATE);
      val->check_result(E_DEC_FATAL_ERROR,
                        decimal2longlong(&truncated, &month_interval));
      break;
    }
    case REAL_RESULT: {
      month_interval = (longlong)intvl_arg->val_real();
      if (intvl_arg->null_value) return true;
      break;
    }
    case STRING_RESULT: {
      switch (intvl_arg->data_type()) {
        case MYSQL_TYPE_DATE:
        case MYSQL_TYPE_DATETIME:
        case MYSQL_TYPE_TIMESTAMP:
        case MYSQL_TYPE_TIME:
          goto invalid_argument;
        default:
          break;
      }

      String str_value, *val;
      val = intvl_arg->val_str(&str_value);
      if (intvl_arg->null_value) return true;
      val->ltrim();
      val->rtrim();
      if (val->length() == 0) return true;

      int error;
      my_decimal dec_buf, truncated;
      error = str2my_decimal(E_DEC_FATAL_ERROR, val->ptr(), val->length(),
                             val->charset(), &dec_buf);
      if (error != E_DEC_OK) goto invalid_argument;
      decimal_round(&dec_buf, &truncated, 0, TRUNCATE);
      dec_buf.check_result(E_DEC_FATAL_ERROR,
                           decimal2longlong(&truncated, &month_interval));
      break;
    }
    default:
      month_interval = intvl_arg->val_int();
      if (intvl_arg->null_value) return true;
  }

  if (month_interval == LLONG_MIN) {
    /* Warn about overflow */
    push_warning_printf(
        current_thd, Sql_condition::SL_WARNING, ER_DATETIME_FUNCTION_OVERFLOW,
        ER_THD(current_thd, ER_DATETIME_FUNCTION_OVERFLOW), func_name());
    return true;
  }

  if (month_interval < 0) {
    interval->neg = true;
    month_interval = -month_interval;
  }
  interval->month = (ulong)month_interval;
  return false;

invalid_argument:
  my_error(ER_WRONG_ARGUMENTS, MYF(0), func_name());
  return true;
}

/**
   add_months_internal.

@retval true if error
@retval false otherwise
*/
bool Item_func_add_months::add_months_internal(MYSQL_TIME *ltime,
                                               Interval interval,
                                               int *warnings) {
  ltime->neg = false;

  long long sign = (interval.neg ? -1 : 1);

  unsigned long long period;
  bool is_last_day =
      ltime->day == calc_days_in_month(ltime->year, ltime->month);
  // Simple guards against arithmetic overflow when calculating period.
  if (interval.month >= UINT_MAX / 2) goto invalid_date;
  if (interval.year >= UINT_MAX / 12) goto invalid_date;

  period = (ltime->year * 12ULL +
            sign * static_cast<unsigned long long>(interval.year) * 12ULL +
            ltime->month - 1ULL +
            sign * static_cast<unsigned long long>(interval.month));
  if (period >= 120000LL) goto invalid_date;
  ltime->year = period / 12;
  ltime->month = (period % 12L) + 1;
  if (is_last_day)
    ltime->day = calc_days_in_month(ltime->year, ltime->month);
  else if (ltime->day > days_in_month[ltime->month - 1]) {
    /* Adjust day if the new month doesn't have enough days */
    ltime->day = days_in_month[ltime->month - 1];
    if (ltime->month == 2 && calc_days_in_year(ltime->year) == 366)
      ltime->day++;  // Leap-year
  }

  return false;  // Ok

invalid_date:
  if (warnings) {
    *warnings |= MYSQL_TIME_WARN_DATETIME_OVERFLOW;
  }
  return true;
}

bool Item_func_add_months::val_datetime(MYSQL_TIME *ltime, my_time_flags_t) {
  if (args[0]->get_date(ltime, TIME_NO_ZERO_DATE)) return (null_value = true);
  date_to_datetime(ltime);

  Interval interval = {0, 0, 0, 0, 0, 0, 0, false};
  if (get_month_interval(args[1], &interval)) return (null_value = true);

  null_value = propagate_datetime_overflow(current_thd, [&](int *w) {
    return add_months_internal(ltime, interval, w);
  });
  if (null_value) return true;
  return false;
}
