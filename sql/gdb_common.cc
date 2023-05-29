/* Copyright (c) 2023, GreatDB Software Co., Ltd. All rights
   reserved.

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

#include "gdb_common.h"
#include "tztime.h"

/* calculate offset seconds between @tz to UTC. '+08:00 UTC' returns 28800 */
int get_timezone_offset_seconds(Time_zone *tz) {
  assert(tz != nullptr);

  MYSQL_TIME my_tm;
  my_time_t sec_cur_tz = 86400 * 10;  // just a tmp value, used for calc offset
  tz->gmt_sec_to_TIME(&my_tm, sec_cur_tz);
  auto sec_utc = my_tz_UTC->TIME_to_gmt_sec(&my_tm, nullptr);
  return sec_utc - sec_cur_tz;
}

/* calculate '[+/-]{HH:MM}' of Time_zone_offset, according to @offset_secs */
std::string get_timezone_offset_str(int offset_secs) {
  /* the range of Time_zone_offset is from '-13:59' to '+14:00' */
  assert(offset_secs > -3600 * 14 && offset_secs <= 3600 * 14);
  assert(offset_secs % 60 == 0);

  char buffer[8] = {};
  char *ptr = buffer;

  auto abs_secs = offset_secs;
  *ptr = '+';
  if (offset_secs < 0) {
    *ptr = '-';
    abs_secs = -offset_secs;
  }
  ptr++;
  sprintf(ptr, "%02d", abs_secs / 3600);
  ptr += 2;
  *ptr++ = ':';
  sprintf(ptr, "%02d", abs_secs % 3600 / 60);

  return std::string(buffer);
}

bool append_ident(String *string, const char *name, size_t length,
                  const char quote_char) {
  bool result = true;
  DBUG_ENTER("append_ident");
  assert(quote_char == IDENT_QUOTE_CHAR || quote_char == VALUE_QUOTE_CHAR);

  result = string->reserve(length * 2 + 2);

  if (result || (result = string->append(&quote_char, 1, system_charset_info)))
    goto err;

  if (quote_char == VALUE_QUOTE_CHAR) {
    String tmpstr(name, length, system_charset_info);
    tmpstr.print(string);
  } else {
    uint clen = 0;
    for (const char *name_end = name + length; name < name_end; name += clen) {
      uchar c = static_cast<uchar>(*name);
      if (!(clen = my_mbcharlen(system_charset_info, c))) goto err;

      if (clen == 1 && c == quote_char &&
          (result = string->append(&quote_char, 1, system_charset_info)))
        goto err;

      if ((result = string->append(name, clen, system_charset_info))) goto err;
    }
  }

  result = string->append(&quote_char, 1, system_charset_info);

err:
  DBUG_RETURN(result);
}

std::string gdb_string_add_quote(const std::string &input, const char type) {
  assert(type == IDENT_QUOTE_CHAR || type == VALUE_QUOTE_CHAR);

  auto charset = system_charset_info;
  if (type == VALUE_QUOTE_CHAR) charset = default_charset_info;

  char buff[FN_REFLEN];
  String tmp_str(buff, sizeof(buff), charset);
  tmp_str.length(0);
  if (append_ident(&tmp_str, input.c_str(), input.size(), type))
    return std::string("");
  return std::string(tmp_str.ptr(), tmp_str.length());
}
