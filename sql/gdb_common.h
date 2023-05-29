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

#ifndef _gdb_common_h
#define _gdb_common_h

#include "errmsg.h"
#include "my_dbug.h"
#include "my_dir.h"
#include "my_inttypes.h"
#include "mysql.h"
#include "mysql/psi/mysql_file.h"
#include "mysqld_error.h"
#include "sql/field.h"

#define IDENT_QUOTE_CHAR '`'   // Character for quoting
#define VALUE_QUOTE_CHAR '\''  // Character for quoting

/* NOTE: to reduce the num of calls to "String::append", some codes doesn't use
 * this const variable */

int get_timezone_offset_seconds(Time_zone *tz);
std::string get_timezone_offset_str(int diff_secs);

/* NOTE: this function will append input string  to an executable string
         as a value-string. For example, append "a'bc" behind "insert into
         t1(c2) values (", and get "insert into t1(c2) values ('a''bc'" when
         quote_char == IDENT_QUOTE_CHAR. And, append "t`1" behind "select *
         from", and get "select * from `t``1`".

         Keep in mind, this function only used for append executable string
         that will send to backend by send_query() */
bool append_ident(String *string, const char *name, size_t length,
                  const char quote_char = IDENT_QUOTE_CHAR);

/* auxiliary func, internally call append_ident()
   NOTE: use "system_charset_info" for "IDENT_QUOTE_CHAR"
         use "default_charset_info" for "VALUE_QUOTE_CHAR" */
std::string gdb_string_add_quote(const std::string &input,
                                 const char type = IDENT_QUOTE_CHAR);

#endif  // _gdb_common_h
