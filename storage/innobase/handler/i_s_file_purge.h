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
Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#ifndef i_s_file_h
#define i_s_file_h

#include <sys/types.h>
#include <time.h>

#include "sql/table.h"

#include "univ.i"

class Field;
class THD;
struct TABLE_LIST;
class Item;

extern struct st_mysql_plugin i_s_innodb_data_file_async_purge;

/* Defined with in 'handler/i_s.cc' */
extern int field_store_string(Field *field, const char *str);

/* Defined with in 'handler/i_s.cc' */
extern int field_store_time_t(Field *field, time_t time);

/** I_S.innodb_* views version postfix. Every time the define of any InnoDB I_S
table is changed, this value has to be increased accordingly */
constexpr uint8_t i_s_innodb_plugin_version_postfix = 3;

#endif  // i_s_file_h
