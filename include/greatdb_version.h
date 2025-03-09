/* Copyright (c) 2023, 2025, GreatDB Software Co., Ltd. All rights
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

#ifndef _greatdb_version_h
#define _greatdb_version_h

#define _IB_TO_STR(s) #s
#define IB_TO_STR(s) _IB_TO_STR(s)

/* datadict version of greatdb */
#define GREATDB_DD_VERSION_ID 8

#define GREATDB_INITIAL_VERSION_ID 80025001  // from Mysql 8.0.25
#define GREATDB_INITIAL_DD_VERSION_ID 1      // fixed, don't modify it

#define GREATDB_VERSION_STR                         \
  IB_TO_STR(MYSQL_VERSION_MAJOR)                    \
  "." IB_TO_STR(MYSQL_VERSION_MINOR) "." IB_TO_STR( \
      MYSQL_VERSION_PATCH) "." IB_TO_STR(GREATDB_DD_VERSION_ID)

#define GDB_MINOR_VER_MASK 100000000llu
#define GDB_PATCH_VER_MASK 1000000llu
#define GDB_DD_VER_MASK 10000llu

#define GREATDB_VERSION_ID                    \
  (MYSQL_VERSION_MAJOR * GDB_MINOR_VER_MASK + \
   MYSQL_VERSION_MINOR * GDB_PATCH_VER_MASK + \
   MYSQL_VERSION_PATCH * GDB_DD_VER_MASK + GREATDB_DD_VERSION_ID)

#endif  // _greatdb_version_h
