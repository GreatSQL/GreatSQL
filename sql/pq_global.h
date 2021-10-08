#ifndef MYSQL_PQ_SQL_GLOBAL_H
#define MYSQL_PQ_SQL_GLOBAL_H

/* Copyright (c) 2020, Oracle and/or its affiliates. All rights reserved.
   Copyright (c) 2021, Huawei Technologies Co., Ltd.
   Copyright (c) 2021, GreatDB Software Co., Ltd.

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

#include <iostream>
#include <memory>
#include "my_alloc.h"
#include "my_compiler.h"

#define TIME_THOUSAND 1000
#define TIME_MILLION 1000000
#define TIME_BILLION 1000000000

const int PQ_MEMORY_USED_BUCKET = 16;

template <typename T>
T atomic_add(T &value, T n) {
  return __sync_fetch_and_add(&value, n);
}

template <typename T>
T atomic_sub(T &value, T n) {
  return __sync_fetch_and_sub(&value, n);
}

#endif  // MYSQL_PQ_SQL_GLOBAL_H
