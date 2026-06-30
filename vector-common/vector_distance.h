/* Copyright (c) 2026, GreatDB Software Co., Ltd.

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

#pragma once

#include <cmath>
#include <cstddef>

namespace vector_distance {

static inline double euclidean_distance(const float *v1, const float *v2,
                                        size_t size) {
  double dis = 0;
  for (size_t i = 0; i < size; i++, v1++, v2++) {
    double dist = *v1 - *v2;
    dis += dist * dist;
  }
  return sqrt(dis);
}

static inline double cosine_distance(const float *v1, const float *v2,
                                     size_t size) {
  double dotp = 0;
  double abs1 = 0;
  double abs2 = 0;
  for (size_t i = 0; i < size; i++, v1++, v2++) {
    float f1 = *v1;
    float f2 = *v2;
    abs1 += f1 * f1;
    abs2 += f2 * f2;
    dotp += f1 * f2;
  }
  return 1 - dotp / sqrt(abs1 * abs2);
}

static inline double dot_product(const float *v1, const float *v2,
                                 size_t size) {
  double dotp = 0;
  for (size_t i = 0; i < size; i++, v1++, v2++) {
    float f1 = *v1;
    float f2 = *v2;
    dotp += f1 * f2;
  }
  return dotp;
}

}  // namespace vector_distance
