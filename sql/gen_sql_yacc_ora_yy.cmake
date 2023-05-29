# Copyright (c) 2023, GreatDB Software Co., Ltd.
# 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License, version 2.0,
# as published by the Free Software Foundation.
#
# This program is also distributed with certain software (including
# but not limited to OpenSSL) that is licensed under separate terms,
# as designated in a particular file or component or in included license
# documentation.  The authors of MySQL hereby grant you an additional
# permission to link the program and your derivative works with the
# separately licensed software that they have included with MySQL.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License, version 2.0, for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

if(POLICY CMP0054)
  cmake_policy(SET CMP0054 NEW)
endif()

file(READ "${IN}" data)
# oracle mode  yy file
file(WRITE "${ORA_OUT}" "")
# default yy file
file(WRITE "${OUT}" "")

# string(REPLACE  "/* Start SQL_MODE_DEFAULT_SPECIFIC */"
#                 "/* Start SQL_MODE_DEFAULT_SPECIFIC"      data "${data}")
# string(REPLACE  "/* End SQL_MODE_DEFAULT_SPECIFIC */"
#                    "End SQL_MODE_DEFAULT_SPECIFIC */"     data "${data}")
#
# string(REPLACE  "/* Start SQL_MODE_ORACLE_SPECIFIC"
#                 "/* Start SQL_MODE_ORACLE_SPECIFIC */"    data "${data}")
#
# string(REPLACE     "End SQL_MODE_ORACLE_SPECIFIC */"
#                 "/* End SQL_MODE_ORACLE_SPECIFIC */"      data "${data}")
#

# 1  only write oracle yy file
# 2  only write default yy file
# 0 both write
set(where 0)

set(MODE_NAME "SQL_MODE_ORACLE")

string(REGEX REPLACE "/\\* sql_yacc\\.yy \\*/" "/* DON'T EDIT THIS FILE. IT'S GENERATED.  EDIT sql_yacc.yy INSTEAD */" data "${data}")

# GENERATED yy file

while(NOT data STREQUAL "")
  string(REGEX MATCH "^(%[ie][^\n]*\n)|((%[^ie\n]|[^%\n])[^\n]*\n)+|\n+" line "${data}")
  string(LENGTH "${line}" ll)
  string(SUBSTRING "${data}" ${ll} -1 data)

  if (line MATCHES "^%ifdef +${MODE_NAME} *\n")
      set(where 1)
      set(line "\n")
  elseif (line MATCHES "^%ifndef +${MODE_NAME} *\n")
      set(where 2)
      set(line "\n")
  elseif(line MATCHES "^%else( *| +.*)\n" AND where GREATER 0)
      math(EXPR where "3-${where}")
      set(line "\n")
  elseif(line MATCHES "^%endif( *| +.*)\n")
      set(where 0)
      set(line "\n")
  endif()

  if(where STREQUAL 1)
    file(APPEND "${ORA_OUT}" "${line}")
    string(REGEX REPLACE "[^\n]+" "" line "${line}")
    file(APPEND "${OUT}" "${line}")
  elseif(where STREQUAL 2)
    file(APPEND "${OUT}" "${line}")
    string(REGEX REPLACE "[^\n]+" "" line "${line}")
    file(APPEND "${ORA_OUT}" "${line}")
  else()
    file(APPEND "${ORA_OUT}" "${line}")
    file(APPEND "${OUT}" "${line}")
  endif()
endwhile()
