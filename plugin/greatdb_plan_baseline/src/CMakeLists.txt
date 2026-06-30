# Copyright (c) 2026, GreatDB Software Co., Ltd.
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

IF(USING_LOCAL_BOOST)
  ADD_DEFINITIONS(
    -DBOOST_ALL_NO_LIB
    -DBOOST_IOSTREAMS_NO_LIB
    -DBOOST_SYSTEM_NO_DEPRECATED)
ENDIF()
INCLUDE_DIRECTORIES(
  ${BOOST_INCLUDE_DIR}
  ${RAPIDJSON_INCLUDE_DIR}
  ${CMAKE_SOURCE_DIR}/extra/unordered_dense/unordered_dense-4.4.0/include
)

ADD_DEFINITIONS(-DLOG_COMPONENT_TAG="PLAN_BASELINE")

FILE(GLOB PLAN_BASELINE_SOURCE_CC "*.cc" "*.cpp" "*.c"  "*.h")

SET(PLAN_BASELINE_SRCS ${PLAN_BASELINE_SOURCE_CC} )

ADD_DEFINITIONS(-DMYSQL_SERVER -DMYSQL_DYNAMIC_PLUGIN)

MYSQL_ADD_PLUGIN(libgreatdb_plan_baseline
${PLAN_BASELINE_SRCS}
MODULE_ONLY 
MODULE_OUTPUT_NAME "libgreatdb_plan_baseline"
)
# get plugin lib dir 
GET_TARGET_PROPERTY(libdir libgreatdb_plan_baseline LIBRARY_OUTPUT_DIRECTORY)
