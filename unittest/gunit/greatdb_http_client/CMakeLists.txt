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

IF(LINUX)

set(CMAKE_CXX_STANDARD 17)
IF(USING_LOCAL_BOOST)
  ADD_DEFINITIONS(
    -DBOOST_ALL_NO_LIB
    -DBOOST_IOSTREAMS_NO_LIB
    -DBOOST_SYSTEM_NO_DEPRECATED)
ENDIF()
INCLUDE_DIRECTORIES(SYSTEM ${BOOST_PATCHES_DIR} ${BOOST_INCLUDE_DIR})

INCLUDE_DIRECTORIES(${CMAKE_CURRENT_BINARY_DIR})
INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/plugin/greatdb_http_client)
FILE(GLOB GREATDB_HTTP_CLIENT_SOURCE
  "${CMAKE_SOURCE_DIR}/plugin/greatdb_http_client/*.cc"
)
ADD_DEFINITIONS(-DLOG_COMPONENT_TAG="greatdb_http_client_tests")
ADD_DEFINITIONS(-DMYSQL_SERVER -DEXTRA_CODE_FOR_UNIT_TESTING)

MYSQL_ADD_EXECUTABLE(greatdb_http_client-t
    greatdb_http_client-t.cc
    ${GREATDB_HTTP_CLIENT_SOURCE}
    ADD_TEST greatdb_http_client
)

SET_TARGET_PROPERTIES(greatdb_http_client-t PROPERTIES ENABLE_EXPORTS TRUE)
TARGET_LINK_LIBRARIES(greatdb_http_client-t
gunit_large 
server_unittest_library
)
ENDIF ()
