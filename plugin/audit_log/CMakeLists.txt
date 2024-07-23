# Copyright (c) 2014 Percona LLC and/or its affiliates. All rights reserved.
# Copyright (c) 2024, GreatDB Software Co., Ltd. 
# 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 2 of the License.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA

MYSQL_ADD_PLUGIN(audit_log audit_log.cc file_logger.cc buffer.cc audit_file.cc
 audit_syslog.cc filter.cc
 MODULE_ONLY MODULE_OUTPUT_NAME "audit_log")

MYSQL_ADD_PLUGIN(audit_login_messages audit_login_msg.cc
 MODULE_ONLY MODULE_OUTPUT_NAME "audit_login_messages")

INCLUDE_DIRECTORIES(SYSTEM ${BOOST_PATCHES_DIR} ${BOOST_INCLUDE_DIR})

IF(UNIX)
  IF(INSTALL_MYSQLTESTDIR)
    INSTALL(DIRECTORY tests/mtr/ DESTINATION ${INSTALL_MYSQLTESTDIR}/suite/audit_log COMPONENT Test)
  ENDIF()
ENDIF()

FOREACH(file
    install_audit_log
    uninstall_audit_log)

  CONFIGURE_FILE(
    ${CMAKE_CURRENT_SOURCE_DIR}/${file}.sql.in
    ${CMAKE_CURRENT_BINARY_DIR}/${file}.sql
    )

  INSTALL(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/${file}.sql
    DESTINATION ${INSTALL_MYSQLSHAREDIR}
    COMPONENT SupportFiles
    PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
    )

ENDFOREACH()
