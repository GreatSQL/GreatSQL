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


#
# fix the #line directives in the generated .cc files
# to refer to the original sql_yacc.yy
#
m4_define([yyfile],m4_bpatsubst(__file__,[[a-z.0-9]+$],sql_yacc.yy))

m4_define([b4_syncline],
[m4_if(m4_index([$2],[.yy]),[-1],
[b4_sync_start([$1], [$2])[]dnl

],[b4_sync_start([$1], ["yyfile"])[]dnl

])])

# try both paths for different bison versions
m4_sinclude(skeletons/c-skel.m4)
m4_sinclude(c-skel.m4)

