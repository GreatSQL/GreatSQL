/* Copyright (c) 2026, GreatDB Software Co., Ltd.

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

#ifndef PLUGIN_GDB_HA_UDF_H
#define PLUGIN_GDB_HA_UDF_H

#include "mysql/plugin.h"
#include "mysql/udf_registration_types.h"

namespace greatdb {
#define HA_GET_BIND_VIPS_FUNC_NAME "HA_GET_BIND_VIPS"
#define HA_SET_ALL_NODE_BIND_VIPS_FUNC_NAME "HA_SET_ALL_NODE_BIND_VIPS"
#define HA_REPLY_OK '0'
#define HA_REPLY_YOU_ARE_NOT_PRIMARY '1'

extern MYSQL_PLUGIN plugin_ptr;

struct udf_descriptor {
  char const *name;
  enum Item_result result_type;
  Udf_func_any main_function;
  Udf_func_init init_function;
  Udf_func_deinit deinit_function;

  udf_descriptor(char const *udf_name, enum Item_result udf_result_type,
                 Udf_func_any udf_function, Udf_func_init udf_init,
                 Udf_func_deinit udf_deinit)
      : name(udf_name),
        result_type(udf_result_type),
        main_function(udf_function),
        init_function(udf_init),
        deinit_function(udf_deinit) {}
  udf_descriptor(udf_descriptor const &) = delete;
  udf_descriptor(udf_descriptor &&other) = default;
  udf_descriptor &operator=(udf_descriptor const &) = delete;
  udf_descriptor &operator=(udf_descriptor &&other) = default;
};

char *udf_get_bind_vips(UDF_INIT *initid [[maybe_unused]], UDF_ARGS *args,
                        char *result, unsigned long *length, char *is_null,
                        char *error);

char *udf_set_all_node_bind_vips(UDF_INIT *initid [[maybe_unused]],
                                 UDF_ARGS *args, char *result,
                                 unsigned long *length, char *is_null,
                                 char *error);

bool register_udfs();
bool unregister_udfs();

enum class privilege_status { ok, no_privilege, error };
class privilege_result {
 public:
  privilege_status status;
  char const *get_user() const {
    assert(status == privilege_status::no_privilege &&
           "get_user() can only be called if status == no_privilege");
    return user;
  }
  char const *get_host() const {
    assert(status == privilege_status::no_privilege &&
           "get_host() can only be called if status == no_privilege");
    return host;
  }
  static privilege_result success() {
    return privilege_result(privilege_status::ok);
  }
  static privilege_result error() {
    return privilege_result(privilege_status::error);
  }
  static privilege_result no_privilege(char const *user, char const *host) {
    return privilege_result(user, host);
  }

 private:
  char const *user;
  char const *host;
  privilege_result(privilege_status status)
      : status(status), user(nullptr), host(nullptr) {
    assert(status != privilege_status::no_privilege &&
           "privilege_result(status) can only be called if status != "
           "no_privilege");
  }
  privilege_result(char const *user, char const *host)
      : status(privilege_status::no_privilege), user(user), host(host) {}
};
}  // namespace greatdb

#endif
