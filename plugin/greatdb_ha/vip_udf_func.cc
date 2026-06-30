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

#include "vip_udf_func.h"
#include "mysql/components/service.h"
#include "mysql/components/services/dynamic_privilege.h"
#include "mysql/components/services/log_builtins.h"
#include "mysql/components/services/registry.h"
#include "mysql/components/services/udf_registration.h"
#include "sql/auth/auth_acls.h"
#include "sql/current_thd.h"
#include "sql/sql_class.h"
#include "sql/sql_udf.h"
#include "udf_utils.h"

namespace greatdb {
privilege_result user_has_gr_admin_privilege() {
  THD *thd = current_thd;
  privilege_result result = privilege_result::error();
  bool super_user = false;

  if (thd == nullptr) {
    /* purecov: begin inspected */
    goto end;
    /* purecov: end */
  }

  super_user = (thd->security_context() != nullptr &&
                thd->security_context()->master_access() & SUPER_ACL);
  if (super_user) {
    result = privilege_result::success();
  } else {
    SERVICE_TYPE(registry) *plugin_registry = mysql_plugin_registry_acquire();

    if (plugin_registry == nullptr) {
      /* purecov: begin inspected */
      goto end;
      /* purecov: end */
    }

    bool has_global_grant = false;
    {
      my_service<SERVICE_TYPE(global_grants_check)> service(
          "global_grants_check", plugin_registry);
      if (service.is_valid()) {
        has_global_grant = service->has_global_grant(
            reinterpret_cast<Security_context_handle>(thd->security_context()),
            STRING_WITH_LEN("REPLICATION_SLAVE_ADMIN"));
      } else {
        /* purecov: begin inspected */
        mysql_plugin_registry_release(plugin_registry);
        goto end;
        /* purecov: end */
      }
      /* service goes out of scope. It is destroyed and unregistered using
         plugin_registry. */
    }
    mysql_plugin_registry_release(plugin_registry);
    if (has_global_grant) {
      result = privilege_result::success();
    } else {
      result = privilege_result::no_privilege(
          thd->security_context()->priv_user().str,
          thd->security_context()->priv_host().str);
    }
  }
end:
  return result;
}

static bool udf_get_func_init(UDF_INIT *initid, UDF_ARGS *args, char *message) {
  if (args->arg_count != 2 || args->arg_type[0] != STRING_RESULT ||
      args->arg_type[1] != INT_RESULT) {
    std::snprintf(message, MYSQL_ERRMSG_SIZE, "Wrong argument count or type");
    return true;
  }
  privilege_result privilege = user_has_gr_admin_privilege();
  bool has_privileges = (privilege.status == privilege_status::ok);
  if (!has_privileges) {
    std::snprintf(message, MYSQL_ERRMSG_SIZE, "UDF not has_privileges");
    return true;
  }

  if (Charset_service::set_return_value_charset(initid) ||
      Charset_service::set_args_charset(args)) {
    std::snprintf(message, MYSQL_ERRMSG_SIZE,
                  "Unable to set character set service for UDF");
    return true;
  }

  initid->maybe_null = false;
  initid->ptr = nullptr;
  return false;
}

static bool udf_set_func_init(UDF_INIT *initid, UDF_ARGS *args, char *message) {
  if (args->arg_count != 3 || args->arg_type[0] != STRING_RESULT ||
      args->arg_type[1] != INT_RESULT || args->arg_type[2] != STRING_RESULT) {
    std::snprintf(message, MYSQL_ERRMSG_SIZE, "Wrong argument count or type");
    return true;
  }
  privilege_result privilege = user_has_gr_admin_privilege();
  bool has_privileges = (privilege.status == privilege_status::ok);
  if (!has_privileges) {
    std::snprintf(message, MYSQL_ERRMSG_SIZE, "UDF not has_privileges");
    return true;
  }

  if (Charset_service::set_return_value_charset(initid) ||
      Charset_service::set_args_charset(args)) {
    std::snprintf(message, MYSQL_ERRMSG_SIZE,
                  "Unable to set character set service for UDF");
    return true;
  }

  initid->maybe_null = false;
  initid->ptr = nullptr;
  return false;
}

static void udf_func_deinit(UDF_INIT *initid) {
  if (initid->ptr) delete[](initid->ptr);
}

std::array udfs = {
    udf_descriptor{HA_GET_BIND_VIPS_FUNC_NAME, Item_result::STRING_RESULT,
                   reinterpret_cast<Udf_func_any>(udf_get_bind_vips),
                   udf_get_func_init, udf_func_deinit},
    udf_descriptor{HA_SET_ALL_NODE_BIND_VIPS_FUNC_NAME,
                   Item_result::STRING_RESULT,
                   reinterpret_cast<Udf_func_any>(udf_set_all_node_bind_vips),
                   udf_set_func_init, udf_func_deinit}};

bool register_udfs() {
  SERVICE_TYPE(registry) *plugin_registry = mysql_plugin_registry_acquire();

  if (plugin_registry == nullptr) {
    /* purecov: begin inspected */
    my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                          " UDF_REGISTER_SERVICE ERROR ");
    return true;
  } else if (Charset_service::init(plugin_registry)) {
    my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                          " UDF INIT Charset_service ERROR ");
    return true;
  } else {
    bool error = false;
    /* We open a new scope so that udf_registrar is (automatically) destroyed
    before plugin_registry. */
    my_service<SERVICE_TYPE(udf_registration)> udf_registrar("udf_registration",
                                                             plugin_registry);
    if (udf_registrar.is_valid()) {
      for (udf_descriptor const &udf : udfs) {
        error = udf_registrar->udf_register(
            udf.name, udf.result_type, udf.main_function, udf.init_function,
            udf.deinit_function);
        if (error) {
          /* purecov: begin inspected */
          my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                                " UDF_REGISTER_SERVICE init failed:%s ",
                                udf.name);
          break;
          /* purecov: end */
        }
      }

      if (error) {
        /* purecov: begin inspected */
        int was_present;
        for (udf_descriptor const &udf : udfs) {
          // Don't care about errors since we are already erroring out.
          udf_registrar->udf_unregister(udf.name, &was_present);
        }
        /* purecov: end */
        return true;
      }
    } else {
      my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                            " UDF_REGISTER_SERVICE udf_registrar.is_valid");
      return true;
      /* purecov: end */
    }
  }
  return false;
}

bool unregister_udfs() {
  SERVICE_TYPE(registry) *plugin_registry = mysql_plugin_registry_acquire();
  if (plugin_registry == nullptr) {
    my_plugin_log_message(&plugin_ptr, MY_ERROR_LEVEL,
                          "plugin_registry not init UDF_UNREGISTER_ERROR");
  } else {
    /* We open a new scope so that udf_registrar is (automatically) destroyed
    before plugin_registry. */
    my_service<SERVICE_TYPE(udf_registration)> udf_registrar("udf_registration",
                                                             plugin_registry);
    bool error = false;
    if (udf_registrar.is_valid()) {
      int was_present;
      for (udf_descriptor const &udf : udfs) {
        // Don't care about the functions not being there.
        error = error || udf_registrar->udf_unregister(udf.name, &was_present);
      }
    } else {
      error = true;
    }
    if (error) {
      return true;
    }
  }
  Charset_service::deinit(plugin_registry);
  return false;
}

}  // namespace greatdb
