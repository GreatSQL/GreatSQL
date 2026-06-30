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

#include "mysql/components/services/udf_registration.h"
#include <mysqld_error.h>
#include <array>
#include "mysql/components/my_service.h"
#include "mysql/components/services/registry.h"
#include "mysql/service_plugin_registry.h"

#include "mysql/components/services/log_builtins.h"

#include "plan_baseline.h"
#include "plan_baseline_cmd_service.h"
#include "udf_descriptor.h"
#include "udf_registration.h"
#include "workMgr.h"

namespace greatdb_plan_baseline {

std::array udfs = {gdb_create_plan_baseline_tables()};

bool register_udfs() {
  if (reg_srv == nullptr) {
    /* purecov: begin inspected */
    LogPluginErrMsg(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
                    " UDF_REGISTER_SERVICE_ERROR ");
    return true;
  } else {
    bool error = false;
    /* We open a new scope so that udf_registrar is (automatically) destroyed
       before plugin_registry. */
    my_service<SERVICE_TYPE(udf_registration)> udf_registrar("udf_registration",
                                                             reg_srv);
    if (udf_registrar.is_valid()) {
      for (udf_descriptor const &udf : udfs) {
        error = udf_registrar->udf_register(
            udf.name, udf.result_type, udf.main_function, udf.init_function,
            udf.deinit_function);
        if (error) {
          /* purecov: begin inspected */
          LogPluginErrMsg(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
                          " UDF_REGISTER_SERVICE init failed:%s ", udf.name);
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
      LogPluginErrMsg(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
                      " UDF_REGISTER_SERVICE udf_registrar.is_valid");
      return true;
      /* purecov: end */
    }
  }
  return false;
}

bool unregister_udfs() {
  if (reg_srv == nullptr) {
    LogPluginErrMsg(ERROR_LEVEL, ER_LOG_PRINTF_MSG,
                    "reg_srv not init UDF_UNREGISTER_ERROR");
    return true;
    /* purecov: end */
  } else {
    /* We open a new scope so that udf_registrar is (automatically) destroyed
       before plugin_registry. */
    my_service<SERVICE_TYPE(udf_registration)> udf_registrar("udf_registration",
                                                             reg_srv);
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
  return false;
}

}  // namespace greatdb_plan_baseline
