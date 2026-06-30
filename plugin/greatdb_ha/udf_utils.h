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

#ifndef PLUGIN_GDB_HA_UDF_UTILES_H
#define PLUGIN_GDB_HA_UDF_UTILES_H

#include <mysql/components/services/registry.h>
#include <mysql/components/services/udf_metadata.h>
#include <mysql/udf_registration_types.h>
#include <cassert>
#include <iostream>

namespace greatdb {
/**
 @class Charset_service

 Class that acquire/release the udf_metadata_service from registry service.
 It provides the APIs to set the character set of return value and arguments
 of UDFs using the udf_metadata service.
*/
class Charset_service {
 public:
  /**
    Acquires the udf_metadata_service from the registry  service.
    @param[in]  reg_srv Registry service from which udf_metadata service
                        will be acquired

    @retval true if service could not be acquired
    @retval false Otherwise
  */
  static bool init(SERVICE_TYPE(registry) * reg_srv);

  /**
    Release the udf_metadata service

    @param[in]  reg_srv Registry service from which the udf_metadata
                        service will be released.

    @retval true if service could not be released
    @retval false Otherwise
  */
  static bool deinit(SERVICE_TYPE(registry) * reg_srv);

  /**
    Set the specified character set of UDF return value

    @param[in] initid  UDF_INIT structure
    @param[in] charset_name Character set that has to be set.
               The default charset is set to 'latin1'

    @retval true Could not set the character set of return value
    @retval false Otherwise
  */
  static bool set_return_value_charset(
      UDF_INIT *initid, const std::string &charset_name = "latin1");
  /**
    Set the specified character set of all UDF arguments

    @param[in] args UDF_ARGS structure
    @param[in] charset_name Character set that has to be set.
               The default charset is set to 'latin1'

    @retval true Could not set the character set of any of the argument
    @retval false Otherwise
  */
  static bool set_args_charset(UDF_ARGS *args,
                               const std::string &charset_name = "latin1");

  static bool set_args_charset(UDF_ARGS *args, uint index,
                               const std::string &charset_name = "latin1");

 private:
  /* Argument type to specify in the metadata service methods */
  static const char *arg_type;
  /* udf_metadata service name */
  static const char *service_name;
  /* Handle of udf_metadata_service */
  static SERVICE_TYPE(mysql_udf_metadata) * udf_metadata_service;
};

}  // namespace greatdb
#endif
