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

#include "udf_utils.h"

namespace greatdb {
const char *Charset_service::arg_type("charset");
const char *Charset_service::service_name("mysql_udf_metadata");

SERVICE_TYPE(mysql_udf_metadata) *Charset_service::udf_metadata_service =
    nullptr;

bool Charset_service::init(SERVICE_TYPE(registry) * reg_srv) {
  my_h_service h_udf_metadata_service;
  if (!reg_srv || reg_srv->acquire(service_name, &h_udf_metadata_service))
    return true;
  udf_metadata_service = reinterpret_cast<SERVICE_TYPE(mysql_udf_metadata) *>(
      h_udf_metadata_service);
  return false;
}

bool Charset_service::deinit(SERVICE_TYPE(registry) * reg_srv) {
  if (!reg_srv) return true;
  using udf_metadata_t = SERVICE_TYPE_NO_CONST(mysql_udf_metadata);
  if (udf_metadata_service)
    reg_srv->release(reinterpret_cast<my_h_service>(
        const_cast<udf_metadata_t *>(udf_metadata_service)));
  return false;
}

/* Set the return value character set as latin1 */
bool Charset_service::set_return_value_charset(
    UDF_INIT *initid, const std::string &charset_name) {
  char *charset = const_cast<char *>(charset_name.c_str());
  if (udf_metadata_service->result_set(initid, Charset_service::arg_type,
                                       static_cast<void *>(charset))) {
    return true;
  }
  return false;
}

bool Charset_service::set_args_charset(UDF_ARGS *args,
                                       const std::string &charset_name) {
  char *charset = const_cast<char *>(charset_name.c_str());
  for (uint index = 0; index < args->arg_count; ++index) {
    if (args->arg_type[index] == STRING_RESULT &&
        udf_metadata_service->argument_set(args, Charset_service::arg_type,
                                           index,
                                           static_cast<void *>(charset))) {
      return true;
    }
  }
  return false;
}

bool Charset_service::set_args_charset(UDF_ARGS *args, uint index,
                                       const std::string &charset_name) {
  char *charset = const_cast<char *>(charset_name.c_str());
  if (udf_metadata_service->argument_set(args, Charset_service::arg_type, index,
                                         static_cast<void *>(charset))) {
    return true;
  }

  return false;
}

}  // namespace greatdb
