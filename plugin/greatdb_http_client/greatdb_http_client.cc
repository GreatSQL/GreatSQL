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

#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>
#include "mysql/components/service.h"
#include "mysql/components/services/log_builtins.h"
#include "mysql/components/services/registry.h"
#include "mysql/components/services/udf_registration.h"
#include "mysql/plugin.h"
#include "mysql/udf_registration_types.h"
#include "mysqld_error.h"
#include "sql/sql_udf.h"

namespace beast = boost::beast;
namespace http = beast::http;
namespace net = boost::asio;
using tcp = net::ip::tcp;

SERVICE_TYPE(registry) *reg_srv = nullptr;
SERVICE_TYPE(log_builtins) *log_bi = nullptr;
SERVICE_TYPE(log_builtins_string) *log_bs = nullptr;

namespace greatdb_http_client {
/**
  Plugin type-specific descriptor
*/
static struct st_mysql_daemon descriptor = {
    0x0001  // interface version
};

ulong max_response_size = 4096L;

/**
  Descriptor of a system variable @@sql_mode2 (the should not conflict with
  existent system variable names).
*/
static MYSQL_SYSVAR_ULONG(
    max_response_size,    // name part of the externally visible variable name
    max_response_size,    // associated value
    PLUGIN_VAR_RQCMDARG,  // flags
    "Limit the length of the function's returned data.", nullptr, nullptr,
    4096L,           // default value
    64L,             // minimal allowed value
    MAX_BLOB_WIDTH,  // maximal allowed value
    0);

static SYS_VAR *system_variables[] = {
    MYSQL_SYSVAR(max_response_size),
    nullptr  // end of array
};

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

bool http_get_init(UDF_INIT *initid, UDF_ARGS *args, char *message) {
  assert(initid && initid->extension && args && args->extension && message);

  if (args->arg_count != 1) {
    snprintf(message, MYSQL_ERRMSG_SIZE,
             "incorrect number of arguments; expected %u, got %u", 1,
             args->arg_count);
    return 1;
  }

  auto *args_extension = pointer_cast<Udf_args_extension *>(args->extension);
  auto *return_value_extension =
      pointer_cast<Udf_return_value_extension *>(initid->extension);
  return_value_extension->charset_info = args_extension->charset_info[0];
  return 0;
}

static bool parse_url(const std::string_view &url, std::string &host,
                      std::string &port, std::string &target) {
  size_t protocol_end = url.find("://");
  if (protocol_end == std::string::npos) {
    return false;
  }

  size_t host_start = protocol_end + 3;
  size_t host_end = url.find_first_of(":/", host_start);

  if (host_end == std::string::npos) {
    host = url.substr(host_start);
    port = "80";
    target = "/";
    return true;
  }

  host = url.substr(host_start, host_end - host_start);
  if (url[host_end] == ':') {
    size_t port_start = host_end + 1;
    size_t port_end = url.find('/', port_start);

    if (port_end == std::string::npos) {
      port = url.substr(port_start);
      target = "/";
    } else {
      port = url.substr(port_start, port_end - port_start);
      target = url.substr(port_end);
    }
  } else {
    port = "80";
    target = url.substr(host_end);
  }

  if (target.empty() || target[0] != '/') {
    target = "/" + target;
  }

  return true;
}

class response_too_large_error : public std::exception {
 public:
  enum exceed_type { EXCEED_BUFFER, EXCEED_LIMIT };
  response_too_large_error(std::size_t actual, std::size_t limit,
                           exceed_type et)
      : actual_size_(actual), limit_size_(limit), et_(et) {}

  const char *what() const noexcept override {
    switch (et_) {
      case EXCEED_BUFFER:
        return "Response body buffer size limit";
      case EXCEED_LIMIT:
        return "Response body exceeds size limit";
      default:
        assert(0);
        break;
    }
  }

  std::size_t actual_size() const { return actual_size_; }
  std::size_t limit_size() const { return limit_size_; }

 private:
  std::size_t actual_size_;
  std::size_t limit_size_;
  exceed_type et_;
};

char *http_get(UDF_INIT *initid [[maybe_unused]], UDF_ARGS *args, char *result,
               unsigned long *length, char *is_null, char *error) {
  assert(args && result && length && is_null && error);
  *is_null = 1;
  *error = 1;
  if (!args->args[0]) {
    my_error(ER_GDB_HTTP_CLIENT_FAILED, MYF(0), "missing URL");
  }
  std::string_view http_url(args->args[0], *args->lengths);
  std::string host, port, target;
  if (!parse_url(http_url, host, port, target)) {
    my_error(ER_GDB_HTTP_CLIENT_FAILED, MYF(0), "URL using bad/illegal format");
    return nullptr;
  }

  char *res = nullptr;
  try {
    net::io_context ioc;
    tcp::resolver resolver(ioc);
    beast::tcp_stream stream(ioc);
    auto const results = resolver.resolve(host, port);
    stream.connect(results);
    http::request<http::string_body> req{http::verb::get, target, 11};
    req.set(http::field::host, host);
    http::write(stream, req);
    beast::flat_buffer buffer;

    http::response_parser<http::string_body> parser;
    parser.body_limit(max_response_size);
    // Check if the Content-Length header exceeds the response size limit.
    http::read_header(stream, buffer, parser);
    if (parser.get().has_content_length()) {
      auto content_length = parser.get().at(http::field::content_length);
      std::size_t length_value = 0;

      try {
        length_value = std::stoull(content_length.to_string());
      } catch (const std::exception &e) {
        throw std::runtime_error("Invalid Content-Length value: " +
                                 std::string(content_length));
      }
      if (length_value > max_response_size) {
        throw response_too_large_error(length_value, max_response_size,
                                       response_too_large_error::EXCEED_LIMIT);
      }
    }

    // Read the complete response data and recheck whether it exceeds the
    // response size limit.
    http::read(stream, buffer, parser);
    http::response<http::string_body> http_response = parser.release();
    auto body = http_response.body();
    if (body.size() > max_response_size) {
      throw response_too_large_error(body.size(), max_response_size,
                                     response_too_large_error::EXCEED_LIMIT);
    } else if (body.size() + 1 > *length) {
      throw response_too_large_error(body.size(), max_response_size,
                                     response_too_large_error::EXCEED_BUFFER);
    }

    beast::error_code ec;
    stream.socket().shutdown(tcp::socket::shutdown_both, ec);
    if (ec) {
      throw beast::system_error{ec};
    }

    *length = body.size();
    strncpy(result, body.c_str(), *length);
    result[*length] = '\0';

    *is_null = 0;
    *error = 0;
    res = result;
  } catch (const std::exception &e) {
    my_error(ER_GDB_HTTP_CLIENT_FAILED, MYF(0), e.what());
  }
  return res;
}

std::array udfs = {udf_descriptor{"http_get", Item_result::STRING_RESULT,
                                  reinterpret_cast<Udf_func_any>(http_get),
                                  http_get_init, nullptr}};

bool register_udfs() {
  if (reg_srv == nullptr) {
    /* purecov: begin inspected */
    LogPluginErrMsg(ERROR_LEVEL, ER_INIT_GREATDB_HTTP_CLIENT_PLUGIN_FAILED,
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
          LogPluginErrMsg(ERROR_LEVEL,
                          ER_INIT_GREATDB_HTTP_CLIENT_PLUGIN_FAILED,
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
      LogPluginErrMsg(ERROR_LEVEL, ER_INIT_GREATDB_HTTP_CLIENT_PLUGIN_FAILED,
                      " UDF_REGISTER_SERVICE udf_registrar.is_valid");
      return true;
      /* purecov: end */
    }
  }
  return false;
}

bool unregister_udfs() {
  if (reg_srv == nullptr) {
    LogPluginErrMsg(WARNING_LEVEL, ER_INIT_GREATDB_HTTP_CLIENT_PLUGIN_FAILED,
                    "reg_srv not init UDF_UNREGISTER_ERROR");
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

static int plugin_init(void *arg [[maybe_unused]]) {
  if (init_logging_service_for_plugin(&reg_srv, &log_bi, &log_bs) ||
      register_udfs()) {
    return 1;
  }
  return 0;
}

static int plugin_deinit(void *arg [[maybe_unused]]) {
  (void)unregister_udfs();
  deinit_logging_service_for_plugin(&reg_srv, &log_bi, &log_bs);
  return 0;
}

}  // namespace greatdb_http_client

/**
  Plugin library descriptor
*/
mysql_declare_plugin(greatdb_http_client){
    MYSQL_UDF_PLUGIN,                  // type
    &greatdb_http_client::descriptor,  // descriptor
    "greatdb_http_client",  // plugin name/head of registered variables
    "Greatdb Automatic Workload Repository", /* author */
    "Provide HTTP client function.",     /* description                     */
    PLUGIN_LICENSE_GPL,                  // license
    greatdb_http_client::plugin_init,    // init function (when loaded)
    nullptr,                             // check uninstall function
    greatdb_http_client::plugin_deinit,  // de-init function (when unloaded)
    0x0100,                              // version
    nullptr,                             // status variables
    greatdb_http_client::system_variables,  // system variables
    nullptr,
    0,
} mysql_declare_plugin_end;
