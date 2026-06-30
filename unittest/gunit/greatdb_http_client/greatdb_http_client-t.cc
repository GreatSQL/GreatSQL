/* Copyright (c) 2026, GreatDB Software Co., Ltd.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have either included with
   the program or referenced in the documentation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include <gtest/gtest.h>
#include <boost/asio.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>
#include <chrono>
#include <cstdlib>
#include <ctime>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include "sql/item_strfunc.h"
#include "sql/parse_tree_items.h"
#include "unittest/gunit/test_utils.h"

using my_testing::Server_initializer;

namespace beast = boost::beast;
namespace http = beast::http;
namespace net = boost::asio;
using tcp = boost::asio::ip::tcp;

namespace my_program_state {
std::size_t request_count() {
  static std::size_t count = 0;
  return ++count;
}

std::time_t now() { return std::time(0); }
}  // namespace my_program_state

class http_connection : public std::enable_shared_from_this<http_connection> {
 public:
  http_connection(tcp::socket socket) : socket_(std::move(socket)) {}

  void start() {
    read_request();
    check_deadline();
  }

 private:
  tcp::socket socket_;
  beast::flat_buffer buffer_{8192};
  http::request<http::dynamic_body> request_;
  http::response<http::dynamic_body> response_;
  net::steady_timer deadline_{socket_.get_executor(), std::chrono::seconds(60)};

  void read_request() {
    auto self = shared_from_this();

    http::async_read(
        socket_, buffer_, request_,
        [self](beast::error_code ec, std::size_t bytes_transferred) {
          boost::ignore_unused(bytes_transferred);
          if (!ec) self->process_request();
        });
  }

  void process_request() {
    response_.version(request_.version());
    response_.keep_alive(false);

    switch (request_.method()) {
      case http::verb::get:
        response_.result(http::status::ok);
        response_.set(http::field::server, "Beast");
        create_response();
        break;

      default:
        response_.result(http::status::bad_request);
        response_.set(http::field::content_type, "text/plain");
        beast::ostream(response_.body())
            << "Invalid request-method '"
            << std::string(request_.method_string()) << "'";
        break;
    }

    write_response();
  }

  void create_response() {
    if (request_.target() == "/") {
      response_.set(http::field::content_type, "text/plain");
      beast::ostream(response_.body())
          << "Hello, World from Simple HTTP Server!";
    } else if (request_.target() == "/geo") {
      response_.set(http::field::content_type, "text/plain");
      beast::ostream(response_.body())
          << "{\"geometries\":[{\"x\":495733.00613641395,\"y\":4345099."
             "004276541}]}";
    } else {
      response_.result(http::status::not_found);
      response_.set(http::field::content_type, "text/plain");
      beast::ostream(response_.body()) << "File not found\r\n";
    }
  }

  void write_response() {
    auto self = shared_from_this();

    response_.content_length(response_.body().size());

    http::async_write(socket_, response_,
                      [self](beast::error_code ec, std::size_t) {
                        self->socket_.shutdown(tcp::socket::shutdown_send, ec);
                        self->deadline_.cancel();
                      });
  }

  void check_deadline() {
    auto self = shared_from_this();

    deadline_.async_wait([self](beast::error_code ec) {
      if (!ec) {
        self->socket_.close(ec);
      }
    });
  }
};

class BoostHttpServer {
 public:
  BoostHttpServer(const std::string &address, unsigned short port)
      : address_(net::ip::make_address(address)),
        port_(port),
        acceptor_(ioc_, {address_, port_}),
        socket_(ioc_),
        running_(false) {}

  ~BoostHttpServer() { stop(); }

  void start() {
    if (running_) return;

    running_ = true;
    server_thread_ = std::thread([this]() {
      accept_connections();
      ioc_.run();
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  void stop() {
    if (!running_) return;

    running_ = false;
    ioc_.stop();
    if (server_thread_.joinable()) {
      server_thread_.join();
    }
  }

  unsigned short port() const { return port_; }

 private:
  void accept_connections() {
    acceptor_.async_accept(socket_, [this](beast::error_code ec) {
      if (!ec) {
        std::make_shared<http_connection>(std::move(socket_))->start();
      }

      if (running_) {
        socket_ = tcp::socket(ioc_);
        accept_connections();
      }
    });
  }

  net::ip::address address_;
  unsigned short port_;
  net::io_context ioc_;
  tcp::acceptor acceptor_;
  tcp::socket socket_;
  std::thread server_thread_;
  bool running_;
};

class ItemFuncHttpGetTest : public ::testing::Test {
 protected:
  void SetUp() override {
    server.start();
    initializer.SetUp();
  }

  void TearDown() override {
    initializer.TearDown();
    server.stop();
  }

  THD *thd() { return initializer.thd(); }

  Server_initializer initializer;
  BoostHttpServer server{"127.0.0.1", 8080};
};

namespace greatdb_http_client {
extern bool http_get_init(UDF_INIT *initid, UDF_ARGS *args, char *message);
extern char *http_get(UDF_INIT *initid, UDF_ARGS *args, char *result,
                      unsigned long *length, char *is_null, char *error);
extern ulong max_response_size;
}  // namespace greatdb_http_client

TEST_F(ItemFuncHttpGetTest, HttpGetInitSucceed) {
  UDF_INIT initid;
  Udf_return_value_extension m_return_value_extension;
  initid.extension = &m_return_value_extension;

  UDF_ARGS f_args;
  Udf_args_extension m_args_extension;
  f_args.extension = &m_args_extension;
  f_args.arg_count = 1;
  m_args_extension.charset_info =
      (const CHARSET_INFO **)malloc(1 * sizeof(CHARSET_INFO *));
  m_args_extension.charset_info[0] = system_charset_info;

  char init_msg_buff[MYSQL_ERRMSG_SIZE];
  *init_msg_buff = '\0';

  EXPECT_EQ(
      0, greatdb_http_client::http_get_init(&initid, &f_args, init_msg_buff));
  free(m_args_extension.charset_info);
}

TEST_F(ItemFuncHttpGetTest, HttpGetInitFailed) {
  UDF_INIT initid;
  Udf_return_value_extension m_return_value_extension;
  initid.extension = &m_return_value_extension;

  UDF_ARGS f_args;
  Udf_args_extension m_args_extension;
  f_args.extension = &m_args_extension;
  f_args.arg_count = 0;
  m_args_extension.charset_info =
      (const CHARSET_INFO **)malloc(1 * sizeof(CHARSET_INFO *));
  m_args_extension.charset_info[0] = system_charset_info;

  char init_msg_buff[MYSQL_ERRMSG_SIZE];
  *init_msg_buff = '\0';

  EXPECT_EQ(
      1, greatdb_http_client::http_get_init(&initid, &f_args, init_msg_buff));
  free(m_args_extension.charset_info);
}

TEST_F(ItemFuncHttpGetTest, HttpGetRootSucceed) {
  UDF_INIT initid;
  Udf_return_value_extension m_return_value_extension;
  initid.extension = &m_return_value_extension;

  UDF_ARGS f_args;
  Udf_args_extension m_args_extension;
  f_args.args = (char **)malloc(1 * sizeof(char *));
  char url[] = "http://127.0.0.1:8080/";
  f_args.args[0] = url;
  f_args.lengths = (ulong *)malloc(1 * sizeof(long));
  *f_args.lengths = strlen(url);
  f_args.extension = &m_args_extension;
  f_args.arg_count = 0;

  ulong buff_length = MAX_FIELD_WIDTH;
  char buff[MAX_FIELD_WIDTH] = {0};
  char error{0};
  char is_null{0};

  char *res = greatdb_http_client::http_get(&initid, &f_args, buff,
                                            &buff_length, &is_null, &error);
  EXPECT_NE(nullptr, res);
  EXPECT_EQ(0, is_null);
  EXPECT_EQ(0, error);
  EXPECT_EQ(37, buff_length);
  EXPECT_STREQ(buff, "Hello, World from Simple HTTP Server!");
  free(f_args.args);
  free(f_args.lengths);
}

TEST_F(ItemFuncHttpGetTest, HttpGetGeoSucceed) {
  UDF_INIT initid;
  Udf_return_value_extension m_return_value_extension;
  initid.extension = &m_return_value_extension;

  UDF_ARGS f_args;
  Udf_args_extension m_args_extension;
  f_args.args = (char **)malloc(1 * sizeof(char *));
  char url[] = "http://127.0.0.1:8080/geo";
  f_args.args[0] = url;
  f_args.lengths = (ulong *)malloc(1 * sizeof(long));
  *f_args.lengths = strlen(url);
  f_args.extension = &m_args_extension;
  f_args.arg_count = 0;

  ulong buff_length = MAX_FIELD_WIDTH;
  char buff[MAX_FIELD_WIDTH] = {0};
  char error{0};
  char is_null{0};

  char *res = greatdb_http_client::http_get(&initid, &f_args, buff,
                                            &buff_length, &is_null, &error);
  EXPECT_NE(nullptr, res);
  EXPECT_EQ(0, is_null);
  EXPECT_EQ(0, error);
  EXPECT_EQ(63, buff_length);
  EXPECT_STREQ(
      buff,
      "{\"geometries\":[{\"x\":495733.00613641395,\"y\":4345099.004276541}]}");
  free(f_args.args);
  free(f_args.lengths);
}

/**
  Error handler which registers if an error has been raised. If an error is
  raised, it asserts that the error is ER_GDB_HTTP_CLIENT_FAILED.
*/
class Failed_with_limit_handler : public Internal_error_handler {
 public:
  Failed_with_limit_handler(THD *thd)
      : m_thd(thd), m_called(false), m_orig_handler(error_handler_hook) {
    error_handler_hook = my_message_sql;
    thd->push_internal_handler(this);
  }

  ~Failed_with_limit_handler() override {
    EXPECT_EQ(this, m_thd->pop_internal_handler());
    error_handler_hook = m_orig_handler;
  }

  bool handle_condition(THD *, uint err, const char *,
                        Sql_condition::enum_severity_level *,
                        const char *) override {
    uint expected = ER_GDB_HTTP_CLIENT_FAILED;
    EXPECT_EQ(expected, err);
    m_called = true;
    return true;
  }

  bool is_called() const { return m_called; }

 private:
  THD *m_thd;
  bool m_called;
  ErrorHandlerFunctionPointer m_orig_handler;
};

TEST_F(ItemFuncHttpGetTest, HttpGetFailedWithBuffLimit) {
  Failed_with_limit_handler connect_refused_handler(thd());
  UDF_INIT initid;
  Udf_return_value_extension m_return_value_extension;
  initid.extension = &m_return_value_extension;

  UDF_ARGS f_args;
  Udf_args_extension m_args_extension;
  f_args.args = (char **)malloc(1 * sizeof(char *));
  char url[] = "http://127.0.0.1:8080/";
  f_args.args[0] = url;
  f_args.lengths = (ulong *)malloc(1 * sizeof(long));
  *f_args.lengths = strlen(url);
  f_args.extension = &m_args_extension;
  f_args.arg_count = 0;

  ulong buff_length = 16;
  char buff[16] = {0};
  char error{0};
  char is_null{0};

  char *res = greatdb_http_client::http_get(&initid, &f_args, buff,
                                            &buff_length, &is_null, &error);
  EXPECT_EQ(nullptr, res);
  EXPECT_EQ(1, is_null);
  EXPECT_EQ(1, error);
  free(f_args.args);
  free(f_args.lengths);
}

TEST_F(ItemFuncHttpGetTest, HttpGetFailedWithResponseLimit) {
  Failed_with_limit_handler connect_refused_handler(thd());
  UDF_INIT initid;
  Udf_return_value_extension m_return_value_extension;
  initid.extension = &m_return_value_extension;

  UDF_ARGS f_args;
  Udf_args_extension m_args_extension;
  f_args.args = (char **)malloc(1 * sizeof(char *));
  char url[] = "http://127.0.0.1:8080/";
  f_args.args[0] = url;
  f_args.lengths = (ulong *)malloc(1 * sizeof(long));
  *f_args.lengths = strlen(url);
  f_args.extension = &m_args_extension;
  f_args.arg_count = 0;

  ulong buff_length = MAX_FIELD_WIDTH;
  char buff[MAX_FIELD_WIDTH] = {0};
  char error{0};
  char is_null{0};

  greatdb_http_client::max_response_size = 16;
  char *res = greatdb_http_client::http_get(&initid, &f_args, buff,
                                            &buff_length, &is_null, &error);
  EXPECT_EQ(nullptr, res);
  EXPECT_EQ(1, is_null);
  EXPECT_EQ(1, error);
  free(f_args.args);
  free(f_args.lengths);
}
