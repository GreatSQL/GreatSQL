/*
  Copyright (c) 2019, 2021, Oracle and/or its affiliates.

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
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA
*/

#include "mock_session.h"

#include <array>
#include <chrono>
#include <iostream>  // cout
#include <memory>    // unique_ptr
#include <system_error>
#include <thread>  // sleep_for

#include <openssl/err.h>

#include "classic_mock_session.h"
#include "harness_assert.h"
#include "mysql/harness/logging/logging.h"  // log_
#include "mysql/harness/net_ts/impl/socket_constants.h"
#include "mysql/harness/net_ts/internet.h"  // net::ip::tcp
#include "mysql/harness/net_ts/socket.h"
#include "mysql/harness/stdx/expected.h"
#include "mysql/harness/tls_error.h"
#include "mysql/harness/tls_server_context.h"
#include "x_mock_session.h"

IMPORT_LOG_FUNCTIONS()

using namespace std::chrono_literals;

namespace server_mock {

ProtocolBase::ProtocolBase(net::ip::tcp::socket &&client_sock,
                           net::impl::socket::native_handle_type wakeup_fd,
                           TlsServerContext &tls_ctx)
    : client_socket_{std::move(client_sock)},
      wakeup_fd_{wakeup_fd},
      tls_ctx_{tls_ctx} {
  // if it doesn't work, no problem.
  //
  client_socket_.set_option(net::ip::tcp::no_delay{true});
  client_socket_.native_non_blocking(true);
}

static stdx::expected<bool, std::error_code> wait_socket(
    net::impl::socket::native_handle_type sock_fd,
    net::impl::socket::native_handle_type wakeup_fd, short ev,
    std::chrono::milliseconds timeout) {
  std::array<pollfd, 2> fds = {{
      {sock_fd, ev, 0},
      {wakeup_fd, POLLIN, 0},
  }};

  const auto poll_res = net::impl::poll::poll(fds.data(), fds.size(), timeout);
  if (!poll_res) {
    if (poll_res.error() == std::errc::timed_out) {
      return false;
    }

    return stdx::make_unexpected(poll_res.error());
  }

  if (fds[0].revents != 0) {
    return true;
  }

  // looks like the wakeup_fd fired
  return stdx::make_unexpected(make_error_code(std::errc::operation_canceled));
}

// check if the current socket is readable/open
stdx::expected<bool, std::error_code> ProtocolBase::wait_until_socket_has_data(
    std::chrono::milliseconds timeout) {
  return wait_socket(client_socket_.native_handle(), wakeup_fd_, POLLIN,
                     timeout);
}

stdx::expected<bool, std::error_code>
ProtocolBase::wait_until_socket_is_writable(std::chrono::milliseconds timeout) {
  return wait_socket(client_socket_.native_handle(), wakeup_fd_, POLLOUT,
                     timeout);
}

void ProtocolBase::send_buffer(net::const_buffer buf) {
  while (buf.size() > 0) {
    if (is_tls()) {
      const auto ssl_res = SSL_write(ssl_.get(), buf.data(), buf.size());
      if (ssl_res <= 0) {
        const auto ec = make_tls_ssl_error(ssl_.get(), ssl_res);

        if (ec == make_error_code(TlsErrc::kWantWrite)) {
          if (wait_until_socket_is_writable(-1ms)) continue;
        }

        throw std::system_error(ec, "send_buffer()");
      }

      buf += ssl_res;
    } else {
      const auto send_res = client_socket_.send(buf);
      if (!send_res) {
        if (send_res.error() == std::errc::operation_would_block) {
          if (wait_until_socket_is_writable(-1ms)) continue;
        }
        throw std::system_error(send_res.error(), "send_buffer()");
      }

      buf += send_res.value();
    }
  }
}

void ProtocolBase::read_buffer(net::mutable_buffer &buf) {
  while (buf.size() > 0) {
    if (is_tls()) {
      const auto res = SSL_read(ssl_.get(), buf.data(), buf.size());
      if (res <= 0) {
        const auto ec = make_tls_ssl_error(ssl_.get(), res);

        if (ec == TlsErrc::kWantRead) {
          const auto readable_res = wait_until_socket_has_data(-1ms);
          if (readable_res) continue;

          throw std::system_error(readable_res.error(), "read_buffer()::wait");
        }

        throw std::system_error(ec, "SSL_read()");
      }
      buf += res;
    } else {
      auto recv_res = client_socket_.receive(buf);
      if (!recv_res) {
        if (recv_res.error() == std::errc::operation_would_block) {
          auto readable_res = wait_until_socket_has_data(-1ms);
          if (readable_res) continue;

          throw std::system_error(readable_res.error(), "read_buffer()");
        }

        throw std::system_error(recv_res.error(), "read_buffer()");
      }

      buf += recv_res.value();
    }
  }
}

MySQLServerMockSession::MySQLServerMockSession(
    ProtocolBase *protocol,
    std::unique_ptr<StatementReaderBase> statement_processor, bool debug_mode)
    : json_reader_{std::move(statement_processor)},
      protocol_{protocol},
      debug_mode_{debug_mode} {}

void MySQLServerMockSession::run() {
  try {
    bool res = process_handshake();
    if (!res) {
      std::cout << "Error processing handshake with client: "
                << protocol_->client_socket().native_handle() << std::endl;
    }

    res = process_statements();
    if (!res) {
      std::cout << "Error processing statements with client: "
                << protocol_->client_socket().native_handle() << std::endl;
    }
  } catch (const std::exception &e) {
    log_warning("Exception caught in connection loop: %s", e.what());
  }
}

}  // namespace server_mock
