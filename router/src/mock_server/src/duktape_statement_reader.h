/*
  Copyright (c) 2018, 2021, Oracle and/or its affiliates.

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

#ifndef MYSQLD_MOCK_DUKTAPE_STATEMENT_READER_INCLUDED
#define MYSQLD_MOCK_DUKTAPE_STATEMENT_READER_INCLUDED

#include <map>
#include <string>

#include "mock_session.h"
#include "mysql/harness/stdx/expected.h"
#include "mysqlrouter/classic_protocol_message.h"
#include "mysqlrouter/mock_server_global_scope.h"
#include "statement_reader.h"

namespace server_mock {
class DuktapeStatementReader : public StatementReaderBase {
 public:
  enum class HandshakeState { INIT, GREETED, AUTH_SWITCHED, DONE };

  DuktapeStatementReader(const std::string &filename,
                         const std::vector<std::string> &module_prefixes,
                         std::map<std::string, std::string> session_data,
                         std::shared_ptr<MockServerGlobalScope> shared_globals);

  /**
   * handle the clients statement
   *
   * @param statement statement-text of the current clients
   *                  COM_QUERY/StmtExecute
   * @param protocol protocol to send response to
   */
  void handle_statement(const std::string &statement,
                        ProtocolBase *protocol) override;

  std::chrono::microseconds get_default_exec_time() override;

  ~DuktapeStatementReader() override;

  std::vector<AsyncNotice> get_async_notices() override;

  stdx::expected<classic_protocol::message::server::Greeting, std::error_code>
  server_greeting(bool with_tls) override;

  stdx::expected<handshake_data, std::error_code> handshake() override;

  std::chrono::microseconds server_greeting_exec_time() override;

  void set_session_ssl_info(const SSL *ssl) override;

 private:
  struct Pimpl;
  std::unique_ptr<Pimpl> pimpl_;
  bool has_notices_{false};

  HandshakeState handshake_state_{HandshakeState::INIT};
};
}  // namespace server_mock

#endif
