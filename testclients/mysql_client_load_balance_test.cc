/* Copyright (c) 2025, GreatDB Software Co., Ltd. All rights reserved.

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

#include <my_systime.h>
#include <mysql.h>
#include <mysqld_error.h>
#include <sql_common.h>
#include <stdio.h>
#include <string.h>
#include <set>
#include <sstream>
#include <thread>
#include <vector>
#include "my_alloc.h"
#include "my_default.h"
#include "my_getopt.h"
#include "my_sys.h"
#include "mysql/service_mysql_alloc.h"
#include "print_version.h"

static char *opt_db = nullptr;
static char *opt_user = nullptr;
static char *opt_password = nullptr;
static char *opt_load_balance_hosts = nullptr;
static unsigned int opt_threads = 1;
static unsigned int opt_query_count_per_thread = 10;
static char *opt_testcase = nullptr;
static MYSQL *global_mysql = nullptr;

/*
Abort unless given expression is non-zero.

SYNOPSIS
DIE_UNLESS(expr)

DESCRIPTION
We can't use any kind of system assert as we need to
preserve tested invariants in release builds as well.
*/

#define DIE_UNLESS(expr) \
  ((void)((expr) ? 0 : (die(__FILE__, __LINE__, #expr), 0)))
#define DIE_IF(expr) ((void)((expr) ? (die(__FILE__, __LINE__, #expr), 0) : 0))
#define DIE(expr) die(__FILE__, __LINE__, #expr)

static void die(const char *file, int line, const char *expr) {
  fflush(stdout);
  fprintf(stderr, "%lu:%s:%d: check failed: '%s'\n", pthread_self(), file, line,
          expr);
  fflush(stderr);
  exit(1);
}

struct my_tests_st {
  const char *name;
  void (*function)();
};

static MYSQL *mysql_client_loadbalance_init(
    MYSQL *con, const char *hosts, mysql_load_balance_strategy strategy,
    unsigned long long block_timeout) {
  MYSQL *res_con = mysql_init(con);
  DIE_UNLESS(res_con != nullptr);

  unsigned long long block_timeout_res = 0;
  mysql_load_balance_strategy strategy_res;
  char *hosts_res = nullptr;
  DIE_UNLESS(0 == mysql_get_option(res_con, MYSQL_LOAD_BALANCE_HOSTS,
                                   (void *)&hosts_res));
  DIE_UNLESS(hosts_res == nullptr);
  DIE_UNLESS(0 == mysql_get_option(res_con, MYSQL_LOAD_BALANCE_STRATEGY,
                                   (void *)&strategy_res));
  DIE_UNLESS(strategy_res == LOAD_BALANCE_RANDOM);
  DIE_UNLESS(0 == mysql_get_option(res_con, MYSQL_LOAD_BALANCE_STRATEGY,
                                   (void *)&block_timeout_res));
  DIE_UNLESS(block_timeout_res == 0);

  int ret = mysql_options(res_con, MYSQL_LOAD_BALANCE_HOSTS,
                          (void *)const_cast<char *>(hosts));
  DIE_UNLESS(0 == ret);
  ret = mysql_options(res_con, MYSQL_LOAD_BALANCE_STRATEGY, (void *)&strategy);
  DIE_UNLESS(0 == ret);
  ret = mysql_options(res_con, MYSQL_LOAD_BALANCE_BLOCKLIST_TIMEOUT,
                      (void *)&block_timeout);
  DIE_UNLESS(0 == ret);
  return res_con;
}

static void prepare_test_table() {
  MYSQL *mysql = mysql_client_loadbalance_init(nullptr, opt_load_balance_hosts,
                                               LOAD_BALANCE_ROUND_ROBIN, 5000);
  DIE_UNLESS(nullptr != mysql_real_connect(
                            mysql, nullptr, opt_user, opt_password, opt_db, 0,
                            nullptr,
                            CLIENT_MULTI_STATEMENTS | CLIENT_MULTI_RESULTS));
  const char *query = "create table if not exists t(id int)";
  int ret = mysql_real_query(mysql, query, strlen(query));
  DIE_UNLESS(0 == ret);
  query = "truncate table t";
  ret = mysql_real_query(mysql, query, strlen(query));
  DIE_UNLESS(0 == ret);
  my_sleep(3000000);
  mysql_close(mysql);
}

/* test load balance hosts format */
static void test_load_balance_hosts() {
  DIE_UNLESS(nullptr != global_mysql);
  const char *hosts1 =
      "127.0.0.1:3306,127.0.0. 1:3307, 127.0.0.1: "
      "3308,2001:0db8:85a3:0000:0000:8a2e:0370:7334:3309";
  int ret = mysql_options(global_mysql, MYSQL_LOAD_BALANCE_HOSTS,
                          (void *)const_cast<char *>(hosts1));
  DIE_UNLESS(0 == ret &&
             0 == strcmp(global_mysql->options.load_balance->load_balance_hosts,
                         hosts1));
  char *host_str = nullptr;
  DIE_UNLESS(0 == mysql_get_option(global_mysql, MYSQL_LOAD_BALANCE_HOSTS,
                                   (void *)&host_str));
  DIE_UNLESS(host_str ==
             global_mysql->options.load_balance->load_balance_hosts);

  DIE_UNLESS(global_mysql->options.load_balance->available_hosts.size() == 4);
  DIE_UNLESS(
      0 == strcmp(global_mysql->options.load_balance->available_hosts[0]->host,
                  "127.0.0.1"));
  DIE_UNLESS(global_mysql->options.load_balance->available_hosts[0]->port ==
             3306);
  DIE_UNLESS(
      0 == strcmp(global_mysql->options.load_balance->available_hosts[1]->host,
                  "127.0.0.1"));
  DIE_UNLESS(global_mysql->options.load_balance->available_hosts[1]->port ==
             3307);
  DIE_UNLESS(
      0 == strcmp(global_mysql->options.load_balance->available_hosts[2]->host,
                  "127.0.0.1"));
  DIE_UNLESS(global_mysql->options.load_balance->available_hosts[2]->port ==
             3308);
  DIE_UNLESS(
      0 == strcmp(global_mysql->options.load_balance->available_hosts[3]->host,
                  "2001:0db8:85a3:0000:0000:8a2e:0370:7334"));
  DIE_UNLESS(global_mysql->options.load_balance->available_hosts[3]->port ==
             3309);
  DIE_UNLESS(global_mysql->options.load_balance->strategy ==
                 LOAD_BALANCE_RANDOM &&
             global_mysql->options.load_balance->blocklist_timeout == 0);

  /* invalid hosts. */
  const char *hosts2 = "127.0.0.1:3306a";
  ret = mysql_options(global_mysql, MYSQL_LOAD_BALANCE_HOSTS,
                      (void *)const_cast<char *>(hosts2));
  DIE_UNLESS(0 != ret &&
             nullptr == global_mysql->options.load_balance->load_balance_hosts);
  DIE_UNLESS(global_mysql->options.load_balance->available_hosts.size() == 0);

  const char *hosts3 = "127.0.0.1:65536";
  ret = mysql_options(global_mysql, MYSQL_LOAD_BALANCE_HOSTS,
                      (void *)const_cast<char *>(hosts3));
  DIE_UNLESS(0 != ret &&
             nullptr == global_mysql->options.load_balance->load_balance_hosts);
  DIE_UNLESS(global_mysql->options.load_balance->available_hosts.size() == 0);
}

static void test_load_balance_strategy() {
  DIE_UNLESS(nullptr != global_mysql);
  mysql_load_balance_strategy strategy = LOAD_BALANCE_ROUND_ROBIN;
  int ret = mysql_options(global_mysql, MYSQL_LOAD_BALANCE_STRATEGY,
                          (void *)&strategy);
  DIE_UNLESS(0 == ret && global_mysql->options.load_balance->strategy ==
                             LOAD_BALANCE_ROUND_ROBIN);
  mysql_load_balance_strategy get_res = LOAD_BALANCE_RANDOM;
  DIE_UNLESS(0 == mysql_get_option(global_mysql, MYSQL_LOAD_BALANCE_STRATEGY,
                                   (void *)&get_res));
  DIE_UNLESS(LOAD_BALANCE_ROUND_ROBIN == get_res);
}

static void test_load_balance_block_timeout() {
  MYSQL *mysql = mysql_client_loadbalance_init(
      nullptr, "127.0.0.1:3306,127.0.0.1:3307", LOAD_BALANCE_ROUND_ROBIN, 500);
  DIE_UNLESS(mysql->options.load_balance->blocklist_timeout == 500);
  unsigned long long block_timeout_res = 0;
  DIE_UNLESS(0 == mysql_get_option(mysql, MYSQL_LOAD_BALANCE_BLOCKLIST_TIMEOUT,
                                   (void *)&block_timeout_res));
  DIE_UNLESS(500 == block_timeout_res);
  DIE_UNLESS(nullptr == mysql_real_connect(
                            mysql, nullptr, opt_user, opt_password, opt_db, 0,
                            nullptr,
                            CLIENT_MULTI_STATEMENTS | CLIENT_MULTI_RESULTS));
  DIE_UNLESS(0 == mysql->options.load_balance->available_hosts.size());
  DIE_UNLESS(2 == mysql->options.load_balance->block_hosts.size());
  my_sleep(5000);

  unsigned long long block_timeout = 0;
  DIE_UNLESS(0 == mysql_options(mysql, MYSQL_LOAD_BALANCE_BLOCKLIST_TIMEOUT,
                                (void *)&block_timeout));
  unsigned long long host_0_conn_cnt =
      mysql->options.load_balance->hosts[0].connect_fail_cnt;
  unsigned long long host_1_conn_cnt =
      mysql->options.load_balance->hosts[1].connect_fail_cnt;
  DIE_UNLESS(nullptr == mysql_real_connect(
                            mysql, nullptr, opt_user, opt_password, opt_db, 0,
                            nullptr,
                            CLIENT_MULTI_STATEMENTS | CLIENT_MULTI_RESULTS));
  DIE_UNLESS(host_0_conn_cnt + 1 ==
             mysql->options.load_balance->hosts[0].connect_fail_cnt);
  DIE_UNLESS(host_1_conn_cnt + 1 ==
             mysql->options.load_balance->hosts[1].connect_fail_cnt);

  net_async_status mysql_conn_status = mysql_real_connect_nonblocking(
      mysql, nullptr, opt_user, opt_password, opt_db, 0, nullptr,
      CLIENT_MULTI_STATEMENTS | CLIENT_MULTI_RESULTS);
  DIE_UNLESS(NET_ASYNC_ERROR == mysql_conn_status);
  DIE_UNLESS(host_0_conn_cnt + 2 ==
             mysql->options.load_balance->hosts[0].connect_fail_cnt);
  DIE_UNLESS(host_1_conn_cnt + 2 ==
             mysql->options.load_balance->hosts[1].connect_fail_cnt);
  mysql_close(mysql);
}

static void normal_test_connect_impl(mysql_load_balance_strategy strategy) {
  MYSQL *mysql = mysql_client_loadbalance_init(nullptr, opt_load_balance_hosts,
                                               strategy, 1000);
  DIE_UNLESS(nullptr != mysql);
  DIE_UNLESS(nullptr != mysql_real_connect(
                            mysql, nullptr, opt_user, opt_password, opt_db, 0,
                            nullptr,
                            CLIENT_MULTI_STATEMENTS | CLIENT_MULTI_RESULTS));

  for (int i = 0; i < 10; ++i) {
    DIE_UNLESS(nullptr != mysql_real_connect(mysql, nullptr, opt_user,
                                             opt_password, opt_db, 0, nullptr,
                                             0));
    DIE_UNLESS(mysql->options.load_balance->blocklist_timeout == 1000);
    // fprintf(stdout, "current connect host: %s:%u\n",
    //         mysql->options.load_balance->current_conn_host->host,
    //         mysql->options.load_balance->current_conn_host->port);
    const char *query = "select id, @@port from t";
    DIE_UNLESS(0 == mysql_real_query(mysql, query, strlen(query)));
    auto *res = mysql_store_result(mysql);
    DIE_UNLESS(nullptr != res && 1 == res->row_count);
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(res)) != nullptr) {
      DIE_UNLESS(std::stoul(row[1]) ==
                     mysql->options.load_balance->current_conn_host->port &&
                 std::stoul(row[0]) == 1);
    }
    mysql_free_result(res);
    char *ip = nullptr;
    unsigned int port = 0;
    DIE_UNLESS(0 == mysql_get_current_connection_host(mysql, &ip, &port));
    DIE_UNLESS(ip == mysql->options.load_balance->current_conn_host->host &&
               port == mysql->options.load_balance->current_conn_host->port);
  }
  char *status = nullptr;
  DIE_UNLESS(0 == mysql_get_loadbalance_status(mysql, &status));
  fprintf(stdout, "status:\n%s\n", status);
  DIE_UNLESS(mysql->options.load_balance->external_connect_cnt == 11);
  mysql_close(mysql);
}

static void test_load_balance_connect_normal() {
  MYSQL *mysql = mysql_client_loadbalance_init(nullptr, opt_load_balance_hosts,
                                               LOAD_BALANCE_RANDOM, 1000);
  DIE_UNLESS(nullptr != mysql);
  DIE_UNLESS(nullptr != mysql_real_connect(
                            mysql, nullptr, opt_user, opt_password, opt_db, 0,
                            nullptr,
                            CLIENT_MULTI_STATEMENTS | CLIENT_MULTI_RESULTS));
  const char *query = "insert into t values(1)";
  int ret = mysql_real_query(mysql, query, strlen(query));
  DIE_UNLESS(0 == ret);
  mysql_close(mysql);
  my_sleep(3000000);

  normal_test_connect_impl(LOAD_BALANCE_RANDOM);
  normal_test_connect_impl(LOAD_BALANCE_ROUND_ROBIN);
}

static void test_load_balance_reconnect() {
  MYSQL *mysql = mysql_client_loadbalance_init(nullptr, opt_load_balance_hosts,
                                               LOAD_BALANCE_ROUND_ROBIN, 5000);
  DIE_UNLESS(nullptr != mysql_real_connect(
                            mysql, nullptr, opt_user, opt_password, opt_db, 0,
                            nullptr,
                            CLIENT_MULTI_STATEMENTS | CLIENT_MULTI_RESULTS));
  load_balance_host *old_host = mysql->options.load_balance->current_conn_host;
  DIE_UNLESS(nullptr != old_host);

  /* simulate network error */
  end_server(mysql);
  mysql->net.error = NET_ERROR_SOCKET_UNUSABLE;
  DIE_UNLESS(nullptr == mysql->net.vio);

  /* network error, exec query failed */
  const char *query = "insert into t values(2)";
  DIE_UNLESS(0 != mysql_real_query(mysql, query, strlen(query)));
  DIE_UNLESS(CR_SERVER_LOST == mysql->net.last_errno);

  /* enable reconnect, mysql will reconnect to old_host when network error */
  bool reconnect = true;
  DIE_UNLESS(0 ==
             mysql_options(mysql, MYSQL_OPT_RECONNECT, (void *)&reconnect));

  query = "insert into t values(2)";
  DIE_UNLESS(0 == mysql_real_query(mysql, query, strlen(query)));
  DIE_UNLESS(nullptr != mysql->net.vio);

  query = "select id, @@port, 1 from t where id=2";
  DIE_UNLESS(0 == mysql_real_query(mysql, query, strlen(query)));
  auto *res = mysql_store_result(mysql);
  DIE_UNLESS(nullptr != res && 1 == res->row_count);
  MYSQL_ROW row;
  while ((row = mysql_fetch_row(res)) != nullptr) {
    DIE_UNLESS(std::stoul(row[1]) == old_host->port &&
               std::stoul(row[0]) == 2 && std::stoul(row[2]) == 1);
  }
  mysql_free_result(res);
  char *ip = nullptr;
  unsigned int port = 0;
  int ret = mysql_get_current_connection_host(mysql, &ip, &port);
  DIE_UNLESS(ip == old_host->host && port == old_host->port);
  DIE_UNLESS(mysql->options.load_balance->block_hosts.size() == 0);
  for (auto &item : mysql->options.load_balance->available_hosts) {
    if (old_host == item) {
      DIE_UNLESS(old_host->connect_cnt == 1 && old_host->reconnect_cnt == 1 &&
                 old_host->connect_fail_cnt == 0);
    } else {
      DIE_UNLESS(item->connect_cnt == 0 && item->reconnect_cnt == 0 &&
                 item->connect_fail_cnt == 0);
    }
  }

  /* disable reconnect, network error, connect to other host */
  reconnect = false;
  DIE_UNLESS(0 ==
             mysql_options(mysql, MYSQL_OPT_RECONNECT, (void *)&reconnect));
  /* simulate network error */
  end_server(mysql);
  mysql->net.error = NET_ERROR_SOCKET_UNUSABLE;
  DIE_UNLESS(nullptr == mysql->net.vio);
  query = "select id, @@port, 2 from t where id=2";
  DIE_UNLESS(0 != mysql_real_query(mysql, query, strlen(query)));
  DIE_UNLESS(CR_SERVER_LOST == mysql->net.last_errno);
  DIE_UNLESS(old_host->reconnect_cnt == 1);
  /* rebalance */
  DIE_UNLESS(nullptr != mysql_real_connect(
                            mysql, nullptr, opt_user, opt_password, opt_db, 0,
                            nullptr,
                            CLIENT_MULTI_STATEMENTS | CLIENT_MULTI_RESULTS));
  query = "select id, @@port, 3 from t where id=2";
  DIE_UNLESS(0 == mysql_real_query(mysql, query, strlen(query)));
  res = mysql_store_result(mysql);
  DIE_UNLESS(nullptr != res && 1 == res->row_count);
  while ((row = mysql_fetch_row(res)) != nullptr) {
    DIE_UNLESS(std::stoul(row[0]) == 2);
    DIE_UNLESS(std::stoul(row[1]) ==
               mysql->options.load_balance->current_conn_host->port);
    DIE_UNLESS(std::stoul(row[2]) == 3);
  }
  mysql_free_result(res);

  char *status = nullptr;
  ret = mysql_get_loadbalance_status(mysql, &status);
  DIE_UNLESS(0 == ret);
  fprintf(stdout, "status:\n%s\n", status);
  mysql_close(mysql);
}

static void test_load_balance_no_avliable_host() {
  /* sub test 1 */
  MYSQL *mysql = mysql_init(nullptr);
  DIE_UNLESS(mysql != nullptr);
  unsigned long long block_timeout = 1000;
  int ret = mysql_options(mysql, MYSQL_LOAD_BALANCE_BLOCKLIST_TIMEOUT,
                          (void *)&block_timeout);
  DIE_UNLESS(0 == ret);
  char *status = nullptr;
  ret = mysql_get_loadbalance_status(mysql, &status);
  DIE_UNLESS(0 == ret);
  fprintf(stdout, "step 1 status:\n%s\n", status);
  mysql_load_balance_strategy strategy = LOAD_BALANCE_ROUND_ROBIN;
  ret = mysql_options(mysql, MYSQL_LOAD_BALANCE_STRATEGY, (void *)&strategy);
  DIE_UNLESS(0 == ret);
  ret = mysql_get_loadbalance_status(mysql, &status);
  DIE_UNLESS(0 == ret);
  fprintf(stdout, "step 2 status:\n%s\n", status);
  mysql_close(mysql);

  /* sub test 2 */
  mysql = mysql_client_loadbalance_init(
      nullptr, "127.0.0.0:666,127.0.0.0:777,127.0.0.0:888",
      LOAD_BALANCE_ROUND_ROBIN, 5000);
  DIE_UNLESS(nullptr == mysql_real_connect(
                            mysql, nullptr, opt_user, opt_password, opt_db, 0,
                            nullptr,
                            CLIENT_MULTI_STATEMENTS | CLIENT_MULTI_RESULTS));
  DIE_UNLESS(0 != mysql->net.last_errno);
  ret = mysql_get_loadbalance_status(mysql, &status);
  DIE_UNLESS(0 == ret);
  fprintf(stdout, "step 3 status:\n%s\n", status);
  mysql_close(mysql);
}

static void test_load_balance_nonblocking() {
  MYSQL *mysql = mysql_client_loadbalance_init(nullptr, opt_load_balance_hosts,
                                               LOAD_BALANCE_ROUND_ROBIN, 5000);
  /* connect host 1*/
  net_async_status mysql_conn_status = mysql_real_connect_nonblocking(
      mysql, nullptr, opt_user, opt_password, opt_db, 0, nullptr,
      CLIENT_MULTI_STATEMENTS);
  load_balance_host *connect_host =
      mysql->options.load_balance->current_conn_host;
  while (NET_ASYNC_NOT_READY == mysql_conn_status) {
    mysql_conn_status = mysql_real_connect_nonblocking(
        mysql, nullptr, opt_user, opt_password, opt_db, 0, nullptr,
        CLIENT_MULTI_STATEMENTS);
    DIE_UNLESS(connect_host == mysql->options.load_balance->current_conn_host);
    connect_host = mysql->options.load_balance->current_conn_host;
  }
  DIE_UNLESS(NET_ASYNC_COMPLETE == mysql_conn_status);
  const char *query = "insert into t values(3)";
  DIE_UNLESS(0 == mysql_real_query(mysql, query, strlen(query)));
  query = "select id, @@port, 1 from t where id=3";
  DIE_UNLESS(0 == mysql_real_query(mysql, query, strlen(query)));
  auto *res = mysql_store_result(mysql);
  DIE_UNLESS(nullptr != res && 1 == res->row_count);
  MYSQL_ROW row;
  while ((row = mysql_fetch_row(res)) != nullptr) {
    DIE_UNLESS(std::stoul(row[1]) == connect_host->port &&
               std::stoul(row[0]) == 3 && std::stoul(row[2]) == 1);
  }
  mysql_free_result(res);

  /* connect host 1 */
  mysql_conn_status = mysql_real_connect_nonblocking(
      mysql, nullptr, opt_user, opt_password, opt_db, 0, nullptr,
      CLIENT_MULTI_STATEMENTS);
  // DIE_UNLESS(connect_host != mysql->options.load_balance->current_conn_host);
  connect_host = mysql->options.load_balance->current_conn_host;
  while (NET_ASYNC_NOT_READY == mysql_conn_status) {
    mysql_conn_status = mysql_real_connect_nonblocking(
        mysql, nullptr, opt_user, opt_password, opt_db, 0, nullptr,
        CLIENT_MULTI_STATEMENTS);
    DIE_UNLESS(connect_host == mysql->options.load_balance->current_conn_host);
    connect_host = mysql->options.load_balance->current_conn_host;
  }
  DIE_UNLESS(NET_ASYNC_COMPLETE == mysql_conn_status);
  query = "select id, @@port, 2 from t where id=3";
  DIE_UNLESS(0 == mysql_real_query(mysql, query, strlen(query)));
  res = mysql_store_result(mysql);
  DIE_UNLESS(nullptr != res && 1 == res->row_count);
  while ((row = mysql_fetch_row(res)) != nullptr) {
    DIE_UNLESS(std::stoul(row[1]) == connect_host->port &&
               std::stoul(row[0]) == 3 && std::stoul(row[2]) == 2);
  }
  mysql_free_result(res);

  /* test reconnect */
  end_server(mysql);
  mysql->net.error = NET_ERROR_SOCKET_UNUSABLE;
  DIE_UNLESS(nullptr == mysql->net.vio);
  /* network error, exec query failed */
  query = "insert into t values(4)";
  DIE_UNLESS(0 != mysql_real_query(mysql, query, strlen(query)));
  DIE_UNLESS(CR_SERVER_LOST == mysql->net.last_errno);
  /* enable reconnect, mysql will reconnect to old_host when network error */
  bool reconnect = true;
  DIE_UNLESS(0 ==
             mysql_options(mysql, MYSQL_OPT_RECONNECT, (void *)&reconnect));
  query = "insert into t values(5)";
  DIE_UNLESS(0 == mysql_real_query(mysql, query, strlen(query)));
  DIE_UNLESS(nullptr != mysql->net.vio);
  DIE_UNLESS(connect_host == mysql->options.load_balance->current_conn_host);

  /* connect blocking */
  DIE_UNLESS(nullptr != mysql_real_connect(
                            mysql, nullptr, opt_user, opt_password, opt_db, 0,
                            nullptr,
                            CLIENT_MULTI_STATEMENTS | CLIENT_MULTI_RESULTS));

  char *status = nullptr;
  int ret = mysql_get_loadbalance_status(mysql, &status);
  DIE_UNLESS(0 == ret);
  fprintf(stdout, "status:\n%s\n", status);
  mysql_close(mysql);
}

static void test_load_balance_nonblocking_no_avliable_host() {
  MYSQL *mysql = mysql_client_loadbalance_init(
      nullptr, "127.0.0.0:666,127.0.0.0:777,127.0.0.0:888",
      LOAD_BALANCE_ROUND_ROBIN, 5000);
  net_async_status mysql_conn_status = mysql_real_connect_nonblocking(
      mysql, nullptr, opt_user, opt_password, opt_db, 0, nullptr,
      CLIENT_MULTI_STATEMENTS);
  DIE_UNLESS(NET_ASYNC_ERROR == mysql_conn_status);
  DIE_UNLESS(3 == mysql->options.load_balance->block_hosts.size());
  char *status = nullptr;
  int ret = mysql_get_loadbalance_status(mysql, &status);
  DIE_UNLESS(0 == ret);
  fprintf(stdout, "status:\n%s\n", status);
  mysql_close(mysql);
}

static void test_load_balance_performance() {
  /* unavailable hosts */
  std::string hosts =
      "127.0.0.1:2002,127.0.0.1:2003,127.0.0.0:3306,127.0.0.0:3307,127.0.0.0:"
      "3308,127.0.0.0:3309,127.0.0.0:3310,127.0.0.0:3311,127.0.0.0:3312,127.0."
      "0.0:3313,127.0.0.0:3314,127.0.0.0:3315,127.0.0.0:3316,127.0.0.0:3317,"
      "127.0.0.0:3318,";
  /* append available hosts */
  hosts += std::string(opt_load_balance_hosts);
  MYSQL *mysql = mysql_client_loadbalance_init(nullptr, hosts.c_str(),
                                               LOAD_BALANCE_RANDOM, 1);
  int connect_cnt = 0;
  unsigned long long begin = my_micro_time();
  while (++connect_cnt < 10000) {
    DIE_UNLESS(mysql_real_connect(mysql, nullptr, opt_user, opt_password,
                                  opt_db, 0, nullptr, CLIENT_MULTI_STATEMENTS));
  }
  unsigned long long connect_consume_time = my_micro_time() - begin;
  fprintf(
      stdout,
      "avliable host count: %lu, unavliable host count: %lu, "
      "mysql_real_connect %d times consume: %llu microseconds\nmark hosts "
      "avliable consume: %llu microseconds, percentage of connect time: "
      "%f%%\nmark hosts unavliable consume: %llu microseconds, percentage of "
      "connect time: %f%%\n",
      mysql->options.load_balance->available_hosts.size(),
      mysql->options.load_balance->block_hosts.size(), connect_cnt,
      connect_consume_time,
      mysql->options.load_balance->mark_host_available_used_time,
      (double)mysql->options.load_balance->mark_host_available_used_time /
          connect_consume_time * 100,
      mysql->options.load_balance->mark_host_block_used_time,
      (double)mysql->options.load_balance->mark_host_block_used_time /
          connect_consume_time * 100);
  mysql_close(mysql);
}

static void thread_func(unsigned int thread_index, bool nonblocking) {
  MYSQL *mysql = mysql_client_loadbalance_init(nullptr, opt_load_balance_hosts,
                                               LOAD_BALANCE_RANDOM, 5000);
  fprintf(stdout, "thread %u random seed=%u\n", thread_index,
          mysql->options.load_balance->random_seed);
  for (unsigned int i = 0; i < opt_query_count_per_thread; ++i) {
    if (!nonblocking) {
      MYSQL *res = mysql_real_connect(mysql, nullptr, opt_user, opt_password,
                                      opt_db, 0, nullptr, 0);
      if (nullptr == res) {
        fprintf(stdout, "connect failed, error:%s", mysql_error(mysql));
      }
      DIE_UNLESS(nullptr != res);
    } else {
      net_async_status mysql_conn_status = mysql_real_connect_nonblocking(
          mysql, nullptr, opt_user, opt_password, opt_db, 0, nullptr,
          CLIENT_MULTI_STATEMENTS);
      while (NET_ASYNC_NOT_READY == mysql_conn_status) {
        mysql_conn_status = mysql_real_connect_nonblocking(
            mysql, nullptr, opt_user, opt_password, opt_db, 0, nullptr,
            CLIENT_MULTI_STATEMENTS);
      }
      if (NET_ASYNC_COMPLETE != mysql_conn_status) {
        fprintf(stdout,
                "error, nonblocking connect failed, errno: %u, error: %s",
                mysql->net.last_errno, mysql->net.last_error);
      }
      DIE_UNLESS(NET_ASYNC_COMPLETE == mysql_conn_status);
    }
    std::string sql = "select @@port, " + std::to_string(i) + ", " +
                      std::to_string(thread_index);
    DIE_UNLESS(0 == mysql_real_query(mysql, sql.c_str(), sql.length()));
    auto *res = mysql_store_result(mysql);
    DIE_UNLESS(nullptr != res && 1 == res->row_count);
    MYSQL_ROW row;
    while ((row = mysql_fetch_row(res)) != nullptr) {
      DIE_UNLESS(std::stoul(row[0]) ==
                     mysql->options.load_balance->current_conn_host->port &&
                 std::stoul(row[1]) == i && std::stoul(row[2]) == thread_index);
    }
    mysql_free_result(res);
  }
  mysql_close(mysql);
  my_thread_end();
}

static void test_load_balance_multi_thread() {
  std::vector<std::thread> thd_vec;
  for (unsigned int i = 0; i < opt_threads; ++i) {
    if (i % 2 == 0) {
      thd_vec.emplace_back(std::thread(&thread_func, i, true));
    } else {
      thd_vec.emplace_back(std::thread(&thread_func, i, false));
    }
  }
  for (unsigned int i = 0; i < opt_threads; ++i) {
    if (thd_vec[i].joinable()) {
      thd_vec[i].join();
    }
  }
}

static void ori_mysql_real_connect_func(bool nonblocking) {
  std::string hosts_str = std::string(opt_load_balance_hosts);
  std::string host = hosts_str.substr(0, hosts_str.find_first_of(','));
  size_t port_offset = host.find_last_of(':');
  std::string ip = host.substr(0, port_offset);
  unsigned int port = atoi(host.c_str() + port_offset + 1);

  for (unsigned int i = 0; i < opt_query_count_per_thread; ++i) {
    MYSQL *mysql = nullptr;
    if (nonblocking) {
      mysql = mysql_init(nullptr);
      net_async_status status;
      while ((status = mysql_real_connect_nonblocking(
                  mysql, ip.c_str(), opt_user, opt_password, opt_db, port,
                  nullptr, 0)) == NET_ASYNC_NOT_READY) {
      }
      if (NET_ASYNC_COMPLETE != status) {
        fprintf(stdout,
                "error, mysql[%s] nonblock connect failed, errno: "
                "%u, error: %s\n",
                host.c_str(), mysql->net.last_errno, mysql->net.last_error);
      }
    } else {
      mysql = mysql_init(nullptr);
      if (!mysql_real_connect(mysql, ip.c_str(), opt_user, opt_password, opt_db,
                              port, nullptr, 0)) {
        fprintf(stdout,
                "error, mysql[%s] block connect failed, errno: %u, "
                "error: %s\n",
                host.c_str(), mysql->net.last_errno, mysql->net.last_error);
      }
    }
    mysql_close(mysql);
  }
  my_thread_end();
}

static void test_ori_mysql_real_connect() {
  std::vector<std::thread> thd_vec;
  for (unsigned int i = 0; i < opt_threads; ++i) {
    if (i % 2 == 0) {
      thd_vec.emplace_back(std::thread(&ori_mysql_real_connect_func, false));
    } else {
      thd_vec.emplace_back(std::thread(&ori_mysql_real_connect_func, false));
    }
  }
  for (unsigned int i = 0; i < opt_threads; ++i) {
    if (thd_vec[i].joinable()) {
      thd_vec[i].join();
    }
  }
}

static struct my_tests_st my_tests[] = {
    {"test_load_balance_hosts", test_load_balance_hosts},
    {"test_load_balance_strategy", test_load_balance_strategy},
    {"test_load_balance_block_timeout", test_load_balance_block_timeout},
    {"test_load_balance_connect_normal", test_load_balance_connect_normal},
    {"test_load_balance_reconnect", test_load_balance_reconnect},
    {"test_load_balance_no_avliable_host", test_load_balance_no_avliable_host},
    {"test_load_balance_nonblocking", test_load_balance_nonblocking},
    {"test_load_balance_nonblocking_no_avliable_host",
     test_load_balance_nonblocking_no_avliable_host},
    {"test_load_balance_performance", test_load_balance_performance},
    {"test_load_balance_multi_thread", test_load_balance_multi_thread},
    {"test_ori_mysql_real_connect", test_ori_mysql_real_connect}};

static struct my_option test_load_balance_options[] = {
    {"database", 'D', "Database to use.", &opt_db, &opt_db, nullptr,
     GET_STR_ALLOC, REQUIRED_ARG, 0, 0, 0, nullptr, 0, nullptr},
    {"user", 'u', "User for login.", &opt_user, &opt_user, nullptr,
     GET_STR_ALLOC, REQUIRED_ARG, 0, 0, 0, nullptr, 0, nullptr},
    {"password", 'p', "Password for login.", &opt_password, &opt_password,
     nullptr, GET_STR_ALLOC, REQUIRED_ARG, 0, 0, 0, nullptr, 0, nullptr},
    {"hosts", 'h', "Load balance hosts.", &opt_load_balance_hosts,
     &opt_load_balance_hosts, nullptr, GET_STR_ALLOC, REQUIRED_ARG, 0, 0, 0,
     nullptr, 0, nullptr},
    {"threads", 't', "thread count.", &opt_threads, &opt_threads, nullptr,
     GET_UINT, REQUIRED_ARG, 1, 1, 100, nullptr, 0, nullptr},
    {"query_count_per_thread", 'q', "query count per thread.",
     &opt_query_count_per_thread, &opt_query_count_per_thread, nullptr,
     GET_UINT, REQUIRED_ARG, 10, 1, 5000, nullptr, 0, nullptr},
    {"testcase", 'c', "testcase list.", &opt_testcase, &opt_testcase, nullptr,
     GET_STR_ALLOC, REQUIRED_ARG, 0, 0, 0, nullptr, 0, nullptr},
    {"help", '?', "Display this help and exit", nullptr, nullptr, nullptr,
     GET_NO_ARG, NO_ARG, 0, 0, 0, nullptr, 0, nullptr},
    {nullptr, 0, nullptr, nullptr, nullptr, nullptr, GET_NO_ARG, NO_ARG, 0, 0,
     0, nullptr, 0, nullptr}};

static void usage() {
  /* show the usage string when the user asks for this */
  print_version();
  printf("Usage: %s \n", my_progname);
  my_print_help(test_load_balance_options);
  my_print_variables(test_load_balance_options);
}

static bool get_one_option(int optid,
                           const struct my_option *opt [[maybe_unused]],
                           char *argument) {
  switch (optid) {
    case 'D':
      if (argument) {
        char *start = argument;
        my_free(opt_db);
        opt_db = my_strdup(PSI_NOT_INSTRUMENTED, argument, MYF(MY_FAE));
        while (*argument) *argument++ = 'x'; /* Destroy argument */
        if (*start) start[1] = 0;
      }
      break;
    case 'u':
      if (argument) {
        char *start = argument;
        my_free(opt_user);
        opt_user = my_strdup(PSI_NOT_INSTRUMENTED, argument, MYF(MY_FAE));
        while (*argument) *argument++ = 'x'; /* Destroy argument */
        if (*start) start[1] = 0;
      }
      break;
    case 'p':
      if (argument) {
        char *start = argument;
        my_free(opt_password);
        opt_password = my_strdup(PSI_NOT_INSTRUMENTED, argument, MYF(MY_FAE));
        while (*argument) *argument++ = 'x'; /* Destroy argument */
        if (*start) start[1] = 0;
      }
      break;
    case 'h':
      if (argument) {
        char *start = argument;
        my_free(opt_load_balance_hosts);
        opt_load_balance_hosts =
            my_strdup(PSI_NOT_INSTRUMENTED, argument, MYF(MY_FAE));
        while (*argument) *argument++ = 'x'; /* Destroy argument */
        if (*start) start[1] = 0;
      }
      break;
    case 'c':
      if (argument) {
        char *start = argument;
        my_free(opt_testcase);
        opt_testcase = my_strdup(PSI_NOT_INSTRUMENTED, argument, MYF(MY_FAE));
        while (*argument) *argument++ = 'x'; /* Destroy argument */
        if (*start) start[1] = 0;
      }
      break;
    case '?':
    case 'I': /* Info */
      usage();
      exit(0);
      break;
  }
  return false;
}

static void get_options(int *argc, char ***argv) {
  int ho_error;
  if ((ho_error = handle_options(argc, argv, test_load_balance_options,
                                 get_one_option)))
    exit(ho_error);
}

int main(int argc, char **argv) {
  get_options(&argc, &argv);
  if (!opt_user || !opt_password || !opt_db || !opt_load_balance_hosts) {
    fprintf(stdout, "Check the test options using --help or -?\n");
    exit(1);
  }
  DIE_UNLESS(0 == mysql_server_init(0, nullptr, nullptr));
  global_mysql = mysql_init(nullptr);
  prepare_test_table();
  size_t test_cnt = sizeof(my_tests) / sizeof(my_tests_st);
  std::set<std::string> testcase_set;
  if (opt_testcase) {
    std::stringstream ss;
    ss << std::string(opt_testcase);
    std::string test_case;
    while (getline(ss, test_case, ',')) testcase_set.insert(test_case);
  }
  for (size_t i = 0; i < test_cnt; ++i) {
    if (!testcase_set.empty() &&
        testcase_set.find(std::string(my_tests[i].name)) == testcase_set.end())
      continue;
    fprintf(stdout, "===== testcase[%lu/%lu] %s begin. =====\n", i + 1,
            test_cnt, my_tests[i].name);
    my_tests[i].function();
    fprintf(stdout, "===== testcase[%lu/%lu] %s success. =====\n", i + 1,
            test_cnt, my_tests[i].name);
  }

  mysql_close(global_mysql);
  mysql_server_end();
  my_end(0);

  my_free(opt_db);
  my_free(opt_user);
  my_free(opt_password);
  my_free(opt_load_balance_hosts);
  return 0;
}
