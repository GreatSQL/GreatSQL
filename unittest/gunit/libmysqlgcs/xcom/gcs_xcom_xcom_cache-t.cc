/* Copyright (c) 2016, 2024, Oracle and/or its affiliates.
   Copyright (c) 2024, 2026, GreatDB Software Co., Ltd.

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

#include <string>
#include <vector>

#include "gcs_base_test.h"

#include "app_data.h"
#include "synode_no.h"
#include "xcom_base.h"
#include "xcom_cache.h"
#include "xcom_cfg.h"

namespace gcs_xcom_xcom_unittest {

#define DEFAULT_CACHE_LENGTH 500000
#define DEFAULT_SHRUNK_LENGTH 1000

void setup_cache() { init_cache(); }

void cleanup_cache() { deinit_cache(); }

/**
 * Mocks the XCom maintenance task.
 */
void *cache_task(void *ptr) {
  bool *run = (bool *)ptr;
  while (*run) {
    do_cache_maintenance();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  return nullptr;
}

class GcsXComXComCache : public GcsBaseTest {
 public:
  const std::string *m_addr;
  char m_payload[512];
  u_int m_payload_size;
  node_address *m_na;
  site_def *m_sd;
  app_data m_a;
  synode_no m_synode;
  My_xp_thread_impl *m_thread;
  bool m_run;

 protected:
  GcsXComXComCache()
      : m_addr(nullptr),
        m_payload_size(0),
        m_na(nullptr),
        m_sd(nullptr),
        m_thread(nullptr),
        m_run(false) {}
  ~GcsXComXComCache() override = default;

  void SetUp() override {
    m_synode = {1, 1, 0};
    m_addr = new std::string("127.0.0.1:12345");
    char const *names[]{m_addr->c_str()};
    m_na = new_node_address(1, names);
    m_sd = new_site_def();
    init_site_def(1, m_na, m_sd);
    push_site_def(m_sd);
    m_payload_size = 512 - (u_int)sizeof(pax_msg) - (u_int)sizeof(app_data);
    init_app_msg(&m_a, m_payload, m_payload_size);
    init_cfg_app_xcom();
  }

  void TearDown() override {
    m_run = false;
    if (m_thread) {
      m_thread->join(nullptr);
      delete m_thread;
    }
    push_site_def(nullptr);
    free_site_defs();
    delete_node_address(1, m_na);
    cleanup_cache();
    deinit_cfg_app_xcom();
    delete m_addr;
  }

  virtual void cache_msg(synode_no synode) {
    pax_machine *pm = nullptr;
    pm = get_cache(synode);
    ASSERT_TRUE(pm != nullptr);
    ASSERT_TRUE(synode_eq(pm->synode, synode));
    unchecked_replace_pax_msg(&pm->proposer.msg, pax_msg_new(synode, m_sd));
    pm->proposer.msg->a = clone_app_data(&m_a);
    add_cache_size(pm);
  }

  virtual void cache_bulk(size_t num_msg) {
    while (m_synode.msgno <= num_msg) {
      cache_msg(m_synode);
      m_synode.msgno++;
    }
  }

  virtual size_t msg_size() {
    return sizeof(pax_msg) + sizeof(app_data) + m_payload_size;
  }

  virtual void basic_test_generic(size_t target_occupation) {
    setup_cache();
    ASSERT_EQ(get_xcom_cache_length(), DEFAULT_CACHE_LENGTH);
    ASSERT_EQ(get_xcom_cache_occupation(), 0);
    ASSERT_EQ(get_xcom_cache_size(), 0);

    cache_bulk(target_occupation);

    if (target_occupation < DEFAULT_CACHE_LENGTH) {
      ASSERT_EQ(get_xcom_cache_occupation(), target_occupation);
      ASSERT_EQ(get_xcom_cache_size(), target_occupation * (msg_size()));
    } else {
      size_t occupation = (target_occupation % DEFAULT_CACHE_LENGTH) +
                          (DEFAULT_CACHE_LENGTH - DEFAULT_SHRUNK_LENGTH);
      if (occupation == DEFAULT_CACHE_LENGTH) {
        occupation -= DEFAULT_SHRUNK_LENGTH;
      }
      ASSERT_EQ(get_xcom_cache_occupation(), occupation);
      ASSERT_EQ(get_xcom_cache_size(), occupation * (msg_size()));
    }
  }
};

/**
 * Basic test verify defaults and that once alloc max length for cache.
 */
TEST_F(GcsXComXComCache, XComCacheTestDefaults) { basic_test_generic(500000); }

/**
 * Checks the booundaries of occupation: length of cache is static value, 500k
 * slots. When the number of occupied slots is below length of cache, it still
 * has some free slots left; When the numbeer of occupied slots is above length
 * of cache, it does not have free slots left, so it will clean 1k slots with
 * LRU.
 */
TEST_F(GcsXComXComCache, XComCacheTestIncrementBelow) {
  basic_test_generic(499999);
  ASSERT_EQ(get_xcom_cache_length(), 500000);
  deinit_cache();
  ASSERT_EQ(get_xcom_cache_length(), 0);
  ASSERT_EQ(get_xcom_cache_occupation(), 0);
  ASSERT_EQ(get_xcom_cache_size(), 0);
}

TEST_F(GcsXComXComCache, XComCacheTestIncrementAbove) {
  basic_test_generic(500001);
  ASSERT_EQ(get_xcom_cache_length(), 500000);
  deinit_cache();
  ASSERT_EQ(get_xcom_cache_length(), 0);
  ASSERT_EQ(get_xcom_cache_occupation(), 0);
  ASSERT_EQ(get_xcom_cache_size(), 0);
}

/**
 * Stress test with a large occupation.
 */
TEST_F(GcsXComXComCache, XComCacheTestDefaultsLargeCache) {
  basic_test_generic(3000000);
  deinit_cache();
  ASSERT_EQ(get_xcom_cache_length(), 0);
  ASSERT_EQ(get_xcom_cache_occupation(), 0);
  ASSERT_EQ(get_xcom_cache_size(), 0);
}

/**
 * Iterates the cache starting with the oldest message. Simulates the recovery
 * of an unreachable node that just got back into the group.
 */
TEST_F(GcsXComXComCache, XComCacheTestIterateForward) {
  basic_test_generic(3000000);
  synode_no synode = {1, 1, 0};
  while (synode.msgno < m_synode.msgno) {
    pax_machine *pm = nullptr;
    pm = get_cache(synode);
    ASSERT_TRUE(pm != nullptr);
    ASSERT_TRUE(synode_eq(pm->synode, synode));
    synode.msgno++;
  }
  deinit_cache();
  ASSERT_EQ(get_xcom_cache_length(), 0);
  ASSERT_EQ(get_xcom_cache_occupation(), 0);
  ASSERT_EQ(get_xcom_cache_size(), 0);
}

/**
 * For performance comparison with the test above, just does 5M accesses to
 * the most recent message.
 */
TEST_F(GcsXComXComCache, XComCacheTestAccessRecent) {
  basic_test_generic(3000000);
  u_int iterations = 3000000;
  while (iterations > 0) {
    pax_machine *pm = nullptr;
    pm = get_cache(m_synode);
    ASSERT_TRUE(pm != nullptr);
    ASSERT_TRUE(synode_eq(pm->synode, m_synode));
    iterations--;
  }
  deinit_cache();
  ASSERT_EQ(get_xcom_cache_length(), 0);
  ASSERT_EQ(get_xcom_cache_occupation(), 0);
  ASSERT_EQ(get_xcom_cache_size(), 0);
}

}  // namespace gcs_xcom_xcom_unittest
