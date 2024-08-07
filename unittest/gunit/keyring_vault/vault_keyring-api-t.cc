/* Copyright (c) 2018, 2021 Percona LLC and/or its affiliates. All rights
   reserved.

   This program is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public License
   as published by the Free Software Foundation; version 2 of
   the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA */

#include <fstream>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "sql_plugin_ref.h"

#include <boost/move/unique_ptr.hpp>
#include <boost/preprocessor/stringize.hpp>

#include "generate_credential_file.h"
#include "mock_logger.h"
#include "vault_environment.h"
#include "vault_mount.h"
#include "vault_test_base.h"

#include "keyring_impl.cc"
#include "vault_keyring.cc"

std::string uuid = generate_uuid();

namespace keyring__api_unittest {
using ::testing::StrEq;
using namespace keyring;

class Keyring_vault_api_test : public Vault_test_base {
 public:
  ~Keyring_vault_api_test() override = default;

 protected:
  void SetUp() override {
    if (!check_env_configured()) {
      GTEST_SKIP() << "The vault environment is not configured";
    }

    Vault_test_base::SetUp();

    const std::string &conf_name =
        get_vault_env()->get_default_conf_file_name();
    keyring_vault_config_file_storage_.assign(
        conf_name.c_str(), conf_name.c_str() + conf_name.size() + 1);

    keyring_vault_config_file = keyring_vault_config_file_storage_.data();

    st_plugin_int plugin_info;  // for Logger initialization
    plugin_info.name.str = plugin_name;
    plugin_info.name.length = strlen(plugin_name);
    ASSERT_FALSE(keyring_vault_init(&plugin_info));
    // use MockLogger instead of Logger
    logger.reset(new Mock_logger);

    key_memory_KEYRING = PSI_NOT_INSTRUMENTED;
    key_LOCK_keyring = PSI_NOT_INSTRUMENTED;
  }
  void TearDown() override {
    keyring_vault_deinit(nullptr);

    Vault_test_base::TearDown();
  }

 protected:
  static char plugin_name[];
  static std::string sample_key_data;

  typedef std::vector<char> char_container;
  char_container keyring_vault_config_file_storage_;
};
/*static*/ char Keyring_vault_api_test::plugin_name[] = "FakeKeyring";
/*static*/ std::string Keyring_vault_api_test::sample_key_data = "Robi";

TEST_F(Keyring_vault_api_test, StoreFetchRemove) {
  EXPECT_FALSE(mysql_key_store(
      (get_vault_env()->get_uuid() + "Robert_key").c_str(), "AES", "Robert",
      sample_key_data.c_str(), sample_key_data.length()));
  char *key_type;
  size_t key_len;
  void *key;
  EXPECT_FALSE(
      mysql_key_fetch((get_vault_env()->get_uuid() + "Robert_key").c_str(),
                      &key_type, "Robert", &key, &key_len));
  EXPECT_STREQ("AES", key_type);
  EXPECT_EQ(key_len, sample_key_data.length());
  ASSERT_TRUE(memcmp((char *)key, sample_key_data.c_str(), key_len) == 0);
  my_free(key_type);
  key_type = nullptr;
  my_free(key);
  key = nullptr;
  EXPECT_FALSE(mysql_key_remove(
      (get_vault_env()->get_uuid() + "Robert_key").c_str(), "Robert"));
  // make sure the key was removed - fetch it
  EXPECT_FALSE(
      mysql_key_fetch((get_vault_env()->get_uuid() + "Robert_key").c_str(),
                      &key_type, "Robert", &key, &key_len));
  ASSERT_TRUE(key == nullptr);
}

TEST_F(Keyring_vault_api_test, CheckIfInmemoryKeyIsNOTXORed) {
  EXPECT_FALSE(mysql_key_store(
      (get_vault_env()->get_uuid() + "Robert_key").c_str(), "AES", "Robert",
      sample_key_data.c_str(), sample_key_data.length()));

  Vault_key key_id((get_vault_env()->get_uuid() + "Robert_key").c_str(),
                   nullptr, "Robert", nullptr, 0);
  IKey *fetched_key = keys->fetch_key(&key_id);
  ASSERT_TRUE(fetched_key != nullptr);
  std::string expected_key_signature =
      get_vault_env()->get_key_signature("Robert_key", "Robert");
  EXPECT_STREQ(fetched_key->get_key_signature()->c_str(),
               expected_key_signature.c_str());
  EXPECT_EQ(fetched_key->get_key_signature()->length(),
            expected_key_signature.length());
  uchar *key_data_fetched = fetched_key->get_key_data();
  size_t key_data_fetched_size = fetched_key->get_key_data_size();
  EXPECT_STREQ("AES", fetched_key->get_key_type_as_string()->c_str());
  ASSERT_TRUE(memcmp(sample_key_data.c_str(), key_data_fetched,
                     key_data_fetched_size) == 0);
  ASSERT_TRUE(sample_key_data.length() == key_data_fetched_size);
  my_free(fetched_key->release_key_data());

  // clean up
  EXPECT_FALSE(mysql_key_remove(
      (get_vault_env()->get_uuid() + "Robert_key").c_str(), "Robert"));
}

TEST_F(Keyring_vault_api_test, FetchNotExisting) {
  char *key_type = nullptr;
  void *key = nullptr;
  size_t key_len = 0;
  EXPECT_FALSE(
      mysql_key_fetch((get_vault_env()->get_uuid() + "Robert_key").c_str(),
                      &key_type, "Robert", &key, &key_len));
  ASSERT_TRUE(key == nullptr);
}

TEST_F(Keyring_vault_api_test, RemoveNotExisting) {
  EXPECT_TRUE(mysql_key_remove(
      (get_vault_env()->get_uuid() + "Robert_key").c_str(), "Robert"));
}

TEST_F(Keyring_vault_api_test, StoreFetchNotExisting) {
  EXPECT_FALSE(mysql_key_store(
      (get_vault_env()->get_uuid() + "Robert_key").c_str(), "AES", "Robert",
      sample_key_data.c_str(), sample_key_data.length()));
  char *key_type;
  size_t key_len;
  void *key;
  EXPECT_FALSE(
      mysql_key_fetch("NotExisting", &key_type, "Robert", &key, &key_len));
  ASSERT_TRUE(key == nullptr);

  EXPECT_FALSE(mysql_key_remove(
      (get_vault_env()->get_uuid() + "Robert_key").c_str(), "Robert"));
}

TEST_F(Keyring_vault_api_test, StoreStoreStoreFetchRemove) {
  std::string key_data1("Robi1");
  std::string key_data2("Robi2");

  EXPECT_FALSE(mysql_key_store(
      (get_vault_env()->get_uuid() + "Robert_key").c_str(), "AES", "Robert",
      sample_key_data.c_str(), sample_key_data.length()));
  EXPECT_FALSE(
      mysql_key_store((get_vault_env()->get_uuid() + "Robert_key1").c_str(),
                      "AES", "Robert", key_data1.c_str(), key_data1.length()));
  EXPECT_FALSE(
      mysql_key_store((get_vault_env()->get_uuid() + "Robert_key2").c_str(),
                      "AES", "Robert", key_data2.c_str(), key_data2.length()));
  char *key_type;
  size_t key_len;
  void *key;
  EXPECT_FALSE(
      mysql_key_fetch((get_vault_env()->get_uuid() + "Robert_key1").c_str(),
                      &key_type, "Robert", &key, &key_len));
  EXPECT_STREQ("AES", key_type);
  EXPECT_EQ(key_len, key_data1.length());
  ASSERT_TRUE(memcmp((char *)key, key_data1.c_str(), key_len) == 0);
  my_free(key_type);
  key_type = nullptr;
  my_free(key);
  key = nullptr;
  EXPECT_FALSE(mysql_key_remove(
      (get_vault_env()->get_uuid() + "Robert_key2").c_str(), "Robert"));
  // make sure the key was removed - fetch it
  EXPECT_FALSE(
      mysql_key_fetch((get_vault_env()->get_uuid() + "Robert_key2").c_str(),
                      &key_type, "Robert", &key, &key_len));
  ASSERT_TRUE(key == nullptr);

  EXPECT_FALSE(mysql_key_remove(
      (get_vault_env()->get_uuid() + "Robert_key").c_str(), "Robert"));
  EXPECT_FALSE(mysql_key_remove(
      (get_vault_env()->get_uuid() + "Robert_key1").c_str(), "Robert"));
}

TEST_F(Keyring_vault_api_test, StoreValidTypes) {
  EXPECT_FALSE(mysql_key_store(
      (get_vault_env()->get_uuid() + "Robert_key").c_str(), "AES", "Robert",
      sample_key_data.c_str(), sample_key_data.length()));
  EXPECT_FALSE(mysql_key_store(
      (get_vault_env()->get_uuid() + "Robert_key3").c_str(), "RSA", "Robert",
      sample_key_data.c_str(), sample_key_data.length()));
  EXPECT_FALSE(mysql_key_store(
      (get_vault_env()->get_uuid() + "Robert_key4").c_str(), "DSA", "Robert",
      sample_key_data.c_str(), sample_key_data.length()));
  // clean up
  EXPECT_FALSE(mysql_key_remove(
      (get_vault_env()->get_uuid() + "Robert_key").c_str(), "Robert"));
  EXPECT_FALSE(mysql_key_remove(
      (get_vault_env()->get_uuid() + "Robert_key3").c_str(), "Robert"));
  EXPECT_FALSE(mysql_key_remove(
      (get_vault_env()->get_uuid() + "Robert_key4").c_str(), "Robert"));
}

TEST_F(Keyring_vault_api_test, StoreInvalidType) {
  EXPECT_CALL(*(reinterpret_cast<Mock_logger *>(logger.get())),
              log(MY_WARNING_LEVEL,
                  StrEq("Error while storing key: invalid key_type")));
  EXPECT_TRUE(mysql_key_store(
      (get_vault_env()->get_uuid() + "Robert_key").c_str(), "YYY", "Robert",
      sample_key_data.c_str(), sample_key_data.length()));
  char *key_type;
  size_t key_len;
  void *key;
  EXPECT_FALSE(
      mysql_key_fetch((get_vault_env()->get_uuid() + "Robert_key").c_str(),
                      &key_type, "Robert", &key, &key_len));
  ASSERT_TRUE(key == nullptr);
}

TEST_F(Keyring_vault_api_test, StoreTwiceTheSameDifferentTypes) {
  EXPECT_FALSE(mysql_key_store(
      (get_vault_env()->get_uuid() + "Robert_key").c_str(), "AES", "Robert",
      sample_key_data.c_str(), sample_key_data.length()));
  EXPECT_TRUE(mysql_key_store(
      (get_vault_env()->get_uuid() + "Robert_key").c_str(), "RSA", "Robert",
      sample_key_data.c_str(), sample_key_data.length()));
  EXPECT_FALSE(mysql_key_remove(
      (get_vault_env()->get_uuid() + "Robert_key").c_str(), "Robert"));
}

TEST_F(Keyring_vault_api_test, KeyGenerate) {
  EXPECT_FALSE(
      mysql_key_generate((get_vault_env()->get_uuid() + "Robert_key").c_str(),
                         "AES", "Robert", 128));
  char *key_type;
  size_t key_len;
  void *key;
  EXPECT_FALSE(
      mysql_key_fetch((get_vault_env()->get_uuid() + "Robert_key").c_str(),
                      &key_type, "Robert", &key, &key_len));
  EXPECT_STREQ("AES", key_type);
  EXPECT_EQ(key_len, (size_t)128);
  // Try accessing the last byte of key
  volatile char ch = ((char *)key)[key_len - 1];
  // Just to get rid of unused variable compiler error
  (void)ch;
  my_free(key);
  my_free(key_type);
  EXPECT_FALSE(mysql_key_remove(
      (get_vault_env()->get_uuid() + "Robert_key").c_str(), "Robert"));
}

TEST_F(Keyring_vault_api_test, NullUser) {
  EXPECT_FALSE(mysql_key_store(
      (get_vault_env()->get_uuid() + "Robert_key").c_str(), "AES", nullptr,
      sample_key_data.c_str(), sample_key_data.length() + 1));
  char *key_type;
  size_t key_len;
  void *key;
  EXPECT_FALSE(
      mysql_key_fetch((get_vault_env()->get_uuid() + "Robert_key").c_str(),
                      &key_type, nullptr, &key, &key_len));
  EXPECT_STREQ("AES", key_type);
  EXPECT_EQ(key_len, sample_key_data.length() + 1);
  ASSERT_TRUE(memcmp((char *)key, sample_key_data.c_str(), key_len) == 0);
  my_free(key_type);
  key_type = nullptr;
  my_free(key);
  key = nullptr;
  EXPECT_TRUE(mysql_key_store(
      (get_vault_env()->get_uuid() + "Robert_key").c_str(), "RSA", nullptr,
      sample_key_data.c_str(), sample_key_data.length() + 1));
  EXPECT_FALSE(mysql_key_store(
      (get_vault_env()->get_uuid() + "Kamil_key").c_str(), "AES", nullptr,
      sample_key_data.c_str(), sample_key_data.length() + 1));
  EXPECT_FALSE(
      mysql_key_fetch((get_vault_env()->get_uuid() + "Kamil_key").c_str(),
                      &key_type, nullptr, &key, &key_len));
  EXPECT_STREQ("AES", key_type);
  EXPECT_EQ(key_len, sample_key_data.length() + 1);
  ASSERT_TRUE(memcmp((char *)key, sample_key_data.c_str(), key_len) == 0);
  my_free(key_type);
  key_type = nullptr;
  my_free(key);
  key = nullptr;
  EXPECT_FALSE(mysql_key_store(
      (get_vault_env()->get_uuid() + "Artur_key").c_str(), "AES", "Artur",
      sample_key_data.c_str(), sample_key_data.length() + 1));
  EXPECT_FALSE(
      mysql_key_fetch((get_vault_env()->get_uuid() + "Artur_key").c_str(),
                      &key_type, "Artur", &key, &key_len));
  EXPECT_STREQ("AES", key_type);
  EXPECT_EQ(key_len, sample_key_data.length() + 1);
  ASSERT_TRUE(memcmp((char *)key, sample_key_data.c_str(), key_len) == 0);
  my_free(key_type);
  key_type = nullptr;
  my_free(key);
  key = nullptr;
  EXPECT_FALSE(mysql_key_remove(
      (get_vault_env()->get_uuid() + "Robert_key").c_str(), nullptr));
  EXPECT_FALSE(
      mysql_key_fetch((get_vault_env()->get_uuid() + "Robert_key").c_str(),
                      &key_type, "Robert", &key, &key_len));
  ASSERT_TRUE(key == nullptr);
  EXPECT_FALSE(
      mysql_key_fetch((get_vault_env()->get_uuid() + "Artur_key").c_str(),
                      &key_type, "Artur", &key, &key_len));
  EXPECT_STREQ("AES", key_type);
  EXPECT_EQ(key_len, sample_key_data.length() + 1);
  ASSERT_TRUE(memcmp((char *)key, sample_key_data.c_str(), key_len) == 0);
  my_free(key_type);
  key_type = nullptr;
  my_free(key);
  key = nullptr;

  EXPECT_FALSE(mysql_key_remove(
      (get_vault_env()->get_uuid() + "Kamil_key").c_str(), nullptr));
  EXPECT_FALSE(mysql_key_remove(
      (get_vault_env()->get_uuid() + "Artur_key").c_str(), "Artur"));
}

TEST_F(Keyring_vault_api_test, NullKeyId) {
  EXPECT_CALL(*(reinterpret_cast<Mock_logger *>(logger.get())),
              log(MY_WARNING_LEVEL,
                  StrEq("Error while storing key: key_id cannot be empty")));
  EXPECT_TRUE(mysql_key_store(nullptr, "AES", "Robert", sample_key_data.c_str(),
                              sample_key_data.length() + 1));
  EXPECT_CALL(*(reinterpret_cast<Mock_logger *>(logger.get())),
              log(MY_WARNING_LEVEL,
                  StrEq("Error while storing key: key_id cannot be empty")));
  EXPECT_TRUE(mysql_key_store(nullptr, "AES", nullptr, sample_key_data.c_str(),
                              sample_key_data.length() + 1));
  EXPECT_CALL(*(reinterpret_cast<Mock_logger *>(logger.get())),
              log(MY_WARNING_LEVEL,
                  StrEq("Error while storing key: key_id cannot be empty")));
  EXPECT_TRUE(mysql_key_store("", "AES", "Robert", sample_key_data.c_str(),
                              sample_key_data.length() + 1));
  EXPECT_CALL(*(reinterpret_cast<Mock_logger *>(logger.get())),
              log(MY_WARNING_LEVEL,
                  StrEq("Error while storing key: key_id cannot be empty")));
  EXPECT_TRUE(mysql_key_store("", "AES", nullptr, sample_key_data.c_str(),
                              sample_key_data.length() + 1));
  char *key_type;
  size_t key_len;
  void *key;
  EXPECT_CALL(*(reinterpret_cast<Mock_logger *>(logger.get())),
              log(MY_WARNING_LEVEL,
                  StrEq("Error while fetching key: key_id cannot be empty")));
  EXPECT_TRUE(mysql_key_fetch(nullptr, &key_type, "Robert", &key, &key_len));
  EXPECT_CALL(*(reinterpret_cast<Mock_logger *>(logger.get())),
              log(MY_WARNING_LEVEL,
                  StrEq("Error while fetching key: key_id cannot be empty")));
  EXPECT_TRUE(mysql_key_fetch(nullptr, &key_type, nullptr, &key, &key_len));
  EXPECT_CALL(*(reinterpret_cast<Mock_logger *>(logger.get())),
              log(MY_WARNING_LEVEL,
                  StrEq("Error while fetching key: key_id cannot be empty")));
  EXPECT_TRUE(mysql_key_fetch("", &key_type, "Robert", &key, &key_len));
  EXPECT_CALL(*(reinterpret_cast<Mock_logger *>(logger.get())),
              log(MY_WARNING_LEVEL,
                  StrEq("Error while fetching key: key_id cannot be empty")));
  EXPECT_TRUE(mysql_key_fetch("", &key_type, nullptr, &key, &key_len));

  EXPECT_CALL(*(reinterpret_cast<Mock_logger *>(logger.get())),
              log(MY_WARNING_LEVEL,
                  StrEq("Error while removing key: key_id cannot be empty")));
  EXPECT_TRUE(mysql_key_remove(nullptr, "Robert"));
  EXPECT_CALL(*(reinterpret_cast<Mock_logger *>(logger.get())),
              log(MY_WARNING_LEVEL,
                  StrEq("Error while removing key: key_id cannot be empty")));
  EXPECT_TRUE(mysql_key_remove(nullptr, nullptr));
  EXPECT_CALL(*(reinterpret_cast<Mock_logger *>(logger.get())),
              log(MY_WARNING_LEVEL,
                  StrEq("Error while removing key: key_id cannot be empty")));
  EXPECT_TRUE(mysql_key_remove("", "Robert"));
  EXPECT_CALL(*(reinterpret_cast<Mock_logger *>(logger.get())),
              log(MY_WARNING_LEVEL,
                  StrEq("Error while removing key: key_id cannot be empty")));
  EXPECT_TRUE(mysql_key_remove("", nullptr));

  EXPECT_CALL(*(reinterpret_cast<Mock_logger *>(logger.get())),
              log(MY_WARNING_LEVEL,
                  StrEq("Error while generating key: key_id cannot be empty")));
  EXPECT_TRUE(mysql_key_generate(nullptr, "AES", "Robert", 128));
  EXPECT_CALL(*(reinterpret_cast<Mock_logger *>(logger.get())),
              log(MY_WARNING_LEVEL,
                  StrEq("Error while generating key: key_id cannot be empty")));
  EXPECT_TRUE(mysql_key_generate(nullptr, "AES", nullptr, 128));
  EXPECT_CALL(*(reinterpret_cast<Mock_logger *>(logger.get())),
              log(MY_WARNING_LEVEL,
                  StrEq("Error while generating key: key_id cannot be empty")));
  EXPECT_TRUE(mysql_key_generate("", "AES", "Robert", 128));
  EXPECT_CALL(*(reinterpret_cast<Mock_logger *>(logger.get())),
              log(MY_WARNING_LEVEL,
                  StrEq("Error while generating key: key_id cannot be empty")));
  EXPECT_TRUE(mysql_key_generate("", "AES", nullptr, 128));
}

}  // namespace keyring__api_unittest
