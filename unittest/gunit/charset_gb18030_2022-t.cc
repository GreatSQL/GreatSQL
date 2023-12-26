/* Copyright (c) 2023, GreatDB Software Co., Ltd. All rights reserved.

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
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/

#include <gtest/gtest.h>
#include <zlib.h>
#include "m_ctype.h"
#include "my_sys.h"
// xxd -i GB18030_2022_MappingTableSMP.txt GB18030_2022_MappingTableSMP.h
#include "GB18030_2022_MappingTableSMP.h"
// xxd -i GB18030_2022_MappingTableBMP.txt GB18030_2022_MappingTableBMP.h
#include "GB18030_2022_MappingTableBMP.h"

namespace charset_gb18030_2022_unittest {

CHARSET_INFO *init_collation(const char *name) {
  MY_CHARSET_LOADER loader;
  return my_collation_get_by_name(&loader, name, MYF(0));
}

int hexCharToInt(unsigned char c) {
  if (c >= '0' && c <= '9') {
    return c - '0';
  } else if (c >= 'A' && c <= 'F') {
    return c - 'A' + 10;
  } else if (c >= 'a' && c <= 'f') {
    return c - 'a' + 10;
  } else {
    return -1;  // 非法字符
  }
}

int hexStringToByteArray(unsigned char *byteArray, unsigned char *hexStr,
                         size_t len) {
  for (size_t i = 0; i < len / 2; i++) {
    auto highNibble = hexCharToInt(hexStr[i * 2]);
    auto lowNibble = hexCharToInt(hexStr[i * 2 + 1]);
    if (highNibble == -1 || lowNibble == -1) {
      return 0;
    }
    byteArray[i] = (highNibble << 4) | lowNibble;
  }
  return len / 2;
}

bool hexStringToULong(unsigned long *res, unsigned char *hexStr, size_t len) {
  unsigned long result = 0;
  for (size_t i = 0; i < len; i++) {
    auto v = hexCharToInt(hexStr[i]);
    if (v == -1) return true;
    result = result << 4 | v;
  }
  *res = result;
  return false;
}

std::string hexdump(const char *msg, unsigned char *data, size_t len) {
  std::stringstream res;
  res << msg << ": " << std::hex << (int)data[0];

  for (size_t i = 1; i < len; i++) {
    res << ", " << std::hex << (int)data[i];
  }
  res << " ";
  return res.str();
}

bool cs_mb_wc(CHARSET_INFO *cs, my_wc_t unicode, unsigned char *gb18030,
              int mb_len) {
  unsigned char gb18030_test[4] = {0};
  my_wc_t unicode_test = 0;
  auto res1 = cs->cset->mb_wc(cs, &unicode_test, gb18030, gb18030 + mb_len);
  if (res1 == 2 || res1 == 1 || res1 == 4) {
    if (unicode_test != unicode) {
      std::cerr << " gb18030 to unicode failed "
                << hexdump("gb18030", gb18030, mb_len);
      std::cerr << " unicode: " << std::oct << unicode << " " << std::hex
                << unicode;
      std::cerr << " unicode_test: " << std::oct << unicode_test << " "
                << std::hex << unicode_test << std::endl;
      return true;
    }
  } else {
    std::cerr << " gb18030 to unicode failed:"
              << hexdump("gb18030", gb18030, mb_len) << "res1 " << res1;
    return true;
  }

  memset(gb18030_test, 0, sizeof(gb18030_test));
  auto res2 = cs->cset->wc_mb(
      cs, unicode, gb18030_test,
      reinterpret_cast<uchar *>(gb18030_test) + sizeof(gb18030_test));
  if (res2 == 2 || res2 == 1 || res2 == 4) {
    if (res2 != mb_len) {
      std::cerr << " unicode " << unicode << " gb18030 len is diffent result "
                << res2 << " mb:" << mb_len;
    }

    if (memcmp(gb18030, gb18030_test, mb_len > res2 ? res2 : mb_len) != 0) {
      std::cerr << "unicode to gb18030 failed ";
      std::cerr << hexdump("gb18030", gb18030, mb_len);
      std::cerr << hexdump("gb18030_test", gb18030_test, res2);
      std::cerr << " unicode " << std::oct << unicode << " " << std::hex
                << unicode << std::endl;
      return true;
    }
  } else {
    std::cerr << "unicode to gb18030 failed:" << unicode << " res:" << res2
              << std::endl;
    return true;
  }
  return false;
}

bool unicode_gb18030_2022(unsigned char *test_data, unsigned int data_len,
                          CHARSET_INFO *cs) {
  // 0x30, 0x30, 0x30, 0x30, 0x09, 0x30, 0x30, 0x0a
  // 0000	00
  unsigned int pos = 0;
  my_wc_t unicode = 0;

  // max is 4byte
  unsigned char gb18030[4] = {0};
  int mb_len = 0;
  for (unsigned int i = 0; i < data_len; i++) {
    if (test_data[i] == 0x09) {
      unicode = 0;
      if (hexStringToULong(&unicode, test_data + pos, i - pos)) {
        std::string buf((char *)test_data + pos, i - pos);
        std::cerr << "unicode failed:" << buf << " pos:" << i;
        return true;
      }
      //  printf("%lX", unicode);
      pos = i + 1;
    }
    if (test_data[i] == 0x0a) {
      memset(gb18030, 0, sizeof(gb18030));
      mb_len = 0;
      mb_len = hexStringToByteArray(gb18030, test_data + pos, i - pos);
      if (mb_len == 0) {
        std::string buf((char *)test_data + pos, i - pos);
        std::cerr << "gb18030 failed:" << buf << " pos:" << i;
        return true;
      }
      /*
      printf(" ");
      for(size_t j=0; j<mb_len ; j++ ) {
        printf("%02X", gb18030[j]);
      }
      printf("\n");
      */
      // gb18030-2022 -> unicode
      if (cs_mb_wc(cs, unicode, gb18030, mb_len)) return true;
      pos = i + 1;
    }
  }

  return false;
}

TEST(CharsetGB180302022BMPUnittest, LoadUninitLoad) {
  CHARSET_INFO *cs1 = init_collation("gb18030_2022_chinese_ci");
  EXPECT_NE(nullptr, cs1);
  unsigned char gb18030[4] = {0x84, 0x30, 0x8d, 0x31};
  my_wc_t unicode = 0xf97a;
  EXPECT_NE(true, cs_mb_wc(cs1, unicode, gb18030, 4));

  EXPECT_NE(true,
            unicode_gb18030_2022(GB18030_2022_MappingTableBMP_txt,
                                 GB18030_2022_MappingTableBMP_txt_len, cs1));
}

TEST(CharsetGB180302022SSMPUnittest, LoadUninitLoad) {
  CHARSET_INFO *cs1 = init_collation("gb18030_2022_chinese_ci");
  EXPECT_NE(nullptr, cs1);
  EXPECT_NE(true,
            unicode_gb18030_2022(GB18030_2022_MappingTableSMP_txt,
                                 GB18030_2022_MappingTableSMP_txt_len, cs1));
}

}  // namespace charset_gb18030_2022_unittest
