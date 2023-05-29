/* Copyright (c) 2023, GreatDB Software Co., Ltd.
  All rights reserved.

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
#define MYSQL_SERVER 1
#include "encrypt.h"
#include "my_sys.h"
#include "mysql/service_mysql_alloc.h"

#include <cstring>
static char plain_buff[ENCRYPT_BUFFER_SIZE + 1] = {0};
static char encry_buffer[ENCRYPT_BUFFER_SIZE + 1] = {0};

TYPELIB set_clone_encrypt_mode_typelib = {MY_AES_END + 1, "",
                                          my_aes_opmode_names, nullptr};

int encrypt_buffer(unsigned char *source, uint32 length, char *dest,
                   my_aes_opmode dump_encrypt_mode, char *dump_encrypt_key,
                   char *dump_encrypt_iv) {
  uint32 main_len;
  uint32 remain_len;
  int enc_len = 0;
  unsigned char remain_buf[MY_AES_BLOCK_SIZE * 2];
  if (length < REMAIN_BLOCK_SIZE || length >= ENCRYPT_BUFFER_SIZE) return -1;

  main_len = (length / MY_AES_BLOCK_SIZE) * MY_AES_BLOCK_SIZE;
  remain_len = length - main_len;

  enc_len =
      my_aes_encrypt(reinterpret_cast<unsigned char *>(source), main_len,
                     reinterpret_cast<unsigned char *>(dest),
                     reinterpret_cast<unsigned char *>(dump_encrypt_key),
                     strlen(dump_encrypt_key), dump_encrypt_mode,
                     reinterpret_cast<unsigned char *>(dump_encrypt_iv), false);
  if ((uint32)enc_len != main_len) return -1;

  if (remain_len != 0) {
    memcpy(dest + main_len, source + main_len, length - main_len);
    remain_len = MY_AES_BLOCK_SIZE * 2;

    enc_len = my_aes_encrypt(
        reinterpret_cast<unsigned char *>(dest + length - remain_len),
        remain_len, remain_buf,
        reinterpret_cast<unsigned char *>(dump_encrypt_key),
        strlen(dump_encrypt_key), dump_encrypt_mode,
        reinterpret_cast<unsigned char *>(dump_encrypt_iv), false);
    if ((uint32)enc_len != remain_len) return -1;

    memcpy(dest + length - remain_len, remain_buf, remain_len);
  }

  return length;
}

/* Key file has 3 lines.
 * First line: aes_opmode, max_size 32
 * Second line: encrypt key, max_size 256
 * Third line: aes_ebc encrypt iv, size 16
 */
int read_clone_encrypt_key(const char *opt_clone_encrypt_key,
                           my_aes_opmode *clone_encrypt_mode,
                           char **clone_encrypt_key, char **clone_encrypt_iv) {
  int c;
  int read_stage = 0;
  char *read_pos = nullptr;
  char *end_pos = nullptr;
  bool got_error = false;
  char *clone_encrypt_mode_str = nullptr;

  FILE *file = fopen(opt_clone_encrypt_key, "r");
  if (file == nullptr) {
    return 1;
  }

  c = fgetc(file);
  while (c != EOF) {
    if (c == '\n') {
      ++read_stage;
      if (read_pos != nullptr) *read_pos = 0;
    } else {
      if (read_stage == 0 && clone_encrypt_mode_str == nullptr) {
        if (!(clone_encrypt_mode_str = (char *)my_malloc(
                  PSI_NOT_INSTRUMENTED, CLONE_ENCRYPT_MODE_SIZE + 1,
                  MYF(MY_WME)))) {
          fclose(file);
          return 2;
        }
        read_pos = clone_encrypt_mode_str;
        end_pos = clone_encrypt_mode_str + CLONE_ENCRYPT_MODE_SIZE;
      } else if (read_stage == 1 && *clone_encrypt_key == nullptr) {
        if (!(*clone_encrypt_key =
                  (char *)my_malloc(PSI_NOT_INSTRUMENTED,
                                    MAX_ENCRYPT_KEY_SIZE + 1, MYF(MY_WME)))) {
          my_free(clone_encrypt_mode_str);
          fclose(file);
          return 2;
        }
        read_pos = *clone_encrypt_key;
        end_pos = *clone_encrypt_key + MAX_ENCRYPT_KEY_SIZE;
      } else if (read_stage == 2 && *clone_encrypt_iv == nullptr) {
        if (!(*clone_encrypt_iv = (char *)my_malloc(
                  PSI_NOT_INSTRUMENTED, MY_AES_IV_SIZE + 1, MYF(MY_WME)))) {
          my_free(clone_encrypt_mode_str);
          my_free(*clone_encrypt_key);
          fclose(file);
          return 2;
        }
        read_pos = *clone_encrypt_iv;
        end_pos = *clone_encrypt_iv + MY_AES_IV_SIZE;
      } else if (read_stage > 2) {
        got_error = true;
        break;
      }
      if (read_pos == end_pos) {
        got_error = true;
        break;
      }
      *read_pos = c;
      ++read_pos;
    }
    c = fgetc(file);
  }
  if (read_pos != nullptr) *read_pos = 0;
  fclose(file);

  if (clone_encrypt_mode_str == nullptr || *clone_encrypt_key == nullptr ||
      got_error || (strlen(*clone_encrypt_iv) != MY_AES_IV_SIZE)) {
    my_free(clone_encrypt_mode_str);
    my_free(*clone_encrypt_key);
    my_free(*clone_encrypt_iv);
    return 3;
  }
  int res = find_type(clone_encrypt_mode_str, &set_clone_encrypt_mode_typelib,
                      FIND_TYPE_BASIC);
  if (res <= 0) {
    my_free(clone_encrypt_mode_str);
    my_free(*clone_encrypt_key);
    my_free(*clone_encrypt_iv);
    return 4;
  }
  *clone_encrypt_mode = static_cast<my_aes_opmode>(res - 1);
  if (clone_encrypt_mode_str) my_free(clone_encrypt_mode_str);
  return 0;
}

int clone_decrypt_buffer(char *source, uint32 length, char *dest,
                         my_aes_opmode clone_encrypt_mode,
                         char *clone_encrypt_key, char *clone_encrypt_iv) {
  uint32 main_len;
  uint32 remain_len;
  int dec_len = 0;
  unsigned char remain_buf[MY_AES_BLOCK_SIZE * 2];
  if (length < REMAIN_BLOCK_SIZE || length >= ENCRYPT_BUFFER_SIZE) return -1;

  main_len = (length / MY_AES_BLOCK_SIZE) * MY_AES_BLOCK_SIZE;
  remain_len = length - main_len;

  if (remain_len != 0) {
    remain_len = MY_AES_BLOCK_SIZE * 2;
    memcpy(remain_buf, source + length - remain_len, remain_len);

    dec_len = my_aes_decrypt(
        remain_buf, remain_len,
        reinterpret_cast<unsigned char *>(source + length - remain_len),
        reinterpret_cast<unsigned char *>(clone_encrypt_key),
        strlen(clone_encrypt_key), clone_encrypt_mode,
        reinterpret_cast<unsigned char *>(clone_encrypt_iv), false);
    if ((uint32)dec_len != remain_len) return -1;
  }

  dec_len = my_aes_decrypt(reinterpret_cast<unsigned char *>(source), main_len,
                           reinterpret_cast<unsigned char *>(dest),
                           reinterpret_cast<unsigned char *>(clone_encrypt_key),
                           strlen(clone_encrypt_key), clone_encrypt_mode,
                           reinterpret_cast<unsigned char *>(clone_encrypt_iv),
                           false);

  if ((uint32)dec_len != main_len) return -1;

  if (remain_len != 0) {
    memcpy(dest + main_len, source + main_len, length - main_len);
  }

  return length;
}

int decrypt_clone_file(FILE *file, const char *clone_file,
                       my_aes_opmode clone_encrypt_mode,
                       char *clone_encrypt_key, char *clone_encrypt_iv) {
  int c;
  int pos = 0;
  int dec_len = 0;
  uint total_len = 0;
  FILE *clone = fopen(clone_file, "r");
  if (clone == nullptr) {
    fprintf(stderr, "Error: Fail to open encrypt clone file, errno: %d\n",
            errno);
    return 1;
  }

  c = fgetc(clone);
  while (c != EOF) {
    plain_buff[pos] = c;
    ++pos;
    if (pos == ENCRYPT_BUFFER_SIZE) {
      dec_len = clone_decrypt_buffer(plain_buff, ENCRYPT_BATCH_SIZE,
                                     encry_buffer, clone_encrypt_mode,
                                     clone_encrypt_key, clone_encrypt_iv);
      if (dec_len != ENCRYPT_BATCH_SIZE) {
        fclose(clone);
        return 1;
      }
      int enc_pos = 0;
      while (enc_pos < dec_len) {
        fputc(encry_buffer[enc_pos], file);
        ++enc_pos;
      }
      memmove(plain_buff, plain_buff + ENCRYPT_BATCH_SIZE, REMAIN_BLOCK_SIZE);
      pos = REMAIN_BLOCK_SIZE;
      total_len += dec_len;
    }
    c = fgetc(clone);
  }
  if (pos > 0) {
    dec_len =
        clone_decrypt_buffer(plain_buff, pos, encry_buffer, clone_encrypt_mode,
                             clone_encrypt_key, clone_encrypt_iv);
    if (dec_len != pos) {
      fclose(clone);
      return 1;
    }
    int enc_pos = 0;
    while (enc_pos < pos) {
      fputc(encry_buffer[enc_pos], file);
      ++enc_pos;
    }
  }
  fclose(clone);
  return 0;
}
