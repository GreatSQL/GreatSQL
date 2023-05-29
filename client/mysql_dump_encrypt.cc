/*
Copyright (c) 2023, GreatDB Software Co., Ltd.

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

#include "mysql_dump_encrypt.h"
#include "my_sys.h"
#include "mysql/service_mysql_alloc.h"

#include <cstring>

static char clear_buff[CIPHER_BUFFER_SIZE + 1] = {0};
static char cipher_buff[CIPHER_BUFFER_SIZE + 1] = {0};

static struct encrypt_op_buffer {
  char *buffer;
  char *pos;
  size_t remain_size;
  size_t size;
} buffer_node = {clear_buff, clear_buff, CLEAR_BUFFER_SIZE, 0};

int read_dump_encrypt_key(const char *key_file, char *key, int max_size) {
  int c;
  int size = 0;

  FILE *file = fopen(key_file, "r");
  if (file == nullptr) {
    return 0;
  }

  c = fgetc(file);
  while (c != EOF) {
    ++size;
    if (c == '\0' || size > max_size) {
      fclose(file);
      return 0;
    }
    key[size - 1] = c;
    c = fgetc(file);
  }
  fclose(file);

  key[size] = 0;
  return size;
}

int encrypt_and_write(FILE *file, const char *source, size_t length,
                      my_aes_opmode mode, char *key, char *iv) {
  size_t cp_len = 0;
  int enc_len = 0;
  while (length > 0) {
    cp_len =
        length < buffer_node.remain_size ? length : buffer_node.remain_size;
    memcpy(buffer_node.pos, source, cp_len);
    buffer_node.pos += cp_len;
    buffer_node.size += cp_len;
    buffer_node.remain_size -= cp_len;
    length -= cp_len;
    if (length > 0) source += cp_len;
    if (buffer_node.remain_size == 0) {
      enc_len =
          my_aes_encrypt(reinterpret_cast<unsigned char *>(buffer_node.buffer),
                         (uint32)buffer_node.size,
                         reinterpret_cast<unsigned char *>(cipher_buff),
                         reinterpret_cast<unsigned char *>(key), strlen(key),
                         mode, reinterpret_cast<unsigned char *>(iv), true);
      if (enc_len == MY_AES_BAD_DATA ||
          fwrite(cipher_buff, enc_len, 1, file) != 1)
        return 1;

      buffer_node.pos = buffer_node.buffer;
      buffer_node.remain_size = CLEAR_BUFFER_SIZE;
      buffer_node.size = 0;
    }
  }
  return 0;
}

int flush_encrypt_buffer(FILE *file, my_aes_opmode mode, char *key, char *iv) {
  if (buffer_node.size > 0) {
    int enc_len =
        my_aes_encrypt(reinterpret_cast<unsigned char *>(buffer_node.buffer),
                       (uint32)buffer_node.size,
                       reinterpret_cast<unsigned char *>(cipher_buff),
                       reinterpret_cast<unsigned char *>(key), strlen(key),
                       mode, reinterpret_cast<unsigned char *>(iv), true);
    if (enc_len == MY_AES_BAD_DATA ||
        fwrite(cipher_buff, enc_len, 1, file) != 1)
      return 1;
  }
  return 0;
}

int decrypt_buffer(char *source, uint32 length, char *dest, my_aes_opmode mode,
                   char *key, char *iv) {
  int dec_len =
      my_aes_decrypt(reinterpret_cast<unsigned char *>(source), length,
                     reinterpret_cast<unsigned char *>(dest),
                     reinterpret_cast<unsigned char *>(key), strlen(key), mode,
                     reinterpret_cast<unsigned char *>(iv), true);

  return dec_len;
}

int decrypt_dump_file(FILE *file, const char *dump_file, my_aes_opmode mode,
                      char *key, char *iv) {
  int dec_len = 0;
  int cipher_len = 0;

  FILE *dump = fopen(dump_file, "r");
  if (dump == nullptr) {
    fprintf(stderr, "Error: Fail to open decrypt dump file.\n");
    return 1;
  }

  cipher_len = fread(cipher_buff, 1, CIPHER_BUFFER_SIZE, dump);

  while (cipher_len > 0) {
    dec_len = my_aes_decrypt(
        reinterpret_cast<unsigned char *>(cipher_buff), cipher_len,
        reinterpret_cast<unsigned char *>(clear_buff),
        reinterpret_cast<unsigned char *>(key), strlen(key), mode,
        reinterpret_cast<unsigned char *>(iv), true);
    if (dec_len == MY_AES_BAD_DATA ||
        fwrite(clear_buff, dec_len, 1, file) != 1) {
      fclose(dump);
      return 1;
    }
    cipher_len = fread(cipher_buff, 1, CIPHER_BUFFER_SIZE, dump);
  }

  if (cipher_len < 0) {
    fclose(dump);
    return 1;
  }

  fclose(dump);
  return 0;
}
