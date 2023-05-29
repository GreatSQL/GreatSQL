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
#ifndef CLIENT_MYSQL_CLONE_ENCRYPT_INCLUDED
#define CLIENT_MYSQL_CLONE_ENCRYPT_INCLUDED

#include "my_aes.h"
#include "typelib.h"

#include <cstdio>

#define MAX_ENCRYPT_KEY_SIZE 256
#define CLONE_ENCRYPT_MODE_SIZE 32
#define ENCRYPT_BATCH_SIZE (512)
#define REMAIN_BLOCK_SIZE (2 * MY_AES_BLOCK_SIZE)
#define ENCRYPT_BUFFER_SIZE (ENCRYPT_BATCH_SIZE + REMAIN_BLOCK_SIZE)

struct encrypt_op_buffer {
  char *buffer;
  char *pos;
  uint size;
  uint remain_size;
};

/* Key file has 3 lines.
 * First line: aes_opmode, max_size 32
 * Second line: encrypt key, max_size 256
 * Third line: aes_ebc encrypt iv, size 16
 */
int read_clone_encrypt_key(const char *opt_clone_encrypt_key,
                           my_aes_opmode *clone_encrypt_mode,
                           char **clone_encrypt_key, char **clone_encrypt_iv);

int clone_decrypt_buffer(char *source, uint32 length, char *dest,
                         my_aes_opmode clone_encrypt_mode,
                         char *clone_encrypt_key, char *clone_encrypt_iv);

int encrypt_buffer(unsigned char *source, uint32 length, char *dest,
                   my_aes_opmode dump_encrypt_mode, char *dump_encrypt_key,
                   char *dump_encrypt_iv);

int decrypt_clone_file(FILE *file, const char *clone_file,
                       my_aes_opmode clone_encrypt_mode,
                       char *clone_encrypt_key, char *clone_encrypt_iv);
#endif