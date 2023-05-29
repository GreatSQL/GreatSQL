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

#ifndef CLIENT_MYSQL_DUMP_ENCRYPT_INCLUDED
#define CLIENT_MYSQL_DUMP_ENCRYPT_INCLUDED

#include "my_aes.h"
#include "typelib.h"

#include <cstdio>

#define array_elements(A) ((size_t)(sizeof(A) / sizeof(A[0])))

#define MAX_ENCRYPT_KEY_SIZE 256
#define DUMP_ENCRYPT_MODE_SIZE 32

#ifndef NDEBUG
#define ENCRYPT_BATCH_SIZE (16 * 1024)
#else
#define ENCRYPT_BATCH_SIZE (16 * 1024 * 1024)
#endif

#define CLEAR_BUFFER_SIZE ENCRYPT_BATCH_SIZE
#define CIPHER_BUFFER_SIZE (CLEAR_BUFFER_SIZE + MY_AES_BLOCK_SIZE)

int decrypt_buffer(char *source, uint32 length, char *dest, my_aes_opmode mode,
                   char *key, char *iv);

int decrypt_dump_file(FILE *file, const char *dump_file, my_aes_opmode mode,
                      char *key, char *iv);
int read_dump_encrypt_key(const char *key_file, char *key, int max_size);
int encrypt_and_write(FILE *file, const char *source, size_t length,
                      my_aes_opmode mode, char *key, char *iv);
int flush_encrypt_buffer(FILE *file, my_aes_opmode mode, char *key, char *iv);

#endif
