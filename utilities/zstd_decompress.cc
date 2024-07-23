/* Copyright (c) 2024, GreatDB Software Co., Ltd.
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

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <zstd.h>

#include "my_sys.h"
#include "print_version.h"
#include "welcome_copyright_notice.h"

/*! CHECK
 * Check that the condition holds. If it doesn't print a message and die.
 */
#define CHECK(cond, ...)                                                      \
  do {                                                                        \
    if (!(cond)) {                                                            \
      fprintf(stderr, "%s:%d CHECK(%s) failed: ", __FILE__, __LINE__, #cond); \
      fprintf(stderr, "" __VA_ARGS__);                                        \
      fprintf(stderr, "\n");                                                  \
      exit(1);                                                                \
    }                                                                         \
  } while (0)

/*! CHECK_ZSTD
 * Check the zstd error code and die if an error occurred after printing a
 * message.
 */
#define CHECK_ZSTD(fn)                                       \
  do {                                                       \
    size_t const err = (fn);                                 \
    CHECK(!ZSTD_isError(err), "%s", ZSTD_getErrorName(err)); \
  } while (0)

static void decompressFile_orDie(const char *in_filename,
                                 const char *out_filename) {
  FILE *const input_file = fopen(in_filename, "rb");
  FILE *const output_file = fopen(out_filename, "wb");
  if (input_file == nullptr) {
    fprintf(stderr,
            "zstd_decompress: [Error] Cannot open input file for reading.\n");
    exit(1);
  }
  if (output_file == nullptr) {
    fprintf(stderr, "zstd_decompress: [Error] Cannot create output file.\n");
    exit(1);
  }

  size_t const buffInSize = ZSTD_DStreamInSize();
  void *const buffIn = malloc(buffInSize);
  if (buffIn == nullptr) {
    fprintf(stderr, "zstd_decompress: [Error] malloc size: %zu.\n", buffInSize);
    exit(1);
  }

  size_t const buffOutSize =
      ZSTD_DStreamOutSize(); /* Guarantee to successfully flush at least one
                                complete compressed block in all circumstances.
                              */
  void *const buffOut = malloc(buffOutSize);
  if (buffOut == nullptr) {
    fprintf(stderr, "zstd_decompress: [Error] malloc size: %zu.\n", buffInSize);
    exit(1);
  }

  ZSTD_DCtx *const dctx = ZSTD_createDCtx();
  CHECK(dctx != NULL, "ZSTD_createDCtx() failed!");

  /* This loop assumes that the input file is one or more concatenated zstd
   * streams. This example won't work if there is trailing non-zstd data at
   * the end, but streaming decompression in general handles this case.
   * ZSTD_decompressStream() returns 0 exactly when the frame is completed,
   * and doesn't consume input after the frame.
   */
  size_t const toRead = buffInSize;
  size_t read;
  size_t lastRet = 0;
  int isEmpty = 1;
  bool is_eof = false;
  while (!is_eof) {
    read = fread(buffIn, 1, toRead, input_file);
    if (read < toRead) {
      is_eof = feof(input_file);
      if (!is_eof) {
        fprintf(stderr,
                "zstd_decompress: [Error] Encountered problem during reading "
                "input.\n");
        exit(1);
      }
    }

    isEmpty = 0;
    ZSTD_inBuffer input = {buffIn, read, 0};
    /* Given a valid frame, zstd won't consume the last byte of the frame
     * until it has flushed all of the decompressed data of the frame.
     * Therefore, instead of checking if the return code is 0, we can
     * decompress just check if input.pos < input.size.
     */
    while (input.pos < input.size) {
      ZSTD_outBuffer output = {buffOut, buffOutSize, 0};
      /* The return code is zero if the frame is complete, but there may
       * be multiple frames concatenated together. Zstd will automatically
       * reset the context when a frame is complete. Still, calling
       * ZSTD_DCtx_reset() can be useful to reset the context to a clean
       * state, for instance if the last decompression call returned an
       * error.
       */
      size_t const ret = ZSTD_decompressStream(dctx, &output, &input);
      CHECK_ZSTD(ret);

      if (fwrite(buffOut, 1, output.pos, output_file) != output.pos) {
        fprintf(stderr,
                "zstd_decompress: [Error] Encountered problem during "
                "writing to buffer.\n");
        exit(1);
      }
      lastRet = ret;
    }
  }

  if (isEmpty) {
    fprintf(stderr, "input is empty\n");
    exit(1);
  }

  if (lastRet != 0) {
    /* The last return value from ZSTD_decompressStream did not end on a
     * frame, but we reached the end of the file! We assume this is an
     * error, and the input was truncated.
     */
    fprintf(stderr, "EOF before end of stream: %zu\n", lastRet);
    exit(1);
  }

  ZSTD_freeDCtx(dctx);
  fclose(input_file);
  fclose(output_file);
  free(buffIn);
  free(buffOut);
}

static void usage() {
  print_version();
  puts(ORACLE_WELCOME_COPYRIGHT_NOTICE("2015"));
  puts(
      "Decompress data compressed by clone using zstd compression "
      "algorithm by reading from input file and writing uncompressed "
      "data to output file");
  printf("Usage: %s input_file output_file\n", "zstd_decompress");
}

int main(int argc, const char **argv) {
  MY_INIT(argv[0]);
  if (argc != 3) {
    usage();
    exit(1);
  }

  const char *const inFilename = argv[1];
  const char *const outFilename = argv[2];

  printf("decompress file: %s \n", inFilename);
  decompressFile_orDie(inFilename, outFilename);
  return 0;
}
