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
#include "sql/clone_compress.h"
#include "mysqld_error.h"
#include "sql/clone_handler.h"

static inline int write_uint32_le(int file, uint32_t n) {
  char tmp[4];
  int4store(tmp, n);
  return clone_os_copy_buf_to_file((uchar *)tmp, file, sizeof(tmp), "");
}

int Zstd_compress_file::compress_and_write(char *buf, int to_file, size_t len,
                                           const char *dest_name) {
  /* make sure we have enough memory for compression */
  const size_t comp_size = ZSTD_CStreamOutSize();
  size_t n_chunks = (len / comp_size * comp_size == len)
                        ? (len / comp_size)
                        : (len / comp_size + 1);
  /* empty file */
  if (n_chunks == 0) n_chunks = 1;
  const size_t comp_buf_size = comp_size * n_chunks;
  if (m_comp_buf_size < comp_buf_size) {
    m_comp_buf = static_cast<char *>(
        my_realloc(PSI_NOT_INSTRUMENTED, m_comp_buf, comp_buf_size,
                   MYF(MY_FAE | MY_ALLOW_ZERO_PTR)));
    if (m_comp_buf == nullptr) {
      my_error(ER_ERROR_ON_WRITE, MYF(0), dest_name, errno,
               "compress: realloc failed");
    }
    m_comp_buf_size = comp_buf_size;
  }

  size_t read_chunk_size = ZSTD_CStreamInSize();
  if (read_chunk_size > len) read_chunk_size = len;
  bool last_chunk = false;
  while (1) {
    if (len - m_raw_bytes <= read_chunk_size) {
      read_chunk_size = len - m_raw_bytes;
      last_chunk = true;
    }
    if (m_comp_bytes > 0 && m_comp_bytes + comp_size > comp_buf_size) {
      /* buffer full */
      if (clone_os_copy_buf_to_file((uchar *)m_comp_buf, to_file, m_comp_bytes,
                                    dest_name)) {
        goto err;
      }
      m_comp_bytes = 0;
    }
    ZSTD_EndDirective const mode = last_chunk ? ZSTD_e_end : ZSTD_e_continue;
    ZSTD_inBuffer input = {((const char *)buf + m_raw_bytes), read_chunk_size,
                           0};
    bool finished = false;
    ZSTD_outBuffer output = {m_comp_buf + m_comp_bytes, comp_size, 0};
    size_t const remaining =
        ZSTD_compressStream2(m_cctx, &output, &input, mode);

    if (ZSTD_isError(remaining)) goto err;

    m_raw_bytes += input.pos;
    m_comp_bytes += output.pos;
    finished = last_chunk ? (remaining == 0) : false;

    if (finished) {
      break;
    }
  }
  /* last write */
  if (clone_os_copy_buf_to_file((uchar *)m_comp_buf, to_file, m_comp_bytes,
                                dest_name)) {
    goto err;
  }
  m_raw_bytes = 0;
  m_comp_bytes = 0;
  return 0;

err:
  my_error(ER_ERROR_ON_WRITE, MYF(0), dest_name, errno,
           "compress: write to the destination file failed");
  return 1;
}

int Lz4_compress_file::compress_and_write(char *buf, int dest_file, size_t len,
                                          const char *dest_name) {
  size_t compress_chunk_size = (size_t)m_chunk_size;
  /* make sure we have enough memory for compression */
  const size_t comp_size = LZ4_compressBound(compress_chunk_size);
  const size_t n_chunks =
      (len / compress_chunk_size * compress_chunk_size == len)
          ? (len / compress_chunk_size)
          : ((len / compress_chunk_size) + 1);
  const size_t comp_buf_size = comp_size * n_chunks;
  if (m_comp_buf_size < comp_buf_size) {
    m_comp_buf = static_cast<char *>(
        my_realloc(PSI_NOT_INSTRUMENTED, m_comp_buf, comp_buf_size,
                   MYF(MY_FAE | MY_ALLOW_ZERO_PTR)));
    if (m_comp_buf == nullptr) {
      my_error(ER_ERROR_ON_WRITE, MYF(0), dest_name, errno,
               "compress: realloc failed");
    }
    m_comp_buf_size = comp_buf_size;
  }

  /* parallel compress using trhead pool */
  if (m_tasks.size() < n_chunks) {
    m_tasks.resize(n_chunks);
  }
  if (m_contexts.size() < n_chunks) {
    m_contexts.resize(n_chunks);
  }

  for (size_t i = 0; i < n_chunks; i++) {
    size_t chunk_len = std::min((int)(len - (i * compress_chunk_size)),
                                (int)compress_chunk_size);
    auto &thd = m_contexts[i];
    thd.from = ((char *)buf) + (compress_chunk_size)*i;
    thd.from_len = chunk_len;
    thd.to_size = comp_size;
    thd.to = m_comp_buf + comp_size * i;

    m_tasks[i] = m_lz4_compress_thread->thread_pool->add_task(
        [&thd](size_t thread_id [[maybe_unused]]) {
          thd.to_len =
              LZ4_compress_default(thd.from, thd.to, thd.from_len, thd.to_size);
        });
  }

  /* while compression is in progress, calculate content checksum */
  const uint32_t checksum = MY_XXH32(buf, len, 0);

  /* write LZ4 frame */

  /* Frame header (4 bytes magic, 1 byte FLG, 1 byte BD,
     8 bytes uncompressed content size, 1 byte HC) */
  uint8_t header[15];

  /* Magic Number */
  int4store(header, LZ4F_MAGICNUMBER);

  /* FLG Byte */
  const uint8_t flg =
      (1 << 6) | /* version = 01 */
      (1 << 5) | /* block independence (1 means blocks are independent) */
      (0 << 4) | /* block checksum (0 means no block checksum, we rely on
                    xbstream checksums) */
      (1 << 3) | /* content size (include uncompressed content size) */
      (1 << 2) | /* content checksum (include checksum of uncompressed data) */
      (0 << 1) | /* reserved */
      (0 << 0) /* dict id */;
  header[4] = flg;

  /* BD Byte (set maximum uncompressed block size to 4M) */
  uint8_t max_block_size_code = 0;
  if (compress_chunk_size <= 64 * 1024) {
    max_block_size_code = 4;
  } else if (compress_chunk_size <= 256 * 1024) {
    max_block_size_code = 5;
  } else if (compress_chunk_size <= 1 * 1204 * 1024) {
    max_block_size_code = 6;
  } else if (compress_chunk_size <= 4 * 1024 * 1024) {
    max_block_size_code = 7;
  } else {
    // msg("compress: compress chunk size is too large for LZ4 compressor.\n");
    return 1;
  }
  const uint8_t bd = (max_block_size_code << 4);
  header[5] = bd;

  /* uncompressed content size */
  int8store(header + 6, len);

  /* HC Byte */
  header[14] = (MY_XXH32(header + 4, 10, 0) >> 8) & 0xff;

  bool error = false;

  /* write frame header */
  if (clone_os_copy_buf_to_file((uchar *)header, dest_file, sizeof(header),
                                dest_name)) {
    error = true;
  }

  /* write compressed blocks */
  for (size_t i = 0; i < n_chunks; i++) {
    const auto &thd = m_contexts[i];

    /* reap */
    m_tasks[i].wait();

    if (error) continue;

    /* Compressing encrypted or already compressed
    data the length of compression should exceed, in such case
    skip the compression */
    if (thd.to_len > 0 && thd.to_len < compress_chunk_size) {
      /* compressed block length */
      if (write_uint32_le(dest_file, thd.to_len)) {
        error = true;
        continue;
      }

      /* block contents */
      if (clone_os_copy_buf_to_file((uchar *)thd.to, dest_file, thd.to_len,
                                    dest_name)) {
        error = true;
      }
    } else {
      /* uncompressed block length */
      if (write_uint32_le(dest_file, thd.from_len | LZ4F_UNCOMPRESSED_BIT)) {
        error = true;
        continue;
      }

      /* block contents */
      if (clone_os_copy_buf_to_file((uchar *)thd.from, dest_file, thd.from_len,
                                    dest_name)) {
        error = true;
        continue;
      }
    }

    m_bytes_processed += thd.from_len;
  }

  if (error) goto err;

  /* LZ4 frame trailer */

  /* empty mark is zero-sized block */
  if (write_uint32_le(dest_file, 0)) {
    goto err;
  }

  /* content checksum */
  if (write_uint32_le(dest_file, checksum)) {
    goto err;
  }

  return 0;

err:
  my_error(ER_ERROR_ON_WRITE, MYF(0), dest_name, errno,
           "compress: write to the destination file failed");
  return 1;
}
