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

#ifndef CLONE_COMPRESS_H
#define CLONE_COMPRESS_H

#include <lz4.h>
#include <my_base.h>
#include <my_byteorder.h>
#include <my_io.h>
#include <mysql/service_mysql_alloc.h>
#include <mysql_version.h>
#include "clone_thread_pool.h"
#define ZSTD_STATIC_LINKING_ONLY  // Thread pool
#include <zstd.h>
#include "extra/lz4/my_xxhash.h"
#include "my_dbug.h"
#include "my_sys.h"
#include "mysys_err.h"

#define LZ4F_MAGICNUMBER 0x184d2204U
#define LZ4F_UNCOMPRESSED_BIT (1U << 31)

enum file_compress_mode_t : unsigned int {
  CLONE_FILE_COMPRESS_NONE,
  CLONE_FILE_COMPRESS_LZ4,
  CLONE_FILE_COMPRESS_ZSTD
};

typedef struct {
  ZSTD_threadPool *thread_pool;
} zstd_thread_pool;

typedef struct {
  char *from;
  size_t from_len;
  char *to;
  size_t to_len;
  size_t to_size;
} comp_thread_ctxt_t;

typedef struct {
  Thread_pool *thread_pool;
} lz4_thread_pool;

typedef struct {
  zstd_thread_pool *zstd_compress_thread;
  lz4_thread_pool *lz4_compress_thread;
} compress_thread_pool;

/* Compression options */
extern char *xtrabackup_compress_alg;
extern uint xtrabackup_compress_threads;

class Compress_file {
 public:
  /** Constructor to initialize members. */
  Compress_file(uint compress_threads, file_compress_mode_t compress_mode)
      : m_compress_threads(compress_threads), m_compress_mode(compress_mode) {
    m_comp_buf = nullptr;
    m_comp_buf_size = 0;
  }

 public:
  virtual ~Compress_file() = default;
  /** Init compress thread pool.
  @param[in]  compress_threads        the number of compress threads
  @return compress_thread_pool */
  virtual void init_compress_thread_pool() = 0;
  virtual void deinit_compress_thread_pool() = 0;

  virtual void compress_init() = 0;
  virtual void compress_deinit() = 0;

  /** Init compress and write file
  @param[in]  buf  data buffer
  @param[in]  len  data length
  @param[in]  dest_file dst file descriptor
  @param[in]  dest_name dst file name
  @return error code  */
  virtual int compress_and_write(char *buf, int to_file, size_t len,
                                 const char *dest_name) = 0;

 protected:
  uint m_compress_threads;
  file_compress_mode_t m_compress_mode;
  char *m_comp_buf;
  size_t m_comp_buf_size;
};

class Zstd_compress_file : public Compress_file {
 public:
  /** Constructor to initialize members. */
  Zstd_compress_file(uint compress_threads, uint zstd_level)
      : Compress_file(compress_threads, CLONE_FILE_COMPRESS_ZSTD) {
    m_zstd_level = zstd_level;
    m_raw_bytes = 0;
    m_comp_bytes = 0;
    m_cctx = nullptr;
    m_zstd_compress_thread = nullptr;
  }

  ~Zstd_compress_file() { compress_deinit(); }

  /** Init compress thread pool.
  @param[in]  compress_threads        the number of compress threads
  @return compress_thread_pool */
  void init_compress_thread_pool() override {
    m_zstd_compress_thread = new zstd_thread_pool;
    m_zstd_compress_thread->thread_pool =
        ZSTD_createThreadPool(m_compress_threads);
  }

  void deinit_compress_thread_pool() override {
    if (m_zstd_compress_thread && m_zstd_compress_thread->thread_pool) {
      ZSTD_freeThreadPool(m_zstd_compress_thread->thread_pool);
    }
  }

  void compress_init() override {
    m_cctx = ZSTD_createCCtx();
    ZSTD_CCtx_refThreadPool(m_cctx, m_zstd_compress_thread->thread_pool);
    ZSTD_CCtx_setParameter(m_cctx, ZSTD_c_compressionLevel, m_zstd_level);
    ZSTD_CCtx_setParameter(m_cctx, ZSTD_c_nbWorkers, m_compress_threads);
    ZSTD_CCtx_setParameter(m_cctx, ZSTD_c_checksumFlag, 1);
  }

  void compress_deinit() override {
    if (m_cctx) {
      ZSTD_freeCCtx(m_cctx);
    }
    if (m_comp_buf) {
      my_free(m_comp_buf);
    }
  }

  zstd_thread_pool *get_thread_pool() { return m_zstd_compress_thread; }

  void set_thread_pool(zstd_thread_pool *thread_pool) {
    m_zstd_compress_thread = thread_pool;
  }

  /** Init compress and write file
  @param[in]  buf  data buffer
  @param[in]  len  data length
  @param[in]  to_file dst file descriptor
  @param[in]  dest_name dst file name
  @return error code  */
  int compress_and_write(char *buf, int to_file, size_t len,
                         const char *dest_name) override;

 private:
  uint m_zstd_level;
  size_t m_raw_bytes;
  size_t m_comp_bytes;
  ZSTD_CCtx *m_cctx;
  zstd_thread_pool *m_zstd_compress_thread;
};

class Lz4_compress_file : public Compress_file {
 public:
  /** Constructor to initialize members. */
  Lz4_compress_file(uint compress_threads, uint chunk_size)
      : Compress_file(compress_threads, CLONE_FILE_COMPRESS_LZ4),
        m_chunk_size(chunk_size) {
    m_bytes_processed = 0;
    m_lz4_compress_thread = nullptr;
  }
  ~Lz4_compress_file() { compress_deinit(); }

 public:
  /** Init compress thread pool.
  @param[in]  compress_threads        the number of compress threads
  @return compress_thread_pool */
  void init_compress_thread_pool() override {
    m_lz4_compress_thread = new lz4_thread_pool;
    m_lz4_compress_thread->thread_pool = new Thread_pool(m_compress_threads);
  }
  void deinit_compress_thread_pool() override {
    if (m_lz4_compress_thread && m_lz4_compress_thread->thread_pool) {
      delete (m_lz4_compress_thread->thread_pool);
    }
  }
  void compress_init() override { return; }
  void compress_deinit() override {
    if (m_comp_buf) {
      my_free(m_comp_buf);
    }
  }
  lz4_thread_pool *get_thread_pool() { return m_lz4_compress_thread; }

  void set_thread_pool(lz4_thread_pool *thread_pool) {
    m_lz4_compress_thread = thread_pool;
  }
  /** Init compress and write file
  @param[in]  buf  data buffer
  @param[in]  len  data length
  @param[in]  to_file dst file descriptor
  @param[in]  dest_name dst file name
  @return error code  */
  int compress_and_write(char *buf, int to_file, size_t len,
                         const char *dest_name) override;

 private:
  uint m_chunk_size;
  size_t m_bytes_processed;
  lz4_thread_pool *m_lz4_compress_thread;
  std::vector<std::future<void>> m_tasks;
  std::vector<comp_thread_ctxt_t> m_contexts;
};

#endif /* CLONE_COMPRESS_H */
