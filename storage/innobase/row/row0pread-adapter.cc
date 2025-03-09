/*****************************************************************************

Copyright (c) 2018, 2021, Oracle and/or its affiliates.
Copyright (c) 2022, Huawei Technologies Co., Ltd.
Copyright (c) 2023, 2025, GreatDB Software Co., Ltd.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
limited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/

/** @file row/row0pread-adapter.cc
Parallel read adapter interface implementation

Created 2018-02-28 by Darshan M N */

#ifndef UNIV_HOTBACKUP

#include "row0pread-adapter.h"
#include "row0pread-fetcher.h"
#include "row0sel.h"
#include "sql/item.h"
#include "univ.i"

Parallel_reader_adapter::Parallel_reader_adapter(size_t max_threads,
                                                 ulint rowlen)
    : m_parallel_reader(max_threads) {
  m_max_read_buf_size = ADAPTER_SEND_BUFFER_SIZE;
  m_batch_size = ADAPTER_SEND_BUFFER_SIZE / rowlen;
}

Parallel_reader_adapter::Parallel_reader_adapter(size_t max_threads,
                                                 ulint rowlen,
                                                 ulint max_read_buf_size)
    : m_parallel_reader(max_threads) {
  m_max_read_buf_size = max_read_buf_size;
  m_batch_size = m_max_read_buf_size / rowlen;
}

Parallel_reader_adapter::~Parallel_reader_adapter() {
  if (m_key_start) {
    ut::delete_arr(m_key_start);
    m_key_start = nullptr;
    m_key_start_len = 0;
  }

  if (m_key_end) {
    ut::delete_arr(m_key_end);
    m_key_end = nullptr;
    m_key_end_len = 0;
  }

  if (m_range_heap) {
    mem_heap_free(m_range_heap);
  }
}

dberr_t Parallel_reader_adapter::add_scan(trx_t *trx,
                                          const Parallel_reader::Config &config,
                                          Parallel_reader::F &&f, bool split) {
  return m_parallel_reader.add_scan(trx, config, std::move(f), split);
}

Parallel_reader_adapter::Thread_ctx::Thread_ctx() {
  m_buffer.resize(ADAPTER_SEND_BUFFER_SIZE);
}

void Parallel_reader_adapter::set(row_prebuilt_t *prebuilt) {
  ut_a(prebuilt->n_template > 0);
  ut_a(m_mysql_row.m_offsets.empty());
  ut_a(m_mysql_row.m_null_bit_mask.empty());
  ut_a(m_mysql_row.m_null_bit_offsets.empty());

  /* Partition structure should be the same across all partitions.
  Therefore MySQL row meta-data is common across all paritions. */

  for (uint i = 0; i < prebuilt->n_template; ++i) {
    const auto &templt = prebuilt->mysql_template[i];

    m_mysql_row.m_offsets.push_back(
        static_cast<ulong>(templt.mysql_col_offset));
    m_mysql_row.m_null_bit_mask.push_back(
        static_cast<ulong>(templt.mysql_null_bit_mask));
    m_mysql_row.m_null_bit_offsets.push_back(
        static_cast<ulong>(templt.mysql_null_byte_offset));
  }

  ut_a(m_mysql_row.m_max_len == 0);
  ut_a(prebuilt->mysql_row_len > 0);
  m_mysql_row.m_max_len = static_cast<ulong>(prebuilt->mysql_row_len);

  m_parallel_reader.set_start_callback(
      [=](Parallel_reader::Thread_ctx *reader_thread_ctx) {
        if (reader_thread_ctx->get_state() == Parallel_reader::State::THREAD) {
          return init(reader_thread_ctx, prebuilt);
        } else {
          return DB_SUCCESS;
        }
      });

  m_parallel_reader.set_finish_callback(
      [=](Parallel_reader::Thread_ctx *reader_thread_ctx) {
        if (reader_thread_ctx->get_state() == Parallel_reader::State::THREAD) {
          return end(reader_thread_ctx);
        } else {
          return DB_SUCCESS;
        }
      });

  ut_a(m_prebuilt == nullptr);
  m_prebuilt = prebuilt;
}

dberr_t Parallel_reader_adapter::run(void **thread_ctxs, Init_fn init_fn,
                                     Load_fn load_fn, End_fn end_fn) {
  m_end_fn = end_fn;
  m_init_fn = init_fn;
  m_load_fn = load_fn;
  m_thread_ctxs = thread_ctxs;

  m_parallel_reader.set_n_threads(m_parallel_reader.max_threads());

  return m_parallel_reader.run(m_parallel_reader.max_threads());
}

dberr_t Parallel_reader_adapter::async_run(void **thread_ctxs, Init_fn init_fn,
                                           Load_fn load_fn, End_fn end_fn) {
  m_end_fn = end_fn;
  m_init_fn = init_fn;
  m_load_fn = load_fn;
  m_thread_ctxs = thread_ctxs;

  m_parallel_reader.set_n_threads(m_parallel_reader.max_threads());

  return m_parallel_reader.async_run(m_parallel_reader.max_threads());
}

dberr_t Parallel_reader_adapter::init(
    Parallel_reader::Thread_ctx *reader_thread_ctx, row_prebuilt_t *prebuilt) {
  auto thread_ctx =
      ut::new_withkey<Thread_ctx>(ut::make_psi_memory_key(mem_key_archive));

  if (thread_ctx == nullptr) {
    return DB_OUT_OF_MEMORY;
  }

  reader_thread_ctx->set_callback_ctx<Thread_ctx>(thread_ctx);

  /** There are data members in row_prebuilt_t that cannot be accessed in
  multi-threaded mode e.g., blob_heap.

  row_prebuilt_t is designed for single threaded access and to share
  it among threads is not recommended unless "you know what you are doing".
  This is very fragile code as it stands.

  To solve the blob heap issue in prebuilt we request parallel reader thread to
  use blob heap per thread and we pass this blob heap to the InnoDB to MySQL
  row format conversion function. */
  if (prebuilt->templ_contains_blob) {
    reader_thread_ctx->create_blob_heap();
  }

  auto ret = m_init_fn(m_thread_ctxs[reader_thread_ctx->m_thread_id],
                       static_cast<ulong>(m_mysql_row.m_offsets.size()),
                       m_mysql_row.m_max_len, &m_mysql_row.m_offsets[0],
                       &m_mysql_row.m_null_bit_offsets[0],
                       &m_mysql_row.m_null_bit_mask[0]);

  return (ret ? DB_INTERRUPTED : DB_SUCCESS);
}

dberr_t Parallel_reader_adapter::send_batch(
    Parallel_reader::Thread_ctx *reader_thread_ctx, size_t partition_id,
    uint64_t n_recs) {
  auto ctx = reader_thread_ctx->get_callback_ctx<Thread_ctx>();
  const auto thread_id = reader_thread_ctx->m_thread_id;

  const auto start = ctx->m_n_sent % m_batch_size;

  ut_a(n_recs <= m_batch_size);
  ut_a(start + n_recs <= m_batch_size);

  const auto rec_loc = &ctx->m_buffer[start * m_mysql_row.m_max_len];

  dberr_t err{DB_SUCCESS};

  if (m_load_fn(m_thread_ctxs[thread_id], n_recs, rec_loc, partition_id)) {
    err = DB_INTERRUPTED;
    m_parallel_reader.set_error_state(DB_INTERRUPTED);
  }

  ctx->m_n_sent += n_recs;

  return err;
}

dberr_t Parallel_reader_adapter::process_rows(
    const Parallel_reader::Ctx *reader_ctx) {
  auto reader_thread_ctx = reader_ctx->thread_ctx();
  auto ctx = reader_thread_ctx->get_callback_ctx<Thread_ctx>();
  auto blob_heap = reader_thread_ctx->m_blob_heap;

  ut_a(ctx->m_n_read >= ctx->m_n_sent);
  ut_a(ctx->m_n_read - ctx->m_n_sent <= m_batch_size);

  dberr_t err{DB_SUCCESS};

  {
    auto n_pending = pending(ctx);

    /* Start of a new range, send what we have buffered. */
    if ((reader_ctx->m_start && n_pending > 0) || is_buffer_full(ctx)) {
      err = send_batch(reader_thread_ctx, ctx->m_partition_id, n_pending);

      if (err != DB_SUCCESS) {
        return (err);
      }

      /* Empty the heap for the next batch */
      if (blob_heap != nullptr) {
        mem_heap_empty(blob_heap);
      }
    }
  }

  mem_heap_t *heap{};
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets = offsets_;

  rec_offs_init(offsets_);

  offsets = rec_get_offsets(reader_ctx->m_rec, reader_ctx->index(), offsets,
                            ULINT_UNDEFINED, UT_LOCATION_HERE, &heap);

  const auto next_rec = ctx->m_n_read % m_batch_size;

  const auto buffer_loc = &ctx->m_buffer[0] + next_rec * m_mysql_row.m_max_len;

  if (row_sel_store_mysql_rec(buffer_loc, m_prebuilt, reader_ctx->m_rec,
                              nullptr, true, reader_ctx->index(),
                              reader_ctx->index(), offsets, false, nullptr,
                              blob_heap)) {
    /* If there is any pending records, then we should not overwrite the
    partition ID with a different one. */
    if (pending(ctx) && ctx->m_partition_id != reader_ctx->partition_id()) {
      err = DB_ERROR;
      ut_d(ut_error);
    } else {
      ++ctx->m_n_read;
      ctx->m_partition_id = reader_ctx->partition_id();
    }

    if (m_parallel_reader.is_error_set()) {
      /* Simply skip sending the records to RAPID in case of an error in the
      parallel reader and return DB_ERROR as the error could have been
      originated from RAPID threads. */
      err = DB_ERROR;
    }
  } else {
    err = DB_ERROR;
  }

  if (heap != nullptr) {
    mem_heap_free(heap);
  }

  return (err);
}

dberr_t Parallel_reader_adapter::end(
    Parallel_reader::Thread_ctx *reader_thread_ctx) {
  dberr_t err{DB_SUCCESS};

  auto thread_id = reader_thread_ctx->m_thread_id;
  auto thread_ctx = reader_thread_ctx->get_callback_ctx<Thread_ctx>();

  ut_a(thread_ctx->m_n_sent <= thread_ctx->m_n_read);
  ut_a(thread_ctx->m_n_read - thread_ctx->m_n_sent <= m_batch_size);

  if (!m_parallel_reader.is_error_set()) {
    /* It's possible that we might not have sent the records in the buffer
    when we have reached the end of records and the buffer is not full.
    Send them now. */
    size_t n_pending = pending(thread_ctx);

    if (n_pending != 0) {
      err =
          send_batch(reader_thread_ctx, thread_ctx->m_partition_id, n_pending);
    }
  }

  m_end_fn(m_thread_ctxs[thread_id]);

  ut::delete_(thread_ctx);
  reader_thread_ctx->set_callback_ctx<Thread_ctx>(nullptr);

  return (err);
}

void Parallel_reader_adapter::build_scan_range(row_prebuilt_t *prebuilt,
                                               unsigned char *key_start,
                                               uint key_start_len,
                                               unsigned char *key_end,
                                               uint key_end_len) {
  ut_a(nullptr == m_key_start && 0 == m_key_start_len);
  ut_a(nullptr == m_key_end && 0 == m_key_end_len);

  /**
   * 1. we keep the key_start/end value in our private buffer
   */
  if (key_start) {
    m_key_start = ut::new_arr_withkey<unsigned char>(UT_NEW_THIS_FILE_PSI_KEY,
                                                     ut::Count{key_start_len});
    m_key_start_len = key_start_len;
    memcpy(m_key_start, key_start, key_start_len);
  }
  if (key_end && key_end_len) {
    m_key_end = ut::new_arr_withkey<unsigned char>(UT_NEW_THIS_FILE_PSI_KEY,
                                                   ut::Count{key_end_len});
    m_key_end_len = key_end_len;
    memcpy(m_key_end, key_end, key_end_len);
  }

  if (nullptr == m_key_start && nullptr == m_key_end) {
    return;
  }

  /** 2. create the scan range */

  dict_table_t *table = prebuilt->index->table;
  ulint max_entry_len =
      2 * (table->get_n_cols() + dict_table_get_n_v_cols(table));
  m_range_start = nullptr;
  m_range_end = nullptr;
  m_range_heap = mem_heap_create(1024, UT_LOCATION_HERE);

  byte *range_start_key_val_buffer = nullptr;
  byte *range_end_key_val_buffer = nullptr;

  if (prebuilt->srch_key_val_len) {
    range_start_key_val_buffer = static_cast<byte *>(
        mem_heap_alloc(m_range_heap, 2 * prebuilt->srch_key_val_len));

    range_end_key_val_buffer =
        (range_start_key_val_buffer + prebuilt->srch_key_val_len);
  }

  if (m_key_start) {
    m_range_start = dtuple_create(m_range_heap, max_entry_len);
    row_sel_convert_mysql_key_to_innobase(
        m_range_start, range_start_key_val_buffer, prebuilt->srch_key_val_len,
        prebuilt->index, static_cast<byte *>(m_key_start), m_key_start_len);
  }

  if (m_key_end) {
    m_range_end = dtuple_create(m_range_heap, max_entry_len);

    row_sel_convert_mysql_key_to_innobase(
        m_range_end, range_end_key_val_buffer, prebuilt->srch_key_val_len,
        prebuilt->index, static_cast<byte *>(m_key_end), m_key_end_len);
  }

  m_read_range.m_start = m_range_start;
  m_read_range.m_end = m_range_end;
}

/**********************************************************************/

Parallel_reader_async_adapter::Parallel_reader_async_adapter(size_t max_threads,
                                                             ulint rowlen)
    : m_parallel_reader(max_threads) {
  m_max_read_buf_size = ADAPTER_SEND_BUFFER_SIZE;
  m_batch_size = ADAPTER_SEND_BUFFER_SIZE / rowlen;
}

Parallel_reader_async_adapter::Parallel_reader_async_adapter(
    size_t max_threads, ulint rowlen, ulint max_read_buf_size)
    : m_parallel_reader(max_threads) {
  m_max_read_buf_size = max_read_buf_size;
  m_batch_size = m_max_read_buf_size / rowlen;
}

Parallel_reader_async_adapter::~Parallel_reader_async_adapter() {
  if (m_key_start) {
    ut::delete_arr(m_key_start);
    m_key_start = nullptr;
    m_key_start_len = 0;
  }

  if (m_key_end) {
    ut::delete_arr(m_key_end);
    m_key_end = nullptr;
    m_key_end_len = 0;
  }

  if (m_range_heap) {
    mem_heap_free(m_range_heap);
  }
}

dberr_t Parallel_reader_async_adapter::add_scan(
    trx_t *trx, const Parallel_reader::Config &config, Parallel_reader::F &&f,
    bool split) {
  return m_parallel_reader.add_scan(trx, config, std::move(f), split);
}

Parallel_reader_async_adapter::Thread_ctx::Thread_ctx(ulint max_buf_size) {
  m_buffer.resize(max_buf_size);
}

void Parallel_reader_async_adapter::set(row_prebuilt_t *prebuilt) {
  ut_a(prebuilt->n_template > 0);
  ut_a(m_mysql_row.m_offsets.empty());
  ut_a(m_mysql_row.m_null_bit_mask.empty());
  ut_a(m_mysql_row.m_null_bit_offsets.empty());
  ulint max_row_len = 0;

  is_use_cluster_index =
      (prebuilt->index == prebuilt->index->table->first_index() ? true : false);

  /* Partition structure should be the same across all partitions.
  Therefore MySQL row meta-data is common across all paritions. */
  max_row_len = fill_mysql_row_info(prebuilt);
  ut_a(m_mysql_row.m_max_len == 0);
  ut_a(prebuilt->mysql_row_len > 0);

  // if (is_use_cluster_index) {
  //   m_mysql_row.m_max_len = static_cast<ulong>(prebuilt->mysql_row_len);
  // } else
  {
    m_batch_size = m_max_read_buf_size / max_row_len;
    m_mysql_row.m_max_len = max_row_len;
  }

  m_parallel_reader.set_start_callback(
      [=](Parallel_reader::Thread_ctx *reader_thread_ctx) {
        Parallel_reader::State thread_state = reader_thread_ctx->get_state();
        if (thread_state == Parallel_reader::State::THREAD) {
          return init(reader_thread_ctx, prebuilt);
        } else if (thread_state == Parallel_reader::State::CTX) {
          return handle_ctx_start(reader_thread_ctx);
        } else {
          return DB_SUCCESS;
        }
      });

  m_parallel_reader.set_finish_callback(
      [=](Parallel_reader::Thread_ctx *reader_thread_ctx) {
        Parallel_reader::State thread_state = reader_thread_ctx->get_state();
        if (Parallel_reader::State::THREAD == thread_state) {
          return end(reader_thread_ctx);
        } else if (Parallel_reader::State::CTX == thread_state) {
          return handle_ctx_end(reader_thread_ctx);
        } else {
          return DB_SUCCESS;
        }
      });

  ut_a(m_prebuilt == nullptr);
  m_prebuilt = prebuilt;
}

dberr_t Parallel_reader_async_adapter::run(void **thread_ctxs, Init_fn init_fn,
                                           Load_fn load_fn, End_fn end_fn) {
  m_end_fn = end_fn;
  m_init_fn = init_fn;
  m_load_fn = load_fn;
  m_thread_ctxs = thread_ctxs;

  m_parallel_reader.set_n_threads(m_parallel_reader.max_threads());

  return m_parallel_reader.run(m_parallel_reader.max_threads());
}

dberr_t Parallel_reader_async_adapter::async_run(void **thread_ctxs,
                                                 Init_fn init_fn,
                                                 Load_fn load_fn,
                                                 End_fn end_fn) {
  m_end_fn = end_fn;
  m_init_fn = init_fn;
  m_load_fn = load_fn;
  m_thread_ctxs = thread_ctxs;

  m_parallel_reader.set_n_threads(m_parallel_reader.max_threads());

  return m_parallel_reader.async_run(m_parallel_reader.max_threads());
}

dberr_t Parallel_reader_async_adapter::init(
    Parallel_reader::Thread_ctx *reader_thread_ctx, row_prebuilt_t *prebuilt) {
  auto thread_ctx = ut::new_withkey<Thread_ctx>(
      ut::make_psi_memory_key(mem_key_archive), m_max_read_buf_size);

  if (thread_ctx == nullptr) {
    return DB_OUT_OF_MEMORY;
  }

  reader_thread_ctx->set_callback_ctx<Thread_ctx>(thread_ctx);

  /** There are data members in row_prebuilt_t that cannot be accessed in
  multi-threaded mode e.g., blob_heap.

  row_prebuilt_t is designed for single threaded access and to share
  it among threads is not recommended unless "you know what you are doing".
  This is very fragile code as it stands.

  To solve the blob heap issue in prebuilt we request parallel reader thread to
  use blob heap per thread and we pass this blob heap to the InnoDB to MySQL
  row format conversion function. */
  if (prebuilt->templ_contains_blob) {
    reader_thread_ctx->create_blob_heap();
  }

  /** create the compress column heap for compress blob */
  reader_thread_ctx->create_com_col_heap();

  auto ret = m_init_fn(m_thread_ctxs[reader_thread_ctx->m_thread_id],
                       static_cast<ulong>(m_mysql_row.m_offsets.size()),
                       m_mysql_row.m_max_len, &m_mysql_row.m_offsets[0],
                       &m_mysql_row.m_null_bit_offsets[0],
                       &m_mysql_row.m_null_bit_mask[0]);

  return (ret ? DB_INTERRUPTED : DB_SUCCESS);
}

dberr_t Parallel_reader_async_adapter::send_batch(
    Parallel_reader::Thread_ctx *reader_thread_ctx, size_t partition_id,
    uint64_t n_recs) {
  auto ctx = reader_thread_ctx->get_callback_ctx<Thread_ctx>();
  const auto thread_id = reader_thread_ctx->m_thread_id;

  const auto start = ctx->m_n_sent % m_batch_size;

  ut_a(n_recs <= m_batch_size);
  ut_a(start + n_recs <= m_batch_size);

  const auto rec_loc = &ctx->m_buffer[start * m_mysql_row.m_max_len];

  dberr_t err{DB_SUCCESS};

  Parallel_reader_table_fetch_handler::table_fetch_handler_thread_ctx_t
      *async_fetch_thread_ctx = static_cast<
          Parallel_reader_table_fetch_handler::table_fetch_handler_thread_ctx_t
              *>(m_thread_ctxs[thread_id]);

  async_fetch_thread_ctx->m_blob_heap = reader_thread_ctx->m_blob_heap;
  async_fetch_thread_ctx->m_compress_cols_heap =
      reader_thread_ctx->get_row_built_para_ctx()->get_compress_col_heap();

  if (n_recs &&
      m_load_fn(m_thread_ctxs[thread_id], n_recs, rec_loc, partition_id)) {
    err = DB_INTERRUPTED;
    m_parallel_reader.set_error_state(DB_INTERRUPTED);
  }

  ctx->m_n_sent += n_recs;

  // ib::info() << "send records count: " << n_recs;

  if (nullptr != reader_thread_ctx->m_blob_heap &&
      nullptr == async_fetch_thread_ctx->m_blob_heap) {
    /** the blob heap has been transfer. need create one new  */
    reader_thread_ctx->m_blob_heap = nullptr;
    reader_thread_ctx->create_blob_heap();
  }

  if (nullptr != reader_thread_ctx->get_row_built_para_ctx()
                     ->get_compress_col_heap() &&
      nullptr == async_fetch_thread_ctx->m_compress_cols_heap) {
    /** the blob heap has been transfer. need create one new  */
    reader_thread_ctx->get_row_built_para_ctx()->reset_compress_col_heap();
    reader_thread_ctx->get_row_built_para_ctx()->create_compress_col_heap();
  }

  /**
   *  if reader_thread_ctx->m_blob_heap need not create, then
   *  there must be error happen, and the the read thread
   *  MUST NOT go further.
   */

  /* reset the async fetch thread callback context */
  async_fetch_thread_ctx->m_blob_heap = nullptr;
  return err;
}

/** this is a sync function call. */
dberr_t Parallel_reader_async_adapter::process_rows(
    const Parallel_reader::Ctx *reader_ctx) {
  auto reader_thread_ctx = reader_ctx->thread_ctx();
  auto adapter_ctx = reader_thread_ctx->get_callback_ctx<Thread_ctx>();
  auto blob_heap = reader_thread_ctx->m_blob_heap;

  ut_a(adapter_ctx->m_n_read >= adapter_ctx->m_n_sent);
  ut_a(adapter_ctx->m_n_read - adapter_ctx->m_n_sent <= m_batch_size);

  dberr_t err{DB_SUCCESS};

  Parallel_reader_table_fetch_handler::table_fetch_handler_thread_ctx_t
      *async_fetch_thread_ctx = static_cast<
          Parallel_reader_table_fetch_handler::table_fetch_handler_thread_ctx_t
              *>(m_thread_ctxs[reader_thread_ctx->m_thread_id]);

  Item *dplan_pushed_cond = async_fetch_thread_ctx->m_pushed_cond;
  mem_root_deque<Item_field *> *dplan_cond_field =
      async_fetch_thread_ctx->m_cond_fields;

  auto is_cluster_rec = reader_ctx->sec_need_clust_rec();

  {
    auto n_pending = pending(adapter_ctx);

    /* Start of a new range, send what we have buffered. */
    if ((reader_ctx->m_start && n_pending > 0) || is_buffer_full(adapter_ctx)) {
      err =
          send_batch(reader_thread_ctx, adapter_ctx->m_partition_id, n_pending);
      if (err != DB_SUCCESS) {
        return (err);
      }

      if (blob_heap) {
        /** use the new-create blob heap */
        ut_a(reader_thread_ctx->m_blob_heap);
        blob_heap = reader_thread_ctx->m_blob_heap;
      }
    }
  }

  mem_heap_t *heap{};
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets = offsets_;

  rec_offs_init(offsets_);

  if (!is_cluster_rec) {
    offsets = rec_get_offsets(reader_ctx->m_rec, reader_ctx->index(), offsets,
                              ULINT_UNDEFINED, UT_LOCATION_HERE, &heap);
  } else {
    ut_a(m_prebuilt->table->first_index() != m_prebuilt->index);
    offsets = rec_get_offsets(
        reader_ctx->m_rec, reader_ctx->index()->table->first_index(), offsets,
        ULINT_UNDEFINED, UT_LOCATION_HERE, &heap);
  }

  const auto next_rec = adapter_ctx->m_n_read % m_batch_size;

  const auto buffer_loc =
      &adapter_ctx->m_buffer[0] + next_rec * m_mysql_row.m_max_len;
  bool rec_clust = (is_use_cluster_index || is_cluster_rec);
  if (row_sel_store_mysql_rec(
          buffer_loc, m_prebuilt, reader_ctx->m_rec, nullptr, rec_clust,
          rec_clust ? reader_ctx->index()->table->first_index()
                    : reader_ctx->index(),
          reader_ctx->index(), offsets, false, nullptr, blob_heap,
          reader_thread_ctx->get_row_built_para_ctx())) {
    /* If there is any pending records, then we should not overwrite the
    partition ID with a different one. */
    if (pending(adapter_ctx) &&
        adapter_ctx->m_partition_id != reader_ctx->partition_id()) {
      err = DB_ERROR;
      ut_d(ut_error);
    } else {
      if (dplan_pushed_cond) {
        dplan_pushed_cond->set_field_ptr(dplan_cond_field, buffer_loc,
                                         m_mysql_row.m_offsets.data(),
                                         m_mysql_row.m_null_bit_offsets.data(),
                                         m_mysql_row.m_null_bit_mask.data());
        /** HERE, restore the user session thd context,
         * since the condition calculation(val_int) need access the
         * original thd, currently need access the thd->is_error().
         * since the multi-thread access, we MUST ensure only READ
         * the thd's infotmation: THIS NEED test for safety assurance.
         */
        ThdGuard _(dplan_pushed_cond->thd_belonged_to);
        if (dplan_pushed_cond->val_int()) {
          ++adapter_ctx->m_n_read;
          adapter_ctx->m_partition_id = reader_ctx->partition_id();
        }

      } else {
        ++adapter_ctx->m_n_read;
        adapter_ctx->m_partition_id = reader_ctx->partition_id();
      }
    }

    if (m_parallel_reader.is_error_set()) {
      /* Simply skip sending the records to RAPID in case of an error in the
      parallel reader and return DB_ERROR as the error could have been
      originated from RAPID threads. */
      err = DB_ERROR;
    }
  } else {
    err = DB_ERROR;
  }

  if (heap != nullptr) {
    mem_heap_free(heap);
  }

  return (err);
}

dberr_t Parallel_reader_async_adapter::end(
    Parallel_reader::Thread_ctx *reader_thread_ctx) {
  dberr_t err{DB_SUCCESS};

  auto thread_id = reader_thread_ctx->m_thread_id;
  auto thread_ctx = reader_thread_ctx->get_callback_ctx<Thread_ctx>();

  ut_a(thread_ctx->m_n_sent <= thread_ctx->m_n_read);
  ut_a(thread_ctx->m_n_read - thread_ctx->m_n_sent <= m_batch_size);

  if (!m_parallel_reader.is_error_set()) {
    /* It's possible that we might not have sent the records in the buffer
    when we have reached the end of records and the buffer is not full.
    Send them now. */
    size_t n_pending = pending(thread_ctx);

    if (n_pending != 0) {
      err =
          send_batch(reader_thread_ctx, thread_ctx->m_partition_id, n_pending);
    }
  }

  m_end_fn(m_thread_ctxs[thread_id]);

  // ib::info() << "thread send: " << thread_ctx->m_n_sent;
  ut::delete_(thread_ctx);
  reader_thread_ctx->set_callback_ctx<Thread_ctx>(nullptr);

  return (err);
}

dberr_t Parallel_reader_async_adapter::handle_ctx_start(
    Parallel_reader::Thread_ctx *reader_thread_ctx) {
  dberr_t err = DB_SUCCESS;

  [[maybe_unused]] std::shared_ptr<Parallel_reader::Ctx> &current_ctx =
      reader_thread_ctx->m_ctx;
  int ret = m_range_satrt_cb(m_thread_ctxs[reader_thread_ctx->m_thread_id],
                             current_ctx);

  if (ret) {
    err = DB_FAIL;
  }

  return err;
}

dberr_t Parallel_reader_async_adapter::handle_ctx_end(
    Parallel_reader::Thread_ctx *reader_thread_ctx) {
  dberr_t err = DB_SUCCESS;

  [[maybe_unused]] std::shared_ptr<Parallel_reader::Ctx> &current_ctx =
      reader_thread_ctx->m_ctx;
  int ret = m_range_end_cb(m_thread_ctxs[reader_thread_ctx->m_thread_id],
                           current_ctx);

  if (ret) {
    err = DB_FAIL;
  }

  return err;
}

ulint Parallel_reader_async_adapter::fill_mysql_row_info(
    row_prebuilt_t *prebuilt) {
  ulint max_row_len = 0;
  size_t mysql_row_info_size = 0;
  std::vector<mysql_row_templ_t *> v_templates;
  v_templates.reserve(prebuilt->n_template);
  for (uint i = 0; i < prebuilt->n_template; ++i) {
    v_templates.emplace_back(prebuilt->mysql_template + i);
  }

  std::stable_sort(
      v_templates.begin(), v_templates.end(),
      [](const mysql_row_templ_t *lhs, const mysql_row_templ_t *rhs) {
        return lhs->col_no < rhs->col_no;
      });

  for (uint i = 0; i < v_templates.size(); ++i) {
    const auto &templt = *(v_templates[i]);

    if (templt.col_no > mysql_row_info_size) {
      /** do this for avoid server layer mapping change */
      for (size_t j = mysql_row_info_size; j < templt.col_no; ++j) {
        m_mysql_row.m_offsets.push_back(0);
        m_mysql_row.m_null_bit_mask.push_back(0);
        m_mysql_row.m_null_bit_offsets.push_back(0);
      }
    }
    m_mysql_row.m_offsets.push_back(
        static_cast<ulong>(templt.mysql_col_offset));
    m_mysql_row.m_null_bit_mask.push_back(
        static_cast<ulong>(templt.mysql_null_bit_mask));
    m_mysql_row.m_null_bit_offsets.push_back(
        static_cast<ulong>(templt.mysql_null_byte_offset));
    if (max_row_len < (templt.mysql_col_offset + templt.mysql_col_len)) {
      max_row_len = (templt.mysql_col_offset + templt.mysql_col_len);
    }

    mysql_row_info_size = m_mysql_row.m_offsets.size();
  }
  ut_a(max_row_len <= prebuilt->mysql_row_len);
  return max_row_len;
}

void Parallel_reader_async_adapter::build_scan_range(row_prebuilt_t *prebuilt,
                                                     unsigned char *key_start,
                                                     uint key_start_len,
                                                     unsigned char *key_end,
                                                     uint key_end_len) {
  ut_a(nullptr == m_key_start && 0 == m_key_start_len);
  ut_a(nullptr == m_key_end && 0 == m_key_end_len);

  /**
   * 1. we keep the key_start/end value in our private buffer
   */
  if (key_start) {
    m_key_start = ut::new_arr_withkey<unsigned char>(UT_NEW_THIS_FILE_PSI_KEY,
                                                     ut::Count{key_start_len});
    m_key_start_len = key_start_len;
    memcpy(m_key_start, key_start, key_start_len);
  }
  if (key_end && key_end_len) {
    m_key_end = ut::new_arr_withkey<unsigned char>(UT_NEW_THIS_FILE_PSI_KEY,
                                                   ut::Count{key_end_len});
    m_key_end_len = key_end_len;
    memcpy(m_key_end, key_end, key_end_len);
  }

  if (nullptr == m_key_start && nullptr == m_key_end) {
    return;
  }

  /** 2. create the scan range */

  dict_table_t *table = prebuilt->index->table;
  ulint max_entry_len =
      2 * (table->get_n_cols() + dict_table_get_n_v_cols(table));
  m_range_start = nullptr;
  m_range_end = nullptr;
  m_range_heap = mem_heap_create(1024, UT_LOCATION_HERE);

  byte *range_start_key_val_buffer = nullptr;
  byte *range_end_key_val_buffer = nullptr;

  if (prebuilt->srch_key_val_len) {
    range_start_key_val_buffer = static_cast<byte *>(
        mem_heap_alloc(m_range_heap, 2 * prebuilt->srch_key_val_len));

    range_end_key_val_buffer =
        (range_start_key_val_buffer + prebuilt->srch_key_val_len);
  }

  if (m_key_start) {
    m_range_start = dtuple_create(m_range_heap, max_entry_len);
    row_sel_convert_mysql_key_to_innobase(
        m_range_start, range_start_key_val_buffer, prebuilt->srch_key_val_len,
        prebuilt->index, static_cast<byte *>(m_key_start), m_key_start_len);
  }

  if (m_key_end) {
    m_range_end = dtuple_create(m_range_heap, max_entry_len);

    row_sel_convert_mysql_key_to_innobase(
        m_range_end, range_end_key_val_buffer, prebuilt->srch_key_val_len,
        prebuilt->index, static_cast<byte *>(m_key_end), m_key_end_len);
  }

  m_read_range.m_start = m_range_start;
  m_read_range.m_end = m_range_end;
}

void Parallel_reader_async_adapter::build_scan_range(
    row_prebuilt_t *prebuilt,
    parallel_read_data_reader_range_t *to_fetch_range) {
  ut_a(nullptr == m_key_start && 0 == m_key_start_len);
  ut_a(nullptr == m_key_end && 0 == m_key_end_len);

  if (nullptr == to_fetch_range->m_min_key &&
      nullptr == to_fetch_range->m_max_key) {
    /** this is table scan? */
    return;
  }

  /** we keep the range in our private buffer */
  if (to_fetch_range->m_min_key) {
    ha_rkey_function k_flag = to_fetch_range->m_min_key->flag;
    ut_a(k_flag == ha_rkey_function::HA_READ_KEY_EXACT ||
         k_flag == ha_rkey_function::HA_READ_AFTER_KEY ||
         k_flag == ha_rkey_function::HA_READ_KEY_OR_NEXT);
    m_key_start = ut::new_arr_withkey<unsigned char>(
        UT_NEW_THIS_FILE_PSI_KEY, ut::Count{to_fetch_range->m_min_key->length});

    memcpy(m_key_start, to_fetch_range->m_min_key->key,
           to_fetch_range->m_min_key->length);
    m_key_range_start = *(to_fetch_range->m_min_key);
    m_key_range_start.key = m_key_start;
    m_key_start_len = to_fetch_range->m_min_key->length;
    m_read_range.m_range_start_flag = to_fetch_range->m_min_key->flag;
  }

  if (to_fetch_range->m_max_key) {
    ut_a(to_fetch_range->m_max_key->flag ==
             ha_rkey_function::HA_READ_BEFORE_KEY ||
         to_fetch_range->m_max_key->flag ==
             ha_rkey_function::HA_READ_AFTER_KEY);
    m_key_end = ut::new_arr_withkey<unsigned char>(
        UT_NEW_THIS_FILE_PSI_KEY, ut::Count{to_fetch_range->m_max_key->length});

    memcpy(m_key_end, to_fetch_range->m_max_key->key,
           to_fetch_range->m_max_key->length);
    m_key_range_end = *(to_fetch_range->m_max_key);
    m_key_range_end.key = m_key_end;
    m_key_end_len = to_fetch_range->m_max_key->length;
    m_read_range.m_range_end_flag = to_fetch_range->m_max_key->flag;
  }

  TABLE *mysql_table = prebuilt->m_mysql_table;

  dict_table_t *table = prebuilt->index->table;
  ulint max_entry_len =
      2 * (table->get_n_cols() + dict_table_get_n_v_cols(table));

  KEY *key = mysql_table->key_info +
             (to_fetch_range->m_keynr == MAX_KEY ? 0 : to_fetch_range->m_keynr);

  m_range_start = nullptr;
  m_range_end = nullptr;
  m_range_heap = mem_heap_create(1024, UT_LOCATION_HERE);

  byte *range_start_key_val_buffer = nullptr;
  byte *range_end_key_val_buffer = nullptr;

  if (prebuilt->srch_key_val_len) {
    range_start_key_val_buffer = static_cast<byte *>(
        mem_heap_alloc(m_range_heap, 2 * prebuilt->srch_key_val_len));

    range_end_key_val_buffer =
        (range_start_key_val_buffer + prebuilt->srch_key_val_len);
  }

  if (m_key_start) {
    m_range_start = dtuple_create(m_range_heap, max_entry_len);
    dict_index_copy_types(m_range_start, prebuilt->index,
                          key->actual_key_parts);

    row_sel_convert_mysql_key_to_innobase(
        m_range_start, range_start_key_val_buffer, prebuilt->srch_key_val_len,
        prebuilt->index, static_cast<byte *>(m_key_start), m_key_start_len);
  }

  if (m_key_end) {
    m_range_end = dtuple_create(m_range_heap, max_entry_len);
    dict_index_copy_types(m_range_end, prebuilt->index, key->actual_key_parts);
    row_sel_convert_mysql_key_to_innobase(
        m_range_end, range_end_key_val_buffer, prebuilt->srch_key_val_len,
        prebuilt->index, static_cast<byte *>(m_key_end), m_key_end_len);
  }

  m_read_range.m_start = m_range_start;
  m_read_range.m_end = m_range_end;
  if (nullptr != m_read_range.m_start || nullptr != m_read_range.m_end) {
    m_read_range.is_greatdb_ap_range_scan = true;
  }
}

bool Parallel_reader_async_adapter::is_readset_set_sec_index(
    TABLE *table, dict_index_t *sec_index, uint sec_keynr) {
  bool is_set = false;

  ut_a(sec_index->table->first_index() != sec_index);

  KEY *key = table->key_info + sec_keynr;
  ulint n_fields = dict_index_get_n_fields(sec_index);
  ut_a(n_fields >= key->user_defined_key_parts);
  for (ulint n = 0; n < key->user_defined_key_parts; ++n) {
    dict_field_t *field = sec_index->get_field(n);
    dict_col_t *col = field->col;
    uint col_index = col->ind;
    is_set = bitmap_is_set(table->read_set, col_index);
    if (is_set) {
      break;
    }
  }

  return is_set;
}

void Parallel_reader_async_adapter::set_readset_sec_index(
    TABLE *table, dict_index_t *sec_index, uint sec_keynr) {
  ut_a(sec_index->table->first_index() != sec_index);

  KEY *key = table->key_info + sec_keynr;
  ulint n_fields = dict_index_get_n_fields(sec_index);
  ut_a(n_fields >= key->user_defined_key_parts);
  for (ulint n = 0; n < key->user_defined_key_parts; ++n) {
    dict_field_t *field = sec_index->get_field(n);
    dict_col_t *col = field->col;
    uint col_index = col->ind;
    bitmap_set_bit(table->read_set, col_index);
  }

  is_set_table_readset = true;
}

#endif /* !UNIV_HOTBACKUP */
