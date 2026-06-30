/*****************************************************************************
Copyright (c) 2018, 2021, Oracle and/or its affiliates.
Copyright (c) 2022, Huawei Technologies Co., Ltd.
Copyright (c) 2025, GreatDB Software Co., Ltd.

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

/** @file include/row0pread-adapter.h
Parallel read adapter interface.

Created 2018-02-28 by Darshan M N. */

#ifndef row0pread_fetcher_h
#define row0pread_fetcher_h

#include <atomic>

#include "row0pread-adapter.h"
#include "row0pread.h"
#include "ut0counter.h"

#include "handler.h"

struct table_fetch_buf_t {
  char *m_buf{nullptr};
  uint64_t m_size{0};
  void init() {
    m_buf = nullptr;
    m_size = 0;
  }
};

struct fetch_table_fetchout_que_item_t {
  uint m_rows_count{0};
  /** the data size in buffer item data_size = 0 means has finished the read */
  uint64_t m_data_size{0};
  uint64_t m_partition_id{0};
  table_fetch_buf_t m_buf_item;
  uint32_t m_buffer_owner{0};
};

class Parallel_reader_table_fetch_data_buffer_list_t {
 public:
  Parallel_reader_table_fetch_data_buffer_list_t();
  ~Parallel_reader_table_fetch_data_buffer_list_t();

 public:
  int init_fetch_data_buffer_list(size_t max_chunk_size,
                                  uint32_t init_list_len);

  table_fetch_buf_t get_fetch_data_buffer(size_t want_chunk_size);
  void attach_blob_heap_to_buffer(table_fetch_buf_t &fetch_buf,
                                  mem_heap_t *blob_heap);
  void attach_compress_heap_to_buffer(table_fetch_buf_t &fetch_buf,
                                      mem_heap_t *compress_heap);
  void ret_fetch_data_buffer(table_fetch_buf_t &fetch_buf);

 private:
  int need_enlarge_list();
  int create_data_chunk_buffer(table_fetch_buf_t &fetch_buf);
  void destory_list();

 private:
  size_t m_max_req_buf_size{2 * 1024 * 1024};
  ib_mutex_t m_mutex;
  /** the mutex pro=*/
  std::map<void *, void *> m_buf_to_blob_heap;
  std::map<void *, void *> m_buf_to_compress_heap;
  std::list<table_fetch_buf_t> m_free_buf_list;
  std::list<table_fetch_buf_t> m_all_buf_list;
};

class Parallel_reader_table_fetch_handler {
 public:
  struct table_fetch_handler_thread_ctx_t {
    uint64_t m_processed_range_count{0};
    uint64_t m_current_range_id{0};
    uint64_t m_last_range_id{0};
    uint64_t m_total_buffer_req_count{0};
    uint64_t m_total_records{0};
    uint32_t m_parallel_read_work_thread_index{0};
    bool m_parallel_read_work_started{false};
    bool m_parallel_read_work_finished{false};

    /** The blob will be correspoding to every block in the data_buf_list.
     *  this heap will be free if the block finish using.
     *  so we need keep the map between the block and heap.
     */
    mem_heap_t *m_blob_heap{nullptr};

    mem_heap_t *m_compress_cols_heap{nullptr};

    Parallel_reader_table_fetch_data_buffer_list_t m_data_buf_list;

    Item *m_pushed_cond{nullptr};
    mem_root_deque<Item_field *> *m_cond_fields{nullptr};
  };

 public:
  Parallel_reader_table_fetch_handler();
  ~Parallel_reader_table_fetch_handler();

 public:
  /** support multi-thread access this interface; so need thread-safe thread */
  int read_data_block(char *buf, uint64_t max_buf_size,
                      uint64_t *actual_data_size);

  int read_ready_data_frame(
      parallel_reader_data_frame_t *data_frame /*in/out*/);
  int finish_use_data_frame(
      parallel_reader_data_frame_t *data_frame /*in/out*/);

 public:
  int table_handler_fetch_init(parallel_read_data_reader_t *data_consumer);
  int table_fetch_start(parallel_read_data_reader_t *data_consumer);

  /** notify the queue the data fetch has been finished */
  int table_fetch_end(parallel_read_data_reader_t *data_consumer);

  void signal_start() {}

 private:
  using fetch_Init_fn = handler::Load_init_cbk;
  using fetch_Load_fn = handler::Load_cbk;
  using fetch_End_fn = handler::Load_end_cbk;

  /**
  This callback is called by each parallel load thread at the beginning of
  the parallel load for the adapter scan.
  @param cookie      The cookie for this thread
  @param ncols       Number of columns in each row
  @param row_len     The size of a row in bytes
  @param col_offsets An array of size ncols, where each element represents
                     the offset of a column in the row data. The memory of
                     this array belongs to the caller and will be free-ed
                     after the pload_end_cbk call.
  @param null_byte_offsets An array of size ncols, where each element
                     represents the offset of a column in the row data. The
                     memory of this array belongs to the caller and will be
                     free-ed after the pload_end_cbk call.
  @param null_bitmasks An array of size ncols, where each element
                     represents the bitmask required to get the null bit. The
                     memory of this array belongs to the caller and will be
                   free-ed after the pload_end_cbk call.
*/
  bool fetch_init_cbk(void *cookie, ulong ncols [[maybe_unused]],
                      ulong row_len [[maybe_unused]],
                      const ulong *col_offsets [[maybe_unused]],
                      const ulong *null_byte_offsets [[maybe_unused]],
                      const ulong *null_bitmasks [[maybe_unused]]);

  /**
This callback is called by each parallel load thread when processing
of rows is required for the adapter scan.
@param[in] cookie       The cookie for this thread
@param[in] nrows        The nrows that are available
@param[in] rowdata      The mysql-in-memory row data buffer. This is a
memory buffer for nrows records. The length of each record is fixed and
communicated via Load_init_cbk
@param[in] partition_id Partition id if it's a partitioned table, else
std::numeric_limits<uint64_t>::max()
@returns true if there is an error, false otherwise.
*/
  bool fetch_data_cbk(void *cookie [[maybe_unused]],
                      uint nrows [[maybe_unused]],
                      void *rowdata [[maybe_unused]],
                      uint64_t partition_id [[maybe_unused]]);

  /**
This callback is called by each parallel load thread when processing
of rows has ended for the adapter scan.
@param[in] cookie    The cookie for this thread
*/
  void fetch_end_cbk(void *cookie);

  int range_start_cbk(void *cookie,
                      std::shared_ptr<Parallel_reader::Ctx> &current_ctx);
  int range_end_cbk(void *cookie,
                    std::shared_ptr<Parallel_reader::Ctx> &current_ctx);

  void drain_the_data_que();

 private:
  Parallel_reader_async_adapter *m_fetch_adapter{nullptr};
  parallel_reader_block_queue_t<fetch_table_fetchout_que_item_t>
      m_fetch_data_queue;

  size_t m_max_data_chunk_size{2 * 1024 * 1024};

  struct mysql_row_t {
    ulong m_ncols{0};
    ulong m_row_len{0};
    const ulong *m_col_offsets{nullptr};
    const ulong *m_null_byte_offsets{nullptr};
    const ulong *m_null_bitmasks{nullptr};
  };

  mysql_row_t m_mysql_row_info;

  uint32_t m_thread_alloc_count{0};
  std::atomic<uint32_t> m_thread_launch_count{0};
  std::atomic<uint32_t> m_thread_finished_count{0};

  std::vector<void *> m_parallel_read_thread_cb_cookies;
  table_fetch_handler_thread_ctx_t *m_parallel_read_thread_ctx_for_fetch{
      nullptr};

  std::atomic<bool> m_stopped{false};
};

/**
 * Parallel_reader_range_fetch_handler
 *
 */

/**
 * With one parallel read worker thread cohort,
 * this "fetcher" has the ability of reading multi-table and multi-index
 */

class Parallel_reader_fetcher {
 public:
  Parallel_reader_fetcher();
  ~Parallel_reader_fetcher();

 public:
  /** call this in the mysql thread context*/
  int init_data_fetcher();

  /** call this in the mysql thread context*/
  int add_target_for_fetch(parallel_read_data_reader_t *data_consumer);
  /** call this in the mysql thread context*/
  int start_data_fetcher(parallel_read_data_reader_t *data_consumer);
  /** call this in the mysql thread context*/
  int stop_data_fetcher(parallel_read_data_reader_t *data_consumer);

  uint64_t get_active_handler_count() { return m_total_target_count.load(); }

 private:
  /** Parallel reader to use.
   *  we can use multi Parallel reader instance.
   */
  Parallel_reader *m_parallel_reader{nullptr};
  trx_t *m_trx{nullptr};

  std::atomic<uint64_t> m_total_target_count{0};
};

int innobase_parallel_read_create_data_fetcher(
    parallel_read_create_data_fetcher_ctx_t &data_fetcher_ctx,
    void *&data_fetcher);

int innobase_parallel_read_destory_data_fetcher(void *&data_fetcher);

int innobase_parallel_read_init_data_fetcher(
    parallel_read_init_data_fetcher_ctx_t &init_data_fetcher_ctx);

int innobase_parallel_read_add_target_to_data_fetcher(
    parallel_read_add_target_to_data_fetcher_ctx_t &target_to_data_fetcher_ctx);

int innobase_parallel_read_start_data_fetch(
    parallel_read_start_data_fetch_ctx_t &start_data_fetch_ctx);

int innobase_parallel_read_end_data_fetch(
    parallel_read_end_data_fetch_ctx_t &end_data_fetch_ctx);

#endif /* !row0pread_fetcher_h */
