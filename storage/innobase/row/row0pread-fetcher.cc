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

/** @file row/row0pread-adapter.cc
Parallel read adapter interface implementation

Created 2018-02-28 by Darshan M N */

#ifndef UNIV_HOTBACKUP

#include "row0pread-fetcher.h"
#include "row0sel.h"
#include "sql/item.h"
#include "univ.i"

#include "ha_innodb.h"
#include "ha_innopart.h"

std::atomic<uint64_t> srv_parallel_fetch_buffer_server_used{0};
std::atomic<uint64_t> srv_parallel_fetch_buffer_engine_used{0};
std::atomic<uint64_t> srv_parallel_fetch_buffer_count{0};
std::atomic<uint64_t> srv_parallel_fetch_buffer_size{0};

Parallel_reader_table_fetch_data_buffer_list_t::
    Parallel_reader_table_fetch_data_buffer_list_t() {
  mutex_create(LATCH_ID_PARALLEL_READ, &m_mutex);
}

Parallel_reader_table_fetch_data_buffer_list_t::
    ~Parallel_reader_table_fetch_data_buffer_list_t() {
  destory_list();
  mutex_destroy(&m_mutex);
}

void Parallel_reader_table_fetch_data_buffer_list_t::destory_list() {
  for (auto &fetch_buf : m_all_buf_list) {
    ut::delete_arr(fetch_buf.m_buf);
    fetch_buf.init();
    srv_parallel_fetch_buffer_count.fetch_sub(1);
    srv_parallel_fetch_buffer_size.fetch_sub(m_max_req_buf_size);
  }
  m_all_buf_list.clear();
  m_free_buf_list.clear();

  for (auto &it : m_buf_to_blob_heap) {
    mem_heap_t *blob_heap = reinterpret_cast<mem_heap_t *>(it.second);
    mem_heap_free(blob_heap);
  }
  m_buf_to_blob_heap.clear();
}

int Parallel_reader_table_fetch_data_buffer_list_t::init_fetch_data_buffer_list(
    size_t max_chunk_size, uint32_t init_list_len) {
  int err = 0;

  m_max_req_buf_size = max_chunk_size;

  for (uint32_t index = 0; index < init_list_len; ++index) {
    table_fetch_buf_t fetch_buf;
    fetch_buf.init();

    err = create_data_chunk_buffer(fetch_buf);
    if (err) {
      break;
    }
    m_all_buf_list.emplace_back(fetch_buf);
    m_free_buf_list.emplace_back(fetch_buf);
  }

  if (err) {
    destory_list();
  }

  return err;
}

table_fetch_buf_t
Parallel_reader_table_fetch_data_buffer_list_t::get_fetch_data_buffer(
    size_t want_chunk_size) {
  table_fetch_buf_t fetch_buf;
  fetch_buf.init();

  if (want_chunk_size > m_max_req_buf_size) {
    return fetch_buf;
  }

  mutex_enter(&m_mutex);

  while (1) {
    if (!m_free_buf_list.empty()) {
      fetch_buf = m_free_buf_list.front();
      m_free_buf_list.pop_front();
      break;
    } else {
      int err = need_enlarge_list();
      if (err) {
        break;
      }
    }
  }
  mutex_exit(&m_mutex);

  return fetch_buf;
}

void Parallel_reader_table_fetch_data_buffer_list_t::attach_blob_heap_to_buffer(
    table_fetch_buf_t &fetch_buf, mem_heap_t *blob_heap) {
  if (nullptr == blob_heap) {
    return;
  }
  mutex_enter(&m_mutex);

  auto it = m_buf_to_blob_heap.find(fetch_buf.m_buf);
  assert(m_buf_to_blob_heap.end() == it);

  m_buf_to_blob_heap[fetch_buf.m_buf] = blob_heap;

  mutex_exit(&m_mutex);
}

void Parallel_reader_table_fetch_data_buffer_list_t::
    attach_compress_heap_to_buffer(table_fetch_buf_t &fetch_buf,
                                   mem_heap_t *compress_heap) {
  if (nullptr == compress_heap) {
    return;
  }

  mutex_enter(&m_mutex);

  auto it = m_buf_to_compress_heap.find(fetch_buf.m_buf);
  assert(m_buf_to_compress_heap.end() == it);

  m_buf_to_compress_heap[fetch_buf.m_buf] = compress_heap;

  mutex_exit(&m_mutex);
}

void Parallel_reader_table_fetch_data_buffer_list_t::ret_fetch_data_buffer(
    table_fetch_buf_t &fetch_buf) {
  ut_a(fetch_buf.m_buf);

  mutex_enter(&m_mutex);

  auto it = m_buf_to_blob_heap.find(fetch_buf.m_buf);
  if (m_buf_to_blob_heap.end() != it) {
    mem_heap_t *blob_heap = reinterpret_cast<mem_heap_t *>(it->second);
    mem_heap_free(blob_heap);
    m_buf_to_blob_heap.erase(it);
  }

  auto it_com = m_buf_to_compress_heap.find(fetch_buf.m_buf);
  if (m_buf_to_compress_heap.end() != it_com) {
    mem_heap_t *compress_heap = reinterpret_cast<mem_heap_t *>(it_com->second);
    mem_heap_free(compress_heap);
    m_buf_to_compress_heap.erase(it_com);
  }

  m_free_buf_list.emplace_back(fetch_buf);

  mutex_exit(&m_mutex);
}

int Parallel_reader_table_fetch_data_buffer_list_t::need_enlarge_list() {
  int err = 0;

  ut_ad(mutex_own(&m_mutex));

  table_fetch_buf_t fetch_buf;

  err = create_data_chunk_buffer(fetch_buf);

  if (0 == err) {
    m_free_buf_list.emplace_back(fetch_buf);
    m_all_buf_list.emplace_back(fetch_buf);
  }

  return err;
}

int Parallel_reader_table_fetch_data_buffer_list_t::create_data_chunk_buffer(
    table_fetch_buf_t &fetch_buf) {
  int err = 0;

  char *buf = nullptr;
  fetch_buf.init();

  do {
    buf = ut::new_arr_withkey<char>(UT_NEW_THIS_FILE_PSI_KEY,
                                    ut::Count{m_max_req_buf_size});
    if (nullptr == buf) {
      err = DB_OUT_OF_MEMORY;
      break;
    }
    fetch_buf.m_buf = buf;
    fetch_buf.m_size = m_max_req_buf_size;

    srv_parallel_fetch_buffer_count.fetch_add(1);
    srv_parallel_fetch_buffer_size.fetch_add(m_max_req_buf_size);
  } while (false);

  return err;
}

int innobase_parallel_read_create_data_fetcher(
    parallel_read_create_data_fetcher_ctx_t &data_fetcher_ctx,
    void *&data_fetcher) {
  Parallel_reader_fetcher *data_fetcher_object = nullptr;
  data_fetcher = nullptr;
  data_fetcher_object =
      ut::new_withkey<Parallel_reader_fetcher>(UT_NEW_THIS_FILE_PSI_KEY);
  if (nullptr == data_fetcher_object) {
    return (HA_ERR_GENERIC);
  }

  data_fetcher = static_cast<void *>(data_fetcher_object);

  return 0;
}

/** if the data_fetcher is destroied, it will be assigned as nullptr
 * if it can't be destroied, it will be kept.
 */
int innobase_parallel_read_destory_data_fetcher(void *&data_fetcher) {
  int err = 0;
  void *mem_obj = data_fetcher;
  Parallel_reader_fetcher *data_fetcher_object =
      static_cast<Parallel_reader_fetcher *>(mem_obj);

  if (data_fetcher_object) {
    uint64_t active_count = data_fetcher_object->get_active_handler_count();

    if (0 == active_count) {
      ut::delete_(data_fetcher_object);
      data_fetcher = nullptr;
    } else {
      err = 1;
    }
  }

  return err;
}

int innobase_parallel_read_init_data_fetcher(
    parallel_read_init_data_fetcher_ctx_t &init_data_fetcher_ctx) {
  Parallel_reader_fetcher *data_fetcher =
      static_cast<Parallel_reader_fetcher *>(
          init_data_fetcher_ctx.data_fetcher);

  ut_a(data_fetcher);

  data_fetcher->init_data_fetcher();

  return 0;
}

int innobase_parallel_read_add_target_to_data_fetcher(
    parallel_read_add_target_to_data_fetcher_ctx_t
        &target_to_data_fetcher_ctx) {
  int err = 0;
  ut_a(target_to_data_fetcher_ctx.data_fetcher &&
       target_to_data_fetcher_ctx.data_consumer);

  Parallel_reader_fetcher *data_fetch_manager =
      static_cast<Parallel_reader_fetcher *>(
          target_to_data_fetcher_ctx.data_fetcher);

  /** after this, the background threads have been launched and
   * begin to fetch data.
   */
  err = data_fetch_manager->add_target_for_fetch(
      target_to_data_fetcher_ctx.data_consumer);

  return err;
}

int innobase_parallel_read_start_data_fetch(
    parallel_read_start_data_fetch_ctx_t &start_data_fetch_ctx) {
  // start_data_fetch_ctx.data_fetcher
  return 0;
}

int innobase_parallel_read_end_data_fetch(
    parallel_read_end_data_fetch_ctx_t &end_data_fetch_ctx) {
  return 0;
}

/*****************************************************/

Parallel_reader_fetcher::Parallel_reader_fetcher() {}

Parallel_reader_fetcher::~Parallel_reader_fetcher() {
  uint64_t active_fetcher_count = m_total_target_count.load();

  if (active_fetcher_count) {
    ib::warn() << "parallel reader fetcher will be released, but stil has "
               << m_total_target_count.load() << " fetcher exist!";
  }
}

int Parallel_reader_fetcher::init_data_fetcher() {
  /** currently, there are no need to do initialize*/

  return 0;
}

/** try to make this interface as atomicity */
int Parallel_reader_fetcher::add_target_for_fetch(
    parallel_read_data_reader_t *data_consumer) {
  int err = 0;
  ut_a(data_consumer);

  Parallel_reader_table_fetch_handler *table_fetch_handler = nullptr;

  do {
    table_fetch_handler = ut::new_withkey<Parallel_reader_table_fetch_handler>(
        UT_NEW_THIS_FILE_PSI_KEY);

    if (nullptr == table_fetch_handler) {
      err = (HA_ERR_OUT_OF_MEM);
      break;
    }

    err = table_fetch_handler->table_handler_fetch_init(data_consumer);
    if (err) {
      break;
    }

    err = table_fetch_handler->table_fetch_start(data_consumer);
    if (err) {
      break;
    }

    /** the following code SHOULD NOT cause failure */
    data_consumer->set_engine_ctx(static_cast<void *>(table_fetch_handler));

    using std::placeholders::_1;
    using std::placeholders::_2;
    using std::placeholders::_3;
    using read_block_hook_t =
        parallel_read_data_reader_t::call_read_data_hook_t;

    read_block_hook_t read_block_hook =
        std::bind(&Parallel_reader_table_fetch_handler::read_data_block,
                  table_fetch_handler, _1, _2, _3);

    data_consumer->set_read_data_hook(std::move(read_block_hook));

    using stop_fetch_hook_t =
        parallel_read_data_reader_t::call_stop_read_data_hook_t;

    stop_fetch_hook_t stop_fetch_hook =
        std::bind(&Parallel_reader_fetcher::stop_data_fetcher, this, _1);

    data_consumer->set_stop_read_data_hook(std::move(stop_fetch_hook));

    using get_data_hook_t =
        parallel_read_data_reader_t::call_get_data_frame_hook_t;
    using ret_data_hook_t =
        parallel_read_data_reader_t::call_ret_data_frame_hook_t;

    get_data_hook_t get_data_hook =
        std::bind(&Parallel_reader_table_fetch_handler::read_ready_data_frame,
                  table_fetch_handler, _1);

    ret_data_hook_t ret_data_hook =
        std::bind(&Parallel_reader_table_fetch_handler::finish_use_data_frame,
                  table_fetch_handler, _1);

    data_consumer->set_get_data_frame_hook(std::move(get_data_hook));
    data_consumer->set_ret_data_frame_hook(std::move(ret_data_hook));

    /** For successful started target, we count it for statistics */
    m_total_target_count.fetch_add(1, std::memory_order_seq_cst);

  } while (false);

  if (err) {
    ut::delete_(table_fetch_handler);
  }

  return err;
}

int Parallel_reader_fetcher::start_data_fetcher(
    parallel_read_data_reader_t *data_consumer) {
  int err = 0;
  ut_a(data_consumer);

  Parallel_reader_table_fetch_handler *table_fetch_handler [[maybe_unused]] =
      static_cast<Parallel_reader_table_fetch_handler *>(
          data_consumer->get_engine_ctx());

  ut_a(table_fetch_handler);

  /** we can do more control here */
  table_fetch_handler->signal_start();

  return err;
}

/** make sure, there is no thread to read before call this.*/
int Parallel_reader_fetcher::stop_data_fetcher(
    parallel_read_data_reader_t *data_consumer) {
  int err = 0;

  Parallel_reader_table_fetch_handler *table_fetch_handler =
      static_cast<Parallel_reader_table_fetch_handler *>(
          data_consumer->get_engine_ctx());
  ut_a(table_fetch_handler);

  err = table_fetch_handler->table_fetch_end(data_consumer);

  ut::delete_(table_fetch_handler);
  data_consumer->set_engine_ctx(nullptr);
  m_total_target_count.fetch_sub(1, std::memory_order_relaxed);

  return err;
}

Parallel_reader_table_fetch_handler::Parallel_reader_table_fetch_handler() {}

Parallel_reader_table_fetch_handler::~Parallel_reader_table_fetch_handler() {
  if (m_parallel_read_thread_ctx_for_fetch) {
    [[maybe_unused]] uint64_t total_rows = 0;
    for (auto cb : m_parallel_read_thread_cb_cookies) {
      [[maybe_unused]] table_fetch_handler_thread_ctx_t *fetch_thread_cb =
          static_cast<table_fetch_handler_thread_ctx_t *>(cb);
      // ib::info() << "thread "
      //            << fetch_thread_cb->m_parallel_read_work_thread_index
      //            << " handle range count: "
      //            << fetch_thread_cb->m_processed_range_count
      //            << " total rows: " << fetch_thread_cb->m_total_records;
      total_rows += fetch_thread_cb->m_total_records;
    }
    // ib::info() << "total fetch rows count: " << total_rows;
    ut::delete_arr(m_parallel_read_thread_ctx_for_fetch);
    m_parallel_read_thread_ctx_for_fetch = nullptr;
  }
  m_parallel_read_thread_cb_cookies.clear();

  m_fetch_data_queue.deinit_queue();
}

/** this function should be thread-safe */
int Parallel_reader_table_fetch_handler::read_data_block(
    char *buf, uint64_t max_buf_size, uint64_t *actual_data_size) {
  ut_a(max_buf_size >= m_max_data_chunk_size);

  fetch_table_fetchout_que_item_t data_item;

  /** this operation may be blocked because the queue maybe empty*/
  data_item = m_fetch_data_queue.de_queue();
  if (0 == data_item.m_data_size) {
    *actual_data_size = 0;
    /** there maybe concurrent read threads, so also need notify them the data
     * end case*/
    fetch_table_fetchout_que_item_t data_end_item;
    data_end_item.m_data_size = 0;
    /** this include two cases:
     * 1. all worker threads has been finished
     * 2. notify the fetch stop
     */
    m_fetch_data_queue.en_queue_head(data_end_item);
    return 0;
  }
  srv_parallel_fetch_buffer_engine_used.fetch_sub(1);
  /**Can avoid the memory here? */
  memcpy(buf, data_item.m_buf_item.m_buf, data_item.m_data_size);
  *actual_data_size = data_item.m_data_size;

  table_fetch_handler_thread_ctx_t *thread_ctx_for_cb =
      (m_parallel_read_thread_ctx_for_fetch + data_item.m_buffer_owner);
  thread_ctx_for_cb->m_data_buf_list.ret_fetch_data_buffer(
      data_item.m_buf_item);

  return 0;
}

int Parallel_reader_table_fetch_handler::read_ready_data_frame(
    parallel_reader_data_frame_t *data_frame) {
  int ret = 0;
  fetch_table_fetchout_que_item_t data_item;

  ut_a(nullptr != data_frame);

  data_frame->init();

  do {
    data_item = m_fetch_data_queue.de_queue();
    if (0 == data_item.m_data_size) {
      data_frame->m_actual_data_size = 0;
      data_frame->m_buf = nullptr;
      fetch_table_fetchout_que_item_t data_end_item;
      data_end_item.m_data_size = 0;
      m_fetch_data_queue.en_queue_head(data_end_item);
      ret = 0;
      break;
    }
    srv_parallel_fetch_buffer_engine_used.fetch_sub(1);

    /** has validate data in the queue */
    data_frame->m_buf_size = data_item.m_buf_item.m_size;
    data_frame->m_buf_id = data_item.m_buffer_owner;
    data_frame->m_buf = data_item.m_buf_item.m_buf;
    data_frame->m_actual_data_size = data_item.m_data_size;
    data_frame->m_buf_partition_id = data_item.m_partition_id;
    srv_parallel_fetch_buffer_server_used.fetch_add(1);

  } while (false);

  return ret;
}
int Parallel_reader_table_fetch_handler::finish_use_data_frame(
    parallel_reader_data_frame_t *data_frame) {
  int ret = 0;
  ut_a(nullptr != data_frame);

  if (data_frame->m_buf && data_frame->m_buf_size) {
    table_fetch_handler_thread_ctx_t *thread_ctx_for_cb =
        (m_parallel_read_thread_ctx_for_fetch + data_frame->m_buf_id);

    table_fetch_buf_t data_frame_buf_item;
    data_frame_buf_item.m_buf = data_frame->m_buf;
    data_frame_buf_item.m_size = data_frame->m_buf_size;

    thread_ctx_for_cb->m_data_buf_list.ret_fetch_data_buffer(
        data_frame_buf_item);
    srv_parallel_fetch_buffer_server_used.fetch_sub(1);
  }

  data_frame->init();

  return ret;
}

int Parallel_reader_table_fetch_handler::table_handler_fetch_init(
    parallel_read_data_reader_t *data_consumer) {
  int err = 0;
  ut_a(data_consumer);
  ut_a(nullptr == m_fetch_adapter);

  void *parallel_adapter = nullptr;
  size_t read_thread_count = 0;
  uint32_t mysql_parallel_thread_count = 0;
  bool use_reserved_threads = true;

  ulint max_read_buf_size = (2 * 1024 * 1024);

  ha_innobase *innobase_handler =
      dynamic_cast<ha_innobase *>(data_consumer->get_target_handler());

  ut_a(innobase_handler);

  bool is_partition_table =
      innobase_handler->get_table_share()->m_part_info ? true : false;

  mysql_parallel_thread_count =
      data_consumer->get_mysql_parallel_thread_count();

  /** partition table don't support the fetched data keep ordered */
  if (data_consumer->is_need_keep_ordered() && !is_partition_table) {
    /** currently, only use one thread to sorted natually */
    mysql_parallel_thread_count = 1;
  }

  max_read_buf_size = data_consumer->get_mysql_block_size();
  /** we can adjust the max_read_buf_size according the total parallel thread
   * count
   */

  parallel_read_data_reader_range_t to_fetch_range =
      data_consumer->get_reader_read_range();

  do {
    err = innobase_handler->parallel_fetch_init(
        parallel_adapter, &read_thread_count, use_reserved_threads,
        mysql_parallel_thread_count, data_consumer->get_read_index(),
        &to_fetch_range, max_read_buf_size);

    if (err) {
      ut_a(nullptr == m_fetch_adapter);
      break;
    }

    m_fetch_adapter =
        static_cast<Parallel_reader_async_adapter *>(parallel_adapter);
    m_max_data_chunk_size = m_fetch_adapter->get_max_buffer_size();

    /** get mysql row information */
    m_fetch_adapter->get_mysql_row_info(
        m_mysql_row_info.m_ncols, m_mysql_row_info.m_row_len,
        m_mysql_row_info.m_col_offsets, m_mysql_row_info.m_null_byte_offsets,
        m_mysql_row_info.m_null_bitmasks);

    /** prepare the fetch ctx for parallel work thread */
    m_parallel_read_thread_ctx_for_fetch =
        ut::new_arr_withkey<table_fetch_handler_thread_ctx_t>(
            UT_NEW_THIS_FILE_PSI_KEY, ut::Count{read_thread_count});

    if (nullptr == m_parallel_read_thread_ctx_for_fetch) {
      err = DB_OUT_OF_MEMORY;
      break;
    }

    std::vector<Item *> pushed_conds;
    std::vector<mem_root_deque<Item_field *> *> cond_fields;

    if (innobase_handler->pushed_cond) {
      m_fetch_adapter->get_push_cond(pushed_conds, cond_fields);
      ut_a(read_thread_count == pushed_conds.size());
    }

    m_parallel_read_thread_cb_cookies.reserve(read_thread_count);
    for (size_t i = 0; i < read_thread_count; ++i) {
      m_parallel_read_thread_ctx_for_fetch[i].m_parallel_read_work_started =
          false;
      err = m_parallel_read_thread_ctx_for_fetch[i]
                .m_data_buf_list.init_fetch_data_buffer_list(
                    m_max_data_chunk_size, 2);
      if (err) {
        err = DB_OUT_OF_MEMORY;
        break;
      }
      if (innobase_handler->pushed_cond) {
        m_parallel_read_thread_ctx_for_fetch[i].m_pushed_cond = pushed_conds[i];
        m_parallel_read_thread_ctx_for_fetch[i].m_cond_fields = cond_fields[i];
      }

      m_parallel_read_thread_cb_cookies.emplace_back(
          m_parallel_read_thread_ctx_for_fetch + i);
    }

    if (err) {
      break;
    }

    /** init the data queue */
    err = m_fetch_data_queue.init_queue(read_thread_count * 4);
    if (err) {
      break;
    }

    ParallelReaderHandler::mysql_row_desc_t reader_mysql_row_info;
    reader_mysql_row_info.m_ncols = m_mysql_row_info.m_ncols;
    reader_mysql_row_info.m_col_offsets = m_mysql_row_info.m_col_offsets;
    reader_mysql_row_info.m_null_bitmasks = m_mysql_row_info.m_null_bitmasks;
    reader_mysql_row_info.m_null_byte_offsets =
        m_mysql_row_info.m_null_byte_offsets;
    reader_mysql_row_info.m_row_len = m_mysql_row_info.m_row_len;

    data_consumer->set_row_info_for_parse(reader_mysql_row_info);
    /** store "this" as the server layer context */
    data_consumer->set_engine_ctx(static_cast<void *>(this));

    data_consumer->set_data_read_max_chunk_size(m_max_data_chunk_size);

    m_thread_alloc_count = read_thread_count;

  } while (false);

  if (err) {
    /** clean the resource because of error:
     * 1. the table_fetch_handler resource can released by
     *    delete the handler object.
     * 2. here, we just clean the adapter object.
     */

    m_parallel_read_thread_cb_cookies.clear();

    innobase_handler->parallel_fetch_end(static_cast<void *>(m_fetch_adapter));
    m_fetch_adapter = nullptr;
  }

  return err;
}

int Parallel_reader_table_fetch_handler::table_fetch_start(
    parallel_read_data_reader_t *data_consumer) {
  int err = 0;

  /** the fetch object must be already ready */
  ut_a(m_fetch_adapter);
  using std::placeholders::_1;
  using std::placeholders::_2;
  using std::placeholders::_3;
  using std::placeholders::_4;
  using std::placeholders::_5;
  using std::placeholders::_6;

  fetch_Init_fn fetch_init_func = std::bind(
      &Parallel_reader_table_fetch_handler::fetch_init_cbk, this, _1 /*cookie*/,
      _2 /*ncols*/, _3 /*row_len*/, _4 /* col_offsets*/,
      _5 /*null_byte_offsets*/, _6 /*null_bitmasks*/);

  fetch_Load_fn scan_load_func = std::bind(
      &Parallel_reader_table_fetch_handler::fetch_data_cbk, this, _1 /*cookie*/,
      _2 /*nrows*/, _3 /*rowdata*/, _4 /*partition_id*/);

  fetch_End_fn scan_end_func = std::bind(
      &Parallel_reader_table_fetch_handler::fetch_end_cbk, this, _1 /*cookie*/);

  Parallel_reader_async_adapter::range_start_fn_t range_start_func = std::bind(
      &Parallel_reader_table_fetch_handler::range_start_cbk, this, _1, _2);

  Parallel_reader_async_adapter::range_end_fn_t range_end_func = std::bind(
      &Parallel_reader_table_fetch_handler::range_end_cbk, this, _1, _2);

  ha_innobase *innobase_handler =
      dynamic_cast<ha_innobase *>(data_consumer->get_target_handler());

  m_fetch_adapter->range_adapter_callback(range_start_func, range_end_func);

  err = innobase_handler->parallel_fetch(
      static_cast<void *>(m_fetch_adapter),
      static_cast<void **>(m_parallel_read_thread_cb_cookies.data()),
      fetch_init_func, scan_load_func, scan_end_func);

  /** if the parallel fetch return error, the parallel read layer should
   *  guarantee the work thread will not running, just like never create
   * the thread: this is convenience to do error handle since we don't know
   * how many threads have been started.
   */
  if (err) {
    innobase_handler->parallel_fetch_end(static_cast<void *>(m_fetch_adapter));
    m_fetch_adapter = nullptr;
  }

  return err;
}

int Parallel_reader_table_fetch_handler::table_fetch_end(
    parallel_read_data_reader_t *data_consumer) {
  int err = 0;

  /** need wait all of thread to exit.*/
  m_stopped.store(true);

  /** we need drop data here since this is stop operation */
  drain_the_data_que();
  while (m_fetch_data_queue.wait_enque_count()) {
    drain_the_data_que();
  }
  m_fetch_adapter->wait_finish();

  ha_innobase *innobase_handler =
      dynamic_cast<ha_innobase *>(data_consumer->get_target_handler());

  innobase_handler->parallel_fetch_end(static_cast<void *>(m_fetch_adapter));
  m_fetch_adapter = nullptr;

  fetch_table_fetchout_que_item_t end_data_item;
  end_data_item.m_data_size = 0;
  m_fetch_data_queue.en_queue_head(end_data_item);

  while (data_consumer->get_active_read_count()) {
    std::this_thread::sleep_for(std::chrono::nanoseconds(100));
  }

  return err;
}

void Parallel_reader_table_fetch_handler::drain_the_data_que() {
  fetch_table_fetchout_que_item_t data_item;
  bool has_data = true;

  while (has_data) {
    data_item = m_fetch_data_queue.de_queue_non_block(has_data);
    if (has_data && data_item.m_data_size) {
      table_fetch_handler_thread_ctx_t *handler_thread_ctx =
          (m_parallel_read_thread_ctx_for_fetch + data_item.m_buffer_owner);

      handler_thread_ctx->m_data_buf_list.ret_fetch_data_buffer(
          data_item.m_buf_item);
    }
  }
}

bool Parallel_reader_table_fetch_handler::fetch_init_cbk(
    void *cookie, ulong ncols [[maybe_unused]], ulong row_len [[maybe_unused]],
    const ulong *col_offsets [[maybe_unused]],
    const ulong *null_byte_offsets [[maybe_unused]],
    const ulong *null_bitmasks [[maybe_unused]]) {
  if (m_stopped.load(std::memory_order_seq_cst)) {
    return true;
  }

  table_fetch_handler_thread_ctx_t *thread_ctx_for_cb =
      static_cast<table_fetch_handler_thread_ctx_t *>(cookie);

  thread_ctx_for_cb->m_parallel_read_work_started = true;
  thread_ctx_for_cb->m_parallel_read_work_thread_index =
      (thread_ctx_for_cb - m_parallel_read_thread_ctx_for_fetch);
  m_thread_launch_count.fetch_add(1, std::memory_order_relaxed);

  return false;
}

bool Parallel_reader_table_fetch_handler::fetch_data_cbk(
    void *cookie [[maybe_unused]], uint nrows [[maybe_unused]],
    void *rowdata [[maybe_unused]], uint64_t partition_id [[maybe_unused]]) {
  if (m_stopped.load(std::memory_order_relaxed)) {
    return true;
  }

  size_t actual_data_size = (m_mysql_row_info.m_row_len * nrows);
  table_fetch_handler_thread_ctx_t *thread_ctx_for_cb =
      static_cast<table_fetch_handler_thread_ctx_t *>(cookie);
  ut_a(actual_data_size <= m_max_data_chunk_size);

  table_fetch_buf_t fetch_buf =
      thread_ctx_for_cb->m_data_buf_list.get_fetch_data_buffer(
          actual_data_size);
  /** need detect the error for the buffer acquire */
  if (nullptr == fetch_buf.m_buf) {
    /** we don't the handle the memory allocation failure.
     *  we just notify the work thread about the failure
     *  which will cause the read failure.
     */
    return true;
  }

  if (thread_ctx_for_cb->m_blob_heap) {
    thread_ctx_for_cb->m_data_buf_list.attach_blob_heap_to_buffer(
        fetch_buf, thread_ctx_for_cb->m_blob_heap);

    /** mark this heap will be free by the data consumer thread. */
    thread_ctx_for_cb->m_blob_heap = nullptr;
  }

  if (thread_ctx_for_cb->m_compress_cols_heap) {
    thread_ctx_for_cb->m_data_buf_list.attach_compress_heap_to_buffer(
        fetch_buf, thread_ctx_for_cb->m_compress_cols_heap);
    thread_ctx_for_cb->m_compress_cols_heap = nullptr;
  }

  memcpy(fetch_buf.m_buf, rowdata, actual_data_size);

  thread_ctx_for_cb->m_total_records += nrows;

  srv_parallel_fetch_buffer_engine_used.fetch_add(1);

  fetch_table_fetchout_que_item_t data_item;
  data_item.m_buf_item = fetch_buf;
  data_item.m_rows_count = nrows;
  data_item.m_data_size = actual_data_size;
  data_item.m_partition_id = partition_id;
  data_item.m_buffer_owner =
      thread_ctx_for_cb->m_parallel_read_work_thread_index;
  /** maybe blocked here because of the queue is full */
  m_fetch_data_queue.en_queue(data_item);

  return m_stopped.load(std::memory_order_relaxed);
}

void Parallel_reader_table_fetch_handler::fetch_end_cbk(void *cookie) {
  table_fetch_handler_thread_ctx_t *thread_ctx_for_cb =
      static_cast<table_fetch_handler_thread_ctx_t *>(cookie);

  /** do not need check the stop flag since it is finishing */
  thread_ctx_for_cb->m_parallel_read_work_finished = true;
  m_thread_finished_count.fetch_add(1, std::memory_order_relaxed);

  if (m_thread_finished_count.load() == m_thread_alloc_count) {
    fetch_table_fetchout_que_item_t data_item;
    data_item.m_data_size = 0;
    m_fetch_data_queue.en_queue(data_item);
  }
}

int Parallel_reader_table_fetch_handler::range_start_cbk(
    void *cookie, std::shared_ptr<Parallel_reader::Ctx> &current_ctx) {
  int err = 0;
  table_fetch_handler_thread_ctx_t *thread_ctx_for_cb =
      static_cast<table_fetch_handler_thread_ctx_t *>(cookie);

  thread_ctx_for_cb->m_current_range_id = current_ctx->id();

  return err;
}

int Parallel_reader_table_fetch_handler::range_end_cbk(
    void *cookie, std::shared_ptr<Parallel_reader::Ctx> &current_ctx) {
  int err = 0;

  table_fetch_handler_thread_ctx_t *thread_ctx_for_cb =
      static_cast<table_fetch_handler_thread_ctx_t *>(cookie);
  thread_ctx_for_cb->m_last_range_id = current_ctx->id();
  thread_ctx_for_cb->m_current_range_id = 0;
  thread_ctx_for_cb->m_processed_range_count += 1;

  return err;
}

/*******the handle interface for fetch**********/

int ha_innobase::parallel_fetch_init(
    void *&fetch_ctx, size_t *num_threads, bool use_reserved_threads,
    uint desire_threads, uint fetch_index,
    parallel_read_data_reader_range_t *to_fetch_range,
    unsigned long int max_read_buf_size) {
  if (dict_table_is_discarded(m_prebuilt->table)) {
    ib_senderrf(ha_thd(), IB_LOG_LEVEL_ERROR, ER_TABLESPACE_DISCARDED,
                table->s->table_name.str);

    return (HA_ERR_NO_SUCH_TABLE);
  }

  m_prebuilt->is_in_ap_parallel_read_ctx = true;
  update_thd();
  auto trx = m_prebuilt->trx;
  innobase_register_trx(ht, ha_thd(), trx);

  trx_start_if_not_started_xa(trx, false, UT_LOCATION_HERE);

  trx_assign_read_view(trx);

  size_t max_threads = desire_threads;

  if (0 == max_threads) {
    max_threads = thd_parallel_read_threads(m_prebuilt->trx->mysql_thd);
  }

  max_threads =
      Parallel_reader::available_threads(max_threads, use_reserved_threads);

  /** In the fetch context, we may need more dedicated thhread control.*/
  if (max_threads == 0) {
    return (HA_ERR_GENERIC);
  }

  const auto row_len = m_prebuilt->mysql_row_len;

  auto adapter = ut::new_withkey<Parallel_reader_async_adapter>(
      UT_NEW_THIS_FILE_PSI_KEY, max_threads, row_len, max_read_buf_size);

  if (nullptr == adapter) {
    /** if the thread control policy change, the release thread number also need
     * change*/
    Parallel_reader::release_threads(max_threads);
    return (HA_ERR_OUT_OF_MEM);
  }

  if (nullptr != to_fetch_range->m_min_key ||
      nullptr != to_fetch_range->m_max_key) {
    /** range must be use the desired index */
    ut_a(to_fetch_range->m_keynr == fetch_index);
  }

  int cai_err = change_active_index(fetch_index);

  if (cai_err) {
    ut::delete_(adapter);
    adapter = nullptr;
    return cai_err;
  }

  ut_a(m_prebuilt->index);

  if (!m_prebuilt->index->is_clustered() &&
      !adapter->is_readset_set_sec_index(table, m_prebuilt->index,
                                         fetch_index)) {
    /** set the hint to read all secondary index fields */
    adapter->set_readset_sec_index(table, m_prebuilt->index, fetch_index);
    build_template(false);
  }

  adapter->build_scan_range(m_prebuilt, to_fetch_range);

  Parallel_reader::Scan_range scan_range = adapter->get_scan_range();

  Parallel_reader::Config config(scan_range, m_prebuilt->index);
  config.m_is_greatdb_ap_read = true;
  config.m_is_read_cluster_index = m_prebuilt->index->is_clustered();
  config.m_is_need_cluster_rec = m_prebuilt->index->is_clustered()
                                     ? false
                                     : m_prebuilt->need_to_access_clustered;
  config.m_is_greatdb_ap_desc_fetch = to_fetch_range->is_desc_fetch;

  dberr_t err =
      adapter->add_scan(trx, config, [=](const Parallel_reader::Ctx *ctx) {
        return (adapter->process_rows(ctx));
      });

  if (err != DB_SUCCESS) {
    ut::delete_(adapter);
    return (convert_error_code_to_mysql(err, 0, ha_thd()));
  }

  fetch_ctx = adapter;
  *num_threads = max_threads;

  /** non-partition table support turbo condition push down */
  std::vector<Item *> pushed_conds;
  std::vector<mem_root_deque<Item_field *> *> cond_fields;
  if (this->pushed_cond) {
    pushed_conds.reserve(max_threads);
    cond_fields.reserve(max_threads);

    for (size_t i = 0; i < max_threads; ++i) {
      mem_root_deque<Item_field *> *all_field_items = nullptr;
      Item *clone_push_cond = nullptr;
      clone_push_cond =
          (const_cast<Item *>(this->pushed_cond))
              ->clone_pushed_cond(current_thd, this->table, &all_field_items);
      if (nullptr == clone_push_cond || nullptr == all_field_items) {
        err = DB_OUT_OF_MEMORY;
        break;
      }
      clone_push_cond->thd_belonged_to = pushed_cond->thd_belonged_to;
      pushed_conds.emplace_back(clone_push_cond);
      cond_fields.emplace_back(all_field_items);
    }
  }

  if (err != DB_SUCCESS) {
    fetch_ctx = nullptr;
    ut::delete_(adapter);
    return (convert_error_code_to_mysql(err, 0, ha_thd()));
  }

  if (this->pushed_cond) {
    adapter->set_pushed_cond(pushed_conds, cond_fields);
  }

  if (m_prebuilt->index->is_clustered()) {
    if (bitmap_is_clear_all(table->read_set)) {
      build_template(true);
    } else {
      build_template(false);
    }
  } else {
    build_template(false);
  }

  adapter->set(m_prebuilt);

  return (0);
}

int ha_innobase::parallel_fetch(void *fetch_ctx, void **thread_ctxs,
                                Reader::Init_fn init_fn,
                                Reader::Load_fn load_fn,
                                Reader::End_fn end_fn) {
  if (dict_table_is_discarded(m_prebuilt->table)) {
    ib_senderrf(ha_thd(), IB_LOG_LEVEL_ERROR, ER_TABLESPACE_DISCARDED,
                table->s->table_name.str);

    return (HA_ERR_NO_SUCH_TABLE);
  }

  ut_a(fetch_ctx != nullptr);

  update_thd();

  if (m_prebuilt->index == m_prebuilt->index->table->first_index()) {
    if (bitmap_is_clear_all(table->read_set)) {
      build_template(true);
    } else {
      build_template(false);
    }
  } else {
    build_template(false);
  }

  auto adapter = static_cast<Parallel_reader_async_adapter *>(fetch_ctx);

  auto err = adapter->async_run(thread_ctxs, init_fn, load_fn, end_fn);

  return (convert_error_code_to_mysql(err, 0, ha_thd()));
}

void ha_innobase::parallel_fetch_end(void *fetch_ctx) {
  m_prebuilt->is_in_ap_parallel_read_ctx = false;

  if (nullptr == fetch_ctx) {
    return;
  }
  Parallel_reader_async_adapter *parallel_reader =
      static_cast<Parallel_reader_async_adapter *>(fetch_ctx);

  if (!m_prebuilt->index->is_clustered() &&
      parallel_reader->is_adapter_set_sec_index_readset()) {
    /** because we set the bitmap in the function in parallel_fetch_init.
     * so we clear it here to avoid effect other function.
     */
    bitmap_clear_all(table->read_set);
  }

  ut::delete_(parallel_reader);
}

/**************Partition table support parallel fetch rows *************/

/** num_threads will return the actual read thread count */

int ha_innopart::parallel_fetch_init(
    void *&fecth_ctx, size_t *num_threads, bool use_reserved_threads,
    uint desire_threads, uint fetch_index,
    parallel_read_data_reader_range_t *to_fetch_range,
    unsigned long int max_read_buf_size) {
  int ret = 0;

  if (nullptr != to_fetch_range->m_min_key ||
      nullptr != to_fetch_range->m_max_key) {
    ut_a(to_fetch_range->m_keynr == fetch_index);
  }

  ulint hint_need_to_fetch_extra_cols [[maybe_unused]] = 0;

  fecth_ctx = nullptr;
  m_prebuilt->is_in_ap_parallel_read_ctx = true;

  /** 0 ==  desire_threads means caller not assign the parallel read thread;
   *  and want to use the session variable innodb_parallel_read_threads.
   */
  size_t read_threads = desire_threads;

  if (0 == read_threads) {
    read_threads = thd_parallel_read_threads(m_prebuilt->trx->mysql_thd);
    ut_a(read_threads <= Parallel_reader::MAX_THREADS);
  }

  read_threads = static_cast<ulong>(
      Parallel_reader::available_threads(read_threads, use_reserved_threads));

  if (read_threads == 0) {
    return (HA_ERR_GENERIC);
  }

  const auto row_len = m_prebuilt->mysql_row_len;

  auto adapter = ut::new_withkey<Parallel_reader_async_adapter>(
      UT_NEW_THIS_FILE_PSI_KEY, read_threads, row_len);

  if (adapter == nullptr) {
    /** if thread policy changed, this also need changed */
    Parallel_reader::release_threads(read_threads);
    return (HA_ERR_OUT_OF_MEM);
  }

  auto trx = m_prebuilt->trx;

  innobase_register_trx(ht, ha_thd(), trx);

  trx_start_if_not_started_xa(trx, false, UT_LOCATION_HERE);

  trx_assign_read_view(trx);

  /** currently, we create one range for all partitions.
   * and maybe need do more detail to get sub ranges for particular partition.
   */
  adapter->build_scan_range(m_prebuilt, to_fetch_range);
  const Parallel_reader::Scan_range scan_range = adapter->get_scan_range();

  /** handle partition scan */
  const auto first_used_partition = m_part_info->get_first_used_partition();

  for (auto i = first_used_partition; i < m_tot_parts;
       i = m_part_info->get_next_used_partition(i)) {
    set_partition(i);

    if (dict_table_is_discarded(m_prebuilt->table)) {
      ib_senderrf(ha_thd(), IB_LOG_LEVEL_ERROR, ER_TABLESPACE_DISCARDED,
                  m_prebuilt->table->name.m_name);

      ut::delete_(adapter);
      return HA_ERR_NO_SUCH_TABLE;
    }

    change_active_index(i, fetch_index);

    if (m_prebuilt->index->is_clustered()) {
      build_template(true);
    } else {
      if (!adapter->is_readset_set_sec_index(table, m_prebuilt->index,
                                             fetch_index)) {
        /** set the hint to fetch all secondary index fields */
        adapter->set_readset_sec_index(table, m_prebuilt->index, fetch_index);
      }

      build_template(false);
    }

    Parallel_reader::Config config(scan_range, m_prebuilt->index, 0,
                                   i /** partition id */);
    config.m_is_greatdb_ap_read = true;
    config.m_is_read_cluster_index = m_prebuilt->index->is_clustered();
    config.m_is_need_cluster_rec = (m_prebuilt->index->is_clustered()
                                        ? false
                                        : m_prebuilt->need_to_access_clustered);

    dberr_t err =
        adapter->add_scan(trx, config, [=](const Parallel_reader::Ctx *ctx) {
          return (adapter->process_rows(ctx));
        });

    if (err != DB_SUCCESS) {
      ut::delete_(adapter);
      return (convert_error_code_to_mysql(err, 0, ha_thd()));
    }
  }

  /** Partition table support turbo condition push down */
  dberr_t err = DB_SUCCESS;
  std::vector<Item *> turbo_pushed_conds;
  std::vector<mem_root_deque<Item_field *> *> turbo_cond_fields;
  if (this->pushed_cond) {
    /** this indicate the turbo layer has been enable the condition push down.
     *  and we need keep the pushed condition and its related fields for
     *  multi-thread usage.
     */
    turbo_cond_fields.reserve(read_threads);
    turbo_cond_fields.reserve(read_threads);
    for (size_t i = 0; i < read_threads; ++i) {
      mem_root_deque<Item_field *> *cond_all_field_items = nullptr;
      Item *clone_push_cond = nullptr;
      clone_push_cond = (const_cast<Item *>(this->pushed_cond))
                            ->clone_pushed_cond(current_thd, this->table,
                                                &cond_all_field_items);
      if (nullptr == clone_push_cond || nullptr == cond_all_field_items) {
        err = DB_OUT_OF_MEMORY;
        break;
      }
      clone_push_cond->thd_belonged_to = this->pushed_cond->thd_belonged_to;
      turbo_pushed_conds.emplace_back(clone_push_cond);
      turbo_cond_fields.emplace_back(cond_all_field_items);
    }
  }

  if (DB_SUCCESS != err) {
    fecth_ctx = nullptr;
    ut::delete_(adapter);
    return (convert_error_code_to_mysql(err, 0, ha_thd()));
  }

  if (this->pushed_cond) {
    adapter->set_pushed_cond(turbo_pushed_conds, turbo_cond_fields);
  }

  /** support turbo condition push down end */

  fecth_ctx = adapter;
  *num_threads = read_threads;

  /** since all partitions use the same index(with respect to keynr)*/
  if (m_prebuilt->index->is_clustered()) {
    if (bitmap_is_clear_all(table->read_set)) {
      build_template(true);
    } else {
      build_template(false);
    }
  } else {
    build_template(false);
  }

  adapter->set(m_prebuilt);

  return ret;
}

int ha_innopart::parallel_fetch(void *fetch_ctx, void **thread_ctxs,
                                Reader::Init_fn init_fn,
                                Reader::Load_fn load_fn,
                                Reader::End_fn end_fn) {
  if (dict_table_is_discarded(m_prebuilt->table)) {
    ib_senderrf(ha_thd(), IB_LOG_LEVEL_ERROR, ER_TABLESPACE_DISCARDED,
                m_prebuilt->table->name.m_name);

    return (HA_ERR_NO_SUCH_TABLE);
  }

  ut_a(fetch_ctx != nullptr);

  if (m_prebuilt->index == m_prebuilt->index->table->first_index()) {
    if (bitmap_is_clear_all(table->read_set)) {
      build_template(true);
    } else {
      build_template(false);
    }
  } else {
    build_template(false);
  }

  auto adapter = static_cast<Parallel_reader_async_adapter *>(fetch_ctx);

  auto err = adapter->async_run(thread_ctxs, init_fn, load_fn, end_fn);

  return (convert_error_code_to_mysql(err, 0, ha_thd()));
}

void ha_innopart::parallel_fetch_end(void *fetch_ctx) {
  m_prebuilt->is_in_ap_parallel_read_ctx = false;
  if (nullptr == fetch_ctx) {
    return;
  }

  Parallel_reader_async_adapter *parallel_reader =
      static_cast<Parallel_reader_async_adapter *>(fetch_ctx);

  if (!m_prebuilt->index->is_clustered() &&
      parallel_reader->is_adapter_set_sec_index_readset()) {
    /** because we set the bitmap in the function in parallel_fetch_init.
     * so we clear it here to avoid effect other function.
     */
    bitmap_clear_all(table->read_set);
  }
  ut::delete_(parallel_reader);
}

#endif /* !UNIV_HOTBACKUP */
