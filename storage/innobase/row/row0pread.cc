/*****************************************************************************

Copyright (c) 2018, 2021, Oracle and/or its affiliates. All rights reserved.
Copyright (c) 2021, Huawei Technologies Co., Ltd.
Copyright (c) 2021, GreatDB Software Co., Ltd

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

/** @file row/row0pread.cc
Parallel read implementation

Created 2018-01-27 by Sunny Bains */

#include <array>

#include "btr0pcur.h"
#include "dict0dict.h"
#include "os0thread-create.h"
#include "row0mysql.h"
#include "row0pread.h"
#include "row0row.h"
#include "row0sel.h"
#include "row0vers.h"
#include "ut0new.h"

#ifdef UNIV_PFS_THREAD
mysql_pfs_key_t parallel_read_thread_key;
#endif /* UNIV_PFS_THREAD */

ICP_RESULT row_search_idx_cond_check(
    byte *mysql_rec,          /*!< out: record
                              in MySQL format (invalid unless
                              prebuilt->idx_cond == true and
                              we return ICP_MATCH) */
    row_prebuilt_t *prebuilt, /*!< in/out: prebuilt struct
                              for the table handle */
    const rec_t *rec,         /*!< in: InnoDB record */
    const ulint *offsets);    /*!< in: rec_get_offsets() */

std::atomic_size_t Parallel_reader::s_active_threads{};

/** Tree depth at which we decide to split blocks further. */
static constexpr size_t SPLIT_THRESHOLD{3};

/** No. of pages to scan, in the case of large tables, before the check for
trx interrupted is made as the call is expensive. */
static constexpr size_t TRX_IS_INTERRUPTED_PROBE{50000};

std::string Parallel_reader::Scan_range::to_string() const {
  std::ostringstream os;

  os << "m_start: ";
  if (m_start != nullptr) {
    m_start->print(os);
  } else {
    os << "null";
  }
  os << ", m_end: ";
  if (m_end != nullptr) {
    m_end->print(os);
  } else {
    os << "null";
  }
  return (os.str());
}

Parallel_reader::Scan_ctx::Iter::~Iter() {
  if (m_heap == nullptr) {
    return;
  }

  if (m_pcur != nullptr) {
    m_pcur->free_rec_buf();
    /* Created with placement new on the heap. */
    call_destructor(m_pcur);
  }

  mem_heap_free(m_heap);
  m_heap = nullptr;
}

Parallel_reader::Ctx::~Ctx() {
  if (m_blob_heap) mem_heap_free(m_blob_heap);

  if (m_heap) mem_heap_free(m_heap);
}

Parallel_reader::Scan_ctx::~Scan_ctx() {}

Parallel_reader::~Parallel_reader() {
  mutex_destroy(&m_mutex);
  os_event_destroy(m_event);

  if (!m_single_threaded_mode) {
    release_unused_threads(m_n_threads);
  }

  for (auto thread_ctx : m_thread_ctxs) {
    if (thread_ctx != nullptr) {
      UT_DELETE(thread_ctx);
    }
  }
}

size_t Parallel_reader::available_threads(size_t n_required,
                                          bool use_reserved) {
  const auto RELAXED = std::memory_order_relaxed;
  auto active = s_active_threads.fetch_add(n_required, RELAXED);

  size_t max_threads = MAX_THREADS;

  if (use_reserved) {
    max_threads += MAX_RESERVED_THREADS;
  }

  if (active < max_threads) {
    const auto available = max_threads - active;

    if (n_required <= available) {
      return (n_required);
    } else {
      s_active_threads.fetch_sub(n_required - available, RELAXED);
      return (available);
    }
  }

  s_active_threads.fetch_sub(n_required, RELAXED);

  return (0);
}

void Parallel_reader::Scan_ctx::index_s_lock() {
  if (m_s_locks.fetch_add(1, std::memory_order_acquire) == 0) {
    auto index = m_config.m_index;
    /* The latch can be unlocked by a thread that didn't originally lock it. */
    rw_lock_s_lock_gen(dict_index_get_lock(index), true);
  }
}

void Parallel_reader::Scan_ctx::index_s_unlock() {
  if (m_s_locks.fetch_sub(1, std::memory_order_acquire) == 1) {
    auto index = m_config.m_index;
    /* The latch can be unlocked by a thread that didn't originally lock it. */
    rw_lock_s_unlock_gen(dict_index_get_lock(index), true);
  }
}

dberr_t Parallel_reader::Ctx::split() {
  ut_ad(m_range.first->m_tuple == nullptr ||
        dtuple_validate(m_range.first->m_tuple));
  ut_ad(m_range.second->m_tuple == nullptr ||
        dtuple_validate(m_range.second->m_tuple));

  /* Setup the sub-range. */
  Scan_range scan_range(m_range.first->m_tuple, m_range.second->m_tuple);

  /* S lock so that the tree structure doesn't change while we are
  figuring out the sub-trees to scan. */
  m_scan_ctx->index_s_lock();

  Parallel_reader::Scan_ctx::Ranges ranges{};
  m_scan_ctx->partition(scan_range, ranges, 1);

  if (!ranges.empty()) {
    ranges.back().second = m_range.second;
  }

  dberr_t err{DB_SUCCESS};

  /* Create the partitioned scan execution contexts. */
  for (auto &range : ranges) {
    err = m_scan_ctx->create_context(range, false);

    if (err != DB_SUCCESS) {
      break;
    }
  }

  if (err != DB_SUCCESS) {
    m_scan_ctx->set_error_state(err);
  }

  m_scan_ctx->index_s_unlock();

  return err;
}

Parallel_reader::Parallel_reader(size_t max_threads, bool sync)
    : m_max_threads(max_threads),
      m_n_threads{max_threads},
      m_ctxs(),
      m_sync(sync),
      m_trx_for_slow_log(innobase_get_trx_for_slow_log()) {
  m_n_completed = 0;

  mutex_create(LATCH_ID_PARALLEL_READ, &m_mutex);

  m_event = os_event_create();
  m_sig_count = os_event_reset(m_event);
}

Parallel_reader::Scan_ctx::Scan_ctx(Parallel_reader *reader, size_t id,
                                    trx_t *trx,
                                    const Parallel_reader::Config &config,
                                    F &&f)
    : m_id(id), m_config(config), m_trx(trx), m_f(f), m_reader(reader) {}

/** Persistent cursor wrapper around btr_pcur_t */
class PCursor {
 public:
  /** Constructor.
  @param[in,out]  pcur  Persistent cursor in use.
  @param[in]      mtr   Mini-transaction used by the persistent cursor.
  @param[in]      read_level  read level where the block should be present. */
  PCursor(btr_pcur_t *pcur, mtr_t *mtr, size_t read_level)
      : m_mtr(mtr), m_pcur(pcur), m_read_level(read_level) {}

  /** Check if there are threads waiting on the index latch. Yield the latch
  so that other threads can progress. */
  void yield();
  void yield_prev();

  /** Move to the next block.
  @param[in]  index  Index being traversed.
  @return DB_SUCCESS or error code. */
  dberr_t move_to_next_block(dict_index_t *index)
      MY_ATTRIBUTE((warn_unused_result));

  dberr_t move_to_prev_block(dict_index_t *index)
      MY_ATTRIBUTE((warn_unused_result));

  /** Restore the cursor position. */
  void restore_position() {
    auto relative = m_pcur->m_rel_pos;

    auto equal =
        m_pcur->restore_position(BTR_SEARCH_LEAF, m_mtr, __FILE__, __LINE__);

    if (relative == BTR_PCUR_ON) {
      if (!equal) {
        page_cur_move_to_next(m_pcur->get_page_cur());
      }
    } else {
      ut_ad(relative == BTR_PCUR_AFTER ||
            relative == BTR_PCUR_AFTER_LAST_IN_TREE ||
            relative == BTR_PCUR_BEFORE);
    }
  }

 private:
  /** Mini-transaction. */
  mtr_t *m_mtr{};

  /** Persistent cursor. */
  btr_pcur_t *m_pcur{};

  /** Level where the cursor is positioned or need to be positioned in case of
  restore. */
  size_t m_read_level{};
};

buf_block_t *Parallel_reader::Scan_ctx::block_get_s_latched(
    const page_id_t &page_id, mtr_t *mtr, int line) const {
  auto block = buf_page_get_gen(page_id, m_config.m_page_size, RW_S_LATCH,
                                nullptr, Page_fetch::SCAN, __FILE__, line, mtr);

  buf_block_dbg_add_level(block, SYNC_TREE_NODE);

  return (block);
}

void PCursor::yield() {
  /* We should always yield on a block boundary. */
  ut_ad(m_pcur->is_after_last_on_page());

  /* Store the cursor position on the last user record on the page. */
  m_pcur->move_to_prev_on_page();

  m_pcur->store_position(m_mtr);

  m_mtr->commit();

  /* Yield so that another thread can proceed. */
  std::this_thread::yield();

  m_mtr->start();

  m_mtr->set_log_mode(MTR_LOG_NO_REDO);

  /* Restore position on the record, or its predecessor if the record
  was purged meanwhile. */

  restore_position();

  if (!m_pcur->is_after_last_on_page()) {
    /* Move to the successor of the saved record. */
    m_pcur->move_to_next_on_page();
  }
}

void PCursor::yield_prev() {
  /* We should always yield on a block boundary. */
  ut_ad(m_pcur->is_before_first_on_page());

  /* Store the cursor position on the first user record on the page. */
  m_pcur->move_to_next_on_page();

  m_pcur->store_position(m_mtr);

  m_mtr->commit();

  /* Yield so that another thread can proceed. */
  std::this_thread::yield();

  m_mtr->start();

  m_mtr->set_log_mode(MTR_LOG_NO_REDO);

  /* Restore position on the record, or its predecessor if the record
  was purged meanwhile. */

  restore_position();

  if (!m_pcur->is_before_first_on_page()) {
    /* Move to the successor of the saved record. */
    m_pcur->move_to_prev_on_page();
  }
}

dberr_t PCursor::move_to_next_block(dict_index_t *index) {
  ut_ad(m_pcur->is_after_last_on_page());

  if (rw_lock_get_waiters(dict_index_get_lock(index))) {
    /* There are waiters on the index tree lock. Store and restore
    the cursor position, and yield so that scanning a large table
    will not starve other threads. */

    yield();

    /* It's possible that the restore places the cursor in the middle of
    the block. We need to account for that too. */

    if (m_pcur->is_on_user_rec()) {
      return (DB_SUCCESS);
    }
  }

  auto cur = m_pcur->get_page_cur();
  auto next_page_no = btr_page_get_next(page_cur_get_page(cur), m_mtr);

  if (next_page_no == FIL_NULL) {
    m_mtr->commit();
    return (DB_END_OF_INDEX);
  }

  auto block = page_cur_get_block(cur);
  const auto &page_id = block->page.id;

  block = buf_page_get_gen(page_id_t(page_id.space(), next_page_no),
                           block->page.size, RW_S_LATCH, nullptr,
                           Page_fetch::SCAN, __FILE__, __LINE__, m_mtr);

  buf_block_dbg_add_level(block, SYNC_TREE_NODE);

  if (page_is_leaf(buf_block_get_frame(block))) {
    btr_leaf_page_release(page_cur_get_block(cur), RW_S_LATCH, m_mtr);
  }

  page_cur_set_before_first(block, cur);

  /* Skip the infimum record. */
  page_cur_move_to_next(cur);

  /* Page can't be empty unless it is a root page. */
  ut_ad(!page_cur_is_after_last(cur));

  return (DB_SUCCESS);
}

dberr_t PCursor::move_to_prev_block(dict_index_t *index) {
  ut_ad(m_pcur->is_before_first_on_page());

  if (rw_lock_get_waiters(dict_index_get_lock(index))) {
    /* There are waiters on the index tree lock. Store and restore
    the cursor position, and yield so that scanning a large table
    will not starve other threads. */

    yield_prev();

    /* It's possible that the restore places the cursor in the middle of
    the block. We need to account for that too. */

    if (m_pcur->is_on_user_rec()) {
      return (DB_SUCCESS);
    }
  }

  auto cur = m_pcur->get_page_cur();

  auto prev_page_no = btr_page_get_prev(page_cur_get_page(cur), m_mtr);

  if (prev_page_no == FIL_NULL) {
    m_mtr->commit();

    return (DB_END_OF_INDEX);
  }

  m_pcur->move_backward_from_page(m_mtr);

  /* Skip the supremum record. */
  page_cur_move_to_prev(cur);

  /* Page can't be empty unless it is a root page. */
  ut_ad(!page_cur_is_before_first(cur));

  return (DB_SUCCESS);
}

bool Parallel_reader::Scan_ctx::check_visibility(const rec_t *&rec,
                                                 ulint *&offsets,
                                                 mem_heap_t *&heap,
                                                 mtr_t *mtr) {
  const auto table_name = m_config.m_index->table->name;

  ut_ad(!m_trx || m_trx->read_view == nullptr ||
        MVCC::is_view_active(m_trx->read_view));

  if (!m_trx) {
    /* Do nothing */
  } else if (m_trx->read_view != nullptr) {
    auto view = m_trx->read_view;

    if (m_config.m_index->is_clustered()) {
      trx_id_t rec_trx_id;

      if (m_config.m_index->trx_id_offset > 0) {
        rec_trx_id = trx_read_trx_id(rec + m_config.m_index->trx_id_offset);
      } else {
        rec_trx_id = row_get_rec_trx_id(rec, m_config.m_index, offsets);
      }

      if (m_trx->isolation_level > TRX_ISO_READ_UNCOMMITTED &&
          !view->changes_visible(rec_trx_id, table_name)) {
        rec_t *old_vers;

        row_vers_build_for_consistent_read(rec, mtr, m_config.m_index, &offsets,
                                           view, &heap, heap, &old_vers,
                                           nullptr, nullptr);

        rec = old_vers;

        if (rec == nullptr) {
          return (false);
        }
      }
    } else {
      /* Secondary index scan not supported yet. */
      auto max_trx_id = page_get_max_trx_id(page_align(rec));

      ut_ad(max_trx_id > 0);

      if (!view->sees(max_trx_id)) {
        /* FIXME: This is not sufficient. We may need to read in the cluster
        index record to be 100% sure. */
        return (false);
      }
    }
  }

  if (rec_get_deleted_flag(rec, m_config.m_is_compact)) {
    /* This record was deleted in the latest committed version, or it was
    deleted and then reinserted-by-update before purge kicked in. Skip it. */
    return (false);
  }

  return (true);
}

dberr_t Parallel_reader::Scan_ctx::find_visible_record(
    byte *buf, const rec_t *&rec, const rec_t *&clust_rec, ulint *&offsets,
    ulint *&clust_offsets, mem_heap_t *&heap, mtr_t *mtr,
    row_prebuilt_t *prebuilt) {
  const auto table_name = m_config.m_index->table->name;
  ut_ad(m_trx->read_view == nullptr || MVCC::is_view_active(m_trx->read_view));
  if (prebuilt != nullptr) {
    prebuilt->pq_requires_clust_rec = false;
  }
  if (m_trx->read_view != nullptr) {
    auto view = m_trx->read_view;

    if (m_config.m_index->is_clustered()) {
      trx_id_t rec_trx_id;

      if (m_config.m_index->trx_id_offset > 0) {
        rec_trx_id = trx_read_trx_id(rec + m_config.m_index->trx_id_offset);
      } else {
        rec_trx_id = row_get_rec_trx_id(rec, m_config.m_index, offsets);
      }

      if (m_trx->isolation_level > TRX_ISO_READ_UNCOMMITTED &&
          !view->changes_visible(rec_trx_id, table_name)) {
        rec_t *old_vers = nullptr;

        row_vers_build_for_consistent_read(rec, mtr, m_config.m_index, &offsets,
                                           view, &heap, heap, &old_vers,
                                           nullptr, nullptr);

        rec = old_vers;
        if (rec == nullptr) {
          return DB_NOT_FOUND;
        }
      }
    } else {
      /* Secondary index scan not supported yet. */
      auto max_trx_id = page_get_max_trx_id(page_align(rec));
      ut_ad(max_trx_id > 0);

      if (!view->sees(max_trx_id) ||
          (prebuilt && prebuilt->need_to_access_clustered)) {
        if (prebuilt) {
          if (prebuilt->idx_cond) {
            switch (row_search_idx_cond_check(buf, prebuilt, rec, offsets)) {
              case ICP_NO_MATCH:
                return DB_NOT_FOUND;
              case ICP_OUT_OF_RANGE:
                return DB_END_OF_RANGE;
              case ICP_MATCH:
                break;
            }
          }
          if (prebuilt->sel_graph == nullptr) row_prebuild_sel_graph(prebuilt);

          Row_sel_get_clust_rec_for_mysql row_sel_get_clust_rec_for_mysql;
          que_thr_t *thr = que_fork_get_first_thr(prebuilt->sel_graph);

          prebuilt->pq_requires_clust_rec = true;
          int err = row_sel_get_clust_rec_for_mysql(
              prebuilt, m_config.m_index, rec, thr, &clust_rec, &clust_offsets,
              &heap, NULL, mtr, nullptr);

          if (err != DB_SUCCESS)
            return DB_NOT_FOUND;
          else {
            if (clust_rec == NULL) {
              /* The record did not exist in the read view */
              ut_ad(prebuilt->select_lock_type == LOCK_NONE);

              return DB_NOT_FOUND;
            } else if (rec_get_deleted_flag(clust_rec, m_config.m_is_compact)) {
              /* The record is delete marked: we can skip it */
              return DB_NOT_FOUND;
            } else {
              return DB_SUCCESS;
            }
          }
        } else
          return DB_NOT_FOUND;
      }
    }
  } else if (srv_read_only_mode && /** innodb_read_only */
             (prebuilt &&
              prebuilt->need_to_access_clustered && /** secondary index and
                                                       non-covered index */
              !m_config.m_index->is_clustered())) {
    if (prebuilt->idx_cond) {
      switch (row_search_idx_cond_check(buf, prebuilt, rec, offsets)) {
        case ICP_NO_MATCH:
          return DB_NOT_FOUND;
        case ICP_OUT_OF_RANGE:
          return DB_END_OF_RANGE;
        case ICP_MATCH:
          break;
      }
    }

    if (prebuilt->sel_graph == nullptr) row_prebuild_sel_graph(prebuilt);

    que_thr_t *thr = que_fork_get_first_thr(prebuilt->sel_graph);
    prebuilt->pq_requires_clust_rec = true;
    Row_sel_get_clust_rec_for_mysql row_sel_get_clust_rec_for_mysql;
    int err = row_sel_get_clust_rec_for_mysql(prebuilt, m_config.m_index, rec,
                                              thr, &clust_rec, &clust_offsets,
                                              &heap, NULL, mtr, nullptr);

    if (err != DB_SUCCESS)
      return DB_NOT_FOUND;
    else {
      if (clust_rec == NULL) {
        /* The record did not exist in the read view */
        ut_ad(prebuilt->select_lock_type == LOCK_NONE);

        return DB_NOT_FOUND;
      } else if (rec_get_deleted_flag(clust_rec, m_config.m_is_compact)) {
        /* The record is delete marked: we can skip it */
        return DB_NOT_FOUND;
      } else {
        return DB_SUCCESS;
      }
    }
  }

  if (rec_get_deleted_flag(rec, m_config.m_is_compact)) {
    /* This record was deleted in the latest committed version, or it was
    deleted and then reinserted-by-update before purge kicked in. Skip it. */
    return DB_NOT_FOUND;
  }

  return DB_SUCCESS;
}

void Parallel_reader::Scan_ctx::copy_row(const rec_t *rec, Iter *iter) const {
  iter->m_offsets = rec_get_offsets(rec, m_config.m_index, nullptr,
                                    ULINT_UNDEFINED, &iter->m_heap);

  /* Copy the row from the page to the scan iterator. The copy should use
  memory from the iterator heap because the scan iterator owns the copy. */
  auto rec_len = rec_offs_size(iter->m_offsets);

  auto copy_rec = static_cast<rec_t *>(mem_heap_alloc(iter->m_heap, rec_len));

  memcpy(copy_rec, rec, rec_len);

  iter->m_rec = copy_rec;

  auto tuple = row_rec_to_index_entry_low(iter->m_rec, m_config.m_index,
                                          iter->m_offsets, iter->m_heap);

  ut_ad(dtuple_validate(tuple));

  /* We have copied the entire record but we only need to compare the
  key columns when we check for boundary conditions. */
  const auto n_compare = dict_index_get_n_unique_in_tree(m_config.m_index);

  dtuple_set_n_fields_cmp(tuple, n_compare);

  iter->m_tuple = tuple;
}

std::shared_ptr<Parallel_reader::Scan_ctx::Iter>
Parallel_reader::Scan_ctx::create_persistent_cursor(
    const page_cur_t &page_cursor, mtr_t *mtr) const {
  ut_ad(index_s_own());

  std::shared_ptr<Iter> iter = std::make_shared<Iter>();

  iter->m_heap = mem_heap_create(sizeof(btr_pcur_t) + (srv_page_size / 16));

  auto rec = page_cursor.rec;

  const bool is_infimum = page_rec_is_infimum(rec);

  if (is_infimum) {
    rec = page_rec_get_next(rec);
  }

  if (page_rec_is_supremum(rec)) {
    /* Empty page, only root page can be empty. */
    ut_a(!is_infimum ||
         page_cursor.block->page.id.page_no() == m_config.m_index->page);
    return (iter);
  }

  void *ptr = mem_heap_alloc(iter->m_heap, sizeof(btr_pcur_t));

  ::new (ptr) btr_pcur_t();

  iter->m_pcur = reinterpret_cast<btr_pcur_t *>(ptr);

  iter->m_pcur->init(m_config.m_read_level);

  /* Make a copy of the rec. */
  copy_row(rec, iter.get());

  iter->m_pcur->open_on_user_rec(page_cursor, PAGE_CUR_GE,
                                 BTR_ALREADY_S_LATCHED | BTR_SEARCH_LEAF);

  ut_ad(btr_page_get_level(buf_block_get_frame(iter->m_pcur->get_block()),
                           mtr) == m_config.m_read_level);

  iter->m_pcur->store_position(mtr);
  iter->m_pcur->set_fetch_type(Page_fetch::SCAN);

  return (iter);
}

bool Parallel_reader::Ctx::move_to_next_node(PCursor *pcursor, mtr_t *mtr) {
  page_cur_t *cur MY_ATTRIBUTE((unused)) =
      m_range.first->m_pcur->get_page_cur();

  auto err = pcursor->move_to_next_block(const_cast<dict_index_t *>(index()));

  if (err != DB_SUCCESS) {
    ut_a(err == DB_END_OF_INDEX);
    return (false);
  }

  /* Page can't be empty unless it is a root page. */
  ut_ad(!page_cur_is_after_last(cur));
  ut_ad(!page_cur_is_before_first(cur));

  return (true);
}

dberr_t Parallel_reader::Ctx::read_record(uchar *buf,
                                          row_prebuilt_t *prebuilt) {
  mtr_t mtr;
  btr_pcur_t *pcur;

  dberr_t err{DB_SUCCESS};
  dberr_t err1{DB_SUCCESS};
  int ret{0};
  const rec_t *clust_rec = nullptr;
  const rec_t *rec = nullptr;
  const rec_t *result_rec = nullptr;
  ulint *offsets = offsets_;
  ulint *clust_offsets = clust_offsets_;

  if (start_read) {
    rec_offs_init(offsets_);
    rec_offs_init(clust_offsets_);
    start_read = false;
  }

  mtr.start();
  mtr.set_log_mode(MTR_LOG_NO_REDO);

  auto &from =
      m_scan_ctx->m_config.m_pq_reverse_scan ? m_range.second : m_range.first;
  pcur = from->m_pcur;

  PCursor pcursor(pcur, &mtr, m_scan_ctx->m_config.m_read_level);
  pcursor.restore_position();

  const auto &end_tuple = m_scan_ctx->m_config.m_pq_reverse_scan
                              ? m_range.first->m_tuple
                              : m_range.second->m_tuple;
  auto index = m_scan_ctx->m_config.m_index;
  auto cur = pcur->get_page_cur();
  dict_index_t *clust_index = index->table->first_index();

  if (m_blob_heap == nullptr) m_blob_heap = mem_heap_create(srv_page_size);
  if (m_heap == nullptr) m_heap = mem_heap_create(srv_page_size / 4);

  if (!m_scan_ctx->m_config.m_pq_reverse_scan && page_cur_is_after_last(cur)) {
    // pcur point to last record, move to next block
    mem_heap_empty(m_heap);
    offsets = offsets_;
    rec_offs_init(offsets_);

    err = pcursor.move_to_next_block(index);
    if (err != DB_SUCCESS) {
      ut_a(!mtr.is_active());
      return err;
    }
    ut_ad(!page_cur_is_before_first(cur));
  } else if (m_scan_ctx->m_config.m_pq_reverse_scan &&
             page_cur_is_before_first(cur)) {
    // pcur point to first record, move to prev block
    mem_heap_empty(m_heap);
    offsets = offsets_;
    rec_offs_init(offsets_);

    err = pcursor.move_to_prev_block(index);
    if (err != DB_SUCCESS) {
      ut_a(!mtr.is_active());
      return err;
    }
    ut_ad(!page_cur_is_after_last(cur));
  }

  // 1. read record
  rec = page_cur_get_rec(cur);
  offsets = rec_get_offsets(rec, index, offsets, ULINT_UNDEFINED, &m_heap);
  clust_offsets =
      rec_get_offsets(rec, index, clust_offsets, ULINT_UNDEFINED, &m_heap);

  // 2. find visible version record
  err1 = m_scan_ctx->find_visible_record(buf, rec, clust_rec, offsets,
                                         clust_offsets, m_heap, &mtr, prebuilt);

  if (err1 == DB_END_OF_RANGE) {
    err = DB_END_OF_RANGE;
    goto func_exit;
  }

  if (err1 != DB_NOT_FOUND) {
    // 3. check range boundary
    m_block = page_cur_get_block(cur);
    if (rec != nullptr && end_tuple != nullptr) {
      if (!m_scan_ctx->m_config.m_index->is_clustered() &&
          prebuilt->need_to_access_clustered)
        ret = ((dtuple_t *)end_tuple)
                  ->compare(clust_rec, index, clust_index, clust_offsets);
      else
        ret = end_tuple->compare(rec, index, offsets);

      /* Note: The range creation doesn't use MVCC. Therefore it's possible
      that the range boundary entry could have been deleted. */
      if ((!m_scan_ctx->m_config.m_pq_reverse_scan && ret <= 0) ||
          (m_scan_ctx->m_config.m_pq_reverse_scan && ret >= 0)) {
        m_scan_ctx->m_reader->ctx_completed_inc();
        err = DB_END_OF_RANGE;
        goto func_exit;
      }
    }

    // 4. convert record to mysql format
    if (prebuilt->pq_requires_clust_rec) {
      result_rec = clust_rec;
      if (!row_sel_store_mysql_rec(buf, prebuilt, clust_rec, nullptr, true,
                                   clust_index, prebuilt->index, clust_offsets,
                                   false, nullptr, m_blob_heap))
        err = DB_ERROR;
    } else {
      result_rec = rec;
      if (!row_sel_store_mysql_rec(buf, prebuilt, rec, nullptr,
                                   m_scan_ctx->m_config.m_index->is_clustered(),
                                   index, prebuilt->index, offsets, false,
                                   nullptr, m_blob_heap))
        err = DB_ERROR;
    }
    if (prebuilt->clust_index_was_generated) {
      pq_row_sel_store_row_id_to_prebuilt(
          prebuilt, result_rec, result_rec == rec ? index : clust_index,
          result_rec == rec ? offsets : clust_offsets);
    }
  } else {
    err = DB_NOT_FOUND;
    goto next_record;
  }

next_record:
  if (!m_scan_ctx->m_config.m_pq_reverse_scan)
    page_cur_move_to_next(cur);
  else
    page_cur_move_to_prev(cur);

func_exit:
  pcur->store_position(&mtr);
  ut_a(mtr.is_active());
  mtr.commit();

  return err;
}

dberr_t Parallel_reader::Ctx::traverse() {
  /* Take index lock if the requested read level is on a non-leaf level as the
  index lock is required to access non-leaf page.  */
  if (m_scan_ctx->m_config.m_read_level != 0) {
    m_scan_ctx->index_s_lock();
  }

  mtr_t mtr;
  mtr.start();
  mtr.set_log_mode(MTR_LOG_NO_REDO);

  auto &from = m_range.first;

  PCursor pcursor(from->m_pcur, &mtr, m_scan_ctx->m_config.m_read_level);
  pcursor.restore_position();

  dberr_t err{DB_SUCCESS};

  err = traverse_recs(&pcursor, &mtr);

  if (mtr.is_active()) {
    mtr.commit();
  }

  if (m_scan_ctx->m_config.m_read_level != 0) {
    m_scan_ctx->index_s_unlock();
  }

  return (err);
}

dberr_t Parallel_reader::Ctx::traverse_recs(PCursor *pcursor, mtr_t *mtr) {
  const auto &end_tuple = m_range.second->m_tuple;
  auto heap = mem_heap_create(srv_page_size / 4);
  auto index = m_scan_ctx->m_config.m_index;
  auto cur = m_range.first->m_pcur->get_page_cur();

  m_start = true;

  dberr_t err{DB_SUCCESS};

  while (err == DB_SUCCESS) {
    if (page_cur_is_after_last(cur)) {
      mem_heap_empty(heap);

      if (!(m_n_pages % TRX_IS_INTERRUPTED_PROBE) &&
          trx_is_interrupted(trx())) {
        err = DB_INTERRUPTED;
        break;
      }

      if (is_error_set() || !move_to_next_node(pcursor, mtr)) {
        break;
      }

      ++m_n_pages;
      m_first_rec = true;
    }

    ulint offsets_[REC_OFFS_NORMAL_SIZE];
    ulint *offsets = offsets_;

    rec_offs_init(offsets_);

    const rec_t *rec = page_cur_get_rec(cur);
    offsets = rec_get_offsets(rec, index, offsets, ULINT_UNDEFINED, &heap);

    if (end_tuple != nullptr) {
      ut_ad(rec != nullptr);

      /* Key value of a record can change only if the record is deleted or if
      its updated. And an update is essentially a delete + insert. So in both
      the cases we just delete mark the record and the original key value is
      preserved on the page.

      Since the range creation is based on the key values and the key value do
      not ever change the latest (non-MVCC) version of the record should always
      tell us correctly whether we're within the range or outside of it. */
      auto ret = end_tuple->compare(rec, index, offsets);

      /* Note: The range creation doesn't use MVCC. Therefore it's possible
      that the range boundary entry could have been deleted. */
      if (ret <= 0) {
        break;
      }
    }

    bool skip{};

    if (page_is_leaf(cur->block->frame)) {
      skip = !m_scan_ctx->check_visibility(rec, offsets, heap, mtr);
    }

    if (!skip) {
      m_rec = rec;
      m_block = cur->block;
      m_offsets = offsets;

      err = m_scan_ctx->m_f(this);

      m_start = false;
    }

    m_first_rec = false;

    page_cur_move_to_next(cur);
  }

  if (err != DB_SUCCESS) {
    m_scan_ctx->set_error_state(err);
  }

  m_thread_ctx->m_prev_partition_id = partition_id();

  mem_heap_free(heap);

  return (err);
}

void Parallel_reader::enqueue(std::shared_ptr<Ctx> ctx) {
  mutex_enter(&m_mutex);
  m_ctxs.push_back(ctx);
  mutex_exit(&m_mutex);
}

std::shared_ptr<Parallel_reader::Ctx> Parallel_reader::dequeue() {
  mutex_enter(&m_mutex);

  if (m_ctxs.empty()) {
    mutex_exit(&m_mutex);
    return (nullptr);
  }

  std::shared_ptr<Parallel_reader::Ctx> ctx{};
  if (!m_pq_reverse_scan) {
    ctx = m_ctxs.front();
    m_ctxs.pop_front();
  } else {
    ctx = m_ctxs.back();
    m_ctxs.pop_back();
  }

  mutex_exit(&m_mutex);

  return (ctx);
}

void Parallel_reader::pq_set_reverse_scan() { m_pq_reverse_scan = true; }

bool Parallel_reader::is_queue_empty() const {
  mutex_enter(&m_mutex);
  auto empty = m_ctxs.empty();
  mutex_exit(&m_mutex);
  return (empty);
}

void Parallel_reader::worker(Parallel_reader::Thread_ctx *thread_ctx) {
  dberr_t err{DB_SUCCESS};

  if (m_start_callback) {
    err = m_start_callback(thread_ctx);
  }

  /* Wait for all the threads to be spawned as it's possible that we could
  abort the operation if there are not enough resources to spawn all the
  threads. */
  constexpr auto FOREVER = OS_SYNC_INFINITE_TIME;
  os_event_wait_time_low(m_event, FOREVER, m_sig_count);

  for (;;) {
    size_t n_completed = 0;
    int64_t sig_count = os_event_reset(m_event);

    while (err == DB_SUCCESS && !is_error_set()) {
      auto ctx = dequeue();

      if (ctx == nullptr) {
        break;
      }

      auto scan_ctx = ctx->m_scan_ctx;

      if (scan_ctx->is_error_set()) {
        break;
      }

      ctx->m_thread_ctx = thread_ctx;

      if (ctx->m_split) {
        err = ctx->split();
        /* Tell the other threads that there is work to do. */
        os_event_set(m_event);
      } else {
        err = ctx->traverse();
      }

      /* Check for trx interrupted (useful in the case of small tables). */
      if (err == DB_SUCCESS && trx_is_interrupted(ctx->trx())) {
        err = DB_INTERRUPTED;
        scan_ctx->set_error_state(err);
        break;
      }

      ut_ad(err == DB_SUCCESS || scan_ctx->is_error_set());

      ++n_completed;
    }

    if (err != DB_SUCCESS || is_error_set()) {
      break;
    }

    m_n_completed.fetch_add(n_completed, std::memory_order_relaxed);

    if (m_n_completed == m_ctx_id) {
      /* Wakeup other worker threads before exiting */
      os_event_set(m_event);
      break;
    }

    constexpr auto FOREVER = OS_SYNC_INFINITE_TIME;

    os_event_wait_time_low(m_event, FOREVER, sig_count);
  }

  if (err != DB_SUCCESS) {
    /* Set the "global" error state. */
    if (!is_error_set()) {
      set_error_state(err);
    }
  }

  if (is_error_set()) {
    /* Wake up any sleeping threads. */
    os_event_set(m_event);
  }

  if (m_finish_callback) {
    dberr_t finish_err = m_finish_callback(thread_ctx);

    if (finish_err != DB_SUCCESS) {
      set_error_state(finish_err);
    }
  }

  ut_a(is_error_set() || (m_n_completed == m_ctx_id && is_queue_empty()));
}

void Parallel_reader::ctx_completed_inc() {
  m_n_completed.fetch_add(1, std::memory_order_relaxed);
}

void Parallel_reader::pq_set_worker_done() {
  work_done.store(true, std::memory_order_relaxed);
}

void Parallel_reader::pq_wakeup_workers() {
  pq_set_worker_done();
  os_event_set(m_event);
}

dberr_t Parallel_reader::dispatch_ctx(row_prebuilt_t *prebuilt) {
  dberr_t err{DB_SUCCESS};

  for (;;) {
    int64_t sig_count = os_event_reset(m_event);

    auto ctx = dequeue();
    bool done = work_done.load(std::memory_order_relaxed);

    if (ctx == nullptr) {
      prebuilt->ctx = nullptr;
      if (is_ctx_over() || done) {
        /* Wakeup other worker threads before exiting */
        os_event_set(m_event);
        ut_a(is_queue_empty());
        return DB_END_OF_INDEX;
      } else {
        /* wait for other worker */
        constexpr auto FOREVER = OS_SYNC_INFINITE_TIME;
        os_event_wait_time_low(m_event, FOREVER, sig_count);
      }
    } else {
      if (ctx->m_split) {
        err = ctx->split();
        /* Tell the other threads that there is work to do. */
        os_event_set(m_event);
        ctx_completed_inc();
      } else {
        prebuilt->ctx = ctx;
        break;
      }
    }
  }

  return err;
}

page_no_t Parallel_reader::Scan_ctx::search(const buf_block_t *block,
                                            const dtuple_t *key) const {
  ut_ad(index_s_own());

  page_cur_t page_cursor;
  const auto index = m_config.m_index;

  if (key != nullptr) {
    page_cur_search(block, index, key, PAGE_CUR_LE, &page_cursor);
  } else {
    page_cur_set_before_first(block, &page_cursor);
  }

  if (page_rec_is_infimum(page_cur_get_rec(&page_cursor))) {
    page_cur_move_to_next(&page_cursor);
  }

  const auto rec = page_cur_get_rec(&page_cursor);

  mem_heap_t *heap = nullptr;

  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  auto offsets = offsets_;

  rec_offs_init(offsets_);

  offsets = rec_get_offsets(rec, index, offsets, ULINT_UNDEFINED, &heap);

  auto page_no = btr_node_ptr_get_child_page_no(rec, offsets);

  if (heap != nullptr) {
    mem_heap_free(heap);
  }

  return (page_no);
}

page_cur_t Parallel_reader::Scan_ctx::start_range(
    page_no_t page_no, mtr_t *mtr, const dtuple_t *key,
    Savepoints &savepoints) const {
  ut_ad(index_s_own());

  auto index = m_config.m_index;
  page_id_t page_id(index->space, page_no);
  ulint height{};

  /* Follow the left most pointer down on each page. */
  for (;;) {
    auto savepoint = mtr->get_savepoint();

    auto block = block_get_s_latched(page_id, mtr, __LINE__);

    height = btr_page_get_level(buf_block_get_frame(block), mtr);

    savepoints.push_back({savepoint, block});

    if (height != 0 && height != m_config.m_read_level) {
      page_id.set_page_no(search(block, key));
      continue;
    }

    page_cur_t page_cursor;

    if (key != nullptr) {
      page_cur_search(block, index, key, PAGE_CUR_GE, &page_cursor);
    } else {
      page_cur_set_before_first(block, &page_cursor);
    }

    if (page_rec_is_infimum(page_cur_get_rec(&page_cursor))) {
      page_cur_move_to_next(&page_cursor);
    }

    return (page_cursor);
  }

  ut_error;

  return (page_cur_t{});
}

void Parallel_reader::Scan_ctx::create_range(Ranges &ranges,
                                             page_cur_t &leaf_page_cursor,
                                             mtr_t *mtr) const {
  leaf_page_cursor.index = m_config.m_index;

  auto iter = create_persistent_cursor(leaf_page_cursor, mtr);

  /* Setup the previous range (next) to point to the current range. */
  if (!ranges.empty()) {
    ut_a(ranges.back().second->m_heap == nullptr);
    ranges.back().second = iter;
  }

  ranges.push_back(Range(iter, std::make_shared<Iter>()));
}

dberr_t Parallel_reader::Scan_ctx::create_ranges(const Scan_range &scan_range,
                                                 page_no_t page_no,
                                                 size_t depth,
                                                 const size_t split_level,
                                                 Ranges &ranges, mtr_t *mtr) {
  ut_ad(index_s_own());
  ut_a(max_threads() > 0);
  ut_a(page_no != FIL_NULL);
  if (m_config.m_range_errno) return (DB_SUCCESS);

  /* Do a breadth first traversal of the B+Tree using recursion. We want to
  set up the scan ranges in one pass. This guarantees that the tree structure
  cannot change while we are creating the scan sub-ranges.

  Once we create the persistent cursor (Range) for a sub-tree we can release
  the latches on all blocks traversed for that sub-tree. */

  const auto index = m_config.m_index;

  page_id_t page_id(index->space, page_no);

  Savepoint savepoint({mtr->get_savepoint(), nullptr});

  auto block = block_get_s_latched(page_id, mtr, __LINE__);

  /* read_level requested should be less than the tree height. */
  ut_ad(m_config.m_read_level <
        btr_page_get_level(buf_block_get_frame(block), mtr) + 1);

  savepoint.second = block;

  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  auto offsets = offsets_;

  rec_offs_init(offsets_);

  page_cur_t page_cursor;

  page_cursor.index = index;

  auto start = scan_range.m_start;

  if (start != nullptr) {
    auto mode = PAGE_CUR_LE;
    page_cur_search(block, index, start, mode, &page_cursor);

    if (page_cur_is_after_last(&page_cursor)) {
      return (DB_SUCCESS);
    } else if (page_cur_is_before_first((&page_cursor))) {
      page_cur_move_to_next(&page_cursor);
    }
  } else {
    page_cur_set_before_first(block, &page_cursor);
    /* Skip the infimum record. */
    page_cur_move_to_next(&page_cursor);
  }

  mem_heap_t *heap{};

  const auto at_leaf = page_is_leaf(buf_block_get_frame(block));
  const auto at_level = btr_page_get_level(buf_block_get_frame(block), mtr);

  Savepoints savepoints{};

  while (!page_cur_is_after_last(&page_cursor)) {
    const auto rec = page_cur_get_rec(&page_cursor);

    ut_a(at_leaf || rec_get_node_ptr_flag(rec) ||
         !dict_table_is_comp(index->table));

    if (heap == nullptr) {
      heap = mem_heap_create(srv_page_size / 4);
    }

    offsets = rec_get_offsets(rec, index, offsets, ULINT_UNDEFINED, &heap);

    const auto end = scan_range.m_end;

    if (end != nullptr && end->compare(rec, index, offsets) < 0) {
      break;
    }

    page_cur_t level_page_cursor;

    /* Split the tree one level below the root if read_level requested is below
    the root level. */
    if (at_level > m_config.m_read_level) {
      auto page_no = btr_node_ptr_get_child_page_no(rec, offsets);

      if (depth < split_level) {
        /* Need to create a range starting at a lower level in the tree. */
        create_ranges(scan_range, page_no, depth + 1, split_level, ranges, mtr);

        page_cur_move_to_next(&page_cursor);
        continue;
      }

      /* Find the range start in the leaf node. */
      level_page_cursor = start_range(page_no, mtr, start, savepoints);
    } else {
      /* In case of root node being the leaf node or in case we've been asked to
      read the root node (via read_level) place the cursor on the root node and
      proceed. */

      if (start != nullptr) {
        page_cur_search(block, index, start, PAGE_CUR_GE, &page_cursor);
        ut_a(!page_rec_is_infimum(page_cur_get_rec(&page_cursor)));
      } else {
        page_cur_set_before_first(block, &page_cursor);

        /* Skip the infimum record. */
        page_cur_move_to_next(&page_cursor);
        ut_a(!page_cur_is_after_last(&page_cursor));
      }

      /* Since we are already at the requested level use the current page
       cursor. */
      memcpy(&level_page_cursor, &page_cursor, sizeof(level_page_cursor));
    }

    if (!page_rec_is_supremum(page_cur_get_rec(&level_page_cursor))) {
      create_range(ranges, level_page_cursor, mtr);
    }

    /* We've created the persistent cursor, safe to release S latches on
    the blocks that are in this range (sub-tree). */
    for (auto &savepoint : savepoints) {
      mtr->release_block_at_savepoint(savepoint.first, savepoint.second);
    }

    if (m_depth == 0 && depth == 0) {
      m_depth = savepoints.size();
    }

    savepoints.clear();

    if (at_level == m_config.m_read_level) {
      break;
    }

    start = nullptr;

    page_cur_move_to_next(&page_cursor);
  }

  savepoints.push_back(savepoint);

  for (auto &savepoint : savepoints) {
    mtr->release_block_at_savepoint(savepoint.first, savepoint.second);
  }

  if (heap != nullptr) {
    mem_heap_free(heap);
  }

  return (DB_SUCCESS);
}

dberr_t Parallel_reader::Scan_ctx::partition(
    const Scan_range &scan_range, Parallel_reader::Scan_ctx::Ranges &ranges,
    size_t split_level) {
  ut_ad(index_s_own());

  mtr_t mtr;
  mtr.start();
  mtr.set_log_mode(MTR_LOG_NO_REDO);

  dberr_t err{DB_SUCCESS};

  err = create_ranges(scan_range, m_config.m_index->page, 0, split_level,
                      ranges, &mtr);

  if (m_config.m_pq_reverse_scan && !ranges.empty()) {
    auto &iter = ranges.back().second;
    auto block = m_config.m_pcur->get_block();
    page_id_t page_id(m_config.m_index->space, block->get_page_no());
    auto s_block MY_ATTRIBUTE((unused)) =
        block_get_s_latched(page_id, &mtr, __LINE__);
    assert(block == s_block);

    auto page_cursor = m_config.m_pcur->get_page_cur();
    page_cursor->index = m_config.m_index;
    iter = create_persistent_cursor(*page_cursor, &mtr);

    /* deep copy of start of first ctx */
    if (scan_range.m_start == nullptr) {
      auto &first_iter = ranges.front().first;
      first_iter = std::make_shared<Iter>();
    }
  } else if (scan_range.m_end != nullptr && !ranges.empty()) {
    auto &iter = ranges.back().second;
    ut_a(iter->m_heap == nullptr);
    iter->m_heap = mem_heap_create(sizeof(btr_pcur_t) + (srv_page_size / 16));
    iter->m_tuple = pq_dtuple_copy(scan_range.m_end, iter->m_heap);

    /* Do a deep copy. */
    for (size_t i = 0; i < dtuple_get_n_fields(iter->m_tuple); ++i) {
      dfield_dup(&iter->m_tuple->fields[i], iter->m_heap);
    }
  }

  mtr.commit();

  return (err);
}

dberr_t Parallel_reader::Scan_ctx::create_context(const Range &range,
                                                  bool split) {
  auto ctx_id = m_reader->m_ctx_id.fetch_add(1, std::memory_order_relaxed);

  // clang-format off

  auto ctx = std::shared_ptr<Ctx>(
      UT_NEW_NOKEY(Ctx(ctx_id, this, range)),
      [](Ctx *ctx) { UT_DELETE(ctx); });

  // clang-format on

  dberr_t err{DB_SUCCESS};

  if (ctx.get() == nullptr) {
    m_reader->m_ctx_id.fetch_sub(1, std::memory_order_relaxed);
    return (DB_OUT_OF_MEMORY);
  } else {
    ctx->m_split = split;
    ctx->reader = m_reader;
    m_reader->enqueue(ctx);
  }

  return (err);
}

dberr_t Parallel_reader::Scan_ctx::create_contexts(const Ranges &ranges) {
  size_t split_point{};

  ut_a(max_threads() > 0 &&
       max_threads() <= Parallel_reader::MAX_TOTAL_THREADS);

  if (ranges.size() > max_threads()) {
    split_point = (ranges.size() / max_threads()) * max_threads();
  } else if (m_depth < SPLIT_THRESHOLD) {
    /* If the tree is not very deep then don't split. For smaller tables
    it is more expensive to split because we end up traversing more blocks*/
    split_point = max_threads();
  }

  if (ranges.size() > split_point) {
    m_reader->m_need_change_dop = false;
  }

  size_t i{};

  for (auto range : ranges) {
    auto err = create_context(range, i >= split_point);

    if (err != DB_SUCCESS) {
      return (err);
    }

    ++i;
  }

  return (DB_SUCCESS);
}

void Parallel_reader::parallel_read() {
  if (m_ctxs.empty()) {
    return;
  }

  if (m_n_threads == 0) {
    auto ptr = UT_NEW_NOKEY(Thread_ctx{0});

    if (ptr == nullptr) {
      set_error_state(DB_OUT_OF_MEMORY);
      return;
    }

    m_thread_ctxs.push_back(ptr);

    /* Set event to indicate to ::worker() that no threads will be spawned. */
    os_event_set(m_event);

    worker(m_thread_ctxs[0]);

    return;
  }

  m_thread_ctxs.reserve(m_n_threads);

  dberr_t err{DB_SUCCESS};

  for (size_t i = 0; i < m_n_threads; ++i) {
    try {
      auto ptr = UT_NEW_NOKEY(Thread_ctx{i});
      if (ptr == nullptr) {
        set_error_state(DB_OUT_OF_MEMORY);
        return;
      }
      m_thread_ctxs.emplace_back(ptr);
      m_parallel_read_threads.emplace_back(
          os_thread_create(parallel_read_thread_key, &Parallel_reader::worker,
                           this, m_thread_ctxs[i]));
      m_parallel_read_threads.back().start();
    } catch (...) {
      err = DB_OUT_OF_RESOURCES;
      /* Set the global error state to tell the worker threads to exit. */
      set_error_state(err);
      break;
    }
  }

  DEBUG_SYNC_C("parallel_read_wait_for_kill_query");

  DBUG_EXECUTE_IF("innodb_pread_thread_OOR", err = DB_OUT_OF_RESOURCES;
                  set_error_state(err););

  os_event_set(m_event);

  DBUG_EXECUTE_IF("bug28079850", set_error_state(DB_INTERRUPTED););

  /* Don't wait for the threads to finish if the read is not synchronous. */
  if (!m_sync) {
    return;
  }

  join();
}

dberr_t Parallel_reader::run(size_t n_threads) {
  if (!m_scan_ctxs.empty()) {
    if (n_threads == 0) {
      n_threads = std::min(m_max_threads, m_ctxs.size());

      /* No need to spawn any threads if only one thread is required. */
      if (n_threads == 1 || m_single_threaded_mode) {
        n_threads = 0;
      }
    }

    m_n_threads = n_threads;

    if (!m_single_threaded_mode) {
      release_unused_threads(m_max_threads - m_n_threads);
    }

    parallel_read();
  }

  /* Don't wait for the threads to finish if the read is not synchronous or if
  there's no parallel read. */
  if (!m_sync || m_n_threads == 0) {
    return DB_SUCCESS;
  }

  if (is_error_set()) {
    return m_err;
  }

  for (auto &scan_ctx : m_scan_ctxs) {
    if (scan_ctx->is_error_set()) {
      /* Return the state of the first Scan context that is in state ERROR. */
      return scan_ctx->m_err;
    }
  }

  return DB_SUCCESS;
}

dberr_t Parallel_reader::add_scan(trx_t *trx,
                                  const Parallel_reader::Config &config,
                                  Parallel_reader::F &&f, bool split) {
  // clang-format off

  auto scan_ctx = std::shared_ptr<Scan_ctx>(
      UT_NEW_NOKEY(Scan_ctx(this, m_scan_ctx_id, trx, config, std::move(f))),
      [](Scan_ctx *scan_ctx) { UT_DELETE(scan_ctx); });

  // clang-format on

  if (scan_ctx.get() == nullptr) {
    ib::error(ER_IB_ERR_PARALLEL_READ_OOM) << "Out of memory";
    return (DB_OUT_OF_MEMORY);
  }

  m_scan_ctxs.push_back(scan_ctx);

  ++m_scan_ctx_id;

  scan_ctx->index_s_lock();

  Parallel_reader::Scan_ctx::Ranges ranges{};
  dberr_t err{DB_SUCCESS};

  /* Split at the root node (level == 0). */
  err = scan_ctx->partition(config.m_scan_range, ranges, 0);

  if (ranges.empty() || err != DB_SUCCESS) {
    /* Table is empty. */
    scan_ctx->index_s_unlock();
    return (err);
  }

  err = scan_ctx->create_contexts(ranges);

  scan_ctx->index_s_unlock();

  return (err);
}
