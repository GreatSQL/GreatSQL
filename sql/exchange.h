#ifndef _EXCHAGE_H
#define _EXCHAGE_H

/* Copyright (c) 2021, Huawei Technologies Co., Ltd.
   Copyright (c) Oracle and/or its affiliates. All rights reserved.
   Copyright (c) 2021, GreatDB Software Co., Ltd.

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

#include "msg_queue.h"

class Exchange {
 private:
  uint32 m_nqueues;
  MQ_event *m_receiver;
  uint m_ref_length;
  bool m_stab_output;

 public:
  MQueue_handle **mqueue_handles;
  THD *m_thd;
  TABLE *m_table;

 public:
  Exchange()
      : m_nqueues(0),
        m_receiver(nullptr),
        m_ref_length(0),
        m_stab_output(false),
        mqueue_handles(nullptr),
        m_thd(nullptr),
        m_table(nullptr) {}

  Exchange(THD *thd, TABLE *table, uint32 workers, uint ref_length,
           bool stab_output = false)
      : m_nqueues(workers),
        m_receiver(nullptr),
        m_ref_length(ref_length),
        m_stab_output(stab_output),
        mqueue_handles(nullptr),
        m_thd(thd),
        m_table(table) {}

  enum EXCHANGE_TYPE {
    EXCHANGE_NOSORT = 0,
    EXCHANGE_SORT,
  };
  virtual ~Exchange() {}

 public:
  virtual bool init();
  virtual void cleanup();
  virtual bool convert_mq_data_to_record(uchar *data, int msg_len,
                                         uchar *row_id = nullptr);

  inline THD *get_thd() { return m_thd ? m_thd : current_thd; }

  inline MQueue_handle *get_mq_handle(uint32 i) {
    assert(mqueue_handles);
    assert(i < m_nqueues);
    return mqueue_handles[i];
  }

  inline void mqueue_mmove(int mq_next_readers, int number_workers) {
    memmove(&mqueue_handles[mq_next_readers],
            &mqueue_handles[mq_next_readers + 1],
            sizeof(MQueue_handle *) * (number_workers - mq_next_readers));
  }

  inline int lanuch_workers() { return m_nqueues; }

  inline TABLE *get_table() { return m_table; }

  inline int ref_length() { return m_ref_length; }

  inline bool is_stable() { return m_stab_output; }
  virtual bool read_mq_record() = 0;
  virtual EXCHANGE_TYPE get_exchange_type() = 0;
};

class Exchange_nosort : public Exchange {
 private:
  int m_next_queue;     /** the next read queue */
  int m_active_readers; /** number of left queues which is sending or
                            receiving message */
  /** read one message from MQ[next_queue] */
  bool get_next(void **datap, uint32 *len, bool *done);
  /** read one message rom MQ in a round-robin manner */
  bool read_next(void **datap, uint32 *len);

 public:
  Exchange_nosort() : Exchange(), m_next_queue(0), m_active_readers(0) {}

  Exchange_nosort(THD *thd, TABLE *table, int workers, int ref_length,
                  bool stab_output = false)
      : Exchange(thd, table, workers, ref_length, stab_output) {
    m_next_queue = 0;
    m_active_readers = workers;
  }

  virtual ~Exchange_nosort() {}

  /** read/convert one message from mq to table->record[0] */
  bool read_mq_record() override;

  inline EXCHANGE_TYPE get_exchange_type() override { return EXCHANGE_NOSORT; }
};

#endif
