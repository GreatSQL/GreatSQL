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

#include "exchange.h"
#include "field.h"
#include "table.h"
/**
 * alloc space for mqueue_handles
 *
 * @return false for success, and otherwise true.
 */
bool Exchange::init() {
  uint i = 0;
  MQueue **mqueues = nullptr;
  /** note that: all workers share one receiver. */
  m_receiver = new (m_thd->pq_mem_root) MQ_event(m_thd);
  if (!m_receiver) goto err;

  mqueue_handles = new (m_thd->pq_mem_root) MQueue_handle *[m_nqueues] { NULL };
  if (!mqueue_handles) goto err;

  mqueues = new (m_thd->pq_mem_root) MQueue *[m_nqueues] { NULL };
  if (!mqueues) goto err;
  for (i = 0; i < m_nqueues; i++) {
    char *ring_buffer = new (m_thd->pq_mem_root) char[RING_SIZE];
    if (!ring_buffer) goto err;
    MQ_event *sender = new (m_thd->pq_mem_root) MQ_event();
    if (!sender) goto err;
    mqueues[i] = new (m_thd->pq_mem_root)
        MQueue(sender, m_receiver, ring_buffer, RING_SIZE);
    if (!mqueues[i] || DBUG_EVALUATE_IF("pq_mq_error1", true, false)) goto err;
  }

  for (i = 0; i < m_nqueues; i++) {
    mqueue_handles[i] =
        new (m_thd->pq_mem_root) MQueue_handle(mqueues[i], MQ_BUFFER_SIZE);
    if (!mqueue_handles[i] || mqueue_handles[i]->init_mqueue_handle(m_thd) ||
        DBUG_EVALUATE_IF("pq_mq_error2", true, false))
      goto err;
  }
  return false;

err:
  sql_print_error("alloc space for exchange_record_pq error");
  return true;
}

void Exchange::cleanup() {
  destroy(m_receiver);
  if (mqueue_handles) {
    for (uint i = 0; i < m_nqueues; i++) {
      if (mqueue_handles[i]) mqueue_handles[i]->cleanup();
    }
  }
}

/*
* determine the checked value corresponding to CONST_ITEM/NULL_FIELD
* using look-up table to accelerate this process, such as
        t[0] = {false, false}, t[1] = {false, true},
        t[2] = {true, false}, t[3] = {true, true}
*/
static char bool_item_field[8] = {0, 0, 0, 1, 1, 0, 1, 1};

char *const_item_and_field_flag(uint value) {
  assert(value < 4);
  return bool_item_field + 2 * value;
}

/**
 * reconstruct table->record[0] from MQ's message
 * @qep_tab: the qep_tab of leader's first tmp table
 * @data: the message data
 * @msg_len: the message length
 *
 * @return true if successful execution, and return false otherwise
 *
 * Note that: in this process, we can receive an error msg from worker, which
 * may come from the several conditions:
 *   (1) some workers may produce an error during the execution;
 *   (2) the sending/receiving msg procedure occurs an error;
 *   (3) some unexpected errors;
 *
 */
bool Exchange::convert_mq_data_to_record(uchar *data, int msg_len,
                                         uchar *row_id) {
  /** there is error */
  if (m_thd->is_killed() || m_thd->pq_error) return false;

  if (msg_len == 1 || DBUG_EVALUATE_IF("pq_worker_error10", true, false)) {
    if (data[0] == EMPTY_MSG &&
        DBUG_EVALUATE_IF("pq_worker_error10", false, true)) {
      return true;
    } else {
      const char *msg = (data[0] == ERROR_MSG) ? "error msg" : "unknown error";
      sql_print_error("[Parallel query]: error info. %s\n", msg);
      m_thd->pq_error = true;
      return false;
    }
  }

  memset(m_table->record[0], 255, m_table->s->reclength);
  int size_field = m_table->s->fields;

  // fetch the row_id info. from MQ
  if (m_stab_output) {
    if (row_id) memcpy(row_id, data, m_ref_length);  // row_id
    data += m_ref_length;
  }

  uint null_len = *(uint16 *)data;
  data = data + sizeof(uint16);
  uchar *null_flag = (uchar *)data;

  /**
   * Note that: we use two more bytes to store Field_varstring::length_bytes,
   * and MYSQL_TIME_TYPE::pq_neg.
   */
  if (DBUG_EVALUATE_IF("pq_worker_error11", true, false) ||
      msg_len >
          (int)(m_table->s->reclength + 6 + null_len + 2 * m_table->s->fields +
                (m_stab_output ? m_ref_length : 0))) {
    m_thd->pq_error = true;
    sql_print_error(
        "[Parallel query]: sending (or receiving) msg from MQ error");
    return false;
  }

  bool null_field = false;
  bool const_item = false;
  uint bit_value;
  char *status_flag = nullptr;

  uint null_offset = 0;
  uint ptr_offset = null_len;
  Field *item_field = nullptr;
  int i = 0, j;
  for (; i < size_field; i++) {
    item_field = m_table->field[i];
    /** determine whether it is a CONST_ITEM or NULL_FIELD */
    j = (null_offset >> 3) + 1;
    assert((null_offset & 1) == 0);
    bit_value = (null_flag[j] >> (6 - (null_offset & 7))) & 3;
    status_flag = const_item_and_field_flag(bit_value);
    const_item = *status_flag;
    null_field = *(status_flag + 1);

    enum_field_types field_type = item_field->type();
    /** we should fill data into record[0] only when NOT_CONST_ITEM &
     * NOT_NULL_FIELD */
    if (!const_item && !null_field) {
      if (field_type == MYSQL_TYPE_VARCHAR ||
          field_type == MYSQL_TYPE_VAR_STRING) {
        Field_varstring *field_var = static_cast<Field_varstring *>(item_field);
        field_var->length_bytes = (uint)data[ptr_offset];
        ptr_offset++;  // moving to the real value

        uint field_length = (field_var->length_bytes == 1)
                                ? (uint)data[ptr_offset]
                                : uint2korr(&data[ptr_offset]);
        uint pack_length = field_length + field_var->length_bytes;
        memcpy(field_var->ptr, &data[ptr_offset], pack_length);
        ptr_offset += pack_length;
      } else {
        uint pack_length = item_field->pack_length();
        memcpy(item_field->ptr, &data[ptr_offset], pack_length);
        ptr_offset += pack_length;
      }
    }
    /** set NULL flag of field */
    if (!const_item) {
      if (null_field)
        item_field->set_null();
      else
        item_field->set_notnull();
    }
    null_offset += 2;
  }
  return true;
}

/**
 * read one message from MQ[next_queue]
 *
 * @retval: true for success, and otherwise false
 */
bool Exchange_nosort::get_next(void **datap, uint32 *m_len, bool *done) {
  MQ_RESULT result;
  if (done != NULL) *done = false;
  MQueue_handle *reader = get_mq_handle(m_next_queue);
  result = reader->receive(datap, m_len);

  if (result == MQ_DETACHED) {
    if (done != NULL) *done = true;
    return false;
  }
  if (result == MQ_WOULD_BLOCK) {
    return false;
  }
  return true;
}

/**
 * read one message from MQ in a round-robin method
 * @datap: the message data
 * @m_len: the message length
 *
 * @retval: true for success, and otherwise false
 */
bool Exchange_nosort::read_next(void **datap, uint32 *m_len) {
  bool readerdone = false;
  int nvisited = 0;
  bool read_result = false;
  THD *thd = get_thd();

  /** round-robin method to acquire the data */
  while (!thd->is_killed() && !thd->pq_error) {
    read_result = get_next(datap, m_len, &readerdone);
    /** detached and its content is also read done */
    if (readerdone) {
      assert(false == read_result);
      m_active_readers--;
      /** read done for all queues */
      if (m_active_readers == 0) {
        return false;
      }
      mqueue_mmove(m_next_queue, m_active_readers);
      if (m_next_queue >= m_active_readers) {
        m_next_queue = 0;
      }
      continue;
    }

    /** data has successfully read into datap */
    if (read_result) return true;
    /** move to next worker */
    m_next_queue++;
    if (m_next_queue >= m_active_readers) {
      m_next_queue = 0;
    }
    nvisited++;
    /** In a round-robin, we cannot read one message from MQ */
    if (nvisited >= m_active_readers) {
      /**
       * this barrier ensures that the receiver first enters into a
       * waiting status and then is waked by one sender.
       */
      memory_barrier();
      MQ_event *receiver = get_mq_handle(0)->get_receiver();
      if (receiver) {
        receiver->wait_latch();
        receiver->reset_latch();
      }
      nvisited = 0;
    }
  }
  return false;
}

/**
 * read one message from MQ and fill it to table->record[0]
 *
 * @retval: true for success, and otherwise false
 */
bool Exchange_nosort::read_mq_record() {
  assert(!is_stable() && get_exchange_type() == EXCHANGE_NOSORT);
  bool result = false;
  uchar *data = nullptr;
  uint32 msg_len = 0;

  /** read a message from MQ's local buffer */
  result = read_next((void **)&data, &msg_len);
  return (result && convert_mq_data_to_record(data, msg_len));
}
