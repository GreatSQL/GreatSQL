#ifndef SQL_ITERATORS_MAC_ITERATOR_H_
#define SQL_ITERATORS_MAC_ITERATOR_H_

/* Copyright (c) 2019, 2022, Oracle and/or its affiliates.
   Copyright (c) 2024, GreatDB Software Co., Ltd.

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

#include "sql/iterators/row_iterator.h"
#include "sql/sql_class.h"
#include "sql/table.h"

template <class RealIterator>
class MacIterator final : public RowIterator {
 public:
  template <class... Args>
  MacIterator(bool unique, THD *thd, TABLE *table, Args &&...args)
      : RowIterator(thd),
        m_table(table),
        m_unique(unique),
        m_iterator(thd, table, std::forward<Args>(args)...) {}

  bool Init() override {
    mark_mac_field(m_table);
    bool ret = m_iterator.Init();
    return ret;
  }

  int Read() override {
    for (;;) {
      int ret = m_iterator.Read();

      if (ret != 0) return ret;

      if (thd()->killed) {
        thd()->send_kill_message();
        return 1;
      }

      if (thd()->is_error()) return 1;

      if (check_mac_row(thd(), m_table, m_table->record[0], MAC_READ, false)) {
        if (m_unique)
          return -1;
        else
          continue;
      }

      return 0;
    }
  }

  void SetNullRowFlag(bool is_null_row) override {
    m_iterator.SetNullRowFlag(is_null_row);
  }
  void UnlockRow() override { m_iterator.UnlockRow(); }
  void StartPSIBatchMode() override { m_iterator.StartPSIBatchMode(); }
  void EndPSIBatchModeIfStarted() override {
    m_iterator.EndPSIBatchModeIfStarted();
  }

 private:
  TABLE *m_table;
  bool m_unique;
  RealIterator m_iterator;
};

#endif  // SQL_ITERATORS_MAC_ITERATOR_H_
