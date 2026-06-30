/* Copyright (c) 2026, GreatDB Software Co., Ltd.

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

#ifndef PLUGIN_GDB_PLAN_BASELINE_PFS_H
#define PLUGIN_GDB_PLAN_BASELINE_PFS_H

#include <atomic>
#include <shared_mutex>
#include "mysql/components/services/pfs_plugin_table_service.h"
#include "mysql/plugin.h"

#include "plan_baseline_thread.h"
namespace greatdb_plan_baseline {

extern SERVICE_TYPE_NO_CONST(pfs_plugin_table_v1) * mysql_pfs_table;
extern SERVICE_TYPE_NO_CONST(pfs_plugin_column_integer_v1) * mysql_pfscol_int;
extern SERVICE_TYPE_NO_CONST(pfs_plugin_column_bigint_v1) * mysql_pfscol_bigint;
extern SERVICE_TYPE_NO_CONST(pfs_plugin_column_decimal_v1) *
    mysql_pfscol_decimal;
extern SERVICE_TYPE_NO_CONST(pfs_plugin_column_timestamp_v2) *
    mysql_pfscol_timestamp;
extern SERVICE_TYPE_NO_CONST(pfs_plugin_column_string_v2) * mysql_pfscol_string;
extern SERVICE_TYPE_NO_CONST(pfs_plugin_column_blob_v1) * mysql_pfscol_blob;

class PSI_Val {
 public:
  PSI_Val() {}
  virtual ~PSI_Val() {}
  virtual void SetField(PSI_field *f) = 0;
  virtual void SetNull() { assert(0); }
  virtual void Set(uint) { assert(0); }
  virtual void Set(double) { assert(0); }
  virtual void Set(ulonglong) { assert(0); }
  virtual void Set(std::string) { assert(0); }
};

#ifndef DEFINE_PSI_VAL_CONSTRUCT
#define DEFINE_PSI_VAL_CONSTRUCT(N, T, func) \
  class PSI_Val_##N : public PSI_Val {       \
    using PSI_Val::Set;                      \
                                             \
   public:                                   \
    PSI_Val_##N() {}                         \
    void SetField(PSI_field *f) override { func(f, value); }
#endif

#ifndef DEFINE_PSI_VAL_SET
#define DEFINE_PSI_VAL_SET(T)                       \
 private:                                           \
  PSI_##T value = {false, 0};                       \
                                                    \
 public:                                            \
  void SetNull() override { value.is_null = true; } \
  void Set(T v) override {                          \
    value.is_null = false;                          \
    value.val = v;                                  \
  }                                                 \
  }
#endif

#ifndef DEFINE_PSI_VAL_SET2
#define DEFINE_PSI_VAL_SET2(T)           \
 private:                                \
  T value = 0;                           \
                                         \
 public:                                 \
  void SetNull() override { value = 0; } \
  void Set(T v) override { value = v; }  \
  }
#endif

#ifndef DEFINE_PSI_VAL
#define DEFINE_PSI_VAL(type, func)           \
  DEFINE_PSI_VAL_CONSTRUCT(type, type, func) \
  DEFINE_PSI_VAL_SET(type)
#endif

DEFINE_PSI_VAL(uint, mysql_pfscol_int->set_unsigned);
DEFINE_PSI_VAL(double, mysql_pfscol_decimal->set);
DEFINE_PSI_VAL(ulonglong, mysql_pfscol_bigint->set_unsigned);

DEFINE_PSI_VAL_CONSTRUCT(timestamp, ulonglong, mysql_pfscol_timestamp->set2)
DEFINE_PSI_VAL_SET2(ulonglong);

class PSI_Val_string : public PSI_Val {
  using PSI_Val::Set;
  std::string value;

 public:
  PSI_Val_string() {}
  void SetField(PSI_field *f) override {
    mysql_pfscol_string->set_varchar_utf8mb4_len(f, value.c_str(),
                                                 value.size());
  }
  void SetNull() override { value.clear(); }
  void Set(std::string str) override {
    if (!str.empty()) {
      value = str;
    } else {
      value.clear();
    }
  }
};

class PSI_Val_blob : public PSI_Val {
  using PSI_Val::Set;
  std::string value;

 public:
  PSI_Val_blob() {}
  void SetField(PSI_field *f) override {
    mysql_pfscol_blob->set(f, value.c_str(), value.size());
  }
  void SetNull() override { value.clear(); }
  void Set(std::string str) override {
    if (!str.empty()) {
      value = str;
    } else {
      value.clear();
    }
  }
};

/**
 *  pfs table define
 *      data
 *      work_thread ->ticker
 *      handle
 */
class Gdb_plan_baseline_pfs_table {
 public:
  /** Constructor.
  @param[in]

  */
  Gdb_plan_baseline_pfs_table() : m_position(0) {}

  /** Destructor. */
  virtual ~Gdb_plan_baseline_pfs_table() {}

  virtual uint32_t get_row_count() = 0;
  /** Read column at index of current row. Implementation
  is specific to table.
   @param[out]	field	column value
   @param[in]	index	column position within row
   @return error code. */
  virtual int read_column_value(PSI_field *field, uint32_t index) = 0;

  /** Initialize the table.
  @return plugin table error code. */
  virtual int rnd_init() = 0;

  /** Set cursor to next record.
  @return plugin table error code. */
  virtual int rnd_next() = 0;

  /** End the table..
  @return plugin table error code. */
  virtual int rnd_end() = 0;

  /** Set cursor to current position: currently no op.
  @return plugin table error code. */
  int rnd_pos() {
    if (m_position > 0 && m_position <= get_row_count()) {
      return (0);
    }
    return (PFS_HA_ERR_END_OF_FILE);
  }

  /** Reset cursor position to beginning. */
  void reset_pos() { m_position = 0; }

  /** Close the table. */
  void close() { m_position = 0; }
  /* @return address of current position. PFS needs it to set
  the position for proxy table. */
  uint32_t *get_position_address() { return (&m_position); }

 protected:
  /** @return Current cursor position. */
  uint32_t get_position() const { return (m_position); }

  /** @return true, if no data in table. */
  virtual bool is_empty() { return get_row_count() == 0; }

 private:
  /** Current position of the cursor. */
  uint32_t m_position;
};

/*
 *  Gdb_plan_baseline_pfs_data interface
 *
 */

class Gdb_plan_baseline_pfs_data {
 public:
  Gdb_plan_baseline_pfs_data() : inited(false) {}
  virtual ~Gdb_plan_baseline_pfs_data() {}

  virtual int Init() = 0;
  virtual int GetData(std::vector<std::unique_ptr<PSI_Val>> &data,
                      uint32_t position, uint32_t index) = 0;
  virtual int End() = 0;
  virtual unsigned long long get_row_count() { return inited.load() ? 1 : 0; }

 protected:
  std::atomic<bool> inited;
};

class Gdb_plan_baseline_pfs_base {
 public:
  virtual ~Gdb_plan_baseline_pfs_base() { deinit(); }
  virtual bool init() = 0;
  virtual void deinit() {
    if (m_data) {
      m_data.reset(nullptr);
    }
  }

  /** @return Proxy table share reference. */
  PFS_engine_table_share_proxy *get_proxy_share() { return (&m_table_def); }

  Gdb_plan_baseline_pfs_base();

  bool _init(std::unique_ptr<Gdb_plan_baseline_pfs_data> data) {
    m_data.swap(data);
    return false;
  }

  bool InitData() {
    if (m_data) {
      return m_data->Init();
    }
    return true;
  }

  int ReadData(std::vector<std::unique_ptr<PSI_Val>> &data) {
    if (m_data) {
      return m_data->GetData(data, 0, 0);
    }
    return 1;
  }

  int EndData() {
    if (m_data) {
      return m_data->End();
    }
    return 1;
  }

  std::unique_ptr<Gdb_plan_baseline_pfs_data> m_data;
  PFS_engine_table_share_proxy m_table_def;

 private:
  inline void run(void) {}
};

bool Init_pfs();
void Deinit_pfs();
void update_plan_baseline_enable_summary(MYSQL_THD thd, SYS_VAR *var,
                                         void *var_ptr, const void *save);
}

#endif
