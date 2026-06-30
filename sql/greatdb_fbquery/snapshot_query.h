/* Copyright (c) 2026, GreatDB Software Co., Ltd.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is designed to work with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have either included with
   the program or referenced in the documentation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#ifndef GREATDB_SNAPSHOT_QUERY_H_
#define GREATDB_SNAPSHOT_QUERY_H_

#include "lex_string.h"
#include "my_dbug.h"
#include "my_time.h"

class THD;
class Item;
struct Parse_context;
class Table_ref;
struct TABLE;
struct LEX;

namespace ns_greatdb_flashback_query {

enum greatdb_flash_back_type_t : uint32_t {

  AS_OF_NONE = 0,
  AS_OF_TIMESTAMP = 1,
  AS_OF_DTID = 2
};

class greatdb_fb_snapshot_interface_t {
 public:
  explicit greatdb_fb_snapshot_interface_t(Item *item) : m_item(item) {}
  virtual ~greatdb_fb_snapshot_interface_t() {}

 public:
  virtual greatdb_flash_back_type_t get_type() = 0;
  virtual bool fix_fields(THD *thd) = 0;
  virtual bool itemize(Parse_context *pc, Table_ref *target_table);
  virtual bool val_int(uint64_t *value) = 0;

 protected:
  Item *m_item{nullptr};
};

/** This is for syntax compatible */
struct Table_fb_snapshot_and_alias {
  LEX_CSTRING table_alias;
  greatdb_fb_snapshot_interface_t *fb_snapshot_provider;
};

/** this cass is for query based on timestamp */

class greatdb_fb_ts_snapshot_t : public greatdb_fb_snapshot_interface_t {
 public:
  greatdb_fb_ts_snapshot_t(Item *item)
      : greatdb_fb_snapshot_interface_t(item) {}

  virtual greatdb_flash_back_type_t get_type() override {
    return greatdb_flash_back_type_t::AS_OF_TIMESTAMP;
  }

  virtual bool fix_fields(THD *thd) override;
  virtual bool val_int(uint64_t *value) override;

 private:
  MYSQL_TIME m_fb_timestamp;
};

/** snapshot implementation part */

class greatdb_fb_table_snapshot_t {
 public:
  greatdb_fb_table_snapshot_t() = default;
  virtual ~greatdb_fb_table_snapshot_t() = default;

 public:
  virtual greatdb_flash_back_type_t get_table_snapshot_type() {
    return greatdb_flash_back_type_t::AS_OF_NONE;
  }
  virtual void set_snapshot_baseline(uint64_t baseline) = 0;
  virtual uint64_t get_snapshot_baseline() = 0;

  virtual void reset_snapshot() = 0;
};

class greatdb_fb_table_ts_snapshot_t : public greatdb_fb_table_snapshot_t {
 public:
  greatdb_fb_table_ts_snapshot_t() { m_ts_base_line = 0; }
  ~greatdb_fb_table_ts_snapshot_t() = default;

 public:
  greatdb_flash_back_type_t get_table_snapshot_type() override {
    return greatdb_flash_back_type_t::AS_OF_TIMESTAMP;
  }

  void set_snapshot_baseline(uint64_t ts_baseline) override {
    m_ts_base_line = ts_baseline;
  }

  uint64_t get_snapshot_baseline() override { return m_ts_base_line; }

  void reset_snapshot() override { m_ts_base_line = 0; }

 private:
  uint64_t m_ts_base_line{0};
};

class greatdb_fb_table_dtid_snapshot_t : public greatdb_fb_table_snapshot_t {
 public:
  greatdb_fb_table_dtid_snapshot_t() { m_global_dtid = 0; }
  ~greatdb_fb_table_dtid_snapshot_t() = default;

 public:
  greatdb_flash_back_type_t get_table_snapshot_type() override {
    return greatdb_flash_back_type_t::AS_OF_DTID;
  }

  void set_snapshot_baseline(uint64_t ts_baseline) override {
    /** Maybe, we need to do some conversion */
    m_global_dtid = ts_baseline;
  }

  uint64_t get_snapshot_baseline() override { return m_global_dtid; }
  void reset_snapshot() override { m_global_dtid = 0; }

 private:
  uint64_t m_global_dtid{0};
};

class greatdb_fb_table_snapshot_handler_t {
 public:
  greatdb_fb_table_snapshot_handler_t() { m_table_snapshot = nullptr; }
  ~greatdb_fb_table_snapshot_handler_t() = default;

  static bool create_table_snapshot_for_query(THD *thd, const LEX *lex);

 public:
  void reset_table_snapshot(TABLE *table, THD *thd);
  bool create_table_snapshot(
      greatdb_fb_snapshot_interface_t *snapshot_provider);
  greatdb_fb_table_snapshot_t *get_fb_table_snapshot();

 private:
  greatdb_fb_table_ts_snapshot_t m_ts_snapshot;
  greatdb_fb_table_dtid_snapshot_t m_dtid_snapshot;
  greatdb_fb_table_snapshot_t *m_table_snapshot{nullptr};
};

}  // namespace ns_greatdb_flashback_query

#endif
