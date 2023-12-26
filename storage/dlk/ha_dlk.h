/* Copyright (c) 2003, 2022, Oracle and/or its affiliates.
   Copyright (c) 2023, GreatDB Software Co., Ltd.

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
   but WITHOUT ANY WARRANTY; without even the implied warranty ofD
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include <sys/stat.h>
#include <sys/types.h>

#include "my_dir.h"
#include "my_inttypes.h"
#include "my_io.h"
#include "sql/handler.h"
#include "sql_string.h"
#include "storage/dlk/transparent_file.h"

namespace dlk {
#define DEFAULT_CHAIN_LENGTH 512
/*
  Version for file format.
  1 - Initial Version. That is, the version when the metafile was introduced.
*/

#define DLK_VERSION 1

struct DLK_SHARE {
  char *table_name;
  char data_file_name[FN_REFLEN];
  uint table_name_length, use_count;

  mysql_mutex_t mutex;
  THR_LOCK lock;

  bool crashed;
};

struct dlk_set {
  my_off_t begin;
  my_off_t end;
};

class ha_dlk : public handler {
  THR_LOCK_DATA lock; /* MySQL lock */
  DLK_SHARE *share;   /* Shared lock info */
  my_off_t
      current_position;   /* Current position in the file during a file scan */
  my_off_t next_position; /* Next position in the file scan */
  my_off_t local_saved_data_file_length; /* save position for reads */
  uchar byte_buffer[IO_SIZE];
  Transparent_file *file_buff;
  File data_file; /* File handler for readers */

  String buffer;
  bool records_is_known;
  MEM_ROOT blobroot;

 private:
  bool get_write_pos(my_off_t *end_pos, dlk_set *closest_hole);
  int open_update_temp_file_if_needed();
  int init_dlk_writer();
  int init_data_file();
 public:
  ha_dlk(handlerton *hton, TABLE_SHARE *table_arg);
  ~ha_dlk() override {
    if (file_buff) delete file_buff;
    blobroot.Clear();
  }
  const char *table_type() const override { return "DLK"; }
  ulonglong table_flags() const override {
    return (HA_NO_TRANSACTIONS | HA_NO_AUTO_INCREMENT | HA_BINLOG_ROW_CAPABLE |
            HA_BINLOG_STMT_CAPABLE | HA_CAN_REPAIR | HA_DELETE_NOT_SUPPORTED |
            HA_UPDATE_NOT_SUPPORTED);
  }
  ulong index_flags(uint, uint, bool) const override {
    /*
      We will never have indexes so this will never be called(AKA we return
      zero)
    */
    return 0;
  }
  uint max_record_length() const { return HA_MAX_REC_LENGTH; }
  uint max_keys() const { return 0; }
  uint max_key_parts() const { return 0; }
  uint max_key_length() const { return 0; }
  /*
     Called in test_quick_select to determine if indexes should be used.
   */
  double scan_time() override {
    return (double)(stats.records + stats.deleted) / 20.0 + 10;
  }
  /* The next method will never be called */
  virtual bool fast_key_read() { return true; }
  /*
    TODO: return actual upper bound of number of records in the table.
    (e.g. save number of records seen on full table scan and/or use file size
    as upper bound)
  */
  ha_rows estimate_rows_upper_bound() override { return HA_POS_ERROR; }

  int open(const char *name, int mode, uint open_options,
           const dd::Table *table_def) override;
  int close(void) override;
  int rnd_init(bool scan = true) override;
  int rnd_next(uchar *buf) override;
  int rnd_pos(uchar *buf, uchar *pos) override;
  bool check_and_repair(THD *thd) override;
  int repair(THD *thd, HA_CHECK_OPT *check_opt) override;
  int check(THD *thd, HA_CHECK_OPT *check_opt) override;
  bool is_crashed() const override { return share->crashed; }
  int rnd_end() override;

  void position(const uchar *record) override;
  int info(uint) override;

  int create(const char *name, TABLE *form, HA_CREATE_INFO *create_info,
             dd::Table *table_def) override;
  bool check_if_incompatible_data(HA_CREATE_INFO *info,
                                  uint table_changes) override;

  THR_LOCK_DATA **store_lock(THD *thd, THR_LOCK_DATA **to,
                             enum thr_lock_type lock_type) override;

  /* The following methods were added just for TINA */
  int find_current_row(uchar *buf);
  enum_alter_inplace_result check_if_supported_inplace_alter(
      TABLE *, Alter_inplace_info *) override;
};

}  // namespace dlk
