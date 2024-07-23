/******************************************************
Copyright (c) 2011-2023 Percona LLC and/or its affiliates.
Copyright (c) 2024, GreatDB Software Co., Ltd.

Declarations for xtrabackup.cc

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; version 2 of the License.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program; if not, write to the Free Software
Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA

*******************************************************/

#ifndef ARCH_APPLY_INCLUDE
#define ARCH_APPLY_INCLUDE

#include <btr0sea.h>
#include <mysqld.h>

#define G_PTR uchar *

#define SQUOTE(str) "'" << str << "'"
#define GDB_DELTA_INFO_SUFFIX ".meta"

const pfs_os_file_t GDB_FILE_UNDEFINED = {NULL, OS_FILE_CLOSED};

inline bool operator==(const pfs_os_file_t &lhs, const pfs_os_file_t &rhs) {
  return lhs.m_file == rhs.m_file;
}
inline bool operator!=(const pfs_os_file_t &lhs, const pfs_os_file_t &rhs) {
  return !(lhs == rhs);
}

struct clone_filter_entry_struct {
  char *name;
  bool has_tables;
  hash_node_t name_hash;
};
typedef struct clone_filter_entry_struct clone_filter_entry_t;

typedef struct {
  ulint page_size;
  ulint zip_size;
  ulint space_id;
  ulint space_flags;
} clone_delta_info_t;

/* ======== Datafiles iterator ======== */
typedef struct {
  std::vector<fil_node_t *> nodes;
  std::vector<fil_node_t *>::iterator i;
} datafiles_iter_t;

struct datadir_entry_t {
  std::string datadir;
  std::string path;
  std::string db_name;
  std::string file_name;
  std::string rel_path;
  bool is_empty_dir;
  ssize_t file_size;

  datadir_entry_t()
      : datadir(), path(), db_name(), file_name(), rel_path(), is_empty_dir() {}

  datadir_entry_t(const datadir_entry_t &) = default;

  datadir_entry_t &operator=(const datadir_entry_t &) = default;

  datadir_entry_t(const char *datadir, const char *path, const char *db_name,
                  const char *file_name, bool is_empty_dir,
                  ssize_t file_size = -1)
      : datadir(datadir),
        path(path),
        db_name(db_name),
        file_name(file_name),
        is_empty_dir(is_empty_dir),
        file_size(file_size) {
    if (db_name != nullptr && *db_name != 0) {
      rel_path =
          std::string(db_name) + std::string("/") + std::string(file_name);
    } else {
      rel_path = std::string(file_name);
    }
  }
};

/************************************************************************
Callback to handle datadir entry. Function of this type will be called
for each entry which matches the mask by clone_process_datadir.
@return should return true on success */
typedef std::function<bool(
    /*=========================================*/
    const datadir_entry_t &entry, /*!<in: datadir entry */
    void *arg)>                   /*!<in: caller-provided data */
    handle_datadir_entry_func_t;

/************************************************************************
Function enumerates files in datadir (provided by path) which are matched
by provided suffix. For each entry callback is called.
@return false if callback for some entry returned false */
bool clone_process_datadir(const char *path,   /*!<in: datadir path */
                           const char *suffix, /*!<in: suffix to match
                                               against */
                           handle_datadir_entry_func_t func, /*!<in: callback */
                           void *data); /*!<in: additional argument for
                                        callback */

void clone_filter_hash_free(hash_table_t *hash);

bool rm_if_not_found(const datadir_entry_t &entry, /*!<in: datadir entry */
                     void *arg __attribute__((unused)));

bool clone_apply_deltas(bool delete_src);

datafiles_iter_t *datafiles_iter_new();

fil_node_t *datafiles_iter_next(datafiles_iter_t *it);

void datafiles_iter_free(datafiles_iter_t *it);

void clone_extend_space();

bool execute_syscmd(std::string cmd);
#endif /* ARCH_APPLY_INCLUDE */
