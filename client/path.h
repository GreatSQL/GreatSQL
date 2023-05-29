/*
   Copyright (c) 2012, 2022, Oracle and/or its affiliates. All rights reserved.
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
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
*/
#ifndef PATH_UTIL_INCLUDED
#define PATH_UTIL_INCLUDED
#include <functional>
#include <ostream>
#include <string>

/**
  A helper class for handling file paths. The class can handle the memory
  on its own or it can wrap an external string.
  @note This is a rather trivial wrapper which doesn't handle malformed paths
  or filenames very well.
  @see unittest/gunit/path-t.cc

*/
class Path {
 public:
  Path();

  Path(const std::string &s);

  Path(const Path &p);

  bool path_getcwd();

  void trim();

  void parent_directory(Path *out);

  Path &up();

  Path &append(const std::string &path);

  Path &filename_append(const std::string &ext);

  void path(const std::string &p);

  void filename(const std::string &f);

  void path(const Path &p);

  void filename(const Path &p);

  bool qpath(const std::string &qp);

  bool normalize_path();

  bool is_qualified_path();

  bool exists();

  const std::string to_str();

  bool empty();
#ifndef _WIN32
  void get_homedir();
#endif

  friend std::ostream &operator<<(std::ostream &op, const Path &p);

 private:
  std::string m_path;
  std::string m_filename;
};

std::ostream &operator<<(std::ostream &op, const Path &p);

struct datadir_entry_t {
  std::string path;
  std::string clear_path;
  datadir_entry_t(const datadir_entry_t &) = default;

  datadir_entry_t &operator=(const datadir_entry_t &) = default;

  datadir_entry_t(const char *path, const char *clear_path)
      : path(path), clear_path(clear_path) {}
};

/** Callback function type to be implemented by caller. It is called for each
entry in directory.
@param[in]      path    path to the file
@param[in]      name    name of the file */
typedef std::function<void(const char *path, const char *name)> os_dir_cbk_t;

/************************************************************************
Callback to handle datadir entry. Function of this type will be called
for each entry which matches the mask by cli_process_datadir.
@return should return true on success */
typedef std::function<bool(
    /*=========================================*/
    const datadir_entry_t &entry, /*!<in: datadir entry */
    void *arg [[maybe_unused]])>  /*!<in: caller-provided data */
    handle_datadir_entry_func_t;

/************************************************************************
Function enumerates files in datadir (provided by path) which are matched
by provided suffix. For each entry callback is called.
@return false if callback for some entry returned false */
bool cli_process_datadir(const char *path,   /*!<in: datadir path */
                         const char *suffix, /*!<in: suffix to match
                                             against */
                         handle_datadir_entry_func_t func, /*!<in: callback */
                         void *data); /*!<in: additional argument for
                                      callback */

#endif
