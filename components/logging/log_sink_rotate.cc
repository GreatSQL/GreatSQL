/* Copyright (c) 2025, GreatDB Software Co., Ltd.

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

#include <atomic>
#include <deque>
#include <filesystem>
#include <iostream>
#include <mutex>
#include <regex>
#include <string>

#include <mysql/components/services/log_builtins.h>

#include <mysql/components/component_implementation.h>
#include <mysql/components/service_implementation.h>

#include <mysql/components/services/component_sys_var_service.h>
#include <mysql/components/services/log_shared.h>
#include <mysql/components/services/log_sink_perfschema.h>
#include "log_service_imp.h"
#include "my_compiler.h"
#include "my_dir.h"
#include "sql/server_component/log_builtins_internal.h"
#include "sql/sql_plugin_var.h"

REQUIRES_SERVICE_PLACEHOLDER(component_sys_variable_register);
REQUIRES_SERVICE_PLACEHOLDER(component_sys_variable_unregister);

REQUIRES_SERVICE_PLACEHOLDER(log_builtins);
REQUIRES_SERVICE_PLACEHOLDER(log_builtins_string);
REQUIRES_SERVICE_PLACEHOLDER(log_sink_perfschema);

SERVICE_TYPE(log_builtins) *log_bi = nullptr;
SERVICE_TYPE(log_builtins_string) *log_bs = nullptr;
SERVICE_TYPE(log_sink_perfschema) *log_ps = nullptr;

/**
  Whether to generate a UTC timestamp, or one following system-time.
  These values are not arbitrary; they must correspond to the range
  and meaning of opt_log_timestamps.
*/
enum enum_iso8601_tzmode {
  iso8601_sysvar_logtimestamps = -1, /**< use value of opt_log_timestamps */
  iso8601_utc = 0,                   /**< create UTC timestamp */
  iso8601_system_time = 1            /**< use system time */
};

extern const char *log_error_dest;
extern int make_iso8601_timestamp(char *buf, ulonglong utime,
                                  enum enum_iso8601_tzmode mode);

extern log_service_error make_log_path(char *result, const char *name_or_ext);

#define LOG_SINK_ROTATE "log_error_rotate"
static bool inited = false;

static longlong total_size_value = 0;
static longlong max_size_value = 0;
namespace fs = std::filesystem;

class Log_sink_rotate {
 public:
  Log_sink_rotate(const Log_sink_rotate &) = delete;
  Log_sink_rotate &operator=(const Log_sink_rotate &) = delete;

  static Log_sink_rotate *getInstance() {
    static Log_sink_rotate instance;
    return &instance;
  }

  ~Log_sink_rotate() {}

  static bool compareFilesById(const fs::directory_entry &a,
                               const fs::directory_entry &b) {
    int idA = extractFileId(a.path().filename().string());
    int idB = extractFileId(b.path().filename().string());
    return idA < idB;
  }

  static int extractFileId(const std::string &fileName) {
    size_t dotPos = fileName.rfind('.');
    if (dotPos != std::string::npos && dotPos + 1 < fileName.size()) {
      try {
        return std::stoi(fileName.substr(dotPos + 1));
      } catch (const std::invalid_argument &) {
        return -1;
      }
    }
    return -1;
  }

  static bool isValidFileName(const std::string &fileName,
                              const std::string &prefix) {
    std::regex pattern("^" +
                       std::regex_replace(prefix,
                                          std::regex(R"([.^$*+?{}()\[\]\\|])"),
                                          R"(\$&)") +
                       R"((\.\d+)?$)");
    return std::regex_match(fileName, pattern);
  }
  bool Init() {
    write_file = false;
    current_size = 0;
    errstream = nullptr;
    return false;
  }
  bool DeInit() { return false; }

  log_service_error close() {
    std::lock_guard<std::mutex> lock(log_mutex);
    if (errstream != nullptr) {
      return log_bi->close_errstream(&errstream);
    }
    return LOG_SERVICE_SUCCESS;
  }

  /**
   * @brief rotate files
   *
   *
   * check total size has more than config size
   * remove oldest file
   *
   * check current size has more than config size
   * rotate current file
   *
   * @param filename
   * @param force    true if next message will write to new file
   *
   */
  bool rotateFiles(const char *filename, bool force) {
    fs::path filePath(filename);
    fs::path dirPath = filePath.parent_path();
    std::string prefix = filePath.filename().string();

    std::deque<fs::directory_entry> files_list;
    for (const auto &entry : fs::directory_iterator(dirPath)) {
      if (fs::is_regular_file(entry.status()) &&
          (entry.status().permissions() & fs::perms::owner_write) !=
              fs::perms::none) {
        if (isValidFileName(entry.path().filename().string(), prefix))
          files_list.push_back(entry);
      }
    }

    if (!files_list.empty()) {
      longlong accumulatedSize = 0;
      std::sort(files_list.begin(), files_list.end(), compareFilesById);
      if (total_size_value > 0) {
        auto totalSize = total_size_value - max_size_value;
        for (auto it = files_list.begin(); it != files_list.end();) {
          accumulatedSize += fs::file_size(it->path());
          if (accumulatedSize > totalSize) {
            fs::remove(it->path());
            it = files_list.erase(it);
          } else {
            it++;
          }
        }
      }

      if (!files_list.empty()) {
        if (force || (fs::file_size(filePath) > (uintmax_t)max_size_value)) {
          int max_backup_size = files_list.size();
          for (auto it = files_list.rbegin(); it != files_list.rend(); ++it) {
            std::string new_name =
                prefix + "." + std::to_string(max_backup_size);
            fs::rename(it->path(), dirPath / new_name);
            max_backup_size--;
          }
        }
      }
    }
    return false;
  }

  log_service_error get_log_name(char *buf, size_t bufsize) {
    fs::path fullpath(log_error_dest);
    strncpy(buf, fullpath.filename().string().c_str(),
            std::min(fullpath.filename().string().size(), bufsize));
    return LOG_SERVICE_SUCCESS;
  }
  log_service_error open(bool force = false) {
    std::lock_guard<std::mutex> lock(log_mutex);

    if (log_error_dest && (0 != strcmp(log_error_dest, "stderr"))) {
      try {
        if (errstream != nullptr) {
          return log_bi->close_errstream(&errstream);
        }
        log_service_error rr;
        // result: path + name + extension
        char errorlog_instance_full[FN_REFLEN] = {0};
        fs::path fullpath(log_error_dest);
        rr = make_log_path(errorlog_instance_full,
                           fullpath.filename().string().c_str());
        if (rr) {
          return rr;
        }
        if (rotateFiles(errorlog_instance_full, force)) {
          return LOG_SERVICE_MISC_ERROR;
        }
        MY_STAT f_stat;
        auto size = f_stat.st_size;
        if (my_stat(errorlog_instance_full, &f_stat, MYF(0)) != nullptr) {
          size = f_stat.st_size;
        } else {
          size = 0;
        }
        rr = log_bi->open_errstream(fullpath.filename().string().c_str(),
                                    &errstream);
        if (log_bi->dedicated_errstream(errstream) == 1) {
          write_file = true;
        } else {
          write_file = false;
        }
        current_size.store(size);
        return rr;
      } catch (std::exception &e) {
        write_file = false;
        current_size.store(0);
        return LOG_SERVICE_OPEN_FAILED;
      }
    } else {
      current_size.store(0);
      write_file = false;
    }
    return LOG_SERVICE_SUCCESS;
  }

  log_service_error reopen(bool force = false) {
    if (write_file) {
      auto rr = open(force);
      if (rr) {
        return rr;
      }
    }
    return LOG_SERVICE_SUCCESS;
  }

  log_service_error write_errstream(const char *buffer, size_t length) {
    log_service_error rr;

    if ((current_size + length) > (size_t)max_size_value) {
      // release version will easier to create scenarios
      std::lock_guard<std::mutex> lock(rotate_log_mutex);
      if ((current_size.load() + length) > (size_t)max_size_value) {
        rr = reopen(true);
        if (rr) {
          return rr;
        }
      }
    }

    rr = log_bi->write_errstream(errstream, buffer, length);
    if (!rr) {
      current_size += length;
    }
    return rr;
  }

  longlong GetSize() { return current_size.load(); }

 private:
  Log_sink_rotate() {}
  std::mutex log_mutex;
  std::mutex rotate_log_mutex;
  std::atomic<bool> write_file{false};
  std::atomic<longlong> current_size{0};
  void *errstream{nullptr};  ///< pointer to errstream in the server
};

/**
 *
 *  if max_size > total_size, return error
 *  if max_size == 0 and total_size > 0, max_size = total_size/2
 *  at least two log file
 *
 */
int check_max_size(THD *thd, SYS_VAR *var, void *save,
                   struct st_mysql_value *value) {
  if (check_func_longlong(thd, var, save, value)) {
    return 1;
  }
  longlong max_size = *(longlong *)save;
  if (max_size > (longlong)(total_size_value / 2)) {
    return 1;
  }

  if (max_size == 0 && total_size_value > 0) {
    *(longlong *)save = total_size_value / 2;
  }
  return 0;
}

/**
 *  check max_size >= total_size, return error
 */
int check_total_size(THD *thd, SYS_VAR *var, void *save,
                     struct st_mysql_value *value) {
  if (check_func_longlong(thd, var, save, value)) {
    return 1;
  }
  longlong *total_size = (longlong *)save;
  if ((*total_size) == 0) {
    *(longlong *)save = LONG_LONG_MAX;
  }

  if (max_size_value < (*total_size)) {
    return 0;
  }
  return 1;
}

static void fix_update_total_size(MYSQL_THD thd [[maybe_unused]],
                                  SYS_VAR *var [[maybe_unused]],
                                  void *val_ptr [[maybe_unused]],
                                  const void *save [[maybe_unused]]) {
  update_func_longlong(thd, var, val_ptr, save);
  Log_sink_rotate::getInstance()->reopen();
}

static void fix_update_max_size(MYSQL_THD thd [[maybe_unused]],
                                SYS_VAR *var [[maybe_unused]],
                                void *val_ptr [[maybe_unused]],
                                const void *save [[maybe_unused]]) {
  update_func_longlong(thd, var, val_ptr, save);
  if (Log_sink_rotate::getInstance()->GetSize() >= max_size_value) {
    Log_sink_rotate::getInstance()->reopen();
  }
}

mysql_service_status_t log_sys_var_init() {
  INTEGRAL_CHECK_ARG(longlong) total_arg, vmax_args;
  total_arg.def_val = 2147483648;  // 2GB 2*1024*1024*1024
  total_arg.min_val = 1024 * 1024;
  total_arg.max_val = LONG_LONG_MAX;
  total_arg.blk_sz = 0;

  if (mysql_service_component_sys_variable_register->register_variable(
          LOG_SINK_ROTATE, "total_size",
          PLUGIN_VAR_LONGLONG | PLUGIN_VAR_OPCMDARG,
          "retate error log total max size", check_total_size,
          fix_update_total_size, (void *)&total_arg,
          (void *)&total_size_value)) {
    return -1;
  }

  vmax_args.def_val = 1073741824;  // 1G 1*1024*1024*1024
  vmax_args.min_val = 0;
  vmax_args.max_val = LONG_LONG_MAX / 2;
  vmax_args.blk_sz = 0;
  if (mysql_service_component_sys_variable_register->register_variable(
          LOG_SINK_ROTATE, "max_size",
          PLUGIN_VAR_LONGLONG | PLUGIN_VAR_OPCMDARG,
          "error log single file max size, error log will rotate when ",
          check_max_size, fix_update_max_size, (void *)&vmax_args,
          (void *)&max_size_value)) {
    return -1;
  }

  return false;
}

/**
  services: log sinks: basic logging ("classic error-log")
  Will write timestamp, label, thread-ID, and message to stderr/file.
  If you should not be able to specify a label, one will be generated
  for you from the line's priority field.

  @param           instance             instance handle
  @param           ll                   the log line to write

  @retval          int                  number of added fields, if any
*/
int log_sink_trad_rotate(void *instance, log_line *ll) {
  const char *label = "", *msg = "";
  int out_fields = 0;
  size_t msg_len = 0, iso_len = 0, label_len = 0, subsys_len = 0;
  enum loglevel prio = ERROR_LEVEL;
  unsigned int errcode = 0;
  log_item_type item_type = LOG_ITEM_END;
  log_item_type_mask out_types = 0;
  const char *iso_timestamp = "", *subsys = "";
  my_thread_id thread_id = 0;
  char *line_buffer = nullptr;
  log_item_iter *it;

  if (instance == nullptr) return LOG_SERVICE_INVALID_ARGUMENT;

  if ((it = log_bi->line_item_iter_acquire(ll)) == nullptr)
    return LOG_SERVICE_MISC_ERROR; /* purecov: inspected */

  for (log_item *li = log_bi->line_item_iter_first(it); li != nullptr;
       li = log_bi->line_item_iter_next(it)) {
    item_type = li->type;
    if (log_bi->item_inconsistent(li)) continue;
    out_fields++;

    switch (item_type) {
      case LOG_ITEM_SQL_ERRCODE:
        errcode = (unsigned int)li->data.data_integer;
        break;
      case LOG_ITEM_LOG_PRIO:
        prio = (enum loglevel)li->data.data_integer;
        break;
      case LOG_ITEM_LOG_MESSAGE: {
        const char *nl;
        msg = li->data.data_string.str;
        msg_len = li->data.data_string.length;

        /*
          If the message contains a newline, copy the message and
          replace the newline so we may print a valid log line,
          i.e. one that doesn't have a line-break in the middle
          of its message.
        */

        if ((nl = (const char *)memchr(msg, '\n', msg_len)) != nullptr) {
          assert(line_buffer == nullptr);

          if (line_buffer != nullptr) log_bs->free(line_buffer);
          line_buffer = (char *)log_bs->malloc(msg_len + 1);
          if (nullptr == line_buffer) {
            msg =
                "The submitted error message contains a newline, "
                "and a buffer to sanitize it for the traditional "
                "log could not be allocated. File a bug against "
                "the message corresponding to this MY-... code.";
            msg_len = strlen(msg);
          } else {
            memcpy(line_buffer, msg, msg_len);
            line_buffer[msg_len] = '\0';
            char *nl2 = line_buffer;
            while ((nl2 = log_bs->find_first(nl2, '\n')) != nullptr)
              *(nl2++) = ' ';
            msg = line_buffer;
          }
        }
      } break;
      case LOG_ITEM_LOG_LABEL:
        label = li->data.data_string.str;
        label_len = li->data.data_string.length;
        break;
      case LOG_ITEM_SRV_SUBSYS:
        subsys = li->data.data_string.str;
        if ((subsys_len = li->data.data_string.length) > 12) subsys_len = 12;
        break;
      case LOG_ITEM_LOG_TIMESTAMP:
        iso_timestamp = li->data.data_string.str;
        iso_len = li->data.data_string.length;
        break;
      case LOG_ITEM_SRV_THREAD:
        thread_id = (my_thread_id)li->data.data_integer;
        break;
      default:
        out_fields--;
    }
    out_types |= item_type;
  }

  if (out_fields > 0) {
    if (!(out_types & LOG_ITEM_LOG_MESSAGE)) {
      msg =
          "No error message, or error message of non-string type. "
          "This is almost certainly a bug!";
      msg_len = strlen(msg);

      prio = ERROR_LEVEL;                  // force severity
      out_types &= ~(LOG_ITEM_LOG_LABEL);  // regenerate label
      out_types |= LOG_ITEM_LOG_MESSAGE;   // we added a message

      out_fields = LOG_SERVICE_INVALID_ARGUMENT;
    }

    char internal_buff[LOG_BUFF_MAX];
    size_t buff_size = sizeof(internal_buff);
    char *buff_line = internal_buff;
    size_t len;

    if (!(out_types & LOG_ITEM_LOG_LABEL)) {
      label = log_bi->label_from_prio(prio);
      label_len = strlen(label);
    }

    if (!(out_types & LOG_ITEM_LOG_TIMESTAMP)) {
      char buff_local_time[iso8601_size];
      make_iso8601_timestamp(buff_local_time, my_micro_time(),
                             iso8601_sysvar_logtimestamps);
      iso_timestamp = buff_local_time;
      iso_len = strlen(buff_local_time);
    }

    len = log_bs->substitute(
        buff_line, buff_size, "%.*s %u [%.*s] [MY-%06u] [%.*s] %.*s",
        (int)iso_len, iso_timestamp, thread_id, (int)label_len, label, errcode,
        (int)subsys_len, subsys, (int)msg_len, msg);

    // We return only the message, not the whole line, so memcpy() is needed.
    log_item *output_buffer = log_bi->line_get_output_buffer(ll);

    if (output_buffer != nullptr) {
      if (msg_len < output_buffer->data.data_buffer.length)
        output_buffer->data.data_buffer.length = msg_len;
      else  // truncate message to buffer-size (and leave space for '\0')
        msg_len = output_buffer->data.data_buffer.length - 1;

      memcpy((char *)output_buffer->data.data_buffer.str, msg, msg_len);
      output_buffer->data.data_buffer.str[msg_len] = '\0';

      output_buffer->type = LOG_ITEM_RET_BUFFER;
    }
    // write the record to the stream / log-file

    Log_sink_rotate::getInstance()->write_errstream(buff_line, len);
  }
  log_bi->line_item_iter_release(it);

  return out_fields;
}

//
DEFINE_METHOD(int, log_service_imp::run, (void *instance, log_line *ll)) {
  return log_sink_trad_rotate(instance, ll);
}

/**
  Find the end of the current field (' ')

  @param  parse_from  start of the token
  @param  token_end   where to store the address of the delimiter found
  @param  buf_end     end of the input line

  @retval -1   delimiter not found, "parsing" failed
  @retval >=0  length of token
*/
ssize_t parse_trad_field(const char *parse_from, const char **token_end,
                         const char *buf_end) {
  assert(token_end != nullptr);
  *token_end = (const char *)memchr(parse_from, ' ', buf_end - parse_from);
  return (*token_end == nullptr) ? -1 : (*token_end - parse_from);
}

/**
  Derive the event's priority (SYSTEM_LEVEL, ERROR_LEVEL, ...)
  from a textual label. If the label can not be identified,
  default to ERROR_LEVEL as it is better to keep something
  that needn't be kept than to discard something that shouldn't
  be.

  @param  label  The prio label as a \0 terminated C-string.

  @retval  the priority (as an enum loglevel)
*/
enum loglevel log_prio_from_label(const char *label) {
  if (0 == native_strcasecmp(label, "SYSTEM")) return SYSTEM_LEVEL;
  if (0 == native_strcasecmp(label, "WARNING")) return WARNING_LEVEL;
  if (0 == native_strcasecmp(label, "NOTE")) return INFORMATION_LEVEL;

  return ERROR_LEVEL; /* purecov: inspected */
}

#ifndef LOG_SINK_PFS_ERROR_CODE_LENGTH
#define LOG_SINK_PFS_ERROR_CODE_LENGTH 10
#endif

/**
  Parse a single line in an error log of this format.

  @param line_start   pointer to the beginning of the line ('{')
  @param line_length  length of the line

  @retval  0   Success
  @retval !=0  Failure (out of memory, malformed argument, etc.)
*/
DEFINE_METHOD(log_service_error, log_service_imp::parse_log_line,
              (const char *line_start, size_t line_length)) {
  char timestamp[iso8601_size];  // space for timestamp + '\0'
  char label[16];
  char msg[LOG_BUFF_MAX];
  ssize_t len;
  const char *line_end = line_start + line_length;
  const char *start = line_start, *end = line_end;

  // sanity check: must start with timestamp
  if (*line_start != '2') return LOG_SERVICE_PARSE_ERROR;

  // parse timestamp
  if ((len = parse_trad_field(start, &end, line_end)) <= 0)
    return LOG_SERVICE_PARSE_ERROR;
  if (len >= iso8601_size) return LOG_SERVICE_ARGUMENT_TOO_LONG;

  memcpy(timestamp, start, len);
  timestamp[len] = '\0';  // terminate target buffer
  start = end + 1;
  auto timestamp_j = log_bi->parse_iso8601_timestamp(timestamp, len);

  // thread_id
  auto thread_id = atoi(start);
  if ((len = parse_trad_field(start, &end, line_end)) <= 0)
    return LOG_SERVICE_PARSE_ERROR;
  start = end + 1;

  // parse prio/label
  if ((len = parse_trad_field(start, &end, line_end)) <= 0)
    return LOG_SERVICE_PARSE_ERROR;
  if ((len > ((ssize_t)sizeof(label))) || (len < 3))
    return LOG_SERVICE_ARGUMENT_TOO_LONG;
  len -= 2;  // We won't copy [ ]
  memcpy(label, start + 1, len);
  label[len] = '\0';
  start = end + 1;
  auto prio = log_prio_from_label(label);

  // parse err_code
  if ((len = parse_trad_field(start, &end, line_end)) <= 0)
    return LOG_SERVICE_PARSE_ERROR;
  if ((len < 4) || (0 != strncmp(start, "[MY-", 4)))
    return LOG_SERVICE_PARSE_ERROR;
  len -= 2;  // We won't copy [ ]

  if (len >= 1) return LOG_SERVICE_ARGUMENT_TOO_LONG;

  char error_code[LOG_SINK_PFS_ERROR_CODE_LENGTH] = {0};

  strncpy(error_code, ++start, len);
  error_code[len] = '\0';
  auto error_code_length = len;  // Should always be 3+6
  start = end + 1;

  // parse subsys
  if ((len = parse_trad_field(start, &end, line_end)) <= 0)
    return LOG_SERVICE_PARSE_ERROR;
  len -= 2;  // We won't copy [ ]
  // LOG_SINK_PFS_SUBSYS_LENGTH is 7

  /** Column ERROR_LOG_SUBSYS. */
  char subsys[7] = {0};
  if (len >= 7) return LOG_SERVICE_ARGUMENT_TOO_LONG;
  memcpy(subsys, start + 1, len);
  auto subsys_length = len;
  subsys[len] = '\0';
  start = end + 1;

  // parse message - truncate if needed.
  len = line_end - start;

  /*
    If we have a message for this, it becomes more easily searchable.
    This is provided in the hope that between error code (which it appears
    we have) and subsystem (which it appears we also have), a human reader
    can find out what happened here even if the log file is not available
    to them. If the log file IS available, they should be able to just find
    this event's time stamp in that file and see whether the line contains
    anything that would break parsing.
  */
  const char *parsing_failed =
      "No message found for this event while parsing a traditional error log! "
      "If you wish to investigate this, use this event's timestamp to find the "
      "offending line in the error log file.";
  if (len <= 0) {
    start = parsing_failed;
    len = strlen(parsing_failed);
  }

  // Truncate length if needed.
  if (len >= ((ssize_t)sizeof(msg))) {
    len = sizeof(msg) - 1;
  }

  // Copy as much of the message as we have space for.
  strncpy(msg, start, len);
  msg[len] = '\0';

  /*
    Store adjusted length in log-event.
    m_message_length is a uint while len is ssize_t, but we capped at
    sizeof(msg) above which is less than either, so we won't
    assert(len <= UINT_MAX) here.
    log_sink_pfs_event_add() below will assert() if m_message_length==0,
    but this should be prevented by us setting a fixed message above if
    parsed resulting in an empty message field. (If parsing any of the
    other fields failed, we won't try to add a message to the
    performance-schema table in the first place.)
  */
  auto message_length = len;

  // Add event to ring-buffer.
  return log_ps->event_add(timestamp_j, thread_id, prio, error_code,
                           error_code_length, subsys, subsys_length, msg,
                           message_length);
}

/**
  Open a new instance.

  @retval  <0        a new instance could not be created
  @retval  =0        success, returned handle is valid
*/
DEFINE_METHOD(log_service_error, log_service_imp::open,
              (log_line * ll [[maybe_unused]], void **instance)) {
  if (instance == nullptr)               // nowhere to return the handle
    return LOG_SERVICE_INVALID_ARGUMENT; /* purecov: inspected */

  *instance = nullptr;

  *instance = Log_sink_rotate::getInstance();

  return Log_sink_rotate::getInstance()->open();
}

/**
  Open a new instance.

  @retval  <0        a new instance could not be created
  @retval  =0        success, returned handle is valid
*/
DEFINE_METHOD(log_service_error, log_service_imp::get_log_name,
              (void *instance, char *buf, size_t bufsize)) {
  if (instance == nullptr)               // nowhere to return the handle
    return LOG_SERVICE_INVALID_ARGUMENT; /* purecov: inspected */
  return Log_sink_rotate::getInstance()->get_log_name(buf, bufsize);
}

/**
  Close and release an instance. Flushes any buffers.

  @retval  <0        an error occurred
  @retval  =0        success
*/
DEFINE_METHOD(log_service_error, log_service_imp::close, (void **instance)) {
  if (instance == nullptr) return LOG_SERVICE_INVALID_ARGUMENT;

  *instance = nullptr;
  return Log_sink_rotate::getInstance()->close();
}

/**
  Flush any buffers.  This function will be called by the server
  on FLUSH ERROR LOGS.  The service may write its buffers, close
  and re-open any log files to work with log-rotation, etc.
  The flush function MUST NOT itself log anything (as the caller
  holds THR_LOCK_log_stack)!
  A service implementation may provide a nullptr if it does not
  wish to provide a flush function.

  @retval  LOG_SERVICE_NOTHING_DONE        no work was done
  @retval  LOG_SERVICE_SUCCESS             flush completed without incident
  @retval  otherwise                       an error occurred
*/
DEFINE_METHOD(log_service_error, log_service_imp::flush, (void **instance)) {
  if (instance == nullptr) return LOG_SERVICE_INVALID_ARGUMENT;

  return Log_sink_rotate::getInstance()->reopen();
}

/**
  Get characteristics of a log-service.

  @retval  <0        an error occurred
  @retval  >=0       characteristics (a set of log_service_chistics flags)
*/
DEFINE_METHOD(int, log_service_imp::characteristics, (void)) {
  return LOG_SERVICE_SINGLETON | LOG_SERVICE_SINK | LOG_SERVICE_LOG_PARSER |
         LOG_SERVICE_PFS_SUPPORT;
}

/**
  De-initialization method for Component used when unloading the Component.

  @return Status of performed operation
  @retval false success
  @retval true  failure
*/
mysql_service_status_t log_service_exit() {
  if (inited) {
    mysql_service_component_sys_variable_unregister->unregister_variable(
        LOG_SINK_ROTATE, "total_size");
    mysql_service_component_sys_variable_unregister->unregister_variable(
        LOG_SINK_ROTATE, "max_size");

    inited = false;
    return Log_sink_rotate::getInstance()->DeInit();
  }
  return true;
}

/**
  Initialization entry method for Component used when loading the Component.

  @return Status of performed operation
  @retval false success
  @retval true  failure
*/
mysql_service_status_t log_service_init() {
  if (inited) return true;

  log_bi = mysql_service_log_builtins;
  log_bs = mysql_service_log_builtins_string;
  log_ps = mysql_service_log_sink_perfschema;
  log_sys_var_init();
  Log_sink_rotate::getInstance()->Init();
  inited = true;

  return false;
}

/* implementing a service: log_service */
BEGIN_SERVICE_IMPLEMENTATION(log_sink_rotate, log_service)
log_service_imp::run, log_service_imp::flush, log_service_imp::open,
    log_service_imp::close, log_service_imp::characteristics,
    log_service_imp::parse_log_line,
    log_service_imp::get_log_name END_SERVICE_IMPLEMENTATION();

/* component provides: just the log_service service, for now */
BEGIN_COMPONENT_PROVIDES(log_sink_rotate)
PROVIDES_SERVICE(log_sink_rotate, log_service), END_COMPONENT_PROVIDES();

/* component requires: log-builtins */
BEGIN_COMPONENT_REQUIRES(log_sink_rotate)
REQUIRES_SERVICE(component_sys_variable_register),
    REQUIRES_SERVICE(component_sys_variable_unregister),
    REQUIRES_SERVICE(log_builtins), REQUIRES_SERVICE(log_builtins_string),
    REQUIRES_SERVICE(log_sink_perfschema), END_COMPONENT_REQUIRES();

/* component description */
BEGIN_COMPONENT_METADATA(log_sink_rotate)
METADATA("mysql.author", "GreatOpenSource"), METADATA("mysql.license", "GPL"),
    METADATA("log_service_type", "sink"), END_COMPONENT_METADATA();

/* component declaration */
DECLARE_COMPONENT(log_sink_rotate, "mysql:log_sink_rotate")
log_service_init, log_service_exit END_DECLARE_COMPONENT();

/* components contained in this library.
   for now assume that each library will have exactly one component. */
DECLARE_LIBRARY_COMPONENTS &COMPONENT_REF(log_sink_rotate)
    END_DECLARE_LIBRARY_COMPONENTS

    /* EOT */
