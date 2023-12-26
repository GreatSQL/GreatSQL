/* Copyright (c) 2017, 2022, Oracle and/or its affiliates.
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
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include "sql/regexp/regexp_engine.h"

#include <stdint.h>

#include <algorithm>  // copy
#include <string>     // strlen

#include "my_dbug.h"
#include "sql/regexp/errors.h"
#include "sql/sql_class.h"
#include "template_utils.h"

namespace regexp {

UBool QueryNotKilled(const void *thd, int32_t) {
  return !static_cast<const THD *>(thd)->is_killed();
}

const char *icu_version_string() { return U_ICU_VERSION; }

void Regexp_engine::Reset(const std::u16string &subject) {
  m_error_code = U_ZERO_ERROR;
  auto usubject = subject.data();
  int length = subject.size();
  uregex_setText(m_re, pointer_cast<const UChar *>(usubject), length,
                 &m_error_code);
  m_current_subject = subject;
  m_replace_buffer.clear();
  m_replace_buffer_pos = 0;
}

bool Regexp_engine::Matches(int start, int occurrence) {
  bool found = uregex_find(m_re, start, &m_error_code);

  for (int i = 1; i < occurrence && found; ++i)
    found = uregex_findNext(m_re, &m_error_code);

  check_icu_status(m_error_code);
  return found;
}

const std::u16string &Regexp_engine::Replace(const std::u16string &replacement,
                                             int start, int occurrence) {
  if ((current_thd->variables.sql_mode & MODE_ORACLE) &&
      (std::u16string::value_type)start > m_current_subject.length()) {
    m_replace_buffer.assign(m_current_subject);
    return m_replace_buffer;
  }
  // Find the first match, starting at the chosen position, ...
  bool found = uregex_find(m_re, start, &m_error_code);

  int end_of_previous_match = 0;
  // ... fast-forward to the chosen occurrence, ...
  for (int i = 1; i < occurrence && found; ++i) {
    end_of_previous_match = uregex_end(m_re, 0, &m_error_code);
    found = uregex_findNext(m_re, &m_error_code);
  }

  /*
    If no match is found, the return value is the same as the subject
    string. Since `found` is false, this is what would eventually be produced
    by falling through to the call to uregex_appendTail() below anyway. This
    is more than just premature optimization, however; according to the ICU
    documentation, calls to uregex_appendReplacement() and uregex_appendTail()
    are meant to be chained, and a call to uregex_appendTail() without a prior
    call to uregex_appendReplacement() leads to ICU trying to free the buffer
    that we own, thus causing a double-delete.
  */
  if (!found && U_SUCCESS(m_error_code)) return m_current_subject;

  m_replace_buffer.resize(std::min(m_current_subject.size(), HardLimit()));

  /**
    If 'replacement' contains the escape character '\', 'replacement' needs to
    be modified, because in many scenarios of oracle, escape characters are
    treated as ordinary characters, but escape processing is performed in the
    ICU library. Another reason is that subexpression references of the form
    '\n' need to be replaced with matching strings

    In addition, if 'replacement' contains '$' characters, you need to modify
    'replacement', because the icu library treats '$n' as a reference to a
    regular subexpression, but oracle does not, so you need to escape the '$'
    characters into normal characters
  */
  bool need_modify_replacement =
      (current_thd->variables.sql_mode & MODE_ORACLE) &&
      (std::find(replacement.begin(), replacement.end(), u'\\') !=
           replacement.end() ||
       std::find(replacement.begin(), replacement.end(), u'$') !=
           replacement.end());

  // ... replacing all occurrences if 'occurrence' is 0, and finally ...
  AppendHead(std::max(end_of_previous_match, start));
  if (found) {
    do {
      if (need_modify_replacement) {
        std::u16string new_replacement{0};
        new_replacement.reserve(m_current_subject.size());
        int32_t group_count = uregex_groupCount(m_re, &m_error_code);
        if (check_icu_status(m_error_code) ||
            ReplaceReplacementORA(group_count, replacement, new_replacement)) {
          m_replace_buffer.clear();
          return m_replace_buffer;
        }
        AppendReplacement(new_replacement);
        continue;
      }
      AppendReplacement(replacement);
    } while (occurrence == 0 && uregex_findNext(m_re, &m_error_code));
  }

  // ... put the part after the matches back.
  AppendTail();

  check_icu_status(m_error_code);
  m_replace_buffer.resize(m_replace_buffer_pos);
  return m_replace_buffer;
}

std::pair<int, int> Regexp_engine::MatchedSubstring() {
  int start = uregex_start(m_re, 0, &m_error_code);
  int end = uregex_end(m_re, 0, &m_error_code);
  int start_in_bytes = start * sizeof(UChar);
  int length_in_bytes = (end - start) * sizeof(UChar);

  if (U_FAILURE(m_error_code)) return {-1, -1};

  return {start_in_bytes, length_in_bytes};
}

void Regexp_engine::AppendHead(size_t size) {
  DBUG_TRACE;

  if (size == 0) return;

  // This won't be written to in case of errors.
  int32_t text_length32 = 0;
  auto text = uregex_getText(m_re, &text_length32, &m_error_code);
#ifndef NDEBUG
  size_t text_length = text_length32;
#endif

  // We make sure we are not in an error state before we start copying.
  if (m_error_code != U_ZERO_ERROR) return;

  assert(size <= text_length);
  if (m_replace_buffer.size() < size) m_replace_buffer.resize(size);
  std::copy(text, text + size, &m_replace_buffer.at(0));
  m_replace_buffer_pos = size;
}

int Regexp_engine::TryToAppendReplacement(const std::u16string &replacement) {
  if (m_replace_buffer.empty()) return 0;
  UChar *ptr =
      pointer_cast<UChar *>(&m_replace_buffer.at(0) + m_replace_buffer_pos);
  int capacity = m_replace_buffer.size() - m_replace_buffer_pos;
  auto repl = replacement.data();
  size_t length = replacement.size();
  return uregex_appendReplacement(m_re, pointer_cast<const UChar *>(repl),
                                  length, &ptr, &capacity, &m_error_code);
}

void Regexp_engine::AppendReplacement(const std::u16string &replacement) {
  DBUG_TRACE;

  int replacement_size = TryToAppendReplacement(replacement);

  if (m_error_code == U_BUFFER_OVERFLOW_ERROR) {
    size_t required_buffer_size = m_replace_buffer_pos + replacement_size;
    if (required_buffer_size >= HardLimit()) return;
    /*
      The buffer size was inadequate to write the replacement, but there is
      still room to try and grow the buffer before we hit the hard limit. ICU
      will now have set capacity to zero, m_replace_buffer to the newly
      allocated buffer, and m_error_code to U_BUFFER_OVERFLOW_ERROR. So we try
      once again, by resetting these values after reserving the extra space in
      the buffer.
    */
    m_replace_buffer.resize(required_buffer_size);
    m_error_code = U_ZERO_ERROR;
    TryToAppendReplacement(replacement);
  }
  m_replace_buffer_pos += replacement_size;
}

int Regexp_engine::TryToAppendTail() {
  if (m_replace_buffer.empty()) return 0;
  UChar *ptr =
      pointer_cast<UChar *>(&m_replace_buffer.at(0) + m_replace_buffer_pos);
  int capacity = m_replace_buffer.size() - m_replace_buffer_pos;
  return uregex_appendTail(m_re, &ptr, &capacity, &m_error_code);
}

void Regexp_engine::AppendTail() {
  DBUG_TRACE;

  int tail_size = TryToAppendTail();

  if (m_error_code == U_BUFFER_OVERFLOW_ERROR) {
    size_t required_buffer_size = m_replace_buffer_pos + tail_size;
    if (required_buffer_size >= HardLimit()) return;

    /*
      The buffer size was inadequate to write the tail, but there is still
      room to try and grow the buffer before we hit the hard limit. ICU will
      now have worked in preflight mode, i.e. it has set capacity to zero and
      m_error_code to U_BUFFER_OVERFLOW_ERROR or
      U_STRING_NOT_TERMINATED_WARNING. So we try once again, by resetting
      these values after reserving the extra space in the buffer.
    */
    m_replace_buffer.resize(required_buffer_size);
    m_error_code = U_ZERO_ERROR;
    TryToAppendTail();
  }
  m_replace_buffer_pos += tail_size;
}

bool Regexp_engine::ReplaceReplacementORA(int32_t group_count,
                                          const std::u16string &src,
                                          std::u16string &replacement) {
  replacement.clear();
  auto add_backslash = [](std::u16string &r, std::u16string::value_type ch) {
    std::u16string::size_type pos = 0;
    while ((pos = r.find_first_of(ch, pos)) != std::u16string::npos) {
      r.insert(pos, 1, u'\\');
      pos += 2;
    }
  };

  for (auto it = src.begin(); it != src.end(); ++it) {
    if (*it == u'\\') { /* Handle escape characters */
      auto next = std::next(it);
      if (next != src.end()) {
        if (group_count > 0 && (std::isdigit(*next) && (*next != u'0'))) {
          if (*next - u'0' <= group_count) {
            /**
              Replace the specified matching subexpression represented by '\n'
              with the matched string. If 'n' > group_count then '\n' replace
              with "".
            */
            std::u16string subexpr_str;
            subexpr_str.resize(m_current_subject.size());
            size_t length = uregex_group(
                m_re, *next - u'0', pointer_cast<UChar *>(&subexpr_str.at(0)),
                subexpr_str.size(), &m_error_code);
            if (m_error_code == U_BUFFER_OVERFLOW_ERROR) {
              if (length >= HardLimit()) {
                my_error(ER_REGEXP_BUFFER_OVERFLOW, myf(0));
                return true;
              }
              subexpr_str.resize(length);
              m_error_code = U_ZERO_ERROR;
              length = uregex_group(m_re, *next - u'0',
                                    pointer_cast<UChar *>(&subexpr_str.at(0)),
                                    subexpr_str.size(), &m_error_code);
              if (check_icu_status(m_error_code)) return true;
            }
            subexpr_str.resize(length);
            /**
              Since the icu library just put out escape characters for backslash
              escapes, but oracle requires to retain backslash, therefore, it is
              necessary to insert backslash escape before the backslash
            */
            add_backslash(subexpr_str, u'\\');
            add_backslash(subexpr_str, u'$');
            replacement.append(subexpr_str);
          }
        } else {
          /**
            If it is not a matching subexpression, the backslash is required to
            be preserved, but '\\' is special and is only treated as a backslash
            character
          */
          if (*next != u'\\') {
            replacement.push_back(u'\\');
          }
          replacement.push_back(*it);
          replacement.push_back(*next);
        }
        it = next;
      } else {
        /** the backslash is required to be preserved */
        replacement.push_back(u'\\');
        replacement.push_back(*it);
      }
    } else {
      if (*it == u'$') { /* Insert '\' to escape '$' */
        replacement.push_back(u'\\');
      }
      /* Copy the characters in the original string */
      replacement.push_back(*it);
    }
  }
  return false;
}

}  // namespace regexp
