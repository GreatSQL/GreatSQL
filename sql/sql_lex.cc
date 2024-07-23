
/*
   Copyright (c) 2000, 2021, Oracle and/or its affiliates.
   Copyright (c) 2022, Huawei Technologies Co., Ltd.
   Copyright (c) 2023, 2024, GreatDB Software Co., Ltd.

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

/* A lexical scanner on a temporary buffer with a yacc interface */

#include "sql/sql_lex.h"

#include <algorithm>  // find_if, iter_swap, reverse
#include <climits>
#include <cstdlib>
#include <initializer_list>

#include "field_types.h"
#include "m_ctype.h"
#include "my_alloc.h"
#include "my_dbug.h"
#include "mysql/mysql_lex_string.h"
#include "mysql/service_mysql_alloc.h"
#include "mysql_version.h"  // MYSQL_VERSION_ID
#include "mysqld_error.h"
#include "prealloced_array.h"  // Prealloced_array
#include "scope_guard.h"
#include "sql/auth/auth_acls.h"
#include "sql/auth/auth_common.h"  // get_column_grant
#include "sql/current_thd.h"
#include "sql/derror.h"
#include "sql/error_handler.h"
#include "sql/item_func.h"
#include "sql/item_subselect.h"
#include "sql/lex_symbol.h"
#include "sql/lexer_yystype.h"
#include "sql/mysqld.h"  // table_alias_charset
#include "sql/nested_join.h"
#include "sql/opt_hints.h"
#include "sql/parse_location.h"
#include "sql/parse_tree_column_attrs.h"
#include "sql/parse_tree_nodes.h"  // PT_with_clause
#include "sql/protocol.h"
#include "sql/select_lex_visitor.h"
#include "sql/sp.h"
#include "sql/sp_head.h"      // sp_head
#include "sql/sp_instr.h"     // sp_lex_instr
#include "sql/sp_pcontext.h"  // sp_pcontext
#include "sql/sql_admin.h"
#include "sql/sql_base.h"
#include "sql/sql_class.h"  // THD
#include "sql/sql_cmd.h"
#include "sql/sql_digest_stream.h"
#include "sql/sql_error.h"
#include "sql/sql_insert.h"  // Sql_cmd_insert_base
#include "sql/sql_lex_hash.h"
#include "sql/sql_lex_hints.h"
#include "sql/sql_optimizer.h"  // JOIN
#include "sql/sql_parse.h"      // add_to_list
#include "sql/sql_plugin.h"     // plugin_unlock_list
#include "sql/sql_profile.h"
#include "sql/sql_resolver.h"
#include "sql/sql_show.h"   // append_identifier
#include "sql/sql_table.h"  // primary_key_name
#include "sql/sql_yacc.h"
#include "sql/system_variables.h"
#include "sql/table_function.h"
#include "sql/window.h"
#include "sql_update.h"  // Sql_cmd_update
#include "template_utils.h"

class PT_hint_list;

extern int HINT_PARSER_parse(THD *thd, Hint_scanner *scanner,
                             PT_hint_list **ret);

static int lex_one_token(Lexer_yystype *yylval, THD *thd);
static int lex_token(YYSTYPE *yacc_yylval, YYLTYPE *yylloc, THD *thd);

/**
  LEX_STRING constant for null-string to be used in parser and other places.
*/
const LEX_STRING null_lex_str = {nullptr, 0};
const LEX_CSTRING null_lex_cstr = {nullptr, 0};
/**
  Mapping from enum values in enum_binlog_stmt_unsafe to error codes.

  @note The order of the elements of this array must correspond to
  the order of elements in enum_binlog_stmt_unsafe.

  Todo/fixme Bug#22860121 ER_BINLOG_UNSAFE_* FAMILY OF ERROR CODES IS UNUSED
    suggests to turn ER_BINLOG_UNSAFE* to private consts/messages.
*/
const int
    Query_tables_list::binlog_stmt_unsafe_errcode[BINLOG_STMT_UNSAFE_COUNT] = {
        ER_BINLOG_UNSAFE_LIMIT,
        ER_BINLOG_UNSAFE_SYSTEM_TABLE,
        ER_BINLOG_UNSAFE_AUTOINC_COLUMNS,
        ER_BINLOG_UNSAFE_UDF,
        ER_BINLOG_UNSAFE_SYSTEM_VARIABLE,
        ER_BINLOG_UNSAFE_SYSTEM_FUNCTION,
        ER_BINLOG_UNSAFE_NONTRANS_AFTER_TRANS,
        ER_BINLOG_UNSAFE_MULTIPLE_ENGINES_AND_SELF_LOGGING_ENGINE,
        ER_BINLOG_UNSAFE_MIXED_STATEMENT,
        ER_BINLOG_UNSAFE_INSERT_IGNORE_SELECT,
        ER_BINLOG_UNSAFE_INSERT_SELECT_UPDATE,
        ER_BINLOG_UNSAFE_WRITE_AUTOINC_SELECT,
        ER_BINLOG_UNSAFE_REPLACE_SELECT,
        ER_BINLOG_UNSAFE_CREATE_IGNORE_SELECT,
        ER_BINLOG_UNSAFE_CREATE_REPLACE_SELECT,
        ER_BINLOG_UNSAFE_CREATE_SELECT_AUTOINC,
        ER_BINLOG_UNSAFE_UPDATE_IGNORE,
        ER_BINLOG_UNSAFE_INSERT_TWO_KEYS,
        ER_BINLOG_UNSAFE_AUTOINC_NOT_FIRST,
        ER_BINLOG_UNSAFE_FULLTEXT_PLUGIN,
        ER_BINLOG_UNSAFE_SKIP_LOCKED,
        ER_BINLOG_UNSAFE_NOWAIT,
        ER_BINLOG_UNSAFE_XA,
        ER_BINLOG_UNSAFE_DEFAULT_EXPRESSION_IN_SUBSTATEMENT,
        ER_BINLOG_UNSAFE_ACL_TABLE_READ_IN_DML_DDL,
        ER_CREATE_SELECT_WITH_GIPK_DISALLOWED_IN_SBR};

/*
  Names of the index hints (for error messages). Keep in sync with
  index_hint_type
*/

const char *index_hint_type_name[] = {"IGNORE INDEX", "USE INDEX",
                                      "FORCE INDEX"};

Prepare_error_tracker::~Prepare_error_tracker() {
  if (unlikely(thd->is_error())) thd->lex->mark_broken();
}

/**
  @note The order of the elements of this array must correspond to
  the order of elements in enum_explain_type.
*/
const char *Query_block::type_str[static_cast<int>(
    enum_explain_type::EXPLAIN_total)] = {"NONE",          "PRIMARY",
                                          "SIMPLE",        "DERIVED",
                                          "SUBQUERY",      "UNION",
                                          "INTERSECT",     "EXCEPT",
                                          "UNION RESULT",  "INTERSECT RESULT",
                                          "EXCEPT RESULT", "RESULT" /*unary*/,
                                          "MATERIALIZED"};

Table_ident::Table_ident(Protocol *protocol, const LEX_CSTRING &db_arg,
                         const LEX_CSTRING &table_arg, bool force)
    : table(table_arg), sel(nullptr), table_function(nullptr) {
  if (!force && protocol->has_client_capability(CLIENT_NO_SCHEMA))
    db = NULL_CSTR;
  else
    db = db_arg;
}

/**
  This method resolves the structure of a variable declared as:
     rec t1%ROWTYPE;
  It opens the table "t1" and copies its structure to %ROWTYPE variable.
*/
bool Table_ident::resolve_table_rowtype_ref(THD *thd,
                                            List<Create_field> &defs) {
  Open_tables_backup open_tables_state_backup;
  thd->reset_n_backup_open_tables_state(&open_tables_state_backup, 0);

  Table_ref *table_list;
  LEX *save_lex = thd->lex;
  bool rc = true;

  /*
    Create a temporary LEX on stack and switch to it.
    In case of VIEW, open_tables_only_view_structure() will open more
    tables/views recursively. We want to avoid them to stick to the current LEX.
  */
  sp_lex_local lex(thd, thd->lex);
  thd->lex = &lex;

  lex.context_analysis_only = CONTEXT_ANALYSIS_ONLY_VIEW;
  // Make %ROWTYPE variables see temporary tables that shadow permanent tables
  thd->temporary_tables = open_tables_state_backup.temporary_tables;
  table_list = lex.query_block->add_table_to_list(
      thd, this, NULL, 0, TL_READ_NO_INSERT, MDL_SHARED_READ);
  if (!table_list) {
    my_error(ER_TYPE_CANT_OPEN_TABLE, MYF(0), table.str);
    thd->lex = save_lex;
    thd->restore_backup_open_tables_state(&open_tables_state_backup);
    return true;
  }

  List<Create_field> defs_create_field;
  if (!check_table_access(thd, SELECT_ACL, table_list, true, UINT_MAX, false) &&
      !get_table_share_only_view_structure(thd, table_list,
                                           &defs_create_field)) {
    List_iterator_fast<Create_field> create_field_iter(defs_create_field);
    Create_field *cf;
    while ((cf = create_field_iter++)) {
      defs.push_back(cf);
    }
    rc = false;
  } else {
    my_error(ER_TYPE_CANT_OPEN_TABLE, MYF(0), table.str);
  }

  lex.cleanup_after_one_table_open();
  for (TABLE *table = thd->temporary_tables; table; table = table->next) {
    table->query_id = 0;  // Avoid ER_CANT_REOPEN_TABLE
  }
  thd->temporary_tables = NULL;  // Avoid closing temporary tables
  close_thread_tables(thd);
  thd->lex = save_lex;
  thd->restore_backup_open_tables_state(&open_tables_state_backup);
  return rc;
}

/**
  This method implementation is very close to fill_schema_table_by_open().
*/
bool Qualified_column_ident::resolve_type_ref(THD *thd, Create_field *def,
                                              bool resolve_action) {
  if (!resolve_action) {
    if (def->init(thd, "", MYSQL_TYPE_NULL, nullptr, nullptr, 0, NULL, NULL,
                  &NULL_CSTR, 0, nullptr, thd->variables.collation_database,
                  false, 0, nullptr, nullptr, nullptr, {},
                  dd::Column::enum_hidden_type::HT_VISIBLE)) {
      return true;
    }
    return false;
  }
  Open_tables_backup open_tables_state_backup;
  thd->reset_n_backup_open_tables_state(&open_tables_state_backup, 0);

  Table_ref *table_list;
  LEX *save_lex = thd->lex;
  bool rc = true;

  sp_lex_local lex(thd, thd->lex);
  thd->lex = &lex;

  lex.context_analysis_only = CONTEXT_ANALYSIS_ONLY_VIEW;
  // Make %TYPE variables see temporary tables that shadow permanent tables
  thd->temporary_tables = open_tables_state_backup.temporary_tables;
  table_list = lex.query_block->add_table_to_list(
      thd, this, NULL, 0, TL_READ_NO_INSERT, MDL_SHARED_READ);
  if (!table_list) {
    my_error(ER_TYPE_CANT_OPEN_TABLE, MYF(0), table.str);
    thd->lex = save_lex;
    thd->restore_backup_open_tables_state(&open_tables_state_backup);
    return true;
  }

  List<Create_field> defs_create_field;
  if (!check_table_access(thd, SELECT_ACL, table_list, true, UINT_MAX, false) &&
      !get_table_share_only_view_structure(thd, table_list,
                                           &defs_create_field)) {
    List_iterator_fast<Create_field> create_field_iter(defs_create_field);
    Create_field *cf;
    bool is_found = false;
    while ((cf = create_field_iter++)) {
      if (!my_strnncoll(system_charset_info, (const uchar *)cf->field_name,
                        strlen(cf->field_name), (const uchar *)m_column.str,
                        m_column.length)) {
        *def = *cf;
        def->flags &= (uint)~NOT_NULL_FLAG;
        is_found = true;
        rc = false;
      }
    }
    if (!is_found) {
      my_error(ER_BAD_FIELD_ERROR, MYF(0), m_column.str, table.str);
    }
  } else {
    my_error(ER_TYPE_CANT_OPEN_TABLE, MYF(0), table.str);
  }

  lex.cleanup_after_one_table_open();
  for (TABLE *table = thd->temporary_tables; table; table = table->next) {
    table->query_id = 0;  // Avoid ER_CANT_REOPEN_TABLE
  }
  thd->temporary_tables = NULL;  // Avoid closing temporary tables
  close_thread_tables(thd);
  thd->lex = save_lex;
  thd->restore_backup_open_tables_state(&open_tables_state_backup);
  return rc;
}

bool lex_init() {
  DBUG_TRACE;

  for (CHARSET_INFO **cs = all_charsets;
       cs < all_charsets + array_elements(all_charsets) - 1; cs++) {
    if (*cs && (*cs)->ctype && is_supported_parser_charset(*cs)) {
      if (init_state_maps(*cs)) return true;  // OOM
    }
  }

  return false;
}

void lex_free() {  // Call this when daemon ends
  DBUG_TRACE;
}

void st_parsing_options::reset() {
  allows_variable = true;
  allows_select_into = true;
}

/**
 Cleans slave connection info.
*/
void struct_slave_connection::reset() {
  user = nullptr;
  password = nullptr;
  plugin_auth = nullptr;
  plugin_dir = nullptr;
}

/**
  Perform initialization of Lex_input_stream instance.

  Basically, a buffer for a pre-processed query. This buffer should be large
  enough to keep a multi-statement query. The allocation is done once in
  Lex_input_stream::init() in order to prevent memory pollution when
  the server is processing large multi-statement queries.
*/

bool Lex_input_stream::init(THD *thd, const char *buff, size_t length) {
  DBUG_EXECUTE_IF("bug42064_simulate_oom",
                  DBUG_SET("+d,simulate_out_of_memory"););

  query_charset = thd->charset();

  m_cpp_buf = (char *)thd->alloc(length + 1);

  DBUG_EXECUTE_IF("bug42064_simulate_oom",
                  DBUG_SET("-d,bug42064_simulate_oom"););

  if (m_cpp_buf == nullptr) return true;

  m_thd = thd;
  reset(buff, length);

  return false;
}

/**
  Prepare Lex_input_stream instance state for use for handling next SQL
  statement.

  It should be called between two statements in a multi-statement query.
  The operation resets the input stream to the beginning-of-parse state,
  but does not reallocate m_cpp_buf.
*/

void Lex_input_stream::reset(const char *buffer, size_t length) {
  yylineno = 1;
  yytoklen = 0;
  yylval = nullptr;
  lookahead_token = grammar_selector_token;
  skip_digest = false;
  /*
    Lex_input_stream modifies the query string in one special case (sic!).
    yyUnput() modifises the string when patching version comments.
    This is done to prevent newer slaves from executing a different
    statement than older masters.

    For now, cast away const here. This means that e.g. SHOW PROCESSLIST
    can see partially patched query strings. It would be better if we
    could replicate the query string as is and have the slave take the
    master version into account.
  */
  m_ptr = const_cast<char *>(buffer);
  m_tok_start = nullptr;
  m_tok_end = nullptr;
  m_end_of_query = buffer + length;
  m_buf = buffer;
  m_buf_length = length;
  m_echo = true;
  m_cpp_tok_start = nullptr;
  m_cpp_tok_end = nullptr;
  m_body_utf8 = nullptr;
  m_cpp_utf8_processed_ptr = nullptr;
  next_state = MY_LEX_START;
  found_semicolon = nullptr;
  ignore_space = m_thd->variables.sql_mode & MODE_IGNORE_SPACE;
  stmt_prepare_mode = false;
  multi_statements = true;
  in_comment = NO_COMMENT;
  m_underscore_cs = nullptr;
  m_cpp_ptr = m_cpp_buf;
  dblink = false;
  dblink_count = 0;
}

/**
  The operation is called from the parser in order to
  1) designate the intention to have utf8 body;
  1) Indicate to the lexer that we will need a utf8 representation of this
     statement;
  2) Determine the beginning of the body.

  @param thd        Thread context.
  @param begin_ptr  Pointer to the start of the body in the pre-processed
                    buffer.
*/

void Lex_input_stream::body_utf8_start(THD *thd, const char *begin_ptr) {
  assert(begin_ptr);
  assert(m_cpp_buf <= begin_ptr && begin_ptr <= m_cpp_buf + m_buf_length);

  size_t body_utf8_length =
      (m_buf_length / thd->variables.character_set_client->mbminlen) *
      my_charset_utf8mb3_bin.mbmaxlen;

  m_body_utf8 = (char *)thd->alloc(body_utf8_length + 1);
  m_body_utf8_ptr = m_body_utf8;
  *m_body_utf8_ptr = 0;

  m_cpp_utf8_processed_ptr = begin_ptr;
}

/**
  @brief The operation appends unprocessed part of pre-processed buffer till
  the given pointer (ptr) and sets m_cpp_utf8_processed_ptr to end_ptr.

  The idea is that some tokens in the pre-processed buffer (like character
  set introducers) should be skipped.

  Example:
    CPP buffer: SELECT 'str1', _latin1 'str2';
    m_cpp_utf8_processed_ptr -- points at the "SELECT ...";
    In order to skip "_latin1", the following call should be made:
      body_utf8_append(<pointer to "_latin1 ...">, <pointer to " 'str2'...">)

  @param ptr      Pointer in the pre-processed buffer, which specifies the
                  end of the chunk, which should be appended to the utf8
                  body.
  @param end_ptr  Pointer in the pre-processed buffer, to which
                  m_cpp_utf8_processed_ptr will be set in the end of the
                  operation.
*/

void Lex_input_stream::body_utf8_append(const char *ptr, const char *end_ptr) {
  assert(m_cpp_buf <= ptr && ptr <= m_cpp_buf + m_buf_length);
  assert(m_cpp_buf <= end_ptr && end_ptr <= m_cpp_buf + m_buf_length);

  if (!m_body_utf8) return;

  if (m_cpp_utf8_processed_ptr >= ptr) return;

  size_t bytes_to_copy = ptr - m_cpp_utf8_processed_ptr;

  memcpy(m_body_utf8_ptr, m_cpp_utf8_processed_ptr, bytes_to_copy);
  m_body_utf8_ptr += bytes_to_copy;
  *m_body_utf8_ptr = 0;

  m_cpp_utf8_processed_ptr = end_ptr;
}

/**
  The operation appends unprocessed part of the pre-processed buffer till
  the given pointer (ptr) and sets m_cpp_utf8_processed_ptr to ptr.

  @param ptr  Pointer in the pre-processed buffer, which specifies the end
              of the chunk, which should be appended to the utf8 body.
*/

void Lex_input_stream::body_utf8_append(const char *ptr) {
  body_utf8_append(ptr, ptr);
}

/**
  The operation converts the specified text literal to the utf8 and appends
  the result to the utf8-body.

  @param thd      Thread context.
  @param txt      Text literal.
  @param txt_cs   Character set of the text literal.
  @param end_ptr  Pointer in the pre-processed buffer, to which
                  m_cpp_utf8_processed_ptr will be set in the end of the
                  operation.
*/

void Lex_input_stream::body_utf8_append_literal(THD *thd, const LEX_STRING *txt,
                                                const CHARSET_INFO *txt_cs,
                                                const char *end_ptr) {
  if (!m_cpp_utf8_processed_ptr) return;

  LEX_STRING utf_txt;

  if (!my_charset_same(txt_cs, &my_charset_utf8mb3_general_ci)) {
    thd->convert_string(&utf_txt, &my_charset_utf8mb3_general_ci, txt->str,
                        txt->length, txt_cs);
  } else {
    utf_txt.str = txt->str;
    utf_txt.length = txt->length;
  }

  /* NOTE: utf_txt.length is in bytes, not in symbols. */

  memcpy(m_body_utf8_ptr, utf_txt.str, utf_txt.length);
  m_body_utf8_ptr += utf_txt.length;
  *m_body_utf8_ptr = 0;

  m_cpp_utf8_processed_ptr = end_ptr;
}

void Lex_input_stream::add_digest_token(uint token, Lexer_yystype *yylval) {
  if (m_digest != nullptr) {
    m_digest = digest_add_token(m_digest, token, yylval);
  }
}

void Lex_input_stream::reduce_digest_token(uint token_left, uint token_right) {
  if (m_digest != nullptr) {
    m_digest = digest_reduce_token(m_digest, token_left, token_right);
  }
}

void LEX::assert_ok_set_current_query_block() {
  // (2) Only owning thread could change m_current_query_block
  // (1) bypass for bootstrap and "new THD"
  assert(!current_thd || !thd ||  //(1)
         thd == current_thd);     //(2)
}

LEX::~LEX() {
  destroy_query_tables_list();
  plugin_unlock_list(nullptr, plugins.begin(), plugins.size());
  unit = nullptr;  // Created in mem_root - no destructor
  query_block = nullptr;
  m_current_query_block = nullptr;
}

/**
  Reset a LEX object so that it is ready for a new query preparation
  and execution.
  Pointers to query expression and query block objects are set to NULL.
  This is correct, as they point into a mem_root that has been recycled.
*/

void LEX::reset() {
  // CREATE VIEW
  create_view_mode = enum_view_create_mode::VIEW_CREATE_NEW;
  create_view_algorithm = VIEW_ALGORITHM_UNDEFINED;
  create_view_suid = true;
  create_force_view_mode = false;
  create_force_view_table_not_found = false;

  // CREATE STORED PRCEDURE/FUNCTION
  create_sp_mode = enum_sp_create_mode::SP_CREATE_NEW;

  context_stack.clear();
  unit = nullptr;
  query_block = nullptr;
  m_current_query_block = nullptr;
  all_query_blocks_list = nullptr;

  bulk_insert_row_cnt = 0;

  purge_value_list.clear();

  kill_value_list.clear();

  set_var_list.clear();
  param_list.clear();
  lock_wait_var = nullptr;
  prepared_stmt_params.clear();
  context_analysis_only = 0;
  safe_to_cache_query = true;
  is_cursor_get_structure = false;
  is_cmp_udt_table = false;
  is_include_udt_table_item = false;
  insert_table = nullptr;
  insert_table_leaf = nullptr;
  parsing_options.reset();
  alter_info = nullptr;
  part_info = nullptr;
  duplicates = DUP_ERROR;
  ignore = false;
  spname = nullptr;
  sphead = nullptr;
  set_sp_current_parsing_ctx(nullptr);
  m_sql_cmd = nullptr;
  query_tables = nullptr;
  reset_query_tables_list(false);
  expr_allows_subselect = true;
  use_only_table_context = false;
  contains_plaintext_password = false;
  keep_diagnostics = DA_KEEP_NOTHING;
  m_statement_options = 0;
  next_binlog_file_nr = 0;

  name.str = nullptr;
  name.length = 0;
  event_parse_data = nullptr;
  profile_options = PROFILE_NONE;
  select_number = 0;
  allow_sum_func = 0;
  m_deny_window_func = 0;
  m_subquery_to_derived_is_impossible = false;
  in_sum_func = nullptr;
  create_info = nullptr;
  server_options.reset();
  seq_option.reset();
  explain_format = nullptr;
  is_explain_analyze = false;
  using_hypergraph_optimizer = false;
  is_lex_started = true;
  reset_slave_info.all = false;
  mi.channel = nullptr;

  wild = nullptr;
  mark_broken(false);
  reset_exec_started();
  max_execution_time = 0;
  reparse_common_table_expr_at = 0;
  reparse_derived_table_condition = false;
  opt_hints_global = nullptr;
  binlog_need_explicit_defaults_ts = false;
  m_extended_show = false;
  option_type = OPT_DEFAULT;
  check_opt = HA_CHECK_OPT();

  clear_privileges();
  grant_as.cleanup();
  donor_transaction_id = nullptr;
  alter_user_attribute = enum_alter_user_attribute::ALTER_USER_COMMENT_NOT_USED;
  m_is_replication_deprecated_syntax_used = false;
  m_was_replication_command_executed = false;

  m_with_func = false;

  grant_if_exists = false;
  ignore_unknown_user = false;
  reset_rewrite_required();
  set_is_materialized_view(false);
  use_rapid_exec = false;
  rapid_fields = nullptr;
}

/**
  Call lex_start() before every query that is to be prepared and executed.
  Because of this, it's critical not to do too many things here.  (We already
  do too much)

  The function creates a query_block and a query_block_query_expression object.
  These objects should rather be created by the parser bottom-up.
*/

bool lex_start(THD *thd) {
  DBUG_TRACE;

  LEX *lex = thd->lex;

  lex->thd = thd;
  lex->reset();
  // Initialize the cost model to be used for this query
  thd->init_cost_model();

  const bool status = lex->start(thd);

  assert(lex->m_IS_table_stats.is_valid() == false);
  assert(lex->m_IS_tablespace_stats.is_valid() == false);

  return status;
}

bool LEX::start(THD *thd_arg) {
  DBUG_TRACE;

  thd = thd_arg;
  reset();

  const bool status = new_top_level_query();
  assert(current_query_block() == nullptr);
  m_current_query_block = query_block;

  m_IS_table_stats.invalidate_cache();
  m_IS_tablespace_stats.invalidate_cache();

  return status;
}

/**
  Call this function after preparation and execution of a query.
*/

void lex_end(LEX *lex) {
  DBUG_TRACE;
  DBUG_PRINT("enter", ("lex: %p", lex));

  /* release used plugins */
  lex->release_plugins();

  sp_head::destroy(lex->sphead);
  lex->sphead = nullptr;
  lex->has_sp = false;
  lex->has_notsupported_func = false;
}

void LEX::release_plugins() {
  if (!plugins.empty()) /* No function call and no mutex if no plugins. */
  {
    plugin_unlock_list(nullptr, plugins.begin(), plugins.size());
    plugins.clear();
  }
}

/**
  Clear execution state for a statement after it has been prepared or executed,
  and before it is (re-)executed.
*/
void LEX::clear_execution() {
  // Clear execution state for all query expressions:
  for (Query_block *sl = all_query_blocks_list; sl;
       sl = sl->next_select_in_list())
    sl->master_query_expression()->clear_execution();

  set_current_query_block(query_block);

  reset_exec_started();

  /*
    m_view_ctx_list contains all the view tables view_ctx objects and must
    be emptied now since it's going to be re-populated below as we reiterate
    over all query_tables and call Table_ref::prepare_security().
  */
  thd->m_view_ctx_list.clear();

  // Reset all table references so that they can be bound with new TABLEs
  /*
    NOTE: We should reset whole table list here including all tables added
    by prelocking algorithm (it is not a problem for substatements since
    they have their own table list).
    Another note: this loop uses query_tables so does not see Table_refs
    which represent join nests.
  */
  for (Table_ref *tr = query_tables; tr != nullptr; tr = tr->next_global)
    tr->reset();
}

Query_block *LEX::new_empty_query_block() {
  Query_block *select =
      new (thd->mem_root) Query_block(thd->mem_root, nullptr, nullptr);
  if (select == nullptr) return nullptr; /* purecov: inspected */

  select->parent_lex = this;

  return select;
}

Query_expression *LEX::create_query_expr_and_block(
    THD *thd, Query_block *current_query_block, Item *where, Item *having,
    enum_parsing_context ctx) {
  if (current_query_block != nullptr &&
      current_query_block->nest_level >= MAX_SELECT_NESTING) {
    my_error(ER_TOO_HIGH_LEVEL_OF_NESTING_FOR_SELECT, MYF(0));
    return nullptr;
  }

  auto *const new_expression = new (thd->mem_root) Query_expression(ctx);
  if (new_expression == nullptr) return nullptr;

  auto *const new_query_block =
      new (thd->mem_root) Query_block(thd->mem_root, where, having);
  if (new_query_block == nullptr) return nullptr;

  // Link the new query expression below the current query block, if any
  if (current_query_block != nullptr)
    new_expression->include_down(this, current_query_block);

  new_query_block->include_down(this, new_expression);

  new_query_block->parent_lex = this;
  new_query_block->include_in_global(&this->all_query_blocks_list);
  // Set root query_term a priori. It is usually set later by
  // Parse_context::finalize_query_expression. This is necessary since it
  // doesn't always happen during server bootstrap of the dictionary, e.g. in
  // View_metadata_updater_context which creates a top level query whose
  // Query_expression/Query_block is not parsed/contextualized, so we never
  // call finalize_query_expression in the usual way, after contextualization
  // of PT_query_expression.
  new_expression->set_query_term(new_query_block);
  return new_expression;
}

/**
  Create new query_block_query_expression and query_block objects for a query
  block, which can be either a top-level query or a subquery. For the second and
  subsequent query block of a UNION query, use LEX::new_set_operation_query()
  instead.
  Set the new query_block as the current query_block of the LEX object.

  @param curr_query_block    current query block, NULL if an outer-most
                             query block should be created.

  @return new query specification if successful, NULL if error
*/
Query_block *LEX::new_query(Query_block *curr_query_block) {
  DBUG_TRACE;

  Name_resolution_context *outer_context = current_context();

  enum_parsing_context parsing_place =
      curr_query_block != nullptr ? curr_query_block->parsing_place : CTX_NONE;

  Query_expression *const new_query_expression = create_query_expr_and_block(
      thd, curr_query_block, nullptr, nullptr, parsing_place);
  if (new_query_expression == nullptr) return nullptr;
  Query_block *const new_query_block =
      new_query_expression->first_query_block();

  if (new_query_block->set_context(nullptr)) return nullptr;
  /*
    Assume that a subquery has an outer name resolution context
    (even a non-lateral derived table may have outer references).
    When we come here for a view, it's when we parse the view (in
    open_tables()): we parse it as a standalone query, where parsing_place
    is CTX_NONE, so the outer context is set to nullptr. Then we'll resolve the
    view's query (thus, using no outer context). Later we may merge the
    view's query, but that happens after resolution, so there's no chance that
    a view "looks outside" (uses outer references). An assertion in
    resolve_derived() checks this.
  */
  if (parsing_place == CTX_NONE)  // Outer-most query block
  {
  } else if (parsing_place == CTX_INSERT_UPDATE &&
             curr_query_block->master_query_expression()->is_set_operation()) {
    /*
      Outer references are not allowed for
      subqueries in INSERT ... ON DUPLICATE KEY UPDATE clauses,
      when the outer query expression is a UNION.
    */
    assert(new_query_block->context.outer_context == nullptr);
  } else {
    new_query_block->context.outer_context = outer_context;
  }
  /*
    in subquery is SELECT query and we allow resolution of names in SELECT
    list
  */
  new_query_block->context.resolve_in_select_list = true;
  DBUG_PRINT("outer_field", ("ctx %p <-> SL# %d", &new_query_block->context,
                             new_query_block->select_number));

  return new_query_block;
}

/**
  Create new query_block object for all branches of a UNION, EXCEPT or INTERSECT
  except the left-most one.
  Set the new query_block as the current query_block of the LEX object.

  @param curr_query_block current query specification
  @return new query specification if successful, NULL if an error occurred.
*/

Query_block *LEX::new_set_operation_query(Query_block *curr_query_block) {
  DBUG_TRACE;
  assert(unit != nullptr && query_block != nullptr);

  // Is this the outer-most query expression?
  bool const outer_most = curr_query_block->master_query_expression() == unit;
  /*
     Only the last SELECT can have INTO. Since the grammar won't allow INTO in
     a nested SELECT, we make this check only when creating a query block on
     the outer-most level:
  */
  if (outer_most && result) {
    my_error(ER_MISPLACED_INTO, MYF(0));
    return nullptr;
  }

  Query_block *const select = new_empty_query_block();
  if (!select) return nullptr; /* purecov: inspected */

  select->include_neighbour(this, curr_query_block);

  Query_expression *const sel_query_expression =
      select->master_query_expression();

  if (select->set_context(
          sel_query_expression->first_query_block()->context.outer_context))
    return nullptr; /* purecov: inspected */

  select->include_in_global(&all_query_blocks_list);

  /*
    By default we assume that this is a regular subquery, in which resolution
    of names in SELECT list is allowed.
  */
  select->context.resolve_in_select_list = true;

  return select;
}

Query_block *Query_expression::create_post_processing_block() {
  Query_block *first_qb = first_query_block();
  DBUG_TRACE;

  Query_block *const qb = first_qb->parent_lex->new_empty_query_block();
  if (qb == nullptr) return nullptr; /* purecov: inspected */
  qb->include_standalone(this);
  qb->select_number = INT_MAX;
  qb->linkage = GLOBAL_OPTIONS_TYPE;
  qb->select_limit = nullptr;

  qb->set_context(first_qb->context.outer_context);

  /* allow item list resolving in fake select for ORDER BY */
  qb->context.resolve_in_select_list = true;
  qb->no_table_names_allowed = true;
  first_qb->parent_lex->pop_context();
  return qb;
}

bool Query_expression::is_leaf_block(Query_block *qb) {
  for (Query_block *q = first_query_block(); q != nullptr;
       q = q->next_query_block()) {
    if (q == qb) return true;
  }
  return false;
}

/**
  Given a LEX object, create a query expression object
  (query_block_query_expression) and a query block object (query_block).

  @return false if successful, true if error
*/

bool LEX::new_top_level_query() {
  DBUG_TRACE;

  // Assure that the LEX does not contain any query expression already
  assert(unit == nullptr && query_block == nullptr);

  // Check for the special situation when using INTO OUTFILE and LOAD DATA.
  assert(result == nullptr);

  query_block = new_query(nullptr);
  if (query_block == nullptr) return true; /* purecov: inspected */

  unit = query_block->master_query_expression();

  return false;
}

/**
  Initialize a LEX object, a query expression object
  (query_block_query_expression) and a query block object (query_block). All
  objects are passed as pointers so they can be stack-allocated. The purpose of
  this structure is for short-lived procedures that need a LEX and a query block
  object.

  Do not extend the struct with more query objects after creation.

  The struct can be abandoned after use, no cleanup is needed.

  @param sel_query_expression  Pointer to the query expression object
  @param select    Pointer to the query block object
*/

void LEX::new_static_query(Query_expression *sel_query_expression,
                           Query_block *select)

{
  DBUG_TRACE;

  reset();

  assert(unit == nullptr && query_block == nullptr &&
         current_query_block() == nullptr);

  select->parent_lex = this;

  select->include_down(this, sel_query_expression);

  select->include_in_global(&all_query_blocks_list);

  (void)select->set_context(nullptr);

  query_block = select;
  unit = sel_query_expression;

  set_current_query_block(select);

  select->context.resolve_in_select_list = true;
}

/**
  Check whether preparation state for prepared statement is invalid.
  Preparation state amy become invalid if a repreparation is forced,
  e.g because of invalid metadata, and that repreparation fails.

  @returns true if preparation state is invalid, false otherwise.
*/
bool LEX::check_preparation_invalid(THD *thd_arg) {
  DBUG_TRACE;

  if (unlikely(is_broken())) {
    // Force a Reprepare, to get a fresh LEX
    if (ask_to_reprepare(thd_arg)) return true;
  }

  return false;
}

bool LEX::check_udt_type_field() {
  // sum(udt) or sum(udt) over()
  if (in_sum_func || m_deny_window_func) {
    my_error(ER_NOT_SUPPORTED_YET, MYF(0), "udt value in aggregate function");
    return true;
  }
  // group by udt or partition by udt or order by udt
  if (current_query_block() && current_query_block()->window_order_field) {
    for (auto order = current_query_block()->order_list.first; order;
         order = order->next) {
      if ((*order->item)->result_type() == ROW_RESULT) {
        my_error(ER_TYPE_IN_ORDER_BY, MYF(0));
        return true;
      }
    }
    for (auto group = current_query_block()->group_list.first; group;
         group = group->next) {
      if ((*group->item)->result_type() == ROW_RESULT) {
        my_error(ER_TYPE_IN_ORDER_BY, MYF(0));
        return true;
      }
    }
  }

  if (current_query_block()) {
    for (Window &w : current_query_block()->m_windows) {
      auto check_udt_type = [](const PT_order_list *order_list) -> bool {
        for (ORDER *o = order_list->value.first; o; o = o->next) {
          if ((*o->item)->fixed && (*o->item)->result_type() == ROW_RESULT) {
            my_error(ER_TYPE_IN_ORDER_BY, MYF(0));
            return true;
          }
        }

        return false;
      };

      if (w.get_partition_list() && check_udt_type(w.get_partition_list()))
        return true;
      if (w.get_order_list() && check_udt_type(w.get_order_list())) return true;
    }
  }

  return false;
}

Yacc_state::~Yacc_state() {
  if (yacc_yyss) {
    my_free(yacc_yyss);
    my_free(yacc_yyvs);
    my_free(yacc_yyls);
  }
}

static bool consume_optimizer_hints(Lex_input_stream *lip) {
  const my_lex_states *state_map = lip->query_charset->state_maps->main_map;
  int whitespace = 0;
  uchar c = lip->yyPeek();
  size_t newlines = 0;

  for (; state_map[c] == MY_LEX_SKIP;
       whitespace++, c = lip->yyPeekn(whitespace)) {
    if (c == '\n') newlines++;
  }

  if (lip->yyPeekn(whitespace) == '/' && lip->yyPeekn(whitespace + 1) == '*' &&
      lip->yyPeekn(whitespace + 2) == '+') {
    lip->yylineno += newlines;
    lip->yySkipn(whitespace);  // skip whitespace

    Hint_scanner hint_scanner(lip->m_thd, lip->yylineno, lip->get_ptr(),
                              lip->get_end_of_query() - lip->get_ptr(),
                              lip->m_digest);
    PT_hint_list *hint_list = nullptr;
    int rc = HINT_PARSER_parse(lip->m_thd, &hint_scanner, &hint_list);
    if (rc == 2) return true;  // Bison's internal OOM error
    if (rc == 1) {
      /*
        This branch is for 2 cases:
        1. YYABORT in the hint parser grammar (we use it to process OOM errors),
        2. open commentary error.
      */
      lip->start_token();  // adjust error message text pointer to "/*+"
      return true;
    }
    lip->yylineno = hint_scanner.get_lineno();
    lip->yySkipn(hint_scanner.get_ptr() - lip->get_ptr());
    lip->yylval->optimizer_hints = hint_list;   // NULL in case of syntax error
    lip->m_digest = hint_scanner.get_digest();  // NULL is digest buf. is full.
    return false;
  } else
    return false;
}

static int find_keyword(Lex_input_stream *lip, uint len, bool function) {
  const char *tok = lip->get_tok_start();

  const SYMBOL *symbol =
      function ? Lex_hash::sql_keywords_and_funcs.get_hash_symbol(tok, len)
               : Lex_hash::sql_keywords.get_hash_symbol(tok, len);

  if (symbol) {
    lip->yylval->keyword.symbol = symbol;
    lip->yylval->keyword.str = const_cast<char *>(tok);
    lip->yylval->keyword.length = len;

    if ((symbol->tok == NOT_SYM) &&
        (lip->m_thd->variables.sql_mode & MODE_HIGH_NOT_PRECEDENCE))
      return NOT2_SYM;
    if ((symbol->tok == OR_OR_SYM) &&
        !(lip->m_thd->variables.sql_mode & MODE_PIPES_AS_CONCAT)) {
      push_deprecated_warn(lip->m_thd, "|| as a synonym for OR", "OR");
      return OR2_SYM;
    }

    lip->yylval->optimizer_hints = nullptr;
    if (symbol->group & SG_HINTABLE_KEYWORDS) {
      lip->add_digest_token(symbol->tok, lip->yylval);
      if (consume_optimizer_hints(lip)) return ABORT_SYM;
      lip->skip_digest = true;
    }

    return symbol->tok;
  }
  return 0;
}

/*
  Check if name is a keyword

  SYNOPSIS
    is_keyword()
    name      checked name (must not be empty)
    len       length of checked name

  RETURN VALUES
    0         name is a keyword
    1         name isn't a keyword
*/

bool is_keyword(const char *name, size_t len) {
  assert(len != 0);
  return Lex_hash::sql_keywords.get_hash_symbol(name, len) != nullptr;
}

/**
  Check if name is a sql function

    @param name      checked name

    @return is this a native function or not
    @retval 0         name is a function
    @retval 1         name isn't a function
*/

bool is_lex_native_function(const LEX_STRING *name) {
  assert(name != nullptr);
  return Lex_hash::sql_keywords_and_funcs.get_hash_symbol(
             name->str, (uint)name->length) != nullptr;
}

/* make a copy of token before ptr and set yytoklen */

static LEX_STRING get_token(Lex_input_stream *lip, uint skip, uint length) {
  LEX_STRING tmp;
  lip->yyUnget();  // ptr points now after last token char
  tmp.length = lip->yytoklen = length;
  tmp.str = lip->m_thd->strmake(lip->get_tok_start() + skip, tmp.length);

  lip->m_cpp_text_start = lip->get_cpp_tok_start() + skip;
  lip->m_cpp_text_end = lip->m_cpp_text_start + tmp.length;

  return tmp;
}

/*
 todo:
   There are no dangerous charsets in mysql for function
   get_quoted_token yet. But it should be fixed in the
   future to operate multichar strings (like ucs2)
*/

static LEX_STRING get_quoted_token(Lex_input_stream *lip, uint skip,
                                   uint length, char quote) {
  LEX_STRING tmp;
  const char *from, *end;
  char *to;
  lip->yyUnget();  // ptr points now after last token char
  tmp.length = lip->yytoklen = length;
  tmp.str = (char *)lip->m_thd->alloc(tmp.length + 1);
  from = lip->get_tok_start() + skip;
  to = tmp.str;
  end = to + length;

  lip->m_cpp_text_start = lip->get_cpp_tok_start() + skip;
  lip->m_cpp_text_end = lip->m_cpp_text_start + length;

  for (; to != end;) {
    if ((*to++ = *from++) == quote) {
      from++;  // Skip double quotes
      lip->m_cpp_text_start++;
    }
  }
  *to = 0;  // End null for safety
  return tmp;
}

/*
  Return an unescaped text literal without quotes
  Fix sometimes to do only one scan of the string
*/

static char *get_text(Lex_input_stream *lip, int pre_skip, int post_skip) {
  uchar c, sep;
  uint found_escape = 0;
  const CHARSET_INFO *cs = lip->m_thd->charset();

  lip->tok_bitmap = 0;
  sep = lip->yyGetLast();  // String should end with this
  while (!lip->eof()) {
    c = lip->yyGet();
    lip->tok_bitmap |= c;
    {
      int l;
      if (use_mb(cs) &&
          (l = my_ismbchar(cs, lip->get_ptr() - 1, lip->get_end_of_query()))) {
        lip->skip_binary(l - 1);
        continue;
      }
    }
    if (c == '\\' && !(lip->m_thd->variables.sql_mode &
                       MODE_NO_BACKSLASH_ESCAPES)) {  // Escaped character
      found_escape = 1;
      if (lip->eof()) return nullptr;
      lip->yySkip();
    } else if (c == sep) {
      if (c == lip->yyGet())  // Check if two separators in a row
      {
        found_escape = 1;  // duplicate. Remember for delete
        continue;
      } else
        lip->yyUnget();

      /* Found end. Unescape and return string */
      const char *str, *end;
      char *start;

      str = lip->get_tok_start();
      end = lip->get_ptr();
      /* Extract the text from the token */
      str += pre_skip;
      end -= post_skip;
      assert(end >= str);

      if (!(start =
                static_cast<char *>(lip->m_thd->alloc((uint)(end - str) + 1))))
        return const_cast<char *>("");  // MEM_ROOT has set error flag

      lip->m_cpp_text_start = lip->get_cpp_tok_start() + pre_skip;
      lip->m_cpp_text_end = lip->get_cpp_ptr() - post_skip;

      if (!found_escape) {
        lip->yytoklen = (uint)(end - str);
        memcpy(start, str, lip->yytoklen);
        start[lip->yytoklen] = 0;
      } else {
        char *to;

        for (to = start; str != end; str++) {
          int l;
          if (use_mb(cs) && (l = my_ismbchar(cs, str, end))) {
            while (l--) *to++ = *str++;
            str--;
            continue;
          }
          if (!(lip->m_thd->variables.sql_mode & MODE_NO_BACKSLASH_ESCAPES) &&
              *str == '\\' && str + 1 != end) {
            switch (*++str) {
              case 'n':
                *to++ = '\n';
                break;
              case 't':
                *to++ = '\t';
                break;
              case 'r':
                *to++ = '\r';
                break;
              case 'b':
                *to++ = '\b';
                break;
              case '0':
                *to++ = 0;  // Ascii null
                break;
              case 'Z':  // ^Z must be escaped on Win32
                *to++ = '\032';
                break;
              case '_':
              case '%':
                *to++ = '\\';  // remember prefix for wildcard
                [[fallthrough]];
              default:
                *to++ = *str;
                break;
            }
          } else if (*str == sep)
            *to++ = *str++;  // Two ' or "
          else
            *to++ = *str;
        }
        *to = 0;
        lip->yytoklen = (uint)(to - start);
      }
      return start;
    }
  }
  return nullptr;  // unexpected end of query
}

uint Lex_input_stream::get_lineno(const char *raw_ptr) const {
  assert(m_buf <= raw_ptr && raw_ptr <= m_end_of_query);
  if (!(m_buf <= raw_ptr && raw_ptr <= m_end_of_query)) return 1;

  uint ret = 1;
  const CHARSET_INFO *cs = m_thd->charset();
  for (const char *c = m_buf; c < raw_ptr; c++) {
    uint mb_char_len;
    if (use_mb(cs) && (mb_char_len = my_ismbchar(cs, c, m_end_of_query))) {
      c += mb_char_len - 1;  // skip the rest of the multibyte character
      continue;              // we don't expect '\n' there
    }
    if (*c == '\n') ret++;
  }
  return ret;
}

Partition_expr_parser_state::Partition_expr_parser_state()
    : Parser_state(GRAMMAR_SELECTOR_PART), result(nullptr) {}

Gcol_expr_parser_state::Gcol_expr_parser_state()
    : Parser_state(GRAMMAR_SELECTOR_GCOL), result(nullptr) {}

Expression_parser_state::Expression_parser_state()
    : Parser_state(GRAMMAR_SELECTOR_EXPR), result(nullptr) {}

Common_table_expr_parser_state::Common_table_expr_parser_state()
    : Parser_state(GRAMMAR_SELECTOR_CTE), result(nullptr) {}

Derived_expr_parser_state::Derived_expr_parser_state()
    : Parser_state(GRAMMAR_SELECTOR_DERIVED_EXPR), result(nullptr) {}
/*
** Calc type of integer; long integer, longlong integer or real.
** Returns smallest type that match the string.
** When using unsigned long long values the result is converted to a real
** because else they will be unexpected sign changes because all calculation
** is done with longlong or double.
*/

static const char *long_str = "2147483647";
static const uint long_len = 10;
static const char *signed_long_str = "-2147483648";
static const char *longlong_str = "9223372036854775807";
static const uint longlong_len = 19;
static const char *signed_longlong_str = "-9223372036854775808";
static const uint signed_longlong_len = 19;
static const char *unsigned_longlong_str = "18446744073709551615";
static const uint unsigned_longlong_len = 20;

static inline uint int_token(const char *str, uint length) {
  if (length < long_len)  // quick normal case
    return NUM;
  bool neg = false;

  if (*str == '+')  // Remove sign and pre-zeros
  {
    str++;
    length--;
  } else if (*str == '-') {
    str++;
    length--;
    neg = true;
  }
  while (*str == '0' && length) {
    str++;
    length--;
  }
  if (length < long_len) return NUM;

  uint smaller, bigger;
  const char *cmp;
  if (neg) {
    if (length == long_len) {
      cmp = signed_long_str + 1;
      smaller = NUM;      // If <= signed_long_str
      bigger = LONG_NUM;  // If >= signed_long_str
    } else if (length < signed_longlong_len)
      return LONG_NUM;
    else if (length > signed_longlong_len)
      return DECIMAL_NUM;
    else {
      cmp = signed_longlong_str + 1;
      smaller = LONG_NUM;  // If <= signed_longlong_str
      bigger = DECIMAL_NUM;
    }
  } else {
    if (length == long_len) {
      cmp = long_str;
      smaller = NUM;
      bigger = LONG_NUM;
    } else if (length < longlong_len)
      return LONG_NUM;
    else if (length > longlong_len) {
      if (length > unsigned_longlong_len) return DECIMAL_NUM;
      cmp = unsigned_longlong_str;
      smaller = ULONGLONG_NUM;
      bigger = DECIMAL_NUM;
    } else {
      cmp = longlong_str;
      smaller = LONG_NUM;
      bigger = ULONGLONG_NUM;
    }
  }
  while (*cmp && *cmp++ == *str++)
    ;
  return ((uchar)str[-1] <= (uchar)cmp[-1]) ? smaller : bigger;
}

/**
  Given a stream that is advanced to the first contained character in
  an open comment, consume the comment.  Optionally, if we are allowed,
  recurse so that we understand comments within this current comment.

  At this level, we do not support version-condition comments.  We might
  have been called with having just passed one in the stream, though.  In
  that case, we probably want to tolerate mundane comments inside.  Thus,
  the case for recursion.

  @retval  Whether EOF reached before comment is closed.
*/
static bool consume_comment(Lex_input_stream *lip,
                            int remaining_recursions_permitted) {
  // only one level of nested comments are allowed
  assert(remaining_recursions_permitted == 0 ||
         remaining_recursions_permitted == 1);
  uchar c;
  while (!lip->eof()) {
    c = lip->yyGet();

    if (remaining_recursions_permitted == 1) {
      if ((c == '/') && (lip->yyPeek() == '*')) {
        push_warning(
            lip->m_thd, Sql_condition::SL_WARNING,
            ER_WARN_DEPRECATED_SYNTAX_NO_REPLACEMENT,
            ER_THD(lip->m_thd, ER_WARN_DEPRECATED_NESTED_COMMENT_SYNTAX));
        lip->yyUnput('(');  // Replace nested "/*..." with "(*..."
        lip->yySkip();      // and skip "("
        lip->yySkip();      /* Eat asterisk */
        if (consume_comment(lip, 0)) return true;
        lip->yyUnput(')');  // Replace "...*/" with "...*)"
        lip->yySkip();      // and skip ")"
        continue;
      }
    }

    if (c == '*') {
      if (lip->yyPeek() == '/') {
        lip->yySkip(); /* Eat slash */
        return false;
      }
    }

    if (c == '\n') lip->yylineno++;
  }

  return true;
}

/**
  yylex() function implementation for the main parser

  @param [out] yacc_yylval   semantic value of the token being parsed (yylval)
  @param [out] yylloc        "location" of the token being parsed (yylloc)
  @param thd                 THD

  @return                    token number

  @note
  MYSQLlex remember the following states from the following MYSQLlex():

  - MY_LEX_END			Found end of query
*/

int MYSQLlex(YYSTYPE *yacc_yylval, YYLTYPE *yylloc, THD *thd) {
  return lex_token(yacc_yylval, yylloc, thd);
}

int ORAlex(YYSTYPE *yacc_yylval, YYLTYPE *yylloc, THD *thd) {
  return lex_token(yacc_yylval, yylloc, thd);
}

static int lex_token(YYSTYPE *yacc_yylval, YYLTYPE *yylloc, THD *thd) {
  auto *yylval = reinterpret_cast<Lexer_yystype *>(yacc_yylval);
  Lex_input_stream *lip = &thd->m_parser_state->m_lip;
  int token;

  if (thd->is_error()) {
    if (thd->get_parser_da()->has_sql_condition(ER_CAPACITY_EXCEEDED))
      return ABORT_SYM;
  }

  if (lip->lookahead_token >= 0) {
    /*
      The next token was already parsed in advance,
      return it.
    */
    token = lip->lookahead_token;
    lip->lookahead_token = -1;
    *yylval = lip->lookahead_yylval;
    lip->yylval = nullptr;
  } else
    token = lex_one_token(yylval, thd);
  yylloc->cpp.start = lip->get_cpp_tok_start();
  yylloc->raw.start = lip->get_tok_start();

  switch (token) {
    case WITH:
      /*
        Parsing 'WITH' 'ROLLUP' requires 2 look ups,
        which makes the grammar LALR(2).
        Replace by a single 'WITH_ROLLUP' token,
        to transform the grammar into a LALR(1) grammar,
        which sql_yacc.yy can process.
      */
      token = lex_one_token(yylval, thd);
      switch (token) {
        case ROLLUP_SYM:
          yylloc->cpp.end = lip->get_cpp_ptr();
          yylloc->raw.end = lip->get_ptr();
          lip->add_digest_token(WITH_ROLLUP_SYM, yylval);
          return WITH_ROLLUP_SYM;
        default:
          /*
            Save the token following 'WITH'
          */
          lip->lookahead_yylval = *(lip->yylval);
          lip->yylval = nullptr;
          lip->lookahead_token = token;
          yylloc->cpp.end = lip->get_cpp_ptr();
          yylloc->raw.end = lip->get_ptr();
          lip->add_digest_token(WITH, yylval);
          return WITH;
      }
      break;
    case START_SYM: {
      yylloc->cpp.end = lip->get_cpp_ptr();
      yylloc->raw.end = lip->get_ptr();
      if (!lip->skip_digest) lip->add_digest_token(START_SYM, yylval);
      lip->skip_digest = false;

      Lexer_yystype save_yylval = *yylval;

      token = lex_one_token(yylval, thd);
      switch (token) {
        case WITH:
          yylloc->cpp.end = lip->get_cpp_ptr();
          yylloc->raw.end = lip->get_ptr();
          if (!lip->skip_digest) lip->add_digest_token(WITH, yylval);
          lip->skip_digest = false;
          lip->yylval->optimizer_hints = save_yylval.optimizer_hints;
          return START_WITH_SYM;
        default:
          /*
            Save the token following 'BEGIN_SYM'
          */
          lip->lookahead_yylval = *(lip->yylval);
          lip->yylval = nullptr;
          lip->lookahead_token = token;

          // restore the yylval because Lexer_yystype.keyword is destroyed
          // in previous lex_one_token(yylval, thd) call.
          // NOTE: lip->yylval also points to yylval. It is necessary to
          // save current token to lip->lookahead_yyval, before restoring
          // old token.
          *yylval = save_yylval;

          yylloc->cpp.end = lip->get_cpp_ptr();
          yylloc->raw.end = lip->get_ptr();

          return START_SYM;
      }
    } break;
    case BEGIN_SYM: {
      yylloc->cpp.end = lip->get_cpp_ptr();
      yylloc->raw.end = lip->get_ptr();
      if (!lip->skip_digest) lip->add_digest_token(BEGIN_SYM, yylval);
      lip->skip_digest = false;

      Lexer_yystype save_yylval = *yylval;
      token = lex_one_token(yylval, thd);
      switch (token) {
        case WORK_SYM:
          yylloc->cpp.end = lip->get_cpp_ptr();
          yylloc->raw.end = lip->get_ptr();
          if (!lip->skip_digest) lip->add_digest_token(WORK_SYM, yylval);
          lip->skip_digest = false;
          lip->yylval->optimizer_hints = save_yylval.optimizer_hints;
          return BEGIN_WORK_SYM;
        default:
          /*
            Save the token following 'BEGIN_SYM'
          */
          lip->lookahead_yylval = *(lip->yylval);
          lip->yylval = nullptr;
          lip->lookahead_token = token;

          // restore the yylval because Lexer_yystype.keyword is destroyed
          // in previous lex_one_token(yylval, thd) call.
          // NOTE: lip->yylval also points to yylval. It is necessary to
          // save current token to lip->lookahead_yyval, before restoring
          // old token.
          *yylval = save_yylval;

          yylloc->cpp.end = lip->get_cpp_ptr();
          yylloc->raw.end = lip->get_ptr();
          return BEGIN_SYM;
      }
    } break;
    case MERGE_SYM: {
      /*
        MERGE_SYM is listed in rule ident_keywords_unambigous.
        It means the yylval should be kept if the next token is not INTO.
        Otherwise, yylval is destroyed in lex_one_token.
      */
      Lexer_yystype save_yylval = *yylval;
      auto skip_digest_merge = lip->skip_digest;
      lip->skip_digest = false;
      token = lex_one_token(yylval, thd);
      switch (token) {
        case INTO:
          yylloc->cpp.end = lip->get_cpp_ptr();
          yylloc->raw.end = lip->get_ptr();
          if (!skip_digest_merge) lip->add_digest_token(MERGE_SYM, yylval);
          if (!lip->skip_digest) lip->add_digest_token(INTO, yylval);
          lip->skip_digest = false;
          // propagate the hints from MERGE_SYM into MERGE_INTO_SYM.
          lip->yylval->optimizer_hints = save_yylval.optimizer_hints;
          return MERGE_INTO_SYM;
        default:
          /*
            Save the token following 'MERGE_SYM'
          */
          lip->lookahead_yylval = *(lip->yylval);
          lip->yylval = nullptr;
          lip->lookahead_token = token;

          // restore the yylval because Lexer_yystype.keyword is destroyed
          // in previous lex_one_token(yylval, thd) call.
          // NOTE: lip->yylval also points to yylval. It is necessary to
          // save current token to lip->lookahead_yyval, before restoring
          // old token.
          *yylval = save_yylval;

          yylloc->cpp.end = lip->get_cpp_ptr();
          yylloc->raw.end = lip->get_ptr();
          if (!skip_digest_merge) lip->add_digest_token(MERGE_SYM, yylval);
          return MERGE_SYM;
      }
    } break;
    case BULK_SYM: {
      /*
        BULK_SYM is listed in rule ident_keywords_unambigous.
        It means the yylval should be kept if the next token is not COLLECT.
        Otherwise, yylval is destroyed in lex_one_token.
      */
      Lexer_yystype save_yylval = *yylval;
      token = lex_one_token(yylval, thd);
      switch (token) {
        case COLLECT_SYM:
          yylloc->cpp.end = lip->get_cpp_ptr();
          yylloc->raw.end = lip->get_ptr();
          lip->add_digest_token(BULK_COLLECT_SYM, yylval);
          return BULK_COLLECT_SYM;
        default:
          /*
            Save the token following 'BULK_SYM'
          */
          lip->lookahead_yylval = *(lip->yylval);
          lip->yylval = nullptr;
          lip->lookahead_token = token;

          // restore the yylval because Lexer_yystype.keyword is destroyed
          // in previous lex_one_token(yylval, thd) call.
          // NOTE: lip->yylval also points to yylval. It is necessary to
          // save current token to lip->lookahead_yyval, before restoring
          // old token.
          *yylval = save_yylval;

          yylloc->cpp.end = lip->get_cpp_ptr();
          yylloc->raw.end = lip->get_ptr();
          lip->add_digest_token(BULK_SYM, yylval);
          return BULK_SYM;
      }
    } break;
    case PERCENT_ORACLE_SYM: {
      /*
        for a%type and so on,it's PERCENT_ORACLE_SYM.
      */
      Lexer_yystype save_yylval = *yylval;
      token = lex_one_token(yylval, thd);
      lip->lookahead_yylval = *(lip->yylval);
      lip->yylval = nullptr;
      lip->lookahead_token = token;
      *yylval = save_yylval;
      yylloc->cpp.end = lip->get_cpp_ptr();
      yylloc->raw.end = lip->get_ptr();
      switch (token) {
        case TYPE_SYM:
        case ROWTYPE_ORACLE_SYM:
        case ISOPEN_SYM:
        case FOUND_SYM:
        case NOTFOUND_SYM:
        case ROWCOUNT_SYM:
          lip->add_digest_token(PERCENT_ORACLE_SYM, yylval);
          return PERCENT_ORACLE_SYM;
        default:
          lip->add_digest_token('%', yylval);
          return '%';
      }
    } break;
    case IN_SYM: {
      /*
        for i in reverse (n) .. n and reverse() conflict when parse.
        REVERSE_SYM must be resolved.
      */
      Lexer_yystype save_yylval = *yylval;
      token = lex_one_token(yylval, thd);
      switch (token) {
        case REVERSE_SYM:
          yylloc->cpp.end = lip->get_cpp_ptr();
          yylloc->raw.end = lip->get_ptr();
          lip->add_digest_token(IN_REVERSE_SYM, yylval);
          return IN_REVERSE_SYM;
        default:
          /*
            Save the token following 'IN_SYM'
          */
          lip->lookahead_yylval = *(lip->yylval);
          lip->yylval = nullptr;
          lip->lookahead_token = token;

          // restore the yylval because Lexer_yystype.keyword is destroyed
          // in previous lex_one_token(yylval, thd) call.
          // NOTE: lip->yylval also points to yylval. It is necessary to
          // save current token to lip->lookahead_yyval, before restoring
          // old token.
          *yylval = save_yylval;

          yylloc->cpp.end = lip->get_cpp_ptr();
          yylloc->raw.end = lip->get_ptr();
          lip->add_digest_token(IN_SYM, yylval);
          return IN_SYM;
      }
    } break;
    case BUILD_SYM: {
      /*
        BUILD_SYM is listed in rule ident_keywords_unambigous.
        It means the yylval should be kept if the next token is not IMMEDIATE or
        DEFERRED. Otherwise, yylval is destroyed in lex_one_token.
      */
      Lexer_yystype save_yylval = *yylval;
      token = lex_one_token(yylval, thd);
      switch (token) {
        case IMMEDIATE_SYM:
          yylloc->cpp.end = lip->get_cpp_ptr();
          yylloc->raw.end = lip->get_ptr();
          lip->add_digest_token(BUILD_IMMEDIATE_SYM, yylval);
          // propagate the hints from BUILD_SYM into BUILD_IMMEDIATE_SYM.
          lip->yylval->optimizer_hints = save_yylval.optimizer_hints;
          return BUILD_IMMEDIATE_SYM;
        case DEFERRED_SYM:
          yylloc->cpp.end = lip->get_cpp_ptr();
          yylloc->raw.end = lip->get_ptr();
          lip->add_digest_token(BUILD_DEFERRED_SYM, yylval);
          // propagate the hints from BUILD_SYM into BUILD_DEFERRED_SYM.
          lip->yylval->optimizer_hints = save_yylval.optimizer_hints;
          return BUILD_DEFERRED_SYM;
        default:
          /*
            Save the token following 'BUILD_SYM'
          */
          lip->lookahead_yylval = *(lip->yylval);
          lip->yylval = nullptr;
          lip->lookahead_token = token;

          // restore the yylval because Lexer_yystype.keyword is destroyed
          // in previous lex_one_token(yylval, thd) call.
          // NOTE: lip->yylval also points to yylval. It is necessary to
          // save current token to lip->lookahead_yyval, before restoring
          // old token.
          *yylval = save_yylval;

          yylloc->cpp.end = lip->get_cpp_ptr();
          yylloc->raw.end = lip->get_ptr();
          lip->add_digest_token(BUILD_SYM, yylval);
          return BUILD_SYM;
      }
    } break;
    case REFRESH_SYM: {
      /*
        REFRESH_SYM is listed in rule ident_keywords_unambigous.
        It means the yylval should be kept if the next token is not COMPLETE.
        Otherwise, yylval is destroyed in lex_one_token.
      */
      Lexer_yystype save_yylval = *yylval;
      token = lex_one_token(yylval, thd);
      switch (token) {
        case COMPLETE_SYM:
          yylloc->cpp.end = lip->get_cpp_ptr();
          yylloc->raw.end = lip->get_ptr();
          lip->add_digest_token(REFRESH_COMPLETE_SYM, yylval);
          // propagate the hints from REFRESH_SYM into REFRESH_COMPLETE_SYM.
          lip->yylval->optimizer_hints = save_yylval.optimizer_hints;
          return REFRESH_COMPLETE_SYM;
        default:
          /*
            Save the token following 'REFRESH_SYM'
          */
          lip->lookahead_yylval = *(lip->yylval);
          lip->yylval = nullptr;
          lip->lookahead_token = token;

          // restore the yylval because Lexer_yystype.keyword is destroyed
          // in previous lex_one_token(yylval, thd) call.
          // NOTE: lip->yylval also points to yylval. It is necessary to
          // save current token to lip->lookahead_yyval, before restoring
          // old token.
          *yylval = save_yylval;

          yylloc->cpp.end = lip->get_cpp_ptr();
          yylloc->raw.end = lip->get_ptr();
          lip->add_digest_token(REFRESH_SYM, yylval);
          return REFRESH_SYM;
      }
    } break;
  }

  yylloc->cpp.end = lip->get_cpp_ptr();
  yylloc->raw.end = lip->get_ptr();
  if (!lip->skip_digest) lip->add_digest_token(token, yylval);
  lip->skip_digest = false;
  return token;
}

static int lex_one_token(Lexer_yystype *yylval, THD *thd) {
  uchar c = 0;
  bool comment_closed;
  int tokval, result_state;
  uint length;
  enum my_lex_states state;
  Lex_input_stream *lip = &thd->m_parser_state->m_lip;
  const CHARSET_INFO *cs = thd->charset();
  const my_lex_states *state_map = cs->state_maps->main_map;
  const uchar *ident_map = cs->ident_map;

  lip->yylval = yylval;  // The global state

  lip->start_token();
  state = lip->next_state;
  lip->next_state = MY_LEX_START;
  for (;;) {
    switch (state) {
      case MY_LEX_START:  // Start of token
        // Skip starting whitespace
        while (state_map[c = lip->yyPeek()] == MY_LEX_SKIP) {
          if (c == '\n') lip->yylineno++;

          lip->yySkip();
        }

        /* Start of real token */
        lip->restart_token();
        c = lip->yyGet();
        state = state_map[c];
        break;
      case MY_LEX_CHAR:  // Unknown or single char token
      case MY_LEX_SKIP:  // This should not happen
        if (c == '-' && lip->yyPeek() == '-' &&
            ((thd->variables.sql_mode & MODE_ORACLE) ||
             (my_isspace(cs, lip->yyPeekn(1)) ||
              my_iscntrl(cs, lip->yyPeekn(1))))) {
          state = MY_LEX_COMMENT;
          break;
        }

        if (c == '-' && lip->yyPeek() == '>')  // '->'
        {
          lip->yySkip();
          lip->next_state = MY_LEX_START;
          if (lip->yyPeek() == '>') {
            lip->yySkip();
            return JSON_UNQUOTED_SEPARATOR_SYM;
          }
          return JSON_SEPARATOR_SYM;
        }

        if (c != ')') lip->next_state = MY_LEX_START;  // Allow signed numbers

        /*
          Check for a placeholder: it should not precede a possible identifier
          because of binlogging: when a placeholder is replaced with its value
          in a query for the binlog, the query must stay grammatically correct.
        */
        if (c == '?' && lip->stmt_prepare_mode && !ident_map[lip->yyPeek()])
          return (PARAM_MARKER);

        if (c == '%' && (thd->variables.sql_mode & MODE_ORACLE)) {
          lip->next_state = MY_LEX_START;
          return PERCENT_ORACLE_SYM;
        }

        return ((int)c);

      case MY_LEX_IDENT_OR_NCHAR:
        if (lip->yyPeek() != '\'') {
          state = MY_LEX_IDENT;
          break;
        }
        /* Found N'string' */
        lip->yySkip();  // Skip '
        if (!(yylval->lex_str.str = get_text(lip, 2, 1))) {
          state = MY_LEX_CHAR;  // Read char by char
          break;
        }
        yylval->lex_str.length = lip->yytoklen;
        return (NCHAR_STRING);

      case MY_LEX_IDENT_OR_DOLLAR_QUOTE:
        state = MY_LEX_IDENT;
        push_deprecated_warn_no_replacement(
            lip->m_thd, "$ as the first character of an unquoted identifier");
        break;

      case MY_LEX_IDENT_OR_HEX:
        if (lip->yyPeek() == '\'') {  // Found x'hex-number'
          state = MY_LEX_HEX_NUMBER;
          break;
        }
        [[fallthrough]];
      case MY_LEX_IDENT_OR_BIN:
        if (lip->yyPeek() == '\'') {  // Found b'bin-number'
          state = MY_LEX_BIN_NUMBER;
          break;
        }
        [[fallthrough]];
      case MY_LEX_IDENT:
        const char *start;
        if (use_mb(cs)) {
          result_state = IDENT_QUOTED;
          switch (my_mbcharlen(cs, lip->yyGetLast())) {
            case 1:
              break;
            case 0:
              if (my_mbmaxlenlen(cs) < 2) break;
              [[fallthrough]];
            default:
              int l =
                  my_ismbchar(cs, lip->get_ptr() - 1, lip->get_end_of_query());
              if (l == 0) {
                state = MY_LEX_CHAR;
                continue;
              }
              lip->skip_binary(l - 1);
          }
          while (ident_map[c = lip->yyGet()]) {
            switch (my_mbcharlen(cs, c)) {
              case 1:
                break;
              case 0:
                if (my_mbmaxlenlen(cs) < 2) break;
                [[fallthrough]];
              default:
                int l;
                if ((l = my_ismbchar(cs, lip->get_ptr() - 1,
                                     lip->get_end_of_query())) == 0)
                  break;
                lip->skip_binary(l - 1);
            }
          }
        } else {
          for (result_state = c; ident_map[c = lip->yyGet()]; result_state |= c)
            ;
          /* If there were non-ASCII characters, mark that we must convert */
          result_state = result_state & 0x80 ? IDENT_QUOTED : IDENT;
        }
        length = lip->yyLength();
        start = lip->get_ptr();
        if (lip->ignore_space) {
          /*
            If we find a space then this can't be an identifier. We notice this
            below by checking start != lex->ptr.
          */
          for (; state_map[c] == MY_LEX_SKIP; c = lip->yyGet()) {
            if (c == '\n') lip->yylineno++;
          }
        }
        if (start == lip->get_ptr() && c == '.' && ident_map[lip->yyPeek()])
          lip->next_state = MY_LEX_IDENT_SEP;
        else {  // '(' must follow directly if function
          lip->yyUnget();
          if ((tokval = find_keyword(lip, length, c == '('))) {
            lip->next_state = MY_LEX_START;  // Allow signed numbers
            return (tokval);                 // Was keyword
          }
          lip->yySkip();  // next state does a unget
        }
        yylval->lex_str = get_token(lip, 0, length);

        /*
           Note: "SELECT _bla AS 'alias'"
           _bla should be considered as a IDENT if charset haven't been found.
           So we don't use MYF(MY_WME) with get_charset_by_csname to avoid
           producing an error.
        */

        if (yylval->lex_str.str[0] == '_') {
          auto charset_name = yylval->lex_str.str + 1;
          const CHARSET_INFO *underscore_cs =
              get_charset_by_csname(charset_name, MY_CS_PRIMARY, MYF(0));
          if (underscore_cs) {
            lip->warn_on_deprecated_charset(underscore_cs, charset_name);
            if (underscore_cs == &my_charset_utf8mb4_0900_ai_ci) {
              /*
                If underscore_cs is utf8mb4, and the collation of underscore_cs
                is the default collation of utf8mb4, then update underscore_cs
                with a value of the default_collation_for_utf8mb4 system
                variable:
              */
              underscore_cs = thd->variables.default_collation_for_utf8mb4;
            }
            yylval->charset = underscore_cs;
            lip->m_underscore_cs = underscore_cs;

            lip->body_utf8_append(lip->m_cpp_text_start,
                                  lip->get_cpp_tok_start() + length);
            return (UNDERSCORE_CHARSET);
          }
        }

        lip->body_utf8_append(lip->m_cpp_text_start);

        lip->body_utf8_append_literal(thd, &yylval->lex_str, cs,
                                      lip->m_cpp_text_end);

        return (result_state);  // IDENT or IDENT_QUOTED

      case MY_LEX_IDENT_SEP:  // Found ident and now '.'
        yylval->lex_str.str = const_cast<char *>(lip->get_ptr());
        yylval->lex_str.length = 1;
        c = lip->yyGet();  // should be '.'
        if (uchar next_c = lip->yyPeek(); ident_map[next_c]) {
          lip->next_state =
              MY_LEX_IDENT_START;  // Next is an ident (not a keyword)
          if (next_c == '$')       // We got .$ident
            push_deprecated_warn_no_replacement(
                lip->m_thd,
                "$ as the first character of an unquoted identifier");
        } else  // Probably ` or "
          lip->next_state = MY_LEX_START;

        return ((int)c);

      case MY_LEX_NUMBER_IDENT:  // number or ident which num-start
        if (lip->yyGetLast() == '0') {
          c = lip->yyGet();
          if (c == 'x') {
            while (my_isxdigit(cs, (c = lip->yyGet())))
              ;
            if ((lip->yyLength() >= 3) && !ident_map[c]) {
              /* skip '0x' */
              yylval->lex_str = get_token(lip, 2, lip->yyLength() - 2);
              return (HEX_NUM);
            }
            lip->yyUnget();
            state = MY_LEX_IDENT_START;
            break;
          } else if (c == 'b') {
            while ((c = lip->yyGet()) == '0' || c == '1')
              ;
            if ((lip->yyLength() >= 3) && !ident_map[c]) {
              /* Skip '0b' */
              yylval->lex_str = get_token(lip, 2, lip->yyLength() - 2);
              return (BIN_NUM);
            }
            lip->yyUnget();
            state = MY_LEX_IDENT_START;
            break;
          }
          lip->yyUnget();
        }

        while (my_isdigit(cs, (c = lip->yyGet())))
          ;
        if (!ident_map[c]) {  // Can't be identifier
          state = MY_LEX_INT_OR_REAL;
          break;
        }
        if (c == 'e' || c == 'E') {
          // The following test is written this way to allow numbers of type 1e1
          if (my_isdigit(cs, lip->yyPeek()) || (c = (lip->yyGet())) == '+' ||
              c == '-') {  // Allow 1E+10
            if (my_isdigit(cs,
                           lip->yyPeek()))  // Number must have digit after sign
            {
              lip->yySkip();
              while (my_isdigit(cs, lip->yyGet()))
                ;
              yylval->lex_str = get_token(lip, 0, lip->yyLength());
              return (FLOAT_NUM);
            }
          }
          lip->yyUnget();
        }
        [[fallthrough]];
      case MY_LEX_IDENT_START:  // We come here after '.'
      {
        result_state = IDENT;
        if (use_mb(cs)) {
          result_state = IDENT_QUOTED;
          while (ident_map[c = lip->yyGet()]) {
            switch (my_mbcharlen(cs, c)) {
              case 1:
                break;
              case 0:
                if (my_mbmaxlenlen(cs) < 2) break;
                [[fallthrough]];
              default:
                int l;
                if ((l = my_ismbchar(cs, lip->get_ptr() - 1,
                                     lip->get_end_of_query())) == 0)
                  break;
                lip->skip_binary(l - 1);
            }
          }
        } else {
          for (result_state = 0; ident_map[c = lip->yyGet()]; result_state |= c)
            ;
          /* If there were non-ASCII characters, mark that we must convert */
          result_state = result_state & 0x80 ? IDENT_QUOTED : IDENT;
        }
        if (c == '.' && ident_map[lip->yyPeek()])
          lip->next_state = MY_LEX_IDENT_SEP;  // Next is '.'

        yylval->lex_str = get_token(lip, 0, lip->yyLength());

        lip->body_utf8_append(lip->m_cpp_text_start);

        lip->body_utf8_append_literal(thd, &yylval->lex_str, cs,
                                      lip->m_cpp_text_end);

        return (result_state);
      }

      case MY_LEX_USER_VARIABLE_DELIMITER:  // Found quote char
      {
        uint double_quotes = 0;
        char quote_char = c;  // Used char
        for (;;) {
          c = lip->yyGet();
          if (c == 0) {
            lip->yyUnget();
            return ABORT_SYM;  // Unmatched quotes
          }

          int var_length;
          if ((var_length = my_mbcharlen(cs, c)) == 1) {
            if (c == quote_char) {
              if (lip->yyPeek() != quote_char) break;
              c = lip->yyGet();
              double_quotes++;
              continue;
            }
          } else if (use_mb(cs)) {
            if ((var_length = my_ismbchar(cs, lip->get_ptr() - 1,
                                          lip->get_end_of_query())))
              lip->skip_binary(var_length - 1);
          }
        }
        if (double_quotes)
          yylval->lex_str = get_quoted_token(
              lip, 1, lip->yyLength() - double_quotes - 1, quote_char);
        else
          yylval->lex_str = get_token(lip, 1, lip->yyLength() - 1);
        if (c == quote_char) lip->yySkip();  // Skip end `
        lip->next_state = MY_LEX_START;

        lip->body_utf8_append(lip->m_cpp_text_start);

        lip->body_utf8_append_literal(thd, &yylval->lex_str, cs,
                                      lip->m_cpp_text_end);

        return (IDENT_QUOTED);
      }
      case MY_LEX_INT_OR_REAL:  // Complete int or incomplete real
        if (c != '.') {         // Found complete integer number.
          yylval->lex_str = get_token(lip, 0, lip->yyLength());
          return int_token(yylval->lex_str.str, (uint)yylval->lex_str.length);
        }
        [[fallthrough]];
      case MY_LEX_REAL:  // Incomplete real number
        while (my_isdigit(cs, c = lip->yyGet()))
          ;

        if (c == 'e' || c == 'E') {
          c = lip->yyGet();
          if (c == '-' || c == '+') c = lip->yyGet();  // Skip sign
          if (!my_isdigit(cs, c)) {                    // No digit after sign
            state = MY_LEX_CHAR;
            break;
          }
          while (my_isdigit(cs, lip->yyGet()))
            ;
          yylval->lex_str = get_token(lip, 0, lip->yyLength());
          return (FLOAT_NUM);
        }
        yylval->lex_str = get_token(lip, 0, lip->yyLength());
        return (DECIMAL_NUM);

      case MY_LEX_HEX_NUMBER:  // Found x'hexstring'
        lip->yySkip();         // Accept opening '
        while (my_isxdigit(cs, (c = lip->yyGet())))
          ;
        if (c != '\'') return (ABORT_SYM);          // Illegal hex constant
        lip->yySkip();                              // Accept closing '
        length = lip->yyLength();                   // Length of hexnum+3
        if ((length % 2) == 0) return (ABORT_SYM);  // odd number of hex digits
        yylval->lex_str = get_token(lip,
                                    2,            // skip x'
                                    length - 3);  // don't count x' and last '
        return (HEX_NUM);

      case MY_LEX_BIN_NUMBER:  // Found b'bin-string'
        lip->yySkip();         // Accept opening '
        while ((c = lip->yyGet()) == '0' || c == '1')
          ;
        if (c != '\'') return (ABORT_SYM);  // Illegal hex constant
        lip->yySkip();                      // Accept closing '
        length = lip->yyLength();           // Length of bin-num + 3
        yylval->lex_str = get_token(lip,
                                    2,            // skip b'
                                    length - 3);  // don't count b' and last '
        return (BIN_NUM);

      case MY_LEX_CMP_OP:  // Incomplete comparison operator
        if (state_map[lip->yyPeek()] == MY_LEX_CMP_OP ||
            state_map[lip->yyPeek()] == MY_LEX_LONG_CMP_OP)
          lip->yySkip();
        if ((tokval = find_keyword(lip, lip->yyLength() + 1, false))) {
          lip->next_state = MY_LEX_START;  // Allow signed numbers
          return (tokval);
        }
        state = MY_LEX_CHAR;  // Something fishy found
        break;

      case MY_LEX_LONG_CMP_OP:  // Incomplete comparison operator
        if (state_map[lip->yyPeek()] == MY_LEX_CMP_OP ||
            state_map[lip->yyPeek()] == MY_LEX_LONG_CMP_OP) {
          lip->yySkip();
          if (state_map[lip->yyPeek()] == MY_LEX_CMP_OP) lip->yySkip();
        }
        if ((tokval = find_keyword(lip, lip->yyLength() + 1, false))) {
          lip->next_state = MY_LEX_START;  // Found long op
          return (tokval);
        }
        state = MY_LEX_CHAR;  // Something fishy found
        break;

      case MY_LEX_BOOL:
        if (c != lip->yyPeek()) {
          state = MY_LEX_CHAR;
          break;
        }
        lip->yySkip();
        tokval = find_keyword(lip, 2, false);  // Is a bool operator
        lip->next_state = MY_LEX_START;        // Allow signed numbers
        return (tokval);

      case MY_LEX_STRING_OR_DELIMITER:
        if (thd->variables.sql_mode & MODE_ANSI_QUOTES) {
          state = MY_LEX_USER_VARIABLE_DELIMITER;
          break;
        }
        // fallthrough
        /* " used for strings */
        [[fallthrough]];
      case MY_LEX_STRING:  // Incomplete text string
        if (!(yylval->lex_str.str = get_text(lip, 1, 1))) {
          state = MY_LEX_CHAR;  // Read char by char
          break;
        }
        yylval->lex_str.length = lip->yytoklen;

        lip->body_utf8_append(lip->m_cpp_text_start);

        lip->body_utf8_append_literal(
            thd, &yylval->lex_str,
            lip->m_underscore_cs ? lip->m_underscore_cs : cs,
            lip->m_cpp_text_end);

        lip->m_underscore_cs = nullptr;

        return (TEXT_STRING);

      case MY_LEX_COMMENT:  //  Comment
        thd->m_parser_state->add_comment();
        while ((c = lip->yyGet()) != '\n' && c)
          ;
        lip->yyUnget();        // Safety against eof
        state = MY_LEX_START;  // Try again
        break;
      case MY_LEX_LONG_COMMENT: /* Long C comment? */
        if (lip->yyPeek() != '*') {
          state = MY_LEX_CHAR;  // Probable division
          break;
        }
        thd->m_parser_state->add_comment();
        /* Reject '/' '*', since we might need to turn off the echo */
        lip->yyUnget();

        lip->save_in_comment_state();

        if (lip->yyPeekn(2) == '!') {
          lip->in_comment = DISCARD_COMMENT;
          /* Accept '/' '*' '!', but do not keep this marker. */
          lip->set_echo(false);
          lip->yySkip();
          lip->yySkip();
          lip->yySkip();

          /*
            The special comment format is very strict:
            '/' '*' '!', followed by exactly
            1 digit (major), 2 digits (minor), then 2 digits (dot).
            32302 -> 3.23.02
            50032 -> 5.0.32
            50114 -> 5.1.14
          */
          char version_str[6];
          if (my_isdigit(cs, (version_str[0] = lip->yyPeekn(0))) &&
              my_isdigit(cs, (version_str[1] = lip->yyPeekn(1))) &&
              my_isdigit(cs, (version_str[2] = lip->yyPeekn(2))) &&
              my_isdigit(cs, (version_str[3] = lip->yyPeekn(3))) &&
              my_isdigit(cs, (version_str[4] = lip->yyPeekn(4)))) {
            version_str[5] = 0;
            ulong version;
            version = strtol(version_str, nullptr, 10);

            if (version <= MYSQL_VERSION_ID) {
              /* Accept 'M' 'm' 'm' 'd' 'd' */
              lip->yySkipn(5);
              /* Expand the content of the special comment as real code */
              lip->set_echo(true);
              state = MY_LEX_START;
              break; /* Do not treat contents as a comment.  */
            } else {
              /*
                Patch and skip the conditional comment to avoid it
                being propagated infinitely (eg. to a slave).
              */
              char *pcom = lip->yyUnput(' ');
              comment_closed = !consume_comment(lip, 1);
              if (!comment_closed) {
                *pcom = '!';
              }
              /* version allowed to have one level of comment inside. */
            }
          } else {
            /* Not a version comment. */
            state = MY_LEX_START;
            lip->set_echo(true);
            break;
          }
        } else {
          if (lip->in_comment != NO_COMMENT) {
            push_warning(
                lip->m_thd, Sql_condition::SL_WARNING,
                ER_WARN_DEPRECATED_SYNTAX_NO_REPLACEMENT,
                ER_THD(lip->m_thd, ER_WARN_DEPRECATED_NESTED_COMMENT_SYNTAX));
          }
          lip->in_comment = PRESERVE_COMMENT;
          lip->yySkip();  // Accept /
          lip->yySkip();  // Accept *
          comment_closed = !consume_comment(lip, 0);
          /* regular comments can have zero comments inside. */
        }
        /*
          Discard:
          - regular '/' '*' comments,
          - special comments '/' '*' '!' for a future version,
          by scanning until we find a closing '*' '/' marker.

          Nesting regular comments isn't allowed.  The first
          '*' '/' returns the parser to the previous state.

          /#!VERSI oned containing /# regular #/ is allowed #/

                  Inside one versioned comment, another versioned comment
                  is treated as a regular discardable comment.  It gets
                  no special parsing.
        */

        /* Unbalanced comments with a missing '*' '/' are a syntax error */
        if (!comment_closed) return (ABORT_SYM);
        state = MY_LEX_START;  // Try again
        lip->restore_in_comment_state();
        break;
      case MY_LEX_END_LONG_COMMENT:
        if ((lip->in_comment != NO_COMMENT) && lip->yyPeek() == '/') {
          /* Reject '*' '/' */
          lip->yyUnget();
          /* Accept '*' '/', with the proper echo */
          lip->set_echo(lip->in_comment == PRESERVE_COMMENT);
          lip->yySkipn(2);
          /* And start recording the tokens again */
          lip->set_echo(true);

          /*
            C-style comments are replaced with a single space (as it
            is in C and C++).  If there is already a whitespace
            character at this point in the stream, the space is
            not inserted.

            See also ISO/IEC 9899:1999 §5.1.1.2
            ("Programming languages — C")
          */
          if (!my_isspace(cs, lip->yyPeek()) &&
              lip->get_cpp_ptr() != lip->get_cpp_buf() &&
              !my_isspace(cs, *(lip->get_cpp_ptr() - 1)))
            lip->cpp_inject(' ');

          lip->in_comment = NO_COMMENT;
          state = MY_LEX_START;
        } else
          state = MY_LEX_CHAR;  // Return '*'
        break;
      case MY_LEX_SET_VAR:  // Check if ':='
        if (lip->yyPeek() != '=') {
          state = MY_LEX_CHAR;  // Return ':'
          break;
        }
        lip->yySkip();
        return (SET_VAR);
      case MY_LEX_SEMICOLON:  // optional line terminator
        state = MY_LEX_CHAR;  // Return ';'
        break;
      case MY_LEX_EOL:
        if (lip->eof()) {
          lip->yyUnget();  // Reject the last '\0'
          lip->set_echo(false);
          lip->yySkip();
          lip->set_echo(true);
          /* Unbalanced comments with a missing '*' '/' are a syntax error */
          if (lip->in_comment != NO_COMMENT) return (ABORT_SYM);
          lip->next_state = MY_LEX_END;  // Mark for next loop
          return (END_OF_INPUT);
        }
        state = MY_LEX_CHAR;
        break;
      case MY_LEX_END:
        lip->next_state = MY_LEX_END;
        return (0);  // We found end of input last time

        /* Actually real shouldn't start with . but allow them anyhow */
      case MY_LEX_REAL_OR_POINT:
        /*oracle procedure 1..2*/
        if (lip->yyPeekn(-2) == '.') {
          return DOT_DOT_SYM;
        }
        if (my_isdigit(cs, lip->yyPeek()))
          state = MY_LEX_REAL;  // Real
        else if (lip->yyPeek() == '.') {
          /*oracle procedure 1 .. 2*/
          lip->yySkip();
          return DOT_DOT_SYM;
        } else {
          state = MY_LEX_IDENT_SEP;  // return '.'
          lip->yyUnget();            // Put back '.'
        }
        break;
      case MY_LEX_USER_END:  // end '@' of user@hostname
        switch (state_map[lip->yyPeek()]) {
          case MY_LEX_STRING:
          case MY_LEX_USER_VARIABLE_DELIMITER:
          case MY_LEX_STRING_OR_DELIMITER:
            break;
          case MY_LEX_USER_END:
            lip->next_state = MY_LEX_SYSTEM_VAR;
            break;
          default:
            lip->next_state = MY_LEX_HOSTNAME;
            break;
        }
        yylval->lex_str.str = const_cast<char *>(lip->get_ptr());
        yylval->lex_str.length = 1;
        return ((int)'@');
      case MY_LEX_HOSTNAME:  // end '@' of user@hostname
        if (lip->dblink) {
          lip->dblink = false;
          state = MY_LEX_IDENT_OR_KEYWORD;
          break;
        }
        for (c = lip->yyGet();
             my_isalnum(cs, c) || c == '.' || c == '_' || c == '$';
             c = lip->yyGet())
          ;
        yylval->lex_str = get_token(lip, 0, lip->yyLength());
        return (LEX_HOSTNAME);
      case MY_LEX_SYSTEM_VAR:
        yylval->lex_str.str = const_cast<char *>(lip->get_ptr());
        yylval->lex_str.length = 1;
        lip->yySkip();  // Skip '@'
        lip->next_state =
            (state_map[lip->yyPeek()] == MY_LEX_USER_VARIABLE_DELIMITER
                 ? MY_LEX_START
                 : MY_LEX_IDENT_OR_KEYWORD);
        return ((int)'@');
      case MY_LEX_IDENT_OR_KEYWORD:
        /*
          We come here when we have found two '@' in a row.
          We should now be able to handle:
          [(global | local | session) .]variable_name
        */

        for (result_state = 0; ident_map[c = lip->yyGet()]; result_state |= c)
          ;
        /* If there were non-ASCII characters, mark that we must convert */
        result_state = result_state & 0x80 ? IDENT_QUOTED : IDENT;

        if (c == '.') lip->next_state = MY_LEX_IDENT_SEP;
        length = lip->yyLength();
        if (length == 0) return (ABORT_SYM);  // Names must be nonempty.
        if ((tokval = find_keyword(lip, length, false))) {
          lip->yyUnget();   // Put back 'c'
          return (tokval);  // Was keyword
        }
        yylval->lex_str = get_token(lip, 0, length);

        lip->body_utf8_append(lip->m_cpp_text_start);

        lip->body_utf8_append_literal(thd, &yylval->lex_str, cs,
                                      lip->m_cpp_text_end);

        return (result_state);
    }
  }
}

void trim_whitespace(const CHARSET_INFO *cs, LEX_STRING *str) {
  /*
    TODO:
    This code assumes that there are no multi-bytes characters
    that can be considered white-space.
  */

  while ((str->length > 0) && (my_isspace(cs, str->str[0]))) {
    str->length--;
    str->str++;
  }

  /*
    FIXME:
    Also, parsing backward is not safe with multi bytes characters
  */
  while ((str->length > 0) && (my_isspace(cs, str->str[str->length - 1]))) {
    str->length--;
    /* set trailing spaces to 0 as there're places that don't respect length */
    str->str[str->length] = 0;
  }
}

/**
   Prints into 'str' a comma-separated list of column names, enclosed in
   parenthesis.
   @param  thd  Thread handler
   @param  str  Where to print
   @param  column_names List to print, or NULL
*/

void print_derived_column_names(const THD *thd, String *str,
                                const Create_col_name_list *column_names) {
  if (!column_names) return;
  str->append(" (");
  for (auto s : *column_names) {
    append_identifier(thd, str, s.str, s.length);
    str->append(',');
  }
  str->length(str->length() - 1);
  str->append(')');
}

/**
  Construct and initialize Query_expression object.
*/
Query_expression::Query_expression(enum_parsing_context parsing_context)
    : next(nullptr),
      prev(nullptr),
      master(nullptr),
      slave(nullptr),
      explain_marker(CTX_NONE),
      prepared(false),
      optimized(false),
      executed(false),
      m_query_result(nullptr),
      uncacheable(0),
      cleaned(UC_DIRTY),
      types(current_thd->mem_root),
      select_limit_cnt(HA_POS_ERROR),
      offset_limit_cnt(0),
      select_limit_ties(false),
      select_limit_percent(false),
      select_limit_percent_value(0.0),
      item(nullptr),
      m_with_clause(nullptr),
      derived_table(nullptr),
      first_recursive(nullptr),
      m_lateral_deps(0) {
  switch (parsing_context) {
    case CTX_ORDER_BY:
      explain_marker = CTX_ORDER_BY_SQ;  // A subquery in ORDER BY
      break;
    case CTX_GROUP_BY:
      explain_marker = CTX_GROUP_BY_SQ;  // A subquery in GROUP BY
      break;
    case CTX_ON:
      explain_marker = CTX_WHERE;
      break;
    case CTX_HAVING:  // A subquery elsewhere
    case CTX_SELECT_LIST:
    case CTX_UPDATE_VALUE:
    case CTX_INSERT_VALUES:
    case CTX_INSERT_UPDATE:
    case CTX_WHERE:
    case CTX_DERIVED:
    case CTX_NONE:  // A subquery in a non-select
    case CTX_START_WITH:
    case CTX_CONNECT_BY:
      explain_marker = parsing_context;
      break;
    default:
      /* Subquery can't happen outside of those ^. */
      assert(false); /* purecov: inspected */
      break;
  }
}

/**
  Construct and initialize Query_block object.
*/

Query_block::Query_block(MEM_ROOT *mem_root, Item *where, Item *having)
    : fields(mem_root),
      ftfunc_list(&ftfunc_list_alloc),
      sj_nests(mem_root),
      first_context(&context),
      top_join_list(mem_root),
      m_update_of_cond_dq(mem_root),
      join_list(&top_join_list),
      m_table_nest(mem_root),
      m_current_table_nest(&m_table_nest),
      m_where_cond(where),
      m_having_cond(having) {}

/**
  Set the name resolution context for the specified query block.

  @param outer_context Outer name resolution context.
                       NULL if none or it will be set later.
*/

bool Query_block::set_context(Name_resolution_context *outer_context) {
  context.init();
  context.query_block = this;
  context.outer_context = outer_context;
  /*
    Add the name resolution context of this query block to the
    stack of contexts for the whole query.
  */
  return parent_lex->push_context(&context);
}

/**
  Add tables from an array to a list of used tables.

  @param thd            Current session.
  @param tables         Tables to add.
  @param table_options  A set of the following bits:
                         - TL_OPTION_UPDATING : Table will be updated,
                         - TL_OPTION_FORCE_INDEX : Force usage of index,
                         - TL_OPTION_ALIAS : an alias in multi table DELETE.
  @param lock_type      How table should be locked.
  @param mdl_type       Type of metadata lock to acquire on the table.

  @returns true if error (reported), otherwise false.
*/

bool Query_block::add_tables(THD *thd,
                             const Mem_root_array<Table_ident *> *tables,
                             ulong table_options, thr_lock_type lock_type,
                             enum_mdl_type mdl_type) {
  if (tables == nullptr) return false;

  for (auto *table : *tables) {
    if (!add_table_to_list(thd, table, nullptr, table_options, lock_type,
                           mdl_type))
      return true;
  }
  return false;
}

/**
  Exclude this query expression and its immediately contained query terms
  and query blocks from AST.

  @note
    Query expressions that belong to the query_block objects of the current
    query expression will be brought up one level and will replace
    the current query expression in the list inside the outer query block.
*/
void Query_expression::exclude_level() {
  /*
    This change to the AST is done only during statement resolution
    so doesn't need LOCK_query_plan
  */
  Query_expression *qe_chain = nullptr;
  Query_expression **last_qe_ref = &qe_chain;
  for (Query_block *sl = first_query_block(); sl != nullptr;
       sl = sl->next_query_block()) {
    assert(sl->join == nullptr);

    // Unlink this query block from global list
    if (sl->link_prev && (*sl->link_prev = sl->link_next))
      sl->link_next->link_prev = sl->link_prev;

    // Bring up underlying query expressions
    Query_expression **last = nullptr;
    for (Query_expression *u = sl->first_inner_query_expression(); u;
         u = u->next_query_expression()) {
      /*
        We are excluding a query block from the AST. Since this level is
        removed, we must also exclude the Name_resolution_context belonging to
        this level. Do this by looping through inner subqueries and changing
        their contexts' outer context pointers to point to the outer query
        block's context.
      */
      for (auto qt : u->query_terms<>()) {
        if (qt->query_block()->context.outer_context == &sl->context) {
          qt->query_block()->context.outer_context =
              &sl->outer_query_block()->context;
        }
      }
      u->master = master;
      last = &(u->next);
    }
    if (last != nullptr) {
      (*last_qe_ref) = sl->first_inner_query_expression();
      last_qe_ref = last;
      // Unlink the query expressions that have been moved to the outer level:
      sl->slave = nullptr;
    }
  }
  if (qe_chain != nullptr) {
    // Include underlying query expressions in place of the current one.
    (*prev) = qe_chain;
    (*last_qe_ref) = next;
    if (next != nullptr) next->prev = last_qe_ref;
    qe_chain->prev = prev;
  } else {
    // Exclude current query expression from list inside query block.
    (*prev) = next;
    if (next != nullptr) next->prev = prev;
  }
  // Cleanup and destroy this query expression, including any temporary tables:
  cleanup(true);
  destroy();
}

/**
  Exclude current query expression with all underlying query terms,
  query blocks and query expressions from AST.
*/
void Query_expression::exclude_tree() {
  for (Query_block *sl = first_query_block(); sl != nullptr;
       sl = sl->next_query_block()) {
    /*
      Exclusion is only done during preparation, however some table-less
      subqueries may have been evaluated during preparation.
    */
    assert(sl->join == nullptr || is_executed());
    if (sl->join != nullptr) {
      sl->join->destroy();
      sl->join = nullptr;
    }

    // Unlink from global query block list
    if (sl->link_prev && (*sl->link_prev = sl->link_next))
      sl->link_next->link_prev = sl->link_prev;

    // Exclude subtrees of all the inner query expressions of this query block
    for (Query_expression *u = sl->first_inner_query_expression();
         u != nullptr;) {
      Query_expression *next = u->next_query_expression();
      u->exclude_tree();
      u = next;
    }
    // All underlying query expressions are now deleted:
    assert(sl->slave == nullptr);
  }
  // Exclude current query expression from list inside query block.
  (*prev) = next;
  if (next != nullptr) next->prev = prev;

  // Cleanup and destroy the internal objects for this query expression.
  cleanup(true);
  destroy();
}

/**
  Invalidate by nulling out pointers to other Query expressions and
  Query blocks.
*/
void Query_expression::invalidate() {
  next = nullptr;
  prev = nullptr;
  master = nullptr;
  slave = nullptr;
}

/**
  Make active options from base options, supplied options, any statement
  options and the environment.

  @param added_options   Options that are added to the active options
  @param removed_options Options that are removed from the active options
*/

void Query_block::make_active_options(ulonglong added_options,
                                      ulonglong removed_options) {
  m_active_options =
      (m_base_options | added_options | parent_lex->statement_options() |
       parent_lex->thd->variables.option_bits) &
      ~removed_options;
}

/**
  Mark all query blocks from this to 'last' as dependent

  @param last Pointer to last Query_block struct, before which all
              Query_block are marked as as dependent.
  @param aggregate true if the dependency is due to a set function, such as
                   COUNT(*), which is aggregated within the query block 'last'.
                   Such functions must have a dependency on all tables of
                   the aggregating query block.

  @note
    last should be reachable from this Query_block

  @todo Update OUTER_REF_TABLE_BIT for intermediate subquery items, by
        replacing the below "if (aggregate)" block with:
        if (last == s->outer_query_block())
        {
          if (aggregate)
            munit->item->accumulate_used_tables(last->all_tables_map());
        }
        else
        {
          munit->item->accumulate_used_tables(OUTER_REF_TABLE_BIT);
        }
        and remove settings from Item_field::fix_outer_field(),
        Item_ref::fix_fields().
*/

void Query_block::mark_as_dependent(Query_block *last, bool aggregate) {
  // The top level query block cannot be dependent, so do not go above this:
  assert(last != nullptr);

  /*
    Mark all selects from resolved to 1 before select where was
    found table as depended (of select where was found table)
  */
  for (Query_block *s = this; s && s != last; s = s->outer_query_block()) {
    Query_expression *munit = s->master_query_expression();
    if (!(s->uncacheable & UNCACHEABLE_DEPENDENT)) {
      // Select is dependent of outer select
      s->uncacheable =
          (s->uncacheable & ~UNCACHEABLE_UNITED) | UNCACHEABLE_DEPENDENT;
      munit->uncacheable =
          (munit->uncacheable & ~UNCACHEABLE_UNITED) | UNCACHEABLE_DEPENDENT;
      for (Query_block *sl = munit->first_query_block(); sl;
           sl = sl->next_query_block()) {
        if (sl != s &&
            !(sl->uncacheable & (UNCACHEABLE_DEPENDENT | UNCACHEABLE_UNITED))) {
          // Prevent early freeing in JOIN::join_free()
          sl->uncacheable |= UNCACHEABLE_UNITED;
        }
      }
    }
    if (aggregate) {
      munit->accumulate_used_tables(last == s->outer_query_block()
                                        ? last->all_tables_map()
                                        : OUTER_REF_TABLE_BIT);
    }
  }
}

/*
  prohibit using LIMIT clause
*/
bool Query_block::test_limit() {
  if (select_limit != nullptr) {
    my_error(ER_NOT_SUPPORTED_YET, MYF(0), "LIMIT & IN/ALL/ANY/SOME subquery");
    return (true);
  }
  return (false);
}

enum_parsing_context Query_expression::get_explain_marker(
    const THD *thd) const {
  thd->query_plan.assert_plan_is_locked_if_other();
  return explain_marker;
}

void Query_expression::set_explain_marker(THD *thd, enum_parsing_context m) {
  thd->lock_query_plan();
  explain_marker = m;
  thd->unlock_query_plan();
}

void Query_expression::set_explain_marker_from(THD *thd,
                                               const Query_expression *u) {
  thd->lock_query_plan();
  explain_marker = u->explain_marker;
  thd->unlock_query_plan();
}

ulonglong limit_offset_value(THD *thd, bool select_limit_fetch, Item *item,
                             bool *isNull) {
  Strict_error_handler strict_handler(
      Strict_error_handler::ENABLE_SET_SELECT_STRICT_ERROR_HANDLER);
  auto strict_handler_guard =
      create_scope_guard([thd, saved_sql_mode = thd->variables.sql_mode]() {
        thd->pop_internal_handler();
        thd->variables.sql_mode = saved_sql_mode;
      });
  thd->push_internal_handler(&strict_handler);
  thd->variables.sql_mode = thd->variables.sql_mode |
                            (MODE_STRICT_TRANS_TABLES | MODE_STRICT_ALL_TABLES |
                             MODE_EMPTYSTRING_EQUAL_NULL);

  if (!select_limit_fetch) {
    return item->val_uint();
  }
  // 3.3  to nd 3
  my_decimal dec_buf;
  // check is include like '1a'
  auto res = item->val_decimal(&dec_buf);
  if (item->null_value || res == nullptr) {
    if (isNull) {
      *isNull = true;
    }
    return 0;
  }
  if (res->sign()) {
    return 0;
  }
  if (my_decimal_round(E_DEC_FATAL_ERROR, res, 0, true, &dec_buf)) {
    return 0;
  }
  ulonglong result;
  auto ret = decimal2ulonglong(&dec_buf, &result);
  if (dec_buf.check_result(E_DEC_FATAL_ERROR, ret)) {
    return 0;
  }
  return result;
}

ha_rows Query_block::get_offset(const THD *thd, bool *isNull) const {
  if (offset_limit != nullptr) {
    return ha_rows{limit_offset_value(
        const_cast<THD *>(thd), select_limit_fetch, offset_limit, isNull)};
  } else
    return ha_rows{0};
}

ha_rows Query_block::get_limit(const THD *thd) {
  /*
    If m_use_select_limit is set in the query block, return the value
    of the variable select_limit, unless an explicit limit is set.
    This is used to implement SQL_SELECT_LIMIT for SELECT statements.
  */
  if (select_limit != nullptr) {
    auto i = limit_offset_value(const_cast<THD *>(thd), select_limit_fetch,
                                select_limit, nullptr);
    if (i != HA_POS_ERROR && select_limit_percent) {
      select_limit_percent_value = select_limit->val_real();

      if (select_limit_percent_value >= 100.0) {
        select_limit_percent_value = 0;
        return ha_rows{HA_POS_ERROR};
      }

      if (select_limit_percent_value < 0) {
        select_limit_percent_value = 0;
      }
      // 0.11 oracle value truncate to 0 -> at least 1
      if (select_limit_percent_value > 0 && i == 0) {
        i = 1;
      }
    }
    return i;
  } else if (m_use_select_limit)
    return ha_rows{thd->variables.select_limit};
  else
    return ha_rows{HA_POS_ERROR};
}

bool Query_block::add_item_to_list(Item *item) {
  DBUG_TRACE;
  DBUG_PRINT("info", ("Item: %p", item));
  assert_consistent_hidden_flags(fields, item, /*hidden=*/false);
  fields.push_back(item);
  item->hidden = false;
  return false;
}

bool Query_block::add_ftfunc_to_list(Item_func_match *func) {
  return !func || ftfunc_list->push_back(func);  // end of memory?
}

/**
  Invalidate by nulling out pointers to other Query_expressions and
  Query_blockes.
*/
void Query_block::invalidate() {
  next = nullptr;
  master = nullptr;
  slave = nullptr;
  link_next = nullptr;
  link_prev = nullptr;
}

bool Query_block::setup_base_ref_items(THD *thd) {
  uint order_group_num = order_list.elements + group_list.elements;

  // find_order_in_list() may need some extra space, so multiply by two.
  order_group_num *= 2;

  // create_distinct_group() may need some extra space
  if (is_distinct()) {
    uint bitcount = 0;
    for (Item *item : visible_fields()) {
      /*
        Same test as in create_distinct_group, when it pushes new items to the
        end of base_ref_items. An extra test for 'fixed' which, at this
        stage, will be true only for columns inserted for a '*' wildcard.
      */
      if (item->fixed && item->type() == Item::FIELD_ITEM &&
          item->data_type() == MYSQL_TYPE_BIT)
        ++bitcount;
    }
    order_group_num += bitcount;
  }
  if (has_connect_by) {
    auto has_level = false;
    for (Item *item : visible_fields()) {
      if (item->type() == Item::CONNECT_BY_FUNC_ITEM) {
        if (dynamic_cast<Item_connect_by_func *>(item)->ConnectBy_func() ==
            Item_connect_by_func::LEVEL_FUNC) {
          has_level = true;
        }
      }
    }
    if (!has_level) n_connect_by_items++;
  }

  /*
    We have to create array in prepared statement memory if it is
    prepared statement
  */
  Query_arena *arena = thd->stmt_arena;
  uint n_elems = n_sum_items + n_child_sum_items + fields.size() +
                 select_n_having_items + select_n_where_fields +
                 order_group_num + n_scalar_subqueries + n_connect_by_items;

  if (enable_data_masking && outer_query_block() == nullptr) {
    //  field will replace to mask data
    n_elems += fields.size() * 2;
  }
  /*
    If it is possible that we transform IN(subquery) to a join to a derived
    table, we will be adding DISTINCT (this possibly has the problem of BIT
    columns as in the logic above), and we will also be adding one expression to
    the SELECT list per decorrelated equality in WHERE. So we have to allocate
    more space.

    The number of decorrelatable equalities is bounded by
    select_n_where_fields. Indeed an equality isn't counted in
    select_n_where_fields if it's:
    expr-without_Item_field = expr-without_Item_field;
    but we decorrelate an equality if one member has OUTER_REF_TABLE_BIT, so
    it has an Item_field inside.

    Note that cond_count cannot be used, as setup_cond() hasn't run yet. So we
    use select_n_where_fields instead.
  */
  if (master_query_expression()->item &&
      (thd->optimizer_switch_flag(OPTIMIZER_SWITCH_SUBQUERY_TO_DERIVED) ||
       (thd->lex->m_sql_cmd != nullptr &&
        thd->secondary_engine_optimization() ==
            Secondary_engine_optimization::SECONDARY))) {
    Item_subselect *subq_predicate = master_query_expression()->item;
    if (subq_predicate->substype() == Item_subselect::EXISTS_SUBS ||
        subq_predicate->substype() == Item_subselect::IN_SUBS) {
      // might be transformed to derived table, so:
      n_elems +=
          // possible additions to SELECT list from decorrelation of WHERE
          select_n_where_fields +
          // add size of new SELECT list, for DISTINCT and BIT type
          (select_n_where_fields + fields.size());
    }
  }

  DBUG_PRINT(
      "info",
      ("setup_ref_array this %p %4u : %4u %4u %4zu %4u %4u %4u %4u", this,
       n_elems,  // :
       n_sum_items, n_child_sum_items, fields.size(), select_n_having_items,
       select_n_where_fields, order_group_num, n_scalar_subqueries));
  if (!base_ref_items.is_null()) {
    /*
      This should not happen, as it's the sign of preparing an already-prepared
      Query_block. It does happen (in test main.sp-error, section for bug13037):
      a table-less substatement fails due to wrong identifier, and
      LEX::mark_broken() doesn't mark it as broken as it uses no tables; so it
      will be reused by the next CALL. WL#6570.
     */
    if (base_ref_items.size() >= n_elems) return false;
  }
  Item **array = static_cast<Item **>(arena->alloc(sizeof(Item *) * n_elems));
  if (array == nullptr) return true;

  base_ref_items = Ref_item_array(array, n_elems);

  return false;
}

void print_set_operation(const THD *thd, Query_term *op, String *str, int level,
                         enum_query_type query_type) {
  if (op->term_type() == QT_QUERY_BLOCK) {
    Query_block *const block = op->query_block();
    const bool needs_parens =
        block->has_limit() || block->order_list.elements > 0;
    if (needs_parens) str->append('(');
    op->query_block()->print(thd, str, query_type);
    if (needs_parens) str->append(')');
  } else {
    Query_term_set_op *qts = down_cast<Query_term_set_op *>(op);
    const bool needs_parens = level > 0;
    if (needs_parens) str->append('(');
    for (uint i = 0; i < qts->m_children.size(); ++i) {
      print_set_operation(thd, qts->m_children[i], str, level + 1, query_type);
      if (i < qts->m_children.size() - 1) {
        switch (op->term_type()) {
          case QT_UNION:
            str->append(STRING_WITH_LEN(" union "));
            break;
          case QT_INTERSECT:
            str->append(STRING_WITH_LEN(" intersect "));
            break;
          case QT_EXCEPT:
            str->append(STRING_WITH_LEN(" except "));
            break;
          default:
            assert(false);
        }
        if (static_cast<signed int>(i) + 1 > qts->m_last_distinct) {
          str->append(STRING_WITH_LEN("all "));
        }
      }
    }
    if (op->query_block()->order_list.elements > 0) {
      str->append(STRING_WITH_LEN(" order by "));
      op->query_block()->print_order(
          thd, str, op->query_block()->order_list.first, query_type);
    }
    op->query_block()->print_limit(thd, str, query_type);
    if (needs_parens) str->append(')');
  }
}

void Query_expression::print(const THD *thd, String *str,
                             enum_query_type query_type) {
  if (m_with_clause) m_with_clause->print(thd, str, query_type);
  if (is_simple()) {
    Query_block *sl = query_term()->query_block();
    assert(sl->next_query_block() == nullptr);
    sl->print(thd, str, query_type);
  } else {
    print_set_operation(thd, query_term(), str, 0, query_type);
  }
}

void Query_block::print_limit(const THD *thd, String *str,
                              enum_query_type query_type) const {
  Query_expression *unit = master_query_expression();
  Item_subselect *item = unit->item;

  if (item && unit->global_parameters() == this) {
    Item_subselect::subs_type subs_type = item->substype();
    if (subs_type == Item_subselect::EXISTS_SUBS ||
        subs_type == Item_subselect::IN_SUBS ||
        subs_type == Item_subselect::ALL_SUBS)
      return;
  }
  if (has_limit() && !m_internal_limit) {
    if (select_limit_fetch) {
      if (offset_limit) {
        str->append(STRING_WITH_LEN(" offset "));
        offset_limit->print(thd, str, query_type);
        str->append(STRING_WITH_LEN(" rows"));
      }

      if (select_limit_percent) {
        if (select_limit_percent_value > 0) {
          str->append(STRING_WITH_LEN(" fetch next "));
          char buffer[MAX_DOUBLE_STR_LENGTH + 1] = {0};
          String num(buffer, sizeof(buffer), &my_charset_bin);
          num.set_real(select_limit_percent_value, DECIMAL_NOT_SPECIFIED,
                       &my_charset_bin);
          str->append(num);
          str->append(STRING_WITH_LEN(" percent"));
          str->append(STRING_WITH_LEN(" rows "));
          if (select_limit_ties) {
            str->append(STRING_WITH_LEN("with ties"));
          } else {
            str->append(STRING_WITH_LEN("only"));
          }
        }

      } else {
        str->append(STRING_WITH_LEN(" fetch next "));
        select_limit->print(thd, str, query_type);
        str->append(STRING_WITH_LEN(" rows "));
        if (select_limit_ties) {
          str->append(STRING_WITH_LEN("with ties"));
        } else {
          str->append(STRING_WITH_LEN("only"));
        }
      }
    } else {
      str->append(STRING_WITH_LEN(" limit "));
      if (offset_limit) {
        offset_limit->print(thd, str, query_type);
        str->append(',');
      }
      select_limit->print(thd, str, query_type);
    }
  }
}

/**
  @brief Print an index hint

  @details Prints out the USE|FORCE|IGNORE index hint.

  @param      thd         the current thread
  @param[out] str         appends the index hint here
*/

void Index_hint::print(const THD *thd, String *str) {
  switch (type) {
    case INDEX_HINT_IGNORE:
      str->append(STRING_WITH_LEN("IGNORE INDEX"));
      break;
    case INDEX_HINT_USE:
      str->append(STRING_WITH_LEN("USE INDEX"));
      break;
    case INDEX_HINT_FORCE:
      str->append(STRING_WITH_LEN("FORCE INDEX"));
      break;
  }
  switch (clause) {
    case INDEX_HINT_MASK_ALL:
      break;
    case INDEX_HINT_MASK_JOIN:
      str->append(STRING_WITH_LEN(" FOR JOIN"));
      break;
    case INDEX_HINT_MASK_ORDER:
      str->append(STRING_WITH_LEN(" FOR ORDER BY"));
      break;
    case INDEX_HINT_MASK_GROUP:
      str->append(STRING_WITH_LEN(" FOR GROUP BY"));
      break;
  }

  str->append(STRING_WITH_LEN(" ("));
  if (key_name.length) {
    if (thd && !my_strnncoll(system_charset_info, (const uchar *)key_name.str,
                             key_name.length, (const uchar *)primary_key_name,
                             strlen(primary_key_name)))
      str->append(primary_key_name);
    else
      append_identifier(thd, str, key_name.str, key_name.length);
  }
  str->append(')');
}

typedef Prealloced_array<Table_ref *, 8> Table_array;

static void print_table_array(const THD *thd, String *str,
                              const Table_array &tables,
                              enum_query_type query_type) {
  assert(!tables.empty());

  Table_array::const_iterator it = tables.begin();
  bool first = true;
  for (; it != tables.end(); ++it) {
    Table_ref *curr = *it;

    const bool is_optimized =
        curr->query_block->join && curr->query_block->join->is_optimized();

    // the JOIN ON condition
    Item *const cond =
        is_optimized ? curr->join_cond_optim() : curr->join_cond();

    // Print the join operator which relates this table to the previous one
    const char *op = nullptr;
    if (curr->is_aj_nest())
      op = " anti join ";
    else if (curr->is_sj_nest())
      op = " semi join ";
    else if (curr->foj_inner)
      op = " full join ";
    else if (curr->outer_join) {
      /* MySQL converts right to left joins */
      op = " left join ";
    } else if (!first || cond) {
      /*
        If it's the first table, and it has an ON condition (can happen due to
        query transformations, e.g. merging a single-table view moves view's
        WHERE to table's ON): ON also needs JOIN.
      */
      op = curr->straight ? " straight_join " : " join ";
    }

    if (op) {
      if (first) {
        // Add a dummy table before the operator, to have sensible SQL:
        str->append(STRING_WITH_LEN("<constant table>"));
      }
      str->append(op);
    }
    curr->print(thd, str, query_type);  // Print table
    /*
      Print table hint info after the table name. Used only
      for explaining views. There is no functionality, just
      additional info for user.
    */
    if (thd->lex->is_explain() && curr->opt_hints_table &&
        curr->belong_to_view) {
      str->append(STRING_WITH_LEN(" /*+ "));
      curr->opt_hints_table->print(thd, str, query_type);
      str->append(STRING_WITH_LEN("*/ "));
    }
    // Print join condition
    if (cond) {
      str->append(STRING_WITH_LEN(" on("));
      cond->print(thd, str, query_type);
      str->append(')');
    }
    first = false;
  }
}

/**
  Print joins from the FROM clause.

  @param thd     thread handler
  @param str     string where table should be printed
  @param tables  list of tables in join
  @param query_type    type of the query is being generated
*/

static void print_join(const THD *thd, String *str,
                       mem_root_deque<Table_ref *> *tables,
                       enum_query_type query_type) {
  /* List is reversed => we should reverse it before using */

  /*
    If the QT_NO_DATA_EXPANSION flag is specified, we print the
    original table list, including constant tables that have been
    optimized away, as the constant tables may be referenced in the
    expression printed by Item_field::print() when this flag is given.
    Otherwise, only non-const tables are printed.

    Example:

    Original SQL:
    select * from (select 1) t

    Printed without QT_NO_DATA_EXPANSION:
    select '1' AS `1` from dual

    Printed with QT_NO_DATA_EXPANSION:
    select `t`.`1` from (select 1 AS `1`) `t`
  */
  const bool print_const_tables = (query_type & QT_NO_DATA_EXPANSION);
  Table_array tables_to_print(PSI_NOT_INSTRUMENTED);

  for (Table_ref *t : *tables) {
    // The single table added to fake_query_block has no name;
    // “from dual” looks slightly better than “from ``”, so drop it.
    // (The fake_query_block query is invalid either way.)
    if (t->alias[0] == '\0') continue;

    if (print_const_tables || !t->optimized_away)
      if (tables_to_print.push_back(t)) return; /* purecov: inspected */
  }

  if (tables_to_print.empty()) {
    str->append(STRING_WITH_LEN("dual"));
    return;  // all tables were optimized away
  }

  std::reverse(tables_to_print.begin(), tables_to_print.end());
  print_table_array(thd, str, tables_to_print, query_type);
}

/**
  @returns whether a database is equal to the connection's default database
*/
bool db_is_default_db(const char *db, size_t db_len, const THD *thd) {
  return thd != nullptr && thd->db().str != nullptr &&
         thd->db().length == db_len && !memcmp(db, thd->db().str, db_len);
}

/*.*
  Print table as it should be in join list.

  @param str   string where table should be printed
*/

void Table_ref::print(const THD *thd, String *str,
                      enum_query_type query_type) const {
  if (nested_join) {
    str->append('(');
    print_join(thd, str, &nested_join->m_tables, query_type);
    str->append(')');
  } else {
    const char *cmp_name;  // Name to compare with alias
    if (is_table_function()) {
      table_function->print(thd, str, query_type);
      cmp_name = table_name;
    } else if (is_derived() && !is_merged() && !common_table_expr()) {
      // A derived table that is materialized or without specified algorithm
      if (!(query_type & QT_DERIVED_TABLE_ONLY_ALIAS)) {
        if (derived_query_expression()->m_lateral_deps)
          str->append(STRING_WITH_LEN("lateral "));
        str->append('(');
        derived->print(thd, str, query_type);
        str->append(')');
      }
      cmp_name = "";  // Force printing of alias
    } else {
      // A normal table, or a view, or a CTE

      if (db_length && !(query_type & QT_NO_DB) &&
          !((query_type & QT_NO_DEFAULT_DB) &&
            db_is_default_db(db, db_length, thd))) {
        append_identifier(thd, str, db, db_length);
        str->append('.');
      }
      append_identifier(thd, str, table_name, table_name_length);
      cmp_name = table_name;
      if (partition_names && partition_names->elements) {
        int i, num_parts = partition_names->elements;
        List_iterator<String> name_it(*(partition_names));
        str->append(STRING_WITH_LEN(" PARTITION ("));
        for (i = 1; i <= num_parts; i++) {
          String *name = name_it++;
          append_identifier(thd, str, name->c_ptr(), name->length());
          if (i != num_parts) str->append(',');
        }
        str->append(')');
      }
    }
    if (my_strcasecmp(table_alias_charset, cmp_name, alias)) {
      char t_alias_buff[MAX_ALIAS_NAME];
      const char *t_alias = alias;

      str->append(' ');
      if (lower_case_table_names == 1) {
        if (alias && alias[0])  // Print alias in lowercase
        {
          my_stpcpy(t_alias_buff, alias);
          my_casedn_str(files_charset_info, t_alias_buff);
          t_alias = t_alias_buff;
        }
      }

      append_identifier(thd, str, t_alias, strlen(t_alias));
    }

    /*
      The optional column list is to be specified in the definition. For a
      CTE, the definition is in WITH, and here we only have a
      reference. For a Derived Table, the definition is here.
    */
    if (is_derived() && !common_table_expr())
      print_derived_column_names(thd, str, m_derived_column_names);

    if (index_hints) {
      List_iterator<Index_hint> it(*index_hints);
      Index_hint *hint;

      while ((hint = it++)) {
        str->append(STRING_WITH_LEN(" "));
        hint->print(thd, str);
      }
    }
  }
}

void Query_block::print(const THD *thd, String *str,
                        enum_query_type query_type) {
  /* QQ: thd may not be set for sub queries, but this should be fixed */
  if (!thd) thd = current_thd;

  if (select_number == 1) {
    if (print_error(thd, str)) return;

    switch (parent_lex->sql_command) {
      case SQLCOM_UPDATE:
        [[fallthrough]];
      case SQLCOM_UPDATE_MULTI:
        print_update(thd, str, query_type);
        return;
      case SQLCOM_DELETE:
        [[fallthrough]];
      case SQLCOM_DELETE_MULTI:
        print_delete(thd, str, query_type);
        return;
      case SQLCOM_INSERT:
        [[fallthrough]];
      case SQLCOM_INSERT_SELECT:
      case SQLCOM_REPLACE:
      case SQLCOM_REPLACE_SELECT:
        print_insert(thd, str, query_type);
        return;
      case SQLCOM_INSERT_ALL_SELECT:
        print_insert_all(thd, str, query_type);
        return;
      case SQLCOM_SELECT:
        [[fallthrough]];
      default:
        break;
    }
  }
  if (is_table_value_constructor) {
    print_values(thd, str, query_type, *row_value_list, "row");
  } else {
    print_query_block(thd, str, query_type);
  }
}

void Query_block::print_query_block(const THD *thd, String *str,
                                    enum_query_type query_type) {
  if (query_type & QT_SHOW_SELECT_NUMBER) {
    /* it makes EXPLAIN's "id" column understandable */
    str->append("/* select#");
    str->append_ulonglong(select_number);
    str->append(" */ select ");
  } else
    str->append(STRING_WITH_LEN("select "));

  print_hints(thd, str, query_type);
  print_select_options(str);
  print_item_list(thd, str, query_type);
  print_from_clause(thd, str, query_type);
  print_where_cond(thd, str, query_type);
  print_connect_by_cond(thd, str, query_type);
  print_group_by(thd, str, query_type);
  print_having(thd, str, query_type);
  print_windows(thd, str, query_type);
  print_order_by(thd, str, query_type);
  print_limit(thd, str, query_type);
  // PROCEDURE unsupported here
}

void Query_block::print_merge(const THD *thd, String *str,
                              enum_query_type query_type) {
  Sql_cmd_update *cmd = (static_cast<Sql_cmd_update *>(parent_lex->m_sql_cmd));
  str->append(STRING_WITH_LEN("merge "));
  print_hints(thd, str, query_type);
  str->append(STRING_WITH_LEN("into "));

  Table_ref *s = m_table_list.first;
  Table_ref *t = m_table_list.first->next_local;

  t->print(thd, str, query_type);
  str->append(STRING_WITH_LEN(" using "));
  s->print(thd, str, query_type);

  const bool is_optimized =
      t->query_block->join && t->query_block->join->is_optimized();
  Item *const cond = is_optimized ? t->join_cond_optim() : t->join_cond();
  if (cond) {
    str->append(STRING_WITH_LEN(" on("));
    cond->print(thd, str, query_type);
    str->append(')');
  }

  if (fields.size() > 0) {
    str->append(STRING_WITH_LEN(" when matched then update set "));
    print_update_list(thd, str, query_type, fields, *cmd->update_value_list);
    if (cmd->opt_merge_update_where) {
      str->append(STRING_WITH_LEN(" where "));
      cmd->opt_merge_update_where->print(thd, str, query_type);
    }
    if (cmd->opt_merge_update_delete) {
      str->append(STRING_WITH_LEN(" delete where "));
      cmd->opt_merge_update_delete->print(thd, str, query_type);
    }
  }

  if (cmd->merge_when_insert) {
    str->append(STRING_WITH_LEN(" when not matched then insert "));
    if (cmd->insert_field_list.size() > 0) {
      str->append(STRING_WITH_LEN(" ("));
      bool first = true;
      for (Item *field : cmd->insert_field_list) {
        if (first)
          first = false;
        else
          str->append(',');

        field->print(thd, str, query_type);
      }
      str->append(") ");
    }
    print_values(thd, str, query_type, cmd->insert_many_values, nullptr);

    if (cmd->opt_merge_insert_where) {
      str->append(STRING_WITH_LEN(" where "));
      cmd->opt_merge_insert_where->print(thd, str, query_type);
    }
  }
}

void Query_block::print_update(const THD *thd, String *str,
                               enum_query_type query_type) {
  Sql_cmd_update *sql_cmd_update =
      (static_cast<Sql_cmd_update *>(parent_lex->m_sql_cmd));
  if (sql_cmd_update->merge_into_stmt) {
    print_merge(thd, str, query_type);
    return;
  }

  str->append(STRING_WITH_LEN("update "));
  print_hints(thd, str, query_type);
  print_update_options(str);
  if (parent_lex->sql_command == SQLCOM_UPDATE) {
    // Single table update
    Table_ref *t = get_table_list();
    t->print(thd, str, query_type);  // table identifier
    str->append(STRING_WITH_LEN(" set "));
    print_update_list(thd, str, query_type, fields,
                      *sql_cmd_update->update_value_list);
    /*
      Print join condition (may happen with a merged view's WHERE condition
      and disappears in simplify_joins(); visible in opt trace only).
    */
    Item *const cond = t->join_cond();
    if (cond) {
      str->append(STRING_WITH_LEN(" on("));
      cond->print(thd, str, query_type);
      str->append(')');
    }
    print_where_cond(thd, str, query_type);
    print_order_by(thd, str, query_type);
    print_limit(thd, str, query_type);
  } else {
    // Multi table update
    print_join(thd, str, &m_table_nest, query_type);
    str->append(STRING_WITH_LEN(" set "));
    print_update_list(thd, str, query_type, fields,
                      *sql_cmd_update->update_value_list);
    print_where_cond(thd, str, query_type);
  }
}

void Query_block::print_delete(const THD *thd, String *str,
                               enum_query_type query_type) {
  str->append(STRING_WITH_LEN("delete "));
  print_hints(thd, str, query_type);
  print_delete_options(str);
  if (parent_lex->sql_command == SQLCOM_DELETE) {
    Table_ref *t = get_table_list();
    // Single table delete
    str->append(STRING_WITH_LEN("from "));
    t->print(thd, str, query_type);  // table identifier
    /*
      Print join condition (may happen with a merged view's WHERE condition
      and disappears in simplify_joins(); visible in opt trace only).
    */
    Item *const cond = t->join_cond();
    if (cond) {
      str->append(STRING_WITH_LEN(" on("));
      cond->print(thd, str, query_type);
      str->append(')');
    }
    print_where_cond(thd, str, query_type);
    print_order_by(thd, str, query_type);
    print_limit(thd, str, query_type);
  } else {
    // Multi table delete
    print_table_references(thd, str, parent_lex->query_tables, query_type);
    str->append(STRING_WITH_LEN(" from "));
    print_join(thd, str, &m_table_nest, query_type);
    print_where_cond(thd, str, query_type);
  }
}

void Query_block::print_insert(const THD *thd, String *str,
                               enum_query_type query_type) {
  /**
    USES: 'INSERT INTO table (fields) VALUES values' syntax over
    'INSERT INTO table SET field = value, ...'
  */
  Sql_cmd_insert_base *sql_cmd_insert =
      down_cast<Sql_cmd_insert_base *>(parent_lex->m_sql_cmd);

  if (parent_lex->sql_command == SQLCOM_REPLACE ||
      parent_lex->sql_command == SQLCOM_REPLACE_SELECT)
    str->append(STRING_WITH_LEN("replace "));
  else
    str->append(STRING_WITH_LEN("insert "));

  // Don't print QB name hints since it will be printed through
  // print_query_block.
  print_hints(thd, str, enum_query_type(query_type | QT_IGNORE_QB_NAME));
  print_insert_options(str);
  str->append(STRING_WITH_LEN("into "));

  Table_ref *tbl = (parent_lex->insert_table_leaf)
                       ? parent_lex->insert_table_leaf
                       : get_table_list();
  tbl->print(thd, str, query_type);  // table identifier

  print_insert_fields(thd, str, query_type);
  str->append(STRING_WITH_LEN(" "));

  if (parent_lex->sql_command == SQLCOM_INSERT ||
      parent_lex->sql_command == SQLCOM_REPLACE) {
    print_values(thd, str, query_type, sql_cmd_insert->insert_many_values,
                 nullptr);
  } else {
    /*
      Print only QB name hint here since other hints were printed in the
      earlier call to print_hints.
    */
    print_query_block(thd, str, enum_query_type(query_type | QT_ONLY_QB_NAME));
  }

  if (!sql_cmd_insert->update_field_list.empty()) {
    str->append(STRING_WITH_LEN(" on duplicate key update "));
    print_update_list(thd, str, query_type, sql_cmd_insert->update_field_list,
                      sql_cmd_insert->update_value_list);
  }
}

void Query_block::print_insert_all(const THD *thd, String *str,
                                   enum_query_type query_type) {
  /**
    with xx ( select xxx)
    insert all into t1(col) values (t1)
               into t2(col) values (t1)

  */
  Sql_cmd_insert_all *sql_cmd_insert =
      down_cast<Sql_cmd_insert_all *>(parent_lex->m_sql_cmd);
  str->append(STRING_WITH_LEN("insert "));
  // Don't print QB name hints since it will be printed through
  // print_query_block.
  print_hints(thd, str, enum_query_type(query_type | QT_IGNORE_QB_NAME));
  print_insert_options(str);

  if (sql_cmd_insert->break_after_match)
    str->append(STRING_WITH_LEN("first "));
  else
    str->append(STRING_WITH_LEN(" all "));
  auto tb = parent_lex->insert_table_leaf;

  auto insert_values_list = visible_fields();
  auto fields_iter = insert_values_list.begin();

  int prev_clause_type = INSERT_INTO_NOT_SET;
  for (uint i = 0; i < sql_cmd_insert->into_table_count;
       i++, tb = tb->next_local) {
    if (!sql_cmd_insert->unconditional) {
      if (sql_cmd_insert->clause_type[i] == INSERT_INTO_ELSE) {
        if (prev_clause_type != INSERT_INTO_ELSE)
          str->append(STRING_WITH_LEN("else "));
      } else if (sql_cmd_insert->clause_type[i] == INSERT_INTO_WHEN) {
        if (sql_cmd_insert->opt_when[i] != nullptr) {
          str->append(STRING_WITH_LEN("when "));
          sql_cmd_insert->opt_when[i]->print(thd, str, query_type);
          str->append(STRING_WITH_LEN(" then "));
        }
      }
      prev_clause_type = sql_cmd_insert->clause_type[i];
    }
    str->append(STRING_WITH_LEN("into "));

    tb->print(thd, str, query_type);  // table identifier

    if (sql_cmd_insert->fields_for_table[i]->size() > 0) {
      str->append(STRING_WITH_LEN(" ("));
      bool first = true;
      for (auto field : *sql_cmd_insert->fields_for_table[i]) {
        if (first) {
          first = false;
        } else {
          str->append(',');
        }
        field->print(thd, str, query_type);
      }
      str->append(STRING_WITH_LEN(") "));
      for (uint j = 0; fields_iter != insert_values_list.end() &&
                       j < sql_cmd_insert->fields_for_table[i]->size();
           j++, fields_iter++) {
        if (j != 0) {
          str->append(',');
        } else {
          str->append(STRING_WITH_LEN("VALUES ("));
          ;
        }
        (*fields_iter)->print(thd, str, query_type);
      }
      str->append(STRING_WITH_LEN(") "));
    }
  }
  print_query_block(thd, str, enum_query_type(query_type | QT_ONLY_QB_NAME));
}

void Query_block::print_hints(const THD *thd, String *str,
                              enum_query_type query_type) {
  if (thd->lex->opt_hints_global) {
    char buff[NAME_LEN];
    String hint_str(buff, sizeof(buff), system_charset_info);
    hint_str.length(0);

    if (select_number == 1 ||
        // First select number is 2 for SHOW CREATE VIEW
        (select_number == 2 && parent_lex->sql_command == SQLCOM_SHOW_CREATE)) {
      if (opt_hints_qb && !(query_type & QT_IGNORE_QB_NAME))
        opt_hints_qb->append_qb_hint(thd, &hint_str);
      if (!(query_type & QT_ONLY_QB_NAME))
        thd->lex->opt_hints_global->print(thd, &hint_str, query_type);
    } else if (opt_hints_qb)
      opt_hints_qb->append_qb_hint(thd, &hint_str);

    if (hint_str.length() > 0) {
      str->append(STRING_WITH_LEN("/*+ "));
      str->append(hint_str.ptr(), hint_str.length());
      str->append(STRING_WITH_LEN("*/ "));
    }
  }
}

bool Query_block::print_error(const THD *thd, String *str) {
  if (thd->is_error()) {
    /*
      It is possible that this query block had an optimization error, but the
      caller didn't notice (caller evaluated this as a subquery and Item::val*()
      don't have an error status). In this case the query block may be broken
      and printing it may crash.
    */
    str->append(STRING_WITH_LEN("had some error"));
    return true;
  }
  /*
    In order to provide info for EXPLAIN FOR CONNECTION units shouldn't be
    completely cleaned till the end of the query. This is valid only for
    explainable commands.
  */
  assert(!(master_query_expression()->cleaned == Query_expression::UC_CLEAN &&
           is_explainable_query(thd->lex->sql_command)));
  return false;
}

void Query_block::print_select_options(String *str) {
  /* First add options */
  if (active_options() & SELECT_STRAIGHT_JOIN)
    str->append(STRING_WITH_LEN("straight_join "));
  if (active_options() & SELECT_HIGH_PRIORITY)
    str->append(STRING_WITH_LEN("high_priority "));
  if (active_options() & SELECT_DISTINCT)
    str->append(STRING_WITH_LEN("distinct "));
  if (active_options() & SELECT_SMALL_RESULT)
    str->append(STRING_WITH_LEN("sql_small_result "));
  if (active_options() & SELECT_BIG_RESULT)
    str->append(STRING_WITH_LEN("sql_big_result "));
  if (active_options() & OPTION_BUFFER_RESULT)
    str->append(STRING_WITH_LEN("sql_buffer_result "));
  if (active_options() & OPTION_FOUND_ROWS)
    str->append(STRING_WITH_LEN("sql_calc_found_rows "));
}

void Query_block::print_update_options(String *str) {
  if (get_table_list() &&
      get_table_list()->mdl_request.type == MDL_SHARED_WRITE_LOW_PRIO)
    str->append(STRING_WITH_LEN("low_priority "));
  if (parent_lex->is_ignore()) str->append(STRING_WITH_LEN("ignore "));
}

void Query_block::print_delete_options(String *str) {
  if (get_table_list() &&
      get_table_list()->mdl_request.type == MDL_SHARED_WRITE_LOW_PRIO)
    str->append(STRING_WITH_LEN("low_priority "));
  if (active_options() & OPTION_QUICK) str->append(STRING_WITH_LEN("quick "));
  if (parent_lex->is_ignore()) str->append(STRING_WITH_LEN("ignore "));
}

void Query_block::print_insert_options(String *str) {
  if (get_table_list()) {
    int type = static_cast<int>(get_table_list()->lock_descriptor().type);

    // Lock option
    if (type == static_cast<int>(TL_WRITE_LOW_PRIORITY))
      str->append(STRING_WITH_LEN("low_priority "));
    else if (type == static_cast<int>(TL_WRITE))
      str->append(STRING_WITH_LEN("high_priority "));
  }

  if (parent_lex->is_ignore()) str->append(STRING_WITH_LEN("ignore "));
}

void Query_block::print_table_references(const THD *thd, String *str,
                                         Table_ref *table_list,
                                         enum_query_type query_type) {
  bool first = true;
  for (Table_ref *tbl = table_list; tbl; tbl = tbl->next_local) {
    if (tbl->updating) {
      if (first)
        first = false;
      else
        str->append(STRING_WITH_LEN(", "));

      Table_ref *t = tbl;

      /*
        Query Rewrite Plugin will not have is_view() set even for a view. This
        is because operations like open_table haven't happened yet. So the
        underlying target tables will not be added, only the original
        table/view list will be reproduced. Ideally, it would be better if
        Table_ref::updatable_base_table() were used here, but that isn't
        possible due to QRP.
      */
      while (t->is_view()) t = t->merge_underlying_list;

      if (!(query_type & QT_NO_DB) &&
          !((query_type & QT_NO_DEFAULT_DB) &&
            db_is_default_db(t->db, t->db_length, thd))) {
        append_identifier(thd, str, t->db, t->db_length);
        str->append('.');
      }
      append_identifier(thd, str, t->table_name, t->table_name_length);
    }
  }
}

void Query_block::print_item_list(const THD *thd, String *str,
                                  enum_query_type query_type) {
  // Item List
  bool first = true;
  for (Item *item : visible_fields()) {
    if (first)
      first = false;
    else
      str->append(',');

    if ((master_query_expression()->item &&
         item->item_name.is_autogenerated()) ||
        (query_type & QT_NORMALIZED_FORMAT) ||
        thd->lex->create_force_view_table_not_found) {
      /*
        Do not print auto-generated aliases in subqueries. It has no purpose
        in a view definition or other contexts where the query is printed.
      */
      item->print(thd, str, query_type);
      /*
       support aliase for create force view, example:
       create force view t1_view as select ifnull(a,0) AS used from t1;
       create force view t1_view as select a as a1 from t1 as e,
       can print 'used','a1' in query statment
    */
      if (thd->lex->create_force_view_table_not_found &&
          item->item_name.is_set() && *(item->item_name.ptr()) != '*') {
        str->append(STRING_WITH_LEN(" AS "));
        append_identifier(thd, str, item->item_name.ptr(),
                          item->item_name.length());
      }
    } else
      item->print_item_w_name(thd, str, query_type);
    /** @note that 'INTO variable' clauses are not printed */
  }
}

void Query_block::print_update_list(const THD *thd, String *str,
                                    enum_query_type query_type,
                                    const mem_root_deque<Item *> &fields,
                                    const mem_root_deque<Item *> &values) {
  auto it_column = VisibleFields(fields).begin();
  auto it_value = values.begin();
  bool first = true;
  while (it_column != VisibleFields(fields).end() && it_value != values.end()) {
    Item *column = *it_column++;
    Item *value = *it_value++;
    if (first)
      first = false;
    else
      str->append(',');

    column->print(thd, str, query_type);
    str->append(STRING_WITH_LEN(" = "));
    value->print(thd, str, enum_query_type(query_type & ~QT_NO_DATA_EXPANSION));
  }
}

void Query_block::print_insert_fields(const THD *thd, String *str,
                                      enum_query_type query_type) {
  Sql_cmd_insert_base *const cmd =
      down_cast<Sql_cmd_insert_base *>(parent_lex->m_sql_cmd);
  const mem_root_deque<Item *> &fields = cmd->insert_field_list;
  if (cmd->column_count > 0) {
    str->append(STRING_WITH_LEN(" ("));
    bool first = true;
    for (Item *field : fields) {
      if (first)
        first = false;
      else
        str->append(',');

      field->print(thd, str, query_type);
    }
    str->append(')');
  }
}

void Query_block::print_values(
    const THD *thd, String *str, enum_query_type query_type,
    const mem_root_deque<mem_root_deque<Item *> *> &values,
    const char *prefix) {
  str->append(STRING_WITH_LEN("values "));
  bool row_first = true;
  for (const mem_root_deque<Item *> *row : values) {
    if (row_first)
      row_first = false;
    else
      str->append(',');

    if (prefix != nullptr) str->append(prefix);

    str->append('(');
    bool col_first = true;
    for (Item *item : *row) {
      if (col_first)
        col_first = false;
      else
        str->append(',');

      item->print(thd, str, query_type);
    }
    str->append(')');
  }
}

void Query_block::print_from_clause(const THD *thd, String *str,
                                    enum_query_type query_type) {
  /*
    from clause
  */
  if (m_table_list.elements) {
    str->append(STRING_WITH_LEN(" from "));
    /* go through join tree */
    print_join(thd, str, &m_table_nest, query_type);
  } else if (m_where_cond) {
    /*
      "SELECT 1 FROM DUAL WHERE 2" should not be printed as
      "SELECT 1 WHERE 2": the 1st syntax is valid, but the 2nd is not.
    */
    str->append(STRING_WITH_LEN(" from DUAL "));
  }
}

void Query_block::print_where_cond(const THD *thd, String *str,
                                   enum_query_type query_type) {
  // Where
  Item *const cur_where =
      (join && join->is_optimized()) ? join->where_cond : m_where_cond;
  Item *const after_where = (join && join->is_optimized())
                                ? join->after_connect_by_cond
                                : m_after_connect_by_where;

  if (cur_where || cond_value != Item::COND_UNDEF) {
    str->append(STRING_WITH_LEN(" where "));
    if (cur_where) {
      cur_where->print(thd, str, query_type);
      if (after_where) {
        // if use the connect by
        //  a and b  /  a or b => cur_where = a   after_where = b
        //  so andor is the same
        str->append(STRING_WITH_LEN(" and /* connect by */ "));
        after_where->print(thd, str, query_type);
      }
    } else if (after_where) {
      str->append(STRING_WITH_LEN(" /* connect by */ "));
      after_where->print(thd, str, query_type);
    } else
      str->append(cond_value != Item::COND_FALSE ? "true" : "false");
  } else {
    if (after_where || after_connect_by_value != Item::COND_UNDEF) {
      str->append(STRING_WITH_LEN(" where /* connect by */ "));
      if (after_where) {
        after_where->print(thd, str, query_type);
      } else
        str->append(after_connect_by_value != Item::COND_FALSE ? "true"
                                                               : "false");
    }
  }
}

void Query_block::print_connect_by_cond(const THD *thd, String *str,
                                        enum_query_type query_type) {
  Item *const connect_by = (join && join->is_optimized())
                               ? join->connect_by_cond
                               : m_connect_by_cond;

  if (connect_by) {
    str->append(STRING_WITH_LEN(" connect by "));
    connect_by->print(thd, str, query_type);

    Item *const start_with = (join && join->is_optimized())
                                 ? join->start_with_cond
                                 : m_start_with_cond;

    if (start_with) {
      str->append(STRING_WITH_LEN(" start with "));
      start_with->print(thd, str, query_type);
    }
  }
}

void Query_block::print_group_by(const THD *thd, String *str,
                                 enum_query_type query_type) {
  // group by & olap
  if (group_list.elements) {
    str->append(STRING_WITH_LEN(" group by "));
    print_order(thd, str, group_list.first, query_type);
    switch (olap) {
      case ROLLUP_TYPE:
        str->append(STRING_WITH_LEN(" with rollup"));
        break;
      default:;  // satisfy compiler
    }
  }
}

void Query_block::print_having(const THD *thd, String *str,
                               enum_query_type query_type) {
  // having
  Item *const cur_having = (join && join->having_for_explain != (Item *)1)
                               ? join->having_for_explain
                               : m_having_cond;

  if (cur_having || having_value != Item::COND_UNDEF) {
    str->append(STRING_WITH_LEN(" having "));
    if (cur_having)
      cur_having->print(thd, str, query_type);
    else
      str->append(having_value != Item::COND_FALSE ? "true" : "false");
  }
}

void Query_block::print_windows(const THD *thd, String *str,
                                enum_query_type query_type) {
  List_iterator<Window> li(m_windows);
  Window *w;
  bool first = true;
  while ((w = li++)) {
    if (w->name() == nullptr) continue;  // will be printed with function

    if (first) {
      first = false;
      str->append(" window ");
    } else {
      str->append(", ");
    }

    append_identifier(thd, str, w->name()->item_name.ptr(),
                      strlen(w->name()->item_name.ptr()));
    str->append(" AS ");
    w->print(thd, str, query_type, true);
  }
}

void Query_block::print_order_by(const THD *thd, String *str,
                                 enum_query_type query_type) const {
  if (order_list.elements) {
    str->append(STRING_WITH_LEN(" order by "));
    print_order(thd, str, order_list.first, query_type);
  }
}

static enum_walk get_walk_flags(const Select_lex_visitor *visitor) {
  if (visitor->visits_in_prefix_order())
    return enum_walk::SUBQUERY_PREFIX;
  else
    return enum_walk::SUBQUERY_POSTFIX;
}

bool walk_item(Item *item, Select_lex_visitor *visitor) {
  if (item == nullptr) return false;
  return item->walk(&Item::visitor_processor, get_walk_flags(visitor),
                    pointer_cast<uchar *>(visitor));
}

bool accept_for_order(SQL_I_List<ORDER> orders, Select_lex_visitor *visitor) {
  if (orders.elements == 0) return false;

  for (ORDER *order = orders.first; order != nullptr; order = order->next)
    if (walk_item(*order->item, visitor)) return true;
  return false;
}

bool Query_expression::accept(Select_lex_visitor *visitor) {
  for (auto qt : query_terms<>()) {
    if (qt->term_type() == QT_QUERY_BLOCK)
      qt->query_block()->accept(visitor);
    else
      // FIXME: why doesn't this also visit limit? done for Query_block's limit
      // FIXME: Worse, Query_block::accept doesn't visit windows' ordering
      // expressions
      accept_for_order(qt->query_block()->order_list, visitor);
  }

  return visitor->visit(this);
}

bool accept_for_join(mem_root_deque<Table_ref *> *tables,
                     Select_lex_visitor *visitor) {
  for (Table_ref *t : *tables) {
    if (accept_table(t, visitor)) return true;
  }
  return false;
}

bool accept_table(Table_ref *t, Select_lex_visitor *visitor) {
  if (t->nested_join && accept_for_join(&t->nested_join->m_tables, visitor))
    return true;
  if (t->is_derived()) t->derived_query_expression()->accept(visitor);
  if (walk_item(t->join_cond(), visitor)) return true;
  return false;
}

bool Query_block::accept(Select_lex_visitor *visitor) {
  // Select clause
  for (Item *item : visible_fields()) {
    if (walk_item(item, visitor)) return true;
  }

  // From clause
  if (m_table_list.elements != 0 &&
      accept_for_join(m_current_table_nest, visitor))
    return true;

  // Where clause
  Item *where_condition = join != nullptr ? join->where_cond : m_where_cond;
  if (where_condition != nullptr && walk_item(where_condition, visitor))
    return true;

  // connect by
  Item *connect_by_condition =
      join != nullptr ? join->connect_by_cond : m_connect_by_cond;
  if (connect_by_condition != nullptr &&
      walk_item(connect_by_condition, visitor))
    return true;

  // start with
  Item *start_with_condition =
      join != nullptr ? join->start_with_cond : m_start_with_cond;
  if (start_with_condition != nullptr &&
      walk_item(start_with_condition, visitor))
    return true;

  // Group by and olap clauses
  if (accept_for_order(group_list, visitor)) return true;

  // Having clause
  Item *having_condition =
      join != nullptr ? join->having_for_explain : m_having_cond;
  if (walk_item(having_condition, visitor)) return true;

  // Order clause
  if (accept_for_order(order_list, visitor)) return true;

  // Limit clause
  if (has_limit()) {
    if (walk_item(offset_limit, visitor) || walk_item(select_limit, visitor))
      return true;
  }
  return visitor->visit(this);
}

void LEX::clear_privileges() {
  users_list.clear();
  columns.clear();
  grant = grant_tot_col = grant_privilege = false;
  all_privileges = false;
  ssl_type = SSL_TYPE_NOT_SPECIFIED;
  ssl_cipher = x509_subject = x509_issuer = nullptr;
  alter_password.cleanup();
  memset(&mqh, 0, sizeof(mqh));
  dynamic_privileges.clear();
  default_roles = nullptr;
}

/*
  Initialize (or reset) Query_tables_list object.

  SYNOPSIS
    reset_query_tables_list()
      init  true  - we should perform full initialization of object with
                    allocating needed memory
            false - object is already initialized so we should only reset
                    its state so it can be used for parsing/processing
                    of new statement

  DESCRIPTION
    This method initializes Query_tables_list so it can be used as part
    of LEX object for parsing/processing of statement. One can also use
    this method to reset state of already initialized Query_tables_list
    so it can be used for processing of new statement.
*/

void Query_tables_list::reset_query_tables_list(bool init) {
  sql_command = SQLCOM_END;
  if (!init && query_tables) {
    Table_ref *table = query_tables;
    for (;;) {
      delete table->view_query();
      if (query_tables_last == &table->next_global ||
          !(table = table->next_global))
        break;
    }
  }
  query_tables = nullptr;
  query_tables_last = &query_tables;
  query_tables_own_last = nullptr;
  if (init) {
    /*
      We delay real initialization of hash (and therefore related
      memory allocation) until first insertion into this hash.
    */
    sroutines.reset();
  } else if (sroutines != nullptr) {
    sroutines->clear();
  }
  sroutines_list.clear();
  sroutines_list_own_last = sroutines_list.next;
  sroutines_list_own_elements = 0;
  binlog_stmt_flags = 0;
  stmt_accessed_table_flag = 0;
  lock_tables_state = LTS_NOT_LOCKED;
  table_count = 0;
  using_match = false;
  stmt_unsafe_with_mixed_mode = false;

  /* Check the max size of the enum to control new enum values definitions. */
  static_assert(BINLOG_STMT_UNSAFE_COUNT <= 32, "");
}

/*
  Destroy Query_tables_list object with freeing all resources used by it.

  SYNOPSIS
    destroy_query_tables_list()
*/

void Query_tables_list::destroy_query_tables_list() { sroutines.reset(); }

/*
  Initialize LEX object.

  SYNOPSIS
    LEX::LEX()

  NOTE
    LEX object initialized with this constructor can be used as part of
    THD object for which one can safely call open_tables(), lock_tables()
    and close_thread_tables() functions. But it is not yet ready for
    statement parsing. On should use lex_start() function to prepare LEX
    for this.
*/

LEX::LEX()
    : unit(nullptr),
      query_block(nullptr),
      all_query_blocks_list(nullptr),
      m_current_query_block(nullptr),
      result(nullptr),
      thd(nullptr),
      opt_hints_global(nullptr),
      // Quite unlikely to overflow initial allocation, so no instrumentation.
      plugins(PSI_NOT_INSTRUMENTED),
      insert_update_values_map(nullptr),
      option_type(OPT_DEFAULT),
      drop_temporary(false),
      sphead(nullptr),
      // Initialize here to avoid uninitialized variable warnings.
      contains_plaintext_password(false),
      keep_diagnostics(DA_KEEP_UNSPECIFIED),
      is_lex_started(false),
      in_update_value_clause(false),
      will_contextualize(true) {
  reset_query_tables_list(true);
}

/**
  check if command can use VIEW with MERGE algorithm (for top VIEWs)

  @details
    Only listed here commands can use merge algorithm in top level
    Query_block (for subqueries will be used merge algorithm if
    LEX::can_not_use_merged() is not true).

  @todo - Add SET as a command that can use merged views. Due to how
          all uses would be embedded in subqueries, this test is worthless
          for the SET command anyway.

  @returns true if command can use merged VIEWs, false otherwise
*/

bool LEX::can_use_merged() {
  switch (sql_command) {
    case SQLCOM_SELECT:
    case SQLCOM_CREATE_TABLE:
    case SQLCOM_UPDATE:
    case SQLCOM_UPDATE_MULTI:
    case SQLCOM_DELETE:
    case SQLCOM_DELETE_MULTI:
    case SQLCOM_INSERT:
    case SQLCOM_INSERT_SELECT:
    case SQLCOM_INSERT_ALL_SELECT:
    case SQLCOM_REPLACE:
    case SQLCOM_REPLACE_SELECT:
    case SQLCOM_LOAD:

    /*
      With WL#6599 following SHOW commands are implemented over the
      INFORMATION_SCHEMA system views, and we do not create
      temporary tables anymore now. So these queries should be
      allowed to be mergeable, which makes the INFORMATION_SCHEMA
      query execution faster.

      According to optimizer team (Roy), making this decision based
      on the command type here is a hack. This should probably change when we
      introduce Sql_cmd_show class, which should treat the following SHOW
      commands same as SQLCOM_SELECT.
     */
    case SQLCOM_SHOW_CHARSETS:
    case SQLCOM_SHOW_COLLATIONS:
    case SQLCOM_SHOW_DATABASES:
    case SQLCOM_SHOW_EVENTS:
    case SQLCOM_SHOW_FIELDS:
    case SQLCOM_SHOW_KEYS:
    case SQLCOM_SHOW_STATUS_FUNC:
    case SQLCOM_SHOW_STATUS_PROC:
    case SQLCOM_SHOW_SEQUENCES:
    case SQLCOM_SHOW_TABLES:
    case SQLCOM_SHOW_TABLE_STATUS:
    case SQLCOM_SHOW_TRIGGERS:
      return true;
    default:
      return false;
  }
}

/**
  Check if command can't use merged views in any part of command

  @details
    Temporary table algorithm will be used on all SELECT levels for queries
    listed here (see also LEX::can_use_merged()).

  @returns true if command cannot use merged view, false otherwise
*/

bool LEX::can_not_use_merged() {
  switch (sql_command) {
    case SQLCOM_CREATE_VIEW:
    case SQLCOM_SHOW_CREATE:
      return true;
    default:
      return false;
  }
}

/*
  case SQLCOM_REVOKE_ROLE:
  case SQLCOM_GRANT_ROLE:
  Should Items_ident be printed correctly

  SYNOPSIS
    need_correct_ident()

  RETURN
    true yes, we need only structure
    false no, we need data
*/

bool LEX::need_correct_ident() {
  switch (sql_command) {
    case SQLCOM_SHOW_CREATE:
    case SQLCOM_SHOW_SEQUENCES:
    case SQLCOM_SHOW_TABLES:
    case SQLCOM_CREATE_VIEW:
      return true;
    default:
      return false;
  }
}

/**
  This method should be called only during parsing.
  It is aware of compound statements (stored routine bodies)
  and will initialize the destination with the default
  database of the stored routine, rather than the default
  database of the connection it is parsed in.
  E.g. if one has no current database selected, or current database
  set to 'bar' and then issues:

  CREATE PROCEDURE foo.p1() BEGIN SELECT * FROM t1 END//

  t1 is meant to refer to foo.t1, not to bar.t1.

  This method is needed to support this rule.

  @return true in case of error (parsing should be aborted, false in
  case of success
*/

bool LEX::copy_db_to(char const **p_db, size_t *p_db_length) const {
  if (sphead) {
    assert(sphead->m_db.str && sphead->m_db.length);
    /*
      It is safe to assign the string by-pointer, both sphead and
      its statements reside in the same memory root.
    */
    *p_db = sphead->m_db.str;
    if (p_db_length) *p_db_length = sphead->m_db.length;
    return false;
  }
  return thd->copy_db_to(p_db, p_db_length);
}

/**
  Set limit and offset for query expression object

  @param thd      thread handler
  @param provider Query_block to get offset and limit from.

  @returns false if success, true if error
*/
bool Query_expression::set_limit(THD *thd, Query_block *provider) {
  bool isNull = false;
  offset_limit_cnt = provider->get_offset(thd, &isNull);
  select_limit_cnt = provider->get_limit(thd);
  select_limit_ties = provider->select_limit_ties;
  select_limit_percent = provider->select_limit_percent;
  select_limit_percent_value =
      isNull ? 0 : provider->select_limit_percent_value;
  if (!isNull) {
    if (select_limit_cnt + offset_limit_cnt >= select_limit_cnt)
      select_limit_cnt += offset_limit_cnt;
    else
      select_limit_cnt = HA_POS_ERROR;
  } else {
    select_limit_cnt = 0;
  }
  return false;
}

/**
  Checks if this query expression has limit defined. For a query expression
  with set operation it checks if any of the query blocks has limit defined.

  @returns true if the query expression has limit.
  false otherwise.
*/
bool Query_expression::has_any_limit() const {
  for (auto qt : query_terms<>())
    if (qt->query_block()->has_limit()) return true;

  return false;
}

bool Query_expression::has_any_connect_by() const {
  for (auto qt : query_terms<>())
    if (qt->query_block()->connect_by_cond()) return true;

  return false;
}

/**
  Include a query expression below a query block.

  @param lex   Containing LEX object
  @param outer The query block that this query expression is included below.
*/
void Query_expression::include_down(LEX *lex, Query_block *outer) {
  if ((next = outer->slave)) next->prev = &next;
  prev = &outer->slave;
  outer->slave = this;
  master = outer;

  renumber_selects(lex);
}

/**
  Return true if query expression can be merged into an outer query, based on
  technical constraints.
  Being mergeable also means that derived table/view is updatable.

  A view/derived table is not mergeable if it is one of the following:
   - A set operation (implementation restriction).
   - An aggregated query, or has HAVING, or has DISTINCT
     (A general aggregated query cannot be merged with a non-aggregated one).
   - A table-less query (unimportant special case).
   - A query with a LIMIT (limit applies to subquery, so the implementation
     strategy is to materialize this subquery, including row count constraint).
   - It has windows
*/

bool Query_expression::is_mergeable() const {
  if (is_set_operation()) return false;

  Query_block *const select = first_query_block();
  Query_block *const parent = outer_query_block();
  bool has_rownum =
      ((select->has_rownum) || (parent && parent->has_rownum)) ? true : false;
  bool has_lnnvl_func =
      (select->has_lnnvl_func || (parent && parent->has_lnnvl_func)) ? true
                                                                     : false;
  bool has_foj =
      (select->has_foj() || (parent && parent->has_foj())) ? true : false;
  return !select->is_grouped() && !select->having_cond() &&
         !select->is_distinct() && select->m_table_list.elements > 0 &&
         !select->has_limit() && select->m_windows.elements == 0 &&
         !has_rownum && !has_any_connect_by() && !has_lnnvl_func && !has_foj;
}

/**
  True if heuristics suggest to merge this query expression.

  A view/derived table is not suggested for merging if it contains subqueries
  in the SELECT list that depend on columns from itself.
  Merging such objects is possible, but we assume they are made derived
  tables because the user wants them to be materialized, for performance
  reasons.

  One possible case is a derived table with dependent subqueries in the select
  list, used as the inner table of a left outer join. Such tables will always
  be read as many times as there are qualifying rows in the outer table,
  and the select list subqueries are evaluated for each row combination.
  The select list subqueries are evaluated the same number of times also with
  join buffering enabled, even though the table then only will be read once.

  Another case is, a query that modifies variables: then try to preserve the
  original structure of the query. This is less likely to cause changes in
  variable assignment order.
*/
bool Query_expression::merge_heuristic(const LEX *lex) const {
  if (lex->set_var_list.elements != 0) return false;

  Query_block *const select = first_query_block();
  for (Item *item : select->visible_fields()) {
    if (item->has_subquery() && !item->const_for_execution()) return false;
  }
  return true;
}

/**
  Renumber contained query_block objects.

  @param  lex   Containing LEX object
*/

void Query_expression::renumber_selects(LEX *lex) {
  for (auto qt : query_terms<>()) qt->query_block()->renumber(lex);
}

/**
  Save prepared statement properties for a query expression and underlying
  query blocks. Required for repeated optimizations of the command.

  @param thd     thread handler

  @returns false if success, true if error (out of memory)
*/
bool Query_expression::save_cmd_properties(THD *thd) {
  assert(is_prepared());

  for (auto qt : query_terms<>()) qt->query_block()->save_cmd_properties(thd);
  return false;
}

/**
  Loop over all query blocks and restore information needed for optimization,
  including binding data for all associated tables.
*/
void Query_expression::restore_cmd_properties() {
  for (auto qt : query_terms<>()) qt->query_block()->restore_cmd_properties();
}

/**
  @brief Set the initial purpose of this Table_ref object in the list of
  used tables.

  We need to track this information on table-by-table basis, since when this
  table becomes an element of the pre-locked list, it's impossible to identify
  which SQL sub-statement it has been originally used in.

  E.g.:

  User request:                 SELECT * FROM t1 WHERE f1();
  FUNCTION f1():                DELETE FROM t2; RETURN 1;
  BEFORE DELETE trigger on t2:  INSERT INTO t3 VALUES (old.a);

  For this user request, the pre-locked list will contain t1, t2, t3
  table elements, each needed for different DML.

  The trigger event map is updated to reflect INSERT, UPDATE, DELETE,
  REPLACE, LOAD DATA, CREATE TABLE .. SELECT, CREATE TABLE ..
  REPLACE SELECT statements, and additionally ON DUPLICATE KEY UPDATE
  clause.
*/

void LEX::set_trg_event_type_for_tables() {
  uint8 new_trg_event_map = 0;

  /*
    Some auxiliary operations
    (e.g. GRANT processing) create Table_ref instances outside
    the parser. Additionally, some commands (e.g. OPTIMIZE) change
    the lock type for a table only after parsing is done. Luckily,
    these do not fire triggers and do not need to pre-load them.
    For these Table_refs set_trg_event_type is never called, and
    trg_event_map is always empty. That means that the pre-locking
    algorithm will ignore triggers defined on these tables, if
    any, and the execution will either fail with an assert in
    sql_trigger.cc or with an error that a used table was not
    pre-locked, in case of a production build.

    TODO: this usage pattern creates unnecessary module dependencies
    and should be rewritten to go through the parser.
    Table list instances created outside the parser in most cases
    refer to mysql.* system tables. It is not allowed to have
    a trigger on a system table, but keeping track of
    initialization provides extra safety in case this limitation
    is circumvented.
  */

  switch (sql_command) {
    case SQLCOM_LOCK_TABLES:
      /*
        On a LOCK TABLE, all triggers must be pre-loaded for this
        Table_ref when opening an associated TABLE.
      */
      new_trg_event_map =
          static_cast<uint8>(1 << static_cast<int>(TRG_EVENT_INSERT)) |
          static_cast<uint8>(1 << static_cast<int>(TRG_EVENT_UPDATE)) |
          static_cast<uint8>(1 << static_cast<int>(TRG_EVENT_DELETE));
      break;
    case SQLCOM_INSERT:
    case SQLCOM_INSERT_SELECT:
    case SQLCOM_INSERT_ALL_SELECT:
      /*
        Basic INSERT. If there is an additional ON DUPLICATE KEY
        UPDATE clause, it will be handled later in this method.
       */
    case SQLCOM_LOAD:
      /*
        LOAD DATA ... INFILE is expected to fire BEFORE/AFTER
        INSERT triggers. If the statement also has REPLACE clause, it will be
        handled later in this method.
       */
    case SQLCOM_REPLACE:
    case SQLCOM_REPLACE_SELECT:
      /*
        REPLACE is semantically equivalent to INSERT. In case
        of a primary or unique key conflict, it deletes the old
        record and inserts a new one. So we also may need to
        fire ON DELETE triggers. This functionality is handled
        later in this method.
      */
    case SQLCOM_CREATE_TABLE:
      /*
        CREATE TABLE ... SELECT defaults to INSERT if the table
        or view already exists. REPLACE option of CREATE TABLE ... REPLACE
        SELECT is handled later in this method.
       */
      new_trg_event_map |=
          static_cast<uint8>(1 << static_cast<int>(TRG_EVENT_INSERT));
      break;
    case SQLCOM_UPDATE:
    case SQLCOM_UPDATE_MULTI:
      /* Basic update and multi-update */
      new_trg_event_map |=
          static_cast<uint8>(1 << static_cast<int>(TRG_EVENT_UPDATE));
      break;
    case SQLCOM_DELETE:
    case SQLCOM_DELETE_MULTI:
      /* Basic delete and multi-delete */
      new_trg_event_map |=
          static_cast<uint8>(1 << static_cast<int>(TRG_EVENT_DELETE));
      break;
    default:
      break;
  }

  switch (duplicates) {
    case DUP_UPDATE:
      new_trg_event_map |=
          static_cast<uint8>(1 << static_cast<int>(TRG_EVENT_UPDATE));
      break;
    case DUP_REPLACE:
      new_trg_event_map |=
          static_cast<uint8>(1 << static_cast<int>(TRG_EVENT_DELETE));
      break;
    case DUP_ERROR:
    default:
      break;
  }

  /*
    Do not iterate over sub-selects, only the tables in the outermost
    Query_block can be modified, if any.
  */
  Table_ref *tables = query_block ? query_block->get_table_list() : nullptr;

  /*
    merge-into shares the same sql_command with update.
    For merge-into, only the target table should be locked for insert/delete.
    In current implementation, only insert is supported.
    TRG_EVENT_DELETE will be added when DELETE clause is implemented.
    The target tabe of merge-into is the latest one in Table_ref.
    It is not necessary to lock other tables.
  */
  if (sql_command == SQLCOM_UPDATE_MULTI) {
    Sql_cmd_update *sql_cmd_update =
        (dynamic_cast<Sql_cmd_update *>(m_sql_cmd));
    if (sql_cmd_update != nullptr && sql_cmd_update->merge_into_stmt) {
      if (sql_cmd_update->merge_when_insert) {
        new_trg_event_map |=
            static_cast<uint8>(1 << static_cast<int>(TRG_EVENT_INSERT));
      }
      if (sql_cmd_update->opt_merge_update_delete != nullptr) {
        new_trg_event_map |=
            static_cast<uint8>(1 << static_cast<int>(TRG_EVENT_DELETE));
      }
      for (; tables->next_local; tables = tables->next_local)
        ;
    }
  }

  while (tables) {
    /*
      This is a fast check to filter out statements that do
      not change data, or tables  on the right side, in case of
      INSERT .. SELECT, CREATE TABLE .. SELECT and so on.
      Here we also filter out OPTIMIZE statement and non-updateable
      views, for which lock_type is TL_UNLOCK or TL_READ after
      parsing.
    */
    if (static_cast<int>(tables->lock_descriptor().type) >=
        static_cast<int>(TL_WRITE_ALLOW_WRITE))
      tables->trg_event_map = new_trg_event_map;
    tables = tables->next_local;
  }
}

/*
  Unlink the first table from the global table list and the first table from
  outer select (lex->query_block) local list

  SYNOPSIS
    unlink_first_table()
    link_to_local	Set to 1 if caller should link this table to local list

  NOTES
    We assume that first tables in both lists is the same table or the local
    list is empty.

  RETURN
    0	If 'query_tables' == 0
    unlinked table
      In this case link_to_local is set.

*/
Table_ref *LEX::unlink_first_table(bool *link_to_local) {
  Table_ref *first;
  if ((first = query_tables)) {
    /*
      Exclude from global table list
    */
    if ((query_tables = query_tables->next_global))
      query_tables->prev_global = &query_tables;
    else
      query_tables_last = &query_tables;
    first->next_global = nullptr;

    if (query_tables_own_last == &first->next_global)
      query_tables_own_last = &query_tables;

    /*
      and from local list if it is not empty
    */
    if ((*link_to_local = query_block->get_table_list() != nullptr)) {
      query_block->context.table_list =
          query_block->context.first_name_resolution_table = first->next_local;
      query_block->m_table_list.first = first->next_local;
      query_block->m_table_list.elements--;  // safety
      first->next_local = nullptr;
      /*
        Ensure that the global list has the same first table as the local
        list.
      */
      first_lists_tables_same();
    }
  }
  return first;
}

/*
  Bring first local table of first most outer select to first place in global
  table list

  SYNOPSIS
     LEX::first_lists_tables_same()

  NOTES
    In many cases (for example, usual INSERT/DELETE/...) the first table of
    main Query_block have special meaning => check that it is the first table
    in global list and re-link to be first in the global list if it is
    necessary.  We need such re-linking only for queries with sub-queries in
    the select list, as only in this case tables of sub-queries will go to
    the global list first.
*/

void LEX::first_lists_tables_same() {
  Table_ref *first_table = query_block->get_table_list();
  if (query_tables != first_table && first_table != nullptr) {
    Table_ref *next;
    if (query_tables_last == &first_table->next_global)
      query_tables_last = first_table->prev_global;

    if (query_tables_own_last == &first_table->next_global)
      query_tables_own_last = first_table->prev_global;

    if ((next = *first_table->prev_global = first_table->next_global))
      next->prev_global = first_table->prev_global;
    /* include in new place */
    first_table->next_global = query_tables;
    /*
       We are sure that query_tables is not 0, because first_table was not
       first table in the global list => we can use
       query_tables->prev_global without check of query_tables
    */
    query_tables->prev_global = &first_table->next_global;
    first_table->prev_global = &query_tables;
    query_tables = first_table;
  }
}

/*
  Link table back that was unlinked with unlink_first_table()

  SYNOPSIS
    link_first_table_back()
    link_to_local	do we need link this table to local

  RETURN
    global list
*/

void LEX::link_first_table_back(Table_ref *first, bool link_to_local) {
  if (first) {
    if ((first->next_global = query_tables))
      query_tables->prev_global = &first->next_global;
    else
      query_tables_last = &first->next_global;

    if (query_tables_own_last == &query_tables)
      query_tables_own_last = &first->next_global;

    query_tables = first;

    if (link_to_local) {
      first->next_local = query_block->m_table_list.first;
      query_block->context.table_list = first;
      query_block->m_table_list.first = first;
      query_block->m_table_list.elements++;  // safety
    }
  }
}

/*
  cleanup lex for case when we open table by table for processing

  SYNOPSIS
    LEX::cleanup_after_one_table_open()

  NOTE
    This method is mostly responsible for cleaning up of selects lists and
    derived tables state. To rollback changes in Query_tables_list one has
    to call Query_tables_list::reset_query_tables_list(false).
*/

void LEX::cleanup_after_one_table_open() {
  if (all_query_blocks_list != query_block) {
    /* cleunup underlying units (units of VIEW) */
    for (Query_expression *un = query_block->first_inner_query_expression(); un;
         un = un->next_query_expression()) {
      un->cleanup(true);
      un->destroy();
    }
    /* reduce all selects list to default state */
    all_query_blocks_list = query_block;
    /* remove underlying units (units of VIEW) subtree */
    query_block->cut_subtree();
  }
}

/*
  Save current state of Query_tables_list for this LEX, and prepare it
  for processing of new statemnt.

  SYNOPSIS
    reset_n_backup_query_tables_list()
      backup  Pointer to Query_tables_list instance to be used for backup
*/

void LEX::reset_n_backup_query_tables_list(Query_tables_list *backup) {
  backup->set_query_tables_list(this);
  /*
    We have to perform full initialization here since otherwise we
    will damage backed up state.
  */
  this->reset_query_tables_list(true);
}

/*
  Restore state of Query_tables_list for this LEX from backup.

  SYNOPSIS
    restore_backup_query_tables_list()
      backup  Pointer to Query_tables_list instance used for backup
*/

void LEX::restore_backup_query_tables_list(Query_tables_list *backup) {
  this->destroy_query_tables_list();
  this->set_query_tables_list(backup);
}

/*
  Checks for usage of routines and/or tables in a parsed statement

  SYNOPSIS
    LEX:table_or_sp_used()

  RETURN
    false  No routines and tables used
    true   Either or both routines and tables are used.
*/

bool LEX::table_or_sp_used() {
  DBUG_TRACE;

  if ((sroutines != nullptr && !sroutines->empty()) || query_tables)
    return true;

  return false;
}

/**
  Locate an assignment to a user variable with a given name, within statement.

  @param name Name of variable to search for

  @returns true if variable is assigned to, false otherwise.
*/

bool LEX::locate_var_assignment(const Name_string &name) {
  List_iterator<Item_func_set_user_var> li(set_var_list);
  Item_func_set_user_var *var;
  while ((var = li++)) {
    if (var->name.eq(name)) return true;
  }
  return false;
}

void Query_block::fix_prepare_information_for_order(
    THD *thd, SQL_I_List<ORDER> *list, Group_list_ptrs **list_ptrs) {
  Group_list_ptrs *p = *list_ptrs;
  if (p == nullptr) {
    void *mem = thd->stmt_arena->alloc(sizeof(Group_list_ptrs));
    *list_ptrs = p = new (mem) Group_list_ptrs(thd->stmt_arena->mem_root);
  }
  p->reserve(list->elements);
  for (ORDER *order = list->first; order; order = order->next)
    p->push_back(order);
}

/**
  Save properties for ORDER clauses so that they can be reconstructed
  for a new optimization of the query block.

  @param      thd       thread handler
  @param      list      list of ORDER elements to be saved
  @param[out] list_ptrs Saved list of ORDER elements

  @returns false if success, true if error (out of memory)
*/
bool Query_block::save_order_properties(THD *thd, SQL_I_List<ORDER> *list,
                                        Group_list_ptrs **list_ptrs) {
  assert(*list_ptrs == nullptr);
  void *mem = thd->stmt_arena->alloc(sizeof(Group_list_ptrs));
  if (mem == nullptr) return true;
  Group_list_ptrs *p = new (mem) Group_list_ptrs(thd->stmt_arena->mem_root);
  if (p == nullptr) return true;
  *list_ptrs = p;
  if (p->reserve(list->elements)) return true;
  for (ORDER *order = list->first; order; order = order->next)
    if (p->push_back(order)) return true;
  return false;
}

/**
  Save properties of a prepared statement needed for repeated optimization.
  Saves the chain of ORDER::next in group_list and order_list, in
  case the list is modified by remove_const().

  @param thd          thread handler

  @returns false if success, true if error (out of memory)
*/
bool Query_block::save_properties(THD *thd) {
  assert(first_execution);
  first_execution = false;
  assert(!thd->stmt_arena->is_regular());
  if (thd->stmt_arena->is_regular()) return false;

  saved_cond_count = cond_count;

  if (group_list.first &&
      save_order_properties(thd, &group_list, &group_list_ptrs))
    return true;
  if (order_list.first &&
      save_order_properties(thd, &order_list, &order_list_ptrs))
    return true;
  return false;
}

static enum_explain_type setop2result(Query_term *qt) {
  switch (qt->term_type()) {
    case QT_UNION:
      return enum_explain_type::EXPLAIN_UNION_RESULT;
    case QT_INTERSECT:
      return enum_explain_type::EXPLAIN_INTERSECT_RESULT;
    case QT_EXCEPT:
      return enum_explain_type::EXPLAIN_EXCEPT_RESULT;
    case QT_UNARY:
      return enum_explain_type::EXPLAIN_UNARY_RESULT;
    default:
      assert(false);
  }
  return enum_explain_type::EXPLAIN_UNION_RESULT;
}

/*
  There are Query_block::add_table_to_list &
  Query_block::set_lock_for_tables are in sql_parse.cc

  Query_block::print is in sql_select.cc

  Query_expression::prepare, Query_expression::exec,
  Query_expression::cleanup, Query_expression::change_query_result
  are in sql_union.cc
*/

enum_explain_type Query_block::type() const {
  Query_term *qt = master_query_expression()->find_blocks_query_term(this);
  if (qt->term_type() != QT_QUERY_BLOCK) {
    return setop2result(qt);
  } else if (!master_query_expression()->outer_query_block() &&
             master_query_expression()->first_query_block() == this) {
    if (first_inner_query_expression() || next_query_block() ||
        m_parent != nullptr)
      return enum_explain_type::EXPLAIN_PRIMARY;
    else
      return enum_explain_type::EXPLAIN_SIMPLE;
  } else if (this == master_query_expression()->first_query_block()) {
    if (linkage == DERIVED_TABLE_TYPE)
      return enum_explain_type::EXPLAIN_DERIVED;
    else
      return enum_explain_type::EXPLAIN_SUBQUERY;
  } else {
    assert(m_parent != nullptr);
    // if left child, call block PRIMARY, else UNION/INTERSECT/EXCEPT
    switch (m_parent->term_type()) {
      case QT_EXCEPT:
        if (m_parent->m_children[0] == this)
          return enum_explain_type::EXPLAIN_PRIMARY;
        else
          return enum_explain_type::EXPLAIN_EXCEPT;
      case QT_UNION:
        if (m_parent->m_children[0] == this)
          return enum_explain_type::EXPLAIN_PRIMARY;
        else
          return enum_explain_type::EXPLAIN_UNION;
      case QT_INTERSECT:
        if (m_parent->m_children[0] == this)
          return enum_explain_type::EXPLAIN_PRIMARY;
        else
          return enum_explain_type::EXPLAIN_INTERSECT;
      case QT_UNARY:
        return enum_explain_type::EXPLAIN_PRIMARY;
      default:
        assert(false);
    }
    return enum_explain_type::EXPLAIN_UNION;
  }
}

/**
  Add this query block below the specified query expression.

  @param lex   Containing LEX object
  @param outer Query expression that query block is added to.

  @note that this query block can never have any underlying query expressions,
        hence it is not necessary to e.g. renumber those, like e.g.
        Query_expression::include_down() does.
*/
void Query_block::include_down(LEX *lex, Query_expression *outer) {
  assert(slave == nullptr);
  next = outer->slave;
  outer->slave = this;
  master = outer;

  select_number = ++lex->select_number;

  nest_level =
      outer_query_block() == nullptr ? 0 : outer_query_block()->nest_level + 1;
}

/**
  Add this query block after the specified query block.

  @param lex    Containing LEX object
  @param before Query block that this object is added after.
*/
void Query_block::include_neighbour(LEX *lex, Query_block *before) {
  next = before->next;
  before->next = this;
  master = before->master;

  select_number = ++lex->select_number;
  nest_level = before->nest_level;
}

/**
  Include query block within the supplied unit.

  Do not link the query block into the global chain of query blocks.

  This function is exclusive for Query_expression::add_fake_query_block() -
  use it with caution.

  @param  outer Query expression this node is included below.
*/
void Query_block::include_standalone(Query_expression *outer) {
  next = nullptr;
  master = outer;
  nest_level = master->first_query_block()->nest_level;
}

/**
  Renumber query_block object, and apply renumbering recursively to
  contained objects.

  @param  lex   Containing LEX object
*/
void Query_block::renumber(LEX *lex) {
  select_number = ++lex->select_number;

  nest_level =
      outer_query_block() == nullptr ? 0 : outer_query_block()->nest_level + 1;

  for (Query_expression *u = first_inner_query_expression(); u;
       u = u->next_query_expression())
    u->renumber_selects(lex);
}

/**
  Include query block into global list.

  @param plink - Pointer to start of list
*/
void Query_block::include_in_global(Query_block **plink) {
  if ((link_next = *plink)) link_next->link_prev = &link_next;
  link_prev = plink;
  *plink = this;
}

/**
  Include chain of query blocks into global list.

  @param start - Pointer to start of list
*/
void Query_block::include_chain_in_global(Query_block **start) {
  Query_block *last_query_block;
  for (last_query_block = this; last_query_block->link_next != nullptr;
       last_query_block = last_query_block->link_next) {
  }
  last_query_block->link_next = *start;
  last_query_block->link_next->link_prev = &last_query_block->link_next;
  link_prev = start;
  *start = this;
}

/**
   Helper function which handles the "ON conditions" part of
   Query_block::get_optimizable_conditions().
   @returns true if OOM
*/
static bool get_optimizable_join_conditions(
    THD *thd, mem_root_deque<Table_ref *> &join_list) {
  for (Table_ref *table : join_list) {
    NESTED_JOIN *const nested_join = table->nested_join;
    if (nested_join &&
        get_optimizable_join_conditions(thd, nested_join->m_tables))
      return true;
    Item *const jc = table->join_cond();
    if (jc && !thd->stmt_arena->is_regular()) {
      table->set_join_cond_optim(jc->copy_andor_structure(thd));
      if (!table->join_cond_optim()) return true;
    } else
      table->set_join_cond_optim(jc);
  }
  return false;
}

/**
   Returns disposable copies of WHERE/HAVING/ON conditions.

   This function returns a copy which can be thrashed during
   this execution of the statement. Only AND/OR items are trashable!
   If in conventional execution, no copy is created, the permanent clauses are
   returned instead, as trashing them is no problem.

   @param      thd        thread handle
   @param[out] new_where  copy of WHERE
   @param[out] new_having copy of HAVING (if passed pointer is not NULL)

   Copies of join (ON) conditions are placed in
   Table_ref::m_join_cond_optim.

   @returns true if OOM
*/
bool Query_block::get_optimizable_conditions(THD *thd, Item **new_where,
                                             Item **new_having) {
  /*
    We want to guarantee that
    join->optimized is true => conditions are ready for reading.
    So if we are here, this should hold:
  */
  assert(!(join && join->is_optimized()));
  if (m_where_cond && !thd->stmt_arena->is_regular()) {
    *new_where = m_where_cond->copy_andor_structure(thd);
    if (!*new_where) return true;
  } else
    *new_where = m_where_cond;
  if (new_having) {
    if (m_having_cond && !thd->stmt_arena->is_regular()) {
      *new_having = m_having_cond->copy_andor_structure(thd);
      if (!*new_having) return true;
    } else
      *new_having = m_having_cond;
  }
  return get_optimizable_join_conditions(thd, m_table_nest);
}

bool Query_block::get_optimizable_connect_by(THD *thd, Item **start_with,
                                             Item **new_connect_by,
                                             Item **new_where) {
  assert(!(join && join->is_optimized()));

  if (m_start_with_cond && !thd->stmt_arena->is_regular()) {
    *start_with = m_start_with_cond->copy_andor_structure(thd);
    if (!*start_with) return true;
  } else {
    *start_with = m_start_with_cond;
  }
  if (m_connect_by_cond && !thd->stmt_arena->is_regular()) {
    *new_connect_by = m_connect_by_cond->copy_andor_structure(thd);
    if (!*new_connect_by) return true;
  } else {
    *new_connect_by = m_connect_by_cond;
  }

  if (m_after_connect_by_where && !thd->stmt_arena->is_regular()) {
    *new_where = m_after_connect_by_where->copy_andor_structure(thd);
    if (!*new_where) return true;
  } else {
    *new_where = m_after_connect_by_where;
  }
  return false;
}

Subquery_strategy Query_block::subquery_strategy(const THD *thd) const {
  if (m_windows.elements > 0)
    /*
      A window function is in the SELECT list.
      In-to-exists could not work: it would attach an equality like
      outer_expr = WF to either WHERE or HAVING; but a WF is not allowed in
      those clauses, and even if we allowed it, it would modify the result
      rows over which the WF is supposed to be calculated.
      So, subquery materialization is imposed. Grep for (and read) WL#10431.
    */
    return Subquery_strategy::SUBQ_MATERIALIZATION;

  if (opt_hints_qb) {
    Subquery_strategy strategy = opt_hints_qb->subquery_strategy();
    if (strategy != Subquery_strategy::UNSPECIFIED) return strategy;
  }

  // No SUBQUERY hint given, base possible strategies on optimizer_switch
  if (thd->optimizer_switch_flag(OPTIMIZER_SWITCH_MATERIALIZATION))
    return thd->optimizer_switch_flag(OPTIMIZER_SWITCH_SUBQ_MAT_COST_BASED)
               ? Subquery_strategy::CANDIDATE_FOR_IN2EXISTS_OR_MAT
               : Subquery_strategy::SUBQ_MATERIALIZATION;

  return Subquery_strategy::SUBQ_EXISTS;
}

bool Query_block::semijoin_enabled(const THD *thd) const {
  return opt_hints_qb ? opt_hints_qb->semijoin_enabled(thd)
                      : thd->optimizer_switch_flag(OPTIMIZER_SWITCH_SEMIJOIN);
}

void Query_block::update_semijoin_strategies(THD *thd) {
  uint sj_strategy_mask =
      OPTIMIZER_SWITCH_FIRSTMATCH | OPTIMIZER_SWITCH_LOOSE_SCAN |
      OPTIMIZER_SWITCH_MATERIALIZATION | OPTIMIZER_SWITCH_DUPSWEEDOUT;

  uint opt_switches = thd->variables.optimizer_switch & sj_strategy_mask;

  bool is_secondary_engine_optimization =
      parent_lex->m_sql_cmd != nullptr &&
      parent_lex->m_sql_cmd->using_secondary_storage_engine();

  for (Table_ref *sj_nest : sj_nests) {
    /*
      After semi-join transformation, original Query_block with hints is lost.
      Fetch hints from last table in semijoin nest, as join_list has the
      convention to list join operators' arguments in reverse order.
    */
    Table_ref *table = sj_nest->nested_join->m_tables.back();
    /*
      Do not respect opt_hints_qb for secondary engine optimization.
      Secondary storage engines may not support all strategies that are
      supported by the MySQL executor. Secondary engines should set their
      supported semi-join strategies in thd->variables.optimizer_switch and not
      respect optimizer hints or optimizer switches specified by the user.
    */
    sj_nest->nested_join->sj_enabled_strategies =
        (table->opt_hints_qb && !is_secondary_engine_optimization)
            ? table->opt_hints_qb->sj_enabled_strategies(opt_switches)
            : opt_switches;
    if (sj_nest->is_aj_nest()) {
      // only these are possible with NOT EXISTS/IN:
      sj_nest->nested_join->sj_enabled_strategies &=
          OPTIMIZER_SWITCH_FIRSTMATCH | OPTIMIZER_SWITCH_MATERIALIZATION |
          OPTIMIZER_SWITCH_DUPSWEEDOUT;
    }
  }
}

/**
  Check if an option that can be used only for an outer-most query block is
  applicable to this query block.

  @param lex    LEX of current statement
  @param option option name to output within the error message

  @returns      false if valid, true if invalid, error is sent to client
*/

bool Query_block::validate_outermost_option(LEX *lex,
                                            const char *option) const {
  if (this != lex->query_block) {
    my_error(ER_CANT_USE_OPTION_HERE, MYF(0), option);
    return true;
  }
  return false;
}

/**
  Validate base options for a query block.

  @param lex                LEX of current statement
  @param options_arg        base options for a SELECT statement.

  @returns false if success, true if validation failed

  These options are supported, per DML statement:

  SELECT: SELECT_STRAIGHT_JOIN
          SELECT_HIGH_PRIORITY
          SELECT_DISTINCT
          SELECT_ALL
          SELECT_SMALL_RESULT
          SELECT_BIG_RESULT
          OPTION_BUFFER_RESULT
          OPTION_FOUND_ROWS
          OPTION_SELECT_FOR_SHOW
  DELETE: OPTION_QUICK
          LOW_PRIORITY
  INSERT: LOW_PRIORITY
          HIGH_PRIORITY
  UPDATE: LOW_PRIORITY

  Note that validation is only performed for SELECT statements.
*/

bool Query_block::validate_base_options(LEX *lex, ulonglong options_arg) const {
  assert(!(options_arg & ~(SELECT_STRAIGHT_JOIN | SELECT_HIGH_PRIORITY |
                           SELECT_DISTINCT | SELECT_ALL | SELECT_SMALL_RESULT |
                           SELECT_BIG_RESULT | OPTION_BUFFER_RESULT |
                           OPTION_FOUND_ROWS | OPTION_SELECT_FOR_SHOW)));

  if (options_arg & SELECT_DISTINCT && options_arg & SELECT_ALL) {
    my_error(ER_WRONG_USAGE, MYF(0), "ALL", "DISTINCT");
    return true;
  }
  if (options_arg & SELECT_HIGH_PRIORITY &&
      validate_outermost_option(lex, "HIGH_PRIORITY"))
    return true;
  if (options_arg & OPTION_BUFFER_RESULT &&
      validate_outermost_option(lex, "SQL_BUFFER_RESULT"))
    return true;
  if (options_arg & OPTION_FOUND_ROWS &&
      validate_outermost_option(lex, "SQL_CALC_FOUND_ROWS"))
    return true;

  return false;
}

/**
  Apply walk() processor to join conditions.

  JOINs may be nested. Walk nested joins recursively to apply the
  processor.
*/
static bool walk_join_condition(mem_root_deque<Table_ref *> *tables,
                                Item_processor processor, enum_walk walk,
                                uchar *arg) {
  for (const Table_ref *table : *tables) {
    if (table->join_cond() && table->join_cond()->walk(processor, walk, arg))
      return true;

    if (table->nested_join != nullptr &&
        walk_join_condition(&table->nested_join->m_tables, processor, walk,
                            arg))
      return true;
  }
  return false;
}

void Query_expression::accumulate_used_tables(table_map map) {
  assert(outer_query_block());
  if (item)
    item->accumulate_used_tables(map);
  else if (m_lateral_deps)
    m_lateral_deps |= map;
}

enum_parsing_context Query_expression::place() const {
  assert(outer_query_block());
  if (item != nullptr) return item->place();
  return CTX_DERIVED;
}

bool Query_block::walk(Item_processor processor, enum_walk walk, uchar *arg) {
  for (Item *item : visible_fields()) {
    if (item->walk(processor, walk, arg)) return true;
  }

  if (m_current_table_nest != nullptr &&
      walk_join_condition(m_current_table_nest, processor, walk, arg))
    return true;

  if ((walk & enum_walk::SUBQUERY)) {
    /*
      for each leaf: if a materialized table, walk the unit
    */
    for (Table_ref *tbl = leaf_tables; tbl; tbl = tbl->next_leaf) {
      if (!tbl->uses_materialization()) continue;
      if (tbl->is_derived()) {
        if (tbl->derived_query_expression()->walk(processor, walk, arg))
          return true;
      } else if (tbl->is_table_function()) {
        if (tbl->table_function->walk(processor, walk, arg)) return true;
      }
    }
  }

  // @todo: Roy thinks that we should always use where_cond.
  Item *const where_cond =
      (join && join->is_optimized()) ? join->where_cond : this->where_cond();

  if (where_cond && where_cond->walk(processor, walk, arg)) return true;

  for (auto order = group_list.first; order; order = order->next) {
    if ((*order->item)->walk(processor, walk, arg)) return true;
  }

  if (having_cond() && having_cond()->walk(processor, walk, arg)) return true;

  for (auto order = order_list.first; order; order = order->next) {
    if ((*order->item)->walk(processor, walk, arg)) return true;
  }

  // walk windows' ORDER BY and PARTITION BY clauses.
  List_iterator<Window> liw(m_windows);
  for (Window *w = liw++; w != nullptr; w = liw++) {
    /*
      We use first_order_by() instead of order() because if a window
      references another window and they thus share the same ORDER BY,
      we want to walk that clause only once here
      (Same for partition as well)".
    */
    for (auto it : {w->first_partition_by(), w->first_order_by()}) {
      if (it != nullptr) {
        for (ORDER *o = it; o != nullptr; o = o->next) {
          if ((*o->item)->walk(processor, walk, arg)) return true;
        }
      }
    }
  }
  return false;
}

/**
  Finds a (possibly unresolved) table reference in the from clause by name.

  There is a hack in the parser which adorns table references with the current
  database. This function piggy-backs on that hack to find fully qualified
  table references without having to resolve the name.

  @param ident The table name, may be qualified or unqualified.

  @retval NULL If not found.
*/
Table_ref *Query_block::find_table_by_name(const Table_ident *ident) {
  LEX_CSTRING db_name = ident->db;
  LEX_CSTRING table_name = ident->table;

  for (Table_ref *table = m_table_list.first; table;
       table = table->next_local) {
    if ((db_name.length == 0 || strcmp(db_name.str, table->db) == 0) &&
        strcmp(table_name.str, table->alias) == 0)
      return table;
  }
  return nullptr;
}

/**
  Save prepared statement properties for a query block and underlying
  query expressions. Required for repeated optimizations of the command.

  @param thd     thread handler

  @returns false if success, true if error (out of memory)
*/
bool Query_block::save_cmd_properties(THD *thd) {
  for (Query_expression *u = first_inner_query_expression(); u;
       u = u->next_query_expression())
    if (u->save_cmd_properties(thd)) return true;

  if (save_properties(thd)) return true;

  for (Table_ref *tbl = leaf_tables; tbl; tbl = tbl->next_leaf) {
    if (!tbl->is_base_table()) continue;
    if (tbl->save_properties()) return true;
  }
  return false;
}

/**
  Restore prepared statement properties for this query block and all
  underlying query expressions so they are ready for optimization.
  Restores properties saved in Table_ref objects into corresponding
  TABLEs. Restores ORDER BY and GROUP by clauses, and window definitions, so
  they are ready for optimization.
*/
void Query_block::restore_cmd_properties() {
  for (Query_expression *u = first_inner_query_expression(); u;
       u = u->next_query_expression())
    u->restore_cmd_properties();

  for (Table_ref *tbl = leaf_tables; tbl; tbl = tbl->next_leaf) {
    if (!tbl->is_base_table()) continue;
    tbl->restore_properties();
    tbl->table->m_record_buffer = Record_buffer{0, 0, nullptr};
  }
  assert(join == nullptr);

  cond_count = saved_cond_count;

  // Restore GROUP BY list
  if (group_list_ptrs && group_list_ptrs->size() > 0) {
    for (uint ix = 0; ix < group_list_ptrs->size() - 1; ++ix) {
      ORDER *order = group_list_ptrs->at(ix);
      order->next = group_list_ptrs->at(ix + 1);
    }
  }
  // Restore ORDER BY list
  if (order_list_ptrs && order_list_ptrs->size() > 0) {
    for (uint ix = 0; ix < order_list_ptrs->size() - 1; ++ix) {
      ORDER *order = order_list_ptrs->at(ix);
      order->next = order_list_ptrs->at(ix + 1);
    }
  }
  if (m_windows.elements > 0) {
    List_iterator<Window> li(m_windows);
    Window *w;
    while ((w = li++)) w->reset_round();
  }
}

bool Query_options::merge(const Query_options &a, const Query_options &b) {
  query_spec_options = a.query_spec_options | b.query_spec_options;
  return false;
}

bool Query_options::save_to(Parse_context *pc) {
  LEX *lex = pc->thd->lex;
  ulonglong options = query_spec_options;
  if (pc->select->validate_base_options(lex, options)) return true;
  pc->select->set_base_options(options);

  return false;
}

bool LEX::accept(Select_lex_visitor *visitor) {
  return m_sql_cmd->accept(thd, visitor);
}

bool LEX::set_wild(LEX_STRING w) {
  if (w.str == nullptr) {
    wild = nullptr;
    return false;
  }
  wild = new (thd->mem_root) String(w.str, w.length, system_charset_info);
  return wild == nullptr;
}

void LEX_MASTER_INFO::initialize() {
  host = user = password = log_file_name = bind_addr = nullptr;
  network_namespace = nullptr;
  port = connect_retry = 0;
  heartbeat_period = 0;
  sql_delay = 0;
  pos = 0;
  server_id = retry_count = 0;
  gtid = nullptr;
  gtid_until_condition = UNTIL_SQL_BEFORE_GTIDS;
  view_id = nullptr;
  until_after_gaps = false;
  ssl = ssl_verify_server_cert = heartbeat_opt = repl_ignore_server_ids_opt =
      retry_count_opt = auto_position = port_opt = get_public_key =
          m_source_connection_auto_failover = m_gtid_only = LEX_MI_UNCHANGED;
  ssl_key = ssl_cert = ssl_ca = ssl_capath = ssl_cipher = nullptr;
  ssl_crl = ssl_crlpath = nullptr;
  public_key_path = nullptr;
  tls_version = nullptr;
  tls_ciphersuites = UNSPECIFIED;
  tls_ciphersuites_string = nullptr;
  relay_log_name = nullptr;
  relay_log_pos = 0;
  repl_ignore_server_ids.clear();
  channel = nullptr;
  for_channel = false;
  compression_algorithm = nullptr;
  zstd_compression_level = 0;
  privilege_checks_none = false;
  privilege_checks_username = privilege_checks_hostname = nullptr;
  require_row_format = LEX_MI_UNCHANGED;
  require_table_primary_key_check = LEX_MI_PK_CHECK_UNCHANGED;
  assign_gtids_to_anonymous_transactions_type =
      LEX_MI_ANONYMOUS_TO_GTID_UNCHANGED;
  assign_gtids_to_anonymous_transactions_manual_uuid = nullptr;
}

void LEX_MASTER_INFO::set_unspecified() {
  initialize();
  sql_delay = -1;
}

uint binlog_unsafe_map[256];

#define UNSAFE(a, b, c)                                  \
  {                                                      \
    DBUG_PRINT("unsafe_mixed_statement",                 \
               ("SETTING BASE VALUES: %s, %s, %02X\n",   \
                LEX::stmt_accessed_table_string(a),      \
                LEX::stmt_accessed_table_string(b), c)); \
    unsafe_mixed_statement(a, b, c);                     \
  }

/*
  Sets the combination given by "a" and "b" and automatically combinations
  given by other types of access, i.e. 2^(8 - 2), as unsafe.

  It may happen a collision when automatically defining a combination as unsafe.
  For that reason, a combination has its unsafe condition redefined only when
  the new_condition is greater then the old. For instance,

     . (BINLOG_DIRECT_ON & TRX_CACHE_NOT_EMPTY) is never overwritten by
     . (BINLOG_DIRECT_ON | BINLOG_DIRECT_OFF).
*/
static void unsafe_mixed_statement(LEX::enum_stmt_accessed_table a,
                                   LEX::enum_stmt_accessed_table b,
                                   uint condition) {
  int type = 0;
  int index = (1U << a) | (1U << b);

  for (type = 0; type < 256; type++) {
    if ((type & index) == index) {
      binlog_unsafe_map[type] |= condition;
    }
  }
}

/**
  Uses parse_tree to instantiate an Sql_cmd object and assigns it to the Lex.

  @param parse_tree The parse tree.

  @returns false on success, true on error.
*/
bool LEX::make_sql_cmd(Parse_tree_root *parse_tree) {
  if (!will_contextualize) return false;

  m_sql_cmd = parse_tree->make_cmd(thd);
  if (m_sql_cmd == nullptr) return true;

  assert(m_sql_cmd->sql_command_code() == sql_command);

  return false;
}

/**
  Set replication channel name

  @param name   If @p name is null string then reset channel name to default.
                Otherwise set it to @p name.

  @returns false if success, true if error (OOM).
*/
bool LEX::set_channel_name(LEX_CSTRING name) {
  if (name.str == nullptr) {
    mi.channel = "";
    mi.for_channel = false;
  } else {
    /*
      Channel names are case insensitive. This means, even the results
      displayed to the user are converted to lower cases.
      system_charset_info is utf8mb3_general_ci as required by channel name
      restrictions
    */
    char *buf = thd->strmake(name.str, name.length);
    if (buf == nullptr) return true;  // OOM
    my_casedn_str(system_charset_info, buf);
    mi.channel = buf;
    mi.for_channel = true;
  }
  return false;
}

/**
  Find a local or a package body variable by name.
  @param IN  name    - the variable name
  @param OUT ctx     - NULL, if the variable was not found,
                       or LEX::spcont (if a local variable was found)
                       or the package top level context
                       (if a package variable was found)
  @param OUT handler - NULL, if the variable was not found,
                       or a pointer to rcontext handler
  @retval            - the variable (if found), or NULL otherwise.
*/
sp_variable *LEX::find_variable(const char *name, size_t name_len,
                                sp_pcontext **ctx,
                                const Sp_rcontext_handler **rh) const {
  sp_variable *spv;
  sp_pcontext *spc = get_sp_current_parsing_ctx();

  if (spc && ((spv = spc->find_variable(name, name_len, false)) ||
              (spv = spc->find_udt_variable(name, name_len, sphead->m_db.str,
                                            sphead->m_db.length, false)))) {
    *ctx = spc;
    *rh = &sp_rcontext_handler_local;
    return spv;
  }
  sp_package *pkg = sphead ? sphead->m_parent : NULL;
  if (pkg && (spv = pkg->find_package_variable(name, name_len))) {
    *ctx = pkg->get_root_parsing_context()->child_context(0);
    *rh = &sp_rcontext_handler_package_body;
    return spv;
  }
  *ctx = NULL;
  *rh = NULL;
  return NULL;
}

sp_package *LEX::get_sp_package() const {
  return sphead ? sphead->get_package() : NULL;
}

/*
  The BINLOG_* AND TRX_CACHE_* values can be combined by using '&' or '|',
  which means that both conditions need to be satisfied or any of them is
  enough. For example,

    . BINLOG_DIRECT_ON & TRX_CACHE_NOT_EMPTY means that the statement is
    unsafe when the option is on and trx-cache is not empty;

    . BINLOG_DIRECT_ON | BINLOG_DIRECT_OFF means the statement is unsafe
    in all cases.

    . TRX_CACHE_EMPTY | TRX_CACHE_NOT_EMPTY means the statement is unsafe
    in all cases. Similar as above.
*/
void binlog_unsafe_map_init() {
  memset((void *)binlog_unsafe_map, 0, sizeof(uint) * 256);

  /*
    Classify a statement as unsafe when there is a mixed statement and an
    on-going transaction at any point of the execution if:

      1. The mixed statement is about to update a transactional table and
      a non-transactional table.

      2. The mixed statement is about to update a transactional table and
      read from a non-transactional table.

      3. The mixed statement is about to update a non-transactional table
      and temporary transactional table.

      4. The mixed statement is about to update a temporary transactional
      table and read from a non-transactional table.

      5. The mixed statement is about to update a transactional table and
      a temporary non-transactional table.

      6. The mixed statement is about to update a transactional table and
      read from a temporary non-transactional table.

      7. The mixed statement is about to update a temporary transactional
      table and temporary non-transactional table.

      8. The mixed statement is about to update a temporary transactional
      table and read from a temporary non-transactional table.
    After updating a transactional table if:

      9. The mixed statement is about to update a non-transactional table
      and read from a transactional table.

      10. The mixed statement is about to update a non-transactional table
      and read from a temporary transactional table.

      11. The mixed statement is about to update a temporary non-transactional
      table and read from a transactional table.

      12. The mixed statement is about to update a temporary non-transactional
      table and read from a temporary transactional table.

      13. The mixed statement is about to update a temporary non-transactional
      table and read from a non-transactional table.

    The reason for this is that locks acquired may not protected a concurrent
    transaction of interfering in the current execution and by consequence in
    the result.
  */
  /* Case 1. */
  UNSAFE(LEX::STMT_WRITES_TRANS_TABLE, LEX::STMT_WRITES_NON_TRANS_TABLE,
         BINLOG_DIRECT_ON | BINLOG_DIRECT_OFF);
  /* Case 2. */
  UNSAFE(LEX::STMT_WRITES_TRANS_TABLE, LEX::STMT_READS_NON_TRANS_TABLE,
         BINLOG_DIRECT_ON | BINLOG_DIRECT_OFF);
  /* Case 3. */
  UNSAFE(LEX::STMT_WRITES_NON_TRANS_TABLE, LEX::STMT_WRITES_TEMP_TRANS_TABLE,
         BINLOG_DIRECT_ON | BINLOG_DIRECT_OFF);
  /* Case 4. */
  UNSAFE(LEX::STMT_WRITES_TEMP_TRANS_TABLE, LEX::STMT_READS_NON_TRANS_TABLE,
         BINLOG_DIRECT_ON | BINLOG_DIRECT_OFF);
  /* Case 5. */
  UNSAFE(LEX::STMT_WRITES_TRANS_TABLE, LEX::STMT_WRITES_TEMP_NON_TRANS_TABLE,
         BINLOG_DIRECT_ON);
  /* Case 6. */
  UNSAFE(LEX::STMT_WRITES_TRANS_TABLE, LEX::STMT_READS_TEMP_NON_TRANS_TABLE,
         BINLOG_DIRECT_ON);
  /* Case 7. */
  UNSAFE(LEX::STMT_WRITES_TEMP_TRANS_TABLE,
         LEX::STMT_WRITES_TEMP_NON_TRANS_TABLE, BINLOG_DIRECT_ON);
  /* Case 8. */
  UNSAFE(LEX::STMT_WRITES_TEMP_TRANS_TABLE,
         LEX::STMT_READS_TEMP_NON_TRANS_TABLE, BINLOG_DIRECT_ON);
  /* Case 9. */
  UNSAFE(LEX::STMT_WRITES_NON_TRANS_TABLE, LEX::STMT_READS_TRANS_TABLE,
         (BINLOG_DIRECT_ON | BINLOG_DIRECT_OFF) & TRX_CACHE_NOT_EMPTY);
  /* Case 10 */
  UNSAFE(LEX::STMT_WRITES_NON_TRANS_TABLE, LEX::STMT_READS_TEMP_TRANS_TABLE,
         (BINLOG_DIRECT_ON | BINLOG_DIRECT_OFF) & TRX_CACHE_NOT_EMPTY);
  /* Case 11. */
  UNSAFE(LEX::STMT_WRITES_TEMP_NON_TRANS_TABLE, LEX::STMT_READS_TRANS_TABLE,
         BINLOG_DIRECT_ON & TRX_CACHE_NOT_EMPTY);
  /* Case 12. */
  UNSAFE(LEX::STMT_WRITES_TEMP_NON_TRANS_TABLE,
         LEX::STMT_READS_TEMP_TRANS_TABLE,
         BINLOG_DIRECT_ON & TRX_CACHE_NOT_EMPTY);
  /* Case 13. */
  UNSAFE(LEX::STMT_WRITES_TEMP_NON_TRANS_TABLE, LEX::STMT_READS_NON_TRANS_TABLE,
         BINLOG_DIRECT_OFF & TRX_CACHE_NOT_EMPTY);
}

void LEX::set_secondary_engine_execution_context(
    Secondary_engine_execution_context *context) {
  assert(m_secondary_engine_context == nullptr || context == nullptr);
  ::destroy(m_secondary_engine_context);
  m_secondary_engine_context = context;
}

void LEX_GRANT_AS::cleanup() {
  grant_as_used = false;
  role_type = role_enum::ROLE_NONE;
  user = nullptr;
  role_list = nullptr;
}

LEX_GRANT_AS::LEX_GRANT_AS() { cleanup(); }

/**oracle sp start **/

bool LEX::sp_block_begin_label(THD *thd, LEX_CSTRING lab_name) {
  sp_label *lab;
  if (lab_name.length) {
    lab = sp_current_parsing_ctx->find_label(lab_name);
    if (lab) {
      my_error(ER_SP_LABEL_REDEFINE, MYF(0), lab_name.str);
      return true;
    }
  }
  lab =
      sp_current_parsing_ctx->push_label(thd, lab_name, sphead->instructions());
  lab->type = sp_label::BEGIN;
  return false;
}

bool LEX::sp_block_end_label(THD *, LEX_CSTRING lab_name) {
  auto lab = sp_current_parsing_ctx->pop_label();
  if (lab_name.length && lab_name.str) {
    if (my_strcasecmp(system_charset_info, lab_name.str, lab->name.str) != 0) {
      my_error(ER_SP_LABEL_MISMATCH, MYF(0), lab_name.str);
      return true;
    }
  }
  return false;
}

bool LEX::sp_block_init(THD *thd) {
  auto child_pctx =
      sp_current_parsing_ctx->push_context(thd, sp_pcontext::REGULAR_SCOPE);
  if (!child_pctx) return true;
  set_sp_current_parsing_ctx(child_pctx);
  return false;
}

bool LEX::sp_block_finalize(THD *thd, int hndlrs, int curs) {
  auto sp = sphead;
  auto pctx = sp_current_parsing_ctx;
  sp->m_parser_data.do_backpatch(pctx->last_label(), sp->instructions());
  if (hndlrs > 0) {
    auto i = new (thd->mem_root) sp_instr_hpop(sp->instructions(), pctx);
    if (!i || sp->add_instr(thd, i)) return true;
  }
  if (curs > 0) {
    auto i = new (thd->mem_root) sp_instr_cpop(sp->instructions(), pctx, curs);
    if (!i || sp->add_instr(thd, i)) return true;
  }
  set_sp_current_parsing_ctx(pctx->pop_context());
  return false;
}

bool LEX::sp_exception_declarations(THD *thd) {
  auto sp = sphead;
  auto pctx = sp_current_parsing_ctx;

  auto i = new (thd->mem_root) sp_instr_jump(sp->instructions(), pctx);
  if (!i || sp->add_instr(thd, i)) return true;
  if (i->m_arena.item_list()) {
    // can't save item in the special instr , give it to next instr
    thd->set_item_list(i->m_arena.item_list());
    i->m_arena.reset_item_list();
  }

  return false;
}

bool LEX::sp_exception_section(THD *thd, int pos) {
  auto sp = sphead;
  auto pctx = sp_current_parsing_ctx;

  // jump  over exception section
  auto i = new (thd->mem_root) sp_instr_jump(sp->instructions(), pctx);
  if (!i || sp->add_instr(thd, i) ||
      sp->m_parser_data.add_backpatch_entry(i, pctx->last_label()))
    return true;

  // jump  declare exception section before sp_proc_stmts
  // sp_branch_instr
  auto instr = static_cast<sp_instr_jump *>(sp->get_instr(pos - 1));

  assert(instr);
  instr->backpatch(sp->instructions());
  return false;
}

bool LEX::sp_exception_finalize(THD *thd, int pos_begin, int exception_num) {
  sp_head *sp = sphead;
  if (exception_num) {
    // jump back to sp_proc_stmts
    sp_instr_jump *i = new (thd->mem_root)
        sp_instr_jump(sp->instructions(), sp_current_parsing_ctx, pos_begin);
    if (!i || sp->add_instr(thd, i)) return true;

  } else {
    // let jump self , removed during sp_head::optimize().
    auto instr = static_cast<sp_instr_jump *>(sp->get_instr(pos_begin - 1));
    assert(instr);
    instr->set_destination(instr->get_destination(), instr->get_ip() + 1);
  }

  return false;
}

/*
  Add declarations for table column and SP variable anchor types:
  - DECLARE spvar1 db1.table1.column1%TYPE;
  - DECLARE spvar1 table1.column1%TYPE;
  - DECLARE spvar1 spvar0%TYPE;
*/
bool LEX::sp_variable_declarations_with_ref_finalize(
    THD *thd, int nvars, Qualified_column_ident *ref) {
  return ref->db.length == 0 && ref->table.length == 0
             ? sp_variable_declarations_vartype_finalize(thd, nvars,
                                                         ref->m_column)
             : sp_variable_declarations_column_type_finalize(thd, nvars, ref);
}

bool LEX::sp_variable_declarations_vartype_finalize(THD *thd, int nvars,
                                                    const LEX_CSTRING &ref) {
  LEX *lex = thd->lex;
  sp_pcontext *pctx = lex->get_sp_current_parsing_ctx();
  sp_variable *spv;

  if (!pctx || !(spv = pctx->find_variable(ref.str, ref.length, false))) {
    my_error(ER_SP_UNDECLARED_VAR, MYF(0), ref.str);
    return true;
  }
  if (spv->field_def.ora_record.is_ref_cursor) {
    my_error(ER_NOT_SUPPORTED_YET, MYF(0), "the use likes ref_cursor%type");
    return true;
  }
  /*case of DECLARE spvar1 spvar0%TYPE; spvar0 cursor%ROWTYPE*/
  if (spv->field_def.ora_record.is_cursor_rowtype_ref()) {
    uint offset = spv->field_def.ora_record.cursor_rowtype_offset();
    return sp_variable_declarations_cursor_rowtype_finalize(thd, nvars, offset,
                                                            nullptr, nullptr);
  }

  /*case of DECLARE spvar1 spvar0%TYPE; spvar0 table.col%TYPE*/
  if (spv->field_def.ora_record.column_type_ref()) {
    Qualified_column_ident *tmp = spv->field_def.ora_record.column_type_ref();
    return sp_variable_declarations_column_type_finalize(thd, nvars, tmp);
  }

  /*case of DECLARE spvar1 spvar0%TYPE; spvar0 table%ROWTYPE*/
  if (spv->field_def.ora_record.table_rowtype_ref() &&
      !spv->field_def.ora_record.row_field_table_definitions()) {
    const Table_ident *tmp = spv->field_def.ora_record.table_rowtype_ref();
    return sp_variable_declarations_table_rowtype_finalize(thd, nvars, tmp->db,
                                                           tmp->table);
  }

  // A reference to a scalar variable with an explicit data type
  return sp_variable_declarations_copy_type_finalize(thd, nvars, spv->field_def,
                                                     spv->default_value, ref);
}

bool LEX::sp_variable_declarations_column_type_finalize(
    THD *thd, int nvars, Qualified_column_ident *ref) {
  LEX *lex = thd->lex;
  sp_pcontext *pctx = lex->get_sp_current_parsing_ctx();
  Create_field *def = new (thd->mem_root) Create_field();
  ref->resolve_type_ref(thd, def, false);
  for (int i = 0; i < nvars; i++) {
    sp_variable *spvar = pctx->get_last_context_variable((uint)nvars - 1 - i);
    spvar->type = def->sql_type;

    spvar->field_def = *def;
    spvar->field_def.ora_record.set_column_type_ref(ref);
    spvar->field_def.field_name = spvar->name.str;
    spvar->field_def.udt_name = def->udt_name.str ? def->udt_name : spvar->name;
    spvar->field_def.udt_db_name =
        def->udt_name.str ? lex->sphead->m_db : NULL_STR;
  }
  return false;
}

bool LEX::sp_variable_declarations_copy_type_finalize(THD *thd, int nvars,
                                                      const Create_field &ref,
                                                      Item *default_value,
                                                      LEX_CSTRING def_type) {
  LEX *lex = thd->lex;
  sp_pcontext *pctx = lex->get_sp_current_parsing_ctx();
  sp_head *sp = lex->sphead;
  for (int i = 0; i < nvars; i++) {
    sp_variable *spvar = pctx->get_last_context_variable((uint)nvars - 1 - i);
    if (!spvar) return true;
    // bugfix:x x%type;
    if (my_strcasecmp(system_charset_info, def_type.str, spvar->name.str) ==
        0) {
      my_error(ER_NOT_SUPPORTED_YET, MYF(0),
               "the same of variable and define of variable%type");
      return true;
    }
    spvar->field_def = ref;
    spvar->default_value = default_value;
    spvar->field_def.field_name = spvar->name.str;
    spvar->field_def.is_nullable = true;
    if (ref.ora_record.row_field_definitions() ||
        ref.ora_record.row_field_table_definitions()) {
      sp_instr_setup_row_field *is = new (thd->mem_root)
          sp_instr_setup_row_field(sp->instructions(), pctx, spvar);

      if (!is || sp->add_instr(thd, is)) return true;
    }
  }
  return false;
}

/**
  Finalize a %ROWTYPE declaration, e.g.:
    DECLARE a,b,c,d t1%ROWTYPE;

  @param thd   - the current thd
  @param nvars - the number of variables in the declaration
  @param ref   - the table or cursor name (see comments below)
*/
bool LEX::sp_variable_declarations_rowtype_finalize(
    THD *thd, int nvars, Qualified_column_ident *ref) {
  int offset = -1;

  if (ref->db.str) {
    my_error(ER_SP_MISMATCH_VAR_QUANTITY, MYF(0));
    return true;
  }

  Table_ident *table_ref = nullptr;
  Row_definition_list *rdl = nullptr;
  int rc = check_percent_rowtype(thd, ref, &table_ref, &rdl, &offset);
  if (rc == -1) return true;
  /*
    When parsing a qualified identifier chain, the parser does not know yet
    if it's going to be a qualified column name (for %TYPE),
    or a qualified table name (for %ROWTYPE). So it collects the chain
    into Qualified_column_ident.
    Now we know that it was actually a qualified table name (%ROWTYPE).
    Create a new Table_ident from Qualified_column_ident,
    shifting fields as follows:
    - ref->m_column becomes table_ref->table
    - ref->table    becomes table_ref->db
  */
  return sp_variable_declarations_cursor_rowtype_finalize(thd, nvars, offset,
                                                          table_ref, rdl);
}

bool LEX::sp_variable_declarations_cursor_rowtype_finalize(
    THD *thd, int nvars, int offset, Table_ident *table_ref,
    Row_definition_list *rdl) {
  LEX *lex = thd->lex;
  sp_head *sp = lex->sphead;
  sp_pcontext *pctx = lex->get_sp_current_parsing_ctx();

  if (!table_ref && !rdl && offset == -1) {
    my_error(ER_NOT_SUPPORTED_YET, MYF(0),
             "non structure variable used in %rowtype");
    return true;
  }
  // Loop through all variables in the same declaration
  for (int i = 0; i < nvars; i++) {
    sp_variable *spvar = pctx->get_last_context_variable((uint)nvars - 1 - i);
    if (!spvar) return true;

    if (spvar->field_def.init(thd, "", MYSQL_TYPE_NULL, nullptr, nullptr, 0,
                              NULL, NULL, &NULL_CSTR, 0, nullptr,
                              thd->variables.collation_database, false, 0,
                              nullptr, nullptr, nullptr, {},
                              dd::Column::enum_hidden_type::HT_VISIBLE)) {
      return true;
    }

    spvar->type = MYSQL_TYPE_NULL;
    spvar->field_def.ora_record.set_row_field_definitions(rdl);
    spvar->field_def.ora_record.set_table_rowtype_ref(table_ref);
    if (offset != -1) {  // it has cursor_rowtype_ref().
      spvar->field_def.ora_record.set_cursor_rowtype_ref(offset);
      sp_instr_cursor_copy_struct *copy_struct =
          new (thd->mem_root) sp_instr_cursor_copy_struct(
              sp->instructions(), pctx, offset, spvar->offset);
      if (!copy_struct || sp->add_instr(thd, copy_struct)) return true;
    } else if (table_ref) {
      sp->reset_lex(thd);
      lex = thd->lex;
      Item *default_tmp = new (thd->mem_root) Item_null();
      sp_instr_setup_row_field *is_row = new (thd->mem_root)
          sp_instr_setup_row_field(sp->instructions(), pctx, spvar);

      if (!is_row || sp->add_instr(thd, is_row)) return true;

      sp_instr_set *is = new (thd->mem_root) sp_instr_set(
          sp->instructions(), lex, spvar->offset, default_tmp, EMPTY_CSTR,
          ((int)i == nvars - 1), pctx, &sp_rcontext_handler_local);

      if (!is || sp->add_instr(thd, is)) return true;
      if (sp->restore_lex(thd)) return true;
      spvar->field_def.ora_record.set_row_field_definitions(
          new (thd->mem_root) Row_definition_list());
    }
    if (prepare_sp_create_field(thd, &spvar->field_def)) {
      return true;
    }
    spvar->field_def.field_name = spvar->name.str;
    spvar->field_def.is_nullable = true;
  }
  return false;
}

bool LEX::sp_variable_declarations_table_rowtype_make_field_def(
    THD *thd, sp_variable *spvar, LEX_CSTRING db, LEX_CSTRING table,
    Row_definition_list **rdl) {
  LEX *lex = thd->lex;
  spvar->type = MYSQL_TYPE_NULL;
  if (spvar->field_def.init(thd, spvar->name.str, MYSQL_TYPE_NULL, nullptr,
                            nullptr, 0, NULL, NULL, &NULL_CSTR, 0, nullptr,
                            thd->variables.collation_database, false, 0,
                            nullptr, nullptr, nullptr, {},
                            dd::Column::enum_hidden_type::HT_VISIBLE)) {
    return true;
  }
  Table_ident *table_ref;
  if (unlikely(!(table_ref = new (thd->mem_root) Table_ident(db, table))))
    return true;
  *rdl = new (thd->mem_root) Row_definition_list();
  spvar->field_def.ora_record.set_record_table_rowtype_ref(table_ref);

  if (prepare_sp_create_field(thd, &spvar->field_def)) {
    return true;
  }
  spvar->field_def.field_name = spvar->name.str;
  spvar->field_def.is_nullable = true;
  spvar->field_def.udt_name = spvar->name;
  spvar->field_def.udt_db_name = lex->sphead->m_db;
  return false;
}

bool LEX::sp_variable_declarations_table_rowtype_finalize(
    THD *thd, int nvars, const LEX_CSTRING &db, const LEX_CSTRING &table) {
  LEX *lex = thd->lex;
  sp_pcontext *pctx = lex->get_sp_current_parsing_ctx();

  sp_head *sp = lex->sphead;
  sp->reset_lex(thd);
  lex = thd->lex;
  // Loop through all variables in the same declaration
  for (int i = 0; i < nvars; i++) {
    sp_variable *spvar = pctx->get_last_context_variable((uint)nvars - 1 - i);
    if (!spvar) return true;
    Row_definition_list *rdl = nullptr;
    if (sp_variable_declarations_table_rowtype_make_field_def(thd, spvar, db,
                                                              table, &rdl))
      return true;
    spvar->field_def.ora_record.set_row_field_definitions(rdl);
    sp_instr_setup_row_field *is_row = new (thd->mem_root)
        sp_instr_setup_row_field(sp->instructions(), pctx, spvar);
    if (!is_row || sp->add_instr(thd, is_row)) return true;

    Item *default_tmp = new (thd->mem_root) Item_null();

    sp_instr_set *is = new (thd->mem_root) sp_instr_set(
        sp->instructions(), lex, spvar->offset, default_tmp, EMPTY_CSTR,
        ((int)i == nvars - 1), pctx, &sp_rcontext_handler_local);

    if (!is || sp->add_instr(thd, is)) return true;
  }
  if (sp->restore_lex(thd)) return true;
  return false;
}

bool LEX::sp_for_loop_bounds_set_cursor(THD *thd, const LEX_STRING name,
                                        PT_item_list *parameters, uint *offset,
                                        LEX *lex_expr, Item **args,
                                        uint arg_count) {
  LEX *lex = thd->lex;
  sp_head *sp = lex->sphead;
  sp_pcontext *pctx = lex->get_sp_current_parsing_ctx();

  uint param_count = parameters ? parameters->elements() : arg_count;
  sp_pcursor *pcursor;
  // It's ref cursor.
  sp_variable *spvar_cursor = pctx->find_variable(name.str, name.length, false);
  if (spvar_cursor && spvar_cursor->field_def.ora_record.is_ref_cursor) {
    my_error(ER_NOT_SUPPORTED_YET, MYF(0), "wrong type of cursor used here");
    return true;
  }
  if (!pctx->find_cursor(name, offset, false)) {
    my_error(ER_SP_CURSOR_MISMATCH, MYF(0), name.str);
    return true;
  }
  if (!(pcursor = pctx->find_cursor_parameters(*offset)) ||
      pcursor->check_param_count_with_error(param_count)) {
    my_error(ER_SP_CURSOR_MISMATCH, MYF(0), name.str);
    return true;
  }

  sp_pcontext *pctx_cursor = pcursor->param_context();
  assert(param_count == pctx_cursor->context_var_count());
  uint num_vars = pctx_cursor->context_var_count();

  if (param_count > 0) {
    assert(pctx_cursor->context_var_count() == param_count);
    Item *prm = nullptr;
    for (uint idx = 0; idx < param_count; idx++) {
      prm = parameters ? parameters->value.at(idx) : args[idx];
      sp_variable *spvar = pctx_cursor->get_context_variable(idx);
      assert(idx < pctx_cursor->context_var_count());
      sp_instr_set *is = new (thd->mem_root) sp_instr_set(
          sp->instructions(), lex_expr, spvar->offset, prm, EMPTY_CSTR,
          (idx == num_vars - 1), pctx_cursor, &sp_rcontext_handler_local);
      if (!is || sp->add_instr(thd, is)) return true;
    }
  }
  return false;
}

bool LEX::sp_for_cursor_in_loop(THD *thd, Item *item_uuid,
                                const char *loc_start, const char *loc_end,
                                uint *cursor_offset, LEX_STRING *cursor_ident) {
  LEX *cursor_lex = thd->lex;
  sp_head *sp = cursor_lex->sphead;

  assert(cursor_lex->sql_command == SQLCOM_SELECT);

  if (cursor_lex->result) {
    my_error(ER_SP_BAD_CURSOR_SELECT, MYF(0));
    return true;
  }
  cursor_lex->m_sql_cmd->set_as_part_of_sp();
  cursor_lex->sp_lex_in_use = true;

  if (sp->restore_lex(thd)) return true;

  LEX *lex = thd->lex;
  sp_pcontext *pctx_new = lex->get_sp_current_parsing_ctx();
  LEX_CSTRING cursor_query = EMPTY_CSTR;
  char temp_ptr[32] = {};
  memset(temp_ptr, 0, sizeof(temp_ptr));
  if (!item_uuid) return true;
  longlong uuid = item_uuid->val_int();

  sprintf(temp_ptr, "cursor_%lld", uuid);
  char *ptr = thd->strmake(temp_ptr, strlen(temp_ptr));
  if (!ptr) return true;
  *cursor_ident = {ptr, strlen(ptr)};

  if (pctx_new->find_cursor(*cursor_ident, cursor_offset, false)) {
    my_error(ER_SP_DUP_CURS, MYF(0), cursor_ident->str);
    return true;
  }
  if (pctx_new->add_cursor_parameters(thd, *cursor_ident, pctx_new,
                                      cursor_offset, nullptr)) {
    return true;
  }

  cursor_query = make_string(thd, loc_start, loc_end);

  if (!cursor_query.str) return true;

  sp_instr_cpush_rowtype *i = new (thd->mem_root) sp_instr_cpush_rowtype(
      sp->instructions(), pctx_new, cursor_lex, cursor_query, *cursor_offset);

  if (!i || sp->add_instr(thd, i)) return true;

  return false;
}

bool LEX::sp_open_cursor(THD *thd, const LEX_STRING name,
                         PT_item_list *parameters) {
  LEX *lex = thd->lex;
  sp_head *sp = lex->sphead;
  sp_pcontext *pctx = lex->get_sp_current_parsing_ctx();
  uint offset;
  if (sp_for_loop_bounds_set_cursor(thd, name, parameters, &offset, lex))
    return true;

  sp_instr_copen *i =
      new (thd->mem_root) sp_instr_copen(sp->instructions(), pctx, offset);

  if (!i || sp->add_instr(thd, i)) return true;
  return false;
}

bool LEX::sp_open_cursor_for(THD *thd, LEX_STRING sql_ident,
                             LEX_STRING cursor_ident, const char *loc) {
  LEX *cursor_lex = thd->lex;
  sp_head *sp = cursor_lex->sphead;
  sp_pcontext *pctx = thd->lex->get_sp_current_parsing_ctx();
  uint offp;
  sp_variable *spvar_cursor =
      pctx->find_variable(cursor_ident.str, cursor_ident.length, false);
  if (!spvar_cursor || !spvar_cursor->field_def.ora_record.is_ref_cursor) {
    my_error(ER_SP_CURSOR_MISMATCH, MYF(0), cursor_ident.str);
    return true;
  }
  if (spvar_cursor->field_def.ora_record.is_ref_cursor_define) {
    my_error(ER_NOT_SUPPORTED_YET, MYF(0), "ref cursor define used as cursor");
    return true;
  }
  if (!pctx->find_cursor(cursor_ident, &offp, false)) {
    my_error(ER_SP_CURSOR_MISMATCH, MYF(0), cursor_ident.str);
    return true;
  }

  if (!sql_ident.str) { /*open c for sql*/
    assert(cursor_lex->sql_command == SQLCOM_SELECT);

    if (cursor_lex->result) {
      my_error(ER_SP_BAD_CURSOR_SELECT, MYF(0));
      return true;
    }
    cursor_lex->m_sql_cmd->set_as_part_of_sp();
    cursor_lex->sp_lex_in_use = true;

    if (sp->restore_lex(thd)) return true;

    LEX *lex = thd->lex;
    sp_pcontext *pctx_new = lex->get_sp_current_parsing_ctx();
    LEX_CSTRING cursor_query = EMPTY_CSTR;

    cursor_query =
        make_string(thd, sp->m_parser_data.get_current_stmt_start_ptr(), loc);

    if (!cursor_query.str) return true;

    sp_instr_copen_for_sql *i_open = new (thd->mem_root)
        sp_instr_copen_for_sql(sp->instructions(), pctx_new, cursor_lex,
                               cursor_query, spvar_cursor->offset);

    if (!i_open || sp->add_instr(thd, i_open)) return true;

  } else { /*open c for ident*/
    LEX *lex = thd->lex;
    sp_pcontext *pctx_new = lex->get_sp_current_parsing_ctx();
    sp_variable *spvar_sql =
        pctx->find_variable(sql_ident.str, sql_ident.length, false);
    if (!spvar_sql) {
      my_error(ER_SP_UNDECLARED_VAR, MYF(0), sql_ident.str);
      return true;
    }
    sp_instr_copen_for_ident *i_open = new (thd->mem_root)
        sp_instr_copen_for_ident(sp->instructions(), pctx_new, lex,
                                 spvar_cursor->offset, spvar_sql->offset);

    if (!i_open || sp->add_instr(thd, i_open)) return true;
    if (sp->restore_lex(thd)) return true;
  }
  return false;
}

bool LEX::sp_variable_declarations_row_finalize(THD *thd, LEX_STRING ident,
                                                Row_definition_list *row) {
  LEX *lex = thd->lex;
  sp_head *sp = lex->sphead;
  sp_pcontext *pctx = lex->get_sp_current_parsing_ctx();
  assert(row);
  /*
    Prepare all row fields.
    Note, we do it only one time outside of the below loop.
    The converted list in "row" is further reused by all variable
    declarations processed by the current call.
    Example:
      case 1:
        type rec_tk is record
        (
          tkno VARCHAR2(100) := 'aaa',
          cg_zdj number(12,0) := 0 ,
          cg_jsf number(12,0) := 0
        );
      case 2:
        type rec_tk1 is record
        (
          rr VARCHAR2(100) := 'bbb',
          tk_val rec_tk  <----Row_definition_list
        );
        rec_tk1_val rec_tk1;
        select rec_tk1_val.tk_val.tkno = 'aaa'.
      case 3:
        type cjr is table of rec_tk index by binary_integer;
        type rec_tk2 is record
        (
          rr VARCHAR2(100) := 'ccc',
          cjr_val cjr  <----Row_definition_table_list
        );
        rec_tk2_val rec_tk2;
        select rec_tk2_val.cjr_val(1).tkno error!
  */
  if (pctx->find_variable(ident.str, ident.length, false)) {
    my_error(ER_SP_DUP_PARAM, MYF(0), ident.str);
    return true;
  }

  sp_variable *spvar =
      pctx->add_variable(thd, ident, MYSQL_TYPE_DECIMAL, sp_variable::MODE_IN);

  if (spvar->field_def.init(
          thd, "", MYSQL_TYPE_NULL, nullptr, nullptr, 0, NULL, NULL, &NULL_CSTR,
          0, nullptr, thd->variables.collation_database, false, 0, nullptr,
          nullptr, nullptr, {}, dd::Column::enum_hidden_type::HT_VISIBLE)) {
    return true;
  }

  if (prepare_sp_create_field(thd, &spvar->field_def)) {
    return true;
  }
  spvar->field_def.field_name = spvar->name.str;
  spvar->field_def.is_nullable = true;
  spvar->field_def.ora_record.set_row_field_definitions(row);
  spvar->field_def.ora_record.is_record_define = true;
  spvar->field_def.udt_name = ident;
  spvar->field_def.udt_db_name = lex->sphead->m_db;

  sp_instr_setup_row_field *is_set = new (thd->mem_root)
      sp_instr_setup_row_field(sp->instructions(), pctx, spvar);

  if (!is_set || sp->add_instr(thd, is_set)) return true;

  List<Create_field> *sd = reinterpret_cast<List<Create_field> *>(row);
  List_iterator<Create_field> it(*sd);
  Create_field *def;
  for (uint offset = 0; (def = it++); offset++) {
    Item *item = def->ora_record.record_default_value;
    if (!item) continue;
    sp->reset_lex(thd);
    lex = thd->lex;
    sp = lex->sphead;
    pctx = lex->get_sp_current_parsing_ctx();

    Parse_context pc(thd, lex->current_query_block());
    if ((lex->will_contextualize && item->itemize(&pc, &item))) return true;
    mem_root_deque<Item *> field_list(thd->mem_root);
    item->walk(&Item::collect_item_field_processor, enum_walk::POSTFIX,
               (uchar *)&field_list);
    for (Item *cur_item : field_list) {
      if (!cur_item->is_splocal()) {
        // resolve case with: type stu_record is record(my_a text:=nullptr)
        if (cur_item->fix_fields(thd, &cur_item)) {
          my_error(ER_BAD_FIELD_ERROR, MYF(0), cur_item->item_name.ptr(),
                   "field list");
          return true;
        }
      }
    }

    sp_instr_set_row_field *is = new (thd->mem_root) sp_instr_set_row_field(
        sp->instructions(), lex, spvar->offset, item, EMPTY_CSTR, true, pctx,
        &sp_rcontext_handler_local, offset);

    if (!is || sp->add_instr(thd, is)) return true;
    def->ora_record.record_default_value = item;
    if (sp->restore_lex(thd)) return true;
  }
  return false;
}

static bool sp_variable_declarations_add_udt_variable(
    THD *thd, LEX_STRING record, LEX_STRING udt_db_name,
    Row_definition_list **rdl_out, Row_definition_table_list **rdl_table_out,
    LEX_CSTRING *nested_table_udt, uint *table_type, ulonglong *varray_limit,
    List<Create_field> **field_def_list) {
  LEX *lex = thd->lex;
  sp_pcontext *pctx = lex->get_sp_current_parsing_ctx();
  sp_variable *spvar_udt = nullptr;
  // add udt var
  ulong reclength = 0;
  sql_mode_t sql_mode;
  MDL_savepoint mdl_savepoint = thd->mdl_context.mdl_savepoint();
  if (!sp_find_ora_type_create_fields(
          thd, udt_db_name, record, false, field_def_list, &reclength,
          &sql_mode, nested_table_udt, table_type, varray_limit)) {
    Row_definition_list *rdl = new (thd->mem_root) Row_definition_list();
    if (rdl->make_from_defs(thd->mem_root, *field_def_list)) return true;

    if (!nested_table_udt->str && *table_type != 0 && *table_type != 1) {
      spvar_udt =
          pctx->add_udt_variable(thd, rdl, record, *table_type, MYSQL_TYPE_NULL,
                                 *varray_limit, *nested_table_udt, udt_db_name);
      if (!spvar_udt) {
        my_error(ER_SP_UNDECLARED_VAR, MYF(0), record.str);
        return true;
      }
      *rdl_out = rdl;
    } else {
      Row_definition_list *rdl_result = nullptr;
      if (rdl->make_new_create_field_to_store_index(thd, 0, false, &rdl_result))
        return true;
      spvar_udt = pctx->add_udt_variable(thd, rdl_result, record, *table_type,
                                         MYSQL_TYPE_NULL, *varray_limit,
                                         *nested_table_udt, udt_db_name);
      if (!spvar_udt) {
        my_error(ER_SP_UNDECLARED_VAR, MYF(0), record.str);
        return true;
      }
      *rdl_table_out =
          Row_definition_table_list::make(thd->mem_root, rdl_result);
    }
  }
  thd->mdl_context.rollback_to_savepoint(mdl_savepoint);
  return false;
}

bool LEX::sp_variable_declarations_record_finalize(
    THD *thd, int nvars, LEX_STRING record, Item *default_value,
    LEX_CSTRING dflt_value_query) {
  LEX *lex = thd->lex;
  sp_pcontext *pctx = lex->get_sp_current_parsing_ctx();
  // First get udt define.
  sp_variable *spvar_udt =
      pctx->find_udt_variable(record.str, record.length, lex->sphead->m_db.str,
                              lex->sphead->m_db.length, false);
  Row_definition_list *rdl = nullptr;
  if (spvar_udt) {
    if (!spvar_udt->field_def.nested_table_udt.str &&
        spvar_udt->field_def.table_type != 0 &&
        spvar_udt->field_def.table_type != 1) {
      return sp_variable_declarations_udt_set_finalize(
          thd, nvars, spvar_udt->field_def.ora_record.row_field_definitions(),
          record, default_value, dflt_value_query);
    } else {
      List<Row_definition_list> *sd = dynamic_cast<List<Row_definition_list> *>(
          spvar_udt->field_def.ora_record.row_field_table_definitions());
      if (!sd) return true;

      List_iterator_fast<Row_definition_list> it_table(*sd);
      Row_definition_list *def_list;
      for (uint offset = 0; (def_list = it_table++); offset++) {
        rdl = def_list;
      }
      return sp_variable_declarations_udt_table_set_finalize(
          thd, nvars, rdl, spvar_udt->field_def.nested_table_udt,
          spvar_udt->field_def.table_type, spvar_udt->field_def.varray_limit,
          record, default_value, dflt_value_query);
    }
  } else {
    List<Create_field> *field_def_list = nullptr;
    Row_definition_list *rdl_result = nullptr;
    Row_definition_table_list *rdl_table = nullptr;
    LEX_CSTRING nested_table_udt = NULL_CSTR;
    uint table_type = 255;
    ulonglong varray_limit = 0;
    if (sp_variable_declarations_add_udt_variable(
            thd, record, lex->sphead->m_db, &rdl_result, &rdl_table,
            &nested_table_udt, &table_type, &varray_limit, &field_def_list))
      return true;
    if (rdl_result || rdl_table) {
      if (rdl_result) {
        return sp_variable_declarations_udt_set_finalize(
            thd, nvars, field_def_list, record, default_value,
            dflt_value_query);
      }
      if (rdl_table) {
        return sp_variable_declarations_udt_table_set_finalize(
            thd, nvars, rdl_table->find_row_fields_by_offset(0),
            nested_table_udt, table_type, varray_limit, record, default_value,
            dflt_value_query);
      }
    }
  }

  // Second get record define.
  sp_variable *spvar_record =
      pctx->find_variable(record.str, record.length, false);
  if (!spvar_record) {
    my_error(ER_SP_UNDECLARED_VAR, MYF(0), record.str);
    return true;
  }

  if (spvar_record->field_def.ora_record.is_ref_cursor_define) {
    bool rc = sp_variable_declarations_define_ref_cursor_finalize(
        thd, nvars, default_value, spvar_record);
    return rc;
  }

  if (!spvar_record->field_def.ora_record.is_record_define &&
      !spvar_record->field_def.ora_record.is_record_table_define) {
    my_error(ER_SP_MISMATCH_RECORD_VAR, MYF(0), record.str);
    return true;
  }
  // type stu_record_arr is table of stu_record; stu_record_arr_val
  // stu_record_arr;
  if (spvar_record->field_def.ora_record.is_record_table_define) {
    if (sp_variable_declarations_type_record_table_set_finalize(
            thd, nvars, record, default_value))
      return true;
  } else {
    // type stu_record_arr is record (); stu_record_arr_val stu_record_arr;
    if (sp_variable_declarations_type_record_set_finalize(
            thd, nvars, record, default_value, dflt_value_query))
      return true;
  }
  return false;
}

bool LEX::sp_variable_declarations_udt_table_set_finalize(
    THD *thd, int nvars, List<Create_field> *field_def_list,
    LEX_CSTRING nested_table_udt, uint table_type, ulonglong varray_limit,
    LEX_STRING record, Item *default_value, LEX_CSTRING dflt_value_query) {
  LEX *lex = thd->lex;
  sp_head *sp = lex->sphead;
  sp_pcontext *pctx = lex->get_sp_current_parsing_ctx();
  sp_variable *spvar = nullptr;
  Row_definition_list *rdl = nullptr;
  pctx->declare_var_boundary(nvars);

  // create or replace type t_test is varray(100) of integer;
  // tval t_test;
  for (uint i = 0; i < (uint)nvars; i++) {
    spvar = pctx->get_last_context_variable((uint)nvars - 1 - i);
    if (spvar->field_def.init(thd, "", MYSQL_TYPE_NULL, nullptr, nullptr, 0,
                              NULL, NULL, &NULL_CSTR, 0, nullptr,
                              thd->variables.collation_database, false, 0,
                              nullptr, nullptr, nullptr, {},
                              dd::Column::enum_hidden_type::HT_VISIBLE)) {
      return true;
    }

    rdl = new (thd->mem_root) Row_definition_list();
    if (rdl->make_from_defs(thd->mem_root, field_def_list)) return true;

    spvar->field_def.ora_record.set_row_field_table_definitions(
        Row_definition_table_list::make(thd->mem_root, rdl));
    spvar->field_def.udt_name = record;
    spvar->field_def.udt_db_name = lex->sphead->m_db;
    spvar->field_def.nested_table_udt = nested_table_udt;
    spvar->field_def.table_type = table_type;
    spvar->field_def.varray_limit = varray_limit;
    if (prepare_sp_create_field(thd, &spvar->field_def)) {
      return true;
    }
    spvar->field_def.field_name = spvar->name.str;
    spvar->field_def.is_nullable = true;
    spvar->field_def.ora_record.is_record_table_define = false;
    spvar->field_def.ora_record.record_default_value = default_value;
    sp_instr_setup_row_field *is_row = new (thd->mem_root)
        sp_instr_setup_row_field(sp->instructions(), pctx, spvar);
    if (!is_row || sp->add_instr(thd, is_row)) return true;
    if (!default_value) break;
    Parse_context pc(thd, lex->current_query_block());
    if ((lex->will_contextualize &&
         default_value->itemize(&pc, &default_value)))
      return true;
    if (default_value->type() != Item::NULL_ITEM) {
      if (default_value->get_udt_name().str) {
        if (my_strcasecmp(system_charset_info, record.str,
                          default_value->get_udt_name().str) != 0) {
          my_error(ER_WRONG_UDT_DATA_TYPE, myf(0), thd->db().str, record.str,
                   thd->db().str, default_value->get_udt_name().str);
          return true;
        }
      }
    }
    sp_instr_set *is = new (thd->mem_root) sp_instr_set(
        sp->instructions(), lex, spvar->offset, default_value, dflt_value_query,
        ((int)i == nvars - 1), pctx, &sp_rcontext_handler_local);

    if (!is || sp->add_instr(thd, is)) return true;
  }
  pctx->declare_var_boundary(0);
  return false;
}

bool LEX::sp_variable_declarations_udt_set_finalize(
    THD *thd, int nvars, List<Create_field> *field_def_list, LEX_STRING record,
    Item *default_value, LEX_CSTRING dflt_value_query) {
  LEX *lex = thd->lex;
  sp_head *sp = lex->sphead;
  sp_pcontext *pctx = lex->get_sp_current_parsing_ctx();
  sp_variable *spvar = nullptr;
  pctx->declare_var_boundary(nvars);
  // create or replace type t_test as object(id integer);
  // tval t_test;
  for (uint i = 0; i < (uint)nvars; i++) {
    spvar = pctx->get_last_context_variable((uint)nvars - 1 - i);
    if (spvar->field_def.init(thd, "", MYSQL_TYPE_NULL, nullptr, nullptr, 0,
                              NULL, NULL, &NULL_CSTR, 0, nullptr,
                              thd->variables.collation_database, false, 0,
                              nullptr, nullptr, nullptr, {},
                              dd::Column::enum_hidden_type::HT_VISIBLE)) {
      return true;
    }
    spvar->field_def.udt_name = record;
    spvar->field_def.udt_db_name = lex->sphead->m_db;
    if (prepare_sp_create_field(thd, &spvar->field_def)) {
      return true;
    }
    Row_definition_list *rdl = new (thd->mem_root) Row_definition_list();
    if (rdl->make_from_defs(thd->mem_root, field_def_list)) return true;
    spvar->field_def.ora_record.set_row_field_definitions(rdl);
    spvar->field_def.field_name = spvar->name.str;
    spvar->field_def.is_nullable = true;
    spvar->field_def.ora_record.is_record_define = false;
    spvar->field_def.ora_record.record_default_value = default_value;
    sp_instr_setup_row_field *is_row = new (thd->mem_root)
        sp_instr_setup_row_field(sp->instructions(), pctx, spvar);
    if (!is_row || sp->add_instr(thd, is_row)) return true;
    if (!default_value) break;

    Parse_context pc(thd, lex->current_query_block());
    if ((lex->will_contextualize &&
         default_value->itemize(&pc, &default_value)))
      return true;
    if (default_value->type() != Item::NULL_ITEM) {
      if (default_value->get_udt_name().str) {
        if (my_strcasecmp(system_charset_info, record.str,
                          default_value->get_udt_name().str) != 0) {
          my_error(ER_WRONG_UDT_DATA_TYPE, myf(0), thd->db().str, record.str,
                   thd->db().str, default_value->get_udt_name().str);
          return true;
        }
      }
    }
    sp_instr_set *is = new (thd->mem_root) sp_instr_set(
        sp->instructions(), lex, spvar->offset, default_value, dflt_value_query,
        ((int)i == nvars - 1), pctx, &sp_rcontext_handler_local);

    if (!is || sp->add_instr(thd, is)) return true;
  }
  pctx->declare_var_boundary(0);
  return false;
}

bool LEX::sp_variable_declarations_type_record_table_set_finalize(
    THD *thd, int nvars, LEX_STRING record, Item *dflt_value) {
  LEX *lex = thd->lex;
  sp_head *sp = lex->sphead;
  sp_pcontext *pctx = lex->get_sp_current_parsing_ctx();

  sp_variable *spvar_record =
      pctx->find_variable(record.str, record.length, false);
  if (!spvar_record) {
    my_error(ER_SP_UNDECLARED_VAR, MYF(0), record.str);
    return true;
  }
  if (!spvar_record->field_def.ora_record.is_record_table_define) {
    my_error(ER_SP_MISMATCH_RECORD_VAR, MYF(0), record.str);
    return true;
  }
  /* type stu_record_arr is table of stu_record;
     stu_record_arr_val stu_record_arr;
     udt_name=stu_record_arr,nested_table_udt=stu_record
  */
  for (uint i = 0; i < (uint)nvars; i++) {
    sp_variable *spvar = pctx->get_last_context_variable((uint)nvars - 1 - i);
    if (spvar->field_def.init(thd, "", MYSQL_TYPE_NULL, nullptr, nullptr, 0,
                              NULL, NULL, &NULL_CSTR, 0, nullptr,
                              thd->variables.collation_database, false, 0,
                              nullptr, nullptr, nullptr, {},
                              dd::Column::enum_hidden_type::HT_VISIBLE)) {
      return true;
    }

    if (prepare_sp_create_field(thd, &spvar->field_def)) {
      return true;
    }

    spvar->field_def.ora_record.set_record(&spvar_record->field_def.ora_record);
    spvar->field_def.ora_record.is_record_table_type_define = false;
    spvar->field_def.field_name = spvar->name.str;
    spvar->field_def.is_nullable = true;
    spvar->field_def.ora_record.is_record_table_define = false;
    spvar->field_def.udt_name = record;
    spvar->field_def.udt_db_name = lex->sphead->m_db;
    spvar->field_def.nested_table_udt =
        spvar_record->field_def.nested_table_udt;
    spvar->field_def.ora_record.is_index_by =
        spvar_record->field_def.ora_record.is_index_by;
    if (spvar_record->field_def.ora_record.is_index_by && !dflt_value &&
        !spvar_record->field_def.ora_record.is_record_table_type_define) {
      dflt_value = new (thd->mem_root)
          Item_splocal(&sp_rcontext_handler_local, spvar_record->name,
                       spvar_record->offset, spvar_record->type);
#ifndef NDEBUG
      Item_splocal *splocal = dynamic_cast<Item_splocal *>(dflt_value);
      if (splocal) splocal->m_sp = thd->lex->sphead;
#endif
    }
    sp_instr_setup_row_field *is_row = new (thd->mem_root)
        sp_instr_setup_row_field(sp->instructions(), pctx, spvar);

    if (!is_row || sp->add_instr(thd, is_row)) return true;
    if (dflt_value) {
      Parse_context pc(thd, lex->current_query_block());
      if ((lex->will_contextualize && dflt_value->itemize(&pc, &dflt_value)))
        return true;
      sp_instr_set *is = new (thd->mem_root) sp_instr_set(
          sp->instructions(), lex, spvar->offset, dflt_value, EMPTY_CSTR,
          ((int)i == nvars - 1), pctx, &sp_rcontext_handler_local);

      if (!is || sp->add_instr(thd, is)) return true;
    }
  }
  return false;
}

Create_field *LEX::sp_variable_declarations_definition_in_record(
    THD *thd, LEX_STRING ident, LEX_STRING record, Item *default_value) {
  /*type aa is record():is_record_define=true,row_field_definitions()=true
    bb aa:is_record_define=false,row_field_definitions()=true
    type cc is table of
    aa:is_record_table_define=true,row_field_table_definitions()=true dd cc:
    is_record_table_define=false,row_field_table_definitions()=true
  */
  sp_pcontext *pctx = thd->lex->get_sp_current_parsing_ctx();
  Create_field *cdf = new (thd->mem_root) Create_field();
  if (cdf->init(thd, ident.str, MYSQL_TYPE_NULL, nullptr, nullptr, 0, NULL,
                NULL, &NULL_CSTR, 0, nullptr, thd->variables.collation_database,
                false, 0, nullptr, nullptr, nullptr, {},
                dd::Column::enum_hidden_type::HT_VISIBLE)) {
    return nullptr;
  }
  // First get udt define.
  sp_variable *spvar_udt = pctx->find_udt_variable(
      record.str, record.length, thd->lex->sphead->m_db.str,
      thd->lex->sphead->m_db.length, false);
  if (spvar_udt) {
    if (!spvar_udt->field_def.nested_table_udt.str &&
        spvar_udt->field_def.table_type != 0 &&
        spvar_udt->field_def.table_type != 1) {
      cdf->ora_record.set_row_field_definitions(
          spvar_udt->field_def.ora_record.row_field_definitions());
    } else {
      cdf->ora_record.set_row_field_table_definitions(
          spvar_udt->field_def.ora_record.row_field_table_definitions());
      cdf->nested_table_udt = spvar_udt->field_def.nested_table_udt;
      cdf->table_type = spvar_udt->field_def.table_type;
      cdf->varray_limit = spvar_udt->field_def.varray_limit;
    }
    cdf->udt_name = record;
    cdf->udt_db_name = to_lex_string(thd->db());
    if (prepare_sp_create_field(thd, cdf)) {
      return nullptr;
    }
    cdf->ora_record.record_default_value = default_value;
    return cdf;
  } else {
    // add udt var
    List<Create_field> *field_def_list = nullptr;
    Row_definition_list *rdl_result = nullptr;
    Row_definition_table_list *rdl_table = nullptr;
    LEX_CSTRING nested_table_udt = NULL_CSTR;
    uint table_type = 255;
    ulonglong varray_limit = 0;
    if (sp_variable_declarations_add_udt_variable(
            thd, record, thd->lex->sphead->m_db, &rdl_result, &rdl_table,
            &nested_table_udt, &table_type, &varray_limit, &field_def_list))
      return nullptr;
    if (rdl_result || rdl_table) {
      if (rdl_result) {
        cdf->ora_record.set_row_field_definitions(rdl_result);
      }
      if (rdl_table) {
        cdf->ora_record.set_row_field_table_definitions(rdl_table);
        cdf->nested_table_udt = nested_table_udt;
        cdf->table_type = table_type;
        cdf->varray_limit = varray_limit;
      }
      cdf->udt_name = record;
      cdf->udt_db_name = to_lex_string(thd->db());
      if (prepare_sp_create_field(thd, cdf)) {
        return nullptr;
      }
      cdf->ora_record.record_default_value = default_value;
      return cdf;
    }
  }
  // Second get record define.
  sp_variable *spvar = pctx->find_variable(record.str, record.length, false);
  if (!spvar) {
    my_error(ER_SP_MISMATCH_RECORD_VAR, MYF(0), record.str);
    return nullptr;
  }

  if (spvar->field_def.ora_record.row_field_definitions() &&
      spvar->field_def.ora_record.is_record_define) {
    cdf->ora_record.set_row_field_definitions(
        spvar->field_def.ora_record.row_field_definitions());
  } else if (spvar->field_def.ora_record.row_field_table_definitions() &&
             spvar->field_def.ora_record.is_record_table_define) {
    cdf->ora_record.set_row_field_table_definitions(
        spvar->field_def.ora_record.row_field_table_definitions());
    cdf->nested_table_udt = spvar->field_def.nested_table_udt;
    cdf->ora_record.is_index_by = spvar->field_def.ora_record.is_index_by;
  } else {
    my_error(ER_SP_MISMATCH_RECORD_VAR, MYF(0), record.str);
    return nullptr;
  }
  cdf->udt_name = record;
  cdf->udt_db_name = to_lex_string(thd->db());
  if (prepare_sp_create_field(thd, cdf)) {
    return nullptr;
  }
  if (!default_value) {
    default_value = new (thd->mem_root) Item_splocal(
        &sp_rcontext_handler_local, spvar->name, spvar->offset, spvar->type);
#ifndef NDEBUG
    Item_splocal *item_sp = dynamic_cast<Item_splocal *>(default_value);
    if (item_sp) item_sp->m_sp = thd->lex->sphead;
#endif
  }
  cdf->ora_record.record_default_value = default_value;
  return cdf;
}

bool LEX::sp_variable_declarations_type_record_set_finalize(
    THD *thd, int nvars, LEX_STRING record, Item *default_value,
    LEX_CSTRING dflt_value_query) {
  LEX *lex = thd->lex;
  sp_head *sp = lex->sphead;
  sp_pcontext *pctx = lex->get_sp_current_parsing_ctx();

  sp_variable *spvar_record =
      pctx->find_variable(record.str, record.length, false);
  if (!spvar_record) {
    my_error(ER_SP_UNDECLARED_VAR, MYF(0), record.str);
    return true;
  }
  if (!spvar_record->field_def.ora_record.is_record_define) {
    my_error(ER_SP_MISMATCH_RECORD_VAR, MYF(0), record.str);
    return true;
  }

  // type stu_record_arr is record (); stu_record_arr_val stu_record_arr;
  for (uint i = 0; i < (uint)nvars; i++) {
    sp_variable *spvar = pctx->get_last_context_variable((uint)nvars - 1 - i);
    if (spvar->field_def.init(thd, "", MYSQL_TYPE_NULL, nullptr, nullptr, 0,
                              NULL, NULL, &NULL_CSTR, 0, nullptr,
                              thd->variables.collation_database, false, 0,
                              nullptr, nullptr, nullptr, {},
                              dd::Column::enum_hidden_type::HT_VISIBLE)) {
      return true;
    }

    if (prepare_sp_create_field(thd, &spvar->field_def)) {
      return true;
    }
    spvar->field_def.field_name = spvar->name.str;
    spvar->field_def.is_nullable = true;
    spvar->field_def.ora_record.set_row_field_definitions(
        spvar_record->field_def.ora_record.row_field_definitions());
    spvar->field_def.ora_record.is_record_define = false;
    spvar->field_def.udt_name = record;
    spvar->field_def.udt_db_name = lex->sphead->m_db;

    sp_instr_setup_row_field *is_row = new (thd->mem_root)
        sp_instr_setup_row_field(sp->instructions(), pctx, spvar);
    if (!is_row || sp->add_instr(thd, is_row)) return true;

    Item *default_tmp = default_value;
    if (default_tmp) {
      Parse_context pc(thd, lex->current_query_block());
      if ((lex->will_contextualize && default_tmp->itemize(&pc, &default_tmp)))
        return true;
      if (default_tmp->type() != Item::NULL_ITEM) {
        if (default_tmp->get_udt_name().str) {
          if (my_strcasecmp(system_charset_info, record.str,
                            default_tmp->get_udt_name().str) != 0) {
            my_error(ER_WRONG_UDT_DATA_TYPE, myf(0), thd->db().str, record.str,
                     thd->db().str, default_tmp->get_udt_name().str);
            return true;
          }
        }
      }
    } else {
      default_tmp = new (thd->mem_root)
          Item_splocal(&sp_rcontext_handler_local, spvar_record->name,
                       spvar_record->offset, spvar_record->type);
#ifndef NDEBUG
      Item_splocal *splocal = dynamic_cast<Item_splocal *>(default_tmp);
      if (splocal) splocal->m_sp = thd->lex->sphead;
#endif
    }
    sp_instr_set *is = new (thd->mem_root) sp_instr_set(
        sp->instructions(), lex, spvar->offset, default_tmp, dflt_value_query,
        ((int)i == nvars - 1), pctx, &sp_rcontext_handler_local);

    if (!is || sp->add_instr(thd, is)) return true;
  }
  return false;
}

bool LEX::sp_variable_declarations_type_table_finalize(THD *thd,
                                                       LEX_STRING ident,
                                                       LEX_STRING table,
                                                       const char *length,
                                                       bool is_index_by) {
  LEX *lex = thd->lex;
  sp_pcontext *pctx = lex->get_sp_current_parsing_ctx();
  sp_head *sp = lex->sphead;
  /*
    Prepare all row fields.
    Note, we do it only one time outside of the below loop.
    The converted list in "row" is further reused by all variable
    declarations processed by the current call.
    Example:
      DECLARE
        type stu_record_arr is table of stu_record;
      BEGIN
        ...
      END;
  */
  if (pctx->find_variable(ident.str, ident.length, false)) {
    my_error(ER_SP_DUP_PARAM, MYF(0), ident.str);
    return true;
  }

  sp_variable *spvar =
      pctx->add_variable(thd, ident, MYSQL_TYPE_DECIMAL, sp_variable::MODE_IN);

  sp_variable *spv_table;
  if (!(spv_table = pctx->find_variable(table.str, table.length, false))) {
    my_error(ER_SP_UNDECLARED_VAR, MYF(0), table.str);
    return true;
  }
  if (!spv_table->field_def.ora_record.is_record_define) {
    my_error(ER_SP_MISMATCH_RECORD_VAR, MYF(0), table.str);
    return true;
  }
  if (spvar->field_def.init(
          thd, "", MYSQL_TYPE_NULL, nullptr, nullptr, 0, NULL, NULL, &NULL_CSTR,
          0, nullptr, thd->variables.collation_database, false, 0, nullptr,
          nullptr, nullptr, {}, dd::Column::enum_hidden_type::HT_VISIBLE)) {
    return true;
  }

  if (prepare_sp_create_field(thd, &spvar->field_def)) {
    return true;
  }

  Row_definition_list *row_new = nullptr;
  if (spv_table->field_def.ora_record.row_field_definitions()
          ->make_new_create_field_to_store_index(thd, length, is_index_by,
                                                 &row_new))
    return true;

  spvar->field_def.field_name = spvar->name.str;
  spvar->field_def.is_nullable = true;
  Row_definition_table_list *list =
      Row_definition_table_list::make(thd->mem_root, row_new);
  spvar->field_def.ora_record.set_row_field_table_definitions(list);
  spvar->field_def.ora_record.is_record_table_define = true;
  spvar->field_def.ora_record.is_index_by = is_index_by;
  spvar->field_def.nested_table_udt = to_lex_cstring(table);
  spvar->field_def.udt_name = spvar->name;
  spvar->field_def.udt_db_name = lex->sphead->m_db;
  spvar->field_def.ora_record.index_length = length ? std::atoi(length) : 0;
  sp_instr_setup_row_field *is = new (thd->mem_root)
      sp_instr_setup_row_field(sp->instructions(), pctx, spvar);

  if (!is || sp->add_instr(thd, is)) return true;
  return false;
}

bool LEX::sp_variable_declarations_type_table_type_finalize(THD *thd,
                                                            LEX_STRING ident,
                                                            PT_type *table_type,
                                                            const char *length,
                                                            bool is_index_by) {
  LEX *lex = thd->lex;
  sp_pcontext *pctx = lex->get_sp_current_parsing_ctx();
  sp_head *sp = lex->sphead;
  /*
    Prepare all row fields.
    Note, we do it only one time outside of the below loop.
    The converted list in "row" is further reused by all variable
    declarations processed by the current call.
    Example:
      DECLARE
        type stu_record_arr is table of stu_record;
      BEGIN
        ...
      END;
  */
  if (pctx->find_variable(ident.str, ident.length, false)) {
    my_error(ER_SP_DUP_PARAM, MYF(0), ident.str);
    return true;
  }

  sp_variable *spvar =
      pctx->add_variable(thd, ident, MYSQL_TYPE_DECIMAL, sp_variable::MODE_IN);

  if (spvar->field_def.init(
          thd, "", MYSQL_TYPE_NULL, nullptr, nullptr, 0, NULL, NULL, &NULL_CSTR,
          0, nullptr, thd->variables.collation_database, false, 0, nullptr,
          nullptr, nullptr, {}, dd::Column::enum_hidden_type::HT_VISIBLE)) {
    return true;
  }

  if (prepare_sp_create_field(thd, &spvar->field_def)) {
    return true;
  }
  Create_field *cdf = new (thd->mem_root) Create_field();
  if (cdf->init(thd, "column_value", table_type->type, table_type->get_length(),
                table_type->get_dec(), table_type->get_type_flags(), NULL, NULL,
                &NULL_CSTR, 0, table_type->get_interval_list(),
                thd->variables.collation_database, false,
                table_type->get_uint_geom_type(), nullptr, nullptr, nullptr, {},
                dd::Column::enum_hidden_type::HT_VISIBLE)) {
    return true;
  }
  if (prepare_sp_create_field(thd, cdf)) {
    return true;
  }
  Row_definition_list *rdl = Row_definition_list::make(thd->mem_root, cdf);
  Row_definition_list *row_new = nullptr;
  if (rdl->make_new_create_field_to_store_index(thd, length, is_index_by,
                                                &row_new))
    return true;
  spvar->field_def.field_name = spvar->name.str;
  spvar->field_def.is_nullable = true;
  Row_definition_table_list *list =
      Row_definition_table_list::make(thd->mem_root, row_new);
  spvar->field_def.ora_record.set_row_field_table_definitions(list);
  spvar->field_def.ora_record.is_record_table_define = true;
  spvar->field_def.ora_record.is_record_table_type_define = true;
  spvar->field_def.ora_record.is_index_by = is_index_by;
  spvar->field_def.udt_name = spvar->name;
  spvar->field_def.udt_db_name = lex->sphead->m_db;
  if (length) {
    spvar->field_def.ora_record.index_length = std::atoi(length);
  }
  sp_instr_setup_row_field *is = new (thd->mem_root)
      sp_instr_setup_row_field(sp->instructions(), pctx, spvar);

  if (!is || sp->add_instr(thd, is)) return true;
  return false;
}

bool LEX::sp_variable_declarations_type_refcursor_finalize(
    THD *thd, LEX_STRING ident, Sp_decl_cursor_return *return_type) {
  LEX *lex = thd->lex;
  sp_pcontext *pctx = lex->get_sp_current_parsing_ctx();
  sp_head *sp = lex->sphead;
  /*
    type mycur is ref cursor;
  */
  if (pctx->find_variable(ident.str, ident.length, true)) {
    my_error(ER_SP_DUP_PARAM, MYF(0), ident.str);
    return true;
  }
  sp_variable *spvar =
      pctx->add_variable(thd, ident, MYSQL_TYPE_DECIMAL, sp_variable::MODE_IN);
  uint offp = 0;
  if (pctx->find_cursor(ident, &offp, false)) {
    my_error(ER_SP_DUP_CURS, MYF(0), ident.str);
    return true;
  }
  if (pctx->add_cursor_parameters(thd, ident, pctx, &offp, spvar)) {
    return true;
  }
  sp_pcursor *pcursor = pctx->find_cursor_parameters(offp);
  if (!pcursor) {
    my_error(ER_SP_CURSOR_MISMATCH, MYF(0), ident.str);
    return true;
  }
  if (spvar->field_def.init(
          thd, "", MYSQL_TYPE_NULL, nullptr, nullptr, 0, NULL, NULL, &NULL_CSTR,
          0, nullptr, thd->variables.collation_database, false, 0, nullptr,
          nullptr, nullptr, {}, dd::Column::enum_hidden_type::HT_VISIBLE)) {
    return true;
  }

  if (prepare_sp_create_field(thd, &spvar->field_def)) {
    return true;
  }
  spvar->field_def.ora_record.is_ref_cursor = true;
  spvar->field_def.ora_record.is_ref_cursor_define = true;
  if (set_static_cursor_with_return_type(thd, return_type, offp, ident))
    return true;
  if (pcursor->get_cursor_rowtype_offset() != -1) {
    sp_instr_cursor_copy_struct *copy_struct = new (thd->mem_root)
        sp_instr_cursor_copy_struct(sp->instructions(), pctx,
                                    pcursor->get_cursor_rowtype_offset(),
                                    spvar->offset);
    if (!copy_struct || sp->add_instr(thd, copy_struct)) return true;
  }
  spvar->field_def.ora_record.set_row_field_definitions(
      pcursor->get_row_definition_list());
  spvar->field_def.ora_record.set_table_rowtype_ref(pcursor->get_table_ref());
  if (pcursor->get_table_ref())
    spvar->field_def.ora_record.set_row_field_definitions(
        new (thd->mem_root) Row_definition_list());
  return false;
}

/*
1.Type is c return record ==> return rdl
2.Type is c return table%rowtype ==> return table_ref
3.Type is c return ref_cursor%rowtype ==> return rdl or table_ref
4.Type is c return static_cursor%rowtype ==> return cursor offset or rdl or
table_ref
*/
bool LEX::sp_variable_declarations_define_cursor_return_type(
    THD *thd, Sp_decl_cursor_return *return_type, Table_ident **table_ref,
    Row_definition_list **rdl, int *offset) {
  // it has no return type.
  *offset = -1;
  if (!return_type) return false;
  LEX *lex = thd->lex;
  sp_pcontext *pctx = lex->get_sp_current_parsing_ctx();
  // TYPE ref_rs1 IS REF CURSOR RETURN record;
  if (return_type->ident.length) {
    // get record define.
    sp_variable *spv_type = pctx->find_variable(
        return_type->ident.str, return_type->ident.length, false);
    if (!spv_type) {
      my_error(ER_SP_UNDECLARED_VAR, MYF(0), return_type->ident.str);
      return true;
    }
    if (!spv_type->field_def.ora_record.is_record_define &&
        !spv_type->field_def.ora_record.is_ref_cursor_define) {
      my_error(
          ER_NOT_SUPPORTED_YET, MYF(0),
          "non record_type or non ref_cursor_type used in cursor return type");
      return true;
    }
    if (!(*rdl = spv_type->field_def.ora_record.row_field_definitions())) {
      my_error(ER_SP_MISMATCH_RECORD_VAR, MYF(0), spv_type->name.str);
      return true;
    }
  } else if (return_type->type ==
             1) {  // TYPE ref_rs1 IS REF CURSOR RETURN %rowtype
    int rc =
        check_percent_rowtype(thd, return_type->ref, table_ref, rdl, offset);
    if (rc == -1) return true;
  } else if (return_type->type ==
             2) {  // TYPE ref_rs1 IS REF CURSOR RETURN %type
    if (check_cursor_type(thd, return_type->ref, rdl)) return true;
  }
  return false;
}

/**
 * for cursor return type
 e.g:
  TYPE ref_rs1 IS REF CURSOR RETURN c%rowtype
  CURSOR c1 return ref_rs1%rowtype IS SELECT a FROM t1 WHERE a>3;
 @retval -1 on error.
 @retval 0 return rdl or table_ref
 @retval 1 return cursor offset.
*/
int LEX::check_percent_rowtype(THD *thd, Qualified_column_ident *ref,
                               Table_ident **table_ref,
                               Row_definition_list **rdl, int *offset) {
  LEX *lex = thd->lex;
  sp_pcontext *pctx = lex->get_sp_current_parsing_ctx();
  if (ref->db.length) {
    my_error(ER_SP_MISMATCH_VAR_QUANTITY, MYF(0));
    return -1;
  }
  bool cursor_rowtype = ref->table.str && ref->db.str
                            ? false
                            : pctx->find_cursor(to_lex_string(ref->m_column),
                                                (uint *)offset, false);
  sp_pcursor *pcursor = nullptr;
  // cursor return cursor%rowtype
  if (cursor_rowtype) {
    pcursor = pctx->find_cursor_parameters(*offset);
    if (!pcursor) {
      my_error(ER_SP_CURSOR_MISMATCH, MYF(0), ref->m_column.str);
      return -1;
    }
    sp_variable *spv_cursor =
        pctx->find_variable(ref->m_column.str, ref->m_column.length, false);
    if (spv_cursor && spv_cursor->field_def.ora_record.is_ref_cursor_define) {
      my_error(ER_NOT_SUPPORTED_YET, MYF(0),
               "non table, cursor or cursor-variable used in cursor return "
               "%rowtype");
      return true;
    }
    if ((*rdl = pcursor->get_row_definition_list()))
      return 0;
    else
      return 1;
  }
  // cursor return table%rowtype
  if (unlikely(!(*table_ref = new (thd->mem_root)
                     Table_ident(ref->table, ref->m_column))))
    return true;

  return 0;
}

/**
 * for cursor return type
 e.g:
  TYPE ref_rs1 IS REF CURSOR RETURN record%type / table.column%type
  CURSOR c1 return ref_rs1%type IS SELECT a FROM t1 WHERE a>3;
 @retval true on error.
 @retval false return rdl
*/
bool LEX::check_cursor_type(THD *thd, Qualified_column_ident *ref,
                            Row_definition_list **rdl) {
  LEX *lex = thd->lex;
  sp_pcontext *pctx = lex->get_sp_current_parsing_ctx();

  sp_variable *record_type =
      ref->table.str && ref->db.str
          ? nullptr
          : pctx->find_variable(ref->m_column.str, ref->m_column.length, false);
  // cursor return record%type
  if (record_type && !record_type->field_def.ora_record.is_record_define) {
    if ((*rdl = record_type->field_def.ora_record.row_field_definitions()))
      return false;
    else {
      my_error(ER_SP_MISMATCH_RECORD_VAR, MYF(0), ref->m_column.str);
      return true;
    }
  } else {
    my_error(ER_SP_MISMATCH_RECORD_VAR, MYF(0), ref->m_column.str);
    return true;
  }

  return false;
}

bool LEX::set_static_cursor_with_return_type(THD *thd,
                                             Sp_decl_cursor_return *return_type,
                                             uint off, LEX_STRING ident) {
  LEX *lex = thd->lex;
  sp_pcontext *pctx = lex->get_sp_current_parsing_ctx();
  Table_ident *table_ref = nullptr;
  Row_definition_list *rdl = nullptr;
  int offset = -1;
  if (sp_variable_declarations_define_cursor_return_type(
          thd, return_type, &table_ref, &rdl, &offset))
    return true;
  sp_pcursor *pcursor = pctx->find_cursor_parameters(off);
  if (!pcursor) {
    my_error(ER_SP_CURSOR_MISMATCH, MYF(0), ident.str);
    return true;
  }
  pcursor->set_table_ref(table_ref);
  pcursor->set_row_definition_list(rdl);
  pcursor->set_cursor_rowtype_offset(offset);
  return false;
}

bool LEX::sp_variable_declarations_define_ref_cursor_finalize(
    THD *thd, int nvars, Item *default_value, sp_variable *spvar_record) {
  LEX *lex = thd->lex;
  sp_pcontext *pctx = lex->get_sp_current_parsing_ctx();
  sp_head *sp = lex->sphead;
  if (default_value) {
    my_error(ER_NOT_SUPPORTED_YET, MYF(0),
             "ref cursor variable with default value");
    return true;
  }
  uint offp_record = 0;
  if (!pctx->find_cursor(spvar_record->name, &offp_record, false)) {
    my_error(ER_SP_CURSOR_MISMATCH, MYF(0), spvar_record->name.str);
    return true;
  }
  sp_pcursor *pcursor_record = pctx->find_cursor_parameters(offp_record);
  if (!pcursor_record) {
    my_error(ER_SP_CURSOR_MISMATCH, MYF(0), spvar_record->name.str);
    return true;
  }
  uint offp = 0;
  for (uint i = 0; i < (uint)nvars; i++) {
    sp_variable *spvar = pctx->get_last_context_variable((uint)nvars - 1 - i);
    if (pctx->find_cursor(spvar->name, &offp, false)) {
      my_error(ER_SP_DUP_CURS, MYF(0), spvar->name.str);
      return true;
    }
    if (pctx->add_cursor_parameters(thd, spvar->name, pctx, &offp, spvar)) {
      return true;
    }
    if (spvar->field_def.init(thd, spvar->name.str, MYSQL_TYPE_NULL, nullptr,
                              nullptr, 0, NULL, NULL, &NULL_CSTR, 0, nullptr,
                              thd->variables.collation_database, false, 0,
                              nullptr, nullptr, nullptr, {},
                              dd::Column::enum_hidden_type::HT_VISIBLE)) {
      return true;
    }

    if (prepare_sp_create_field(thd, &spvar->field_def)) return true;
    spvar->field_def.ora_record.is_ref_cursor = true;
    spvar->field_def.ora_record.m_cursor_offset = offp;
    // cursor return type
    sp_pcursor *pcursor = pctx->find_cursor_parameters(offp);
    if (!pcursor) {
      my_error(ER_SP_CURSOR_MISMATCH, MYF(0), spvar->name.str);
      return -1;
    }

    if (pcursor_record->has_return_type()) {
      sp_instr_cursor_copy_struct *copy_struct =
          new (thd->mem_root) sp_instr_cursor_copy_struct(
              sp->instructions(), pctx, offp_record, spvar->offset);
      if (!copy_struct || sp->add_instr(thd, copy_struct)) return true;
    }
    spvar->field_def.udt_name = spvar_record->name;
    spvar->field_def.udt_db_name = lex->sphead->m_db;
  }
  return false;
}

bool LEX::sp_variable_declarations_type_table_rowtype_finalize(
    THD *thd, LEX_STRING ident, Qualified_column_ident *table,
    const char *length, bool is_index_by) {
  LEX *lex = thd->lex;
  sp_pcontext *pctx = lex->get_sp_current_parsing_ctx();
  sp_head *sp = lex->sphead;
  /*
    Prepare all row fields.
    Note, we do it only one time outside of the below loop.
    The converted list in "row" is further reused by all variable
    declarations processed by the current call.
    Example:
      DECLARE
        type stu_record_arr is table of t1%ROWTYPE/cursor%rowtype;
      BEGIN
        ...
      END;
  */
  if (pctx->find_variable(ident.str, ident.length, false)) {
    my_error(ER_SP_DUP_PARAM, MYF(0), ident.str);
    return true;
  }

  sp_variable *spvar =
      pctx->add_variable(thd, ident, MYSQL_TYPE_NULL, sp_variable::MODE_IN);

  Table_ident *table_ref = nullptr;
  Row_definition_list *rdl = nullptr;
  int offset = -1;
  int rc = check_percent_rowtype(thd, table, &table_ref, &rdl, &offset);
  if (rc == -1) return true;

  if (offset != -1) {  // type is table of cursor%rowtype
    if (spvar->field_def.init(thd, ident.str, MYSQL_TYPE_NULL, nullptr, nullptr,
                              0, NULL, NULL, &NULL_CSTR, 0, nullptr,
                              thd->variables.collation_database, false, 0,
                              nullptr, nullptr, nullptr, {},
                              dd::Column::enum_hidden_type::HT_VISIBLE)) {
      return true;
    }
    if (prepare_sp_create_field(thd, &spvar->field_def)) {
      return true;
    }
    spvar->field_def.is_nullable = true;
    spvar->field_def.udt_name = spvar->name;
    spvar->field_def.udt_db_name = lex->sphead->m_db;
    spvar->field_def.ora_record.set_cursor_rowtype_ref(offset);
    // make a empty Row_definition_list
    rdl = new (thd->mem_root) Row_definition_list();
    sp_instr_cursor_copy_struct *copy_struct =
        new (thd->mem_root) sp_instr_cursor_copy_struct(
            sp->instructions(), pctx, offset, spvar->offset);
    if (!copy_struct || sp->add_instr(thd, copy_struct)) return true;
  } else if (!rdl) {  // type is table of table%rowtype
    if (sp_variable_declarations_table_rowtype_make_field_def(
            thd, spvar, table->table, table->m_column, &rdl))
      return true;
  }

  spvar->field_def.ora_record.set_row_field_table_definitions(
      Row_definition_table_list::make(thd->mem_root, rdl));
  spvar->field_def.ora_record.is_record_table_define = true;
  spvar->field_def.ora_record.is_index_by = is_index_by;
  spvar->field_def.nested_table_udt = to_lex_cstring(ident);

  if (length) {
    spvar->field_def.ora_record.index_length = std::atoi(length);
  }
  sp_instr_setup_row_field *is = new (thd->mem_root)
      sp_instr_setup_row_field(sp->instructions(), pctx, spvar);

  if (!is || sp->add_instr(thd, is)) return true;
  return false;
}

bool LEX::ora_sp_forall_loop_insert_value(THD *thd, LEX_CSTRING query,
                                          LEX_STRING ident_i, const char *start,
                                          const char *end) {
  LEX *lex = thd->lex;
  sp_head *sp = lex->sphead;
  sp_pcontext *pctx = lex->get_sp_current_parsing_ctx();

  sp_instr_stmt *i =
      new (thd->mem_root) sp_instr_stmt(sp->instructions(), lex, query);

  if (!i || sp->add_instr(thd, i)) return true;
  if (sp->restore_lex(thd)) return true;

  thd->lex->sphead->reset_lex(thd);
  lex = thd->lex;
  sp_variable *spvar_ident_i =
      pctx->find_variable(ident_i.str, ident_i.length, true);
  Item_splocal *ident_splocal = create_item_for_sp_var(
      thd, to_lex_cstring(ident_i), &sp_rcontext_handler_local, spvar_ident_i,
      sp->m_parser_data.get_current_stmt_start_ptr(), start, end);
  Item *expr = pctx->make_item_plsql_plus_one(thd, 1, ident_splocal);
  sp_instr_set *is = new (thd->mem_root)
      sp_instr_set(sp->instructions(), lex, spvar_ident_i->offset, expr,
                   EMPTY_CSTR, true, pctx, &sp_rcontext_handler_local);

  if (!is || sp->add_instr(thd, is)) return true;
  sp_label *lab = pctx->pop_label();
  sp_instr_jump *i_jump =
      new (thd->mem_root) sp_instr_jump(sp->instructions(), pctx, lab->ip);
  if (!i_jump || sp->add_instr(thd, i_jump)) return true;
  sp->m_parser_data.do_backpatch(lab, sp->instructions());

  sp->m_parser_data.do_cont_backpatch(sp->instructions());
  if (sp->restore_lex(thd)) return true;
  return false;
}

bool LEX::ora_sp_for_loop_index_and_bounds(
    THD *thd, LEX_STRING ident, Oracle_sp_for_loop_bounds *sp_for_loop_bounds,
    uint *var_offset, const char *start, const char *end,
    Item_splocal **cursor_var_item) {
  LEX *lex = thd->lex;
  sp_head *sp = lex->sphead;
  sp_pcontext *pctx = lex->get_sp_current_parsing_ctx();

  uint offset = sp_for_loop_bounds->offset;

  sp_variable *spvar = nullptr;
  if (pctx->find_variable(ident.str, ident.length, true)) {
    my_error(ER_SP_DUP_VAR, MYF(0), ident.str);
    return true;
  }
  sp_assignment_lex *lex_expr_from = sp_for_loop_bounds->from;
  Item *dflt_value_item = lex_expr_from->get_item();

  if (sp_for_loop_bounds->is_cursor) {
    spvar =
        pctx->add_variable(thd, ident, MYSQL_TYPE_NULL, sp_variable::MODE_IN);

    *var_offset = spvar->offset;
    if (spvar->field_def.init(thd, "", MYSQL_TYPE_NULL, nullptr, nullptr, 0,
                              NULL, NULL, &NULL_CSTR, 0, nullptr,
                              thd->variables.collation_database, false, 0,
                              nullptr, nullptr, nullptr, {},
                              dd::Column::enum_hidden_type::HT_VISIBLE)) {
      return true;
    }

    if (prepare_sp_create_field(thd, &spvar->field_def)) {
      return true;
    }
    spvar->field_def.field_name = spvar->name.str;
    spvar->field_def.is_nullable = true;
    spvar->field_def.udt_name = spvar->name;
    spvar->field_def.udt_db_name = lex->sphead->m_db;

    sp_pcursor *pcursor = pctx->find_cursor_parameters(offset);
    if (!pcursor) {
      my_error(ER_SP_CURSOR_MISMATCH, MYF(0), spvar->name.str);
      return true;
    }
    spvar->field_def.ora_record.set_record_table_rowtype_ref(
        pcursor->get_table_ref());
    spvar->field_def.ora_record.set_row_field_definitions(
        pcursor->get_row_definition_list());
    if (!pcursor->get_row_definition_list() && !pcursor->get_table_ref()) {
      spvar->field_def.ora_record.set_cursor_rowtype_ref(offset);

      sp_instr_cursor_copy_struct *copy_struct =
          new (thd->mem_root) sp_instr_cursor_copy_struct(
              sp->instructions(), pctx, offset, spvar->offset);
      if (!copy_struct || sp->add_instr(thd, copy_struct)) return true;
    } else {
      if (!spvar->field_def.ora_record.row_field_definitions())
        spvar->field_def.ora_record.set_row_field_definitions(
            new (thd->mem_root) Row_definition_list());

      sp_instr_setup_row_field *is = new (thd->mem_root)
          sp_instr_setup_row_field(sp->instructions(), pctx, spvar);
      if (!is || sp->add_instr(thd, is)) return true;
    }

    // OPEN Cursor;
    sp_instr_copen *iOpen =
        new (thd->mem_root) sp_instr_copen(sp->instructions(), pctx, offset);

    if (!iOpen || sp->add_instr(thd, iOpen)) return true;

    // FETCH Cursor
    sp_instr_cfetch *i =
        new (thd->mem_root) sp_instr_cfetch(sp->instructions(), pctx, offset);

    if (!i || sp->add_instr(thd, i)) return true;

    sp_variable *spv = pctx->find_variable(ident.str, ident.length, false);
    i->add_to_varlist(spv);
  } else {
    /*no cursor type,for i in expr .. expr*/
    sp_assignment_lex *lex_expr_to = sp_for_loop_bounds->to;
    if (sp_for_loop_bounds->direction < 0)
      dflt_value_item = lex_expr_to->get_item();

    spvar =
        pctx->add_variable(thd, ident, MYSQL_TYPE_LONG, sp_variable::MODE_IN);

    spvar->type = MYSQL_TYPE_LONG;
    spvar->default_value = dflt_value_item;
    if (spvar->field_def.init(thd, "", MYSQL_TYPE_LONG, nullptr, nullptr, 0,
                              NULL, NULL, &NULL_CSTR, 0, nullptr,
                              thd->variables.collation_database, false, 0,
                              nullptr, nullptr, nullptr, {},
                              dd::Column::enum_hidden_type::HT_VISIBLE)) {
      return true;
    }
    if (prepare_sp_create_field(thd, &spvar->field_def)) {
      return true;
    }
    spvar->field_def.field_name = spvar->name.str;
    spvar->field_def.is_nullable = true;

    *var_offset = spvar->offset;
    sp_instr_set *is = new (thd->mem_root) sp_instr_set(
        sp->instructions(),
        sp_for_loop_bounds->direction > 0 ? lex_expr_from : lex_expr_to,
        spvar->offset, dflt_value_item,
        sp_for_loop_bounds->direction > 0 ? lex_expr_from->get_value_query()
                                          : lex_expr_to->get_value_query(),
        true, pctx, &sp_rcontext_handler_local);

    if (!is || sp->add_instr(thd, is)) return true;
    *cursor_var_item = create_item_for_sp_var(
        thd, to_lex_cstring(ident), &sp_rcontext_handler_local, spvar,
        sp->m_parser_data.get_current_stmt_start_ptr(), start, end);
    if (*cursor_var_item == nullptr) return true;
  }

  spvar->field_def.ora_record.is_for_loop_var = true;
  return false;
}

bool LEX::make_temp_upper_bound_variable_and_set(THD *thd, Item *item_uuid,
                                                 int m_direction,
                                                 sp_assignment_lex *lex_from,
                                                 sp_assignment_lex *lex_to,
                                                 Item **item_out) {
  LEX *lex = thd->lex;
  sp_head *sp = lex->sphead;
  sp_pcontext *pctx = lex->get_sp_current_parsing_ctx();
  /* value_to must be evaluated once only, make a temporary int variable to
   * store the value_to.
   * 34 = strlen('alias_forto_')+item_uuid->max_length+'\0'
   * item_uuid->max_length = 21
   */
  char temp_ptr[34];
  memset(temp_ptr, 0, sizeof(temp_ptr));
  longlong uuid = item_uuid->val_int();

  sprintf(temp_ptr, "alias_forto_%lld", uuid);
  char *ptr = thd->strmake(temp_ptr, strlen(temp_ptr));
  LEX_STRING name{ptr, strlen(ptr)};
  sp_variable *spvar =
      pctx->add_variable(thd, name, MYSQL_TYPE_LONG, sp_variable::MODE_IN);

  if (spvar->field_def.init(
          thd, "", MYSQL_TYPE_LONG, nullptr, nullptr, 0, NULL, NULL, &NULL_CSTR,
          0, nullptr, thd->variables.collation_database, false, 0, nullptr,
          nullptr, nullptr, {}, dd::Column::enum_hidden_type::HT_VISIBLE)) {
    return true;
  }
  if (prepare_sp_create_field(thd, &spvar->field_def)) {
    return true;
  }
  spvar->field_def.field_name = spvar->name.str;
  spvar->field_def.is_nullable = true;

  Item *expr = m_direction > 0 ? lex_to->get_item() : lex_from->get_item();
  sp_assignment_lex *lex_assign = m_direction > 0 ? lex_to : lex_from;
  sp_instr_set *is = new (thd->mem_root) sp_instr_set(
      sp->instructions(), lex_assign, spvar->offset, expr,
      lex_assign->get_value_query(), true, pctx, &sp_rcontext_handler_local);

  if (!is || sp->add_instr(thd, is)) return true;
  *item_out = new (thd->mem_root) Item_splocal(
      &sp_rcontext_handler_local, spvar->name, spvar->offset, spvar->type);
#ifndef NDEBUG
  Item_splocal *splocal = dynamic_cast<Item_splocal *>(*item_out);
  if (splocal) splocal->m_sp = thd->lex->sphead;
#endif

  return false;
}

bool LEX::sp_goto_statement(THD *thd, LEX_CSTRING label_name) {
  LEX *lex = thd->lex;
  sp_head *sp = lex->sphead;
  sp_pcontext *spcont = lex->get_sp_current_parsing_ctx();
  sp_label *lab = spcont->find_goto_label(label_name, true);
  if (!lab || lab->ip == 0) {
    sp_label *delayedlabel;
    if (!lab) {
      // Label not found --> add forward jump to an unknown label
      spcont->push_goto_label(thd, label_name, 0);
      delayedlabel = spcont->last_goto_label();
    } else {
      delayedlabel = lab;
    }
    return sp->m_parser_data.add_backpatch_goto(thd, spcont, delayedlabel);
  } else {
    // Label found (backward goto)
    uint ip = sp->instructions();
    /* 1.for next case,it's unallowed.
      BEGIN
        WHILE 1=1 loop
          <<lab18_1>>
          a := 2;
        END LOOP;
        goto lab18_1; ==> find_label_by_ip() == false
      END;
      2.for next case,it's allowed.
      BEGIN
        <<lab18_1>>  beginblocklabel->ip > lab->ip,
        WHILE 1=1 loop
          a := 2;
          goto lab18_1; ==> find_label_by_ip() == true
        END LOOP;
      END;
      3.for next case,it's unallowed.
      BEGIN WHILE 1=1 loop
        <<lab18_1>>
          a := 2;
        END LOOP;
        WHILE 1=1 loop
          goto lab18_1; ==> find_label_by_ip() == false
        END LOOP;
      END;
      4.for next case,it's allowed.
      BEGIN
        WHILE 1=1 loop
        <<lab18_1>>
          WHILE 1=1 loop
            goto lab18_1; ==> find_label_by_ip() == true
          END LOOP;
        END LOOP;
      END;
    */
    if (!spcont->find_label_by_ip(lab->beginblock_label_ip)) {
      /*
        only jump target from the beginning of the block where the
        label is defined.
      */
      my_error(ER_SP_LILABEL_MISMATCH, MYF(0), "GOTO", lab->name.str);
      return true;
    }
    /* Inclusive the dest. */
    if (sp_change_context(thd, lab, true)) return true;

    /* Jump back */
    sp_instr_jump *i = new (thd->mem_root) sp_instr_jump(ip, spcont, lab->ip);
    if (!i || sp->add_instr(thd, i)) return true;
  }
  return false;
}

bool LEX::sp_push_goto_label(THD *thd, LEX_CSTRING label_name) {
  LEX *lex = thd->lex;
  sp_head *sp = lex->sphead;
  sp_pcontext *spcont = lex->get_sp_current_parsing_ctx();
  if (!spcont) return false;
  sp_label *lab = spcont->find_goto_label(label_name, false);
  if (lab) {
    if (lab->ip != 0) {
      my_error(ER_SP_LABEL_REDEFINE, MYF(0), label_name.str);
      return true;
    }
    lab->ip = sp->instructions();

    sp_label *beginblocklabel = spcont->find_label(EMPTY_CSTR);
    sp->m_parser_data.do_backpatch_goto(thd, lab, beginblocklabel,
                                        sp->instructions());
  } else {
    sp_label *beginblocklabel = spcont->find_label(EMPTY_CSTR);
    if (!beginblocklabel) {
      my_error(ER_NOT_SUPPORTED_YET, MYF(0),
               "label used in this block without BEGIN");
      return true;
    }
    spcont->push_goto_label(thd, label_name, sp->instructions());
    spcont->last_goto_label()->beginblock_label_ip = beginblocklabel->ip;
  }
  return false;
}

bool LEX::sp_if_sp_block_init(THD *thd) {
  return ((thd->variables.sql_mode & MODE_ORACLE) && sp_block_init(thd))
             ? true
             : false;
}

void LEX::sp_if_sp_block_finalize(THD *thd) {
  if (thd->variables.sql_mode & MODE_ORACLE)
    set_sp_current_parsing_ctx(get_sp_current_parsing_ctx()->pop_context());
}

bool LEX::close_ref_cursor(THD *thd, uint cursor_offset) {
  LEX *lex = thd->lex;
  sp_pcontext *pctx = lex->get_sp_current_parsing_ctx();
  sp_head *sp = lex->sphead;
  sp_instr_cclose *iclose = new (thd->mem_root)
      sp_instr_cclose(sp->instructions(), pctx, cursor_offset);
  if (!iclose || sp->add_instr(thd, iclose)) return true;
  const LEX_STRING *cursor_name = pctx->find_cursor(cursor_offset);
  sp_variable *spvar =
      cursor_name
          ? pctx->find_variable(cursor_name->str, cursor_name->length, false)
          : nullptr;
  if (spvar && spvar->field_def.ora_record.is_ref_cursor) {
    iclose->set_sysrefcursor_spvar(spvar);
  }
  return false;
}

bool LEX::sp_continue_loop(THD *thd, uint *offset, sp_label *lab) {
  LEX *lex = thd->lex;
  sp_pcontext *pctx = lex->get_sp_current_parsing_ctx();
  sp_head *sp = lex->sphead;
  Oracle_sp_for_loop_index_and_bounds *for_loop = lab->m_for_loop;
  if (for_loop) {
    // We're in a FOR loop, increment the index variable before backward jump
    sp_variable *spv = pctx->find_variable(for_loop->cursor_var.str,
                                           for_loop->cursor_var.length, false);

    if (for_loop->is_cursor) {
      // cfetch once more
      if (!pctx->find_cursor(for_loop->cursor_name, offset, false)) {
        my_error(ER_SP_CURSOR_MISMATCH, MYF(0), for_loop->cursor_name.str);
        return true;
      }
      sp_instr_cfetch *i_fetch = new (thd->mem_root)
          sp_instr_cfetch(sp->instructions(), pctx, *offset);
      if (!i_fetch || sp->add_instr(thd, i_fetch)) return true;

      i_fetch->add_to_varlist(spv);
    } else {
      /*set i=i+1 or i=i-1*/
      sp->reset_lex(thd);
      lex = thd->lex;
      sp = lex->sphead;
      uint var_idx = for_loop->var_index;

      Item *expr = pctx->make_item_plsql_plus_one(thd, for_loop->direction,
                                                  for_loop->cursor_var_item);
      sp_instr_set *is = new (thd->mem_root)
          sp_instr_set(sp->instructions(), lex, var_idx, expr, EMPTY_CSTR, true,
                       pctx, &sp_rcontext_handler_local);

      if (!is || sp->add_instr(thd, is)) return true;
      sp->restore_lex(thd);
    }
  }
  return false;
}

/*for next case,it should pop cursor select_stmt2 when continue label.
e.g:
begin
  <<label>>
  for i in (select_stmt1) loop
    for j in (select_stmt2) loop
      continue label;
    end loop;
  end loop;
end;
for next case,it should close cursor cc1 when continue label.
e.g:
begin
  <<label>>
  for i in cc loop
    for j in cc1 loop
      continue label;
    end loop;
  end loop;
end;*/
bool LEX::sp_change_context(THD *thd, sp_label *lab, bool is_goto) {
  LEX *lex = thd->lex;
  sp_pcontext *pctx = lex->get_sp_current_parsing_ctx();
  sp_head *sp = lex->sphead;

  // 1.close opened cursors
  for (sp_pcontext *pctx_tmp = pctx;
       pctx_tmp != (is_goto ? lab->ctx->parent_context() : lab->ctx);
       pctx_tmp = pctx_tmp->parent_context()) {
    sp_label *lab_tmp = pctx_tmp->find_continue_label_current_loop_start(true);
    if (!lab_tmp) continue;
    /*for next case,it doesn't close cc when goto lab. e.g:
    <<lab>>
      for i in cc loop
        goto lab;*/
    if (is_goto && lab_tmp->name.str && lab->name.str &&
        my_strcasecmp(system_charset_info, lab_tmp->name.str, lab->name.str) ==
            0)
      break;
    uint cursor_offset = 0;
    if (lab_tmp->m_for_loop && lab_tmp->m_for_loop->is_cursor &&
        // If it's not for in (select statement)
        !lab_tmp->m_for_loop->is_select_loop) {
      if (!pctx->find_cursor(lab_tmp->m_for_loop->cursor_name, &cursor_offset,
                             false)) {
        my_error(ER_SP_CURSOR_MISMATCH, MYF(0),
                 lab_tmp->m_for_loop->cursor_name.str);
        return true;
      }
      sp_instr_cclose *iclose = new (thd->mem_root)
          sp_instr_cclose(sp->instructions(), pctx, cursor_offset);
      if (!iclose || sp->add_instr(thd, iclose)) return true;
    }
  }
  // 2.pop cpush cursors
  uint cursor_off =
      (lab->m_for_loop && lab->m_for_loop->is_select_loop) ? 1 : 0;
  size_t n = pctx->diff_handlers(lab->ctx, false);
  size_t m = pctx->diff_cursors(lab->ctx, false) - cursor_off;
  sp_instr_goto *goto_instr = nullptr;
  if (is_goto)
    goto_instr = new (thd->mem_root) sp_instr_goto(sp->instructions(), pctx);
  else
    goto_instr =
        new (thd->mem_root) sp_instr_continue(sp->instructions(), pctx);
  if (!goto_instr || sp->add_instr(thd, goto_instr)) return true;
  if (n) {
    goto_instr->update_handler_count(n);
  } else
    goto_instr->update_ignore_handler_execute(true);

  if (m) {
    goto_instr->update_cursor_count(m);
  } else
    goto_instr->update_ignore_cursor_execute(true);

  return false;
}

bool LEX::sp_continue_statement(THD *thd, LEX_CSTRING name) {
  LEX *lex = thd->lex;
  sp_head *sp = lex->sphead;
  sp_pcontext *pctx = lex->get_sp_current_parsing_ctx();
  sp_label *lab = name.str ? pctx->find_continue_label(name)
                           : pctx->find_continue_label_current_loop_start();

  if (name.str && !lab) {
    my_error(ER_SP_LILABEL_MISMATCH, MYF(0), "CONTINUE", name.str);
    return true;
  }
  if (!lab || lab->type != sp_label::ITERATION) {
    my_error(ER_NOT_SUPPORTED_YET, MYF(0),
             "CONTINUE clause without loop statement");
    return true;
  }
  uint offset = 0;
  if (sp_continue_loop(thd, &offset, lab) || sp_change_context(thd, lab))
    return true;

  sp_instr_jump *i =
      new (thd->mem_root) sp_instr_jump(sp->instructions(), pctx, lab->loop_ip);

  if (!i || sp->add_instr(thd, i)) return true;
  return false;
}

bool LEX::sp_continue_when_statement(THD *thd, LEX_CSTRING name, Item *expr,
                                     const char *loc_start,
                                     const char *loc_end) {
  LEX *lex = thd->lex;
  sp_head *sp = lex->sphead;
  sp_pcontext *pctx = lex->get_sp_current_parsing_ctx();
  sp_label *lab = name.str ? pctx->find_continue_label(name)
                           : pctx->find_continue_label_current_loop_start();
  if (name.str && !lab) {
    my_error(ER_SP_LILABEL_MISMATCH, MYF(0), "CONTINUE", name.str);
    return true;
  }
  if (!lab || lab->type != sp_label::ITERATION) {
    my_error(ER_NOT_SUPPORTED_YET, MYF(0),
             "CONTINUE clause without loop statement");
    return true;
  }
  // Extract expression string.
  LEX_CSTRING expr_query = EMPTY_CSTR;
  const char *expr_start_ptr = loc_start;

  if (lex->is_metadata_used()) {
    expr_query = make_string(thd, expr_start_ptr, loc_end);
    if (!expr_query.str) return true;
  }
  sp_instr_jump_if_not *i = new (thd->mem_root)
      sp_instr_jump_if_not(sp->instructions(), lex, expr, expr_query);

  // Add jump instruction.
  if (i == NULL ||
      sp->m_parser_data.add_backpatch_entry(
          i, pctx->push_label(thd, EMPTY_CSTR, sp->instructions())) ||
      // sp->m_parser_data.add_cont_backpatch_entry(i) ||
      sp->add_instr(thd, i) || sp->restore_lex(thd)) {
    return true;
  }

  uint offset = 0;
  if (sp_continue_loop(thd, &offset, lab) || sp_change_context(thd, lab, false))
    return true;

  sp_instr_jump *i_jump_end =
      new (thd->mem_root) sp_instr_jump(sp->instructions(), pctx, lab->loop_ip);

  if (!i_jump_end || sp->add_instr(thd, i_jump_end)) return true;

  sp->m_parser_data.do_backpatch(pctx->pop_label(), sp->instructions());
  return false;
}

/**oracle sp end **/

PT_set *LEX::dbmsotpt_set_serveroutput(THD *thd, bool enabled, YYLTYPE pos_set,
                                       YYLTYPE pos_val) {
  Item *val_enabled = enabled ? new (thd->mem_root) Item_int(1)
                              : new (thd->mem_root) Item_int_0();

  LEX_CSTRING val_str = {"SERVEROUTPUT", strlen("SERVEROUTPUT")};
  Bipartite_name val_name = Bipartite_name{{}, val_str};

  PT_option_value_no_option_type *option_value =
      new (thd->mem_root) PT_set_system_variable(
          OPT_DEFAULT, pos_val, val_name.prefix, val_name.name, val_enabled);

  PT_start_option_value_list *value_list = new (thd->mem_root)
      PT_start_option_value_list_no_type(option_value, pos_val, nullptr);

  PT_set *ret = new (thd->mem_root) PT_set(pos_set, value_list);

  return ret;
}
