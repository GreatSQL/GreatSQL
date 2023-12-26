#ifndef SQL_CREATE_FIELD_INCLUDED
#define SQL_CREATE_FIELD_INCLUDED

/* Copyright (c) 2018, 2022, Oracle and/or its affiliates. All rights reserved.
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

#include <optional>

#include "field_types.h"
#include "lex_string.h"
#include "m_ctype.h"
#include "my_alloc.h"
#include "my_base.h"
#include "sql/dd/types/column.h"
#include "sql/field.h"
#include "sql/gis/srid.h"
#include "sql/sql_lex.h"  // Table_ident, Qualified_column_ident
#include "sql/sql_list.h"
#include "typelib.h"

class Item;
class String;
class Value_generator;

/**
  List of record element definitions, e.g.:
    DECLARE type stu_record_arr is table of stu_record;
*/
class Row_definition_table_list : public List<class Row_definition_list> {
 public:
  static Row_definition_table_list *make(MEM_ROOT *mem_root,
                                         Row_definition_list *var) {
    Row_definition_table_list *list;
    if (!(list = new (mem_root) Row_definition_table_list())) return NULL;
    return list->push_back(var, mem_root) ? NULL : list;
  }
  bool append_uniq(MEM_ROOT *thd, Row_definition_list *var);
  Row_definition_list *find_row_fields_by_offset(uint offset) const;
  Create_field *find_row_field_by_name(LEX_STRING name, uint *offset) const;
};

/**
  List of record element definitions, e.g.:
    DECLARE type rec_tk is record(a INT := 0,b VARCHAR(10))
*/
class Row_definition_list : public List<class Create_field> {
 private:
  int m_number;  // def_name(num).b
 public:
  inline bool eq_name(const Create_field *def, LEX_STRING name) const;
  /**
    Find a ROW field by name.
    @param [IN]  name   - the name
    @param [OUT] offset - if the ROW field found, its offset it returned here
    @retval NULL        - the ROW field was not found
    @retval !NULL       - the pointer to the found ROW field
  */
  Create_field *find_row_field_by_name(LEX_STRING name, uint *offset) const;
  Create_field *find_row_field_by_offset(uint offset) const;
  static Row_definition_list *make(MEM_ROOT *mem_root, Create_field *var) {
    Row_definition_list *list;
    if (!(list = new (mem_root) Row_definition_list())) return NULL;
    return list->push_back(var, mem_root) ? NULL : list;
  }
  bool append_uniq(MEM_ROOT *thd, Create_field *var);
  int get_number() { return m_number; }
  void set_number(int num) { m_number = num; }
  bool make_new_create_field_to_store_index(
      THD *thd, const char *str_length, bool is_index_by,
      Row_definition_list **list_new) const;
};

class Ora_record_refer {
  Row_definition_list *m_row_field_definitions;  // for record
  Row_definition_table_list
      *m_row_field_table_definitions;  // for type of table
  Table_ident *m_table_rowtype_ref;    // for table%ROWTYPE

 public:
  bool m_cursor_rowtype_ref;                  // for cursor%ROWTYPE
  Qualified_column_ident *m_column_type_ref;  // for %TYPE
  uint m_cursor_rowtype_offset;               // for cursor%ROWTYPE
  bool is_record_define;                      // for type of record()
  bool is_record_table_define;                // for type is table of ident
  bool is_record_table_type_define;  // for type is table of varchar(20)
  bool is_for_loop_var;              // for i in
  bool is_record_table_ref;
  bool is_forall_loop_var;     // forall i in
  Item *record_default_value;  // used for type is record default value
  /*For type is table of index by varchar(xx):
      1<= index_length <=32767
    For type is table of index by int(default):
      index_length = 0
  */
  int index_length;
  bool is_index_by;  // for TYPE IS table OF index by
  bool is_ref_cursor;
  bool is_ref_cursor_define;
  int m_cursor_offset;  // for c sys_refcursor

  Ora_record_refer()
      : m_row_field_definitions(nullptr),
        m_row_field_table_definitions(nullptr),
        m_table_rowtype_ref(nullptr),
        m_cursor_rowtype_ref(false),
        m_column_type_ref(nullptr),
        m_cursor_rowtype_offset(0),
        is_record_define(false),
        is_record_table_define(false),
        is_record_table_type_define(false),
        is_for_loop_var(false),
        is_record_table_ref(false),
        is_forall_loop_var(false),
        record_default_value(nullptr),
        index_length(0),
        is_index_by(false),
        is_ref_cursor(false),
        is_ref_cursor_define(false),
        m_cursor_offset(-1) {}
  void set_record(Ora_record_refer *from) {
    m_row_field_definitions = from->row_field_definitions();
    m_row_field_table_definitions = from->row_field_table_definitions();
    m_table_rowtype_ref = from->table_rowtype_ref();
    m_cursor_rowtype_ref = from->m_cursor_rowtype_ref;
    m_column_type_ref = from->m_column_type_ref;
    m_cursor_rowtype_offset = from->m_cursor_rowtype_offset;
    is_record_define = from->is_record_define;
    is_record_table_define = from->is_record_table_define;
    is_record_table_type_define = from->is_record_table_type_define;
    is_for_loop_var = from->is_for_loop_var;
    is_record_table_ref = from->is_record_table_ref;
    is_forall_loop_var = from->is_forall_loop_var;
    record_default_value = from->record_default_value;
    index_length = from->index_length;
    is_index_by = from->is_index_by;
    is_ref_cursor = from->is_ref_cursor;
    is_ref_cursor_define = from->is_ref_cursor_define;
    m_cursor_offset = from->m_cursor_offset;
  }
  bool is_cursor_rowtype_ref() const { return m_cursor_rowtype_ref; }
  bool is_column_type_ref() const { return m_column_type_ref != nullptr; }
  bool is_table_rowtype_ref() const { return m_table_rowtype_ref != nullptr; }

  Qualified_column_ident *column_type_ref() const { return m_column_type_ref; }

  void set_column_type_ref(Qualified_column_ident *ref) {
    m_column_type_ref = ref;
  }

  Table_ident *table_rowtype_ref() const { return m_table_rowtype_ref; }

  void set_table_rowtype_ref(Table_ident *ref) { m_table_rowtype_ref = ref; }
  void set_record_table_rowtype_ref(Table_ident *ref) {
    if (!ref) return;
    is_record_table_ref = true;
    m_table_rowtype_ref = ref;
  }
  uint cursor_rowtype_offset() const { return m_cursor_rowtype_offset; }

  void set_cursor_rowtype_ref(uint offset) {
    m_cursor_rowtype_ref = true;
    m_cursor_rowtype_offset = offset;
  }

  uint is_row() const { return m_row_field_definitions != nullptr; }

  uint is_row_table() const { return m_row_field_table_definitions != nullptr; }

  Row_definition_list *row_field_definitions() const {
    return m_row_field_definitions;
  }
  void set_row_field_definitions(Row_definition_list *list) {
    m_row_field_definitions = list;
  }
  // for type of table
  Row_definition_table_list *row_field_table_definitions() const {
    return m_row_field_table_definitions;
  }
  void set_row_field_table_definitions(Row_definition_table_list *list) {
    m_row_field_table_definitions = list;
  }
};

/// Create_field is a description a field/column that may or may not exists in
/// a table.
///
/// The main usage of Create_field is to contain the description of a column
/// given by the user (usually given with CREATE TABLE). It is also used to
/// describe changes to be carried out on a column (usually given with ALTER
/// TABLE ... CHANGE COLUMN).
class Create_field {
 public:
  /// @returns the maximum display width of this column, in number of.
  ///          code points. See m_max_display_width_in_codepoints for an
  ///          explanation of "display width" and "code point".
  size_t max_display_width_in_codepoints() const;

  /// @returns the maximum display width of this column, in number of bytes. For
  ///          numeric types, temporal types, YEAR and BIT, this method returns
  ///          the same as max_display_width_in_codepoints(). For character
  ///          types (CHAR, VARCHAR, TEXT etc) the returned value depends on
  ///          max_display_width_in_codepoints() and the character set of this
  ///          column.
  size_t max_display_width_in_bytes() const;

  /// @returns the pack length for this column, which is the number of bytes
  ///          needed to store this column in memory. Note that blob returns
  ///          a length variable + the size of a pointer to an external memory
  ///          location where the actual data is stored. So LONGBLOB would
  ///          return 4 bytes for the length variable + 8 bytes for the pointer
  ///          to the data (12 bytes in total).
  ///  @param dont_override  Don't use pack_length_override even if non-zero
  ///                        Used by multi-valued index, where pack_length
  ///                        and key_length aren't the same.
  size_t pack_length(bool dont_override = false) const;

  /// @returns the key length for this column.
  size_t key_length() const;

  /// @retval true if the maximum column length was given explicitly by the
  ///         user.
  /// @retval false if the user didn't specify any maximum length.
  bool explicit_display_width() const { return m_explicit_display_width; }

  /// Set the maximum display width based on another Create_field.
  void set_max_display_width_from_create_field(
      const Create_field &create_field) {
    m_max_display_width_in_codepoints =
        create_field.max_display_width_in_codepoints();
  }

  dd::Column::enum_hidden_type hidden;

  const char *field_name;
  /**
    Name of column modified by ALTER TABLE's CHANGE/MODIFY COLUMN clauses,
    NULL for columns added.
  */
  const char *change;
  const char *after{nullptr};  // Put column after this one
  LEX_CSTRING comment;         // Comment for field

  /**
     The declared default value, if any, otherwise NULL. Note that this member
     is NULL if the default is a function. If the column definition has a
     function declared as the default, the information is found in
     Create_field::auto_flags.

     @see Create_field::auto_flags
  */
  Item *constant_default;
  enum_field_types sql_type;
  uint decimals;
  uint flags{0};
  /**
    Bitmap of flags indicating if field value should be auto-generated
    by default and/or on update, and in which way.

    @sa Field::enum_auto_flags for possible options.
  */
  uchar auto_flags{Field::NONE};
  TYPELIB *interval;  // Which interval to use
                      // Used only for UCS2 intervals
  List<String> interval_list;
  const CHARSET_INFO *charset;
  bool is_explicit_collation;  // User exeplicitly provided charset ?
  Field::geometry_type geom_type;
  Field *field;  // For alter table

  uint offset;

  /**
    Indicate whether column is nullable, zerofill or unsigned.

    Initialized based on flags and other members at prepare_create_field()/
    init_for_tmp_table() stage.
  */
  bool is_nullable;
  bool is_zerofill;
  bool is_unsigned;

  /**
    Indicates that storage engine doesn't support optimized BIT field
    storage.

    @note We also use safe/non-optimized version of BIT field for
          special cases like virtual temporary tables.

    Initialized at mysql_prepare_create_table()/sp_prepare_create_field()/
    init_for_tmp_table() stage.
  */
  bool treat_bit_as_char;

  /**
    Row based replication code sometimes needs to create ENUM and SET
    fields with pack length which doesn't correspond to number of
    elements in interval TYPELIB.

    When this member is non-zero ENUM/SET field to be created will use
    its value as pack length instead of one calculated from number
    elements in its interval.

    Initialized at prepare_create_field()/init_for_tmp_table() stage.
  */
  uint pack_length_override{0};

  LEX_CSTRING zip_dict_name;  // Compression dictionary name

  /* Generated column expression information */
  Value_generator *gcol_info{nullptr};
  /*
    Indication that the field is phycically stored in tables
    rather than just generated on SQL queries.
    As of now, false can only be set for virtual generated columns.
  */
  bool stored_in_db;

  /// Holds the expression to be used to generate default values.
  Value_generator *m_default_val_expr{nullptr};
  std::optional<gis::srid_t> m_srid;

  // Whether the field is actually an array of the field's type;
  bool is_array{false};

  /*
    Store dict_id after verifying zip_dict_name exists. The stored id
    is filled in dd::Column::options and later used to fill TABLE_SHARE*
    zip_dict_name and zip_dict_data
  */
  uint64_t zip_dict_id;

  LEX_CSTRING m_engine_attribute = EMPTY_CSTR;
  LEX_CSTRING m_secondary_engine_attribute = EMPTY_CSTR;
  LEX_STRING udt_name = NULL_STR;
  LEX_STRING udt_db_name =
      NULL_STR;  // For udt column,use db name to get udt object.
  LEX_CSTRING nested_table_udt = NULL_CSTR;
  uint table_type = 255;
  ulonglong varray_limit = 0;

  Create_field()
      : after(nullptr),
        is_explicit_collation(false),
        geom_type(Field::GEOM_GEOMETRY),
        is_nullable(false),
        is_zerofill(false),
        is_unsigned(false),
        /*
          Initialize treat_bit_as_char for all field types even if
          it is only used for MYSQL_TYPE_BIT. This avoids bogus
          valgrind warnings in optimized builds.
        */
        treat_bit_as_char(false),
        pack_length_override(0),
        zip_dict_name(NULL_CSTR),
        stored_in_db(false),
        m_default_val_expr(nullptr),
        zip_dict_id(0),
        ora_record() {}
  Create_field(Field *field, Field *orig_field);

  /* Used to make a clone of this object for ALTER/CREATE TABLE */
  Create_field *clone(MEM_ROOT *mem_root) const {
    return new (mem_root) Create_field(*this);
  }
  bool is_gcol() const { return gcol_info; }
  bool is_virtual_gcol() const {
    return gcol_info && !gcol_info->get_field_stored();
  }

  /* Init for a tmp table field. To be extended if need be. */
  void init_for_tmp_table(enum_field_types sql_type_arg, uint32 max_length,
                          uint32 decimals, bool is_nullable, bool is_unsigned,
                          uint pack_length_override,
                          const char *field_name = "");

  bool init(THD *thd, const char *field_name, enum_field_types type,
            const char *length, const char *decimals, uint type_modifier,
            Item *default_value, Item *on_update_value,
            const LEX_CSTRING *comment, const char *change,
            List<String> *interval_list, const CHARSET_INFO *cs,
            bool has_explicit_collation, uint uint_geom_type,
            const LEX_CSTRING *zip_dict_name, Value_generator *gcol_info,
            Value_generator *default_val_expr, std::optional<gis::srid_t> srid,
            dd::Column::enum_hidden_type hidden, bool is_array = false,
            Qualified_column_ident *udt_ident = nullptr);

  ha_storage_media field_storage_type() const {
    return (ha_storage_media)((flags >> FIELD_FLAGS_STORAGE_MEDIA) & 3);
  }

  column_format_type column_format() const {
    return (column_format_type)((flags >> FIELD_FLAGS_COLUMN_FORMAT) & 3);
  }

  void set_column_format(column_format_type column_format_arg) noexcept {
    flags &= ~(FIELD_FLAGS_COLUMN_FORMAT_MASK);
    flags |= (column_format_arg << FIELD_FLAGS_COLUMN_FORMAT);
  }

 private:
  /// The maximum display width of this column.
  ///
  /// The "display width" is the number of code points that is needed to print
  /// out the string representation of a value. It can be given by the user
  /// both explicitly and implicitly. If a user creates a table with the columns
  /// "a VARCHAR(3), b INT(3)", both columns are given an explicit display width
  /// of 3 code points. But if a user creates a table with the columns
  /// "a INT, b TINYINT UNSIGNED", the first column has an implicit display
  /// width of 11 (-2147483648 is the longest value for a signed int) and the
  /// second column has an implicit display width of 3 (255 is the longest value
  /// for an unsigned tinyint).
  /// This is related to storage size for some types (VARCHAR, BLOB etc), but
  /// not for all types (an INT is four bytes regardless of the display width).
  ///
  /// A "code point" is basically a numeric value. For instance, ASCII
  /// compromises of 128 code points (0x00 to 0x7F), while unicode contains way
  /// more. In most cases a code point represents a single graphical unit (aka
  /// grapheme), but not always. For instance, É may consists of two code points
  /// where one is the letter E and the other one is the quotation mark above
  /// the letter.
  size_t m_max_display_width_in_codepoints{0};

  /// Whether or not the display width was given explicitly by the user.
  bool m_explicit_display_width{false};

 public:
  /**
    Used during a stored routine execution,
    for cursor loop , type is record ..

    Notes:
    - ROW variables are defined as having MYSQL_TYPE_NULL,
      with a non-empty m_field_definitions.
  */
  Ora_record_refer ora_record;
  void set_udt_db_name(LEX_STRING name) { udt_db_name = name; }
};

/// @returns whether or not this field is a hidden column that represents a
///          functional index.
bool is_field_for_functional_index(const Create_field *create_field);

/**
  @retval true  If this column is hidden either in the storage engine
                or SQL layer. Either way, it is completely hidden from
                the user.
  @retval false Otherwise.
*/
bool is_hidden_by_system(const Create_field *create_field);

/**
  @retval true  If this column is hidden by the user.
  @retval false otherwise.
*/
bool is_hidden_by_user(const Create_field *create_field);
#endif
