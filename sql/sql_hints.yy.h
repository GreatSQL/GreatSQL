/* A Bison parser, made by GNU Bison 3.0.4.  */

/* Bison interface for Yacc-like parsers in C

   Copyright (C) 1984, 1989-1990, 2000-2015 Free Software Foundation, Inc.

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.

   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */

#ifndef YY_MY_HINT_PARSER_BUILDS_GREATDB_GREATDB_SERVER_BUILD_GS_WORKDIR_MYBUILD_SQL_SQL_HINTS_YY_H_INCLUDED
# define YY_MY_HINT_PARSER_BUILDS_GREATDB_GREATDB_SERVER_BUILD_GS_WORKDIR_MYBUILD_SQL_SQL_HINTS_YY_H_INCLUDED
/* Debug traces.  */
#ifndef MY_HINT_PARSER_DEBUG
# if defined YYDEBUG
#if YYDEBUG
#   define MY_HINT_PARSER_DEBUG 1
#  else
#   define MY_HINT_PARSER_DEBUG 0
#  endif
# else /* ! defined YYDEBUG */
#  define MY_HINT_PARSER_DEBUG 0
# endif /* ! defined YYDEBUG */
#endif  /* ! defined MY_HINT_PARSER_DEBUG */
#if MY_HINT_PARSER_DEBUG
extern int my_hint_parser_debug;
#endif

/* Token type.  */
#ifndef MY_HINT_PARSER_TOKENTYPE
# define MY_HINT_PARSER_TOKENTYPE
  enum my_hint_parser_tokentype
  {
    MAX_EXECUTION_TIME_HINT = 1000,
    RESOURCE_GROUP_HINT = 1001,
    BKA_HINT = 1002,
    BNL_HINT = 1003,
    DUPSWEEDOUT_HINT = 1004,
    FIRSTMATCH_HINT = 1005,
    INTOEXISTS_HINT = 1006,
    LOOSESCAN_HINT = 1007,
    MATERIALIZATION_HINT = 1008,
    NO_BKA_HINT = 1009,
    NO_BNL_HINT = 1010,
    NO_ICP_HINT = 1011,
    NO_MRR_HINT = 1012,
    NO_RANGE_OPTIMIZATION_HINT = 1013,
    NO_SEMIJOIN_HINT = 1014,
    MRR_HINT = 1015,
    QB_NAME_HINT = 1016,
    SEMIJOIN_HINT = 1017,
    SUBQUERY_HINT = 1018,
    DERIVED_MERGE_HINT = 1019,
    NO_DERIVED_MERGE_HINT = 1020,
    JOIN_PREFIX_HINT = 1021,
    JOIN_SUFFIX_HINT = 1022,
    JOIN_ORDER_HINT = 1023,
    JOIN_FIXED_ORDER_HINT = 1024,
    INDEX_MERGE_HINT = 1025,
    NO_INDEX_MERGE_HINT = 1026,
    SET_VAR_HINT = 1027,
    SKIP_SCAN_HINT = 1028,
    NO_SKIP_SCAN_HINT = 1029,
    HASH_JOIN_HINT = 1030,
    NO_HASH_JOIN_HINT = 1031,
    HINT_ARG_NUMBER = 1032,
    HINT_ARG_IDENT = 1033,
    HINT_ARG_QB_NAME = 1034,
    HINT_ARG_TEXT = 1035,
    HINT_IDENT_OR_NUMBER_WITH_SCALE = 1036,
    HINT_CLOSE = 1037,
    HINT_ERROR = 1038,
    INDEX_HINT = 1039,
    NO_INDEX_HINT = 1040,
    JOIN_INDEX_HINT = 1041,
    NO_JOIN_INDEX_HINT = 1042,
    GROUP_INDEX_HINT = 1043,
    NO_GROUP_INDEX_HINT = 1044,
    ORDER_INDEX_HINT = 1045,
    NO_ORDER_INDEX_HINT = 1046,
    DERIVED_CONDITION_PUSHDOWN_HINT = 1047,
    NO_DERIVED_CONDITION_PUSHDOWN_HINT = 1048,
    HINT_ARG_FLOATING_POINT_NUMBER = 1049
  };
#endif
/* Tokens.  */
#define MAX_EXECUTION_TIME_HINT 1000
#define RESOURCE_GROUP_HINT 1001
#define BKA_HINT 1002
#define BNL_HINT 1003
#define DUPSWEEDOUT_HINT 1004
#define FIRSTMATCH_HINT 1005
#define INTOEXISTS_HINT 1006
#define LOOSESCAN_HINT 1007
#define MATERIALIZATION_HINT 1008
#define NO_BKA_HINT 1009
#define NO_BNL_HINT 1010
#define NO_ICP_HINT 1011
#define NO_MRR_HINT 1012
#define NO_RANGE_OPTIMIZATION_HINT 1013
#define NO_SEMIJOIN_HINT 1014
#define MRR_HINT 1015
#define QB_NAME_HINT 1016
#define SEMIJOIN_HINT 1017
#define SUBQUERY_HINT 1018
#define DERIVED_MERGE_HINT 1019
#define NO_DERIVED_MERGE_HINT 1020
#define JOIN_PREFIX_HINT 1021
#define JOIN_SUFFIX_HINT 1022
#define JOIN_ORDER_HINT 1023
#define JOIN_FIXED_ORDER_HINT 1024
#define INDEX_MERGE_HINT 1025
#define NO_INDEX_MERGE_HINT 1026
#define SET_VAR_HINT 1027
#define SKIP_SCAN_HINT 1028
#define NO_SKIP_SCAN_HINT 1029
#define HASH_JOIN_HINT 1030
#define NO_HASH_JOIN_HINT 1031
#define HINT_ARG_NUMBER 1032
#define HINT_ARG_IDENT 1033
#define HINT_ARG_QB_NAME 1034
#define HINT_ARG_TEXT 1035
#define HINT_IDENT_OR_NUMBER_WITH_SCALE 1036
#define HINT_CLOSE 1037
#define HINT_ERROR 1038
#define INDEX_HINT 1039
#define NO_INDEX_HINT 1040
#define JOIN_INDEX_HINT 1041
#define NO_JOIN_INDEX_HINT 1042
#define GROUP_INDEX_HINT 1043
#define NO_GROUP_INDEX_HINT 1044
#define ORDER_INDEX_HINT 1045
#define NO_ORDER_INDEX_HINT 1046
#define DERIVED_CONDITION_PUSHDOWN_HINT 1047
#define NO_DERIVED_CONDITION_PUSHDOWN_HINT 1048
#define HINT_ARG_FLOATING_POINT_NUMBER 1049

/* Value type.  */



int my_hint_parser_parse (class THD *thd, class Hint_scanner *scanner, class PT_hint_list **ret);

#endif /* !YY_MY_HINT_PARSER_BUILDS_GREATDB_GREATDB_SERVER_BUILD_GS_WORKDIR_MYBUILD_SQL_SQL_HINTS_YY_H_INCLUDED  */
