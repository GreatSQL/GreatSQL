/* Copyright (c) 2020, 2024, Oracle and/or its affiliates.
   Copyright (c) 2026, GreatDB Software Co., Ltd.

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

#ifndef SQL_JOIN_OPTIMIZER_EXPLAIN_ACCESS_PATH_H
#define SQL_JOIN_OPTIMIZER_EXPLAIN_ACCESS_PATH_H

#include <memory>
#include <string>
#include "sql-common/json_dom.h"
#include "sql/join_optimizer/access_path.h"
#include "sql/join_optimizer/relational_expression.h"

struct AccessPath;
class Query_expression;
class JOIN;
class THD;

/// Print out an access path and all of its children (if any) in a tree.
std::string PrintQueryPlan(THD *ethd, const THD *query_thd,
                           Query_expression *unit);
/// For debugging purposes.
std::string PrintQueryPlan(int level, AccessPath *path, JOIN *join,
                           bool is_root_of_join);

/**
  Generate a digest based on the subplan that the given access path
  represents. This can be used by developers to force a given subplan,
  to investigate e.g. whether a given choice is actually faster in practice,
  force-apply a plan from the old join optimizer (or at least the types of
  subplans that are ever considered; e.g. aggregation through temporary
  tables are not) into the hypergraph join optimizer (to see how it's costed),
  or whether a given plan is even generated. If DEBUG contains
  force_subplan_0x<token>, subplans with the given token are unconditionally
  preferred over all others.

  The token returned is “0x@<digest@>”, where @<digest@> is the first 64 bits
  of the SHA-256 sum of this string:

    desc1,desc2,...,[child1_desc:]0xchild1,[child2_desc:]0xchild2,@<more
  children@>

  where desc1, desc2, etc. are the description lines given by EXPLAIN,
  and 0xchild1 is the token for children. The normal way to generate such
  tokens is to use SET DEBUG='+d,subplan_tokens' and look at the EXPLAIN
  FORMAT=tree, but in a pinch, you can also write them by hand and use
  sha256sum or a similar tool.

  Only the hypergraph join optimizer honors token preferences, but EXPLAIN
  FORMAT=tree shows computed tokens for both optimizers.
 */
std::string GetForceSubplanToken(AccessPath *path, JOIN *join);

/// This structure encapsulates the information needed to create a Json object
/// for a child access path.
struct ExplainChild {
  AccessPath *path;

  // Normally blank. If not blank, a heading for this iterator
  // saying what kind of role it has to the parent if it is not
  // obvious. E.g., FilterIterator can print iterators that are
  // children because they come out of subselect conditions.
  std::string description = "";

  // If this child is the root of a new JOIN, it is contained here.
  JOIN *join = nullptr;

  // If it's convenient to assign json fields for this child while creating this
  // structure, then a json object can be allocated and set here.
  std::unique_ptr<Json_object> obj{nullptr};
};

std::string JoinTypeToString(JoinType join_type);
std::string HashJoinTypeToString(RelationalExpression::Type join_type,
                                 std::string *explain_json_value);
bool GetAccessPathsFromSelectList(JOIN *join,
                                  std::vector<ExplainChild> *children);
bool AddSubqueryPaths(const Item *item_arg, const char *source_text,
                      std::vector<ExplainChild> *children);
bool AddChildrenFromPushedCondition(const TABLE &table,
                                    std::vector<ExplainChild> *children);
bool AddPathCosts(const AccessPath *path, const AccessPath *materialized_path,
                  Json_object *obj, bool explain_analyze);
bool AddTableInfoToObject(Json_object *obj, const TABLE *table);
bool IsCoveringIndexScan(const KEY &key, const TABLE &table);

#endif  // SQL_JOIN_OPTIMIZER_EXPLAIN_ACCESS_PATH_H
