//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nlj_as_hash_join.cpp
//
// Identification: src/optimizer/nlj_as_hash_join.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include "catalog/column.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

/**
 * @brief Helper function to extract equi-join conditions from a predicate expression
 * @param expr The expression to analyze
 * @param left_keys Output vector for left side join keys
 * @param right_keys Output vector for right side join keys
 * @return true if the expression contains equi-join conditions, false otherwise
 */
auto ExtractEquiJoinConditions(const AbstractExpression *expr, std::vector<AbstractExpressionRef> &left_keys,
                               std::vector<AbstractExpressionRef> &right_keys) -> bool {
  if (expr == nullptr) {
    return false;
  }

  // handle AND expressions - recursively extract from both sides
  if (const auto *logic_expr = dynamic_cast<const LogicExpression *>(expr);
      logic_expr != nullptr && logic_expr->logic_type_ == LogicType::And) {
    bool left_has_equi = ExtractEquiJoinConditions(logic_expr->GetChildAt(0).get(), left_keys, right_keys);
    bool right_has_equi = ExtractEquiJoinConditions(logic_expr->GetChildAt(1).get(), left_keys, right_keys);

    return left_has_equi || right_has_equi;
  }

  // handle equality comparisons
  if (const auto *comp_expr = dynamic_cast<const ComparisonExpression *>(expr);
      comp_expr != nullptr && comp_expr->comp_type_ == ComparisonType::Equal) {
    const auto *left_expr = comp_expr->GetChildAt(0).get();
    const auto *right_expr = comp_expr->GetChildAt(1).get();

    // check for column-to-column equality
    const auto *left_col = dynamic_cast<const ColumnValueExpression *>(left_expr);
    const auto *right_col = dynamic_cast<const ColumnValueExpression *>(right_expr);

    if (left_col != nullptr && right_col != nullptr) {
      // ensure columns are from different tables (tuple_idx 0 and 1)
      if (left_col->GetTupleIdx() != right_col->GetTupleIdx()) {
        if (left_col->GetTupleIdx() == 0 && right_col->GetTupleIdx() == 1) {
          left_keys.push_back(comp_expr->GetChildAt(0));
          right_keys.push_back(comp_expr->GetChildAt(1));
          return true;
        } else if (left_col->GetTupleIdx() == 1 && right_col->GetTupleIdx() == 0) {  // NOLINT
          left_keys.push_back(comp_expr->GetChildAt(1));
          right_keys.push_back(comp_expr->GetChildAt(0));
          return true;
        }
      }
    }
  }

  return false;
}

/**
 * @brief optimize nested loop join into hash join.
 * In the starter code, we will check NLJs with exactly one equal condition. You can further support optimizing joins
 * with multiple eq conditions.
 */
auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // recursively optimize children first
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  // check if this is a NestedLoopJoin that can be converted to HashJoin
  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);

    // extract equi-join conditions
    std::vector<AbstractExpressionRef> left_keys;
    std::vector<AbstractExpressionRef> right_keys;
    bool has_equi_conditions = ExtractEquiJoinConditions(nlj_plan.Predicate().get(), left_keys, right_keys);

    // only convert to hash join if we found equi-join conditions
    if (has_equi_conditions && !left_keys.empty() && !right_keys.empty()) {
      // create a HashJoin plan with the same output schema, children, and join type
      return std::make_shared<HashJoinPlanNode>(optimized_plan->output_schema_, nlj_plan.GetLeftPlan(),
                                                nlj_plan.GetRightPlan(), std::move(left_keys), std::move(right_keys),
                                                nlj_plan.GetJoinType());
    }
  }

  return optimized_plan;
}

}  // namespace bustub
