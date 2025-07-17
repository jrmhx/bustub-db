//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seqscan_as_indexscan.cpp
//
// Identification: src/optimizer/seqscan_as_indexscan.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

/**
 * @brief Helper function to extract constant values from OR conditions like (col = val1 OR col = val2)
 * @param expr The OR expression to analyze
 * @param target_col_idx The column index we're looking for
 * @return Vector of constant values if the OR expression matches the pattern, empty otherwise
 */
auto ExtractOrEqualityConstants(const AbstractExpression *expr, uint32_t target_col_idx) -> std::vector<Value> {
  std::vector<Value> values;

  // check if this is a logic expression with OR
  if (const auto *logic_expr = dynamic_cast<const LogicExpression *>(expr);
      logic_expr != nullptr && logic_expr->logic_type_ == LogicType::Or) {
    const auto *left_expr = logic_expr->GetChildAt(0).get();
    const auto *right_expr = logic_expr->GetChildAt(1).get();

    // recursively extract values from left and right side
    auto left_values = ExtractOrEqualityConstants(left_expr, target_col_idx);
    auto right_values = ExtractOrEqualityConstants(right_expr, target_col_idx);

    // if both sides have values, combine them
    if (!left_values.empty() && !right_values.empty()) {
      values.insert(values.end(), left_values.begin(), left_values.end());
      values.insert(values.end(), right_values.begin(), right_values.end());
    }
  } else if (const auto *comp_expr = dynamic_cast<const ComparisonExpression *>(expr);
           comp_expr != nullptr && comp_expr->comp_type_ == ComparisonType::Equal) {
    // check if this is a single equality comparison (column = constant or constant = column)
    const auto *left_expr = comp_expr->GetChildAt(0).get();
    const auto *right_expr = comp_expr->GetChildAt(1).get();

    if (const auto *left_col = dynamic_cast<const ColumnValueExpression *>(left_expr);
        left_col != nullptr && left_col->GetTupleIdx() == 0 && left_col->GetColIdx() == target_col_idx) {
      if (const auto *right_const = dynamic_cast<const ConstantValueExpression *>(right_expr); right_const != nullptr) {
        values.push_back(right_const->val_);
      }
    } else if (const auto *right_col = dynamic_cast<const ColumnValueExpression *>(right_expr);
               right_col != nullptr && right_col->GetTupleIdx() == 0 && right_col->GetColIdx() == target_col_idx) {
      if (const auto *left_const = dynamic_cast<const ConstantValueExpression *>(left_expr); left_const != nullptr) {
        values.push_back(left_const->val_);
      }
    }
  }

  return values;
}

/**
 * @brief Optimizes seq scan as index scan if there's an index on a table
 */
auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // recursively optimize children first
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }

  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  if (optimized_plan->GetType() == PlanType::SeqScan) {
    const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);

    // only optimize if there's a filter predicate
    if (seq_scan_plan.filter_predicate_ != nullptr) {
      for (uint32_t col_idx = 0; col_idx < seq_scan_plan.output_schema_->GetColumnCount(); col_idx++) {
        auto or_values = ExtractOrEqualityConstants(seq_scan_plan.filter_predicate_.get(), col_idx);

        if (or_values.size() > 1) {  // found OR condition with multiple values
          if (auto index = MatchIndex(seq_scan_plan.table_name_, col_idx); index != std::nullopt) {
            auto [index_oid, index_name] = index.value();

            return std::make_shared<IndexScanPlanNode>(
                seq_scan_plan.output_schema_, seq_scan_plan.table_oid_, index_oid,
                seq_scan_plan.filter_predicate_,      
                std::vector<AbstractExpressionRef>{} 
            );
          }
        }
      }

      // fall back to single equality optimization
      if (const auto *comp_expr = dynamic_cast<const ComparisonExpression *>(seq_scan_plan.filter_predicate_.get());
          comp_expr != nullptr && comp_expr->comp_type_ == ComparisonType::Equal) {
        const auto *left_expr = comp_expr->GetChildAt(0).get();
        const auto *right_expr = comp_expr->GetChildAt(1).get();
        const ColumnValueExpression *column_expr = nullptr;
        const ConstantValueExpression *const_expr = nullptr;

        if (const auto *left_col = dynamic_cast<const ColumnValueExpression *>(left_expr);
            left_col != nullptr && left_col->GetTupleIdx() == 0) {
          if (const auto *right_const = dynamic_cast<const ConstantValueExpression *>(right_expr);
              right_const != nullptr) {
            column_expr = left_col;
            const_expr = right_const;
          }
        } else if (const auto *right_col = dynamic_cast<const ColumnValueExpression *>(right_expr);
                   right_col != nullptr && right_col->GetTupleIdx() == 0) {
          if (const auto *left_const = dynamic_cast<const ConstantValueExpression *>(left_expr);
              left_const != nullptr) {
            column_expr = right_col;
            const_expr = left_const;
          }
        }

        if (column_expr != nullptr && const_expr != nullptr) {
          if (auto index = MatchIndex(seq_scan_plan.table_name_, column_expr->GetColIdx()); index != std::nullopt) {
            auto [index_oid, index_name] = index.value();

            // create IndexScan with point lookup
            std::vector<AbstractExpressionRef> pred_keys;
            pred_keys.emplace_back(std::make_shared<ConstantValueExpression>(const_expr->val_));

            return std::make_shared<IndexScanPlanNode>(
                seq_scan_plan.output_schema_, seq_scan_plan.table_oid_, index_oid,
                nullptr, 
                std::move(pred_keys));
          }
        }
      }
    }
  }

  return optimized_plan;
}

}  // namespace bustub
