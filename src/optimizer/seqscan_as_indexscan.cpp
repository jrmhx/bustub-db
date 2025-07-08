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

#include "optimizer/optimizer.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"

namespace bustub {

/**
 * @brief Optimizes seq scan as index scan if there's an index on a table
 */
auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // implement seq scan with predicate -> index scan optimizer rule
  // filter predicate pushdown has been enabled for you in optimizer.cpp when forcing starter rule
  
  // recursively optimize children first
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }
  
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  
  // check if this is a SeqScan with a filter predicate
  if (optimized_plan->GetType() == PlanType::SeqScan) {
    const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
    
    // only optimize if there's a filter predicate
    if (seq_scan_plan.filter_predicate_ != nullptr) {
      // check if the predicate is an equality comparison with a constant
      if (const auto *comp_expr = dynamic_cast<const ComparisonExpression *>(seq_scan_plan.filter_predicate_.get());
          comp_expr != nullptr && comp_expr->comp_type_ == ComparisonType::Equal) {
        
        const auto *left_expr = comp_expr->GetChildAt(0).get();
        const auto *right_expr = comp_expr->GetChildAt(1).get();
        
        // check if we have column = constant or constant = column pattern
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
        
        // if we found a suitable pattern, check for matching index
        if (column_expr != nullptr && const_expr != nullptr) {
          if (auto index = MatchIndex(seq_scan_plan.table_name_, column_expr->GetColIdx());
              index != std::nullopt) {
            auto [index_oid, index_name] = index.value();
            
            // create IndexScan with point lookup
            std::vector<AbstractExpressionRef> pred_keys;
            pred_keys.emplace_back(std::make_shared<ConstantValueExpression>(const_expr->val_));
            
            return std::make_shared<IndexScanPlanNode>(
                seq_scan_plan.output_schema_,
                seq_scan_plan.table_oid_,
                index_oid,
                nullptr, // no additional filter predicate needed for point lookup
                std::move(pred_keys)
            );
          }
        }
      }
    }
  }
  
  return optimized_plan;
}

}  // namespace bustub
