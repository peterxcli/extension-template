#include "cdf_extension.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

static std::atomic<int64_t> global_commit_version(0);

void CdfPreOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
    if (!plan) return;
    for (auto &child : plan->children) {
        CdfPreOptimize(input, child);
    }
    
    if (plan->type == LogicalOperatorType::LOGICAL_INSERT) {
        auto &insert = plan->Cast<LogicalInsert>();
        
        string log_table_name = "__duckdb_cdf_" + insert.table.name + "_log";
        auto log_table_entry = Catalog::GetEntry<TableCatalogEntry>(
            input.context, insert.table.catalog.GetName(), insert.table.schema.name, log_table_name, OnEntryNotFound::RETURN_NULL
        );
        if (!log_table_entry) return;
        
        auto insert_ptr = unique_ptr_cast<LogicalOperator, LogicalInsert>(std::move(plan));
        bool original_return_chunk = insert_ptr->return_chunk;
        idx_t original_index = insert_ptr->table_index;
        
        insert_ptr->return_chunk = true; 
        insert_ptr->table_index = input.optimizer.binder.GenerateTableIndex();
        insert_ptr->ResolveOperatorTypes();
        
        vector<unique_ptr<Expression>> proj_exprs;
        auto &columns_list = insert_ptr->table.GetColumns();
        auto base_columns = columns_list.GetColumnTypes();
        auto base_names = columns_list.GetColumnNames();
        
        auto expected_log_types = log_table_entry->GetColumns().GetColumnTypes();
        
        for (idx_t i = 0; i < base_columns.size(); i++) {
            proj_exprs.push_back(make_uniq<BoundColumnRefExpression>(base_names[i], base_columns[i], ColumnBinding(insert_ptr->table_index, i)));
        }
        
        proj_exprs.push_back(make_uniq<BoundConstantExpression>(Value("insert")));
        int64_t version = ++global_commit_version;
        proj_exprs.push_back(make_uniq<BoundConstantExpression>(Value::BIGINT(version))); 
        proj_exprs.push_back(make_uniq<BoundConstantExpression>(Value::TIMESTAMPTZ(timestamp_tz_t(Timestamp::FromEpochSeconds(0))))); 
        
        idx_t proj1_idx = input.optimizer.binder.GenerateTableIndex();
        auto projection = make_uniq<LogicalProjection>(proj1_idx, std::move(proj_exprs));
        projection->children.push_back(std::move(insert_ptr));
        projection->ResolveOperatorTypes();
        
        for (idx_t i = 0; i < expected_log_types.size(); i++) {
            if (projection->types[i] != expected_log_types[i]) {
                auto expr = std::move(projection->expressions[i]);
                projection->expressions[i] = BoundCastExpression::AddCastToType(input.context, std::move(expr), expected_log_types[i]);
                projection->types[i] = expected_log_types[i];
            }
        }
        
        idx_t log_insert_idx = input.optimizer.binder.GenerateTableIndex();
        auto log_insert = make_uniq<LogicalInsert>(*log_table_entry, log_insert_idx);
        log_insert->expected_types = log_table_entry->GetColumns().GetColumnTypes();
        log_insert->return_chunk = original_return_chunk;
        if (original_return_chunk) {
            log_insert->table_index = original_index;
        }
        log_insert->children.push_back(std::move(projection));
        log_insert->ResolveOperatorTypes();
        
        plan = std::move(log_insert);
    } else if (plan->type == LogicalOperatorType::LOGICAL_DELETE) {
        auto &del = plan->Cast<LogicalDelete>();
        
        string log_table_name = "__duckdb_cdf_" + del.table.name + "_log";
        auto log_table_entry = Catalog::GetEntry<TableCatalogEntry>(
            input.context, del.table.catalog.GetName(), del.table.schema.name, log_table_name, OnEntryNotFound::RETURN_NULL
        );
        if (!log_table_entry) return;
        
        auto del_ptr = unique_ptr_cast<LogicalOperator, LogicalDelete>(std::move(plan));
        bool original_return_chunk = del_ptr->return_chunk;
        idx_t original_index = del_ptr->table_index;
        
        del_ptr->return_chunk = true;
        del_ptr->table_index = input.optimizer.binder.GenerateTableIndex();
        del_ptr->ResolveOperatorTypes();
        
        vector<unique_ptr<Expression>> proj_exprs;
        auto &columns_list = del_ptr->table.GetColumns();
        auto base_columns = columns_list.GetColumnTypes();
        auto base_names = columns_list.GetColumnNames();
        auto expected_log_types = log_table_entry->GetColumns().GetColumnTypes();
        
        for (idx_t i = 0; i < base_columns.size(); i++) {
            proj_exprs.push_back(make_uniq<BoundColumnRefExpression>(base_names[i], base_columns[i], ColumnBinding(del_ptr->table_index, i)));
        }
        
        proj_exprs.push_back(make_uniq<BoundConstantExpression>(Value("delete")));
        int64_t version = ++global_commit_version;
        proj_exprs.push_back(make_uniq<BoundConstantExpression>(Value::BIGINT(version))); 
        proj_exprs.push_back(make_uniq<BoundConstantExpression>(Value::TIMESTAMPTZ(timestamp_tz_t(Timestamp::FromEpochSeconds(0))))); 
        
        idx_t proj_idx = input.optimizer.binder.GenerateTableIndex();
        auto projection = make_uniq<LogicalProjection>(proj_idx, std::move(proj_exprs));
        projection->children.push_back(std::move(del_ptr));
        projection->ResolveOperatorTypes();
        
        for (idx_t i = 0; i < expected_log_types.size(); i++) {
            if (projection->types[i] != expected_log_types[i]) {
                auto expr = std::move(projection->expressions[i]);
                projection->expressions[i] = BoundCastExpression::AddCastToType(input.context, std::move(expr), expected_log_types[i]);
                projection->types[i] = expected_log_types[i];
            }
        }
        
        idx_t log_insert_idx = input.optimizer.binder.GenerateTableIndex();
        auto log_insert = make_uniq<LogicalInsert>(*log_table_entry, log_insert_idx);
        log_insert->expected_types = log_table_entry->GetColumns().GetColumnTypes();
        log_insert->return_chunk = original_return_chunk;
        if (original_return_chunk) {
            log_insert->table_index = original_index;
        }
        log_insert->children.push_back(std::move(projection));
        log_insert->ResolveOperatorTypes();
        
        plan = std::move(log_insert);
    } else if (plan->type == LogicalOperatorType::LOGICAL_UPDATE) {
        auto &update = plan->Cast<LogicalUpdate>();
        string log_table_name = "__duckdb_cdf_" + update.table.name + "_log";
        auto log_table_entry = Catalog::GetEntry<TableCatalogEntry>(
            input.context, update.table.catalog.GetName(), update.table.schema.name, log_table_name, OnEntryNotFound::RETURN_NULL
        );
        if (!log_table_entry) return;

        auto &columns_list = update.table.GetColumns();
        
        LogicalOperator *current = update.children[0].get();
        LogicalProjection *proj_node = nullptr;
        if (current->type == LogicalOperatorType::LOGICAL_PROJECTION) {
            proj_node = (LogicalProjection*)current;
        }
        
        if (proj_node) {
            // First pass: add missing columns
            for (idx_t col_idx = 0; col_idx < columns_list.LogicalColumnCount(); col_idx++) {
                auto &col = columns_list.GetColumn(LogicalIndex(col_idx));
                column_t physical_col_idx = col.Physical().index;
                
                bool is_updated = false;
                for (auto &upd_col : update.columns) {
                    if (upd_col.index == physical_col_idx) {
                        is_updated = true;
                        break;
                    }
                }
                
                if (!is_updated) {
                    LogicalOperator *scan_node = proj_node->children[0].get();
                    LogicalGet *get_node = nullptr;
                    while (scan_node) {
                        if (scan_node->type == LogicalOperatorType::LOGICAL_GET) {
                            get_node = (LogicalGet*)scan_node;
                            break;
                        }
                        if (!scan_node->children.empty()) {
                            scan_node = scan_node->children[0].get();
                        } else {
                            break;
                        }
                    }
                    
                    if (get_node) {
                        idx_t get_proj_idx = 0;
                        bool found_in_get = false;
                        auto &column_ids = get_node->GetColumnIds();
                        for (idx_t i = 0; i < column_ids.size(); i++) {
                            if (column_ids[i].GetPrimaryIndex() == physical_col_idx) {
                                found_in_get = true;
                                get_proj_idx = i;
                                break;
                            }
                        }
                        
                        if (!found_in_get) {
                            get_node->AddColumnId(physical_col_idx);
                            get_node->returned_types.push_back(col.GetType());
                            get_node->names.push_back(col.Name());
                            get_proj_idx = get_node->GetColumnIds().size() - 1;
                        }
                        
                        auto rowid_expr = std::move(proj_node->expressions.back());
                        proj_node->expressions.pop_back();
                        
                        proj_node->expressions.push_back(make_uniq<BoundColumnRefExpression>(col.Name(), col.GetType(), ColumnBinding(get_node->table_index, get_proj_idx)));
                        idx_t new_proj_idx = proj_node->expressions.size() - 1;
                        
                        proj_node->expressions.push_back(std::move(rowid_expr));
                        
                        update.columns.push_back(PhysicalIndex(physical_col_idx));
                        update.expressions.push_back(make_uniq<BoundColumnRefExpression>(col.Name(), col.GetType(), ColumnBinding(proj_node->table_index, new_proj_idx)));
                    }
                }
            }
            
            LogicalOperator *scan_node2 = proj_node->children[0].get();
            LogicalGet *get_node2 = nullptr;
            while (scan_node2) {
                if (scan_node2->type == LogicalOperatorType::LOGICAL_GET) {
                    get_node2 = (LogicalGet*)scan_node2;
                    break;
                }
                if (!scan_node2->children.empty()) {
                    scan_node2 = scan_node2->children[0].get();
                } else {
                    break;
                }
            }
            
            if (get_node2) get_node2->ResolveOperatorTypes();
            
            scan_node2 = proj_node->children[0].get();
            while (scan_node2) {
                scan_node2->ResolveOperatorTypes();
                if (scan_node2->type == LogicalOperatorType::LOGICAL_GET) break;
                scan_node2 = scan_node2->children.empty() ? nullptr : scan_node2->children[0].get();
            }
            proj_node->ResolveOperatorTypes();
            update.ResolveOperatorTypes();
        }

        auto update_ptr = unique_ptr_cast<LogicalOperator, LogicalUpdate>(std::move(plan));
        bool original_return_chunk = update_ptr->return_chunk;
        idx_t original_index = update_ptr->table_index;

        update_ptr->return_chunk = true;
        update_ptr->table_index = input.optimizer.binder.GenerateTableIndex();
        update_ptr->ResolveOperatorTypes();

        vector<unique_ptr<Expression>> proj_exprs;
        auto &columns_list_new = update_ptr->table.GetColumns();
        auto base_columns = columns_list_new.GetColumnTypes();
        auto base_names = columns_list_new.GetColumnNames();
        auto expected_log_types = log_table_entry->GetColumns().GetColumnTypes();

        for (idx_t i = 0; i < base_columns.size(); i++) {
            proj_exprs.push_back(make_uniq<BoundColumnRefExpression>(base_names[i], base_columns[i], ColumnBinding(update_ptr->table_index, i)));
        }

        proj_exprs.push_back(make_uniq<BoundConstantExpression>(Value("update_postimage")));
        int64_t version = ++global_commit_version;
        proj_exprs.push_back(make_uniq<BoundConstantExpression>(Value::BIGINT(version)));
        proj_exprs.push_back(make_uniq<BoundConstantExpression>(Value::TIMESTAMPTZ(timestamp_tz_t(Timestamp::FromEpochSeconds(0)))));

        idx_t proj_idx = input.optimizer.binder.GenerateTableIndex();
        auto projection = make_uniq<LogicalProjection>(proj_idx, std::move(proj_exprs));
        projection->children.push_back(std::move(update_ptr));
        projection->ResolveOperatorTypes();

        for (idx_t i = 0; i < expected_log_types.size(); i++) {
            if (projection->types[i] != expected_log_types[i]) {
                auto expr = std::move(projection->expressions[i]);
                projection->expressions[i] = BoundCastExpression::AddCastToType(input.context, std::move(expr), expected_log_types[i]);
                projection->types[i] = expected_log_types[i];
            }
        }

        idx_t log_insert_idx = input.optimizer.binder.GenerateTableIndex();
        auto log_insert = make_uniq<LogicalInsert>(*log_table_entry, log_insert_idx);
        log_insert->expected_types = log_table_entry->GetColumns().GetColumnTypes();
        log_insert->return_chunk = original_return_chunk;
        if (original_return_chunk) {
            log_insert->table_index = original_index;
        }
        log_insert->children.push_back(std::move(projection));
        log_insert->ResolveOperatorTypes();

        plan = std::move(log_insert);
    }
}

} // namespace duckdb
