#include "sql_parser.h"
#include "storage_layer.h"
#include <iostream>
#include <unordered_map>
#include <functional>
#include <algorithm>

class SqlExecutor {
public:
    void execute(const SqlAst& ast, FileStorageLayer& storage);
};

namespace {
int col_index(const std::vector<std::string>& cols, const std::string& name) {
    auto it = std::find(cols.begin(), cols.end(), name);
    if (it == cols.end()) throw std::runtime_error("Column not found: " + name);
    return static_cast<int>(std::distance(cols.begin(), it));
}
}

void SqlExecutor::execute(const SqlAst& ast, FileStorageLayer& storage) {
    if (ast.type != SqlAstType::Select) {
        std::cout << "Only SELECT supported." << std::endl;
        return;
    }
    auto col_names = storage.get_column_names(ast.from_table);
    std::vector<int> projection;
    std::optional<std::pair<std::string, int>> aggregate;
    for (const auto& col : ast.select_columns) {
        if (col.find("SUM(") == 0) {
            std::string cname = col.substr(4, col.size() - 5);
            projection.push_back(col_index(col_names, cname));
            aggregate = std::make_pair("SUM", projection.back());
        } else if (col.find("ABS(") == 0) {
            std::string cname = col.substr(4, col.size() - 5);
            projection.push_back(col_index(col_names, cname));
            aggregate = std::make_pair("ABS", projection.back());
        } else {
            projection.push_back(col_index(col_names, col));
        }
    }
    std::optional<std::function<bool(const std::vector<std::string>&)>> filter_func;
    if (!ast.where_clauses.empty()) {
        std::vector<std::tuple<int, std::string, std::string>> filters;
        for (const auto& w : ast.where_clauses) {
            filters.emplace_back(col_index(col_names, w.col), w.op, w.val);
        }
        filter_func = [filters](const std::vector<std::string>& row) {
            for (const auto& [idx, op, val] : filters) {
                if (idx < 0 || static_cast<size_t>(idx) >= row.size()) return false;
                if (op == "=") { if (row[idx] != val) return false; }
                else if (op == ">") { if (!(std::stoi(row[idx]) > std::stoi(val))) return false; }
                else if (op == "<") { if (!(std::stoi(row[idx]) < std::stoi(val))) return false; }
                else if (op == ">=") { if (!(std::stoi(row[idx]) >= std::stoi(val))) return false; }
                else if (op == "<=") { if (!(std::stoi(row[idx]) <= std::stoi(val))) return false; }
                else if (op == "!=") { if (row[idx] == val) return false; }
                else return false;
            }
            return true;
        };
    }
    std::optional<std::vector<std::pair<int, bool>>> order_by;
    if (!ast.order_by.empty()) {
        std::vector<std::pair<int, bool>> order;
        for (const auto& [col, asc] : ast.order_by) {
            order.emplace_back(col_index(col_names, col), asc);
        }
        order_by = order;
    }
    std::optional<size_t> limit;
    if (ast.limit) limit = *ast.limit;
    if (!ast.join_table.empty()) {
        auto join_col_names = storage.get_column_names(ast.join_table);
        int left_idx = col_index(col_names, ast.join_left_col);
        int right_idx = col_index(join_col_names, ast.join_right_col);
        auto left_rows = storage.scan(ast.from_table);
        auto right_rows = storage.scan(ast.join_table);
        std::unordered_multimap<std::string, std::vector<std::string>> right_map;
        for (const auto& row : right_rows) {
            if (right_idx < 0 || static_cast<size_t>(right_idx) >= row.size()) continue;
            right_map.emplace(row[right_idx], row);
        }
        std::vector<std::vector<std::string>> joined;
        for (const auto& lrow : left_rows) {
            if (left_idx < 0 || static_cast<size_t>(left_idx) >= lrow.size()) continue;
            auto range = right_map.equal_range(lrow[left_idx]);
            for (auto it = range.first; it != range.second; ++it) {
                std::vector<std::string> combined = lrow;
                combined.insert(combined.end(), it->second.begin(), it->second.end());
                joined.push_back(std::move(combined));
            }
        }
        std::vector<std::string> all_cols = col_names;
        all_cols.insert(all_cols.end(), join_col_names.begin(), join_col_names.end());
        std::vector<int> join_proj;
        for (const auto& col : ast.select_columns) {
            if (col.find("SUM(") == 0 || col.find("ABS(") == 0) {
                std::string cname = col.substr(col.find('(') + 1, col.size() - col.find('(') - 2);
                join_proj.push_back(col_index(all_cols, cname));
            } else {
                join_proj.push_back(col_index(all_cols, col));
            }
        }
        std::optional<std::function<bool(const std::vector<std::string>&)>> join_filter;
        if (!ast.where_clauses.empty()) {
            std::vector<std::tuple<int, std::string, std::string>> filters;
            for (const auto& w : ast.where_clauses) {
                filters.emplace_back(col_index(all_cols, w.col), w.op, w.val);
            }
            join_filter = [filters](const std::vector<std::string>& row) {
                for (const auto& [idx, op, val] : filters) {
                    if (idx < 0 || static_cast<size_t>(idx) >= row.size()) return false;
                    if (op == "=") { if (row[idx] != val) return false; }
                    else if (op == ">") { if (!(std::stoi(row[idx]) > std::stoi(val))) return false; }
                    else if (op == "<") { if (!(std::stoi(row[idx]) < std::stoi(val))) return false; }
                    else if (op == ">=") { if (!(std::stoi(row[idx]) >= std::stoi(val))) return false; }
                    else if (op == "<=") { if (!(std::stoi(row[idx]) <= std::stoi(val))) return false; }
                    else if (op == "!=") { if (row[idx] == val) return false; }
                    else return false;
                }
                return true;
            };
        }
        std::optional<std::vector<std::pair<int, bool>>> join_order_by;
        if (!ast.order_by.empty()) {
            std::vector<std::pair<int, bool>> order;
            for (const auto& [col, asc] : ast.order_by) {
                order.emplace_back(col_index(all_cols, col), asc);
            }
            join_order_by = order;
        }
        std::optional<std::pair<std::string, int>> join_agg;
        for (const auto& col : ast.select_columns) {
            if (col.find("SUM(") == 0) join_agg = std::make_pair("SUM", col_index(all_cols, col.substr(4, col.size() - 5)));
            if (col.find("ABS(") == 0) join_agg = std::make_pair("ABS", col_index(all_cols, col.substr(4, col.size() - 5)));
        }
        std::vector<std::vector<std::string>> filtered;
        for (const auto& row : joined) {
            if (join_filter && !(*join_filter)(row)) continue;
            if (!join_proj.empty()) {
                std::vector<std::string> proj_row;
                for (int idx : join_proj) {
                    if (idx >= 0 && static_cast<size_t>(idx) < row.size()) proj_row.push_back(row[idx]);
                }
                filtered.push_back(std::move(proj_row));
            } else {
                filtered.push_back(row);
            }
        }
        if (join_order_by && !join_order_by->empty()) {
            std::sort(filtered.begin(), filtered.end(), [&](const std::vector<std::string>& a, const std::vector<std::string>& b) {
                for (const auto& [col, asc] : *join_order_by) {
                    if (col < 0 || static_cast<size_t>(col) >= a.size() || static_cast<size_t>(col) >= b.size()) continue;
                    try {
                        int ai = std::stoi(a[col]);
                        int bi = std::stoi(b[col]);
                        if (ai != bi) return asc ? ai < bi : ai > bi;
                    } catch (...) {
                        if (a[col] != b[col]) return asc ? a[col] < b[col] : a[col] > b[col];
                    }
                }
                return false;
            });
        }
        if (limit && filtered.size() > *limit) filtered.resize(*limit);
        if (join_agg) {
            const std::string& op = join_agg->first;
            int col = join_agg->second;
            if (col < 0 || (filtered.empty() || static_cast<size_t>(col) >= filtered[0].size())) {
                throw std::runtime_error("Invalid column index for aggregation");
            }
            if (op == "SUM") {
                int64_t sum = 0;
                for (const auto& row : filtered) {
                    try { sum += std::stoll(row[col]); } catch (...) {}
                }
                std::cout << "SUM: " << sum << std::endl;
                return;
            } else if (op == "ABS") {
                for (auto& row : filtered) {
                    try { int val = std::stoi(row[col]); row[col] = std::to_string(std::abs(val)); } catch (...) {}
                }
            }
        }
        for (size_t i = 0; i < ast.select_columns.size(); ++i) {
            if (i > 0) std::cout << " | ";
            std::cout << ast.select_columns[i];
        }
        std::cout << std::endl;
        for (const auto& row : filtered) {
            for (size_t i = 0; i < row.size(); ++i) {
                if (i > 0) std::cout << " | ";
                std::cout << row[i];
            }
            std::cout << std::endl;
        }
        return;
    }
    bool is_select_star = (!ast.select_columns.empty() && ast.select_columns[0] == "*") || projection.empty();
    std::vector<std::string> header_cols = col_names;
    std::vector<std::vector<std::string>> rows;
    if (is_select_star) {
        header_cols = col_names;
        rows = storage.scan(ast.from_table);
    } else {
        rows = storage.scan(ast.from_table, projection, filter_func, order_by, limit, aggregate);
        header_cols.clear();
        for (int idx : projection) {
            if (idx >= 0 && static_cast<size_t>(idx) < col_names.size()) header_cols.push_back(col_names[idx]);
        }
        if (header_cols.empty()) header_cols = ast.select_columns;
    }
    bool is_sum = aggregate && aggregate->first == "SUM";
    if (!is_sum) {
        for (size_t i = 0; i < header_cols.size(); ++i) {
            if (i > 0) std::cout << " | ";
            std::cout << header_cols[i];
        }
        std::cout << std::endl;
    }
    if (!rows.empty() && !is_sum) {
        for (const auto& row : rows) {
            for (size_t i = 0; i < row.size(); ++i) {
                if (i > 0) std::cout << " | ";
                std::cout << row[i];
            }
            std::cout << std::endl;
        }
    } else if (is_sum && !rows.empty()) {
        std::cout << "SUM: " << rows[0][0] << std::endl;
    }
} 