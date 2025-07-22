#include "sql_lexer.h"
#include "sql_parser.h"
#include "sql_executor.h"
#include "storage_layer.h"
#include <iostream>
#include <string>
#include <sstream>
#include <vector>
#include <algorithm>

void print_sql_help() {
    std::cout << "\nSQL CLI Help:\n";
    std::cout << "  Supported commands (SQL-92 subset):\n";
    std::cout << "    CREATE TABLE table (col1 TYPE, col2 TYPE, ...);\n";
    std::cout << "    INSERT INTO table VALUES (val1, val2, ...);\n";
    std::cout << "    DELETE FROM table [WHERE col = val [AND ...]];\n";
    std::cout << "    SELECT col1, col2 FROM table [WHERE col = val [AND ...]] [ORDER BY col [ASC|DESC]] [LIMIT N];\n";
    std::cout << "    SELECT * FROM table ...\n";
    std::cout << "    SELECT SUM(col) FROM table ...\n";
    std::cout << "    SELECT ... FROM t1 JOIN t2 ON t1.col = t2.col ...\n";
    std::cout << "    SELECT ABS(col) FROM table ...\n";
    std::cout << "  Type 'help' to see this message again.\n";
    std::cout << "  Type 'exit' or 'quit' to leave the SQL CLI.\n";
    std::cout << "  Type 'AST ON' or 'AST OFF' to enable/disable AST printing.\n";
    std::cout << std::endl;
}

// Helper: trim and uppercase
std::string trim(const std::string& s) {
    size_t start = s.find_first_not_of(" \t\n\r");
    size_t end = s.find_last_not_of(" \t\n\r");
    return (start == std::string::npos) ? "" : s.substr(start, end - start + 1);
}
std::string to_upper(const std::string& s) {
    std::string r = s;
    std::transform(r.begin(), r.end(), r.begin(), ::toupper);
    return r;
}

int main() {
    FileStorageLayer storage;
    std::string db_path;
    std::cout << "Enter storage path: ";
    std::getline(std::cin, db_path);
    storage.open(db_path);
    print_sql_help();
    std::cout << "SQL CLI. Type SQL queries, or 'exit' to quit." << std::endl;
    SqlLexer lexer;
    SqlParser parser;
    SqlExecutor executor;
    bool print_ast = false;
    while (true) {
        std::cout << "sql> ";
        std::string line;
        std::getline(std::cin, line);
        if (line == "exit" || line == "quit") break;
        if (line == "help") { print_sql_help(); continue; }
        std::string uline = to_upper(trim(line));
        if (uline == "AST ON") { print_ast = true; std::cout << "AST printing enabled.\n"; continue; }
        if (uline == "AST OFF") { print_ast = false; std::cout << "AST printing disabled.\n"; continue; }
        // CREATE TABLE
        if (uline.find("CREATE TABLE") == 0) {
            size_t name_start = uline.find("TABLE") + 5;
            size_t paren_start = line.find('(');
            size_t paren_end = line.find(')');
            if (paren_start == std::string::npos || paren_end == std::string::npos) {
                std::cout << "Syntax error in CREATE TABLE." << std::endl;
                continue;
            }
            std::string table = trim(line.substr(name_start, paren_start - name_start));
            std::string cols = line.substr(paren_start + 1, paren_end - paren_start - 1);
            std::vector<ColumnSchema> schema;
            std::stringstream ss(cols);
            std::string coldef;
            bool col_error = false;
            while (std::getline(ss, coldef, ',')) {
                size_t space = coldef.find_last_of(" ");
                if (space == std::string::npos) { std::cout << "Column definition error." << std::endl; col_error = true; break; }
                std::string cname = trim(coldef.substr(0, space));
                std::string ctype = trim(coldef.substr(space + 1));
                if (cname.empty() || ctype.empty()) { std::cout << "Column name/type missing." << std::endl; col_error = true; break; }
                ColumnSchema col{};
                std::memset(&col, 0, sizeof(ColumnSchema));
                size_t clen = std::min(cname.size(), sizeof(col.name) - 1);
                std::memcpy(col.name, cname.c_str(), clen);
                col.name[clen] = '\0';
                if (to_upper(ctype) == "INT") { col.type = ColumnType::INT; col.size = INT_SIZE; }
                else if (to_upper(ctype) == "TEXT") { col.type = ColumnType::TEXT; col.size = 0; }
                else { std::cout << "Unknown type: " << ctype << std::endl; col_error = true; break; }
                schema.push_back(col);
            }
            if (col_error || schema.empty()) {
                std::cout << "CREATE TABLE failed: invalid column definitions." << std::endl;
                continue;
            }
            try {
                storage.create(trim(table), schema);
                std::cout << "Table created: " << trim(table) << std::endl;
                storage.flush();
            } catch (const std::exception& e) {
                std::cout << "Error: " << e.what() << std::endl;
            }
            continue;
        }
        // INSERT INTO
        if (uline.find("INSERT INTO") == 0) {
            size_t name_start = uline.find("INTO") + 4;
            size_t values_pos = uline.find("VALUES");
            if (values_pos == std::string::npos) { std::cout << "Syntax error in INSERT." << std::endl; continue; }
            std::string table = trim(line.substr(name_start, values_pos - name_start));
            size_t paren_start = line.find('(', values_pos);
            size_t paren_end = line.find(')', paren_start);
            if (paren_start == std::string::npos || paren_end == std::string::npos) { std::cout << "Syntax error in INSERT." << std::endl; continue; }
            std::string vals = line.substr(paren_start + 1, paren_end - paren_start - 1);
            std::vector<std::string> values;
            std::stringstream ss(vals);
            std::string val;
            while (std::getline(ss, val, ',')) values.push_back(trim(val));
            // Check table exists and value count matches
            try {
                auto col_names = storage.get_column_names(table);
                if (col_names.size() != values.size()) {
                    std::cout << "INSERT failed: value count does not match column count." << std::endl;
                    continue;
                }
            } catch (...) {
                std::cout << "INSERT failed: table does not exist." << std::endl;
                continue;
            }
            try {
                int record_id = storage.insert(trim(table), values);
                std::cout << "Inserted record with ID: " << record_id << std::endl;
                storage.flush();
            } catch (const std::exception& e) {
                std::cout << "Error: " << e.what() << std::endl;
            }
            continue;
        }
        // DELETE FROM
        if (uline.find("DELETE FROM") == 0) {
            size_t name_start = uline.find("FROM") + 4;
            size_t where_pos = uline.find("WHERE");
            std::string table = trim(line.substr(name_start, (where_pos == std::string::npos ? std::string::npos : where_pos - name_start)));
            // Check table exists
            std::vector<std::string> col_names;
            try {
                col_names = storage.get_column_names(table);
            } catch (...) {
                std::cout << "DELETE failed: table does not exist." << std::endl;
                continue;
            }
            std::vector<int> to_delete;
            // If WHERE present, scan and delete matching
            if (where_pos != std::string::npos) {
                std::string conds = line.substr(where_pos + 5);
                std::vector<std::pair<int, std::string>> filters;
                std::stringstream ss(conds);
                std::string cond;
                while (std::getline(ss, cond, 'A')) { // crude split on AND
                    size_t eq = cond.find('=');
                    if (eq == std::string::npos) continue;
                    std::string col = trim(cond.substr(0, eq));
                    std::string val = trim(cond.substr(eq + 1));
                    auto it = std::find(col_names.begin(), col_names.end(), col);
                    if (it != col_names.end()) filters.emplace_back(static_cast<int>(std::distance(col_names.begin(), it)), val);
                }
                auto rows = storage.scan(table);
                for (size_t i = 0; i < rows.size(); ++i) {
                    bool match = true;
                    for (const auto& [idx, val] : filters) {
                        if (idx < 0 || static_cast<size_t>(idx) >= rows[i].size() || rows[i][idx] != val) { match = false; break; }
                    }
                    if (match) to_delete.push_back(i + 1); // record_id is 1-based
                }
            } else {
                // No WHERE: delete all
                auto rows = storage.scan(table);
                for (size_t i = 0; i < rows.size(); ++i) to_delete.push_back(i + 1);
            }
            int deleted = 0;
            for (int rid : to_delete) {
                try { storage.delete_record(table, rid); ++deleted; } catch (...) {}
            }
            std::cout << "Deleted " << deleted << " record(s) from " << table << std::endl;
            storage.flush();
            continue;
        }
        // Otherwise, try SELECT or other supported SQL
        try {
            auto tokens = lexer.tokenize(line);
            auto ast = parser.parse(tokens);
            // SELECT * support: expand * to all columns
            if (!ast->select_columns.empty() && ast->select_columns[0] == "*") {
                if (!ast->join_table.empty()) {
                    // Check both tables exist
                    std::vector<std::string> left_cols, right_cols;
                    try { left_cols = storage.get_column_names(ast->from_table); } catch (...) {
                        std::cout << "SELECT failed: table '" << ast->from_table << "' does not exist." << std::endl; continue; }
                    try { right_cols = storage.get_column_names(ast->join_table); } catch (...) {
                        std::cout << "SELECT failed: table '" << ast->join_table << "' does not exist." << std::endl; continue; }
                    ast->select_columns.clear();
                    ast->select_columns.insert(ast->select_columns.end(), left_cols.begin(), left_cols.end());
                    ast->select_columns.insert(ast->select_columns.end(), right_cols.begin(), right_cols.end());
                } else {
                    std::vector<std::string> cols;
                    try { cols = storage.get_column_names(ast->from_table); } catch (...) {
                        std::cout << "SELECT failed: table '" << ast->from_table << "' does not exist." << std::endl; continue; }
                    ast->select_columns = cols;
                }
            } else {
                // Check all columns exist in the table(s)
                std::vector<std::string> all_cols;
                try { all_cols = storage.get_column_names(ast->from_table); } catch (...) {
                    std::cout << "SELECT failed: table '" << ast->from_table << "' does not exist." << std::endl; continue; }
                if (!ast->join_table.empty()) {
                    std::vector<std::string> right_cols;
                    try { right_cols = storage.get_column_names(ast->join_table); } catch (...) {
                        std::cout << "SELECT failed: table '" << ast->join_table << "' does not exist." << std::endl; continue; }
                    all_cols.insert(all_cols.end(), right_cols.begin(), right_cols.end());
                }
                for (const auto& col : ast->select_columns) {
                    std::string cname = col;
                    if (col.find("SUM(") == 0 || col.find("ABS(") == 0) {
                        size_t l = col.find('(') + 1, r = col.find(')');
                        if (l == std::string::npos || r == std::string::npos || r <= l) continue;
                        cname = col.substr(l, r - l);
                    }
                    if (std::find(all_cols.begin(), all_cols.end(), cname) == all_cols.end()) {
                        std::cout << "SELECT failed: column '" << cname << "' does not exist." << std::endl; goto select_end;
                    }
                }
            }
            if (print_ast) ast->pretty_print(std::cout);
            executor.execute(*ast, storage);
            storage.flush();
select_end:;
        } catch (const std::exception& e) {
            std::cout << "Error: " << e.what() << std::endl;
        }
    }
    storage.close();
    return 0;
} 