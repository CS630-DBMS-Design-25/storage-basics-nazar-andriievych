#pragma once
#include "sql_lexer.h"
#include <string>
#include <vector>
#include <memory>
#include <optional>
#include <ostream>

enum class SqlAstType {
    Select
};

struct WhereClause {
    std::string col;
    std::string op;
    std::string val;
};

struct SqlAst {
    SqlAstType type;
    std::vector<std::string> select_columns;
    std::string from_table;
    std::string join_table;
    std::string join_left_col;
    std::string join_right_col;
    std::vector<WhereClause> where_clauses;
    std::vector<std::pair<std::string, bool>> order_by;
    std::optional<int> limit;
    std::optional<std::pair<std::string, std::string>> aggregate; // op, col
    void pretty_print(std::ostream& os) const;
};

class SqlParser {
public:
    std::unique_ptr<SqlAst> parse(const std::vector<Token>& tokens);
}; 