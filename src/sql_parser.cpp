#include "sql_parser.h"
#include <stdexcept>
#include <iostream>

static void expect(const std::vector<Token>& tokens, size_t& i, TokenType type, const std::string& value = "") {
    if (i >= tokens.size() || tokens[i].type != type || (!value.empty() && tokens[i].text != value))
        throw std::runtime_error("Unexpected token: " + (i < tokens.size() ? tokens[i].text : "<end>"));
}

std::unique_ptr<SqlAst> SqlParser::parse(const std::vector<Token>& tokens) {
    size_t i = 0;
    expect(tokens, i, TokenType::Keyword, "SELECT");
    ++i;
    auto ast = std::make_unique<SqlAst>();
    ast->type = SqlAstType::Select;
    while (i < tokens.size() && tokens[i].type != TokenType::Keyword) {
        if (tokens[i].type == TokenType::Identifier) {
            ast->select_columns.push_back(tokens[i].text);
        }
        ++i;
        if (i < tokens.size() && tokens[i].text == ",") ++i;
    }
    expect(tokens, i, TokenType::Keyword, "FROM");
    ++i;
    if (tokens[i].type != TokenType::Identifier) throw std::runtime_error("Expected table name");
    ast->from_table = tokens[i++].text;
    if (i < tokens.size() && tokens[i].type == TokenType::Keyword && tokens[i].text == "JOIN") {
        ++i;
        if (tokens[i].type != TokenType::Identifier) throw std::runtime_error("Expected join table name");
        ast->join_table = tokens[i++].text;
        expect(tokens, i, TokenType::Keyword, "ON");
        ++i;
        if (tokens[i].type != TokenType::Identifier) throw std::runtime_error("Expected join column");
        ast->join_left_col = tokens[i++].text;
        expect(tokens, i, TokenType::Operator, "=");
        ++i;
        if (tokens[i].type != TokenType::Identifier) throw std::runtime_error("Expected join column");
        ast->join_right_col = tokens[i++].text;
    }
    if (i < tokens.size() && tokens[i].type == TokenType::Keyword && tokens[i].text == "WHERE") {
        ++i;
        while (i < tokens.size() && tokens[i].type == TokenType::Identifier) {
            std::string col = tokens[i++].text;
            expect(tokens, i, TokenType::Operator);
            std::string op = tokens[i++].text;
            std::string val = tokens[i++].text;
            ast->where_clauses.push_back({col, op, val});
            if (i < tokens.size() && tokens[i].text == "AND") ++i;
        }
    }
    if (i < tokens.size() && tokens[i].type == TokenType::Keyword && tokens[i].text == "ORDER") {
        ++i;
        expect(tokens, i, TokenType::Keyword, "BY");
        ++i;
        while (i < tokens.size() && tokens[i].type == TokenType::Identifier) {
            std::string col = tokens[i++].text;
            bool asc = true;
            if (i < tokens.size() && tokens[i].type == TokenType::Keyword && (tokens[i].text == "ASC" || tokens[i].text == "DESC")) {
                asc = (tokens[i].text == "ASC");
                ++i;
            }
            ast->order_by.push_back({col, asc});
            if (i < tokens.size() && tokens[i].text == ",") ++i;
        }
    }
    if (i < tokens.size() && tokens[i].type == TokenType::Keyword && tokens[i].text == "LIMIT") {
        ++i;
        ast->limit = std::stoi(tokens[i++].text);
    }
    for (auto& col : ast->select_columns) {
        if (col.find("SUM(") == 0) ast->aggregate = {"SUM", col.substr(4, col.size() - 5)};
        if (col.find("ABS(") == 0) ast->aggregate = {"ABS", col.substr(4, col.size() - 5)};
    }
    return ast;
}

void SqlAst::pretty_print(std::ostream& os) const {
    os << "SELECT ";
    for (size_t i = 0; i < select_columns.size(); ++i) {
        if (i > 0) os << ", ";
        os << select_columns[i];
    }
    os << " FROM " << from_table;
    if (!join_table.empty()) {
        os << " JOIN " << join_table << " ON " << join_left_col << " = " << join_right_col;
    }
    if (!where_clauses.empty()) {
        os << " WHERE ";
        for (size_t i = 0; i < where_clauses.size(); ++i) {
            if (i > 0) os << " AND ";
            os << where_clauses[i].col << " " << where_clauses[i].op << " " << where_clauses[i].val;
        }
    }
    if (!order_by.empty()) {
        os << " ORDER BY ";
        for (size_t i = 0; i < order_by.size(); ++i) {
            if (i > 0) os << ", ";
            os << order_by[i].first << (order_by[i].second ? " ASC" : " DESC");
        }
    }
    if (limit) os << " LIMIT " << *limit;
    os << std::endl;
}

#ifdef SQL_PARSER_MAIN
#include "sql_lexer.h"
int main() {
    SqlLexer lexer;
    SqlParser parser;
    std::string sql = "SELECT a, SUM(b) FROM t1 JOIN t2 ON t1.id = t2.id WHERE x = 5 AND y > 2 ORDER BY z DESC LIMIT 10";
    auto tokens = lexer.tokenize(sql);
    auto ast = parser.parse(tokens);
    ast->pretty_print(std::cout);
    return 0;
}
#endif 