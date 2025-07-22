#include "sql_lexer.h"
#include <cctype>
#include <sstream>
#include <stdexcept>
#include <unordered_set>

static const std::unordered_set<std::string> keywords = {
    "SELECT", "FROM", "WHERE", "ORDER", "BY", "LIMIT", "INSERT", "INTO", "VALUES", "UPDATE", "SET", "DELETE", "JOIN", "ON", "AS", "AND", "OR", "SUM", "ABS"
};

std::vector<Token> SqlLexer::tokenize(const std::string& input) {
    std::vector<Token> tokens;
    size_t i = 0;
    while (i < input.size()) {
        if (isspace(input[i])) { ++i; continue; }
        if (isalpha(input[i]) || input[i] == '_') {
            size_t start = i;
            while (i < input.size() && (isalnum(input[i]) || input[i] == '_')) ++i;
            std::string word = input.substr(start, i - start);
            std::string upper_word = word;
            for (auto& c : upper_word) c = toupper(c);
            if (keywords.count(upper_word)) tokens.push_back({TokenType::Keyword, upper_word});
            else tokens.push_back({TokenType::Identifier, word});
        } else if (isdigit(input[i])) {
            size_t start = i;
            while (i < input.size() && isdigit(input[i])) ++i;
            tokens.push_back({TokenType::Number, input.substr(start, i - start)});
        } else if (input[i] == '\'' || input[i] == '"') {
            char quote = input[i++];
            size_t start = i;
            while (i < input.size() && input[i] != quote) ++i;
            if (i >= input.size()) throw std::runtime_error("Unterminated string literal");
            tokens.push_back({TokenType::String, input.substr(start, i - start)});
            ++i;
        } else if (ispunct(input[i])) {
            std::string op(1, input[i]);
            if ((input[i] == '<' || input[i] == '>') && i + 1 < input.size() && input[i+1] == '=') {
                op += input[++i];
            } else if (input[i] == '!' && i + 1 < input.size() && input[i+1] == '=') {
                op += input[++i];
            }
            tokens.push_back({TokenType::Operator, op});
            ++i;
        } else {
            throw std::runtime_error("Unknown character in SQL: " + std::string(1, input[i]));
        }
    }
    return tokens;
}

#ifdef SQL_LEXER_MAIN
#include <iostream>
int main() {
    SqlLexer lexer;
    std::string sql = "SELECT a, b FROM t WHERE x = 5 ORDER BY y DESC LIMIT 10";
    auto tokens = lexer.tokenize(sql);
    for (const auto& t : tokens) {
        std::cout << (int)t.type << ": " << t.text << std::endl;
    }
    return 0;
}
#endif 