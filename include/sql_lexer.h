#pragma once
#include <string>
#include <vector>

enum class TokenType {
    Keyword,
    Identifier,
    Number,
    String,
    Operator
};

struct Token {
    TokenType type;
    std::string text;
};

class SqlLexer {
public:
    std::vector<Token> tokenize(const std::string& input);
}; 