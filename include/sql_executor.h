#pragma once
#include "sql_parser.h"
#include "storage_layer.h"

class SqlExecutor {
public:
    void execute(const SqlAst& ast, FileStorageLayer& storage);
}; 