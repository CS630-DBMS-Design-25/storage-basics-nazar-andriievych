#include <iostream>
#include <string>
#include <sstream>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include "storage_layer.h"

constexpr char CMD_OPEN[] = "open";
constexpr char CMD_CLOSE[] = "close";
constexpr char CMD_INSERT[] = "insert";
constexpr char CMD_GET[] = "get";
constexpr char CMD_UPDATE[] = "update";
constexpr char CMD_DELETE[] = "delete";
constexpr char CMD_SCAN[] = "scan";
constexpr char CMD_FLUSH[] = "flush";
constexpr char CMD_HELP[] = "help";
constexpr char CMD_EXIT[] = "exit";
constexpr char CMD_QUIT[] = "quit";
constexpr char PROMPT[] = "storage-cli> ";
constexpr char TYPE_INT[] = "INT";
constexpr char TYPE_TEXT[] = "TEXT";
constexpr char HELP_MESSAGE[] =
    "Storage Layer CLI - Available commands:\n"
    "  open <path>                  - Open storage at specified path\n"
    "  close                        - Close the storage\n"
    "  create <table> <col1>:<type1> ... - Create a table with schema\n"
    "  insert <table> <val1,val2,...>    - Insert a record\n"
    "  get <table> <record_id>            - Get a record by ID\n"
    "  update <table> <record_id> <val1,val2,...> - Update a record\n"
    "  delete <table> <record_id>         - Delete a record\n"
    "  scan <table> [options]             - Scan records in a table\n"
    "    Options:\n"
    "      --projection <field1> <field2> ...   - Select columns to return\n"
    "      --where <col>=<val>                  - Filter rows (exact match, repeatable)\n"
    "      --orderby <col>[:asc|desc] ...       - Order by columns (default asc)\n"
    "      --limit <N>                          - Limit number of rows\n"
    "      --aggregate <SUM|ABS>:<col>          - Aggregate (SUM or ABS) on INT column\n"
    "  flush                        - Flush data to disk\n"
    "  help                         - Display this help message\n"
    "  exit/quit                    - Exit the program\n";

#define COLOR_RESET   "\033[0m"
#define COLOR_BOLD    "\033[1m"
#define COLOR_UNDER   "\033[4m"
#define COLOR_RED     "\033[31m"
#define COLOR_GREEN   "\033[32m"
#define COLOR_YELLOW  "\033[33m"
#define COLOR_BLUE    "\033[34m"
#define COLOR_CYAN    "\033[36m"
#define COLOR_WHITE   "\033[37m"

void print_success(const std::string& msg) {
    std::cout << COLOR_GREEN << msg << COLOR_RESET << std::endl;
}
void print_error(const std::string& msg) {
    std::cout << COLOR_RED << msg << COLOR_RESET << std::endl;
}
void print_prompt() {
    std::cout << COLOR_CYAN << "storage-cli> " << COLOR_RESET;
}
void print_help() {
    std::cout << COLOR_BOLD << HELP_MESSAGE << COLOR_RESET;
}
void print_table_header(const std::vector<std::string>& headers) {
    std::cout << COLOR_UNDER << COLOR_BOLD;
    for (size_t i = 0; i < headers.size(); ++i) {
        if (i > 0) std::cout << " | ";
        std::cout << headers[i];
    }
    std::cout << COLOR_RESET << std::endl;
}
void print_table_row(const std::vector<std::string>& values) {
    for (size_t i = 0; i < values.size(); ++i) {
        if (i > 0) std::cout << " | ";
        std::cout << values[i];
    }
    std::cout << std::endl;
}

std::vector<uint8_t> string_to_bytes(const std::string& str) {
    return std::vector<uint8_t>(str.begin(), str.end());
}

std::string bytes_to_string(const std::vector<uint8_t>& bytes) {
    return std::string(bytes.begin(), bytes.end());
}

std::vector<std::string> parse_args(const std::string& input) {
    std::vector<std::string> args;
    std::istringstream iss(input);
    std::string arg;
    while (iss >> arg) {
        args.push_back(arg);
    }
    return args;
}

ColumnType parse_column_type(const std::string& s) {
    if (s == TYPE_INT) return ColumnType::INT;
    if (s == TYPE_TEXT) return ColumnType::TEXT;
    throw std::runtime_error("Unknown column type: " + s);
}

std::vector<std::string> parse_values(const std::string& s) {
    std::vector<std::string> vals;
    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, ',')) {
        vals.push_back(item);
    }
    return vals;
}

bool check_args(const std::vector<std::string>& args, size_t min_args, const char* usage_msg) {
    if (args.size() < min_args) {
        std::cout << usage_msg << std::endl;
        return false;
    }
    return true;
}

template<typename Func>
void run_command(Func&& f) {
    try {
        f();
    } catch (const std::exception& e) {
        std::cout << "Error: " << e.what() << std::endl;
    }
}

ColumnSchema parse_column_schema(const std::string& spec) {
    auto pos = spec.find(":");
    if (pos == std::string::npos) {
        throw std::runtime_error("Column format must be name:TYPE");
    }
    std::string cname = spec.substr(0, pos);
    std::string ctype = spec.substr(pos + 1);
    ColumnSchema col{};
    std::memset(&col, 0, sizeof(ColumnSchema));
    size_t clen = std::min(cname.size(), sizeof(col.name) - 1);
    std::memcpy(col.name, cname.c_str(), clen);
    col.name[clen] = '\0';
    col.type = parse_column_type(ctype);
    col.size = (col.type == ColumnType::INT) ? INT_SIZE : 0;
    return col;
}

int main() {
    FileStorageLayer storage;

    print_help();

    while (true) {
        std::string input;
        print_prompt();
        std::getline(std::cin, input);

        if (input.empty()) {
            continue;
        }

        std::vector<std::string> args = parse_args(input);
        std::string command = args[0];

        if (command == CMD_EXIT || command == CMD_QUIT) {
            break;
        } else if (command == CMD_HELP) {
            print_help();
        } else if (command == CMD_OPEN) {
            if (!check_args(args, 2, "Error: Missing path argument")) continue;
            run_command([&] {
                storage.open(args[1]);
                print_success("Storage opened at " + args[1]);
            });
        } else if (command == CMD_CLOSE) {
            run_command([&] {
                storage.close();
                print_success("Storage closed");
            });
        } else if (command == "create") {
            if (!check_args(args, 3, "Error: Usage: create <table> <col1>:<type1> ...")) continue;
            std::vector<ColumnSchema> schema;
            try {
                for (size_t i = 2; i < args.size(); ++i) {
                    schema.push_back(parse_column_schema(args[i]));
                }
                run_command([&] {
                    storage.create(args[1], schema);
                    print_success("Table created: " + args[1]);
                });
            } catch (const std::exception& e) {
                print_error(std::string("Error: ") + e.what());
            }
        } else if (command == CMD_INSERT) {
            if (!check_args(args, 3, "Error: Usage: insert <table> <val1,val2,...>")) continue;
            run_command([&] {
                int record_id = storage.insert(args[1], parse_values(args[2]));
                print_success("Record inserted with ID " + std::to_string(record_id));
            });
        } else if (command == CMD_GET) {
            if (!check_args(args, 3, "Error: Missing arguments. Usage: get <table> <record_id>")) continue;
            run_command([&] {
                int record_id = std::stoi(args[2]);
                auto values = storage.get(args[1], record_id);
                std::cout << COLOR_BOLD << "Retrieved record: " << COLOR_RESET;
                print_table_row(values);
            });
        } else if (command == CMD_UPDATE) {
            if (!check_args(args, 4, "Error: Usage: update <table> <record_id> <val1,val2,...>")) continue;
            run_command([&] {
                int record_id = std::stoi(args[2]);
                storage.update(args[1], record_id, parse_values(args[3]));
                print_success("Record updated");
            });
        } else if (command == CMD_DELETE) {
            if (!check_args(args, 3, "Error: Missing arguments. Usage: delete <table> <record_id>")) continue;
            run_command([&] {
                int record_id = std::stoi(args[2]);
                storage.delete_record(args[1], record_id);
                print_success("Record deleted");
            });
        } else if (command == CMD_SCAN) {
            if (!check_args(args, 2, "Error: Missing table argument. Usage: scan <table> [options]")) continue;
            run_command([&] {
                std::string table = args[1];
                std::optional<std::vector<int>> projection;
                std::optional<std::function<bool(const std::vector<std::string>&)>> filter_func;
                std::optional<std::vector<std::pair<int, bool>>> order_by;
                std::optional<size_t> limit;
                std::optional<std::pair<std::string, int>> aggregate;
                std::vector<std::string> col_names = storage.get_column_names(table);
                for (size_t i = 2; i < args.size(); ++i) {
                    if (args[i] == "--projection" && i + 1 < args.size()) {
                        std::vector<int> proj;
                        ++i;
                        while (i < args.size() && args[i].rfind("--", 0) != 0) {
                            auto it = std::find(col_names.begin(), col_names.end(), args[i]);
                            if (it != col_names.end()) {
                                proj.push_back(static_cast<int>(std::distance(col_names.begin(), it)));
                            }
                            ++i;
                        }
                        --i;
                        if (!proj.empty()) projection = proj;
                    } else if (args[i] == "--where" && i + 1 < args.size()) {
                        std::vector<std::pair<int, std::string>> filters;
                        ++i;
                        while (i < args.size() && args[i].rfind("--", 0) != 0) {
                            auto eq = args[i].find('=');
                            if (eq != std::string::npos) {
                                std::string col = args[i].substr(0, eq);
                                std::string val = args[i].substr(eq + 1);
                                auto it = std::find(col_names.begin(), col_names.end(), col);
                                if (it != col_names.end()) {
                                    filters.emplace_back(static_cast<int>(std::distance(col_names.begin(), it)), val);
                                }
                            }
                            ++i;
                        }
                        --i;
                        if (!filters.empty()) {
                            filter_func = [filters](const std::vector<std::string>& row) {
                                for (const auto& [idx, val] : filters) {
                                    if (idx < 0 || static_cast<size_t>(idx) >= row.size() || row[idx] != val) return false;
                                }
                                return true;
                            };
                        }
                    } else if (args[i] == "--orderby" && i + 1 < args.size()) {
                        std::vector<std::pair<int, bool>> order;
                        ++i;
                        while (i < args.size() && args[i].rfind("--", 0) != 0) {
                            std::string col = args[i];
                            bool asc = true;
                            auto pos = col.find(":");
                            if (pos != std::string::npos) {
                                asc = (col.substr(pos + 1) != "desc");
                                col = col.substr(0, pos);
                            }
                            auto it = std::find(col_names.begin(), col_names.end(), col);
                            if (it != col_names.end()) {
                                order.emplace_back(static_cast<int>(std::distance(col_names.begin(), it)), asc);
                            }
                            ++i;
                        }
                        --i;
                        if (!order.empty()) order_by = order;
                    } else if (args[i] == "--limit" && i + 1 < args.size()) {
                        limit = static_cast<size_t>(std::stoul(args[++i]));
                    } else if (args[i] == "--aggregate" && i + 1 < args.size()) {
                        std::string agg = args[++i];
                        auto pos = agg.find(":");
                        if (pos != std::string::npos) {
                            std::string op = agg.substr(0, pos);
                            std::string col = agg.substr(pos + 1);
                            auto it = std::find(col_names.begin(), col_names.end(), col);
                            if (it != col_names.end()) {
                                aggregate = std::make_pair(op, static_cast<int>(std::distance(col_names.begin(), it)));
                            }
                        }
                    }
                }
                auto rows = storage.scan(table, projection, filter_func, order_by, limit, aggregate);
                bool is_sum = aggregate && aggregate->first == "SUM";
                if (!rows.empty() && !is_sum) {
                    std::vector<std::string> headers;
                    if (projection) {
                        for (int idx : *projection) {
                            if (idx >= 0 && static_cast<size_t>(idx) < col_names.size()) headers.push_back(col_names[idx]);
                        }
                    } else {
                        headers = col_names;
                    }
                    print_table_header(headers);
                }
                for (const auto& values : rows) {
                    print_table_row(values);
                }
                if (is_sum && !rows.empty()) {
                    std::cout << COLOR_BOLD << "SUM: " << rows[0][0] << COLOR_RESET << std::endl;
                }
            });
        } else if (command == CMD_FLUSH) {
            run_command([&] {
                storage.flush();
                print_success("Storage flushed");
            });
        } else {
            print_error("Unknown command: " + command + "\nType 'help' for available commands");
        }
    }

    return 0;
}