#include <iostream>
#include <string>
#include <sstream>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include "storage_layer.h"

void print_help() {
    std::cout << "Storage Layer CLI - Available commands:\n"
              << "  open <path>                  - Open storage at specified path\n"
              << "  close                        - Close the storage\n"
              << "  insert <table> <record>      - Insert a record\n"
              << "  get <table> <record_id>      - Get a record by ID\n"
              << "  update <table> <record_id> <record> - Update a record\n"
              << "  delete <table> <record_id>   - Delete a record\n"
              << "  scan <table> [--projection <field1> <field2> ...] - Scan records in a table\n"
              << "  flush                        - Flush data to disk\n"
              << "  help                         - Display this help message\n"
              << "  exit/quit                    - Exit the program\n";
}

// Convert a string to a vector of bytes
std::vector<uint8_t> string_to_bytes(const std::string& str) {
    return std::vector<uint8_t>(str.begin(), str.end());
}

// Convert a vector of bytes to a string
std::string bytes_to_string(const std::vector<uint8_t>& bytes) {
    return std::string(bytes.begin(), bytes.end());
}

// Parse command line arguments
std::vector<std::string> parse_args(const std::string& input) {
    std::vector<std::string> args;
    std::istringstream iss(input);
    std::string arg;

    while (iss >> arg) {
        args.push_back(arg);
    }

    return args;
}

// Helper: Parse column type from string
ColumnType parse_column_type(const std::string& s) {
    if (s == "INT") return ColumnType::INT;
    if (s == "TEXT") return ColumnType::TEXT;
    throw std::runtime_error("Unknown column type: " + s);
}

// Helper: Parse values from comma-separated string
std::vector<std::string> parse_values(const std::string& s) {
    std::vector<std::string> vals;
    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, ',')) {
        vals.push_back(item);
    }
    return vals;
}

int main() {
    FileStorageLayer storage;

    std::cout << "Storage Layer CLI - Type 'help' for available commands or 'exit' to quit\n";

    while (true) {
        std::string input;
        std::cout << "storage-cli> ";
        std::getline(std::cin, input);

        if (input.empty()) {
            continue;
        }

        std::vector<std::string> args = parse_args(input);
        std::string command = args[0];

        if (command == "exit" || command == "quit") {
            break;
        } else if (command == "help") {
            print_help();
        } else if (command == "open") {
            if (args.size() < 2) {
                std::cout << "Error: Missing path argument\n";
                continue;
            }
            try {
                storage.open(args[1]);
                std::cout << "Storage opened at " << args[1] << std::endl;
            } catch (const std::exception& e) {
                std::cout << "Error: " << e.what() << std::endl;
            }
        } else if (command == "close") {
            try {
                storage.close();
                std::cout << "Storage closed\n";
            } catch (const std::exception& e) {
                std::cout << "Error: " << e.what() << std::endl;
            }
        } else if (command == "create") {
            // Usage: create <table> <col1>:<type1> <col2>:<type2> ...
            if (args.size() < 3) {
                std::cout << "Error: Usage: create <table> <col1>:<type1> ...\n";
                continue;
            }
            std::vector<ColumnSchema> schema;
            for (size_t i = 2; i < args.size(); ++i) {
                auto pos = args[i].find(":");
                if (pos == std::string::npos) {
                    std::cout << "Error: Column format must be name:TYPE\n";
                    goto create_fail;
                }
                std::string cname = args[i].substr(0, pos);
                std::string ctype = args[i].substr(pos + 1);
                ColumnSchema col{};
                std::memset(&col, 0, sizeof(ColumnSchema));
                size_t clen = std::min(cname.size(), sizeof(col.name) - 1);
                std::memcpy(col.name, cname.c_str(), clen);
                col.name[clen] = '\0';
                col.type = parse_column_type(ctype);
                col.size = (col.type == ColumnType::INT) ? 4 : 0;
                schema.push_back(col);
            }
            try {
                storage.create(args[1], schema);
                std::cout << "Table created: " << args[1] << std::endl;
            } catch (const std::exception& e) {
                std::cout << "Error: " << e.what() << std::endl;
            }
        create_fail:;
        } else if (command == "insert") {
            // Usage: insert <table> <val1,val2,...>
            if (args.size() < 3) {
                std::cout << "Error: Usage: insert <table> <val1,val2,...>\n";
                continue;
            }
            try {
                int record_id = storage.insert(args[1], parse_values(args[2]));
                std::cout << "Record inserted with ID " << record_id << std::endl;
            } catch (const std::exception& e) {
                std::cout << "Error: " << e.what() << std::endl;
            }
        } else if (command == "get") {
            if (args.size() < 3) {
                std::cout << "Error: Missing arguments. Usage: get <table> <record_id>\n";
                continue;
            }
            try {
                int record_id = std::stoi(args[2]);
                auto values = storage.get(args[1], record_id);
                std::cout << "Retrieved record: ";
                for (size_t i = 0; i < values.size(); ++i) {
                    if (i > 0) std::cout << ",";
                    std::cout << values[i];
                }
                std::cout << std::endl;
            } catch (const std::exception& e) {
                std::cout << "Error: " << e.what() << std::endl;
            }
        } else if (command == "update") {
            // Usage: update <table> <record_id> <val1,val2,...>
            if (args.size() < 4) {
                std::cout << "Error: Usage: update <table> <record_id> <val1,val2,...>\n";
                continue;
            }
            try {
                int record_id = std::stoi(args[2]);
                storage.update(args[1], record_id, parse_values(args[3]));
                std::cout << "Record updated\n";
            } catch (const std::exception& e) {
                std::cout << "Error: " << e.what() << std::endl;
            }
        } else if (command == "delete") {
            if (args.size() < 3) {
                std::cout << "Error: Missing arguments. Usage: delete <table> <record_id>\n";
                continue;
            }
            try {
                int record_id = std::stoi(args[2]);
                storage.delete_record(args[1], record_id);
                std::cout << "Record deleted\n";
            } catch (const std::exception& e) {
                std::cout << "Error: " << e.what() << std::endl;
            }
        } else if (command == "scan") {
            if (args.size() < 2) {
                std::cout << "Error: Missing table argument. Usage: scan <table> [--projection <field1> <field2> ...]\n";
                continue;
            }
            try {
                auto rows = storage.scan(args[1]);
                for (const auto& values : rows) {
                    for (size_t i = 0; i < values.size(); ++i) {
                        if (i > 0) std::cout << ",";
                        std::cout << values[i];
                    }
                    std::cout << std::endl;
                }
            } catch (const std::exception& e) {
                std::cout << "Error: " << e.what() << std::endl;
            }
        }
        else if (command == "flush") {
            try {
                storage.flush();
                std::cout << "Storage flushed\n";
            } catch (const std::exception& e) {
                std::cout << "Error: " << e.what() << std::endl;
            }
        } else {
            std::cout << "Unknown command: " << command << "\nType 'help' for available commands\n";
        }
    }

    return 0;
}