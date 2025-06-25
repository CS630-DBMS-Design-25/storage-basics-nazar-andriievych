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
        } else if (command == "insert") {
            if (args.size() < 3) {
                std::cout << "Error: Missing arguments. Usage: insert <table> <record>\n";
                continue;
            }
            try {
                int record_id = storage.insert(args[1], string_to_bytes(args[2]));
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
                auto record = storage.get(args[1], record_id);
                std::cout << "Retrieved record: " << bytes_to_string(record) << std::endl;
            } catch (const std::exception& e) {
                std::cout << "Error: " << e.what() << std::endl;
            }
        } else if (command == "update") {
            if (args.size() < 4) {
                std::cout << "Error: Missing arguments. Usage: update <table> <record_id> <record>\n";
                continue;
            }
            try {
                int record_id = std::stoi(args[2]);
                storage.update(args[1], record_id, string_to_bytes(args[3]));
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