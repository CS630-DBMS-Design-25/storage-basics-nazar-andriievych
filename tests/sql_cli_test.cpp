#include "gtest/gtest.h"
#include "storage_layer.h"
#include <filesystem>
#include <cstdio>
#include <vector>
#include <string>
#include <sstream>
#include <iostream>

namespace fs = std::filesystem;

class SqlCliTest : public ::testing::Test {
protected:
    std::string temp_dir;
    FileStorageLayer storage;

    void SetUp() override {
        temp_dir = (fs::temp_directory_path() / fs::path("sql_cli_test_dir")).string();
        fs::remove_all(temp_dir);
        fs::create_directory(temp_dir);
        storage.open(temp_dir);
    }

    void TearDown() override {
        storage.close();
        fs::remove_all(temp_dir);
    }
};

TEST_F(SqlCliTest, CreateTableAndInsertSelectStar) {
    std::vector<ColumnSchema> schema = {
        {"name", ColumnType::TEXT, 0},
        {"age", ColumnType::INT, INT_SIZE}
    };
    storage.create("pets", schema);
    storage.insert("pets", {"Dog", "5"});
    storage.insert("pets", {"Cat", "3"});
    auto rows = storage.scan("pets");
    ASSERT_EQ(rows.size(), 2u);
    EXPECT_EQ(rows[0][0], "Dog");
    EXPECT_EQ(rows[0][1], "5");
    EXPECT_EQ(rows[1][0], "Cat");
    EXPECT_EQ(rows[1][1], "3");
}

TEST_F(SqlCliTest, InsertValueCountMismatch) {
    std::vector<ColumnSchema> schema = {
        {"name", ColumnType::TEXT, 0},
        {"age", ColumnType::INT, INT_SIZE}
    };
    storage.create("pets", schema);
    // Only one value instead of two
    try {
        storage.insert("pets", {"Dog"});
        FAIL() << "Expected exception for value count mismatch";
    } catch (const std::exception&) {
        SUCCEED();
    }
}

TEST_F(SqlCliTest, DeleteAndSelect) {
    std::vector<ColumnSchema> schema = {
        {"name", ColumnType::TEXT, 0},
        {"age", ColumnType::INT, INT_SIZE}
    };
    storage.create("pets", schema);
    storage.insert("pets", {"Dog", "5"});
    storage.insert("pets", {"Cat", "3"});
    // Delete one
    storage.delete_record("pets", 1);
    auto rows = storage.scan("pets");
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(rows[0][0], "Cat");
}

TEST_F(SqlCliTest, SelectProjectionAndWhere) {
    std::vector<ColumnSchema> schema = {
        {"name", ColumnType::TEXT, 0},
        {"age", ColumnType::INT, INT_SIZE}
    };
    storage.create("pets", schema);
    storage.insert("pets", {"Dog", "5"});
    storage.insert("pets", {"Cat", "3"});
    // Projection: only name, filter: age > 3
    std::vector<int> proj = {0};
    auto filter = [](const std::vector<std::string>& row) { return std::stoi(row[1]) > 3; };
    auto rows = storage.scan("pets", proj, filter);
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(rows[0][0], "Dog");
}

// Add more tests for error handling, SELECT *, and edge cases as needed. 