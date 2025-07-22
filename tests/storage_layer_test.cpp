#include "gtest/gtest.h"
#include "storage_layer.h"
#include <filesystem>
#include <cstdio>

namespace fs = std::filesystem;

class FileStorageLayerTest : public ::testing::Test {
protected:
    std::string temp_dir;
    FileStorageLayer storage;

    void SetUp() override {
        temp_dir = (fs::temp_directory_path() / fs::path("storage_test_dir")).string();
        fs::remove_all(temp_dir);
        fs::create_directory(temp_dir);
        storage.open(temp_dir);
    }

    void TearDown() override {
        storage.close();
        fs::remove_all(temp_dir);
    }
};

TEST_F(FileStorageLayerTest, CreateTableAndInsertGetInt) {
    std::vector<ColumnSchema> schema = {
        {"id", ColumnType::INT, INT_SIZE},
        {"age", ColumnType::INT, INT_SIZE}
    };
    storage.create("users", schema);
    std::vector<std::string> values = {"1", "42"};
    uint32_t record_id = storage.insert("users", values);
    auto got = storage.get("users", record_id);
    ASSERT_EQ(got.size(), 2u);
    EXPECT_EQ(got[0], "1");
    EXPECT_EQ(got[1], "42");
}

TEST_F(FileStorageLayerTest, InsertGetText) {
    std::vector<ColumnSchema> schema = {
        {"name", ColumnType::TEXT, 0},
        {"desc", ColumnType::TEXT, 0}
    };
    storage.create("things", schema);
    std::vector<std::string> values = {"apple", "fruit"};
    uint32_t record_id = storage.insert("things", values);
    auto got = storage.get("things", record_id);
    ASSERT_EQ(got.size(), 2u);
    EXPECT_EQ(got[0], "apple");
    EXPECT_EQ(got[1], "fruit");
}

TEST_F(FileStorageLayerTest, ScanTable) {
    std::vector<ColumnSchema> schema = {
        {"id", ColumnType::INT, INT_SIZE},
        {"name", ColumnType::TEXT, 0}
    };
    storage.create("scan_test", schema);
    storage.insert("scan_test", {"1", "A"});
    storage.insert("scan_test", {"2", "B"});
    storage.insert("scan_test", {"3", "C"});
    auto rows = storage.scan("scan_test");
    ASSERT_EQ(rows.size(), 3u);
    EXPECT_EQ(rows[0][1], "A");
    EXPECT_EQ(rows[1][1], "B");
    EXPECT_EQ(rows[2][1], "C");
}

TEST_F(FileStorageLayerTest, OpenClosePersistence) {
    std::vector<ColumnSchema> schema = {
        {"id", ColumnType::INT, INT_SIZE},
        {"name", ColumnType::TEXT, 0}
    };
    storage.create("persist", schema);
    uint32_t record_id = storage.insert("persist", {"99", "Zed"});
    storage.close();
    storage.open(temp_dir);
    auto got = storage.get("persist", record_id);
    ASSERT_EQ(got[0], "99");
    ASSERT_EQ(got[1], "Zed");
}

TEST_F(FileStorageLayerTest, ScanProjectionAndWhere) {
    std::vector<ColumnSchema> schema = {
        {"id", ColumnType::INT, INT_SIZE},
        {"age", ColumnType::INT, INT_SIZE},
        {"name", ColumnType::TEXT, 0}
    };
    storage.create("projwhere", schema);
    storage.insert("projwhere", {"1", "20", "Alice"});
    storage.insert("projwhere", {"2", "30", "Bob"});
    storage.insert("projwhere", {"3", "40", "Carol"});
    // Projection: only age and name
    std::vector<int> proj = {1, 2};
    // Filter: age >= 30
    auto filter = [](const std::vector<std::string>& row) {
        return std::stoi(row[1]) >= 30;
    };
    auto rows = storage.scan("projwhere", proj, filter);
    ASSERT_EQ(rows.size(), 2u);
    EXPECT_EQ(rows[0][1], "Bob");
    EXPECT_EQ(rows[1][1], "Carol");
}

TEST_F(FileStorageLayerTest, ScanOrderByAndLimit) {
    std::vector<ColumnSchema> schema = {
        {"id", ColumnType::INT, INT_SIZE},
        {"score", ColumnType::INT, INT_SIZE},
        {"name", ColumnType::TEXT, 0}
    };
    storage.create("orderlim", schema);
    storage.insert("orderlim", {"1", "50", "X"});
    storage.insert("orderlim", {"2", "70", "Y"});
    storage.insert("orderlim", {"3", "60", "Z"});
    // Order by score descending, limit 2
    std::vector<std::pair<int, bool>> order = { {1, false} };
    size_t lim = 2;
    auto rows = storage.scan("orderlim", std::nullopt, std::nullopt, order, lim);
    ASSERT_EQ(rows.size(), 2u);
    EXPECT_EQ(rows[0][2], "Y");
    EXPECT_EQ(rows[1][2], "Z");
}

TEST_F(FileStorageLayerTest, ScanSumAggregate) {
    std::vector<ColumnSchema> schema = {
        {"id", ColumnType::INT, INT_SIZE},
        {"val", ColumnType::INT, INT_SIZE}
    };
    storage.create("sumagg", schema);
    storage.insert("sumagg", {"1", "10"});
    storage.insert("sumagg", {"2", "20"});
    storage.insert("sumagg", {"3", "-5"});
    auto rows = storage.scan("sumagg", std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::make_pair(std::string("SUM"), 1));
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(rows[0][0], "25");
}

TEST_F(FileStorageLayerTest, ScanAbsAggregate) {
    std::vector<ColumnSchema> schema = {
        {"id", ColumnType::INT, INT_SIZE},
        {"val", ColumnType::INT, INT_SIZE}
    };
    storage.create("absagg", schema);
    storage.insert("absagg", {"1", "-7"});
    storage.insert("absagg", {"2", "3"});
    auto rows = storage.scan("absagg", std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::make_pair(std::string("ABS"), 1));
    ASSERT_EQ(rows.size(), 2u);
    EXPECT_EQ(rows[0][1], "7");
    EXPECT_EQ(rows[1][1], "3");
}
