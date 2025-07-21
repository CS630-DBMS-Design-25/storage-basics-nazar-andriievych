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

TEST_F(FileStorageLayerTest, UpdateAndDelete) {
    std::vector<ColumnSchema> schema = {
        {"id", ColumnType::INT, INT_SIZE},
        {"name", ColumnType::TEXT, 0}
    };
    storage.create("people", schema);
    uint32_t record_id = storage.insert("people", {"7", "Bob"});
    storage.update("people", record_id, {"7", "Alice"});
    auto got = storage.get("people", record_id);
    ASSERT_EQ(got[1], "Alice");
    storage.delete_record("people", record_id);
    EXPECT_THROW(storage.get("people", record_id), std::runtime_error);
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
