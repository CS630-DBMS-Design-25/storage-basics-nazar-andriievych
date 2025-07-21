#pragma once

#include <string>
#include <vector>
#include <functional>
#include <cstdint>
#include <optional>
#include <cstring>
#include <stdexcept>
#include <unordered_map>
#include <bitset>

constexpr uint32_t PAGE_SIZE = 8192;
constexpr uint32_t INVALID_PAGE_ID = UINT32_MAX;
constexpr uint32_t MAX_TABLES = 256;
constexpr uint32_t MAX_PAGE_ID = UINT32_MAX - 1;
constexpr uint32_t CATALOG_PAGE_ID = 0;
constexpr uint32_t MAX_TABLE_NAME_LEN = 63;
constexpr uint32_t IDS_PER_PAGE = 1024;
constexpr uint32_t INT_SIZE = 4;
constexpr uint32_t MAX_COLUMNS = 16;
constexpr char PAGE_FILE_PREFIX[] = "page_";
constexpr char PAGE_FILE_EXTENSION[] = ".dat";
constexpr uint32_t FIRST_ID_BLOCK = 1;
constexpr char VALUE_DELIMITER = ',';

// New: Supported column types
enum class ColumnType : uint8_t {
    INT = 0, // 4 bytes
    TEXT = 1 // variable size
};

// New: Column schema definition
struct ColumnSchema {
    char name[32];
    ColumnType type;
    uint32_t size; // Only used for INT (fixed size), ignored for TEXT
};

enum PageFlags : uint8_t {
    PAGE_CLEAN = 0x00,
    PAGE_DIRTY = 0x01,
    PAGE_OVERFLOW = 0x02
};

enum SlotFlags : uint8_t {
    SLOT_OCCUPIED = 0x01,
    SLOT_DELETED = 0x02
};

enum CatalogFlags : uint8_t {
	CATALOG_CLEAN = 0x00,
	CATALOG_DIRTY = 0x01
};

struct PageHeader {
    uint32_t page_id;
    uint16_t slot_count;
    uint16_t free_space;
    uint16_t free_space_offset;
    uint32_t next_page_id;
    uint8_t flags;
    uint32_t lsn;
    // New: ID range for this page
    uint32_t id_range_start;
    uint32_t id_range_end; // exclusive

    void initialize(uint32_t id) {
        page_id = id;
        slot_count = 0;
        free_space = PAGE_SIZE - sizeof(PageHeader);
        free_space_offset = sizeof(PageHeader);
        next_page_id = INVALID_PAGE_ID;
        flags = PAGE_CLEAN;
        lsn = 0;
        id_range_start = id;
        id_range_end = id + IDS_PER_PAGE;
    }
};

struct Slot {
    uint16_t offset;
    uint16_t length;
    uint8_t flags;
    uint32_t record_id;

    bool is_occupied() const { return flags & SLOT_OCCUPIED; }
    bool is_deleted() const { return flags & SLOT_DELETED; }
};

class Page {
public:
    Page() : Page(INVALID_PAGE_ID, 0) {}
    Page(uint32_t page_id);
    Page(uint32_t page_id, uint32_t id_range_start);

    std::optional<uint32_t> insert_record(uint32_t record_id, const std::vector<uint8_t>& data);
    std::optional<std::vector<uint8_t>> get_record(uint32_t record_id) const;
    bool update_record(uint32_t record_id, const std::vector<uint8_t>& new_data);
    bool delete_record(uint32_t record_id);

    bool is_dirty() const { return header_.flags & PAGE_DIRTY; }
    bool has_space(uint32_t required) const { return header_.free_space >= required; }

    uint32_t get_page_id() const { return header_.page_id; }
    uint32_t get_next_page_id() const { return header_.next_page_id; }
    std::vector<Slot>& get_slots() { return slots_; }
	void set_next_page_id(uint32_t next_page_id) { header_.next_page_id = next_page_id; }

    // Getters/setters for id range
    uint32_t get_id_range_start() const { return header_.id_range_start; }
    uint32_t get_id_range_end() const { return header_.id_range_end; }
    void set_id_range(uint32_t start, uint32_t end) { header_.id_range_start = start; header_.id_range_end = end; }
    // Getters/setters for free_id_bitmap
    std::bitset<IDS_PER_PAGE>& free_id_bitmap() { return free_id_bitmap_; }
    const std::bitset<IDS_PER_PAGE>& free_id_bitmap() const { return free_id_bitmap_; }

    std::vector<uint8_t> serialize();
    void deserialize(const std::vector<uint8_t>& data);

private:
    PageHeader header_;
    std::vector<Slot> slots_;
    std::vector<uint8_t> data_;
    // New: Bitmap for free IDs in this page
    std::bitset<IDS_PER_PAGE> free_id_bitmap_;

    void compact_page();
};

// Tuple header for variable-length fields
struct TupleHeader {
    uint16_t field_count;
    uint16_t offsets[16]; // Offset of each field in the tuple (for TEXT fields, points to start of data)
};

struct TableMetadata {
    char name[64];
    uint32_t first_data_page;
    uint32_t last_data_page;
    uint32_t record_count;
    // Removed: uint32_t next_record_id;
    uint32_t free_space_head;
    // New: Schema
    uint32_t column_count;
    ColumnSchema columns[16]; // Max 16 columns per table for simplicity
    // New: Next available id block for new pages
    uint32_t next_id_block;
};

struct CatalogHeader {
    uint32_t table_count;
    uint32_t free_page_id;
    uint32_t system_page_count;
    uint8_t flags;
    uint32_t lsn;
};

class CatalogPage {
public:
    CatalogPage();
    bool add_table(const std::string& table_name);
    std::optional<TableMetadata> get_table(const std::string& table_name) const;
    bool update_table(const TableMetadata& metadata);
    bool remove_table(const std::string& table_name);

    uint32_t get_table_count() const { return header_.table_count; }
    uint32_t get_lsn() const { return header_.lsn; }
	uint32_t get_free_page_id() const { return header_.free_page_id; }
	void increment_free_page_id() { header_.free_page_id++; }
	uint32_t get_system_page_count() const { return header_.system_page_count; }
	void set_system_page_count(uint32_t count) { header_.system_page_count = count; }
	void set_dirty() { catalog_dirty_ = true; }
    bool is_dirty() const { return catalog_dirty_; }
	void increment_lsn() { header_.lsn++; }
	void increment_system_page_count() { header_.system_page_count++; }

    std::vector<uint8_t> serialize() const;
    void deserialize(const std::vector<uint8_t>& data);

private:
    CatalogHeader header_;
    std::vector<TableMetadata> tables_;

    bool catalog_dirty_ = false;
};

/**
 * Abstract base class that defines the interface for a simple storage system.
 */
class StorageLayer {
public:
    virtual ~StorageLayer() = default;

    /**
     * Initialize or open existing storage at the given path.
     */
    virtual void open(const std::string& path) = 0;

    /**
     * Close storage safely and ensure all data is persisted.
     */
    virtual void close() = 0;

    /**
     * Create table with schema.
     * @param table Table name
     * @param schema Vector of ColumnSchema (name, type, size)
     */
    virtual void create(const std::string& table, const std::vector<ColumnSchema>& schema) = 0;

    /**
     * Insert a new record into the specified table, returning a unique record ID.
     * @param table Table name
     * @param values Vector of string values (must match schema)
     */
    virtual uint32_t insert(const std::string& table, const std::vector<std::string>& values) = 0;

    /**
     * Retrieve a record by its unique ID from the specified table.
     * @param table Table name
     * @param record_id Record ID
     * @return Vector of string values (decoded)
     */
    virtual std::vector<std::string> get(const std::string& table, uint32_t record_id) = 0;

    /**
     * Update an existing record identified by record ID.
     * @param table Table name
     * @param record_id Record ID
     * @param values Vector of string values (must match schema)
     */
    virtual void update(const std::string& table, uint32_t record_id, const std::vector<std::string>& values) = 0;

    /**
     * Delete a record identified by its unique ID.
     */
    virtual void delete_record(const std::string& table, uint32_t record_id) = 0;

    /**
     * Scan records in a table optionally using projection and filter. Callback is optional.
     * @param table Table name
     * @return Vector of rows, each row is a vector of string values (decoded)
     */
    virtual std::vector<std::vector<std::string>> scan(
        const std::string& table,
        const std::optional<std::function<bool(int, const std::vector<std::string>&)>>& callback = std::nullopt,
        const std::optional<std::vector<int>>& projection = std::nullopt,
        const std::optional<std::function<bool(const std::vector<std::string>&)>>& filter_func = std::nullopt) = 0;

    /**
     * Persist all buffered data immediately to disk.
     */
    virtual void flush() = 0;
};

/**
 * Example implementation of the StorageLayer interface.
 * Students should fill in the method implementations.
 */
class FileStorageLayer : public StorageLayer {
public:
    FileStorageLayer();
    ~FileStorageLayer() override;

    void open(const std::string& path) override;
    void close() override;
    void create(const std::string& table, const std::vector<ColumnSchema>& schema) override;
    uint32_t  insert(const std::string& table, const std::vector<std::string>& values) override;
    std::vector<std::string> get(const std::string& table, uint32_t  record_id) override;
    void update(const std::string& table, uint32_t  record_id, const std::vector<std::string>& values) override;
    std::vector<std::vector<std::string>> scan(
        const std::string& table,
        const std::optional<std::function<bool(int, const std::vector<std::string>&)>>& callback = std::nullopt,
        const std::optional<std::vector<int>>& projection = std::nullopt,
        const std::optional<std::function<bool(const std::vector<std::string>&)>>& filter_func = std::nullopt) override;
    void flush() override;

    void delete_record(const std::string& table, uint32_t record_id) override;

private:
    bool is_open;
    std::string storage_path;

    CatalogPage catalog_;
    std::unordered_map<uint32_t, Page> page_cache_;
    std::unordered_map<std::string, TableMetadata> table_cache_;

    std::string get_page_path(uint32_t page_id) const;
    uint32_t allocate_new_page();
    void write_page_to_disk(Page& page);
    Page& get_or_load_page(uint32_t page_id);
    Page& get_or_create_page(uint32_t page_id);
    Page& get_or_create_page(uint32_t page_id, uint32_t id_range_start);

    TableMetadata& get_table_metadata(const std::string& table_name);
    Page& get_last_page_for_table(const std::string& table_name);
    Page& find_free_page_for_table(const std::string& table_name, uint32_t record_size);
}; 