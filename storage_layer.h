#pragma once

#include <string>
#include <vector>
#include <functional>
#include <cstdint>
#include <optional>
#include <cstring>
#include <stdexcept>
#include <unordered_map>

constexpr uint32_t PAGE_SIZE = 8192;
constexpr uint32_t INVALID_PAGE_ID = UINT32_MAX;
constexpr uint32_t MAX_TABLES = 256;
constexpr uint32_t MAX_PAGE_ID = UINT32_MAX - 1;
constexpr uint32_t CATALOG_PAGE_ID = 0;
constexpr uint32_t MAX_TABLE_NAME_LEN = 63;

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

    void initialize(uint32_t id) {
        page_id = id;
        slot_count = 0;
        free_space = PAGE_SIZE - sizeof(PageHeader);
        free_space_offset = sizeof(PageHeader);
        next_page_id = INVALID_PAGE_ID;
        flags = PAGE_CLEAN;
        lsn = 0;
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
    Page() : Page(INVALID_PAGE_ID) {}
    Page(uint32_t page_id);

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

    std::vector<uint8_t> serialize();
    void deserialize(const std::vector<uint8_t>& data);

private:
    PageHeader header_;
    std::vector<Slot> slots_;
    std::vector<uint8_t> data_;

    void compact_page();
};

struct TableMetadata {
    char name[64];
    uint32_t first_data_page;
    uint32_t last_data_page;
    uint32_t record_count;
    uint32_t next_record_id;
    uint32_t free_space_head;
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
     * Insert a new record into the specified table, returning a unique record ID.
     */
    virtual int insert(const std::string& table, const std::vector<uint8_t>& record) = 0;

    /**
     * Retrieve a record by its unique ID from the specified table.
     */
    virtual std::vector<uint8_t> get(const std::string& table, int record_id) = 0;

    /**
     * Update an existing record identified by record ID.
     */
    virtual void update(const std::string& table, int record_id, const std::vector<uint8_t>& updated_record) = 0;

    /**
     * Delete a record identified by its unique ID.
     */
    virtual void delete_record(const std::string& table, int record_id) = 0;

    /**
     * Scan records in a table optionally using projection and filter. Callback is optional.
     */
    virtual std::vector<std::vector<uint8_t>> scan(
        const std::string& table,
        const std::optional<std::function<bool(int, const std::vector<uint8_t>&)>>& callback = std::nullopt,
        const std::optional<std::vector<int>>& projection = std::nullopt,
        const std::optional<std::function<bool(const std::vector<uint8_t>&)>>& filter_func = std::nullopt) = 0;

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
    int insert(const std::string& table, const std::vector<uint8_t>& record) override;
    std::vector<uint8_t> get(const std::string& table, int record_id) override;
    void update(const std::string& table, int record_id, const std::vector<uint8_t>& updated_record) override;
    void delete_record(const std::string& table, int record_id) override;
    std::vector<std::vector<uint8_t>> scan(
        const std::string& table,
        const std::optional<std::function<bool(int, const std::vector<uint8_t>&)>>& callback = std::nullopt,
        const std::optional<std::vector<int>>& projection = std::nullopt,
        const std::optional<std::function<bool(const std::vector<uint8_t>&)>>& filter_func = std::nullopt) override;
    void flush() override;

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

    TableMetadata& get_table_metadata(const std::string& table_name);
    Page& get_last_page_for_table(const std::string& table_name);
    Page& find_free_page_for_table(const std::string& table_name, uint32_t record_size);
}; 