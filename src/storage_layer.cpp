#include "storage_layer.h"
#include <algorithm>
#include <cstring>
#include <chrono>
#include <fstream>
#include <filesystem>
#include <stdexcept>
#include <sstream>

namespace fs = std::filesystem;

Page::Page(uint32_t page_id) : Page(page_id, 0) {}
Page::Page(uint32_t page_id, uint32_t id_range_start) {
    header_.initialize(page_id);
    header_.id_range_start = id_range_start;
    header_.id_range_end = id_range_start + IDS_PER_PAGE;
    data_.resize(PAGE_SIZE - sizeof(PageHeader));
    free_id_bitmap_.reset();
}

std::optional<uint32_t> Page::insert_record(uint32_t record_id, const std::vector<uint8_t>& data) {
    const uint32_t required_space = sizeof(Slot) + data.size();

    if (!has_space(required_space)) {
        compact_page();
        if (!has_space(required_space)) {
            return std::nullopt;
        }
    }

    Slot new_slot;
    new_slot.offset = header_.free_space_offset;
    new_slot.length = data.size();
    new_slot.flags = SLOT_OCCUPIED;
    new_slot.record_id = record_id;

    std::memcpy(data_.data() + new_slot.offset, data.data(), data.size());

    slots_.push_back(new_slot);

    header_.free_space -= required_space;
    header_.free_space_offset += data.size();
    header_.slot_count++;
    header_.flags |= PAGE_DIRTY;

    return record_id;
}

std::optional<std::vector<uint8_t>> Page::get_record(uint32_t record_id) const {
    for (const auto& slot : slots_) {
        if (slot.record_id == record_id && slot.is_occupied()) {
            return std::vector<uint8_t>(
                data_.begin() + slot.offset,
                data_.begin() + slot.offset + slot.length
            );
        }
    }
    return std::nullopt;
}

bool Page::update_record(uint32_t record_id, const std::vector<uint8_t>& new_data) {
    auto slot_it = std::find_if(slots_.begin(), slots_.end(),
        [record_id](const Slot& s) {
            return s.record_id == record_id && s.is_occupied();
        });

    if (slot_it == slots_.end()) {
        return false;
    }

    const uint32_t space_needed = new_data.size();
    const uint32_t space_available = header_.free_space + slot_it->length;

    if (space_needed <= slot_it->length) {
        std::memcpy(data_.data() + slot_it->offset, new_data.data(), new_data.size());
        if (space_needed < slot_it->length) {
            header_.free_space += (slot_it->length - space_needed);
            slot_it->length = new_data.size();
        }
        header_.flags |= PAGE_DIRTY;
        return true;
    }
    else if (space_needed <= space_available) {
        compact_page();
        if (space_needed <= header_.free_space) {
            slot_it->offset = header_.free_space_offset;
            std::memcpy(data_.data() + slot_it->offset, new_data.data(), new_data.size());
            slot_it->length = new_data.size();
            header_.free_space -= new_data.size();
            header_.free_space_offset += new_data.size();
            header_.flags |= PAGE_DIRTY;
            return true;
        }
    }
    return false;
}

bool Page::delete_record(uint32_t record_id) {
    for (auto& slot : slots_) {
        if (slot.record_id == record_id && slot.is_occupied()) {
            slot.flags = SLOT_DELETED;
            header_.flags |= PAGE_DIRTY;
            return true;
        }
    }
    return false;
}

void Page::compact_page() {
    std::vector<uint8_t> new_data(PAGE_SIZE - sizeof(PageHeader));
    uint16_t current_offset = sizeof(PageHeader);

    for (auto& slot : slots_) {
        if (slot.is_occupied()) {
            std::memcpy(
                new_data.data() + current_offset,
                data_.data() + slot.offset,
                slot.length
            );
            slot.offset = current_offset;
            current_offset += slot.length;
        }
    }

    data_ = std::move(new_data);
    header_.free_space_offset = current_offset;
    header_.free_space = PAGE_SIZE - sizeof(PageHeader) - current_offset;
    header_.flags |= PAGE_DIRTY;
}

std::vector<uint8_t> Page::serialize() {
    std::vector<uint8_t> buffer(PAGE_SIZE);
    compact_page();
    std::memcpy(buffer.data(), &header_, sizeof(PageHeader));
    uint32_t slots_offset = sizeof(PageHeader);
    size_t slots_bytes = slots_.size() * sizeof(Slot);
    if (slots_offset + slots_bytes > buffer.size()) throw std::runtime_error("Serialize error: slot data out of bounds");
    std::memcpy(
        buffer.data() + slots_offset,
        slots_.data(),
        slots_bytes
    );
    size_t data_offset = sizeof(PageHeader) + slots_bytes;
    size_t data_length = header_.free_space_offset - data_offset;
    if (data_offset + data_length > buffer.size()) throw std::runtime_error("Serialize error: data out of bounds");
    std::memcpy(
        buffer.data() + data_offset,
        data_.data(),
        data_length
    );
    size_t bitmap_offset = PAGE_SIZE - sizeof(free_id_bitmap_);
    if (bitmap_offset + sizeof(free_id_bitmap_) > buffer.size()) throw std::runtime_error("Serialize error: free_id_bitmap out of bounds");
    std::memcpy(buffer.data() + bitmap_offset, &free_id_bitmap_, sizeof(free_id_bitmap_));
    return buffer;
}

void Page::deserialize(const std::vector<uint8_t>& data) {
    if (data.size() < sizeof(PageHeader)) throw std::runtime_error("Corrupt page: too small");
    std::memcpy(&header_, data.data(), sizeof(PageHeader));

    if (header_.slot_count > IDS_PER_PAGE) throw std::runtime_error("Corrupt page: too many slots");
    slots_.resize(header_.slot_count);

    uint32_t slots_offset = sizeof(PageHeader);
    size_t slots_bytes = header_.slot_count * sizeof(Slot);
    if (data.size() < slots_offset + slots_bytes) throw std::runtime_error("Corrupt page: slot data out of bounds");
    std::memcpy(
        slots_.data(),
        data.data() + slots_offset,
        slots_bytes
    );

    data_.resize(PAGE_SIZE - sizeof(PageHeader));
    if (header_.free_space_offset > data_.size()) throw std::runtime_error("Corrupt page: free_space_offset out of bounds");
    if (data.size() < header_.free_space_offset + data_.size()) throw std::runtime_error("Corrupt page: data out of bounds");
    std::memcpy(
        data_.data(),
        data.data() + header_.free_space_offset,
        data_.size()
    );

    size_t bitmap_offset = PAGE_SIZE - sizeof(free_id_bitmap_);
    if (data.size() < bitmap_offset + sizeof(free_id_bitmap_)) throw std::runtime_error("Corrupt page: free_id_bitmap out of bounds");
    std::memcpy(&free_id_bitmap_, data.data() + bitmap_offset, sizeof(free_id_bitmap_));
}

static TableMetadata make_table_metadata(const std::string& table_name, const std::vector<ColumnSchema>& schema) {
    TableMetadata new_table{};
    std::memset(&new_table, 0, sizeof(TableMetadata));
    size_t copy_len = std::min(table_name.size(), static_cast<size_t>(MAX_TABLE_NAME_LEN));
    table_name.copy(new_table.name, copy_len);
    new_table.name[copy_len] = '\0';
    new_table.first_data_page = INVALID_PAGE_ID;
    new_table.last_data_page = INVALID_PAGE_ID;
    new_table.record_count = 0;
    new_table.free_space_head = INVALID_PAGE_ID;
    new_table.column_count = schema.size();
    for (size_t i = 0; i < schema.size() && i < MAX_COLUMNS; ++i) {
        new_table.columns[i] = schema[i];
    }
    new_table.next_id_block = 0;
    return new_table;
}

CatalogPage::CatalogPage() {
    header_.table_count = 0;
    header_.free_page_id = INVALID_PAGE_ID;
    header_.system_page_count = 1;
    header_.flags = CATALOG_CLEAN;
    header_.lsn = 0;
}

bool CatalogPage::add_table(const std::string& table_name) {
    if (header_.table_count >= MAX_TABLES) {
        return false;
    }

    if (get_table(table_name).has_value()) {
        return false;
    }

    TableMetadata new_table = make_table_metadata(table_name, {});
    new_table.first_data_page = INVALID_PAGE_ID;
    new_table.last_data_page = INVALID_PAGE_ID;
    new_table.record_count = 0;
    new_table.free_space_head = INVALID_PAGE_ID;

    tables_.push_back(new_table);
    header_.table_count++;
    catalog_dirty_ = true;
    header_.flags |= CATALOG_DIRTY;
    header_.lsn++;

    return true;
}

std::optional<TableMetadata> CatalogPage::get_table(const std::string& name) const {
    auto it = std::find_if(tables_.begin(), tables_.end(),
        [&name](const TableMetadata& tm) {
            return strncmp(tm.name, name.c_str(), MAX_TABLE_NAME_LEN) == 0;
        });

    if (it != tables_.end()) {
        return *it;
    }
    return std::nullopt;
}

bool CatalogPage::update_table(const TableMetadata& metadata) {
    auto it = std::find_if(tables_.begin(), tables_.end(),
        [&metadata](const TableMetadata& tm) {
            return strncmp(tm.name, metadata.name, MAX_TABLE_NAME_LEN) == 0;
        });

    if (it != tables_.end()) {
        *it = metadata;
        catalog_dirty_ = true;
        header_.flags |= CATALOG_DIRTY;
        header_.lsn++;
        return true;
    }
    return false;
}

bool CatalogPage::remove_table(const std::string& table_name) {
    auto it = std::remove_if(tables_.begin(), tables_.end(),
        [&table_name](const TableMetadata& tm) {
            return strncmp(tm.name, table_name.c_str(), MAX_TABLE_NAME_LEN) == 0;
        });

    if (it != tables_.end()) {
        tables_.erase(it, tables_.end());
        header_.table_count--;
        catalog_dirty_ = true;
        header_.flags |= CATALOG_DIRTY;
        header_.lsn++;
        return true;
    }
    return false;
}

std::vector<uint8_t> CatalogPage::serialize() const {
    std::vector<uint8_t> buffer(PAGE_SIZE, 0);
    if (sizeof(CatalogHeader) > buffer.size()) throw std::runtime_error("Catalog serialize: header out of bounds");
    memcpy(buffer.data(), &header_, sizeof(CatalogHeader));
    size_t offset = sizeof(CatalogHeader);
    for (const auto& table : tables_) {
        if (offset + sizeof(TableMetadata) > buffer.size()) throw std::runtime_error("Catalog serialize: table out of bounds");
        memcpy(buffer.data() + offset, &table, sizeof(TableMetadata));
        offset += sizeof(TableMetadata);
    }
    CatalogHeader* serialized_header = reinterpret_cast<CatalogHeader*>(buffer.data());
    serialized_header->flags = CATALOG_CLEAN;
    return buffer;
}

void CatalogPage::deserialize(const std::vector<uint8_t>& data) {
    if (data.size() < sizeof(CatalogHeader)) throw std::runtime_error("Corrupt catalog: too small");
    memcpy(&header_, data.data(), sizeof(CatalogHeader));
    if (header_.table_count > MAX_TABLES) throw std::runtime_error("Corrupt catalog: too many tables");
    tables_.resize(header_.table_count);
    size_t offset = sizeof(CatalogHeader);
    for (size_t i = 0; i < header_.table_count; i++) {
        if (offset + sizeof(TableMetadata) > data.size()) throw std::runtime_error("Corrupt catalog: table out of bounds");
        memcpy(&tables_[i], data.data() + offset, sizeof(TableMetadata));
        offset += sizeof(TableMetadata);
    }
    catalog_dirty_ = false;
}

FileStorageLayer::FileStorageLayer() : is_open(false) {}

FileStorageLayer::~FileStorageLayer() {
    if (is_open) {
        close();
    }
}

void FileStorageLayer::open(const std::string& path) {
    storage_path = path;

    if (!fs::exists(storage_path)) {
        fs::create_directory(storage_path);
    }

    std::string catalog_path = get_page_path(CATALOG_PAGE_ID);
    if (fs::exists(catalog_path)) {
        std::ifstream in(catalog_path, std::ios::binary);
        std::vector<uint8_t> data(PAGE_SIZE);
        in.read(reinterpret_cast<char*>(data.data()), PAGE_SIZE);
        catalog_.deserialize(data);
    }
    else {
        catalog_ = CatalogPage();
    }

    is_open = true;
}

void FileStorageLayer::close() {
    if (!is_open) return;

    flush();
    is_open = false;
}

static std::vector<uint8_t> serialize_row(const std::vector<ColumnSchema>& schema, const std::vector<std::string>& values) {
    TupleHeader header{};
    header.field_count = schema.size();
    std::vector<uint8_t> data;
    size_t offset = sizeof(TupleHeader);
    data.resize(sizeof(TupleHeader), 0);
    for (size_t i = 0; i < schema.size(); ++i) {
        header.offsets[i] = offset;
        const auto& col = schema[i];
        const auto& val = values[i];
        if (col.type == ColumnType::INT) {
            int32_t intval = std::stoi(val);
            uint8_t buf[INT_SIZE];
            std::memcpy(buf, &intval, INT_SIZE);
            data.insert(data.end(), buf, buf + INT_SIZE);
            offset += INT_SIZE;
        } else if (col.type == ColumnType::TEXT) {
            uint32_t len = val.size();
            data.insert(data.end(), reinterpret_cast<uint8_t*>(&len), reinterpret_cast<uint8_t*>(&len) + INT_SIZE);
            data.insert(data.end(), val.begin(), val.end());
            offset += INT_SIZE + len;
        }
    }
    std::memcpy(data.data(), &header, sizeof(TupleHeader));
    return data;
}

static std::vector<std::string> deserialize_row(const std::vector<ColumnSchema>& schema, const std::vector<uint8_t>& data) {
    std::vector<std::string> values;
    if (data.size() < sizeof(TupleHeader)) return values;
    TupleHeader header;
    std::memcpy(&header, data.data(), sizeof(TupleHeader));
    for (size_t i = 0; i < schema.size(); ++i) {
        const auto& col = schema[i];
        size_t field_offset = header.offsets[i];
        if (col.type == ColumnType::INT) {
            int32_t intval;
            std::memcpy(&intval, data.data() + field_offset, INT_SIZE);
            values.push_back(std::to_string(intval));
        } else if (col.type == ColumnType::TEXT) {
            uint32_t len;
            std::memcpy(&len, data.data() + field_offset, INT_SIZE);
            std::string str(reinterpret_cast<const char*>(data.data() + field_offset + INT_SIZE), len);
            values.push_back(str);
        }
    }
    return values;
}

void FileStorageLayer::create(const std::string& table, const std::vector<ColumnSchema>& schema) {
    if (catalog_.get_table(table).has_value()) {
        throw std::runtime_error("Table already exists");
    }
    TableMetadata new_table = make_table_metadata(table, schema);
    catalog_.add_table(table);
    catalog_.update_table(new_table);
    table_cache_[table] = new_table;
    catalog_.set_dirty();
}

uint32_t FileStorageLayer::insert(const std::string& table, const std::vector<std::string>& values) {
    if (!is_open) throw std::runtime_error("Storage not open");
    TableMetadata& metadata = get_table_metadata(table);
    if (values.size() != metadata.column_count) throw std::runtime_error("Column count mismatch");
    std::vector<ColumnSchema> schema(metadata.columns, metadata.columns + metadata.column_count);
    std::vector<uint8_t> record = serialize_row(schema, values);
    uint32_t current_page_id = metadata.first_data_page;
    while (current_page_id != INVALID_PAGE_ID) {
        Page& page = get_or_load_page(current_page_id);
        for (uint32_t i = 0; i < IDS_PER_PAGE; ++i) {
            if (!page.free_id_bitmap().test(i)) {
                uint32_t record_id = page.get_id_range_start() + i;
                if (page.insert_record(record_id, record).has_value()) {
                    page.free_id_bitmap().set(i);
                    metadata.record_count++;
                    catalog_.update_table(metadata);
                    return record_id;
                }
            }
        }
        current_page_id = page.get_next_page_id();
    }
    uint32_t new_page_id = allocate_new_page();
    uint32_t id_range_start = (metadata.next_id_block == 0) ? 1 : (metadata.next_id_block * IDS_PER_PAGE + 1);
    Page& new_page = get_or_create_page(new_page_id, id_range_start);
    new_page.set_id_range(id_range_start, id_range_start + IDS_PER_PAGE);
    new_page.free_id_bitmap().reset();
    uint32_t record_id = id_range_start;
    if (!new_page.insert_record(record_id, record).has_value()) {
        throw std::runtime_error("Failed to insert record in new page");
    }
    new_page.free_id_bitmap().set(0);
    if (metadata.last_data_page == INVALID_PAGE_ID) {
        metadata.first_data_page = new_page_id;
    } else {
        Page& prev_last = get_or_load_page(metadata.last_data_page);
        prev_last.set_next_page_id(new_page_id);
        page_cache_[metadata.last_data_page] = prev_last;
    }
    metadata.last_data_page = new_page_id;
    metadata.record_count++;
    metadata.next_id_block++;
    catalog_.update_table(metadata);
    return record_id;
}

std::vector<std::string> FileStorageLayer::get(const std::string& table, uint32_t record_id) {
    if (!is_open) throw std::runtime_error("Storage not open");
    const TableMetadata& metadata = get_table_metadata(table);
    std::vector<ColumnSchema> schema(metadata.columns, metadata.columns + metadata.column_count);
    uint32_t current_page_id = metadata.first_data_page;
    while (current_page_id != INVALID_PAGE_ID) {
        Page& page = get_or_load_page(current_page_id);
        auto record = page.get_record(record_id);
        if (record.has_value()) {
            return deserialize_row(schema, record.value());
        }
        current_page_id = page.get_next_page_id();
    }
    throw std::runtime_error("Record not found");
}

void FileStorageLayer::update(const std::string& table, uint32_t record_id, const std::vector<std::string>& values) {
    if (!is_open) throw std::runtime_error("Storage not open");
    TableMetadata& metadata = get_table_metadata(table);
    if (values.size() != metadata.column_count) throw std::runtime_error("Column count mismatch");
    std::vector<ColumnSchema> schema(metadata.columns, metadata.columns + metadata.column_count);
    std::vector<uint8_t> updated_record = serialize_row(schema, values);
    uint32_t current_page_id = metadata.first_data_page;
    while (current_page_id != INVALID_PAGE_ID) {
        Page& page = get_or_load_page(current_page_id);
        if (page.update_record(record_id, updated_record)) {
            return;
        }
        current_page_id = page.get_next_page_id();
    }
    throw std::runtime_error("Record not found for update");
}

void FileStorageLayer::delete_record(const std::string& table, uint32_t record_id) {
    if (!is_open) throw std::runtime_error("Storage not open");
    TableMetadata& metadata = get_table_metadata(table);
    uint32_t current_page_id = metadata.first_data_page;
    while (current_page_id != INVALID_PAGE_ID) {
        Page& page = get_or_load_page(current_page_id);
        if (record_id >= page.get_id_range_start() && record_id < page.get_id_range_end()) {
            uint32_t idx = record_id - page.get_id_range_start();
            if (page.delete_record(record_id)) {
                if (idx < IDS_PER_PAGE) {
                    page.free_id_bitmap().reset(idx);
                }
                if (metadata.record_count > 0) {
                    metadata.record_count--;
                }
                catalog_.update_table(metadata);
                return;
            } else {
                throw std::runtime_error("Delete failed: record not found or already deleted");
            }
        }
        current_page_id = page.get_next_page_id();
    }
    throw std::runtime_error("Record not found for deletion");
}

void FileStorageLayer::flush() {
    if (!is_open) return;

    for (auto& [page_id, page] : page_cache_) {
        if (page.is_dirty()) {
            write_page_to_disk(page);
        }
    }

    if (catalog_.is_dirty()) {
        std::ofstream out(get_page_path(CATALOG_PAGE_ID), std::ios::binary);
        auto data = catalog_.serialize();
        out.write(reinterpret_cast<const char*>(data.data()), data.size());
    }
}

std::string FileStorageLayer::get_page_path(uint32_t page_id) const {
    return storage_path + "/" + PAGE_FILE_PREFIX + std::to_string(page_id) + PAGE_FILE_EXTENSION;
}

uint32_t FileStorageLayer::allocate_new_page() {
    if (catalog_.get_free_page_id() != INVALID_PAGE_ID) {
        uint32_t allocated_page = catalog_.get_free_page_id();

        catalog_.increment_free_page_id();

        if (allocated_page >= catalog_.get_system_page_count()) {
            catalog_.set_system_page_count(allocated_page + 1);
        }

        catalog_.set_dirty();
        catalog_.increment_lsn();
        return allocated_page;
    }

    catalog_.increment_system_page_count();
    uint32_t new_page_id = catalog_.get_system_page_count();
    catalog_.set_dirty();
    catalog_.increment_lsn();

    return new_page_id;
}

void FileStorageLayer::write_page_to_disk(Page& page) {
    std::ofstream out(get_page_path(page.get_page_id()), std::ios::binary);
    auto data = page.serialize();
    out.write(reinterpret_cast<const char*>(data.data()), data.size());
}

Page& FileStorageLayer::get_or_load_page(uint32_t page_id) {
    auto it = page_cache_.find(page_id);
    if (it != page_cache_.end()) {
        return it->second;
    }

    std::ifstream in(get_page_path(page_id), std::ios::binary);
    if (!in) throw std::runtime_error("Page not found");

    std::vector<uint8_t> data(PAGE_SIZE);
    in.read(reinterpret_cast<char*>(data.data()), PAGE_SIZE);

    Page new_page(page_id);
    new_page.deserialize(data);
    auto [insert_it, _] = page_cache_.emplace(page_id, std::move(new_page));
    return insert_it->second;
}

Page& FileStorageLayer::get_or_create_page(uint32_t page_id) {
    return get_or_create_page(page_id, 0);
}

Page& FileStorageLayer::get_or_create_page(uint32_t page_id, uint32_t id_range_start) {
    try {
        return get_or_load_page(page_id);
    }
    catch (...) {
        Page new_page(page_id, id_range_start);
        auto [it, _] = page_cache_.emplace(page_id, std::move(new_page));
        return it->second;
    }
}

TableMetadata& FileStorageLayer::get_table_metadata(const std::string& table_name) {
    auto cache_it = table_cache_.find(table_name);
    if (cache_it != table_cache_.end()) {
        return cache_it->second;
    }
    auto table_opt = catalog_.get_table(table_name);
    if (!table_opt.has_value()) {
        throw std::runtime_error("Table does not exist");
    }
    auto [it, _] = table_cache_.emplace(table_name, table_opt.value());
    return it->second;
}

Page& FileStorageLayer::get_last_page_for_table(const std::string& table_name) {
    TableMetadata& metadata = get_table_metadata(table_name);
    if (metadata.last_data_page == INVALID_PAGE_ID) {
        uint32_t new_page_id = allocate_new_page();
        Page& new_page = get_or_create_page(new_page_id);
        metadata.first_data_page = new_page_id;
        metadata.last_data_page = new_page_id;
        catalog_.update_table(metadata);
        return new_page;
    }
    return get_or_load_page(metadata.last_data_page);
}

Page& FileStorageLayer::find_free_page_for_table(const std::string& table_name, uint32_t record_size) {
    TableMetadata& metadata = get_table_metadata(table_name);
    uint32_t current_page_id = metadata.first_data_page;

    while (current_page_id != INVALID_PAGE_ID) {
        Page& page = get_or_load_page(current_page_id);
        if (page.has_space(record_size + sizeof(Slot))) {
            return page;
        }
        current_page_id = page.get_next_page_id();
    }

    throw std::runtime_error("No free page found");
}

std::vector<std::vector<std::string>> FileStorageLayer::scan(
    const std::string& table,
    const std::optional<std::vector<int>>& projection,
    const std::optional<std::function<bool(const std::vector<std::string>&)>>& filter_func,
    const std::optional<std::vector<std::pair<int, bool>>>& order_by,
    const std::optional<size_t>& limit,
    const std::optional<std::pair<std::string, int>>& aggregate)
{
    if (!is_open) {
        throw std::runtime_error("Storage not open");
    }
    std::vector<std::vector<std::string>> results;
    const TableMetadata& metadata = get_table_metadata(table);
    std::vector<ColumnSchema> schema(metadata.columns, metadata.columns + metadata.column_count);
    uint32_t current_page_id = metadata.first_data_page;
    while (current_page_id != INVALID_PAGE_ID) {
        Page& page = get_or_load_page(current_page_id);
        for (const auto& slot : page.get_slots()) {
            if (slot.is_occupied()) {
                auto record_data = page.get_record(slot.record_id);
                if (record_data.has_value()) {
                    auto row = deserialize_row(schema, record_data.value());
                    if (filter_func && !(*filter_func)(row)) {
                        continue;
                    }
                    if (projection) {
                        std::vector<std::string> projected_row;
                        for (int idx : *projection) {
                            if (idx >= 0 && static_cast<size_t>(idx) < row.size()) {
                                projected_row.push_back(row[idx]);
                            }
                        }
                        results.push_back(std::move(projected_row));
                    } else {
                        results.push_back(std::move(row));
                    }
                }
            }
        }
        current_page_id = page.get_next_page_id();
    }
    if (order_by && !order_by->empty()) {
        std::sort(results.begin(), results.end(), [&](const std::vector<std::string>& a, const std::vector<std::string>& b) {
            for (const auto& [col, asc] : *order_by) {
                if (col < 0 || static_cast<size_t>(col) >= a.size() || static_cast<size_t>(col) >= b.size()) continue;
                try {
                    int ai = std::stoi(a[col]);
                    int bi = std::stoi(b[col]);
                    if (ai != bi) return asc ? ai < bi : ai > bi;
                } catch (...) {
                    if (a[col] != b[col]) return asc ? a[col] < b[col] : a[col] > b[col];
                }
            }
            return false;
        });
    }
    if (limit && results.size() > *limit) {
        results.resize(*limit);
    }
    if (aggregate) {
        const std::string& op = aggregate->first;
        int col = aggregate->second;
        if (col < 0 || (results.empty() || static_cast<size_t>(col) >= results[0].size())) {
            throw std::runtime_error("Invalid column index for aggregation");
        }
        if (op == "SUM") {
            int64_t sum = 0;
            for (const auto& row : results) {
                try {
                    sum += std::stoll(row[col]);
                } catch (...) {}
            }
            return { { std::to_string(sum) } };
        } else if (op == "ABS") {
            std::vector<std::vector<std::string>> abs_results;
            for (const auto& row : results) {
                std::vector<std::string> abs_row = row;
                try {
                    int val = std::stoi(row[col]);
                    abs_row[col] = std::to_string(std::abs(val));
                } catch (...) {}
                abs_results.push_back(std::move(abs_row));
            }
            return abs_results;
        } else {
            throw std::runtime_error("Unsupported aggregate operation");
        }
    }
    return results;
}

std::vector<std::string> FileStorageLayer::get_column_names(const std::string& table) {
    const TableMetadata& meta = get_table_metadata(table);
    std::vector<std::string> names;
    for (size_t i = 0; i < meta.column_count; ++i) {
        names.push_back(meta.columns[i].name);
    }
    return names;
}