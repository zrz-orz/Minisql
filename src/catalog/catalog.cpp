#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
    ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
    MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
    buf += 4;
    MACH_WRITE_UINT32(buf, table_meta_pages_.size());
    buf += 4;
    MACH_WRITE_UINT32(buf, index_meta_pages_.size());
    buf += 4;
    for (auto map_iterator_ : table_meta_pages_) {
        MACH_WRITE_TO(table_id_t, buf, map_iterator_.first);
        buf += 4;
        MACH_WRITE_TO(page_id_t, buf, map_iterator_.second);
        buf += 4;
    }
    for (auto map_iterator_ : index_meta_pages_) {
        MACH_WRITE_TO(index_id_t, buf, map_iterator_.first);
        buf += 4;
        MACH_WRITE_TO(page_id_t, buf, map_iterator_.second);
        buf += 4;
    }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf) {
    // check valid
    uint32_t magic_num = MACH_READ_UINT32(buf);
    buf += 4;
    ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
    // get table and index nums
    uint32_t table_nums = MACH_READ_UINT32(buf);
    buf += 4;
    uint32_t index_nums = MACH_READ_UINT32(buf);
    buf += 4;
    // create metadata and read value
    CatalogMeta *meta = new CatalogMeta();
    for (uint32_t i = 0; i < table_nums; i++) {
        auto table_id = MACH_READ_FROM(table_id_t, buf);
        buf += 4;
        auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
        buf += 4;
        meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
    }
    for (uint32_t i = 0; i < index_nums; i++) {
        auto index_id = MACH_READ_FROM(index_id_t, buf);
        buf += 4;
        auto index_page_id = MACH_READ_FROM(page_id_t, buf);
        buf += 4;
        meta->index_meta_pages_.emplace(index_id, index_page_id);
    }
    return meta;
}

/**
 * TODO: zrz
 * Done
 */
uint32_t CatalogMeta::GetSerializedSize() const {
  uint32_t res = 0;
  res += sizeof(CATALOG_METADATA_MAGIC_NUM);
  res += sizeof(uint32_t);                                                     
  res += sizeof(uint32_t);                                    
  res += table_meta_pages_.size() * (sizeof(table_id_t) + sizeof(page_id_t));
  res += index_meta_pages_.size() * (sizeof(index_id_t) + sizeof(page_id_t));
  return res;
}

CatalogMeta::CatalogMeta() {}

/**
 * TODO: zrz
 * Done
 */
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {
    ASSERT(false, "Not Implemented yet");
  if (init) {
    catalog_meta_ = new CatalogMeta();
    next_index_id_ = 0;
    next_table_id_ = 0;
    table_names_ = std::unordered_map<std::string, table_id_t>();
    table_names_.clear();
    tables_ = std::unordered_map<table_id_t, TableInfo *>();
    tables_.clear();
    index_names_ = std::unordered_map<std::string, std::unordered_map<std::string, index_id_t>>();
    index_names_.clear();
    indexes_ = std::unordered_map<index_id_t, IndexInfo *>();
    indexes_.clear();
  } else {
    Page *page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
    if (page == nullptr) {
      LOG(ERROR) << "Failed to fetch catalog metadata page.";
      return;
    }
    catalog_meta_ = CatalogMeta::DeserializeFrom(page->GetData());
    next_index_id_ = 0;
    next_table_id_ = 0;
    std::map<table_id_t, page_id_t> table_meta_pages = *catalog_meta_->GetTableMetaPages();
    std::map<index_id_t, page_id_t> index_meta_pages = *catalog_meta_->GetIndexMetaPages();
    for (auto map_iterator_ : table_meta_pages) {
      LoadTable(map_iterator_.first, map_iterator_.second);
    }
    for (auto map_iterator_ : index_meta_pages) {
      LoadIndex(map_iterator_.first, map_iterator_.second);
    }
  }
}

CatalogManager::~CatalogManager() {
  FlushCatalogMetaPage();
  delete catalog_meta_;
  for (auto map_iterator_ : tables_) {
    delete map_iterator_.second;
  }
  for (auto map_iterator_ : indexes_) {
    delete map_iterator_.second;
  }
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema,
                                    Transaction *txn, TableInfo *&table_info) {
  if (table_names_.count(table_name) > 0) {
    LOG(ERROR) << "Table " << table_name << " already exists";
    return DB_TABLE_ALREADY_EXIST;
  }

  table_id_t table_id = next_table_id_;
  ++next_table_id_;
  page_id_t table_heap_page_id;
  buffer_pool_manager_ -> NewPage(table_heap_page_id);
  TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, schema, txn, log_manager_, lock_manager_);
  TableMetadata *table_meta = TableMetadata::Create(table_id, table_name, table_heap_page_id, schema);
  TableInfo *new_table_info = TableInfo::Create(table_meta, table_heap);

  tables_.emplace(table_id, new_table_info);

  table_names_.emplace(table_name, table_id);
  index_names_.emplace(table_name, std::unordered_map<std::string, index_id_t>());

  table_info = new_table_info;

  return DB_SUCCESS;
}

/**
 * TODO: zrz
 * Done
 */
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  for (auto iterator_ : table_names_) {
    if (iterator_.first == table_name) {
      table_info = tables_[iterator_.second];
      return DB_SUCCESS;
    }
  }
  return DB_TABLE_NOT_EXIST;
}

/**
 * TODO: zrz
 * Done
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  // ASSERT(false, "Not Implemented yet");
  if (tables_.empty()) return DB_TABLE_NOT_EXIST;
  for (auto iterator_ : tables_) {
    tables.emplace_back(iterator_.second);
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 * Done
 */
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info, const string &index_type) {
  TableInfo *table_info;
  dberr_t result = GetTable(table_name, table_info);
  if (result != DB_SUCCESS) {
    LOG(ERROR) << "Table " << table_name << " does not exist";
    return result;
  }
  
  if (index_names_[table_name].count(index_name) > 0) {
    LOG(ERROR) << "Index " << index_name << " already exists";
    return DB_INDEX_ALREADY_EXIST;
  }

  index_id_t index_id = next_index_id_++;

  std::vector<uint32_t> key_map;
  const TableSchema *table_schema = table_info->GetSchema();
  for (const auto &key : index_keys) {
    uint32_t column_index = 0;
    dberr_t result = table_schema->GetColumnIndex(key, column_index);
    if (result != DB_SUCCESS) {
      LOG(ERROR) << "Column " << key << " does not exist in table " << table_name;
      return result;
    }
    key_map.push_back(column_index);
  }

  IndexMetadata *index_meta = IndexMetadata::Create(index_id, index_name, table_info->GetTableId(), key_map);
  IndexInfo *new_index_info = IndexInfo::Create();
  new_index_info->Init(*index_meta, *table_info, *buffer_pool_manager_, key_map);

  page_id_t index_meta_page_id;
  buffer_pool_manager_->NewPage(index_meta_page_id);
  new_index_info->SetRootPageId(index_meta_page_id);

  indexes_.emplace(index_id, new_index_info);

  index_names_[table_name].emplace(index_name, index_id);

  index_info = new_index_info;

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  // ASSERT(false, "Not Implemented yet");
  std::unordered_map<std::string, index_id_t> index_map;
  for (auto iter : index_names_) {
    if (iter.first == table_name) {
      index_map = iter.second;
      for (auto iter : index_map) {
        if (iter.first == index_name) {
          auto index_id = iter.second;
          index_info = indexes_.find(index_id)->second;
          return DB_SUCCESS;
        }
      }
      return DB_INDEX_NOT_FOUND;
    }
  }
  return DB_TABLE_NOT_EXIST;
}

/**
 * TODO: Student Implement
 * Done
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  // ASSERT(false, "Not Implemented yet");
  std::unordered_map<std::string, index_id_t> index_map;
  for (auto iter : index_names_) {
    if (iter.first == table_name) {
      index_map = iter.second;
      for (auto iter : index_map) {
        auto index_id = iter.second;
        indexes.push_back(indexes_.find(index_id)->second);
      }
      return DB_SUCCESS;
    }
  }
  return DB_TABLE_NOT_EXIST;
}

/**
 * TODO: Student Implement
 * Done
 */
dberr_t CatalogManager::DropTable(const string &table_name) {
  auto table_id_find_iter = table_names_.find(table_name);
  if (table_id_find_iter == table_names_.end()) return DB_TABLE_NOT_EXIST;
  auto table_id = table_id_find_iter->second;
  auto index_in_table_iter = index_names_.find(table_name);
  if (index_in_table_iter != index_names_.end()) {
    auto index_map = index_in_table_iter->second;
    for (auto iter : index_map) {
      auto index_id = iter.second;
      auto index_iter = indexes_.find(index_id);
      if (index_iter != indexes_.end()) {
        indexes_.erase(index_iter);
      }
    }
    index_names_.erase(table_name);
  }
  table_names_.erase(table_name);
  tables_.erase(table_id);
  next_table_id_--;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 * Done
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  auto index_in_table_iter = index_names_.find(table_name);
  if (index_in_table_iter == index_names_.end()) return DB_TABLE_NOT_EXIST;
  auto index_map = index_in_table_iter->second;
  auto index_id_find_iter = index_map.find(index_name);
  if (index_id_find_iter == index_map.end()) return DB_INDEX_NOT_FOUND;
  auto index_id = index_id_find_iter->second;
  indexes_.erase(index_id);
  index_names_.erase(index_name);
  next_index_id_--;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  catalog_meta_->GetTableMetaPages()->clear();
  catalog_meta_->GetIndexMetaPages()->clear();

  for (auto iter : tables_) {
    auto table_meta = iter.second->GetTableMetaData();
    char *buf = new char[PAGE_SIZE];
    table_meta->SerializeTo(buf);

    Page *page = buffer_pool_manager_->FetchPage(table_meta->GetFirstPageId());
    if (page == nullptr) {
      delete[] buf;
      return DB_FAILED;
    }

    memcpy(page->GetData(), buf, PAGE_SIZE);
    page->SetDirty();

    buffer_pool_manager_->UnpinPage(table_meta->GetFirstPageId(), true);
    delete[] buf;
    catalog_meta_->GetTableMetaPages()->emplace(iter.second->GetTableId(), table_meta->GetFirstPageId());
  }

  for (auto iter : indexes_) {
    auto index_meta = iter.second->GetIndexMetaData();
    char *buf = new char[PAGE_SIZE];
    index_meta->SerializeTo(buf);

    page_id_t page_id = index_meta->GetRootPageId();
    Page *page = buffer_pool_manager_->FetchPage(page_id);
    if (page == nullptr) {
      delete[] buf; 
      return DB_FAILED;
    }

    memcpy(page->GetData(), buf, PAGE_SIZE);
    page->SetDirty();

    buffer_pool_manager_->UnpinPage(page_id, true);
    delete[] buf;

    catalog_meta_->GetIndexMetaPages()->emplace(index_meta->GetIndexId(), page_id);
  }

  char *buf = new char[PAGE_SIZE];
  catalog_meta_->SerializeTo(buf);

  Page *page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  if (page == nullptr) {
    delete[] buf;
    return DB_FAILED;
  }

  memcpy(page->GetData(), buf, PAGE_SIZE);
  page->SetDirty();

  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  delete[] buf;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  Page *page = buffer_pool_manager_->FetchPage(page_id);
  if (page == nullptr) {
    return DB_FAILED;
  }

  char *buf = page->GetData();
  TableMetadata *table_metadata = nullptr;
  TableMetadata::DeserializeFrom(buf, table_metadata);

  TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, table_metadata->GetFirstPageId(),
                                            table_metadata->GetSchema(), log_manager_, lock_manager_);
  TableInfo *table_info = TableInfo::Create(table_metadata, table_heap);

  tables_.emplace(table_id, table_info);
  table_names_.emplace(table_info->GetTableName(), table_id);
  index_names_.emplace(table_info->GetTableName(), std::unordered_map<std::string, index_id_t>());
  next_table_id_++;

  buffer_pool_manager_->UnpinPage(page_id, false);
 
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 * Done
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  Page *page = buffer_pool_manager_->FetchPage(page_id);
  if (page == nullptr) {
    return DB_FAILED;
  }

  char *buf = page->GetData();
  IndexMetadata *index_metadata = nullptr;
  IndexMetadata::DeserializeFrom(buf, index_metadata);

  table_id_t table_id = index_metadata->GetTableId();
  TableInfo *table_info = TableInfo::Create();
  GetTable(table_id, table_info);

  IndexInfo *index_info = IndexInfo::Create();
  std::vector<uint32_t> key_map = index_metadata->GetKeyMapping();
  index_info->Init(*index_metadata, *table_info, *buffer_pool_manager_, key_map);

  indexes_.emplace(index_id, index_info);

  index_names_[table_info->GetTableName()].emplace(index_info->GetIndexName(), index_id);

  next_index_id_++;

  buffer_pool_manager_->UnpinPage(page_id, false);

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  auto iter = tables_.find(table_id);
  if (iter == tables_.end()) return DB_TABLE_NOT_EXIST;
  table_info = iter->second;
  return DB_SUCCESS;
}