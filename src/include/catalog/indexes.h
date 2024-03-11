#ifndef MINISQL_INDEXES_H
#define MINISQL_INDEXES_H

#include <memory>

#include "catalog/table.h"
#include "common/macros.h"
#include "common/rowid.h"
#include "index/b_plus_tree_index.h"
#include "index/generic_key.h"
#include "record/schema.h"

class IndexMetadata {
  friend class IndexInfo;

 public:
  static IndexMetadata *Create(const index_id_t index_id, const std::string &index_name, const table_id_t table_id,
                               const std::vector<uint32_t> &key_map);

  uint32_t SerializeTo(char *buf) const;

  uint32_t GetSerializedSize() const;

  static uint32_t DeserializeFrom(char *buf, IndexMetadata *&index_meta);

  inline std::string GetIndexName() const { return index_name_; }

  inline table_id_t GetTableId() const { return table_id_; }

  uint32_t GetIndexColumnCount() const { return key_map_.size(); }

  inline const std::vector<uint32_t> &GetKeyMapping() const { return key_map_; }

  inline index_id_t GetIndexId() const { return index_id_; } 

  inline page_id_t GetRootPageId() const { return root_page_id_; }

  inline void SetRootPageId(const page_id_t root_page_id) { root_page_id_ = root_page_id; }

 private:
  IndexMetadata() = delete;

  explicit IndexMetadata(const index_id_t index_id, const std::string &index_name, const table_id_t table_id,
                         const std::vector<uint32_t> &key_map);

 private:
  static constexpr uint32_t INDEX_METADATA_MAGIC_NUM = 344528;
  index_id_t index_id_;
  page_id_t root_page_id_;
  std::string index_name_;
  table_id_t table_id_;
  std::vector<uint32_t> key_map_; /** The mapping of index key to tuple key */
};

/**
 * The IndexInfo class maintains metadata about a index.
 */
class IndexInfo {
 public:
  static IndexInfo *Create() { return new IndexInfo(); }

  ~IndexInfo() {
    delete meta_data_;
    delete index_;
    delete key_schema_;
  }

/**
 * TODO: Student Implement
 * Done
 */
  void Init(IndexMetadata &index_meta_data, TableInfo &table_info, BufferPoolManager &buffer_pool_manager,
            std::vector<uint32_t> &key_map) {
    meta_data_ = new IndexMetadata(index_meta_data);
    Schema *table_schema = table_info.GetSchema();
    key_schema_ = Schema::ShallowCopySchema(table_schema, key_map);
    index_ = new BPlusTreeIndex(index_meta_data.GetIndexId(), key_schema_, 256, &buffer_pool_manager);
  }

  inline Index *GetIndex() { return index_; }

  std::string GetIndexName() { return meta_data_->GetIndexName(); }

  IndexMetadata *GetIndexMetaData() { return meta_data_; }

  IndexSchema *GetIndexKeySchema() { return key_schema_; }

  void SetRootPageId(page_id_t root_page_id) { meta_data_->root_page_id_ = root_page_id; }

  page_id_t GetRootPageId() { return meta_data_->root_page_id_; }

 private:
  explicit IndexInfo() : meta_data_{nullptr}, index_{nullptr}, key_schema_{nullptr} {}

  Index *CreateIndex(BufferPoolManager *buffer_pool_manager, const string &index_type);

 private:
  IndexMetadata *meta_data_;
  Index *index_;
  IndexSchema *key_schema_;
};

#endif  // MINISQL_INDEXES_H
