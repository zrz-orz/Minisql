#include "record/schema.h"

/**
 * TODO: zrz
 * Done
 */
uint32_t Schema::SerializeTo(char *buf) const {
  uint32_t size = columns_.size(), offset = sizeof(uint32_t) + sizeof(bool);
  bool is_manage = is_manage_;
  MACH_WRITE_TO(uint32_t, buf, size);
  MACH_WRITE_TO(bool, buf + sizeof(uint32_t), is_manage);

  for (auto col : columns_) {
    offset += col->SerializeTo(buf + offset);
  }
  return offset;
}

uint32_t Schema::GetSerializedSize() const {
  uint32_t offset = sizeof(uint32_t) + sizeof(bool);
  for (auto this_col : columns_) offset += this_col->GetSerializedSize();
  return offset;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
  if (schema != nullptr) {
    LOG(WARNING) << "Pointer to schema is not null in schema deserialize." << std::endl;
  }
  uint32_t size = 0, offset = sizeof(uint32_t) + sizeof(bool);
  bool is_manage = 0;
  size = MACH_READ_FROM(uint32_t, buf);
  is_manage = MACH_READ_FROM(bool, buf + sizeof(uint32_t));
  std::vector<Column *> cols;
  for (uint32_t i = 1; i <= size; i++) {
    Column *tmpcol = nullptr;
    offset += Column::DeserializeFrom(buf + offset, tmpcol);
    cols.push_back(tmpcol);
  }
  schema = new Schema(cols, is_manage);
  return offset;
}