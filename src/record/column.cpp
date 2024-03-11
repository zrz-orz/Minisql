#include "record/column.h"

#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), len_(length), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

/**
 * TODO: zrz
 * Done
 */
uint32_t Column::SerializeTo(char *buf) const {
  uint32_t offset = 0;
  uint32_t len = name_.length();
  memcpy(buf, &len, sizeof(uint32_t));
  memcpy(buf + sizeof(uint32_t), name_.c_str(), len);
  offset += len + sizeof(uint32_t);
  MACH_WRITE_TO(TypeId, buf + offset, type_);
  offset += sizeof(TypeId);
  MACH_WRITE_TO(uint32_t, buf + offset, len_);
  offset += sizeof(uint32_t);
  MACH_WRITE_TO(uint32_t, buf + offset, table_ind_);
  offset += sizeof(uint32_t);
  MACH_WRITE_TO(bool, buf + offset, nullable_);
  offset += sizeof(bool);
  MACH_WRITE_TO(bool, buf + offset, unique_);
  offset += sizeof(bool);
  return offset;
}

/**
 * TODO: zrz
 * Done
 */
uint32_t Column::GetSerializedSize() const {
  uint32_t offset = 0;
  offset += name_.length() + sizeof(uint32_t);
  offset += sizeof(type_);
  offset += sizeof(uint32_t) << 1;
  offset += sizeof(bool) << 1;
  return offset;
}

/**
 * TODO: Student Implement
 * Done
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
  if (column != nullptr) {
    LOG(WARNING) << "Pointer to column is not null in column deserialize." << std::endl;
  }
  uint32_t offset = 0, namelen, len, tabindex;
  bool nullable, unique;
  TypeId type;
  namelen = MACH_READ_FROM(uint32_t, buf);
  std::string name(buf + sizeof(uint32_t), namelen);
  offset += namelen + sizeof(uint32_t);
  type = MACH_READ_FROM(TypeId, buf + offset);
  offset += sizeof(TypeId);
  len = MACH_READ_FROM(uint32_t, buf + offset);
  offset += sizeof(uint32_t);
  tabindex = MACH_READ_FROM(uint32_t, buf + offset);
  offset += sizeof(uint32_t);
  nullable = MACH_READ_FROM(bool, buf + offset);
  offset += sizeof(bool);
  unique = MACH_READ_FROM(bool, buf + offset);
  offset += sizeof(bool);

  if (type == kTypeChar) {
    column = new Column(name, type, len, tabindex, nullable, unique);
  } else {
    column = new Column(name, type, tabindex, nullable, unique);
  }
  return offset;
}
