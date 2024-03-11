#include "record/row.h"

/**
 * TODO: Student Implement
 */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  ASSERT(fields_.size() <= 32, "Null map overflows");
  uint32_t num = fields_.size(), _null = 0, offset = sizeof(uint32_t) * 2 + sizeof(RowId);
  MACH_WRITE_TO(RowId, buf, rid_);
  MACH_WRITE_TO(uint32_t, buf + sizeof(RowId), num);

  for (auto f : fields_) {
    offset += f->SerializeTo(buf + offset);
    _null = (_null << 1) | f->IsNull();
  }
  MACH_WRITE_TO(uint32_t, buf + sizeof(uint32_t) + sizeof(RowId), _null);
  return offset;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(fields_.empty(), "Non empty field in row.");

  ASSERT(schema->GetColumnCount() <= 32, "Null map overflows");

  uint32_t num = 0, _null = 0, offset = sizeof(uint32_t) * 2 + sizeof(RowId);
  rid_ = MACH_READ_FROM(RowId, buf);
  num = MACH_READ_UINT32(buf + sizeof(RowId));
  _null = MACH_READ_UINT32(buf + sizeof(uint32_t) + sizeof(RowId));
  ASSERT(num == schema->GetColumnCount(), "Fields size do not match schema's column size.");

  for (auto col : schema->GetColumns()) {
    Field **f = new Field *;
    offset += Field::DeserializeFrom(buf + offset, col->GetType(), f, _null & (1 << (num - 1)));
    fields_.push_back(*f);
    num--;
  }
  return offset;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  uint32_t res = 0;
  for (auto field : fields_) {
    res += field->GetSerializedSize();
  }
  return res + 2 * sizeof(uint32_t) + sizeof(RowId);
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column : columns) {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
}
