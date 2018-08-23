#include "leveldb/index.h"
#include "btree_index.h"

namespace leveldb {

bool IsEqual(const IndexMeta* lhs, const IndexMeta* rhs) {
  if (lhs == nullptr || rhs == nullptr) return false;
  if (lhs->file_number != rhs->file_number) return false;
  if (lhs->offset != rhs->offset) return false;
  if (lhs->size != rhs->size) return false;
  return true;
}

Index* CreateBtreeIndex() {
  return new BtreeIndex();
}

} // namespace leveldb