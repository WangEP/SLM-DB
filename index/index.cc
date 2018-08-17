#include "leveldb/index.h"
#include "btree_index.h"

namespace leveldb {


void* convert(IndexMeta meta) {
  uint64_t t = 0;
  t += meta.offset;
  t = t << 16;
  t += meta.size;
  t = t << 16;
  t += meta.file_number;
  return (void*) t;
}

IndexMeta convert(void* ptr) {
  uint64_t t = (uint64_t) ptr;
  IndexMeta meta;
  meta.file_number = t % (1 << 17);
  meta.size = (t >> 16) % (1 << 17);
  meta.offset = t >> 32;
  return meta;
}

Index* CreateBtreeIndex() {
  return new BtreeIndex();
}

FFBtreeIterator* Index::BtreeIterator() {
  return tree_.GetIterator();
}

} // namespace leveldb