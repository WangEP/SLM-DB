#include "persistent_memtable.h"

namespace leveldb {
PersistentMemtable::PersistentMemtable(const Comparator& cmp)
    : cmp_(cmp), table_(cmp) {

}

PersistentMemtable::~PersistentMemtable() {
}

size_t PersistentMemtable::ApproximateMemoryUsage() {
  return table_.ApproximateMemoryUsage();
}

Iterator *PersistentMemtable::NewIterator() {
  return nullptr;
}

void PersistentMemtable::Add(const Slice& key, const Slice& value) {
  table_.Insert(key, value);
}

bool PersistentMemtable::Get(const Slice& key, std::string* value) {
  auto node = table_.Find(key);
  if (node != NULL) {
    value->assign(node->value);
    return true;
  }
  return false;
}

}
