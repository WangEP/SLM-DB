#include "persistent_memtable.h"

namespace leveldb {

PersistentMemtable::PersistentMemtable(const Comparator* cmp)
    : cmp_(cmp),
      table_(new PersistentSkiplist(cmp)),
      refs_(0) { }

PersistentMemtable::PersistentMemtable(const Comparator* cmp,
                                       PersistentSkiplist::Node* begin,
                                       PersistentSkiplist::Node* end,
                                       size_t size)
    : cmp_(cmp),
      refs_(0) {
  table_ = new PersistentSkiplist(cmp, begin, end, size);
}

PersistentMemtable::~PersistentMemtable() {
  delete table_;
}

Iterator* PersistentMemtable::NewIterator() {
  return new MemtableIterator(table_, table_->Head()->next[0]);
}

void PersistentMemtable::Add(const Slice& key, const Slice& value) {
  table_->Insert(key, value);
}

bool PersistentMemtable::Get(const Slice& key, std::string* value) {
  auto node = table_->Find(key);
  if (node != NULL) {
    value->assign(node->value);
    return true;
  }
  return false;
}

std::pair<const Slice&, const Slice&> PersistentMemtable::GetRange() {
  Slice smallest = table_->Head()->next[0]->key;
  Slice largest = table_->Tail()->prev[0]->key;
  return {smallest, largest};
};

}
