#include "persistent_memtable.h"

namespace leveldb {

PersistentMemtable::PersistentMemtable(const Comparator* cmp)
    : cmp_(cmp),
      table_(new PersistentSkiplist(cmp)),
      mutex(new port::Mutex),
      compaction_iter(NULL),
      refs_(0) { }

PersistentMemtable::PersistentMemtable(const Comparator* cmp,
                                       PersistentSkiplist::Node* begin,
                                       PersistentSkiplist::Node* end,
                                       size_t size)
    : cmp_(cmp),
      mutex(new port::Mutex),
      compaction_iter(NULL),
      refs_(0) {
  table_ = new PersistentSkiplist(cmp, begin, end, size);
}

PersistentMemtable::~PersistentMemtable() {
  delete compaction_iter;
  delete table_;
  delete mutex;
}

Iterator* PersistentMemtable::NewIterator() {
  return new MemtableIterator(table_, table_->Head());
}

void PersistentMemtable::Add(const Slice& key, const Slice& value) {
  mutex->Lock();
  auto node = table_->Insert(key, value);
  if (compaction_iter == NULL)
    compaction_iter = new MemtableIterator(table_, node);
  mutex->Unlock();
}

bool PersistentMemtable::Get(const Slice& key, std::string* value) {
  auto node = table_->Find(key);
  if (node != NULL) {
    value->assign(node->value);
    return true;
  }
  return false;
}

PersistentMemtable* PersistentMemtable::Compact() {
  size_t compaction_size = 0;
  auto node = compaction_iter->GetNode();
  while (compaction_size < compaction_target_size && node != table_->Tail()) {
    compaction_size += node->GetSize();
    assert(node->next[0] != NULL);
    node = node->next[0];
  }
  auto left_node = compaction_iter->GetNode();
  auto right_node = node->prev[0];
  mutex->Lock();
  // move sublist and create new memtable
  table_->Erase(left_node, right_node, compaction_size);
  auto imm = new PersistentMemtable(cmp_, left_node, right_node, compaction_size);
  // delete old iter
  delete compaction_iter;
  // check whether iterator hit end, then move to beginning
  if (node == table_->Tail()) {
    if (table_->Head()->next[0] == node) {
      node = NULL; // memtable empty
      compaction_iter = NULL;
    } else {
      node = table_->Head()->next[0];
    }
  }
  if (node != NULL) {
    compaction_iter = new MemtableIterator(table_, node);
  }
  mutex->Unlock();
  return imm;
}

std::pair<const Slice&, const Slice&> PersistentMemtable::GetRange() {
  Slice smallest = table_->Head()->next[0]->key;
  Slice largest = table_->Tail()->prev[0]->key;
  return {smallest, largest};
};

}
