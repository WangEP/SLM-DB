#include "persistent_memtable.h"

namespace leveldb {

PersistentMemtable::PersistentMemtable(const Comparator* cmp)
    : cmp_(cmp),
      table_(new PersistentSkiplist(cmp)),
      compaction_current_size(0),
      mutex(new port::Mutex) { }

PersistentMemtable::PersistentMemtable(const Comparator* cmp,
                                       MemtableIterator* begin,
                                       MemtableIterator* end,
                                       size_t size)
    : cmp_(cmp) {
  table_ = new PersistentSkiplist(cmp, begin->GetNode(), end->GetNode(), size);
}

PersistentMemtable::~PersistentMemtable() {
}

Iterator* PersistentMemtable::NewIterator() {
  return new MemtableIterator(table_, table_->Head());
}

void PersistentMemtable::Add(const Slice& key, const Slice& value) {
  mutex->Lock();
  auto node = table_->Insert(key, value);
  if (compaction_start == NULL) {
    compaction_start = new MemtableIterator(table_, node);
    compaction_end = new MemtableIterator(table_, node);
    compaction_current_size += node->GetSize();
  }
  else if (compaction_current_size < compaction_target_size) {
    if (cmp_->Compare(key, compaction_end->key()) > 0) {
      compaction_end->Next();
      compaction_current_size += compaction_end->GetSize();
    } else {
      compaction_current_size += node->GetSize();
    }
  }
  else if (compaction_current_size >= compaction_target_size &&
      cmp_->Compare(key, compaction_end->key()) < 0) {
    compaction_current_size += node->GetSize() - compaction_end->GetSize();
    compaction_end->Prev();
  }
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
  mutex->Lock();
  auto node = compaction_start->GetNode()->next[0];
  table_->Erase(compaction_start->GetNode(), compaction_end->GetNode());
  auto imm = new PersistentMemtable(cmp_, compaction_start, compaction_end, compaction_current_size);
  // delete iterators
  delete compaction_start;
  delete compaction_end;
  // initialize new
  compaction_current_size = node->GetSize();
  compaction_start = new MemtableIterator(table_, node);
  compaction_end = new MemtableIterator(table_, node);
  mutex->Unlock();
  return imm;
}

std::pair<const Slice&, const Slice&> PersistentMemtable::GetRange() {
  Slice smallest = table_->Head()->next[0]->key;
  Slice largest = table_->Tail()->prev[0]->key;
  return {smallest, largest};
};

}
