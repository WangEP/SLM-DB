#include "persistent_memtable.h"

namespace leveldb {

PersistentMemtable::PersistentMemtable(const Comparator* cmp)
    : cmp_(cmp),
      table_(new PersistentSkiplist(cmp)),
      compaction_current_size(0),
      mutex(new port::Mutex),
      compaction_start(NULL),
      compaction_end(NULL) { }

PersistentMemtable::PersistentMemtable(const Comparator* cmp,
                                       MemtableIterator* begin,
                                       MemtableIterator* end,
                                       size_t size)
    : cmp_(cmp),
      mutex(new port::Mutex),
      compaction_start(begin),
      compaction_end(end){
  table_ = new PersistentSkiplist(cmp, begin->GetNode(), end->GetNode(), size);
}

PersistentMemtable::~PersistentMemtable() {
  delete compaction_start;
  delete compaction_end;
  delete table_;
  delete mutex;
}

Iterator* PersistentMemtable::NewIterator() {
  return new MemtableIterator(table_, table_->Head());
}

void PersistentMemtable::Add(const Slice& key, const Slice& value) {
  mutex->Lock();
  assert(compaction_start == NULL || !compaction_start->key().empty());
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
    } else if (cmp_->Compare(key, compaction_start->key()) > 0) {
      compaction_current_size += node->GetSize();
    }
  }
  else if (compaction_current_size >= compaction_target_size &&
      cmp_->Compare(compaction_start->key(), key) < 0 &&
      cmp_->Compare(key, compaction_end->key()) < 0) {
    compaction_current_size += node->GetSize() - compaction_end->GetSize();
    compaction_end->Prev();
    assert(cmp_->Compare(compaction_start->key(), compaction_end->key()) < 0);
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
  assert(compaction_current_size >= 2 << 20);
  auto node = compaction_end->GetNode()->next[0];
  table_->Erase(compaction_start->GetNode(), compaction_end->GetNode(), compaction_current_size);
  auto imm = new PersistentMemtable(cmp_, compaction_start, compaction_end, compaction_current_size);
//  // delete iterators
//  delete compaction_start;
//  delete compaction_end;
  // initialize new
  if (node == table_->Tail()) {
    if (table_->Head()->next[0] == node) {
      node = NULL;
    } else {
      node = table_->Head()->next[0];
    }
  }

  if (node == NULL) {
    compaction_current_size = 0;
    compaction_start = NULL;
    compaction_end = NULL;
  } else {
    compaction_current_size = node->GetSize();
    compaction_start = new MemtableIterator(table_, node);
    compaction_end = new MemtableIterator(table_, node);
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
