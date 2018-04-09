#include <util/coding.h>
#include "index_iterator.h"
#include "dbformat.h"

namespace leveldb {

IndexIterator::IndexIterator(std::vector<LeafEntry*> entries, void* ptr)
    : entries_(entries),
      vset_(reinterpret_cast<VersionControl*>(ptr)),
      index_ptr_(NULL),
      table_handle_(NULL),
      block_iterator_(NULL) {
  SeekToFirst();
}

IndexIterator::~IndexIterator() {
  index_ptr_->Unref();
  delete table_handle_;
  delete block_iterator_;
}

bool IndexIterator::Valid() const {
  return iterator_ != entries_.end();
}

void IndexIterator::SeekToFirst() {
  iterator_ = entries_.begin();
  IndexChange();
}

void IndexIterator::SeekToLast() {
  iterator_ = entries_.end();
  IndexChange();
}

void IndexIterator::Seek(const Slice& target) {
//  int64_t t = fast_atoi(target.data(), target.size());
//  iterator_ = std::lower_bound(entries_.begin(), entries_.end(), t,
//                   [](LeafEntry* a, LeafEntry* b) {
//                     return a->key < b->key;
//                   });
}

void IndexIterator::Next() {
  iterator_++;
  if (iterator_ != entries_.end()) {
    IndexChange();
  }
}

void IndexIterator::Prev() {
  iterator_--;
  if (iterator_ != entries_.begin()) {
    IndexChange();
  }
}

Slice IndexIterator::key() const {
  return block_iterator_->key();
}

Slice IndexIterator::value() const {
  return block_iterator_->value();
}

Status IndexIterator::status() const {
  if (entries_.empty()) {
    return Status::Corruption("Indexing is corrupted");
  }
  return Status();
}

void IndexIterator::IndexChange() {
  bool changed = false;
  it++;
  if (index_ptr_ != (*iterator_)->ptr) {
    if (index_ptr_) index_ptr_->Unref();
    index_ptr_ = reinterpret_cast<IndexMeta*>((*iterator_)->ptr);
    index_ptr_->Ref();
    changed = true;
  }
  if (file_number_ != index_ptr_->file_number) {
    delete table_handle_;
    table_handle_ = new TableHandle;
    file_number_ = index_ptr_->file_number;
    vset_->cache()->GetTable(file_number_, table_handle_);
  }
  if (changed) {
    delete block_iterator_;
    block_iterator_ = table_handle_->table_->BlockReader2(
        table_handle_->table_, options_, index_ptr_->handle);
    char k[100];
    snprintf(k, sizeof(k), "%016d", (*iterator_)->key);
    std::string key = k;
    LookupKey lkey(k, vset_->LastSequence());
    block_iterator_->Seek(lkey.internal_key());
  } else {
    block_iterator_->Next();
  }
}

}
