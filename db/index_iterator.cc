#include <util/coding.h>
#include "index_iterator.h"
#include "dbformat.h"

namespace leveldb {

IndexIterator::IndexIterator(std::vector<LeafEntry*> entries, void* vcurrent, uint64_t number)
    : entries_(entries),
      index_ptr(NULL),
      vcurrent_(reinterpret_cast<Version*>(vcurrent)),
      number_(number) {
  SeekToFirst();
}

bool IndexIterator::Valid() const {
  return iterator_ != entries_.begin() && iterator_ != entries_.end();
}

void IndexIterator::SeekToFirst() {
  iterator_ = entries_.begin();
  iterator_++;
  key_ = std::to_string((*iterator_)->key);
  IndexChange();
}

void IndexIterator::SeekToLast() {
  iterator_ = entries_.end();
  iterator_--;
  key_ = std::to_string((*iterator_)->key);
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
  if (iterator_ != entries_.end()) iterator_++;
  key_ = std::to_string((*iterator_)->key);
  IndexChange();
}

void IndexIterator::Prev() {
  if (iterator_ != entries_.begin()) iterator_--;
  key_ = std::to_string((*iterator_)->key);
  IndexChange();
}

Slice IndexIterator::key() const {
  return key_;
}

Slice IndexIterator::value() const {
  return Slice(value_->data(), value_->size());
}

Status IndexIterator::status() const {
  if (entries_.empty()) {
    return Status::Corruption("Indexing is corrupted");
  }
  return Status();
}

void IndexIterator::IndexChange() {
  if (index_ptr != (*iterator_)->ptr) {
    index_ptr = reinterpret_cast<IndexMeta*>((*iterator_)->ptr);
  }
  LookupKey lkey(key_, number_);
  value_->clear();
  vcurrent_->Get3(options_, lkey, value_, index_ptr);
}

}
