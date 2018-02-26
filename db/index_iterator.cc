#include "index_iterator.h"


namespace leveldb {

IndexIterator::IndexIterator(std::vector<LeafEntry*> entries) {

}

bool IndexIterator::Valid() const {
  return iterator_ != entries_.begin() && iterator_ != entries_.end();
}

void IndexIterator::SeekToFirst() {
  iterator_ = entries_.begin();
}

void IndexIterator::SeekToLast() {
  iterator_ = entries_.end();
}

void IndexIterator::Seek(const Slice& target) {

}

void IndexIterator::Next() {
  if (iterator_ != entries_.end()) iterator_++;
}

void IndexIterator::Prev() {
  if (iterator_ != entries_.begin()) iterator_--;
}

Slice IndexIterator::key() const {
  return std::to_string((*iterator_)->key);
}

Slice IndexIterator::value() const {
  return Slice();
}
Status IndexIterator::status() const {
  return Status();
}

}
