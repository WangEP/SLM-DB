//
// Created by olzhas on 1/25/18.
//

#include "range_iterator.h"

namespace leveldb {

bool RangeIterator::Valid() const {
  return false;
}
void RangeIterator::SeekToFirst() {

}
void RangeIterator::SeekToLast() {

}
void RangeIterator::Seek(const Slice& target) {

}
void RangeIterator::Next() {

}
void RangeIterator::Prev() {

}
Slice RangeIterator::key() const {
  return Slice();
}
Slice RangeIterator::value() const {
  return Slice();
}
Status RangeIterator::status() const {
  return Status();
}

} // namespace leveldb
