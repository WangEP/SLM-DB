#include <future>
#include "raw_block_iter.h"

namespace leveldb {

RawBlockIterator::RawBlockIterator(uint64_t buffer_size, SequentialFile* file) {
  stream_ = new Streamer(buffer_size, file);
  count = 0;
  while (!stream_->eof()) {
    Slice key;
    stream_->Get(&key);
    Slice value;
    stream_->Get(&value);
    vector_.push_back({key, value});
  }
  iterator_ = vector_.begin();
}

bool RawBlockIterator::Valid() const {
  return iterator_ != vector_.end();
}

void RawBlockIterator::SeekToFirst() {
  iterator_ = vector_.begin();
  count = 0;
}

void RawBlockIterator::SeekToLast() {
  iterator_ = vector_.end();
  count = vector_.size();
}

void RawBlockIterator::Seek(const Slice &target) {
}

void RawBlockIterator::Next() {
  iterator_++;
  count++;
}

void RawBlockIterator::Prev() {
  iterator_--;
  count++;
}

Slice RawBlockIterator::key() const {
  return iterator_->first;
}

Slice RawBlockIterator::value() const {
  return iterator_->second;
}

Status RawBlockIterator::status() const {
  return stream_->status();
}

}
