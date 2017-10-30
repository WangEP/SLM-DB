#include <future>
#include "raw_block_iter.h"

namespace leveldb {

RawBlockIterator::RawBlockIterator(SequentialFile* file) : stream_(new Streamer(file)) {
  count = 0;
  auto handle = std::async(std::launch::async, [this]() {
    Init();
  });
}

void RawBlockIterator::Init() {
  while (!stream_->eof()) {
    Slice key;
    stream_->Get(&key);
    Slice value;
    stream_->Get(&value);
    vector_.push_back({key, value});
  }
}

bool RawBlockIterator::Valid() const {
  return count < vector_.size();
}

void RawBlockIterator::SeekToFirst() {
  count = 0;
}

void RawBlockIterator::SeekToLast() {
  count = vector_.size() - 1;
}

void RawBlockIterator::Seek(const Slice &target) {
}

void RawBlockIterator::Next() {
  count++;
}

void RawBlockIterator::Prev() {
  count--;
}

Slice RawBlockIterator::key() const {
  return vector_[count].first;
}

Slice RawBlockIterator::value() const {
  return vector_[count].second;
}

Status RawBlockIterator::status() const {
  return stream_->status();
}

}
