#include <future>
#include "raw_block_iter.h"

namespace leveldb {

RawBlockIterator::RawBlockIterator(SequentialFile* file) : stream_(file) {
  auto handle = std::async(std::launch::async, [this]() {
    Init();
  });
}

void RawBlockIterator::Init() {
  while (!stream_.eof()) {
    vector_.push_back({stream_.Get(), stream_.Get()});
  }
}

bool RawBlockIterator::Valid() const {
  return iterator_ != vector_.end();
}

void RawBlockIterator::SeekToFirst() {
}

void RawBlockIterator::SeekToLast() {
}

void RawBlockIterator::Seek(const Slice &target) {
}

void RawBlockIterator::Next() {
  iterator_++;
}

void RawBlockIterator::Prev() {
  iterator_--;
}

Slice RawBlockIterator::key() const {
  return iterator_->first;
}

Slice RawBlockIterator::value() const {
  return iterator_->second;
}

Status RawBlockIterator::status() const {
  return stream_.status();
}

}
