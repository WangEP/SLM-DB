#include "raw_block_builder.h"

namespace leveldb {

RawBlockBuilder::RawBlockBuilder(const leveldb::Options *options, uint64_t max_size)
    : options_(options),
      finished_(false) {
  size_ = 0;
  max_size_ = max_size;
  buffer_ = new char[max_size_];
}


RawBlockBuilder::~RawBlockBuilder() {
  delete buffer_;
}

void RawBlockBuilder::Reset() {
  delete buffer_;
  size_ = 0;
  buffer_ = nullptr;
  buffer_ = new char[max_size_];
  finished_ = false;
}

void RawBlockBuilder::Add(const Slice &key, const Slice &value) {
  strncpy(buffer_ + size_, key.data(), key.size());
  size_ += key.size();
  strcpy(buffer_ + size_, "\t");
  size_++;
  strncpy(buffer_ + size_, value.data(), value.size());
  size_ += value.size();
  strcpy(buffer_ + size_, "\t");
  size_++;
}

Slice RawBlockBuilder::Finish() {
  return Slice(buffer_, size_);
}

size_t RawBlockBuilder::CurrentSizeEstimate() const {
  return size_ + sizeof(int);
}

uint64_t RawBlockBuilder::GetBufferSize() {
  return size_;
}


} // namespace leveldb
