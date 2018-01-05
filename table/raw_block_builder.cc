#include "raw_block_builder.h"

namespace leveldb {

RawBlockBuilder::RawBlockBuilder(const leveldb::Options *options)
    : options_(options),
      finished_(false),
      buffer_(std::string(32, '0')) { }

RawBlockBuilder::~RawBlockBuilder() {
  buffer_.clear();
}

void RawBlockBuilder::Reset() {
  buffer_.clear();
  finished_ = false;
}

void RawBlockBuilder::Add(const Slice &key, const Slice &value) {
  buffer_.append(key.data(), key.size());
  buffer_.append("\t");
  buffer_.append(value.data(), value.size());
  buffer_.append("\t");
}

Slice RawBlockBuilder::Finish() {
  return Slice(buffer_);
}

size_t RawBlockBuilder::CurrentSizeEstimate() const {
  return buffer_.size();
}

uint64_t RawBlockBuilder::GetBufferSize() {
  return buffer_.size();
}


} // namespace leveldb
