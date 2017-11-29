#include "raw_block_builder.h"

namespace leveldb {

RawBlockBuilder::RawBlockBuilder(const leveldb::Options *options)
    : options_(options),
      finished_(false) {
}


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
  std::string size = std::to_string(buffer_.size());
  char prefix[32];
  memset(prefix, '0', 32-size.size());
  memcpy(prefix+32-size.size(), size.data(), size.size());
  buffer_.insert(0, prefix, 32);
  return Slice(buffer_);
}

size_t RawBlockBuilder::CurrentSizeEstimate() const {
  return buffer_.size();
}

uint64_t RawBlockBuilder::GetBufferSize() {
  return buffer_.size();
}


} // namespace leveldb
