#include <db/version_edit.h>
#include <future>
#include "raw_table_builder.h"
#include "raw_block_builder.h"
#include "include/leveldb/index.h"

namespace leveldb {

struct RawTableBuilder::Rep {
  Options options;
  WritableFile* file;
  uint64_t file_number;
  Status status;
  RawBlockBuilder data_block;
  std::string last_key;
  uint64_t num_entries;
  bool closed;
  Index* index;

  Rep(const Options& opt, WritableFile* f, uint64_t number)
      : options(opt),
        file(f),
        file_number(number),
        num_entries(0),
        closed(false),
        index(opt.index),
        data_block(&options) {  }
};

RawTableBuilder::RawTableBuilder(const Options& options, WritableFile* file, uint64_t file_number)
    : rep_(new Rep(options, file, file_number)) {
}

RawTableBuilder::~RawTableBuilder() {
  delete rep_;
}

void RawTableBuilder::Add(const Slice& key, const Slice& value) {
  Rep* r = rep_;
  assert(!r->closed);
  if (r->options.comparator->Compare(key, Slice("0000000000000210")) == 0) {
    int i = 0;
  }
  if (r->num_entries > 0) {
    assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
  }
  if (!ok()) return;
  r->last_key.assign(key.data(), key.size());
  r->num_entries++;
  r->data_block.Add(key, value);
}

void RawTableBuilder::Flush() {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->data_block.empty()) return;
  Slice raw = r->data_block.Finish();
  std::string prefix = std::to_string(raw.size()-32);
  const char* data = raw.data();
  memcpy((void *) (data+32-prefix.size()), prefix.data(), prefix.size());
  r->file->Append(raw);
  r->data_block.Reset();
}

Status RawTableBuilder::status() const {
  return rep_->status;
}

Status RawTableBuilder::Finish() {
  Rep *r = rep_;
  Flush();
  assert(!r->closed);
  r->closed = true;
  return r->status;
}

void RawTableBuilder::Abandon() {
  Rep *r = rep_;
  assert(!r->closed);
  r->closed = true;
}

uint64_t RawTableBuilder::FileSize() {
  return rep_->data_block.GetBufferSize();
}

uint64_t RawTableBuilder::NumEntries() const {
  return rep_->num_entries;
}

} // namespace leveldb