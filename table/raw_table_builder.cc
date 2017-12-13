#include <db/version_edit.h>
#include <future>
#include "raw_table_builder.h"
#include "raw_block_builder.h"
#include "include/leveldb/global_index.h"

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
  GlobalIndex* global_index;

  Rep(const Options& opt, WritableFile* f, uint64_t number)
      : options(opt),
        file(f),
        file_number(number),
        num_entries(0),
        closed(false),
        global_index(opt.global_index),
        data_block(&options) {}
};

RawTableBuilder::RawTableBuilder(const Options& options, WritableFile* file, uint64_t file_number)
    : rep_(new Rep(options, file, file_number)) { }

RawTableBuilder::~RawTableBuilder() {
  delete rep_;
}

void RawTableBuilder::Add(const Slice &key, const Slice &value) {
  Rep* r = rep_;
  assert(!r->closed);
  if (r->num_entries > 0) {
    assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
  }
  GlobalIndex* index = r->global_index;
  assert(!r->closed);
  if (!ok()) return;
  Slice pref_key(key.data(), key.size());
  r->last_key.assign(pref_key.data(), pref_key.size());
  r->num_entries++;
  r->data_block.Add(pref_key, value);
  uint64_t offset = 32 + r->data_block.GetBufferSize() - value.size() - 1;
  index->Insert(key, offset, value.size(), r->file_number);
}

void RawTableBuilder::Flush() {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->data_block.empty()) return;
  Slice raw = r->data_block.Finish();
  std::string size = std::to_string(raw.size());
  char prefix[32];
  memset(prefix, '0', 32-size.size());
  memcpy(prefix+32-size.size(), size.data(), size.size());
  Slice block_size(prefix, 32);
  r->file->Append(block_size);
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