#include "raw_table_builder.h"
#include "raw_block_builder.h"
#include "db/global_index.h"

namespace leveldb {

struct RawTableBuilder::Rep {
  Options options;
  WritableFile* file;
  Status status;
  RawBlockBuilder data_block;
  std::string last_key;
  uint64_t num_entries;
  bool closed;
  GlobalIndex* global_index;

  Rep(const Options& opt, WritableFile* f)
      : options(opt),
        file(f),
        num_entries(0),
        closed(false),
        global_index(opt.global_index),
        data_block(&options) {}
};

RawTableBuilder::RawTableBuilder(const Options& options, WritableFile* file)
    : rep_(new Rep(options, file)) { }

RawTableBuilder::~RawTableBuilder() {
  delete rep_;
}

void RawTableBuilder::Add(const Slice &key, const Slice &value) {
  Rep* r = rep_;
  GlobalIndex* index = r->global_index;
  assert(!r->closed);
  if (!ok()) return;
  Slice pref_key(key.data(), key.size() - 8);
  r->last_key.assign(pref_key.data(), pref_key.size());
  r->num_entries++;
  r->data_block.Add(pref_key, value);
  uint64_t offset = r->data_block.GetBufferSize() - value.size() - 1;
  index->Add(pref_key.ToString(), offset, value.size(), r->file);
}

void RawTableBuilder::Flush() {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->data_block.empty()) return;
  Slice raw = r->data_block.Finish();
  r->data_block.Reset();
  r->file->Append(raw);
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

} // namespace leveldb