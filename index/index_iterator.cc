#include <util/coding.h>
#include "index_iterator.h"
#include "db/dbformat.h"
#include "db/version_control.h"
#ifdef PERF_LOG
#include "util/perf_log.h"
#endif

namespace leveldb {

static void DeleteIterator(const Slice& key, void* value) {
  Iterator* iterator = reinterpret_cast<Iterator*>(value);
  delete iterator;
}

IndexIterator::IndexIterator(ReadOptions options, FFBtreeIterator* btree_iter, TableCache* table_cache, VersionControl* vcontrol)
  : options_(options),
    btree_iterator_(btree_iter),
    table_cache_(table_cache),
    block_iterator_(nullptr),
    index_meta_(nullptr),
    vcontrol_(vcontrol),
    counter_(0) {
  SeekToFirst();
}

IndexIterator::~IndexIterator() {
  if (files_to_merge_.size() > config::ScanCheckMinFileNumber &&
  vcontrol_->current()->MoveToMerge(files_to_merge_, true)) {
    vcontrol_->StateChange();
  }
  delete btree_iterator_;
}

bool IndexIterator::Valid() const {
  return btree_iterator_->Valid() && block_iterator_ != nullptr && block_iterator_->Valid();
}

void IndexIterator::SeekToFirst() {
  btree_iterator_->SeekToFirst();
  Advance();
}

void IndexIterator::SeekToLast() {
  btree_iterator_->SeekToLast();
}

void IndexIterator::Seek(const Slice& target) {
  btree_iterator_->Seek(fast_atoi(ExtractUserKey(target)));
  Advance();
  block_iterator_->Seek(target);
  status_ = block_iterator_->status();
}

void IndexIterator::Next() {
  assert(btree_iterator_->Valid());
  btree_iterator_->Next();
  Advance();
  if (!status_.ok()) {
    fprintf(stderr, "%s\n", status_.ToString().c_str());
    assert(status_.ok());
  }
  entry_key_t key = 0;
  while (block_iterator_->Valid() &&
         (key = fast_atoi(ExtractUserKey(block_iterator_->key()))) < btree_iterator_->key()) {
    assert(block_iterator_->Valid());
    block_iterator_->Next();
  }
  if (!btree_iterator_->Valid()) {
    return; // last index
  }
  if (key != btree_iterator_->key()) {
    status_ = Status::NotFound(std::to_string(btree_iterator_->key()));
  }
}

void IndexIterator::Prev() {
  // not implemented
}

Slice IndexIterator::key() const {
  return block_iterator_->key();
}

Slice IndexIterator::value() const {
  return block_iterator_->value();
}

Status IndexIterator::status() const {
  assert(block_iterator_->status().ok());
  assert(status_.ok());
  if (!block_iterator_->status().ok()) return block_iterator_->status();
  return status_;
}

void IndexIterator::CacheLookup() {
  assert(index_meta_ != nullptr);
  delete block_iterator_;
  status_ = table_cache_->GetBlockIterator(options_, index_meta_, &block_iterator_);
  if (!status_.ok()) return; // something went wrong
  char key[100];
  snprintf(key, sizeof(key), config::key_format, btree_iterator_->key());
  block_iterator_->Seek(key);
}

void IndexIterator::Advance() {
  counter_++;
  if (!IsEqual(index_meta_, (IndexMeta*)btree_iterator_->value())) {
    index_meta_ = (IndexMeta*) btree_iterator_->value();
    uniq_files_.insert(index_meta_->file_number);
    if (counter_ % cardinality == 0 && uniq_files_.size() > files_to_merge_.size()) {
      files_to_merge_.swap(uniq_files_);
      uniq_files_.clear();
    }
    CacheLookup();
  }
}

}
