#include <util/coding.h>
#include "index_iterator.h"
#include "db/dbformat.h"
#ifdef PERF_LOG
#include "util/perf_log.h"
#endif

namespace leveldb {

static void DeleteIterator(const Slice& key, void* value) {
  Iterator* iterator = reinterpret_cast<Iterator*>(value);
  delete iterator;
}

IndexIterator::IndexIterator(ReadOptions options, FFBtreeIterator* btree_iter, TableCache* table_cache)
  : options_(options),
    btree_iterator_(btree_iter),
    table_cache_(table_cache),
    cache_(NewLRUCache(20)),
    block_iterator_(nullptr),
    handle_(nullptr),
    index_meta_(convert(0)) {
  SeekToFirst();
}

IndexIterator::~IndexIterator() {
  if (handle_ != nullptr) cache_->Release(handle_);
  delete cache_;
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
  btree_iterator_->Seek(fast_atoi(target));
  Advance();
  block_iterator_->Seek(target);
  status_ = block_iterator_->status();
}

void IndexIterator::Next() {
  assert(btree_iterator_->Valid());
  btree_iterator_->Next();
  Advance();
  assert(status_.ok());
  uint32_t key;
  while ((key = fast_atoi(block_iterator_->key())) < btree_iterator_->key()) {
    block_iterator_->Next();
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
  if (!block_iterator_->status().ok()) return block_iterator_->status();
  return status_;
}

void IndexIterator::CacheLookup() {
  if (handle_ != nullptr) cache_->Release(handle_);
  assert(btree_iterator_->value() != nullptr);
  char buf[sizeof(void*)];
  EncodeFixed64(buf, (uint64_t) btree_iterator_->value());
  Slice cache_key(buf, sizeof(buf));
  handle_ = cache_->Lookup(cache_key);
  if (handle_ == nullptr) {
#ifdef PERF_LOG
    uint64_t start_micros = NowMicros();
#endif
    status_ = table_cache_->GetBlockIterator(options_, index_meta_.file_number,
                                             index_meta_.offset, index_meta_.size, &block_iterator_);
#ifdef PERF_LOG
    uint64_t micros = NowMicros() - start_micros;
    logMicro(RANGE, micros);
#endif
    if (!status_.ok()) return; // something went wrong
    char key[100];
    snprintf(key, sizeof(key), "%016lu", btree_iterator_->key());
    block_iterator_->Seek(key);
    handle_ = cache_->Insert(cache_key, block_iterator_, 1,&DeleteIterator);
  } else {
    block_iterator_ = reinterpret_cast<Iterator*>(cache_->Value(handle_));
  }
}

void IndexIterator::Advance() {
  if (btree_iterator_->value() != convert(index_meta_)) {
    index_meta_ = convert(btree_iterator_->value());
    CacheLookup();
  }
}

}
