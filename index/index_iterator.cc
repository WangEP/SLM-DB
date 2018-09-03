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
//    cache_(NewLRUCache(20)),
    block_iterator_(nullptr),
//    handle_(nullptr),
    index_meta_(nullptr) {
  SeekToFirst();
}

IndexIterator::~IndexIterator() {
//  if (handle_ != nullptr) cache_->Release(handle_);
//  delete cache_;
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
  entry_key_t key = 0;
  while (btree_iterator_->value() != index_meta_) {
    Advance();
    if (!status_.ok()) {
      fprintf(stderr, "%s\n", status_.ToString().c_str());
      assert(status_.ok());
    }
    while (block_iterator_->Valid() &&
           (key = fast_atoi(ExtractUserKey(block_iterator_->key()))) < btree_iterator_->key()) {
      assert(block_iterator_->Valid());
      block_iterator_->Next();
    }
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
//  if (handle_ != nullptr) cache_->Release(handle_);
  assert(index_meta_ != nullptr);
  delete block_iterator_;
  status_ = table_cache_->GetBlockIterator(options_, index_meta_, &block_iterator_);
  if (!status_.ok()) return; // something went wrong
  char key[100];
  snprintf(key, sizeof(key), config::key_format, btree_iterator_->key());
  block_iterator_->Seek(key);
  return;
//  char buf[27];
//  snprintf(buf, sizeof(buf), "%06d%010d%010d", index_meta_->file_number, index_meta_->size, index_meta_->offset);
//  Slice cache_key(buf, sizeof(buf));
//  handle_ = cache_->Lookup(cache_key);
//  if (handle_ == nullptr) {
//    status_ = table_cache_->GetBlockIterator(options_, index_meta_, &block_iterator_);
//    if (!status_.ok()) return; // something went wrong
//    char key[100];
//    snprintf(key, sizeof(key), config::key_format, btree_iterator_->key());
//#ifdef PERF_LOG
//    uint64_t start_micros = benchmark::NowMicros();
//    block_iterator_->Seek(key);
//    benchmark::LogMicros(benchmark::QUERY_VALUE, benchmark::NowMicros() - start_micros);
//#else
//    block_iterator_->Seek(key);
//#endif
//    handle_ = cache_->Insert(cache_key, block_iterator_, 1,&DeleteIterator);
//  } else {
//    block_iterator_ = reinterpret_cast<Iterator*>(cache_->Value(handle_));
//  }
}

void IndexIterator::Advance() {
  index_meta_ = (IndexMeta*)btree_iterator_->value();
  CacheLookup();
}

}
