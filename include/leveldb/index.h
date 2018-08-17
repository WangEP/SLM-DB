#ifndef STORAGE_LEVELDB_INCLUDE_INDEX_H_
#define STORAGE_LEVELDB_INCLUDE_INDEX_H_

#include <cstdint>
#include <map>
#include <deque>
#include <shared_mutex>
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "leveldb/options.h"

namespace leveldb {

class TableCache;
class VersionEdit;
class FFBtree;

struct IndexMeta {
public:
  uint32_t offset;
  uint16_t size;
  uint16_t file_number;

  IndexMeta() : offset(0), size(0), file_number(0) { }

  IndexMeta(uint32_t offset, uint16_t size, uint16_t file_number) :
    offset(offset), size(size), file_number(file_number) { }
};

void* convert(IndexMeta meta);
IndexMeta convert(void* ptr);

struct KeyAndMeta{
  uint32_t key;
  std::shared_ptr<IndexMeta> meta;
};

class Index {
public:
  Index();

  IndexMeta Get(const Slice& key);

  void Insert(const uint32_t& key, IndexMeta meta);

  void AsyncInsert(const KeyAndMeta& key_and_meta);

  void AddQueue(std::deque<KeyAndMeta>& queue, VersionEdit* edit);

  Iterator* NewIterator(const ReadOptions& options, TableCache* table_cache);

  bool Acceptable() {
    return queue_.empty() && free_;
  }

  bool IsQueueEmpty() { return queue_.empty(); }

  void CompactionFinished() {
    free_ = true;
  }

  void CompactionStarted() {
    free_ = false;
  }

  void Runner();

  static void* ThreadWrapper(void* index);

private:

  FFBtree* tree_; // Temporary
  bool bgstarted_;
  pthread_t thread_;
  bool free_;

  std::deque<KeyAndMeta> queue_;
  VersionEdit* edit_;

  Index(const Index&);
  void operator=(const Index&);
};

} // namespace leveldb

#endif // STORAGE_LEVELDB_INCLUDE_INDEX_H_
