#ifndef STORAGE_LEVELDB_DB_INDEX_H
#define STORAGE_LEVELDB_DB_INDEX_H

#include <cstdint>
#include <map>
#include <deque>
#include "port/port.h"
#include "table/format.h"
#include "db/nvm_btree.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"

namespace leveldb {

class IndexMeta {
 private:
  ~IndexMeta() { }
  uint64_t refs;
 public:
  uint64_t file_number;
  BlockHandle handle;

  IndexMeta(uint32_t offset, uint32_t size, uint32_t file_number) :
      handle(size, offset), file_number(file_number), refs(0) { }

  void Ref() {
    ++refs;
  }

  void Unref() {
    if (--refs == 0)
      delete this;
  }

};

struct KeyAndMeta{
  uint32_t key;
  IndexMeta* meta;
};

class Index {
 public:
  Index();

  const IndexMeta* Get(const Slice& key);

  void Insert(const uint32_t& key, IndexMeta* meta);

  void Update(const uint32_t& key, const uint32_t& fnumber, IndexMeta* meta);

  Iterator* Range(const uint32_t& begin, const uint32_t& end, void* ptr);

  void AsyncInsert(const KeyAndMeta& key_and_meta);

  void AddQueue(std::deque<KeyAndMeta>& queue);

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

  BTree tree_; // Temporary
  bool bgstarted_;
  pthread_t thread_;
  port::Mutex mutex_;
  port::CondVar condvar_;
  bool free_;

  std::deque<KeyAndMeta> queue_;

  Index(const Index&);
  void operator=(const Index&);
};

} // namespace leveldb

#endif //STORAGE_LEVELDB_DB_GLOBAL_INDEX_H
