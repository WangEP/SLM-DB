#ifndef STORAGE_LEVELDB_DB_INDEX_H_
#define STORAGE_LEVELDB_DB_INDEX_H_

#include <cstdint>
#include <map>
#include <deque>
#include "port/port.h"
#include "table/format.h"
#include "db/nvm_btree.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"

namespace leveldb {

class ZeroLevelVersionEdit;

class IndexMeta {
 private:
  ~IndexMeta() = default;
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
  Key key;
  IndexMeta* meta;
};

class Index {
 public:
  Index();

  const IndexMeta* Get(const Slice& key);

  void Insert(const Key& key, IndexMeta* meta);

  Iterator* Range(const Slice& begin, const Slice& end, void* ptr);

  void AsyncInsert(const KeyAndMeta& key_and_meta);

  void AddQueue(std::deque<KeyAndMeta>& queue, ZeroLevelVersionEdit* edit);

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
  ZeroLevelVersionEdit* edit_;

  Index(const Index&);
  void operator=(const Index&);
};

} // namespace leveldb

#endif //STORAGE_LEVELDB_DB_GLOBAL_INDEX_H_
