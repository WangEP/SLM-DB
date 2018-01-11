#ifndef STORAGE_LEVELDB_DB_INDEX_H
#define STORAGE_LEVELDB_DB_INDEX_H

#include <cstdint>
#include <map>
#include <port/port.h>
#include <deque>
#include "db/nvm_btree.h"
#include "leveldb/env.h"

namespace leveldb {

struct IndexMeta {
  uint32_t offset;
  uint32_t size;
  uint32_t file_number;

  IndexMeta(uint32_t offset, uint32_t size, uint32_t file_number) :
      offset(offset), size(size), file_number(file_number) { }
};

class Index {
 public:
  Index();

  const IndexMeta* Get(const Slice& key);

  void Insert(const uint32_t& key, IndexMeta* meta);

  void Range(const std::string&, const std::string&);

  void AsyncInsert(const Slice &key, const uint32_t &offset,
                   const uint32_t &size, const uint32_t &file_number);

  bool Acceptable() {
    return queue_.empty() && free_;
  }

  void CompactionFinished() {
    free_ = true;
  }

  void CompactionStarted() {
    free_ = false;
  }

  void Runner();

  static void* ThreadWrapper(void* index);

 private:
  struct KeyAndMeta{
    uint32_t key;
    IndexMeta* meta;
  };

  BTree tree_; // Temporary
  bool bgstarted_;
  pthread_t thread_;
  port::Mutex* mutex_;
  port::CondVar* condvar_;
  bool free_;

  std::deque<KeyAndMeta> queue_;

  Index(const Index&);
  void operator=(const Index&);
};

} // namespace leveldb

#endif //STORAGE_LEVELDB_DB_GLOBAL_INDEX_H
