#ifndef STORAGE_LEVELDB_DB_GLOBAL_INDEX_H
#define STORAGE_LEVELDB_DB_GLOBAL_INDEX_H

#include <cstdint>
#include <map>
#include <port/port.h>
#include <deque>
#include "leveldb/env.h"
#include "db/nvm_btree.h"

namespace leveldb {

// TODO: concurrency control

struct DataMeta {
  uint64_t offset;
  uint64_t size;
  uint64_t file_number; // NULL if in-memory
};

class GlobalIndex {
 public:
  struct IndexItem {
    Slice key;
    DataMeta meta;
  };

  GlobalIndex();

  const DataMeta* Get(const Slice& key);

  void Insert(const Slice &key, const uint64_t &offset,
              const uint64_t &size, const uint64_t &file_number);

  void Update(const Slice& key , const uint64_t& offset,
              const uint64_t& size, const uint64_t& file_number);

  void Delete(const std::string&);

  void Range(const std::string&, const std::string&);

  void SetEnv(Env* env) { env_ = env; }

  void AsyncInsert(const Slice &key, const uint64_t &offset,
                   const uint64_t &size, const uint64_t &file_number);

  void Runner();

  static void* ThreadWrapper(void* index);

 private:
  BTree *tree_;
  Env* env_;
  bool bgstarted_;
  pthread_t thread_;
  port::Mutex* mutex_;
  port::CondVar* condvar_;

  std::deque<IndexItem> queue_;

  GlobalIndex(const GlobalIndex&);
  void operator=(const GlobalIndex&);
};

} // namespace leveldb

#endif //STORAGE_LEVELDB_DB_GLOBAL_INDEX_H
