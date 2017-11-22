#ifndef STORAGE_LEVELDB_DB_GLOBAL_INDEX_H
#define STORAGE_LEVELDB_DB_GLOBAL_INDEX_H

#include <cstdint>
#include <map>
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
  GlobalIndex();

  const DataMeta* Get(const Slice& key);

  void Add(const Slice& key, const uint64_t& offset,
           const uint64_t& size, const uint64_t& file_number);

  void Update(const Slice& key , const uint64_t& offset,
              const uint64_t& size, const uint64_t& file_number);

  void Delete(const std::string&);

  void Range(const std::string&, const std::string&);

 private:
  BTree *tree_;
  //std::map<std::string, void*> tree_; // temporary

  GlobalIndex(const GlobalIndex&);
  void operator=(const GlobalIndex&);
};

} // namespace leveldb

#endif //STORAGE_LEVELDB_DB_GLOBAL_INDEX_H
