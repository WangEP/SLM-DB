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
  void* file; // NULL if in-memory
};

class GlobalIndex {
 public:
  GlobalIndex() {}

  const DataMeta* Get(const std::string&);

  void Add(const std::string&, const uint64_t&, const uint64_t&, void*);

  void Delete(const std::string&);

  void Range(const std::string&, const std::string&);

 private:
  //BTree tree;
  std::map<std::string, void*> tree_; // temporary

  GlobalIndex(const GlobalIndex&);
  void operator=(const GlobalIndex&);
};

} // namespace leveldb

#endif //STORAGE_LEVELDB_DB_GLOBAL_INDEX_H
