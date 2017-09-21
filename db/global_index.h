#ifndef STORAGE_LEVELDB_DB_GLOBAL_INDEX_H
#define STORAGE_LEVELDB_DB_GLOBAL_INDEX_H

#include <cstdint>

namespace leveldb {

class GlobalIndex {
 public:
  GlobalIndex() {}

  virtual void Get(uint64_t, void*) = 0;

  virtual void Add(uint64_t, void*) = 0;

  virtual void Delete(uint64_t) = 0;

  virtual void Range(uint64_t, uint64_t) = 0;
};

} // namespace leveldb

#endif //STORAGE_LEVELDB_DB_GLOBAL_INDEX_H
