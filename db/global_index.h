#ifndef STORAGE_LEVELDB_DB_GLOBAL_INDEX_H
#define STORAGE_LEVELDB_DB_GLOBAL_INDEX_H

#include <cstdint>
#include "nvm_btree.h"

namespace leveldb {

class GlobalIndex {
 public:
  GlobalIndex() {}

  void Get(const uint64_t&, void*);

  void Add(const uint64_t&, const void*);

  void Update(const uint64_t&, void*);

  void Delete(uint64_t);

  void Range(uint64_t, uint64_t);

 private:
  BTree tree;
};

void GlobalIndex::Get(const uint64_t& key, void* offset) {
  tree.search(key, offset);
}

void GlobalIndex::Add(const uint64_t& key, const void* offset) {
  tree.insert(key, (void *) offset);
}

void GlobalIndex::Delete(uint64_t) {

}

void GlobalIndex::Range(uint64_t, uint64_t) {

}

void GlobalIndex::Update(const uint64_t &, void*) {

}

} // namespace leveldb

#endif //STORAGE_LEVELDB_DB_GLOBAL_INDEX_H
