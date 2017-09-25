#ifndef STORAGE_LEVELDB_DB_GLOBAL_INDEX_H
#define STORAGE_LEVELDB_DB_GLOBAL_INDEX_H

#include <cstdint>
#include <map>
#include "nvm_btree.h"

namespace leveldb {

// TODO: concurrency control

struct DataMeta {
  uint64_t offset;
  uint64_t size;
  bool in_memory;
};

class GlobalIndex {
 public:
  GlobalIndex() {}

  const DataMeta* Get(const std::string&);

  void Add(const std::string&, const uint64_t&, const uint64_t&, bool);

  void Delete(const std::string&);

  void Range(const std::string&, const std::string&);

 private:
  //BTree tree;
  std::map<std::string, void*> tree_; // temporary
  Arena arena_;

  GlobalIndex(const GlobalIndex&);
  void operator=(const GlobalIndex&);
};

const DataMeta* GlobalIndex::Get(const std::string& key) {
  void* p = tree_.find(key)->second;
  return (const DataMeta *) p;
}

void GlobalIndex::Add(const std::string& key, const uint64_t& offset, const uint64_t& size, bool in_memory) {
  char* buf = arena_.AllocateAligned(sizeof(DataMeta));
  auto meta = (DataMeta*)buf;
  meta->offset = offset;
  meta->size = size;
  meta->in_memory = in_memory;
  clflush((char *) meta, sizeof(DataMeta));
  tree_.insert({key, meta});
}


void GlobalIndex::Delete(const std::string&) {
}

void GlobalIndex::Range(const std::string&, const std::string&) {
}



} // namespace leveldb

#endif //STORAGE_LEVELDB_DB_GLOBAL_INDEX_H
