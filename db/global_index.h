#ifndef STORAGE_LEVELDB_DB_GLOBAL_INDEX_H
#define STORAGE_LEVELDB_DB_GLOBAL_INDEX_H

#include <cstdint>
#include <map>
#include "nvm_btree.h"

namespace leveldb {

// TODO: concurrency control


class GlobalIndex {
 public:
  GlobalIndex() {}

  const Slice& Get(const std::string&);

  void Add(const std::string&, const char*);

  void Add(const std::string&, const Slice&);

  void Update(const std::string&, const Slice&);

  void Delete(const std::string&);

  void Range(const std::string&, const std::string&);

 private:
  //BTree tree;
  std::map<std::string, void*> tree_; // temporary
  Arena arena_;
};

const Slice& GlobalIndex::Get(const std::string& key) {
  const char *p = (const char *) tree_.find(key)->second;
  return Slice(p);
}

void GlobalIndex::Add(const std::string& key, const char* value) {
  tree_.insert({key, value});
}

void GlobalIndex::Add(const std::string& key, const Slice& value) {
  char* buf = arena_.AllocateAligned(sizeof(Slice));
  size_t size = value.size();
  memcpy(buf, value.data(), size);
  clflush(buf, size);
  tree_.insert({key, buf});
}

void GlobalIndex::Update(const std::string &key, const Slice& value) {
  char* buf = arena_.AllocateAligned(sizeof(Slice));
  size_t size = value.size();
  memcpy(buf, value.data(), size);
  clflush(buf, size);
  tree_.insert({key, buf});
}

void GlobalIndex::Delete(const std::string&) {
}

void GlobalIndex::Range(const std::string&, const std::string&) {
}


} // namespace leveldb

#endif //STORAGE_LEVELDB_DB_GLOBAL_INDEX_H
