#include <include/leveldb/slice.h>
#include "include/leveldb/global_index.h"

namespace leveldb {

GlobalIndex::GlobalIndex() {
  tree_ = new BTree();
}

const DataMeta* GlobalIndex::Get(const Slice& key) {
  std::string s(key.data(), key.size());
  int64_t hash = std::hash<std::string>{}(s);
  hash = hash < 0 ? -hash : hash;
  void *p = tree_->search(hash);
  return (const DataMeta *) p;
}

void GlobalIndex::Add(const Slice& key, const uint64_t& offset,
                      const uint64_t& size, const uint64_t& file_number) {
  DataMeta *meta = new DataMeta;
  meta->offset = offset;
  meta->size = size;
  meta->file_number = file_number;
  clflush((char *) meta, sizeof(DataMeta));
  std::string s(key.data(), key.size());
  int64_t hash = std::hash<std::string>{}(s);
  hash = hash < 0 ? -hash : hash;
  tree_->insert(hash, meta);
}

void GlobalIndex::Update(const Slice& key, const uint64_t& offset,
                         const uint64_t& size, const uint64_t& file_number) {
  DataMeta *meta = new DataMeta;
  meta->offset = offset;
  meta->size = size;
  meta->file_number = file_number;
  clflush((char *) meta, sizeof(DataMeta));
  std::string s(key.data(), key.size());
  int64_t hash = std::hash<std::string>{}(s);
  hash = hash < 0 ? -hash : hash;
  tree_->update(hash, meta);
}

void GlobalIndex::Delete(const std::string&) {
}

void GlobalIndex::Range(const std::string&, const std::string&) {
}

} // namespace leveldb