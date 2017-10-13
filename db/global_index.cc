#include "include/leveldb/global_index.h"

namespace leveldb {

GlobalIndex::GlobalIndex() {
  tree_ = new BTree();
}

const DataMeta* GlobalIndex::Get(const std::string& key) {
  uint32_t hash = (uint32_t) std::hash<std::string>{}(key);
  void *p = tree_->search(hash);
  return (const DataMeta *) p;
}

void GlobalIndex::Add(const std::string& key, const uint64_t& offset, const uint64_t& size, void* file_meta) {
  DataMeta *meta = new DataMeta;
  meta->offset = offset;
  meta->size = size;
  meta->file_meta = file_meta;
  clflush((char *) meta, sizeof(DataMeta));
  uint32_t hash = (uint32_t) std::hash<std::string>{}(key);
  tree_->insert(hash, meta);
}

void GlobalIndex::Update(const std::string& key, const uint64_t& offset, const uint64_t& size, void* file_meta) {
  DataMeta *meta = new DataMeta;
  meta->offset = offset;
  meta->size = size;
  meta->file_meta = file_meta;
  clflush((char *) meta, sizeof(DataMeta));
  uint32_t hash = (uint32_t) std::hash<std::string>{}(key);
  tree_->update(hash, meta);
}

void GlobalIndex::Delete(const std::string&) {
}

void GlobalIndex::Range(const std::string&, const std::string&) {
}

} // namespace leveldb