#include "include/leveldb/global_index.h"
#if QUARTZ
#include "quartz/src/lib/pmalloc.h"
#endif


namespace leveldb {

GlobalIndex::GlobalIndex() {
  tree_ = new BTree();
}

const DataMeta* GlobalIndex::Get(const std::string& key) {
  size_t hash = std::hash<std::string>{}(key);
  void *p = tree_->search(hash);
  return (const DataMeta *) p;
}

void GlobalIndex::Add(const std::string& key, const uint64_t& offset, const uint64_t& size, void* file_meta) {
  DataMeta *meta;
#if QUARTZ
  meta = (DataMeta *) pmalloc(sizeof(DataMeta));
#else
  meta = (DataMeta *) malloc(sizeof(DataMeta));
#endif
  meta->offset = offset;
  meta->size = size;
  meta->file_meta = file_meta;
  clflush((char *) meta, sizeof(DataMeta));
  size_t hash = std::hash<std::string>{}(key);
  tree_->insert(hash, meta);
}


void GlobalIndex::Delete(const std::string&) {
}

void GlobalIndex::Range(const std::string&, const std::string&) {
}

} // namespace leveldb