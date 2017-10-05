#include "include/leveldb/global_index.h"
#if QUARTZ
#include "quartz/src/lib/pmalloc.h"
#endif


namespace leveldb {

const DataMeta* GlobalIndex::Get(const std::string& key) {
  void* p = tree_.find(key)->second;
  return (const DataMeta *) p;
}

void GlobalIndex::Add(const std::string& key, const uint64_t& offset, const uint64_t& size, void* meta ) {
  DataMeta *meta;
#if QUARTZ
  meta = (DataMeta *) pmalloc(sizeof(DataMeta));
#else
  meta = (DataMeta *) malloc(sizeof(DataMeta));
#endif
  meta->offset = offset;
  meta->size = size;
  meta->meta = meta;
  clflush((char *) meta, sizeof(DataMeta));
  tree_.insert({key, meta});
}


void GlobalIndex::Delete(const std::string&) {
}

void GlobalIndex::Range(const std::string&, const std::string&) {
}

} // namespace leveldb