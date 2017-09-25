#include "global_index.h"

namespace leveldb {

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