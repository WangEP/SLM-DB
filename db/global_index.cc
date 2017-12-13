#include "leveldb/slice.h"
#include "leveldb/global_index.h"
#include "port/port_posix.h"

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

void GlobalIndex::Insert(const Slice &key, const uint64_t &offset,
                         const uint64_t &size, const uint64_t &file_number) {
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

void GlobalIndex::Delete(const std::string&) {
}

void GlobalIndex::Range(const std::string&, const std::string&) {
}

void GlobalIndex::AsyncInsert(const Slice &key, const uint64_t &offset,
                              const uint64_t &size, const uint64_t &file_number) {
  mutex_->Lock();
  if (!bgstarted_) {
    bgstarted_ = true;
    port::PthreadCall("create thread", pthread_create(&thread_, NULL, &GlobalIndex::ThreadWrapper, this));
  }
  if (queue_.empty()) {
    condvar_->Signal();
  }
  queue_.push_back(IndexItem());
  queue_.back().key = key;
  queue_.back().meta.size = size;
  queue_.back().meta.file_number = file_number;
  queue_.back().meta.offset = offset;

  mutex_->Unlock();
}

void GlobalIndex::Runner() {
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wmissing-noreturn"
  for (;;) {
    mutex_->Lock();
    while(queue_.empty())
      condvar_->Wait();

    IndexItem item = queue_.front();
    queue_.pop_front();
    mutex_->Unlock();
    Insert(item.key, item.meta.offset, item.meta.size, item.meta.file_number);
  }
#pragma clang diagnostic pop
}

void* GlobalIndex::ThreadWrapper(void *index) {
  reinterpret_cast<GlobalIndex*>(index)->Runner();
  return NULL;
}

} // namespace leveldb