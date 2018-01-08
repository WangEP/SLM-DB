#include "leveldb/slice.h"
#include "leveldb/index.h"
#include "port/port_posix.h"
#include "util/persist.h"

namespace leveldb {

Index::Index() {
  mutex_ = new port::Mutex;
  condvar_ = new port::CondVar(mutex_);
  free_ = true;
}

const IndexMeta* Index::Get(const Slice& key) {
  auto result = tree_.find(key.ToString());
  return (result != tree_.end()) ? result->second.get() : NULL;
}

void Index::Insert(const std::string& key, IndexMeta* meta) {
  IndexMeta* m = meta;
  clflush((char *) m, sizeof(IndexMeta));
  clflush((char *) &key, sizeof(std::string));
  tree_.insert_or_assign(std::string(key), std::make_unique<IndexMeta>(*m));
}

void Index::Range(const std::string&, const std::string&) {
}

void Index::AsyncInsert(const Slice& key, const uint32_t& offset,
                        const uint32_t& size, const uint32_t& file_number) {
  mutex_->Lock();
  if (!bgstarted_) {
    bgstarted_ = true;
    port::PthreadCall("create thread", pthread_create(&thread_, NULL, &Index::ThreadWrapper, this));
  }
  if (queue_.empty()) {
    condvar_->Signal();
  }
  queue_.push_back({key.ToString(), new IndexMeta(offset, size, file_number)});
  mutex_->Unlock();
}

void Index::Runner() {
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wmissing-noreturn"
  for (;;) {
    mutex_->Lock();
    while (queue_.empty()) {
      condvar_->Wait();
    }
    auto item = queue_.front();
    queue_.pop_front();
    mutex_->Unlock();
    Insert(item.first, item.second);
  }
#pragma clang diagnostic pop
}

void* Index::ThreadWrapper(void *index) {
  reinterpret_cast<Index*>(index)->Runner();
  return NULL;
}

} // namespace leveldb