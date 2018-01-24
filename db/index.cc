#include <stdlib.h>
#include "util/fast_atoi.h"
#include "leveldb/slice.h"
#include "leveldb/index.h"

namespace leveldb {

Index::Index() {
  mutex_ = new port::Mutex;
  condvar_ = new port::CondVar(mutex_);
  free_ = true;
  bgstarted_ = false;
}

const IndexMeta* Index::Get(const Slice& key) {
  auto result = tree_.search(fast_atoi(key.data()));
  return reinterpret_cast<const IndexMeta *>(result);
}

void Index::Insert(const uint32_t& key, IndexMeta* meta) {
  IndexMeta* m = meta;
  clflush((char *) m, sizeof(IndexMeta));
  clflush((char *) &key, sizeof(uint32_t));
  tree_.insert(key, m);
}

void Index::Update(const uint32_t& key, const uint32_t& fnumber, IndexMeta* meta) {
  IndexMeta* m = meta;
  clflush((char *) m, sizeof(IndexMeta));
  clflush((char *) &key, sizeof(uint32_t));
  tree_.update(key, fnumber, meta);
}

void Index::Range(const std::string&, const std::string&) {
}

void Index::AsyncInsert(const KeyAndMeta& key_and_meta) {
  mutex_->Lock();
  if (!bgstarted_) {
    bgstarted_ = true;
    port::PthreadCall("create thread", pthread_create(&thread_, NULL, &Index::ThreadWrapper, this));
  }
  if (queue_.empty()) {
    condvar_->Signal();
  }
  queue_.push_back(key_and_meta);
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
    auto key = queue_.front().key;
    auto fnumber = queue_.front().fnumber;
    auto value = queue_.front().meta;
    queue_.pop_front();
    mutex_->Unlock();
    if (fnumber == 0)
      Insert(key, value);
    else
      Update(key, fnumber, value);
  }
#pragma clang diagnostic pop
}

void* Index::ThreadWrapper(void *index) {
  reinterpret_cast<Index*>(index)->Runner();
  return NULL;
}
void Index::AddQueue(std::deque<KeyAndMeta>& queue) {
  mutex_->Lock();
  queue_.swap(queue);
  if (!bgstarted_) {
    bgstarted_ = true;
    port::PthreadCall("create thread", pthread_create(&thread_, NULL, &Index::ThreadWrapper, this));
  }
  condvar_->Signal();
  mutex_->Unlock();
}

} // namespace leveldb