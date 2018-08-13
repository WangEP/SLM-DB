#include <stdlib.h>
#include "util/coding.h"
#include "leveldb/slice.h"
#include "leveldb/index.h"
#include "index_iterator.h"
#include "db/table_cache.h"
#include "db/version_edit.h"

namespace leveldb {

void* convert(IndexMeta meta) {
  uint64_t t = 0;
  t += meta.offset;
  t = t << 16;
  t += meta.size;
  t = t << 16;
  t += meta.file_number;
  return (void*) t;
}

IndexMeta convert(void* ptr) {
  uint64_t t = (uint64_t) ptr;
  IndexMeta meta;
  meta.file_number = t % (1 << 17);
  meta.size = (t >> 16) % (1 << 17);
  meta.offset = t >> 32;
  return meta;
}

Index::Index()
  : condvar_(port::CondVar(&mutex_))  {
  free_ = true;
  bgstarted_ = false;
}

IndexMeta Index::Get(const Slice& key) {
  void* result = tree_.Search(fast_atoi(key.data(), key.size()));
  return convert(result);
}

void Index::Insert(const uint32_t& key, IndexMeta meta) {
  edit_->AddToRecoveryList(meta.file_number);
  // TODO: check btree if updated
  void* old_meta = tree_.Insert(key, convert(meta));
  if (old_meta != nullptr) {
    edit_->DecreaseCount(convert(old_meta).file_number);
  }
}

void Index::AsyncInsert(const KeyAndMeta& key_and_meta) {
  mutex_.Lock();
  if (!bgstarted_) {
    bgstarted_ = true;
    port::PthreadCall("create thread", pthread_create(&thread_, NULL, &Index::ThreadWrapper, this));
  }
  if (queue_.empty()) {
    condvar_.Signal();
  }
  queue_.push_back(key_and_meta);
  mutex_.Unlock();
}

void Index::Runner() {
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wmissing-noreturn"
  for (;;) {
    mutex_.Lock();
    for (;queue_.empty();) {
      condvar_.Wait();
    }
    edit_->AllocateRecoveryList(queue_.size());
    assert(queue_.size() > 0);
    for (;!queue_.empty();) {
      auto key = queue_.front().key;
      auto value = queue_.front().meta;
      queue_.pop_front();
      Insert(key, *value);
    }
    edit_->Unref();
    assert(queue_.empty());
    mutex_.Unlock();
  }
#pragma clang diagnostic pop
}

void* Index::ThreadWrapper(void* index) {
  reinterpret_cast<Index*>(index)->Runner();
  return NULL;
}
void Index::AddQueue(std::deque<KeyAndMeta>& queue, VersionEdit* edit) {
  mutex_.Lock();
  assert(queue_.size() == 0);
  queue_.swap(queue);
  edit_ = edit;
  edit_->Ref();
  if (!bgstarted_) {
    bgstarted_ = true;
    port::PthreadCall("create thread", pthread_create(&thread_, NULL, &Index::ThreadWrapper, this));
  }
  condvar_.Signal();
  mutex_.Unlock();
}

Iterator* Index::NewIterator(const ReadOptions& options, TableCache* table_cache) {
  return new IndexIterator(options, tree_.GetIterator(), table_cache);
}

} // namespace leveldb