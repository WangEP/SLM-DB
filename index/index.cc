#include <cstdlib>
#include "util/coding.h"
#include "leveldb/slice.h"
#include "leveldb/index.h"
#include "index_iterator.h"
#include "db/zero_level_version_edit.h"

namespace leveldb {

Index::Index()
    : condvar_(port::CondVar(&mutex_))  {
  free_ = true;
  bgstarted_ = false;
}

const IndexMeta* Index::Get(const Slice& key) {
  int64_t k = fast_atoi(key.data(), key.size());
  auto result = tree_.search(k);
  return reinterpret_cast<const IndexMeta *>(result);
}

void Index::Insert(const int64_t& key, IndexMeta* meta) {
  IndexMeta* m = meta;
  clflush((char *) m, sizeof(IndexMeta));
  edit_->AddToRecoveryList(m->file_number);
  void* ptr = tree_.insert(key, m);
  if (ptr != nullptr) {
    IndexMeta* old_m = reinterpret_cast<IndexMeta*>(ptr);
    edit_->DecreaseCount(old_m->file_number);
    old_m->Unref();
  }
}

Iterator* Index::Range(const Slice& begin, const Slice& end, void* ptr) {
  int64_t k1 = fast_atoi(begin.data(), begin.size());
  int64_t k2 = fast_atoi(end.data(), end.size());
  std::vector<LeafEntry*> entries = tree_.range(k1, k2);
  Iterator* iter = new IndexIterator(entries, ptr);
  return iter;
}

void Index::AsyncInsert(const KeyAndMeta& key_and_meta) {
  mutex_.Lock();
  if (!bgstarted_) {
    bgstarted_ = true;
    port::PthreadCall("create thread", pthread_create(&thread_, nullptr, &Index::ThreadWrapper, this));
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
    edit_->AllocateRecoveryList(queue_.size(), is_numa);
    assert(!queue_.empty());
    for (;!queue_.empty();) {
      auto key = queue_.front().key;
      auto value = queue_.front().meta;
      queue_.pop_front();
      Insert(key, value);
    }
    edit_->Unref();
    assert(queue_.empty());
    mutex_.Unlock();
  }
#pragma clang diagnostic pop
}

void* Index::ThreadWrapper(void* index) {
  reinterpret_cast<Index*>(index)->Runner();
  return nullptr;
}
void Index::AddQueue(std::deque<KeyAndMeta>& queue, ZeroLevelVersionEdit* edit) {
  mutex_.Lock();
  assert(queue_.empty());
  queue_.swap(queue);
  edit_ = edit;
  if (!bgstarted_) {
    bgstarted_ = true;
    port::PthreadCall("create thread", pthread_create(&thread_, nullptr, &Index::ThreadWrapper, this));
  }
  condvar_.Signal();
  mutex_.Unlock();
}

} // namespace leveldb