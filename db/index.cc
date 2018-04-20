#include <cstdlib>
#include "util/coding.h"
#include "leveldb/slice.h"
#include "leveldb/index.h"
#include "db/index_iterator.h"
#include "zero_level_version_edit.h"

namespace leveldb {

Index::Index()
    : condvar_(port::CondVar(&mutex_))  {
  free_ = true;
  bgstarted_ = false;
}

const IndexMeta* Index::Get(const Slice& key) {
  auto result = tree_.search(Key(key.data(), key.size()));
  return reinterpret_cast<const IndexMeta *>(result);
}

void Index::Insert(const Key& key, IndexMeta* meta) {
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
  std::vector<LeafEntry*> entries = tree_.range(Key(begin.data(), begin.size()), Key(end.data(), end.size()));
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
    edit_->AllocateRecoveryList(queue_.size());
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