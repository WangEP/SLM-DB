#include <stdlib.h>
#include "util/coding.h"
#include "leveldb/slice.h"
#include "btree_index.h"
#include "index_iterator.h"
#include "table/format.h"

namespace leveldb {

BtreeIndex::BtreeIndex() : condvar_(&mutex_) {
  bgstarted_ = false;
}

IndexMeta BtreeIndex::Get(const Slice& key) {
  void* result = tree_.Search(fast_atoi(key.data(), key.size()));
  return convert(result);
}

void BtreeIndex::Insert(const uint32_t& key, IndexMeta meta) {
  edit_->AddToRecoveryList(meta.file_number);
  // TODO: check btree if updated
  void* old_meta = tree_.Insert(key, convert(meta));
  if (old_meta != nullptr) {
    edit_->DecreaseCount(convert(old_meta).file_number);
  }
}

void BtreeIndex::Runner() {
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
      uint32_t key = queue_.front().key;
      std::shared_ptr<IndexMeta> value = queue_.front().meta;
      queue_.pop_front();
      Insert(key, *value);
    }
    edit_->Unref();
    assert(queue_.empty());
    mutex_.Unlock();
  }
#pragma clang diagnostic pop
}

void* BtreeIndex::ThreadWrapper(void* ptr) {
  reinterpret_cast<BtreeIndex*>(ptr)->Runner();
  return NULL;
}
void BtreeIndex::AddQueue(std::deque<KeyAndMeta>& queue, VersionEdit* edit) {
  mutex_.Lock();
  assert(queue_.size() == 0);
  queue_.swap(queue);
  edit_ = edit;
  edit_->Ref();
  if (!bgstarted_) {
    bgstarted_ = true;
    port::PthreadCall("create thread", pthread_create(&thread_, NULL, &BtreeIndex::ThreadWrapper, this));
  }
  condvar_.Signal();
  mutex_.Unlock();
}

Iterator* BtreeIndex::NewIterator(const ReadOptions& options, TableCache* table_cache) {
  return new IndexIterator(options, tree_.GetIterator(), table_cache);
}

FFBtreeIterator* BtreeIndex::BtreeIterator() {
  return tree_.GetIterator();
}


} // namespace leveldb