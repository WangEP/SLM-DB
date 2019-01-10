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

IndexMeta* BtreeIndex::Get(const Slice& key) {
  IndexMeta* result = (IndexMeta*)tree_.Search(fast_atoi(key));
  return result;
}

void BtreeIndex::Insert(const entry_key_t& key, const IndexMeta& meta) {
  edit_->AddToRecoveryList(meta.file_number);
  // check btree if updated
  IndexMeta* ptr = (IndexMeta*) nvram::pmalloc(sizeof(IndexMeta));
  ptr->size = meta.size;
  ptr->file_number = meta.file_number;
  ptr->offset = meta.offset;
  clflush((char*)ptr, sizeof(IndexMeta));
  IndexMeta* old_ptr = (IndexMeta*) tree_.Insert(key, ptr);
  if (old_ptr != nullptr) {
    edit_->DecreaseCount(old_ptr->file_number);
    nvram::pfree(old_ptr);
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
    assert(!queue_.empty());
    for (;!queue_.empty();) {
      uint64_t key = queue_.front().key;
      std::shared_ptr<IndexMeta> value = queue_.front().meta;
      queue_.pop_front();
      Insert(key, *value);
    }
    if (edit_ != nullptr) edit_->Unref();
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
  if (edit == nullptr) return;
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

Iterator* BtreeIndex::NewIterator(const ReadOptions& options, TableCache* table_cache, VersionControl* vcontrol) {
  return new IndexIterator(options, tree_.GetIterator(), table_cache, vcontrol);
}

FFBtreeIterator* BtreeIndex::BtreeIterator() {
  return tree_.GetIterator();
}

void BtreeIndex::Break() {
  pthread_cancel(thread_);
}


} // namespace leveldb