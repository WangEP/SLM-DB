#ifndef STORAGE_LEVELDB_DB_PERSISTENT_MEMTABLE_H_
#define STORAGE_LEVELDB_DB_PERSISTENT_MEMTABLE_H_

#include <assert.h>
#include "persistent_skiplist.h"
#include "port/port_posix.h"

namespace leveldb {

class MemtableIterator;

class PersistentMemtable {
 public:
  explicit PersistentMemtable(const Comparator* cmp);

  explicit PersistentMemtable(const Comparator* cmp,
                              PersistentSkiplist::Node* begin,
                              PersistentSkiplist::Node* end,
                              size_t size);

  void Ref() { ++refs_; }

  void Unref() {
    --refs_;
    assert(refs_ >= 0);
    if (refs_ <= 0) {
      delete this;
    }
  }

  size_t ApproximateMemoryUsage() { return table_->ApproximateMemoryUsage(); }

  Iterator* NewIterator();

  void Add(const Slice& key, const Slice& value);

  bool Get(const Slice& key, std::string* value);

  std::pair<const Slice&, const Slice&> GetRange();

 private:
  PersistentSkiplist* table_;
  const Comparator* cmp_;
  int refs_;

  ~PersistentMemtable();
  // no copy allowed
  PersistentMemtable(const PersistentMemtable&);
  void operator=(const PersistentMemtable&);
};

class MemtableIterator : public Iterator {
 public:
  MemtableIterator(PersistentSkiplist* list, PersistentSkiplist::Node* node)
      : list_(list), node_(node) { }

  ~MemtableIterator() { }

  bool Valid() const  { return node_ != list_->Tail() && node_ != list_->Head(); }

  void SeekToFirst() { node_ = list_->Head(); }

  void SeekToLast() { node_ = list_->Tail(); }

  void Seek(const Slice& target) { node_ = list_->Find(target); }

  void Next() { assert(node_->next[0] != NULL); node_ = node_->next[0]; }

  void Prev() { assert(node_->prev[0] != NULL); node_ = node_->prev[0]; }

  Slice key() const { return node_->key; }

  Slice value() const { return node_->value; }

  Status status() const { return Status::OK(); }

  size_t GetSize() const { return node_->GetSize(); }

  PersistentSkiplist::Node* GetNode() const { return node_; }

 private:
  PersistentSkiplist::Node* node_;
  PersistentSkiplist* list_;
};

} // namespace leveldb

#endif // STORAGE_LEVELDB_DB_PERSISTENT_MEMTABLE_H_
