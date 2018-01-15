#ifndef STORAGE_LEVELDB_DB_PERSISTENT_MEMTABLE_H_
#define STORAGE_LEVELDB_DB_PERSISTENT_MEMTABLE_H_

#include <assert.h>
#include "leveldb/comparator.h"
#include "leveldb/iterator.h"
#include "persistent_skiplist.h"

namespace leveldb {

class PersistentMemtable {
 public:
  explicit PersistentMemtable(const Comparator& cmp);

  void Ref() { ++refs_; }

  void Unref() {
    --refs_;
    assert(refs_ >= 0);
    if (refs_ <= 0) {
      delete this;
    }
  }

  size_t ApproximateMemoryUsage();

  Iterator* NewIterator();

  void Add(const Slice& key, const Slice& value);

  bool Get(const Slice& key, std::string* value);

 private:
  ~PersistentMemtable();

  PersistentSkiplist table_;
  const Comparator cmp_;
  int refs_;

  // no copy allowed
  PersistentMemtable(const PersistentMemtable&);
  void operator=(const PersistentMemtable&);
};

} // namespace leveldb

#endif // STORAGE_LEVELDB_DB_PERSISTENT_MEMTABLE_H_
