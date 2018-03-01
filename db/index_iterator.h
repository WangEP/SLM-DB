#ifndef STORAGE_LEVELDB_DB_INDEX_ITERATOR_H
#define STORAGE_LEVELDB_DB_INDEX_ITERATOR_H

#include <vector>
#include "leveldb/iterator.h"
#include "leveldb/index.h"
#include "db/nvm_btree.h"
#include "table/format.h"
#include "version_set.h"

namespace leveldb {

class IndexIterator : public Iterator {
 public:
  IndexIterator(std::vector<LeafEntry*> entries, void* vcurrent, uint64_t number);

  virtual bool Valid() const;
  virtual void SeekToFirst();
  virtual void SeekToLast();
  virtual void Seek(const Slice& target);
  virtual void Next();
  virtual void Prev();
  virtual Slice key() const;
  virtual Slice value() const;
  virtual Status status() const;

 private:
  std::vector<LeafEntry*> entries_;
  std::vector<LeafEntry*>::iterator iterator_;
  std::string key_;
  ReadOptions options_;
  uint64_t number_;
  IndexMeta* index_ptr;
  std::string* value_;
  Version* vcurrent_;

  void IndexChange();
};

}

#endif //STORAGE_LEVELDB_DB_INDEX_ITERATOR_H
