#ifndef STORAGE_LEVELDB_DB_INDEX_ITERATOR_H
#define STORAGE_LEVELDB_DB_INDEX_ITERATOR_H

#include <vector>
#include "leveldb/iterator.h"
#include "db/nvm_btree.h"
#include "table/format.h"

namespace leveldb {

class IndexIterator : public Iterator {
 public:
  IndexIterator(std::vector<LeafEntry*> entries);

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
  BlockHandle block_handle_;
};

}

#endif //STORAGE_LEVELDB_DB_INDEX_ITERATOR_H
