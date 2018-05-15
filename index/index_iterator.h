#ifndef STORAGE_LEVELDB_INDEX_INDEX_ITERATOR_H_
#define STORAGE_LEVELDB_INDEX_INDEX_ITERATOR_H_

#include <vector>
#include "leveldb/iterator.h"
#include "leveldb/index.h"
#include "index/nvm_btree.h"
#include "table/format.h"
#include "db/version_control.h"
#include "db/table_cache.h"

namespace leveldb {

class IndexIterator : public Iterator {
 public:
  IndexIterator(std::vector<LeafEntry*> entries, void* ptr);
  ~IndexIterator();

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
  uint64_t file_number_;
  IndexMeta* index_ptr_;
  VersionControl* vset_;
  TableHandle* table_handle_;
  Iterator* block_iterator_;
//  int it = 0;

  void IndexChange();
};

}

#endif //STORAGE_LEVELDB_INDEX_INDEX_ITERATOR_H_
