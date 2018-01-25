#ifndef LEVELDB_RANGE_ITERATOR_H
#define LEVELDB_RANGE_ITERATOR_H

#include "leveldb/iterator.h"

namespace leveldb {

class RangeIterator : public Iterator {
  RangeIterator();
  ~RangeIterator();

  virtual bool Valid() const;
  virtual void SeekToFirst();
  virtual void SeekToLast();
  virtual void Seek(const Slice& target);
  virtual void Next();
  virtual void Prev();
  virtual Slice key() const;
  virtual Slice value() const;
  virtual Status status() const;
};

} // namespace leveldb

#endif //LEVELDB_RANGE_ITERATOR_H
