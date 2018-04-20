#ifndef STORAGE_LEVELDB_DB_RAW_BLOCK_ITER_H
#define STORAGE_LEVELDB_DB_RAW_BLOCK_ITER_H

#include <include/leveldb/options.h>
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "util/streamer.h"

namespace leveldb {

class LEVELDB_EXPORT RawBlockIterator : public Iterator{
 public:
  explicit RawBlockIterator(SequentialFile* file);

  ~RawBlockIterator();

  bool Valid() const;

  void SeekToFirst();

  void SeekToLast();

  void Seek(const Slice& target);

  void Next();

  void Prev();

  Slice key() const;

  Slice value() const;

  Status status() const;

  void SetFileNumber(uint64_t);

  uint64_t GetFileNumber();

 private:
  std::vector<std::pair<Slice, Slice>> vector_;
  std::vector<std::pair<Slice, Slice>>::iterator iterator_;
  Streamer stream_;
  uint64_t count;
  uint64_t fnumber;
};

} // namespace leveldb

#endif //STORAGE_LEVELDB_DB_RAW_BLOCK_ITER_H
