#ifndef STORAGE_LEVELDB_DB_RAW_BLOCK_ITER_H
#define STORAGE_LEVELDB_DB_RAW_BLOCK_ITER_H

#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "util/streamer.h"

namespace leveldb {

class RawBlockIterator : public Iterator {
 public:
  RawBlockIterator(SequentialFile* file);

  ~RawBlockIterator() { delete stream_; }

  void Init();

  bool Valid() const;

  void SeekToFirst();

  void SeekToLast();

  void Seek(const Slice& target);

  void Next();

  void Prev();

  Slice key() const;

  Slice value() const;

  Status status() const;

 private:
  Streamer* stream_;
  std::vector<std::pair<Slice, Slice>> vector_;
  uint64_t count;
};

} // namespace leveldb

#endif //STORAGE_LEVELDB_DB_RAW_BLOCK_ITER_H
