#ifndef STORAGE_LEVELDB_DB_RAW_BUILDER_H_
#define STORAGE_LEVELDB_DB_RAW_BUILDER_H_

#include <stdint.h>
#include <leveldb/options.h>
#include <leveldb/slice.h>

namespace leveldb {

class RawBlockBuilder {
 public:
  RawBlockBuilder(const Options *options);

  void Reset();

  void Add(const Slice& key, const Slice& value);

  Slice Finish();

  size_t CurrentSizeEstimate() const;

  uint64_t GetBufferSize();


  bool empty() const {
    return buffer_.empty();
  }

 private:
  const Options* options_;
  std::string buffer_;
  bool finished_;

  RawBlockBuilder(const RawBlockBuilder&);
  void operator=(const RawBlockBuilder&);
};

} // namespace leveldb


#endif //STORAGE_LEVELDB_DB_RAW_BUILDER_H
