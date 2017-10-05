#ifndef STORAGE_LEVELDB_DB_RAW_TABLE_BUILDER_H_
#define STORAGE_LEVELDB_DB_RAW_TABLE_BUILDER_H_

#include <leveldb/env.h>
#include <leveldb/options.h>

namespace leveldb {

class RawTableBuilder {
 public:
  RawTableBuilder(const Options& options, WritableFile* file, FileMetaData* meta);

  ~RawTableBuilder();

  void Add(const Slice& key, const Slice& value);

  void Flush();

  Status status() const;

  Status Finish();

  void Abandon();

  uint64_t FileSize();

 private:
  bool ok() const { return status().ok(); }

  struct Rep;
  Rep* rep_;

};

} // namespace leveldb


#endif // STORAGE_LEVELDB_DB_RAW_TABLE_BUILDER_H
