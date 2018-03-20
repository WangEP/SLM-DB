#ifndef STORAGE_LEVELDB_DB_ZERO_LEVEL_LSM_H_
#define STORAGE_LEVELDB_DB_ZERO_LEVEL_LSM_H_

#include <cstdint>
#include <map>
#include <set>
#include "leveldb/env.h"
#include "log_writer.h"
#include "table_cache.h"
namespace leveldb {

struct FileMetaData {
  int refs;
  int allowed_seeks;          // Seeks allowed until compaction
  uint64_t number;
  uint64_t file_size;         // File size in bytes
  InternalKey smallest;       // Smallest internal key served by table
  InternalKey largest;        // Largest internal key served by table

  FileMetaData() : refs(0), allowed_seeks(1 << 30), file_size(0) { }
};

struct FileMetaDataCmp {
  FileMetaDataCmp(const Comparator& cmp) : cmp_(cmp) { }
  bool operator() (const FileMetaData& lhs, const FileMetaData& rhs) const {
    return cmp_.Compare(lhs.largest.Encode(), rhs.largest.Encode());
  }
  const Comparator cmp_;
};

class ZeroLevelVersion {
 public:
  std::map<uint64_t, FileMetaData*> CopyFiles();
  std::vector<FileMetaData*> CopyCompactionState();

  uint64_t NumFiles() { return files_.size() + to_compact_.size(); }
  uint64_t NumBytes() { return 0; }

 private:
  std::map<uint64_t, FileMetaData> files_;
  std::vector<FileMetaData> to_compact_;

  // no copy
  ZeroLevelVersion(const ZeroLevelVersion&);
  void operator=(const ZeroLevelVersion&);
};


}

#endif // STORAGE_LEVELDB_DB_ZERO_LEVEL_LSM_H
