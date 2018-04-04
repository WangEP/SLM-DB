#ifndef STORAGE_LEVELDB_DB_ZERO_LEVEL_LSM_H_
#define STORAGE_LEVELDB_DB_ZERO_LEVEL_LSM_H_

#include <cstdint>
#include <map>
#include <set>
#include "leveldb/env.h"
#include "log_writer.h"
#include "table_cache.h"
#include "version_control.h"
namespace leveldb {

struct FileMetaData {
  int refs;
  int allowed_seeks;          // Seeks allowed until compaction
  uint64_t number;
  uint64_t alive;             // Count of live keys
  uint64_t total;             // Total count of keys
  uint64_t file_size;         // File size in bytes
  InternalKey smallest;       // Smallest internal key served by table
  InternalKey largest;        // Largest internal key served by table

  FileMetaData() : refs(0), allowed_seeks(1 << 30), file_size(0) { }
};

class ZeroLevelVersion {
 public:
  ZeroLevelVersion(VersionControl* vcontrol)
      : vcontrol_(vcontrol), refs_(0) { }

  Status Get(const ReadOptions&, const LookupKey& key, std::string* val);

  void Ref();
  void Unref();

  void AddFile(FileMetaData* f);
  void AddCompactionFile(FileMetaData* f);
  std::map<uint64_t, FileMetaData*> GetFiles() { return files_; };

  uint64_t NumFiles() { return files_.size() + to_compact_.size(); }
  uint64_t NumBytes() {
    uint64_t bytes = 0;
    for (auto f : files_) bytes += f.second->file_size;
    for (auto f : to_compact_) bytes += f->file_size;
    return bytes;
  }

  std::string DebugString() const;

 private:
  std::map<uint64_t, FileMetaData*> files_;
  std::vector<FileMetaData*> to_compact_;
  VersionControl* vcontrol_;
  int refs_;

  ~ZeroLevelVersion();
  // no copy
  ZeroLevelVersion(const ZeroLevelVersion&);
  void operator=(const ZeroLevelVersion&);
};


}

#endif // STORAGE_LEVELDB_DB_ZERO_LEVEL_LSM_H
