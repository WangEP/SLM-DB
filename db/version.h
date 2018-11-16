#ifndef STORAGE_LEVELDB_DB_ZERO_LEVEL_VERSION_H_
#define STORAGE_LEVELDB_DB_ZERO_LEVEL_VERSION_H_

#include <memory>
#include <cstdint>
#include <map>
#include <set>
#include <utility>
#include "leveldb/env.h"
#include "log_writer.h"
#include "table_cache.h"

namespace leveldb {

class VersionControl;

struct FileMetaData {
  uint64_t number;
  uint32_t allowed_seeks;          // Seeks allowed until compaction
  uint64_t file_size;         // File size in bytes
  uint64_t total;             // Total count of keys
  uint64_t alive;             // Count of live keys
  InternalKey smallest;       // Smallest internal key served by table
  InternalKey largest;        // Largest internal key served by table

  FileMetaData() : file_size(0), total(0), alive(0) { }
  FileMetaData(uint64_t number_, uint64_t file_size_,
               uint64_t total_, uint64_t alive_,
               InternalKey smallest_, InternalKey largest_)
      : number(number_), file_size(file_size_),
        total(total_), alive(alive_),
        smallest(std::move(smallest_)), largest(std::move(largest_)) { }
};

class Version {
 public:
  explicit Version(VersionControl* vcontrol)
      : vcontrol_(vcontrol), refs_(0), max_key_(0) { }

  Status Get(const ReadOptions&, const LookupKey& key, std::string* val, uint16_t*);

  void Ref();
  void Unref();

  void AddFile(std::shared_ptr<FileMetaData> f);
  void AddCompactionFile(std::shared_ptr<FileMetaData> f);

  uint64_t NumFiles() { return files_.size(); }
  uint64_t MergeNumFiles() { return merge_candidates_.size(); }
  uint64_t NumBytes() {
    uint64_t bytes = 0;
    for (auto f : files_) bytes += f.second->file_size;
    for (auto f : merge_candidates_) bytes += f.second->file_size;
    return bytes;
  }

  uint64_t GetFileSize(uint64_t file_number) {
    uint64_t size = 0;
    if (files_.count(file_number) > 0) {
      size = files_.at(file_number)->file_size;
    } else if (merge_candidates_.count(file_number) > 0) {
      size = merge_candidates_.at(file_number)->file_size;
    }
    return size;
  }

  bool MoveToMerge(std::set<uint16_t> array, bool is_scan);

  bool IsAlive(uint64_t fnumber) { return files_.count(fnumber) > 0 || merge_candidates_.count(fnumber) > 0; }

  std::string DebugString() const;

  friend class VersionControl;
 private:
  std::unordered_map<uint64_t, std::shared_ptr<FileMetaData>> files_;
  std::unordered_map<uint64_t, std::shared_ptr<FileMetaData>> merge_candidates_;
  VersionControl* vcontrol_;
  entry_key_t max_key_;
  int refs_;

  ~Version() = default;
  // no copy
  Version(const Version&);
  void operator=(const Version&);
};


}

#endif // STORAGE_LEVELDB_DB_ZERO_LEVEL_LSM_H
