#ifndef STORAGE_LEVELDB_DB_ZERO_LEVEL_VERSION_EDIT_H_
#define STORAGE_LEVELDB_DB_ZERO_LEVEL_VERSION_EDIT_H_

#include <vector>
#include <cstdint>
#include "dbformat.h"
#include "zero_level_version.h"

namespace leveldb {

class ZeroLevelVersionEdit {
 public:
  void DeleteFile(uint64_t file) { deleted_files_.push_back(file); }
  void AddFile(uint64_t file, uint64_t file_size, const InternalKey& smallest, const InternalKey& largest);
 private:
  std::vector<FileMetaData> new_files_;
  std::vector<uint64_t> deleted_files_;
};

} // namespace leveldb

#endif // STORAGE_LEVELDB_DB_ZERO_LEVEL_VERSION_EDIT_H_
