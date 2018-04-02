#ifndef STORAGE_LEVELDB_DB_ZERO_LEVEL_VERSION_EDIT_H_
#define STORAGE_LEVELDB_DB_ZERO_LEVEL_VERSION_EDIT_H_

#include <vector>
#include <cstdint>
#include "dbformat.h"
#include "zero_level_version.h"

namespace leveldb {

class ZeroLevelVersionEdit {
 public:
  ZeroLevelVersionEdit() { Clear(); };
  ~ZeroLevelVersionEdit() { };

  void Clear();

  void SetComparatorName(const Slice&);
  void SetLogNumber(uint64_t);
  void SetPrevLogNumber(uint64_t);
  void SetNextFile(uint64_t);
  void SetLastSequence(uint64_t);

  Slice GetComparatorName() { return comparator_; }
  uint64_t GetLogNumber() { return log_number_; }
  uint64_t GetPrevLogNumber() { return prev_log_number_; }
  uint64_t GetNextFile() { return next_file_number_; }
  uint64_t GetLastSequence() { return last_sequence_; }

  bool HasCompartorName() { return has_comparator_; }
  bool HasLogNumber() { return has_log_number_; }
  bool HasPrevLogNumber() { return has_prev_log_number_; }
  bool HasNextFile() { return has_next_file_number_; }
  bool HasLastSequence() { return has_last_sequence_; }

  std::set<uint64_t> GetDeletedFiles() { return deleted_files_; }
  std::vector<FileMetaData> GetNewFiles() { return new_files_; }

  void DeleteFile(uint64_t file) {
    deleted_files_.insert(file);
  }

  void AddFile(uint64_t file,
               uint64_t file_size,
               const InternalKey& smallest,
               const InternalKey& largest) {
    FileMetaData f;
    f.number = file;
    f.file_size = file_size;
    f.smallest = smallest;
    f.largest = largest;
    new_files_.push_back(f);
  }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(const Slice& src);

  std::string DebugString() const;
 private:
  std::vector<FileMetaData> new_files_;
  std::set<uint64_t> deleted_files_;

  std::string comparator_;
  uint64_t log_number_;
  uint64_t prev_log_number_;
  uint64_t next_file_number_;
  SequenceNumber last_sequence_;
  bool has_comparator_;
  bool has_log_number_;
  bool has_prev_log_number_;
  bool has_next_file_number_;
  bool has_last_sequence_;
};

} // namespace leveldb

#endif // STORAGE_LEVELDB_DB_ZERO_LEVEL_VERSION_EDIT_H_
