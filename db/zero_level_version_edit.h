#ifndef STORAGE_LEVELDB_DB_ZERO_LEVEL_VERSION_EDIT_H_
#define STORAGE_LEVELDB_DB_ZERO_LEVEL_VERSION_EDIT_H_

#include <vector>
#include <cstdint>
#include <unordered_map>
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

  std::string GetComparatorName() const { return comparator_; }
  uint64_t GetLogNumber() { return log_number_; }
  uint64_t GetPrevLogNumber() { return prev_log_number_; }
  uint64_t GetNextFile() { return next_file_number_; }
  uint64_t GetLastSequence() { return last_sequence_; }

  bool HasCompartorName() { return has_comparator_; }
  bool HasLogNumber() { return has_log_number_; }
  bool HasPrevLogNumber() { return has_prev_log_number_; }
  bool HasNextFileNumber() { return has_next_file_number_; }
  bool HasLastSequence() { return has_last_sequence_; }

  std::vector<uint64_t> GetDeletedFiles() const { return deleted_files_; }
  std::vector<FileMetaData> GetNewFiles() const { return new_files_; }
  std::unordered_map<uint64_t, uint64_t> GetDeadKeyCounter() const { return dead_key_counter_; };

  void DecreaseCount(uint64_t fnumber) {
    if (dead_key_counter_.find(fnumber) != dead_key_counter_.end()) {
      dead_key_counter_[fnumber]++;
    } else {
      dead_key_counter_[fnumber] = 1;
    }
  }

  void DeleteFile(uint64_t file) {
    deleted_files_.push_back(file);
  }

  void AddFile(uint64_t file,
               uint64_t file_size,
               uint64_t total,
               uint64_t alive,
               const InternalKey& smallest,
               const InternalKey& largest) {
    FileMetaData f;
    f.number = file;
    f.file_size = file_size;
    f.total = total;
    f.alive = alive;
    f.smallest = smallest;
    f.largest = largest;
    new_files_.push_back(f);
  }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(const Slice& src);

  std::string DebugString() const;
 private:
  std::vector<FileMetaData> new_files_;
  std::vector<uint64_t> deleted_files_;
  std::unordered_map<uint64_t, uint64_t> dead_key_counter_;

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
