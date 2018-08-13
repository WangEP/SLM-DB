#ifndef STORAGE_LEVELDB_DB_ZERO_LEVEL_VERSION_EDIT_H_
#define STORAGE_LEVELDB_DB_ZERO_LEVEL_VERSION_EDIT_H_

#include <vector>
#include <cstdint>
#include <unordered_map>
#include <util/persist.h>
#include "dbformat.h"
#include "version.h"
#include "index/nvm_btree.h"

namespace leveldb {

class VersionEdit {
 public:
  VersionEdit() : signal_(&mutex_) { Clear(); };
  ~VersionEdit() = default;

  void Ref() { refs_++; };
  void Unref() {
    assert(refs_ > 0);
    refs_--;
    if (refs_ <= 0) {
      signal_.SignalAll();
    }
  };

  void Wait() {
    if (refs_ > 0) {
      signal_.Wait();
    }
  }

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

  bool HasComparatorName() { return has_comparator_; }
  bool HasLogNumber() { return has_log_number_; }
  bool HasPrevLogNumber() { return has_prev_log_number_; }
  bool HasNextFileNumber() { return has_next_file_number_; }
  bool HasLastSequence() { return has_last_sequence_; }

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

  void AddMergeCandidates(uint64_t file,
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
    merge_candidates_.push_back(f);
  }

  void AllocateRecoveryList(uint64_t size) {
    recovery_list_.reserve(size);
  }

  void AddToRecoveryList(uint64_t fnumber) {
    recovery_list_.push_back(fnumber);
    clflush((char*)&recovery_list_[recovery_list_.size()-1], sizeof(uint64_t));
  }

  void EncodeTo(std::string* dst) const;
  Status DecodeFrom(const Slice& src);
  std::string DebugString() const;

  friend class VersionControl;
 private:
  std::vector<FileMetaData> new_files_;
  std::vector<FileMetaData> merge_candidates_;
  std::vector<uint64_t> deleted_files_;
  std::unordered_map<uint64_t, uint64_t> dead_key_counter_;

  std::vector<uint64_t> recovery_list_;

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

  volatile uint64_t refs_;
  port::Mutex mutex_;
  port::CondVar signal_;
};

} // namespace leveldb

#endif // STORAGE_LEVELDB_DB_ZERO_LEVEL_VERSION_EDIT_H_
