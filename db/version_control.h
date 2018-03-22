#ifndef STORAGE_LEVELDB_DB_VERSION_CONTROL_H_
#define STORAGE_LEVELDB_DB_VERSION_CONTROL_H_

#include "zero_level_version.h"
#include "zero_level_version_edit.h"
#include "port/port_posix.h"

namespace leveldb {

class VersionControl {
 public:

  ZeroLevelVersion* current() { return current_; }
  ZeroLevelVersion* next() { return next_; }
  TableCache* cache() { return cache_; }
  const Options* const options() { return options_; }
  const Comparator* user_comparator() const { return icmp_.user_comparator(); }
  const Comparator* internal_comparator() const { return &icmp_;}

  Status LogAndApply(ZeroLevelVersion* new_, port::Mutex* mu);

  uint64_t ManifestFileNumber() const { return manifest_file_number_; }
  uint64_t NewFileNumber() { return next_file_number_++; }
  uint64_t LogNumber() const { return log_number_; }
  uint64_t PrevLogNumber() const { return prev_log_number_; }
  uint64_t LastSequence() const { return last_sequence_; }

  void ReuseFileNumber(uint64_t file_number) {
    if (next_file_number_ == file_number + 1) {
      next_file_number_ = file_number;
    }
  }

  uint64_t NumFiles() { return current()->NumFiles(); }
  uint64_t NumBytes() { return current()->NumBytes(); }

 private:
  Env* const env_;
  const std::string dbname_;

  uint64_t next_file_number_;
  uint64_t manifest_file_number_;
  uint64_t last_sequence_;
  uint64_t log_number_;
  uint64_t prev_log_number_;

  const InternalKeyComparator icmp_;
  const Options* const options_;
  WritableFile* descriptor_file_;
  log::Writer* descriptor_log_;
  ZeroLevelVersion* current_;
  ZeroLevelVersion* next_;
  ZeroLevelVersionEdit* edit_;
  TableCache* cache_;

  // no copy
  VersionControl(const VersionControl&);
  void operator=(const VersionControl&);
};

} // namespace leveldb

#endif // STORAGE_LEVELDB_DB_VERSION_CONTROL_H_
