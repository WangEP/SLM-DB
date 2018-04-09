#ifndef STORAGE_LEVELDB_DB_VERSION_CONTROL_H_
#define STORAGE_LEVELDB_DB_VERSION_CONTROL_H_

#include "zero_level_version.h"
#include "zero_level_version_edit.h"
#include "port/port_posix.h"

namespace leveldb {

class ZeroLevelCompaction;

class VersionControl {
 public:
  VersionControl(const std::string& dbname,
                 const Options* options,
                 TableCache* table_cache,
                 const InternalKeyComparator*);

  struct SummaryStorage {
    char buffer[100];
  };

  ZeroLevelVersion* current() { return current_; }
  ZeroLevelVersion* next_version() { return next_; }
  TableCache* cache() { return table_cache_; }
  const Options* const options() { return options_; }
  const Comparator* user_comparator() const { return icmp_.user_comparator(); }
  const Comparator* internal_comparator() const { return &icmp_;}

  Status LogAndApply(ZeroLevelVersionEdit* edit, port::Mutex* mu);
  ZeroLevelCompaction* PickCompaction();
  Status Recover(bool* save_manifest);
  Iterator* MakeInputIterator(ZeroLevelCompaction* c);
  const char* Summary(SummaryStorage* scratch) const;

  bool NeedsCompaction() const;

  uint64_t ManifestFileNumber() const { return manifest_file_number_; }
  uint64_t NewFileNumber() { return next_file_number_++; }
  uint64_t LogNumber() const { return log_number_; }
  uint64_t PrevLogNumber() const { return prev_log_number_; }
  uint64_t LastSequence() const { return last_sequence_; }

  void MarkFileNumberUsed(uint64_t number);
  void ReuseFileNumber(uint64_t file_number);
  void SetLastSequence(uint64_t s);

  uint64_t NumFiles() { return current_->NumFiles(); }
  uint64_t NumBytes() { return current_->NumBytes(); }

 private:
  class Builder;

  void AppendVersion(ZeroLevelVersion* v);
  void Finalize(ZeroLevelVersion* v);
  Status WriteSnapshot(log::Writer* log);
  bool ReuseManifest(const std::string& dscname, const std::string& dscbase);

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
  TableCache* table_cache_;

  // no copy
  VersionControl(const VersionControl&);
  void operator=(const VersionControl&);
};

class ZeroLevelCompaction {
 public:
  ~ZeroLevelCompaction();

  ZeroLevelVersionEdit* edit() { return edit_; }
  int num_input_files() const { return inputs_.size(); }

  FileMetaData* input(int i) { return inputs_[i]; }

  bool IsInput(uint64_t num);

  size_t size() { return inputs_.size(); }

  uint64_t MaxOutputFileSize() const { return max_output_file_size_; }

  void AddInputDeletions(ZeroLevelVersionEdit* edit);

  void ReleaseInputs();

 private:
  uint64_t max_output_file_size_;
  ZeroLevelVersion* input_version_;
  ZeroLevelVersionEdit* edit_;

  std::vector<FileMetaData*> inputs_;
};

} // namespace leveldb

#endif // STORAGE_LEVELDB_DB_VERSION_CONTROL_H_
