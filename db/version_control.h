#ifndef STORAGE_LEVELDB_DB_VERSION_CONTROL_H_
#define STORAGE_LEVELDB_DB_VERSION_CONTROL_H_

#include <memory>
#include <random>
#include "version.h"
#include "version_edit.h"
#include "port/port_posix.h"

namespace leveldb {

class Compaction;
class DBImpl;

class VersionControl {
 public:
  VersionControl(DBImpl* db,
                 const std::string& dbname,
                 const Options* options,
                 TableCache* table_cache,
                 const InternalKeyComparator*);
  ~VersionControl();

  struct SummaryStorage {
    char buffer[100];
  };

  Version* current() { return current_; }
  TableCache* cache() { return table_cache_; }
  const Options* const options() { return options_; }
  const Comparator* user_comparator() const { return icmp_.user_comparator(); }
  const Comparator* internal_comparator() const { return &icmp_;}

  Status LogAndApply(VersionEdit* edit, port::Mutex* mu);
  Compaction* PickCompaction();
  void RegisterFileAccess(const uint16_t& file_number);
  void CheckLocality();
  void UpdateLocalityCheckKey(const Slice& target);
  Status Recover(bool* save_manifest);
  Iterator* MakeInputIterator(Compaction* c);
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
  void StateChange();

  uint64_t NumFiles() { return current_->NumFiles() + current_->MergeNumFiles(); }
  uint64_t NumBytes() { return current_->NumBytes(); }
  uint64_t CompactionSize() { return current_->merge_candidates_.size(); }

  bool State() { return state_change_; }

 private:
  class Builder;

  void AppendVersion(Version* v);
  Status WriteSnapshot(log::Writer* log);
  bool ReuseManifest(const std::string& dscname, const std::string& dscbase);
  void ForcedPick(Compaction**);
  void TryToPick(Compaction**);

  Env* const env_;
  const std::string dbname_;

  uint64_t next_file_number_;
  uint64_t manifest_file_number_;
  uint64_t last_sequence_;
  uint64_t log_number_;
  uint64_t prev_log_number_;

  const InternalKeyComparator icmp_;
  const Options* const options_;
  DBImpl* db_;
  WritableFile* descriptor_file_;
  log::Writer* descriptor_log_;
  Version* current_;
  TableCache* table_cache_;
  entry_key_t locality_check_key;
  std::random_device rd;
  std::mt19937 gen;
  std::uniform_int_distribution<> distribution;
  bool state_change_;

  // no copy
  VersionControl(const VersionControl&);
  void operator=(const VersionControl&);
};

class Compaction {
 public:
  Compaction(const Options* options)
      : max_output_file_size_(options->max_file_size),
        edit_(nullptr),
        input_version_(nullptr) {
    inputs_.reserve(config::CompactionMaxSize);
  }

  ~Compaction();

  VersionEdit* edit() { return edit_; }
  size_t num_input_files() const { return inputs_.size(); }

  std::shared_ptr<FileMetaData> input(int i) { return inputs_[i]; }

  void SetEdit(VersionEdit* edit) { edit_ = edit; }

  void AddInput(const std::shared_ptr<FileMetaData>& f) { inputs_.push_back(f); }

  bool IsInput(uint64_t num);

  uint64_t MaxOutputFileSize() const { return max_output_file_size_; }

  void AddInputDeletions(VersionEdit* edit);

  void ReleaseInputs();

  void ReleaseFiles();

 private:
  uint64_t max_output_file_size_;
  Version* input_version_;
  VersionEdit* edit_;

  std::vector<std::shared_ptr<FileMetaData>> inputs_;
};

} // namespace leveldb

#endif // STORAGE_LEVELDB_DB_VERSION_CONTROL_H_
