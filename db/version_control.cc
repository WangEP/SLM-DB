#include "version_control.h"
#include "filename.h"

namespace leveldb {

VersionControl::VersionControl(const std::string& dbname,
                               const Options* options,
                               TableCache* table_cache,
                               const InternalKeyComparator* cmp)
    : env_(options->env),
      dbname_(dbname),
      options_(options),
      table_cache_(table_cache),
      icmp_(cmp),
      next_file_number_(2),
      manifest_file_number_(0),
      last_sequence_(0),
      log_number_(0),
      prev_log_number_(0),
      descriptor_file_(NULL),
      descriptor_log_(NULL),
      vnext_(NULL),
      vcurrent_(NULL) {
  AppendVersion(new ZeroLevelVersion(this));
}

void VersionControl::AppendVersion(ZeroLevelVersion* v) {
  assert(v->refs_ == 0);
  assert(v != vcurrent_);
  if (vcurrent_ != NULL) {
    vcurrent_->Unref();
  }
  vcurrent_ = v;
  v->Ref();
}

Status VersionControl::LogAndApply(ZeroLevelVersionEdit* edit, port::Mutex* mu) {
  if (edit->HasLogNumber()) {
    assert(edit->GetLogNumber() >= log_number_);
    assert(edit->GetLogNumber() < next_file_number_);
  } else {
    edit->SetLogNumber(log_number_);
  }
  if (!edit->HasPrevLogNumber()) {
    edit->SetPrevLogNumber(prev_log_number_);
  }
  edit->SetNextFile(next_file_number_);
  edit->SetLastSequence(last_sequence_);

  ZeroLevelVersion* v = new ZeroLevelVersion(this);
  {
    Builder builder(this, vcurrent_);
    builder.Apply(edit);
    builder.SaveTo(v);
  }
  Finalize(v);

  std::string new_manifest_file;
  Status s;
  if (descriptor_log_ == NULL) {
    assert(descriptor_file_ == NULL);
    new_manifest_file = DescriptorFileName(dbname_, manifest_file_number_);
    edit->SetNextFile(next_file_number_);
    s = env_->NewWritableFile(new_manifest_file, &descriptor_file_);
    if (s.ok()) {
      descriptor_log_ = new log::Writer(descriptor_file_);
      s = WriteSnapshot(descriptor_log_);
    }
  }

  {
    mu->Unlock();
    if (s.ok()) {
      std::string record;
      edit->EncodeTo(&record);
      s = descriptor_log_->AddRecord(record);
      if (s.ok()) {
        s = descriptor_file_->Sync();
      }
      if (!s.ok()) {
        Log(options_->info_log, "MANIFEST write: %s\n", s.ToString().c_str());
      }
    }
    if (s.ok() && !new_manifest_file.empty()) {
      s = SetCurrentFile(env_, dbname_, manifest_file_number_);
    }
    mu->Lock();
  }

  if (s.ok()) {
    // append vers
    AppendVersion(v);
    log_number_ = edit->GetLogNumber();
    prev_log_number_ = edit->GetPrevLogNumber();
  } else {
    delete v;
    if (!new_manifest_file.empty()) {
      delete descriptor_log_;
      delete descriptor_file_;
      descriptor_log_ = NULL;
      descriptor_file_ = NULL;
      env_->DeleteFile(new_manifest_file);
    }
  }
  return s;
}

ZeroLevelCompaction* VersionControl::PickCompaction() {
  // get compaction and return
  ZeroLevelCompaction* c;
  return c;
}

void VersionControl::Finalize(ZeroLevelVersion* v) {
  // search for compaction
}

Status VersionControl::WriteSnapshot(log::Writer* log) {
  ZeroLevelVersionEdit edit;
  edit.SetComparatorName(icmp_.user_comparator()->Name());
  for (auto iter : current_version()->GetFiles()) {
    const FileMetaData* f = iter.second;
    edit.AddFile(f->number, f->file_size, f->total, f->alive, f->smallest, f->largest);
  }
  std::string record;
  edit.EncodeTo(&record);
  return log->AddRecord(record);
}

// Builder class

class VersionControl::Builder {
  std::vector<FileMetaData*> added_files_;
  std::set<uint64_t> deleted_files_;
  std::set<uint64_t> to_compact_files_;
  VersionControl* vcontrol_;
  ZeroLevelVersion* base_;
 public:

  Builder(VersionControl* vcontrol, ZeroLevelVersion* base)
      : vcontrol_(vcontrol), base_(base) {
    base_->Ref();
  }

  ~Builder() {
    for (auto iter : added_files_) {
      FileMetaData* f = iter;
      f->refs--;
      if (f->refs <= 0) {
        delete f;
      }
    }
  }

  void Apply(ZeroLevelVersionEdit* edit) {
    for (auto iter : edit->GetDeletedFiles()) {
      deleted_files_.insert(iter);
    }
    for (auto iter : edit->GetNewCompactFiles()) {
      to_compact_files_.insert(iter);
    }
    for (auto iter : edit->GetNewFiles()) {
      FileMetaData* f = new FileMetaData(iter);
      f->refs = 1;
      f->allowed_seeks = (f->file_size / 16384);
      if (f->allowed_seeks < 100) f->allowed_seeks = 100;
      deleted_files_.erase(f->number);
      added_files_.push_back(f);
    }
  }

  void SaveTo(ZeroLevelVersion* v) {
    for (auto iter : base_->GetFiles()) {
      // move to compaction list
      if (to_compact_files_.count(iter.first) > 0) {
        v->AddCompactionFile(iter.second);
        iter.second->refs--;
      }
      // do not add if file got deleted
      else if (deleted_files_.count(iter.first) <= 0) {
        v->AddFile(iter.second);
        iter.second->refs--;
      }
    }
    for (auto iter : added_files_) {
      v->AddFile(iter);
    }
  }

};

// Compaction class

ZeroLevelCompaction::~ZeroLevelCompaction() {
  if (input_version_ != NULL) {
    input_version_->Unref();
  }
}

void ZeroLevelCompaction::AddInputDeletions(ZeroLevelVersionEdit* edit) {
  for (auto f : inputs_) {
    edit->DeleteFile(f->number);
  }
}


void ZeroLevelCompaction::ReleaseInputs() {
  if (input_version_ != NULL) {
    input_version_->Unref();
    input_version_ = NULL;
  }
}


} // namespace leveldb
