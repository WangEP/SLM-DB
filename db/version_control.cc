#include "version_control.h"
#include "filename.h"
#include "log_reader.h"
#include "table/merger.h"

namespace leveldb {


// Builder class

class VersionControl::Builder {
  std::vector<FileMetaData*> added_files_;
  std::set<uint64_t> deleted_files_;
  std::unordered_map<uint64_t, uint64_t> dead_key_counter_;
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
    for (auto iter : edit->GetDeadKeyCounter()) {
      dead_key_counter_.insert({iter.first, iter.second});
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

  void SaveTo(ZeroLevelVersion* v, int threshold) {
    for (auto iter : base_->GetFiles()) {
      FileMetaData* f = iter.second;
      uint64_t dead = 0;
      try {
        dead = dead_key_counter_.at(f->number);
      } catch (std::exception& e) {}
      f->alive -= dead;
      if (f->total/f->alive > threshold) { // move to compaction list
        v->AddCompactionFile(f);
      } else if (deleted_files_.count(iter.first) <= 0) { // do not add if file got deleted
        v->AddFile(f);
      }
      f->refs--;
    }
    for (auto iter : added_files_) {
      assert(dead_key_counter_.find(iter->number) == dead_key_counter_.end());
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

// Version Control class

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
      next_(NULL),
      current_(NULL) {
  AppendVersion(new ZeroLevelVersion(this));
}

void VersionControl::AppendVersion(ZeroLevelVersion* v) {
  assert(v->refs_ == 0);
  assert(v != current_);
  if (current_ != NULL) {
    current_->Unref();
  }
  current_ = v;
  v->Ref();
}

Status VersionControl::Recover(bool* save_manifest) {
  struct LogReporter : public log::Reader::Reporter {
    Status* status;
    virtual void Corruption(size_t bytes, const Status& s) {
      if (this->status->ok()) *this->status = s;
    }
  };

  std::string current;
  Status s = ReadFileToString(env_, CurrentFileName(dbname_), &current);
  if (!s.ok()) {
    return s;
  }
  if (current.empty() || current[current.size()-1] != '\n') {
    return Status::Corruption("CURRENT file does not end with newline");
  }
  current.resize(current.size() - 1);

  std::string dscname = dbname_ + "/" + current;
  SequentialFile* file;
  s = env_->NewSequentialFile(dscname, &file);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      return Status::Corruption(
          "CURRENT points to a non-existent file", s.ToString());
    }
    return s;
  }

  bool have_log_number = false;
  bool have_prev_log_number = false;
  bool have_next_file = false;
  bool have_last_sequence = false;
  uint64_t next_file = 0;
  uint64_t last_sequence = 0;
  uint64_t log_number = 0;
  uint64_t prev_log_number = 0;
  Builder builder(this, current_);

  {
    LogReporter reporter;
    reporter.status = &s;
    log::Reader reader(file, &reporter, true, 0);
    Slice record;
    std::string scratch;
    while (reader.ReadRecord(&record, &scratch) && s.ok()) {
      ZeroLevelVersionEdit edit;
      s = edit.DecodeFrom(record);
      if (s.ok()) {
        if (edit.HasCompartorName() &&
            edit.GetComparatorName() != icmp_.user_comparator()->Name()) {
          s = Status::InvalidArgument(
              edit.GetComparatorName() + " does not match existing comparator ",
              icmp_.user_comparator()->Name());
        }
      }

      if (s.ok()) {
        builder.Apply(&edit);
      }

      if (edit.HasLogNumber()) {
        log_number = edit.GetLogNumber();
        have_log_number = true;
      }

      if (edit.HasPrevLogNumber()) {
        prev_log_number = edit.GetPrevLogNumber();
        have_prev_log_number = true;
      }

      if (edit.HasNextFileNumber()) {
        next_file = edit.GetNextFile();
        have_next_file = true;
      }

      if (edit.HasLastSequence()) {
        last_sequence = edit.GetLastSequence();
        have_last_sequence = true;
      }
    }
  }

  delete file;
  file = NULL;

  if (s.ok()) {
    if (!have_next_file) {
      s = Status::Corruption("no meta-nextfile entry in descriptor");
    } else if (!have_log_number) {
      s = Status::Corruption("no meta-lognumber entry in descriptor");
    } else if (!have_last_sequence) {
      s = Status::Corruption("no last-sequence-number entry in descriptor");
    }

    if (!have_prev_log_number) {
      prev_log_number = 0;
    }

    MarkFileNumberUsed(prev_log_number);
    MarkFileNumberUsed(log_number);
  }

  if (s.ok()) {
    ZeroLevelVersion* v = new ZeroLevelVersion(this);
    builder.SaveTo(v);
    Finalize(v);
    AppendVersion(v);
    manifest_file_number_ = next_file;
    next_file_number_ = next_file + 1;
    last_sequence_ = last_sequence;
    log_number_ = log_number;
    prev_log_number_ = prev_log_number;

    if (ReuseManifest(dscname, current)) {
    } else {
      *save_manifest = true;
    }
  }
  return s;
}

bool VersionControl::ReuseManifest(const std::string& dscname,
                                   const std::string& dscbase) {
  if (!options_->reuse_logs) {
    return false;
  }
  // Skip it, don't need to reuse manifest
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
    Builder builder(this, current_);
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
    // append version
    AppendVersion(v);
    log_number_ = edit->GetLogNumber();
    prev_log_number_ = edit->GetPrevLogNumber();
  } else {
    v->Unref();
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

bool VersionControl::NeedsCompaction() const {
  // decide whether it needed or not looking for current version

}

Iterator* VersionControl::MakeInputIterator(ZeroLevelCompaction* c) {
  ReadOptions options;
  options.verify_checksums = options_->paranoid_checks;
  options.fill_cache = false;

  Iterator** list = new Iterator*[c->size()];
  for (size_t i = 0; i < c->size(); i++) {
    list[i] = table_cache_->NewIterator(options, c->input(i)->number, c->input(i)->file_size);
  }
  Iterator* result = NewMergingIterator(&icmp_, list, c->size());
  delete[] list;
  return result;
}

void VersionControl::MarkFileNumberUsed(uint64_t number) {
  if (next_file_number_ <= number) {
    next_file_number_ = number + 1;
  }
}

void VersionControl::ReuseFileNumber(uint64_t file_number) {
  if (next_file_number_ == file_number + 1) {
    next_file_number_ = file_number;
  }
}

void VersionControl::SetLastSequence(uint64_t s) {
  assert(s >= last_sequence_);
  last_sequence_ = s;
}

Status VersionControl::WriteSnapshot(log::Writer* log) {
  ZeroLevelVersionEdit edit;
  edit.SetComparatorName(icmp_.user_comparator()->Name());
  for (auto iter : current()->GetFiles()) {
    const FileMetaData* f = iter.second;
    edit.AddFile(f->number, f->file_size, f->total, f->alive, f->smallest, f->largest);
  }
  std::string record;
  edit.EncodeTo(&record);
  return log->AddRecord(record);
}

const char* VersionControl::Summary(SummaryStorage* scratch) const {
  snprintf(scratch->buffer, sizeof(scratch->buffer), "files %d", int(current_->NumFiles()));
  return scratch->buffer;
}

} // namespace leveldb
