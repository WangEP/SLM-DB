#include "version_control.h"

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
  Finalize(v);

}

void VersionControl::Finalize(ZeroLevelVersion* v) {

}

class VersionControl::Builder {
  std::vector<FileMetaData*> added_files_;
  std::set<uint64_t> deleted_files_;
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
      if (deleted_files_.count(iter.first) <= 0) {
        v->AddFile(iter.second);
      }
    }
    for (auto iter : added_files_) {
      v->AddFile(iter);
    }
  }

};

} // namespace leveldb
