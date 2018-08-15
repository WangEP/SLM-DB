#include <util/random.h>
#include "version_control.h"
#include "filename.h"
#include "log_reader.h"
#include "table/merger.h"
#ifdef PERF_LOG
#include "util/perf_log.h"
#endif


namespace leveldb {


static Random generator(0);

// Builder class

class VersionControl::Builder {
  std::vector<std::shared_ptr<FileMetaData>> added_files_;
  std::set<uint64_t> deleted_files_;
  std::unordered_map<uint64_t, uint64_t> dead_key_counter_;
  VersionControl* vcontrol_;
  Version* base_;
 public:

  Builder(VersionControl* vcontrol, Version* base)
      : vcontrol_(vcontrol), base_(base) {
    base_->Ref();
  }

  ~Builder() {
    base_->Unref();
  }

  void Apply(VersionEdit* edit) {
    for (auto iter : edit->deleted_files_) {
      deleted_files_.insert(iter);
    }
    for (auto iter : edit->dead_key_counter_) {
      dead_key_counter_.insert({iter.first, iter.second});
    }
    for (const auto& iter : edit->new_files_) {
      std::shared_ptr<FileMetaData> f = std::make_shared<FileMetaData>();
      f->number = iter.number;
      f->file_size = iter.file_size;
      f->total = iter.total;
      f->alive = iter.alive;
      f->smallest = iter.smallest;
      f->largest = iter.largest;
      deleted_files_.erase(f->number);
      added_files_.push_back(f);
    }
  }

  void SaveTo(Version* v, int threshold) {
    for (auto iter : base_->files_) {
      assert(iter.first == iter.second->number);
      auto f = iter.second;
      if (deleted_files_.count(iter.first) <= 0) { // do not add if got deleted
        uint64_t dead = 0;
        try {
          dead = dead_key_counter_.at(f->number);
        } catch (std::exception& e) { }
        if (f->alive > dead) {
          f->alive -= dead;
          if (100 * f->alive / f->total <= threshold) { // move to compaction list
            v->AddCompactionFile(f);
            vcontrol_->new_merge_candidates_ = true;
          } else {
            v->AddFile(f);
          }
        }
      }
    }
    for (auto iter : base_->merge_candidates_) {
      assert(iter.first == iter.second->number);
      auto f = iter.second;
      if (deleted_files_.count(iter.first) <= 0) { // do not add if got deleted
        uint64_t dead = 0;
        try {
          dead = dead_key_counter_.at(f->number);
        } catch (std::exception& e) { }
        if (f->alive > dead) {
          f->alive -= dead;
          v->AddCompactionFile(f);
        }
      }
    }
    for (const auto& f : added_files_) {
      assert(dead_key_counter_.count(f->number) <= 0);
      v->AddFile(f);
    }
  }

};

// Compaction class

Compaction::~Compaction() {
  if (input_version_ != nullptr) {
    input_version_->Unref();
  }
}

void Compaction::AddInputDeletions(VersionEdit* edit) {
  for (auto f : inputs_) {
    edit->DeleteFile(f->number);
  }
}


void Compaction::ReleaseInputs() {
  if (input_version_ != nullptr) {
    input_version_->Unref();
    input_version_ = nullptr;
  }
}

bool Compaction::IsInput(uint64_t num) {
  for (auto f : inputs_) {
    if (f->number == num) {
      return true;
    }
  }
  return false;
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
      icmp_(*cmp),
      next_file_number_(2),
      manifest_file_number_(0),
      last_sequence_(0),
      log_number_(0),
      prev_log_number_(0),
      compaction_pointer_(0),
      new_merge_candidates_(false),
      descriptor_file_(nullptr),
      descriptor_log_(nullptr),
      current_(nullptr) {
  AppendVersion(new Version(this));
}

VersionControl::~VersionControl() {
  delete descriptor_log_;
  delete descriptor_file_;
  current_->Unref();
}

void VersionControl::AppendVersion(Version* v) {
  assert(v->refs_ == 0);
  assert(v != current_);
  if (current_ != nullptr) {
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
      VersionEdit edit;
      s = edit.DecodeFrom(record);
      if (s.ok()) {
        if (edit.HasComparatorName() &&
            edit.GetComparatorName() != icmp_.user_comparator()->Name()) {
          s = Status::InvalidArgument(
              {edit.GetComparatorName() + " does not match existing comparator "},
              {icmp_.user_comparator()->Name()});
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
  file = nullptr;

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
    Version* v = new Version(this);
    builder.SaveTo(v, options_->merge_threshold);
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

Status VersionControl::LogAndApply(VersionEdit* edit, port::Mutex* mu) {
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

  Version* v = new Version(this);
  {
    Builder builder(this, current_);
    edit->Wait();
    builder.Apply(edit);
    builder.SaveTo(v, options_->merge_threshold);
  }

  std::string new_manifest_file;
  Status s;
  if (descriptor_log_ == nullptr) {
    assert(descriptor_file_ == nullptr);
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
      descriptor_log_ = nullptr;
      descriptor_file_ = nullptr;
      env_->DeleteFile(new_manifest_file);
    }
  }
  return s;
}

Compaction* VersionControl::PickCompaction() {
  // get compaction and return
  if (current_->merge_candidates_.size() <= 1) return nullptr;
  Log(options_->info_log, "Picking compaction");
  srand(time(nullptr));
  Compaction* c = new Compaction(options_);
  auto rand_it = current_->merge_candidates_.begin();
  int rand_index = std::rand() % current_->merge_candidates_.size();
  while (--rand_index > 0) rand_it++;
  auto candidate1 = rand_it->second;
  assert(candidate1 != nullptr);
  assert(candidate1->number >= 2);
  assert(candidate1->number < next_file_number_);
  Slice begin = candidate1->smallest.user_key();
  Slice end = candidate1->largest.user_key();
  c->AddInput(candidate1);
  for (auto iter : current_->merge_candidates_) {
    if (c->num_input_files() > config::CompactionMaxSize) {
      break;
    }
    if (iter.first != rand_it->first) {
      auto f = iter.second;
      const Slice file_start = f->smallest.user_key();
      const Slice file_end = f->largest.user_key();
      if (user_comparator()->Compare(file_end, begin) < 0) {
        // skip it
      } else if (user_comparator()->Compare(file_start, end) > 0) {
        // skip it
      } else {
        c->AddInput(f);
      }

    }
  }
  if (c->num_input_files() <= 1) {
    delete c;
    c = nullptr;
    new_merge_candidates_ = false;
    Log(options_->info_log, "No compaction candidates");
    if (current_->merge_candidates_.size() >= config::CompactionForceTrigger) {
      new_merge_candidates_ = true;
      c = ForcedCompaction();
    } else {
      return nullptr;
    }
  }
  assert(c != nullptr);
  assert(c->num_input_files() > 1);
  Log(options_->info_log, "Compact %zu candidates for merge", c->num_input_files());
  std::string msg;
  for (int i = 0; i < c->num_input_files(); i++) {
    msg.append(std::to_string(c->input(i)->number));
    msg.append(" ");
  }
  Log(options_->info_log, "Merge %s", msg.c_str());
  return c;
}

Compaction* VersionControl::ForcedCompaction() {
  Compaction* c = new Compaction(options_);
  Log(options_->info_log, "Forced compaction");
  for (auto iter = current_->merge_candidates_.begin();
    iter != current_->merge_candidates_.end() &&
    c->num_input_files() <= options_->forced_compaction_size;
    iter++) {
      c->AddInput(iter->second);
  }
  return c;
}

bool VersionControl::NeedsCompaction() const {
  // decide whether it needed or not looking for current version
  return current_->merge_candidates_.size() > config::CompactionTrigger && new_merge_candidates_;
}

Iterator* VersionControl::MakeInputIterator(Compaction* c) {
  ReadOptions options;
  options.verify_checksums = options_->paranoid_checks;
  options.fill_cache = false;

  Iterator** list = new Iterator*[c->num_input_files()];
  for (size_t i = 0; i < c->num_input_files(); i++) {
    list[i] = table_cache_->NewIterator(options, c->input(i)->number, c->input(i)->file_size);
  }
  Iterator* result = NewMergingIterator(&icmp_, list, c->num_input_files());
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
  VersionEdit edit;
  edit.SetComparatorName(icmp_.user_comparator()->Name());
  for (auto iter : current_->files_) {
    auto f = iter.second;
    edit.AddFile(f->number, f->file_size, f->total, f->alive, f->smallest, f->largest);
  }
  for (auto iter : current_->merge_candidates_) {
    auto f = iter.second;
    edit.AddMergeCandidates(f->number, f->file_size, f->total, f->alive, f->smallest, f->largest);
  }
  std::string record;
  edit.EncodeTo(&record);
  return log->AddRecord(record);
}

const char* VersionControl::Summary(SummaryStorage* scratch) const {
  snprintf(scratch->buffer, sizeof(scratch->buffer), " Regular files number %lu, Merge files number %lu", current_->NumFiles(), current_->MergeNumFiles());
  return scratch->buffer;
}

} // namespace leveldb
