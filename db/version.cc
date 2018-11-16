#include "version.h"
#include "version_control.h"
#include "leveldb/index.h"
#ifdef PERF_LOG
#include "util/perf_log.h"
#endif

namespace leveldb {

enum SaverState {
  kNotFound,
  kFound,
  kDeleted,
  kCorrupt,
};

struct Saver {
  SaverState state;
  const Comparator* ucmp;
  Slice user_key;
  std::string* value;
};

static void SaveValue(void* arg, const Slice& ikey, const Slice& v) {
  Saver* s = reinterpret_cast<Saver*>(arg);
  ParsedInternalKey parsed_key;
  if (!ParseInternalKey(ikey, &parsed_key)) {
    s->state = kCorrupt;
  } else {
    if (s->ucmp->Compare(parsed_key.user_key, s->user_key) == 0) {
      s->state = (parsed_key.type == kTypeValue) ? kFound : kDeleted;
      if (s->state == kFound) {
        s->value->assign(v.data(), v.size());
      }
    }
  }
}

Status Version::Get(const ReadOptions& options, const LookupKey& key, std::string* val, uint16_t* file_number) {
  Status s;
  Slice ikey = key.internal_key();
  Slice user_key = key.user_key();
  const Comparator* ucmp = vcontrol_->user_comparator();

  Index* index = vcontrol_->options()->index;

#ifdef PERF_LOG
  uint64_t start_micros = benchmark::NowMicros();
  const IndexMeta* index_meta = index->Get(user_key);
  benchmark::LogMicros(benchmark::QUERY, benchmark::NowMicros() - start_micros);
#else
  const IndexMeta* index_meta = index->Get(user_key);
#endif

  if (index_meta != nullptr) {
    Saver saver;
    saver.state = kNotFound;
    saver.ucmp = ucmp;
    saver.user_key = user_key;
    saver.value = val;
    s = vcontrol_->cache()->Get(options, index_meta, ikey, &saver, SaveValue);
    *file_number = index_meta->file_number;
    if (!s.ok()) {
      return s;
    }
    switch (saver.state) {
      case kNotFound:
        s = Status::NotFound(Slice());
        return s;
      case kFound:
        return s;
      case kDeleted:
        s = Status::NotFound(Slice());
        return s;
      case kCorrupt:
        s = Status::Corruption("corrupted key for", user_key);
        return s;
    }
  }
  return Status::NotFound(Slice());
}

void Version::Ref() {
  ++refs_;
}

void Version::Unref() {
//  assert(this != vcontrol_->next_version());
  assert(refs_ >= 1);
  --refs_;
  if (refs_ == 0) {
    delete this;
  }
}

std::string Version::DebugString() const {
  std::string r;
  r.append("Files:\n");
  r.append("number, size, smallest, largest, alive/total, \n");
  for (const auto& file : files_) {
    AppendNumberTo(&r, file.second->number);
    r.append(", ");
    AppendNumberTo(&r, file.second->file_size);
    r.append(", ");
    r.append(file.second->smallest.user_key().ToString());
    r.append(", ");
    r.append(file.second->largest.user_key().ToString());
    r.append(", ");
    AppendNumberTo(&r, file.second->alive);
    r.append("/");
    AppendNumberTo(&r, file.second->total);
    r.append(", \n");
  }
  r.append("Files to merge:\n");
  r.append("number, size, smallest, largest, alive/total, \n");
  for (const auto& file : merge_candidates_) {
    AppendNumberTo(&r, file.second->number);
    r.append(", ");
    AppendNumberTo(&r, file.second->file_size);
    r.append(", ");
    r.append(file.second->smallest.user_key().ToString());
    r.append(", ");
    r.append(file.second->largest.user_key().ToString());
    r.append(", ");
    AppendNumberTo(&r, file.second->alive);
    r.append("/");
    AppendNumberTo(&r, file.second->total);
    r.append(", \n");
  }
  return r;
}

void Version::AddFile(std::shared_ptr<FileMetaData> f) {
  max_key_ = std::max(max_key_, fast_atoi(f->largest.user_key()));
  files_.insert({f->number, f});
}

void Version::AddCompactionFile(std::shared_ptr<FileMetaData> f) {
  merge_candidates_.insert({f->number, f});
}

bool Version::MoveToMerge(std::set<uint16_t> array, bool is_scan) {
  // restrict scan for less compaction
  if (is_scan && merge_candidates_.size() > config::SlowdownWritesTrigger) return false;
  // else still restrict if too many candidates
  else if (merge_candidates_.size() > config::StopWritesTrigger) return false;
  int added_files = 0;
  std::string msg;
  for (auto f : array) {
    try{
      if (is_scan && ++added_files > config::CompactionMaxSize/2) break;
      auto file = files_.at(f); // should rise exception if not among files
      files_.erase(f);
      merge_candidates_.insert({f, file});
    } catch (const std::exception& e) {

    }
  }
  return true;
}

} // namespace leveldb
