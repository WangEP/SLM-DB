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

Status Version::Get(const ReadOptions& options, const LookupKey& key, std::string* val) {
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
//    uint64_t fsize = GetFileSize(index_meta.file_number);
    s = vcontrol_->cache()->Get(options, index_meta, ikey, &saver, SaveValue);
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
  for (const auto& file : files_) {
    r.push_back(' ');
    AppendNumberTo(&r, file.second->number);
    r.push_back(':');
    AppendNumberTo(&r, file.second->file_size);
    r.append("[");
    r.append(file.second->smallest.DebugString());
    r.append(" .. ");
    r.append(file.second->largest.DebugString());
    r.append("]");
    r.append(" {");
    AppendNumberTo(&r, file.second->alive);
    r.append(" / ");
    AppendNumberTo(&r, file.second->total);
    r.append("}\n");
  }
  r.append("Files to merge:\n");
  for (const auto& file : merge_candidates_) {
    r.push_back(' ');
    AppendNumberTo(&r, file.second->number);
    r.push_back(':');
    AppendNumberTo(&r, file.second->file_size);
    r.append("[");
    r.append(file.second->smallest.DebugString());
    r.append(" .. ");
    r.append(file.second->largest.DebugString());
    r.append("]");
    r.append(" {");
    AppendNumberTo(&r, file.second->alive);
    r.append(" / ");
    AppendNumberTo(&r, file.second->total);
    r.append("}\n");
  }
  return r;
}

void Version::AddFile(std::shared_ptr<FileMetaData> f) {
  files_.insert({f->number, f});
}

void Version::AddCompactionFile(std::shared_ptr<FileMetaData> f) {
  merge_candidates_.insert({f->number, f});
}

void Version::SortMergeCandidates() {

}

void Version::MoveToMerge(std::set<uint16_t> array) {
  for (auto f : array) {
    if (files_.count(f) > 0) {
      auto file = files_.at(f);
      files_.erase(f);
      merge_candidates_.insert({f, file});
    }
  }
}

} // namespace leveldb
