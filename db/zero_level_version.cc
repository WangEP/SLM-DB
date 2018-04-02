#include "zero_level_version.h"
#include "leveldb/index.h"

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

Status ZeroLevelVersion::Get(const ReadOptions& options, const LookupKey& key, std::string* val) {
  Status s;
  Slice ikey = key.internal_key();
  Slice user_key = key.user_key();
  const Comparator* ucmp = vcontrol_->user_comparator();

  Index* index = vcontrol_->options()->index;
  const IndexMeta* index_meta = index->Get(user_key);
  if (index_meta != NULL) {
    BlockHandle block_handle = index_meta->handle;

    Saver saver;
    saver.state = kNotFound;
    saver.ucmp = ucmp;
    saver.user_key = user_key;
    saver.value = val;
    s = vcontrol_->cache()->Get2(options, index_meta->file_number, block_handle,
                                 ikey, &saver, SaveValue);
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

void ZeroLevelVersion::Ref() {
  ++refs_;
}

void ZeroLevelVersion::Unref() {
  assert(this != vcontrol_->next_version());
  assert(refs_ >= 1);
  --refs_;
  if (refs_ == 0) {
    delete this;
  }
}

std::string ZeroLevelVersion::DebugString() const {
  std::string r;
  for (auto iter : files_) {
    r.push_back(' ');
    AppendNumberTo(&r, iter.second->number);
    r.push_back(':');
    AppendNumberTo(&r, iter.second->file_size);
    r.append("[");
    r.append(iter.second->smallest.DebugString());
    r.append(" .. ");
    r.append(iter.second->largest.DebugString());
    r.append("]\n");
  }
  return std::__cxx11::string();
}

ZeroLevelVersion::~ZeroLevelVersion() {
  for (auto pair : files_) {
    FileMetaData* f = pair.second;
    assert(f->refs > 0);
    f->refs--;
    if (f->refs <= 0) {
      delete f;
    }
  }
}
void ZeroLevelVersion::AddFile(FileMetaData* f) {
  f->refs++;
  files_.insert({f->number, f});
}

} // namespace leveldb
