#include "zero_level_version_edit.h"

namespace leveldb {

enum Tag {
  kComparator     = 1,
  kLogNumber      = 2,
  kNextFileNumber = 3,
  kLastSequence   = 4,
  kDeletedFile    = 5,
  kNewFile        = 6,
  kPrevLogNumber  = 7,
  kToCompactFile  = 8
};

void ZeroLevelVersionEdit::Clear() {
  comparator_.clear();
  log_number_ = 0;
  prev_log_number_ = 0;
  last_sequence_ = 0;
  next_file_number_ = 0;
  has_comparator_ = false;
  has_log_number_ = false;
  has_prev_log_number_ = false;
  has_next_file_number_ = false;
  has_last_sequence_ = false;
  deleted_files_.clear();
  new_files_.clear();
}

void ZeroLevelVersionEdit::SetComparatorName(const Slice& comparator) {
  has_comparator_ = true;
  comparator_ = comparator.ToString();
}

void ZeroLevelVersionEdit::SetLogNumber(uint64_t num) {
  has_log_number_ = true;
  log_number_ = num;
}

void ZeroLevelVersionEdit::SetPrevLogNumber(uint64_t num) {
  has_prev_log_number_ = true;
  prev_log_number_ = num;
}

void ZeroLevelVersionEdit::SetNextFile(uint64_t num) {
  has_next_file_number_ = true;
  next_file_number_ = num;
}

void ZeroLevelVersionEdit::SetLastSequence(uint64_t num) {
  has_last_sequence_ = true;
  last_sequence_ = num;
}

void ZeroLevelVersionEdit::EncodeTo(std::string* dst) const {
  if (has_comparator_) {
    PutVarint32(dst, kComparator);
    PutLengthPrefixedSlice(dst, comparator_);
  }
  if (has_log_number_) {
    PutVarint32(dst, kLogNumber);
    PutVarint64(dst, log_number_);
  }
  if (has_prev_log_number_) {
    PutVarint32(dst, kPrevLogNumber);
    PutVarint64(dst, prev_log_number_);
  }
  if (has_next_file_number_) {
    PutVarint32(dst, kNextFileNumber);
    PutVarint64(dst, next_file_number_);
  }
  if (has_last_sequence_) {
    PutVarint32(dst, kLastSequence);
    PutVarint64(dst, last_sequence_);
  }
  for (auto file : deleted_files_) {
    PutVarint32(dst, kDeletedFile);
    PutVarint64(dst, file);
  }
  for (auto file : new_files_) {
    PutVarint32(dst, kNewFile);
    PutVarint64(dst, file.number);
    PutVarint64(dst, file.file_size);
    PutVarint64(dst, file.total);
    PutVarint64(dst, file.alive);
    PutLengthPrefixedSlice(dst, file.smallest.Encode());
    PutLengthPrefixedSlice(dst, file.largest.Encode());
  }
  for (auto file : to_compact_files_) {
    PutVarint32(dst, kToCompactFile);
    PutVarint64(dst, file);
  }
}

static bool GetInternalKey(Slice* input, InternalKey* dst) {
  Slice str;
  if (GetLengthPrefixedSlice(input, &str)) {
    dst->DecodeFrom(str);
    return true;
  } else {
    return false;
  }
}

Status ZeroLevelVersionEdit::DecodeFrom(const Slice& src) {
  Clear();
  Slice input = src;
  const char* msg = NULL;
  uint32_t tag;

  uint64_t number;
  FileMetaData f;
  Slice str;
  InternalKey key;

  while (msg == NULL && GetVarint32(&input, &tag)) {
    switch (tag) {
      case kComparator:
        if (GetLengthPrefixedSlice(&input, &str)) {
          comparator_ = str.ToString();
          has_comparator_ = true;
        } else {
          msg = "comparator name";
        }
        break;

      case kLogNumber:
        if (GetVarint64(&input, &log_number_)) {
          has_log_number_ = true;
        } else {
          msg = "log number";
        }
        break;

      case kPrevLogNumber:
        if (GetVarint64(&input, &prev_log_number_)) {
          has_prev_log_number_ = true;
        } else {
          msg = "prev log number";
        }
        break;

      case kNextFileNumber:
        if (GetVarint64(&input, &next_file_number_)) {
          has_next_file_number_ = true;
        } else {
          msg = "next file number";
        }
        break;

      case kLastSequence:
        if (GetVarint64(&input, &last_sequence_)) {
          has_last_sequence_ = true;
        } else {
          msg = "last sequence number";
        }
        break;

      case kDeletedFile:
        if (GetVarint64(&input, &number)) {
          deleted_files_.push_back(number);
        } else {
          msg = "deleted file";
        }
        break;

      case kNewFile:
        if (GetVarint64(&input, &f.number) &&
            GetVarint64(&input, &f.file_size) &&
            GetVarint64(&input, &f.alive) &&
            GetVarint64(&input, &f.total) &&
            GetInternalKey(&input, &f.smallest) &&
            GetInternalKey(&input, &f.largest)) {
          new_files_.push_back(f);
        } else {
          msg = "new-file entry";
        }
        break;

      case kToCompactFile:
        if (GetVarint64(&input, &number)) {
          to_compact_files_.push_back(number);
        } else {
          msg = "to compact file";
        }
        break;

      default:
        msg = "unknown tag";
        break;
    }
  }

  if (msg == NULL && !input.empty()) {
    msg = "invalid tag";
  }

  Status result;
  if (msg != NULL) {
    result = Status::Corruption("ZeroVersionEdit", msg);
  }
  return result;
}

std::string ZeroLevelVersionEdit::DebugString() const {
  return std::string();
}



}
