#include "zero_level_version_edit.h"

namespace leveldb {

enum Tag {
	kComparator     = 1,
	kLogNumber      = 2,
	kNextFileNumber = 3,
	kLastSequence   = 4,
	kDeletedFile    = 5,
	kNewFile        = 6,
	kPrevLogNumber  = 7
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

void ZeroLevelVersionEdit::AddFile(uint64_t file, uint64_t file_size,
                                   const InternalKey& smallest,
                                   const InternalKey& largest) {

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
}

Status ZeroLevelVersionEdit::DecodeFrom(const Slice& src) {
	return Status();
}

std::string ZeroLevelVersionEdit::DebugString() const {
	return std::string();
}

}
