#ifndef STORAGE_LEVELDB_DB_ZERO_LEVEL_VERSION_EDIT_H_
#define STORAGE_LEVELDB_DB_ZERO_LEVEL_VERSION_EDIT_H_

#include <vector>
#include <cstdint>
#include "dbformat.h"
#include "zero_level_version.h"

namespace leveldb {

class ZeroLevelVersionEdit {
 public:
	ZeroLevelVersionEdit() { Clear(); };
	~ZeroLevelVersionEdit() { };

	void Clear();

	void SetComparatorName(const Slice&);
	void SetLogNumber(uint64_t);
	void SetPrevLogNumber(uint64_t);
	void SetNextFile(uint64_t);
	void SetLastSequence(uint64_t);

  void DeleteFile(uint64_t file) { deleted_files_.push_back(file); }
  void AddFile(uint64_t file, uint64_t file_size, const InternalKey& smallest, const InternalKey& largest);

	void EncodeTo(std::string* dst) const;
	Status DecodeFrom(const Slice& src);

	std::string DebugString() const;
 private:
  std::vector<FileMetaData> new_files_;
  std::vector<uint64_t> deleted_files_;

	std::string comparator_;
	uint64_t log_number_;
	uint64_t prev_log_number_;
	uint64_t next_file_number_;
	SequenceNumber last_sequence_;
	bool has_comparator_;
	bool has_log_number_;
	bool has_prev_log_number_;
	bool has_next_file_number_;
	bool has_last_sequence_;
};

} // namespace leveldb

#endif // STORAGE_LEVELDB_DB_ZERO_LEVEL_VERSION_EDIT_H_
