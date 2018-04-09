#ifndef STORAGE_LEVELDB_DB_INDEX_BASED_COMPACTION_H_
#define STORAGE_LEVELDB_DB_INDEX_BASED_COMPACTION_H_

#include "zero_level_version.h"

namespace leveldb {

class IndexBasedCompaction {
 public:
 private:
  ZeroLevelVersion* input_version_;
  std::vector<FileMetaData*> inputs_;
};

} // namespace leveldb

#endif // STORAGE_LEVELDB_INDEX_BASED_COMPACTION_H_
