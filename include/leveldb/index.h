#ifndef STORAGE_LEVELDB_INCLUDE_INDEX_H_
#define STORAGE_LEVELDB_INCLUDE_INDEX_H_

#include <cstdint>
#include <memory>
#include <deque>
#include "leveldb/slice.h"
#include "leveldb/iterator.h"
#include "leveldb/options.h"

namespace leveldb {

class TableCache;
class VersionEdit;

struct IndexMeta {
public:
  uint32_t offset;
  uint16_t size;
  uint16_t file_number;

  IndexMeta() : offset(0), size(0), file_number(0) { }

  IndexMeta(uint32_t offset, uint16_t size, uint16_t file_number) :
    offset(offset), size(size), file_number(file_number) { }
};

void* convert(IndexMeta meta);
IndexMeta convert(void* ptr);

struct KeyAndMeta{
  uint64_t key;
  std::shared_ptr<IndexMeta> meta;
};

class Index {
public:
  Index() = default;
  virtual ~Index() = default;
  //virtual void Insert(const uint32_t& key, IndexMeta meta) = 0;
  virtual IndexMeta Get(const Slice& key) = 0;
  virtual void AddQueue(std::deque<KeyAndMeta>& queue, VersionEdit* edit) = 0;
  virtual Iterator* NewIterator(const ReadOptions& options, TableCache* table_cache) = 0;
};

Index* CreateBtreeIndex();

} // namespace leveldb

#endif //STORAGE_LEVELDB_INCLUDE_INDEX_H_
