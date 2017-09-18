#ifndef STORAGE_LEVELDB_UTIL_PMARENA_H_
#define STORAGE_LEVELDB_UTIL_PMARENA_H_

#include "arena.h"
#include "quartz/src/lib/pmalloc.h"

namespace leveldb {

class PMArena : public Arena {
 public:
  virtual ~PMArena() {}

  virtual char* AllocateNewBlock(size_t block_bytes) override;
};

} // namespace leveldb

#endif // STORAGE_LEVEVDB_UTIL_PMARENA_H_
