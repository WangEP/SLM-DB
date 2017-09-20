#include "pm_arena.h"

#if QUARTZ
#include "quartz/src/lib/pmalloc.h"
#endif

namespace leveldb {

PMArena::~PMArena() {
  for (size_t i = 0; i < blocks_.size(); i++) {
#if QUARTZ
    pfree(blocks[i], blocks_bytes_[i]);
#else
    delete[] blocks_[i];
#endif
  }
}

char *PMArena::AllocateNewBlock(size_t block_bytes) {
#if QUARTZ
  char* result = (char *) pmalloc(block_bytes);
  blocks_bytes_.push_back(block_bytes);
#else
  char *result = new char[block_bytes];
#endif
  blocks_.push_back(result);
  memory_usage_.NoBarrier_Store(
      reinterpret_cast<void *>(MemoryUsage() + block_bytes + sizeof(char *)));
  return result;
}

} // namespace leveldb
