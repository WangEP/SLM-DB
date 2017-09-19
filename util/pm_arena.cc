#include "pm_arena.h"
#include "quartz/src/lib/pmalloc.h"

char *leveldb::PMArena::AllocateNewBlock(size_t block_bytes) {
  char* result = (char *) pmalloc(block_bytes);
  blocks_.push_back(result);
  memory_usage_.NoBarrier_Store(
      reinterpret_cast<void*>(MemoryUsage() + block_bytes + sizeof(char*)));
  return result;
}
