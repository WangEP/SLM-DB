//
// Created by olzhas on 9/18/17.
//

#include "pm_arena.h"

char *leveldb::PMArena::AllocateNewBlock(size_t block_bytes) {
  char* result = (char *) pmalloc(block_bytes);
  blocks_.push_back(result);
  memory_usage_.NoBarrier_Store(
      reinterpret_cast<void*>(MemoryUsage() + block_bytes + sizeof(char*)));
  return result;
}
