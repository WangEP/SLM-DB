#ifndef STORAGE_LEVELDB_UTIL_PERSISTANT_POOL_H_
#define STORAGE_LEVELDB_UTIL_PERSISTANT_POOL_H_

#include <string>
#include <libpmemcto.h>

namespace leveldb {

namespace nvram {

#define LAYOUT_NAME "PMINDEXDB"

static PMEMctopool* vmem = nullptr;

static void create_pool(const std::string& dir, const size_t& s) {
  size_t size = (s < PMEMCTO_MIN_POOL) ? PMEMCTO_MIN_POOL : s;
  printf("Creating NVM pool size of %lu\n", size);
  vmem = pmemcto_create(dir.data(), LAYOUT_NAME, size, 0666);
  if (vmem == nullptr) {
    perror(dir.data());
    exit(1);
  }
}

static void close_pool() {
  if (vmem != nullptr) {
    pmemcto_close(vmem);
  }
}

static inline void pfree(void* ptr) {
  if (vmem == nullptr) {
    free(ptr);
  } else {
    pmemcto_free(vmem, ptr);
  }
}

static inline void* pmalloc(size_t size) {
  void* ptr;
  if (vmem == nullptr) {
    ptr = malloc(size);
  } else {
    if ((ptr = pmemcto_malloc(vmem, size)) == nullptr) {
      perror("vmem_malloc");
      exit(1);
    }
  }
  return ptr;
}

static void stats() {
//  char *msg;
//  pmemcto_stats_print(vmem, msg);
//  printf("%s\n", msg);
}

} // namespace nvram

} // namespace leveldb

#endif // STORAGE_LEVELDB_UTIL_PERSISTANT_POOL_H_
