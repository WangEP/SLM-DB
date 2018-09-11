#include "leveldb/persistant_pool.h"

#include <libpmemcto.h>

namespace leveldb {
namespace nvram {

#define LAYOUT_NAME "PMINDEXDB"

static PMEMctopool* pm_pool;
static bool init = false;
static uint64_t allocs = 0;


void create_pool(const std::string& dir, const size_t& s) {
  size_t size = (s < PMEMCTO_MIN_POOL) ? PMEMCTO_MIN_POOL : s;
  printf("Creating NVM pool size of %lu\n", size);
  pm_pool = pmemcto_create(dir.data(), LAYOUT_NAME, size, 0666);
  init = true;
  if (pm_pool == nullptr) {
    fprintf(stderr, "pmem create error\n");
    perror(dir.data());
    exit(1);
  }
}

void close_pool() {
  if (init) {
    fprintf(stdout, "pmem allocs %lu\n", allocs);
    pmemcto_close(pm_pool);
  }
}

void pfree(void* ptr) {
  if (!init) {
    free(ptr);
  } else {
    pmemcto_free(pm_pool, ptr);
  }
}

void* pmalloc(size_t size) {
  void* ptr;
  if (!init) {
    ptr = malloc(size);
  } else {
    allocs++;
    if ((ptr = pmemcto_malloc(pm_pool, size)) == nullptr) {
      fprintf(stderr, "pmem malloc error 2 \n");
      perror("vmem_malloc");
      exit(1);
    }
  }
  return ptr;
}

void stats() {
//  char *msg;
//  pmemcto_stats_print(vmem, msg);
//  printf("%s\n", msg);
}

}
}