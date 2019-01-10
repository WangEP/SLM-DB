#include <cstdlib>
#include <string>
#include "leveldb/slice.h"
#include "util/perf_log.h"
#include "leveldb/persistant_pool.h"
#include "index/ff_btree.h"
#include "util/random.h"

#define N 64000
#define VAL_SIZE 1024

constexpr char nvm_dir[] = "/mnt/mem/tmp";
constexpr size_t nvm_size = 2147483648;

using namespace leveldb;

int main() {
  Random rand(10);
  nvram::create_pool(nvm_dir, nvm_size);
  FFBtree* tree = new FFBtree;
  // populate index with some data
  for (uint64_t i = 0; i < N*50; i++) {
    uint64_t k = rand.Next();
    tree->Insert(k, &k);
  }
  uint64_t s = rand.Next();
  // benchmark
  uint64_t start_us = benchmark::NowMicros();
  for (uint64_t i = 0; i < N; i++) {
    uint64_t k = rand.Next();
    tree->Insert(s+i, &k);
  }
  uint64_t end_us = benchmark::NowMicros();
  fprintf(stdout, "[BTree] micros: %lu\n", end_us - start_us);
}
