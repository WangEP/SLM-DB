#include <cstdlib>
#include <string>
#include "db/memtable.h"
#include "leveldb/slice.h"
#include "util/perf_log.h"

using namespace leveldb;

#define N 64000
#define VAL_SIZE 1024

static constexpr std::string nvm_dir = "/mnt/mem/tmp";
static constexpr size_t nvm_size = 1*1024*1024;


int main() {
  Random rand(10);
  RandomGenerator gen;
  nvram::create_pool(nvm_dir, nvm_size);
  const Comparator* comparator = BytewiseComparator();
  const InternalKeyComparator icomparator = comparator;
  MemTable* memtable = new MemTable(comparator);
  uint64_t start_us = benchmark::NowMicros();
  for (uint64_t i = 0; i < N; i++) {
    uint64_t k = rand.Next();
    char key[100];
    snprintf(key, sizeof(key), config::key_format, k);
    Slice value = gen.Generate(VAL_SIZE);
    memtable->Add(i, kTypeValue, key, value);
  }
  uint64_t end_us = benchmark::NowMicros();
  fprintf(stdout, "[Memtable] micros: %lu", end_us - start_us);
}