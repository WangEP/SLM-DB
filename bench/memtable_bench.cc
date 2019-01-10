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

Slice RandomString(Random* rnd, int len, std::string* dst) {
  dst->resize(len);
  for (int i = 0; i < len; i++) {
    (*dst)[i] = static_cast<char>(' ' + rnd->Uniform(95));   // ' ' .. '~'
  }
  return Slice(*dst);
}

int main() {
  Random rand(10);
  nvram::create_pool(nvm_dir, nvm_size);
  const Comparator* comparator = BytewiseComparator();
  const InternalKeyComparator icomparator = comparator;
  MemTable* memtable = new MemTable(comparator);
  uint64_t start_us = benchmark::NowMicros();
  std::string s;
  for (uint64_t i = 0; i < N; i++) {
    uint64_t k = rand.Next();
    char key[100];
    snprintf(key, sizeof(key), config::key_format, k);
    Slice value = RandomString(&rand, VAL_SIZE, &s);
    memtable->Add(i, kTypeValue, key, value);
  }
  uint64_t end_us = benchmark::NowMicros();
  fprintf(stdout, "[Memtable] micros: %lu", end_us - start_us);
}