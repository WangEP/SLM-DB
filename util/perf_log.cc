#include <cstdio>
#include <sys/time.h>
#include <cstdint>
#include <cstdarg>
#include "perf_log.h"

namespace leveldb {

static FILE* block_log;

void logMicro(uint64_t micro) {
    fprintf(block_log, "%lu\n", micro);
}

uint64_t NowMicros() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
}

void createPerfLog() {
  block_log = fopen("block_log", "w");
}

void closePerfLog() {
  fclose(block_log);
}


}
