#include <cstdio>
#include <sys/time.h>
#include <cstdint>
#include <cstdarg>
#include "perf_log.h"

namespace leveldb {

static FILE* block_log;
static FILE* version_log;
static FILE* query_log;
static FILE* compaction_log;
static FILE* compaction_f_log;
static FILE* range_log;

void logMicro(Type type, uint64_t micro) {
  switch (type) {
    case VERSION:
      fprintf(version_log, "%lu\n", micro);
      break;
    case QUERY:
      fprintf(query_log, "%lu\n", micro);
      break;
    case BLOCK:
      fprintf(block_log, "%lu\n", micro);
      break;
    case RANGE:
      fprintf(range_log, "%lu\n", micro);
      break;
    case COMPACTION_F:
      fprintf(compaction_f_log, "%lu\n", micro);
      break;
  }
}

void logMicro(Type type, uint64_t start, uint64_t end) {
  switch (type) {
    case COMPACTION:
      fprintf(compaction_log, "%lu %lu\n", start, end);
      break;
  }
}

uint64_t NowMicros() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
}

void createPerfLog() {
  block_log = fopen("block_log", "w");
  version_log = fopen("version_log", "w");
  query_log = fopen("query_log", "w");
  compaction_log = fopen("compaction_log", "w");
  compaction_f_log = fopen("compaction_f_log", "w");
  range_log = fopen("range_log", "w");
}

void closePerfLog() {
  fclose(block_log);
  fclose(version_log);
  fclose(query_log);
  fclose(compaction_log);
  fclose(range_log);
}

}
