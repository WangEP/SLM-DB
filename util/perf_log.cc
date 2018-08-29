#include <cstdio>
#include <sys/time.h>
#include <cstdint>
#include <cstdarg>
#include "perf_log.h"

namespace leveldb {

namespace benchmark {

static PerfLog* log;

void CreatePerfLog() {
  log = new PerfLog;
}

void ClearPerfLog() {
  if (log == nullptr) return;
  log->Clear();
};

uint64_t NowMicros() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return static_cast<uint64_t>(tv.tv_sec) * 1000000 + tv.tv_usec;
}

void LogMicros(Type type, uint64_t micros) {
  if (log == nullptr) return;
  log->LogMicro(type, micros);
}

std::string GetInfo() {
  if (log == nullptr) return std::string();
  return log->GetInfo();
}

std::string GetHistogram() {
  if (log == nullptr) return std::string();
  return log->GetHistogram();
}

void ClosePerfLog() {
  delete log;
  log = nullptr;
}

}

}
