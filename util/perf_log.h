#ifndef STORAGE_LEVELDB_UTIL_PERF_LOG_H
#define STORAGE_LEVELDB_UTIL_PERF_LOG_H

#include <cstdio>
#include <cstdint>
#include <sys/time.h>
#include "histogram.h"

namespace leveldb {

namespace  benchmark {

enum Type {
  QUERY = 0,
  VERSION = 1,
  BLOCK = 2,
  MEMTABLE = 3,
};

class PerfLog {
public:
  PerfLog() {
    query_.Clear();
    version_.Clear();
    block_.Clear();
    memtable_.Clear();
  }

  ~PerfLog() = default;

  void LogMicro(Type type, uint64_t micros) {
    switch (type) {
      case QUERY:
        query_.Add(micros);
        break;
      case VERSION:
        version_.Add(micros);
        break;
      case BLOCK:
        block_.Add(micros);
        break;
      case MEMTABLE:
        memtable_.Add(micros);
        break;
    }
  }

  std::string GetInfo() {
    std::string r;
    r.append("Memtable info,\n");
    r.append(memtable_.GetInfo());
    r.append("Version info,\n");
    r.append(version_.GetInfo());
    r.append("Query info,\n");
    r.append(query_.GetInfo());
    r.append("Block info,\n");
    r.append(block_.GetInfo());
    return r;
  }

  std::string GetHistogram() {
    std::string r;
    r.append("Memtable info,\n");
    r.append(memtable_.GetHistogram());
    r.append("Version info,\n");
    r.append(version_.GetHistogram());
    r.append("Query info,\n");
    r.append(query_.GetHistogram());
    r.append("Block info,\n");
    r.append(block_.GetHistogram());
    return r;
  }

private:
  Histogram query_;
  Histogram version_;
  Histogram block_;
  Histogram memtable_;
};

extern void CreatePerfLog();
extern uint64_t NowMicros();
extern void LogMicros(Type, uint64_t);
extern std::string GetInfo();
extern std::string GetHistogram();
extern void ClosePerfLog();

} // namespace benchmark

} // namespace leveldb

#endif // STORAGE_LEVELDB_UTIL_PERF_LOG_H
