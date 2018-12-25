#ifndef STORAGE_LEVELDB_UTIL_PERF_LOG_H
#define STORAGE_LEVELDB_UTIL_PERF_LOG_H

#include <cstdio>
#include <cstdint>
#include <sys/time.h>
#include <unordered_map>
#include "histogram.h"

namespace leveldb {

namespace  benchmark {

enum Type {
  INSERT,
  MEMTABLE,
  VERSION,
  QUERY,
  BLOCK_READ,
  QUERY_VALUE,
  VALUE_COPY,
};

static const Type AllTypes[] = { INSERT, MEMTABLE, VERSION, QUERY, BLOCK_READ, QUERY_VALUE,VALUE_COPY };

class PerfLog {
public:
  PerfLog() {
    names_.insert({Type::INSERT, "Insert"});
    names_.insert({Type::MEMTABLE, "Memtable"});
    names_.insert({Type::VERSION, "Version"});
    names_.insert({Type::QUERY, "Query for file"});
    names_.insert({Type::BLOCK_READ, "Read for data block"});
    names_.insert({Type::QUERY_VALUE, "Query for value"});
    names_.insert({Type::VALUE_COPY, "Copy for value"});
    for (auto type : AllTypes) {
      histograms_.insert({type, Histogram()});
    }
    Clear();
  }

  ~PerfLog() = default;

  void Clear() {
    for (auto type : AllTypes) {
      histograms_.at(type).Clear();
    }
  }

  void LogMicro(Type type, uint64_t micros) {
    histograms_.at(type).Add(micros);
  }

  std::string GetInfo() {
    std::string r;
    for (auto type : AllTypes) {
      r.append(names_.at(type));
      r.append("\n");
      r.append(histograms_.at(type).GetInfo());
    }
    return r;
  }

  std::string GetHistogram() {
    std::string r;
    for (auto type : AllTypes) {
      r.append(names_.at(type));
      r.append("\n");
      r.append(histograms_.at(type).GetHistogram());
    }
    return r;
  }

private:
  std::unordered_map<Type, Histogram> histograms_;
  std::unordered_map<Type, std::string> names_;
};

extern void CreatePerfLog();
extern void ClearPerfLog();
extern uint64_t NowMicros();
extern void LogMicros(Type, uint64_t);
extern std::string GetInfo();
extern std::string GetHistogram();
extern void ClosePerfLog();

} // namespace benchmark

} // namespace leveldb

#endif // STORAGE_LEVELDB_UTIL_PERF_LOG_H
