#include <iostream>
#include <chrono>
#include <thread>
#include <util/perf_log.h>
#include "leveldb/db.h"
#include "leveldb/index.h"

int data_cnt = 5000000;
int data_begin = 0;

int main(int argc, char** argv) {
#ifdef PERF_LOG
  leveldb::createPerfLog();
#endif
  struct timespec start, end;
  uint64_t tsize = 0;
  leveldb::DB* db;
  leveldb::Options options;
  options.filter_policy = NULL;
  options.create_if_missing = true;
  options.compression = leveldb::kNoCompression;
  options.index = new leveldb::Index();
  const char *c = "/tmp/testdb";
  std::string dbpath(c);
  leveldb::DestroyDB(dbpath, leveldb::Options());
  leveldb::Status status = leveldb::DB::Open(options, dbpath, &db);
  assert(status.ok());
  clock_gettime(CLOCK_MONOTONIC, &start);
  for (auto i = data_begin; i < data_begin+data_cnt; i++) {
    std::string key = std::to_string(i);
    std::string value = "valuevalue" + std::to_string(i);
    tsize += key.size() + value.size();
    status = db->Put(leveldb::WriteOptions(), key, value);
  }
  clock_gettime(CLOCK_MONOTONIC, &end);
  int64_t elapsed = (end.tv_sec - start.tv_sec)*1000000000 + (end.tv_nsec - start.tv_nsec);
  std::cout << elapsed/1000 << "\tusec\t" << (uint64_t)(1000000*(data_cnt/(elapsed/1000.0))) << "\tOps/sec\tInsertion" << std::endl;
  std::cout << tsize << "\tbytes written" << std::endl;
  clock_gettime(CLOCK_MONOTONIC, &start);
  for (auto i = data_begin; i < data_begin+data_cnt; i++) {
    std::string v = "valuevalue" + std::to_string(i);
    std::string key = std::to_string(i);
    std::string value;
    status = db->Get(leveldb::ReadOptions(), key, &value);
    if (!status.ok()) {
      printf("key %s %s\n", key.data(), status.ToString().data());
      return 1;
    }
    if (v != value) {
      printf("%s %s %s %lu\n", key.data(), v.data(), value.data(), value.size());
    }
  }
  clock_gettime(CLOCK_MONOTONIC, &end);
  elapsed = (end.tv_sec - start.tv_sec)*1000000000 + (end.tv_nsec - start.tv_nsec);
  std::cout << elapsed/1000 << "\tusec\t" << (uint64_t)(1000000*(data_cnt/(elapsed/1000.0))) << "\tOps/sec\tRead" << std::endl;
}