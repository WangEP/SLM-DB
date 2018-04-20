#include <iostream>
#include <chrono>
#include <thread>
#include "leveldb/db.h"
#include "leveldb/index.h"

uint64_t clflush_cnt = 0;
uint64_t WRITE_LATENCY_IN_NS = 1000;
int data_cnt = 5000000;
int val_size = 100;

std::string gen_random(const uint64_t len) {
  std::string s;
  for (int i = 0; i < len; ++i) {
    int randomChar = rand()%(26+26+10);
    if (randomChar < 26)
      s.push_back((char)('a' + randomChar));
    else if (randomChar < 26+26)
      s.push_back((char)('A' + randomChar - 26));
    else
      s.push_back((char)('0' + randomChar - 26 - 26));
  }
  return std::move(s);
}


int main(int argc, char** argv) {
  if (argc < 2) {
    printf("file arg\n");
    return 1;
  }
  struct timespec start, end;
  leveldb::DB* db;
  leveldb::Options options;
  options.filter_policy = nullptr;
  options.create_if_missing = true;
  options.compression = leveldb::kNoCompression;
  options.index = new leveldb::Index;
  char *c = argv[1];
  std::string dbpath(c);
  leveldb::Status status = leveldb::DB::Open(options, dbpath, &db);
  assert(status.ok());
  clock_gettime(CLOCK_MONOTONIC, &start);
  for (auto i = 0; i < data_cnt; i++) {
    std::string key = std::to_string(i);
    std::string value = gen_random(val_size);
    status = db->Put(leveldb::WriteOptions(), key, value);
  }
  clock_gettime(CLOCK_MONOTONIC, &end);
  int64_t elapsed = (end.tv_sec - start.tv_sec)*1000000000 + (end.tv_nsec - start.tv_nsec);
  std::cout << elapsed/1000 << "\tusec\t" << (uint64_t)(1000000*(data_cnt/(elapsed/1000.0))) << "\tOps/sec\tInsertion" << std::endl;
  std::cout << clflush_cnt << "\tclflush count" << std::endl;
  clock_gettime(CLOCK_MONOTONIC, &start);
  for (auto i = 0; i < data_cnt; i++) {
    std::string key = std::to_string(i);
    std::string value;
    status = db->Get(leveldb::ReadOptions(), key, &value);
    if (!status.ok()) {
      printf("%s\n", status.ToString().data());
      return 1;
    }
  }
  clock_gettime(CLOCK_MONOTONIC, &end);
  elapsed = (end.tv_sec - start.tv_sec)*1000000000 + (end.tv_nsec - start.tv_nsec);
  std::cout << elapsed/1000 << "\tusec\t" << (uint64_t)(1000000*(data_cnt/(elapsed/1000.0))) << "\tOps/sec\tRead" << std::endl;
}