#include <iostream>
#include <sstream>
#include "leveldb/db.h"
#include "leveldb/table_builder.h"

uint64_t clflush_cnt = 0;
uint64_t WRITE_LATENCY_IN_NS = 1000;

std::string gen_random(const uint64_t len) {
  char* s = new char[len];
  for (int i = 0; i < len; ++i) {
    int randomChar = rand()%(26+26+10);
    if (randomChar < 26)
      s[i] = 'a' + randomChar;
    else if (randomChar < 26+26)
      s[i] = 'A' + randomChar - 26;
    else
      s[i] = '0' + randomChar - 26 - 26;
  }
  return std::move(std::string(s, len));
}


int main(int argc, char** argv) {
  if (argc < 2) {
    printf("file arg\n");
    return 1;
  }
  struct timespec start, end;
  int numData = 5 << 6;
  leveldb::DB* db;
  leveldb::Options options;
  options.filter_policy = NULL;
  options.create_if_missing = true;
  options.compression = leveldb::kNoCompression;
  char *c = argv[1];
  std::string dbpath(c);
  leveldb::Status status = leveldb::DB::Open(options, dbpath, &db);
  assert(status.ok());
  clock_gettime(CLOCK_MONOTONIC, &start);
  for (auto i = 0; i < numData; i++) {
    std::string key = std::to_string(i);
    std::string value = gen_random(2 << 16);
    status = db->Put(leveldb::WriteOptions(), key, value);
  }
  clock_gettime(CLOCK_MONOTONIC, &end);
  int64_t elapsed = (end.tv_sec - start.tv_sec)*1000000000 + (end.tv_nsec - start.tv_nsec);
  std::cout << elapsed/1000 << "\tusec\t" << (uint64_t)(1000000*(numData/(elapsed/1000.0))) << "\tOps/sec\tInsertion" << std::endl;

  clock_gettime(CLOCK_MONOTONIC, &start);
  for (auto i = 0; i < numData; i++) {
    std::string v = "valuevalue" + std::to_string(i);
    std::stringstream key;
    key << i;
    std::string value;
    status = db->Get(leveldb::ReadOptions(), key.str(), &value);
    if (!status.ok()) {
      printf("%s\n", status.ToString().data());
      return 1;
    }
    if (v.compare(value) != 0) {
      printf("%s %s %s %lu\n", key.str().data(), v.data(), value.data(), value.size());
    }
  }
  clock_gettime(CLOCK_MONOTONIC, &end);
  elapsed = (end.tv_sec - start.tv_sec)*1000000000 + (end.tv_nsec - start.tv_nsec);
  std::cout << elapsed/1000 << "\tusec\t" << (uint64_t)(1000000*(numData/(elapsed/1000.0))) << "\tOps/sec\tRead" << std::endl;

}