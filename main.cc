#include <iostream>
#include <sstream>
#include "leveldb/db.h"
#include "leveldb/table_builder.h"
#include "leveldb/global_index.h"

uint64_t clflush_cnt = 0;

void standard_db_test() {
  std::cout << "start\n";
  leveldb::DB* db;
  leveldb::Options options;
  options.filter_policy = NULL;
  options.create_if_missing = true;
  options.global_index = new leveldb::GlobalIndex();
  options.compression = leveldb::kNoCompression;
  std::string dbpath = "/tmp/testdb";
  leveldb::Status status = leveldb::DB::Open(options, dbpath, &db);
  std::cout << "open\n";
  assert(status.ok());
  std::string key = "test";
  std::string val = "test_value";
  status = db->Put(leveldb::WriteOptions(), key, val);
  if (status.ok())
    std::cout << "success put\n";
  std::string outval;
  status = db->Get(leveldb::ReadOptions(), key, &outval);
  if (status.ok())
    std::cout << "success get " << outval << "\n";
  std::cout << key << " : " << outval << std::endl;
}


void sst_db_test() {
  struct timespec start, end;
  int numData = 1000000;
  leveldb::DB* db;
  leveldb::Options options;
  options.global_index = new leveldb::GlobalIndex();
  options.filter_policy = NULL;
  options.create_if_missing = true;
  options.compression = leveldb::kNoCompression;
  options.write_buffer_size = 8000;
  std::string dbpath = "/temp/testdb";
  leveldb::Status status = leveldb::DB::Open(options, dbpath, &db);
  assert(status.ok());
  clock_gettime(CLOCK_MONOTONIC, &start);
  for (auto i = 0; i < numData; i++) {
    std::stringstream key;
    key << "key0key" << i ;
    std::stringstream value;
    value << "value" << i ;
    status = db->Put(leveldb::WriteOptions(), key.str(), value.str());
  }
  clock_gettime(CLOCK_MONOTONIC, &end);
  int64_t elapsed = (end.tv_sec - start.tv_sec)*1000000000 + (end.tv_nsec - start.tv_nsec);
  std::cout << elapsed/1000 << "\tusec\t" << (uint64_t)(1000000*(numData/(elapsed/1000.0))) << "\tOps/sec\tInsertion" << endl;

  clock_gettime(CLOCK_MONOTONIC, &start);
  for (auto i = 0; i < numData; i++) {
    std::stringstream key;
    key << "key0key" << i;
    std::string value;
    status = db->Get(leveldb::ReadOptions(), key.str(), &value);
  }
  clock_gettime(CLOCK_MONOTONIC, &end);
  elapsed = (end.tv_sec - start.tv_sec)*1000000000 + (end.tv_nsec - start.tv_nsec);
  std::cout << elapsed/1000 << "\tusec\t" << (uint64_t)(1000000*(numData/(elapsed/1000.0))) << "\tOps/sec\tRead" << endl;
}

int main(int argc, char** argv) {
  //standard_db_test();
  sst_db_test();
}