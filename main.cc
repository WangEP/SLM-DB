#include <iostream>
#include <sstream>
#include "leveldb/db.h"
#include "leveldb/table_builder.h"

uint64_t clflush_cnt = 0;

void standard_db_test() {
  leveldb::DB* db;
  leveldb::Options options;
  options.filter_policy = NULL;
  options.create_if_missing = true;
  std::string dbpath = "/tmp/testdb";
  leveldb::Status status = leveldb::DB::Open(options, dbpath, &db);
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
  leveldb::DB* db;
  leveldb::Options options;
  options.filter_policy = NULL;
  options.create_if_missing = true;
  options.compression = leveldb::kNoCompression;
  options.write_buffer_size = 8000;
  std::string dbpath = "/tmp/testdb";
  leveldb::Status status = leveldb::DB::Open(options, dbpath, &db);
  assert(status.ok());
  for (auto i = 0; i < 3000; i++) {
    std::stringstream key;
    key << "key" << i ;
    std::stringstream value;
    value << "value" << i ;
    status = db->Put(leveldb::WriteOptions(), key.str(), value.str());
  }
  std::string val;
  status = db->Get(leveldb::ReadOptions(), "key1",  &val);
  std::cout << val << "\n";
  status = db->Get(leveldb::ReadOptions(), "key50", &val);
  std::cout << val << "\n";
}

int main(int argc, char** argv) {
  //standard_db_test();
  sst_db_test();
}