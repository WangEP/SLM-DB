#include "iostream"
#include "leveldb/db.h"
#include "leveldb/status.h"

int main(int argc, char** argv) {
  std::cout << "Hello, hac" << std::endl;
  leveldb::DB* db;
  leveldb::Options options;
  options.create_if_missing = true;
  std::string dbpath = "testdb";
  leveldb::Status status = leveldb::DB::Open(options, dbpath, &db);
  assert(status.ok());

  std::string key = "test";
  std::string val = "test_value";
  status = db->Put(leveldb::WriteOptions(), key, val);
  std::string outval;
  status = db->Get(leveldb::ReadOptions(), key, &outval);

  std::cout << key << " : " << outval << std::endl;
}