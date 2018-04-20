#ifndef STORAGE_LEVELDB_DB_LOGGER_H
#define STORAGE_LEVELDB_DB_LOGGER_H
#include <iostream>

using namespace std;
namespace leveldb {

class Logger {
 private:
  int32_t level;
  int32_t type;

 public:
  explicit Logger(int32_t _level);

  Logger();

  void log(string log, int _level);

  void log(string log);

};
} // namespace levedb


#endif // STORAGE_LEVELDB_DB_LOGGER_H
