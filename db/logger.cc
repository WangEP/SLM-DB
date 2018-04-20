#include "logger.h"

#include <utility>

namespace leveldb {

Logger::Logger(int32_t _level) {
  level = _level;
}

Logger::Logger() {
  Logger(0);
}

void Logger::log(string str, int _level) {
  if ( _level < level ) return;
  cout << str << endl;
}

void Logger::log(string str) {
  log(std::move(str), 0);
}

} // namespace leveldb