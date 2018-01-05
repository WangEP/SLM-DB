#ifndef STORAGE_LEVELDB_DB_STREAMER_H
#define STORAGE_LEVELDB_DB_STREAMER_H

#include <include/leveldb/status.h>
#include <include/leveldb/env.h>
#include <queue>
#include <future>
#include <sstream>

namespace leveldb {

class Streamer {
  // have to do this prettier
 public:
  Streamer(SequentialFile* file)
      :  file_(file) {
    Status s;
    current_ = 0;
    char* p = new char[32];
    Slice prefix;
    s = file_->Read(32, &prefix, p);
    buffer_size = std::stoul(prefix.ToString());
    delete[] p;
    ptr = new char[buffer_size];
    if (buffer_size > 0) {
      s = file_->Read(buffer_size, &buffer_, ptr);
    }
  }

  ~Streamer() {
    delete ptr;
    delete file_;
  }

  bool eof() {
    return ptr == nullptr || buffer_.size() == 0 || buffer_.size() <= current_;
  }

  Slice Get() {
    size_t start = current_;
    while (!eof() && buffer_[current_] != '\t') {
      current_++;
    }
    current_++;
    return Slice(ptr+start, current_-start);
  }

  Status status()const {
    return status_;
  }

 private:

  uint64_t buffer_size;
  SequentialFile* file_;
  size_t current_;
  char* ptr;
  Slice buffer_;
  Status status_;
};

} // namespace leveldb

#endif //STORAGE_LEVELDB_DB_STREAMER_H
