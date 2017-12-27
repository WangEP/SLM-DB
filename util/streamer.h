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
    char p[32];
    Slice* prefix = new Slice();
    s = file_->Read(32, prefix, p);
    BUFFER_SIZE = std::stoul(prefix->ToString());
    if (BUFFER_SIZE > 0) {
      char *scratch = new char[BUFFER_SIZE];
      buffer_ = new Slice();
      s = file_->Read(BUFFER_SIZE, buffer_, scratch);
    }
  }

  ~Streamer() {
    buffer_->clear();
    delete buffer_;
    delete file_;
  }

  bool eof() {
    return buffer_ == nullptr || buffer_->size() == 0 || buffer_->size() <= current_;
  }

  void Get(Slice* result) {
    std::string ss;
    ss.clear();
    char c;
    while (!eof() && buffer_->data()[current_] != '\t') {
      c = buffer_->data()[current_];
      ss.append(1, c);
      current_++;
    }
    current_++;
    char *p = new char[ss.size()];
    memcpy(p, ss.data(), ss.size());
    *result = Slice(p, ss.size());
  }

  Status status()const {
    return status_;
  }

 private:

  uint64_t BUFFER_SIZE;
  SequentialFile* file_;
  size_t current_;
  Slice* buffer_;
  Status status_;
};

} // namespace leveldb

#endif //STORAGE_LEVELDB_DB_STREAMER_H
