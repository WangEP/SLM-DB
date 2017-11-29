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
  Streamer(uint64_t buffer_size, SequentialFile* file)
      :  file_(file) {
    Status s;
    current_ = 0;
    char p[32];
    Slice* prefix = new Slice();
    s = file->Read(64, prefix, p);
    uint64_t hash = std::stoul(prefix->ToString());
    if (BUFFER_SIZE > 0) {
      char *scratch = new char[BUFFER_SIZE];
      buffer_ = new Slice();
      s = file_->Read(BUFFER_SIZE, buffer_, scratch);
    }
  }

  ~Streamer() {
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
    if (current_ >= buffer_->size() || buffer_->data()[current_] == '\000') {
      delete buffer_;
      buffer_ = NULL;
    }
    char *p = new char[ss.size()];
    memcpy(p, ss.data(), ss.size());
    *result = Slice(p, ss.size());
    assert(*(result->data()) != '\000');
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
