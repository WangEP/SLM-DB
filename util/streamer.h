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
  Streamer(SequentialFile* file) : file_(file) {
    current_ = 0;
    finished_ = false;
    char* scratch = new char[BUFFER_SIZE];
    Status s;
    buffer_ = new Slice();
    s = file_->Read(BUFFER_SIZE, buffer_, scratch);
    scratch = new char[BUFFER_SIZE];
    future_buffer_ = new Slice();
    s = file_->Read(BUFFER_SIZE, future_buffer_, scratch);
  }

  ~Streamer() {
    delete file_;
  }

  bool eof() {
    return buffer_ == nullptr || buffer_->size() == 0 ;
  }

  void Get(Slice* result) {
    std::string ss;
    ss.clear();
    char c;
    while (!eof() && buffer_->data()[current_] != '\t') {
      c = buffer_->data()[current_];
      ss.append(1, c);
      current_++;
      MaybeSwap();
    }
    current_++;
    char *p = new char[ss.size()];
    strncpy(p, ss.data(), ss.size());
    *result = Slice(p, ss.size());
    MaybeSwap();
  }

  Status status()const {
    return status_;
  }

 private:
  void MaybeSwap() {
    if (current_ < buffer_->size() && buffer_->data()[current_] == '\000') {
      finished_ = true;
      delete buffer_;
      delete future_buffer_;
      buffer_ = NULL;
    }
    else if (current_ >= buffer_->size()) {
      delete buffer_;
      buffer_ = future_buffer_;
      current_ = 0;
      future_buffer_ = NULL;
      if (!finished_) {
          auto handler = std::async(std::launch::async, [this](){
            future_buffer_ = new Slice();
            char* scratch = new char[BUFFER_SIZE];
            Status s;
            s = file_->Read(BUFFER_SIZE, future_buffer_, scratch);
            if (!s.ok()) {
              status_ = s;
            }
            if (future_buffer_->size() < BUFFER_SIZE) {
              finished_ = true;
            }
          });
      }
    }
  }

  const uint16_t BUFFER_SIZE = 1 << 12;
  SequentialFile* file_;
  size_t current_;
  Slice* buffer_;
  Slice* future_buffer_;
  bool finished_;
  Status status_;
};

} // namespace leveldb

#endif //STORAGE_LEVELDB_DB_STREAMER_H
