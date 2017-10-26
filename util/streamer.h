#ifndef STORAGE_LEVELDB_DB_STREAMER_H
#define STORAGE_LEVELDB_DB_STREAMER_H

#include <include/leveldb/status.h>
#include <include/leveldb/env.h>
#include <queue>
#include <future>
#include <sstream>

namespace leveldb {

class Streamer {
 public:
  Streamer(SequentialFile* file) : file_(file) { }

  ~Streamer() {
    delete file_;
  }

  bool eof() {
    return finished_ && current_ >= buffer_.size();
  }

  std::string Get() {
    std::ostringstream result;
    char c = 0;
    while (c != '\t') {
      c = buffer_[current_++];
      if (current_ == buffer_.size()) {
        buffer_ = buffer_queue_.front();
        buffer_queue_.pop();
        current_ = 0;
        if (!finished_) {
          auto handler = std::async(std::launch::async, [this](){
            Slice buffer;
            char* scratch = new char[BUFFER_SIZE];
            Status s;
            s = file_->Read(BUFFER_SIZE, &buffer, scratch);
            buffer_queue_.push(buffer);
            if (!s.ok()) {
              status_ = s;
            }
            if (buffer.size() < BUFFER_SIZE) {
              finished_ = true;
            }
          });
        }
      }
      result << c;
    }
    return result.str();
  }

  Status status()const {
    return status_;
  }

 private:
  const uint16_t BUFFER_SIZE = 1 << 12;
  SequentialFile* file_;
  size_t current_;
  Slice buffer_;
  std::queue<Slice> buffer_queue_;
  bool finished_;
  Status status_;
};

} // namespace leveldb

#endif //STORAGE_LEVELDB_DB_STREAMER_H
