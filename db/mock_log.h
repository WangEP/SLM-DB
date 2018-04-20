#ifndef DB_MOCK_LOG_H
#define DB_MOCK_LOG_H

#include "log_writer.h"
#include "log_reader.h"

namespace leveldb {
namespace log {

class MockWriter : public Writer {
 public:
  explicit MockWriter(WritableFile *dest) : Writer(dest) {}

  MockWriter(WritableFile *file, uint64_t size) : Writer(file, size) {

  }

  virtual Status AddRecord(const Slice& slice) override {
    return Status::OK();
  }

};

class MockReader : public Reader {
 public:
  MockReader(SequentialFile *file,
             Reporter *reporter,
             bool checksum,
             uint64_t initial_offset)
      : Reader(file, reporter, checksum, initial_offset) {}

  virtual bool ReadRecord(Slice* record, std::string* scratch) override {
    return false;
  }

  virtual uint64_t LastRecordOffset() override {
    return 0;
  }

};

} // namespace log
} // namespace leveldb


#endif //DB_MOCK_LOG_H
