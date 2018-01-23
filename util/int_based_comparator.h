#ifndef STORAGE_LEVELDB_DB_INT_BASED_COMPARATOR_H
#define STORAGE_LEVELDB_DB_INT_BASED_COMPARATOR_H

#include "leveldb/comparator.h"
#include "leveldb/slice.h"
namespace leveldb {
class IntBasedComparator : public Comparator {
 public:
  IntBasedComparator() = default;
  ~IntBasedComparator() = default;;

  int Compare(const Slice& a, const Slice& b) const {
    int numa = strtol(a.data(), (char**) (a.data() + a.size()), 10);
    int numb = strtol(b.data(), (char**) (b.data() + b.size()), 10);
    if (numa < numb) return -1;
    if (numa > numb) return 1;
    return 0;
  }

  const char* Name() const { return "IntegerBasedComparator"; }

  virtual void FindShortestSeparator(std::string* start, const Slice& limit) const = 0;

  virtual void FindShortSuccessor(std::string* key) const = 0;

};

} // namespace leveldb

#endif // STORAGE_LEVELDB_DB_INT_BASED_COMPARATOR_H
