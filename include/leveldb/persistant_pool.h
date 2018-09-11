#ifndef STORAGE_LEVELDB_UTIL_PERSISTANT_POOL_H_
#define STORAGE_LEVELDB_UTIL_PERSISTANT_POOL_H_

#include <string>


namespace leveldb {

namespace nvram {

extern void create_pool(const std::string& dir, const size_t& s);
extern void close_pool();
extern void pfree(void*);
extern void* pmalloc(size_t);
extern void stats();

} // namespace nvram

} // namespace leveldb

#endif // STORAGE_LEVELDB_UTIL_PERSISTANT_POOL_H_
