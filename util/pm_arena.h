#ifndef STORAGE_LEVELDB_UTIL_PMARENA_H_
#define STORAGE_LEVELDB_UTIL_PMARENA_H_

#include <cstdlib>
#include <iostream>
#include "arena.h"
#include "pmalloc.h"

#define CPU_FREQ_MHZ (1994) // cat /proc/cpuinfo
#define CAS(_p, _u, _v)  (__atomic_compare_exchange_n (_p, _u, _v, false, __ATOMIC_ACQUIRE, __ATOMIC_ACQUIRE))

#define CACHE_LINE_SIZE (64)

namespace leveldb {

class PMArena : public Arena {
 public:
  virtual ~PMArena() {}

  virtual char* AllocateNewBlock(size_t block_bytes) override;

  inline void clflush(char *data, int len);

  extern uint64_t clflush_cnt;

 private:

  static inline void cpu_pause();

  static inline unsigned long read_tsc(void);

  inline void mfence();

};

} // namespace leveldb

#endif // STORAGE_LEVEVDB_UTIL_PMARENA_H_
