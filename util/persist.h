#ifndef STORAGE_LEVELDB_UTIL_PERSIST_H_
#define STORAGE_LEVELDB_UTIL_PERSIST_H_

#include <cstdlib>
#include <iostream>

#define CPU_FREQ_MHZ (1994) // cat /proc/cpuinfo
#define CAS(_p, _u, _v)  (__atomic_compare_exchange_n (_p, _u, _v, false, __ATOMIC_ACQUIRE, __ATOMIC_ACQUIRE))

#define CACHE_LINE_SIZE (64)

extern uint64_t WRITE_LATENCY_IN_NS;

// NVM PERSIST OPERATIONS //
extern uint64_t clflush_cnt;

static inline void cpu_pause() {
  __asm__ volatile ("pause" ::: "memory");
}

static inline unsigned long read_tsc(void) {
  unsigned long var;
  unsigned int hi, lo;
  asm volatile ("rdtsc" : "=a" (lo), "=d" (hi));
  var = ((unsigned long long int) hi << 32) | lo;
  return var;
}

inline void mfence() {
  asm volatile("mfence":::"memory");
}

inline void clflush(char *data, int len) {
  if (data == NULL) return;
  volatile char *ptr = (char *)((unsigned long)data &~(CACHE_LINE_SIZE-1));
  mfence();
  for (; ptr<data+len; ptr+=CACHE_LINE_SIZE) {
//#if WRITE_LATENCY_IN_NS != 0
    unsigned long etsc = read_tsc() + (unsigned long)(WRITE_LATENCY_IN_NS*CPU_FREQ_MHZ/1000);
//#endif
    asm volatile("clflush %0" : "+m" (*(volatile char *)ptr));
//#if WRITE_LATENCY_IN_NS != 0
    while (read_tsc() < etsc)
      cpu_pause();
//#endif
    clflush_cnt++;
  }
  mfence();
}

static inline void* allocate(size_t size) {
  return malloc(size);
}

#endif // STORAGE_LEVEVDB_UTIL_PMARENA_H_
