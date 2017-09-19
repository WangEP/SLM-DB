#include "pm_arena.h"

char *leveldb::PMArena::AllocateNewBlock(size_t block_bytes) {
  char* result = (char *) pmalloc(block_bytes);
  blocks_.push_back(result);
  memory_usage_.NoBarrier_Store(
      reinterpret_cast<void*>(MemoryUsage() + block_bytes + sizeof(char*)));
  return result;
}

void leveldb::PMArena::clflush(char *data, int len) {
  volatile char *ptr = (char *)((unsigned long)data &~(CACHE_LINE_SIZE-1));
  mfence();
  for (; ptr<data+len; ptr+=CACHE_LINE_SIZE) {
#if WRITE_LATENCY_IN_NS != 0
    unsigned long etsc = read_tsc() + (unsigned long)(WRITE_LATENCY_IN_NS*CPU_FREQ_MHZ/1000);
#endif
    asm volatile("clflush %0" : "+m" (*(volatile char *)ptr));
#if WRITE_LATENCY_IN_NS != 0
    while (read_tsc() < etsc)
      cpu_pause();
#endif
    clflush_cnt++;
  }
  mfence();

}

void leveldb::PMArena::mfence() {
  asm volatile("mfence":::"memory");
}

unsigned long leveldb::PMArena::read_tsc(void) {
  unsigned long var;
  unsigned int hi, lo;
  asm volatile ("rdtsc" : "=a" (lo), "=d" (hi));
  var = ((unsigned long long int) hi << 32) | lo;
  return var;
}

void leveldb::PMArena::cpu_pause() {
  __asm__ volatile ("pause" ::: "memory");
}
