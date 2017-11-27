#ifndef LEVELDB_THREAD_POOL_HH
#define LEVELDB_THREAD_POOL_HH

#include <port/port.h>
#include <vector>
#include <deque>
#include "task.hh"

namespace leveldb {

class ThreadPool {
 public:
  ThreadPool();
  ThreadPool(int pool_size);
  ~ThreadPool();
  void* ExecuteThread();
  int AddTask(Task* task);

 private:
  size_t pool_size_;
  port::Mutex* task_mutex_;
  port::CondVar* task_cond_var_;
  std::vector<pthread_t> threads_;
  std::deque<Task*> tasks_;
  volatile int pool_state_;
};

}


#endif //LEVELDB_THREAD_POOL_HH
