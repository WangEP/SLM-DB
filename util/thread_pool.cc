#include <thread>
#include "leveldb/env.h"
#include "port/port_posix.h"
#include "thread_pool.hh"

namespace leveldb {

const int STARTED = 0;
const int STOPPED = 1;

void* StartThread(void* arg) {
  ThreadPool* tp = (ThreadPool*) arg;
  tp->ExecuteThread();
  return NULL;
}

ThreadPool::ThreadPool()
    : pool_size_(std::max<size_t>(1u, std::thread::hardware_concurrency())),
      task_mutex_(new port::Mutex),
      task_cond_var_(new port::CondVar(task_mutex_))  {

}

ThreadPool::ThreadPool(int pool_size)
    : pool_size_(std::max<int>(1, pool_size)) {
  pool_state_ = STARTED;
  for (int i = 0; i < pool_size_; i++) {
    pthread_t tid;
    port::PthreadCall("thread create", pthread_create(&tid, NULL, StartThread, (void*) this));
    threads_.push_back(tid);
  }
}

ThreadPool::~ThreadPool() {
  if (pool_state_ != STOPPED) {
    task_mutex_->Lock();
    pool_state_ = STOPPED;
    task_mutex_->Unlock();
    task_cond_var_->SignalAll();
    for (auto thread : threads_) {
      void* result;
      port::PthreadCall("thread join", pthread_join(thread, &result));
      task_cond_var_->SignalAll();
    }
  }
}


void *ThreadPool::ExecuteThread() {
  Task* task = NULL;
  while (true) {
    task_mutex_->Lock();
    while(pool_state_ != STOPPED && tasks_.empty()) {
      task_cond_var_->Wait();
    }
    if (pool_state_ == STOPPED) {
      task_mutex_->Unlock();
      pthread_exit(NULL);
    }

    task = tasks_.front();
    tasks_.pop_front();
    task_mutex_->Unlock();
    (*task)();
    delete task;
  }
  return NULL;
}

int ThreadPool::AddTask(Task *task) {
  task_mutex_->Lock();
  tasks_.push_back(task);
  task_cond_var_->Signal();
  task_mutex_->Unlock();
  return 0;
}

}