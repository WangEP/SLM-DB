#ifndef LEVELDB_TASK_HH
#define LEVELDB_TASK_HH

namespace leveldb {

class Task {
 public:
  Task(void (*fn_ptr)(void*), void* arg);
  ~Task();
  void operator()();
  bool IsFinished();

 private:
  void (*fn_ptr_)(void*);
  void* arg_;
  bool finished;
};

}

#endif //LEVELDB_TASK_HH
