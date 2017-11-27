#include <cstdlib>
#include "task.hh"

namespace leveldb {

Task::Task(void (*fn_ptr)(void *), void *arg)
    : fn_ptr_(fn_ptr),
      arg_(arg) {
  finished = false;
}

Task::~Task() {
}

void Task::operator()() {
  (*fn_ptr_)(arg_);
  finished = true;
}

bool Task::IsFinished() {
  return finished;
}

}
