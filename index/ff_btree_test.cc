#include "ff_btree.h"
#include "util/testharness.h"
#include "util/testutil.h"

namespace leveldb {

class FFBtreeTest { };

TEST(FFBtree, Mix1) {
  FFBtree btree;
  for (int i = 1; i < 100; i++) {
    btree.Insert(i, (void*)i);
  }
  for (int i = 1; i < 100; i++) {
    void* ptr = btree.Search(i);
    assert(ptr == (void*)i);
    ASSERT_EQ(ptr, (void*)i);
  }
}

}

int main() {
  return leveldb::test::RunAllTests();
}

