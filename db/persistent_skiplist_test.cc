#include <iostream>
#include "util/testharness.h"
#include "persistent_skiplist.h"

uint64_t WRITE_LATENCY_IN_NS = 1000;
uint64_t clflush_cnt = 0;

namespace leveldb {
class SkiplistTest {
};

TEST(SkiplistTest, TEST1) {
  const Comparator* cmp = BytewiseComparator();
  PersistentSkiplist skiplist(cmp);
  for (int i = 0; i < 1000; i++) {
    std::string num = std::to_string(i);
    char* p = new char[num.size()];
    memcpy(p, num.data(), num.size());
    Slice slice(p, num.size());
    skiplist.Insert(slice, slice);
  }
  PersistentSkiplist::Node* node = skiplist.Find(std::to_string(0));
  while (node->next[0] != NULL) {
    ASSERT_TRUE(cmp->Compare(node->prev[0]->key, node->key) < 0);
    node = node->next[0];
  }
}

TEST(SkiplistTest, TEST2) {
  const Comparator* cmp = BytewiseComparator();
  PersistentSkiplist skiplist(cmp);
  for (int i = 0; i < 1000; i++) {
    std::string num = std::to_string(i);
    char* p = new char[num.size()];
    memcpy(p, num.data(), num.size());
    Slice slice(p, num.size());
    skiplist.Insert(slice, slice);
  }
  for (int i = 0; i < 100; i++) {
    std::string s = std::to_string(i);
    auto node = skiplist.Find(s);
    ASSERT_TRUE(cmp->Compare(s, node->key) == 0);
    ASSERT_TRUE(cmp->Compare(s, node->value) == 0);
  }
}
}

int main() {
  return leveldb::test::RunAllTests();

}