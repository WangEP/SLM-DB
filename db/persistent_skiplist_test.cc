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

TEST(SkiplistTest, Erase) {
  const Comparator* cmp = BytewiseComparator();
  PersistentSkiplist skiplist(cmp);
  for (int i = 0; i < 100; i++) {
    std::string num = std::to_string(i);
    char* p = new char[num.size()];
    memcpy(p, num.data(), num.size());
    Slice slice(p, num.size());
    skiplist.Insert(slice, slice);
  }
  auto left = skiplist.Find("41");
  auto right = skiplist.Find("49");
  for (int i = 41; i <= 49; i++) {
    auto node = skiplist.Find(std::to_string(i));
    assert(node != NULL);
  }
  skiplist.Erase(left, right);
  for (int i = 41; i <= 49; i++) {
    auto node = skiplist.Find(std::to_string(i));
    assert(node == NULL);
  }
}

TEST(SkiplistTest, Constructor) {
  const Comparator* cmp = BytewiseComparator();
  PersistentSkiplist skiplist(cmp);
  for (int i = 0; i < 100; i++) {
    std::string num = std::to_string(i);
    char* p = new char[num.size()];
    memcpy(p, num.data(), num.size());
    Slice slice(p, num.size());
    skiplist.Insert(slice, slice);
  }
  auto left = skiplist.Find("41");
  auto right = skiplist.Find("49");
  skiplist.Erase(left, right);
  // do not care about size for test
  PersistentSkiplist new_skiplist(cmp, left, right, 0);
  for (int i = 41; i <= 49; i++) {
    std::string key = std::to_string(i);
    auto node1 = new_skiplist.Find(key);
    assert(node1 != NULL);
    assert(node1->key == std::to_string(i));
    auto node2 = skiplist.Find(key);
    assert(node2 == NULL);
  }
//  auto node = new_skiplist.Head();
//  while (node != new_skiplist.Tail()) {
//    std::cout << node->key << " ";
//    node = node->next[0];
//  }
}

}

int main() {
  return leveldb::test::RunAllTests();

}