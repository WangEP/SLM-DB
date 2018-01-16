#ifndef STORAGE_LEVELDB_DB_PERSISTENT_SKIPLIST_H_
#define STORAGE_LEVELDB_DB_PERSISTENT_SKIPLIST_H_

#include <vector>
#include <ctime>
#include "leveldb/comparator.h"
#include "leveldb/iterator.h"
#include "util/persist.h"

namespace leveldb {
class PersistentSkiplist {
 public:
  struct Node;

  explicit PersistentSkiplist(const Comparator* cmp);
  PersistentSkiplist(const Comparator* cmp, Node* first, Node* last, size_t size);
  ~PersistentSkiplist();
  size_t ApproximateMemoryUsage();
  Node* Insert(const Slice& key, const Slice& value);
  Node* Find(const Slice& key);

  void Erase(Node* first, Node* last); // erase nodes in range [first, last]

  Node* Head() { return head; }
  Node* Tail() { return tail; }

 private:
  static const size_t max_level = 32;

  size_t current_level;
  size_t current_size;

  const Comparator* comparator;

  Node* head;
  Node* tail;;

  bool Equal(const Slice& a, const Slice& b) const { return (comparator->Compare(a, b) == 0); }
  size_t RandomLevel();
  Node* FindGreaterOrEqual(Slice key);
  Node* MakeNode(Slice key, Slice value, size_t level);
};

struct PersistentSkiplist::Node {
  std::string key;
  std::string value;
  const size_t level;
  std::vector<Node*> next;
  std::vector<Node*> prev;

  Node(const Slice& k, const Slice& v, size_t level) : level(level) {
    key.assign(k.data(), k.size());
    value.assign(v.data(), v.size());
    for (auto i = 0; i < level; i++) {
      next.push_back(NULL);
      prev.push_back(NULL);
    }
  }

  size_t GetSize() { return key.size() + value.size(); }
};

}

#endif //STORAGE_LEVELDB_DB_PERSISTENT_SKIPLIST_H_
