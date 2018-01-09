#ifndef STORAGE_LEVELDB_DB_PERSISTENT_SKIPLIST_H_
#define STORAGE_LEVELDB_DB_PERSISTENT_SKIPLIST_H_

#include "port/port_posix.h"
#include "leveldb/iterator.h"

namespace leveldb {
template<typename Key, typename Value, class Comparator>
class PersistentSkiplist {
  class Node;
  class Iterator;

 public:
  explicit PersistentSkiplist(Comparator* cmp);
  ~PersistentSkiplist();
  void Insert(const Key& key, const Value& value);
  Value& Get(const Key& key) const;
  Iterator Find(const Key& key);
  Iterator Begin();
  Iterator End();

 private:
  static const size_t max_level = 32;
  static const float probability = 0.5;

  Comparator comparator;

  Node* head;
  port::Mutex* mutex;
  port::CondVar* signal;

  int RandomLevel();
  Node* MakeNode(Key key, Value value, size_t level);

};

template <typename Key, typename Value, class Comparator>
class PersistentSkiplist<Key, Value, Comparator>::Node {
 public:
  // Initialize empty node
  Node(Key k, Value v, int level) : max_level(level) {
    key = k;
    value = v;
    next = NULL;
    prev = NULL;
    for (auto i = 0; i < max_level; i++) {
      next.push_back(NULL);
    }
  }
  // Set pointer to next node on given level
  void SetNext(int level, Node* node) { next[level] = node; }

  // Set pointer to prev node on base level
  void SetPrev(Node* node) { prev = node; }

  // Get pointer to next node on given level
  Node* Next(int level) { return next[level]; }

  // Get pointer to prev node on base level
  Node* Prev() { return prev; }

  // Return key
  Key GetKey() { return key; };

  // Return value
  Value GetValue() { return value; }

 private:
  Key key;
  Value value;
  const int max_level;
  std::vector<Node*> next;
  Node* prev;
};

template <typename Key, typename Value, class Comparator>
class PersistentSkiplist<Key, Value, Comparator>::Iterator {
 public:
  // Initialize iterator
  Iterator(Node* node) : node(node) { }

  // Iterate to next node on base level
  void Next() { node = node->Next(0); }

  // Iterate to prev node on base level
  void Prev() { node = node->Prev(); };

  // Check if iterator is valid
  bool Valid() const { return node != NULL; }

  // Return the node
  Node* GetNode() { return node; }

  // Return key of the node
  const Key& GetKey() { return node->GetKey(); }

  // Return value of the node
  const Value& GetValue() { return node->GetValue(); }
 private:
  Node* node;
};

template<typename Key, typename Value, class Comparator>
PersistentSkiplist<Key, Value, Comparator>::PersistentSkiplist(Comparator* cmp)
    : comparator(cmp),
      mutex(new port::Mutex),
      signal(new port::CondVar(mutex)),
      head(MakeNode(Key(), Value(), max_level)) {
}

template<typename Key, typename Value, class Comparator>
PersistentSkiplist<Key, Value, Comparator>::~PersistentSkiplist() {
  delete mutex;
  delete signal;
  auto iterator = Begin();
  while (iterator.Valid()) {
    auto node = iterator.GetNode();
    delete node;
    iterator.Next();
  }
}

}

#endif //STORAGE_LEVELDB_DB_PERSISTENT_SKIPLIST_H_
