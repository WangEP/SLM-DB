#ifndef NVB
#define NVB
#include <numa.h>
#include <cstring>
#include <cassert>
#include <iostream>
#include <array>
#include <vector>
#include <queue>
#include <thread>
#include <sstream>
#include "util/persist.h"

#define CAS(_p, _u, _v)  (__atomic_compare_exchange_n (_p, _u, _v, false, __ATOMIC_ACQUIRE, __ATOMIC_ACQUIRE))

// #define SplitTime
// #define WritingTime
// #define MergeTime
// #define RemoveTime
#define PAGESIZE (256)
// #define MULTITHREAD
// #define EXTRA

bool is_numa = numa_max_node() > 0;

using namespace std;

struct Key {
  static constexpr size_t kKeySize = 64;
  char arr[kKeySize];
  Key() = default;
  Key(const char* _key) {
    auto len = strlen(_key);
    strncpy(arr, _key, len < kKeySize? len : kKeySize);
    arr[kKeySize-1] = '\0';
  }
  Key(const char* _key, size_t len) {
    strncpy(arr, _key, len < kKeySize? len : kKeySize);
    arr[kKeySize-1] = '\0';
  }
  Key& operator=(const char* _key) {
    auto len = strlen(_key);
    strncpy(arr, _key, len < kKeySize? len : kKeySize);
    arr[kKeySize-1] = '\0';
  }
  Key& operator=(const leveldb::Slice& _key) {
    auto len = _key.size();
    strncpy(arr, _key.data(), len < kKeySize? len : kKeySize);
    arr[kKeySize-1] = '\0';
  }
  Key& operator=(const string& _key) {
    auto len = _key.size();
    strncpy(arr, _key.data(), len < kKeySize? len : kKeySize);
    arr[kKeySize-1] = '\0';
  }
  char operator[](size_t i) {
    assert(i < kKeySize);
    return arr[i];
  }
  bool operator<(const Key& _key) {
    return strcmp(arr, _key.arr) < 0;
  }
  bool operator<=(const Key& _key) {
    return strcmp(arr, _key.arr) <= 0;
  }
  bool operator==(const Key& _key) {
    return strcmp(arr, _key.arr) == 0;
  }
  bool operator!=(const Key& _key) {
    return strcmp(arr, _key.arr) != 0;
  }
  bool operator>(const Key& _key) {
    return strcmp(arr, _key.arr) > 0;
  }
  bool operator>=(const Key& _key) {
    return strcmp(arr, _key.arr) >= 0;
  }
  bool operator<(const char* _key) {
    return strcmp(arr, _key) < 0;
  }
  bool operator==(const char* _key) {
    return strcmp(arr, _key) == 0;
  }
  bool operator!=(const char* _key) {
    return strcmp(arr, _key) != 0;
  }
  bool operator>(const char* _key) {
    return strcmp(arr, _key) > 0;
  }
};

ostream& operator<<(ostream&, Key&);
istream& operator>>(istream&, Key&);

class BTree;
class Node;

struct LeafEntry {
  Key key;
  void*   ptr;
};

struct InternalEntry {
  Key key;
  int32_t left;
  int32_t right;
  Node* lPtr;
  Node* rPtr;
};

class Node {
 public:
  enum Type : int32_t { Leaf = 0, Internal = 1 };
  Node(Type);
  Node(Type, Key);
  Node(Type, Node*);
  Node(Type, Key, Node*);
  virtual ~Node();

  void *operator new(size_t size) {
    if (is_numa) {
      return numa_alloc_onnode(size, 1);
    } else {
      void* ret;
      return posix_memalign(&ret, 64, size) == 0 ? ret : nullptr;
    }
  }
  void operator delete(void* buffer) {
    if (is_numa) {
      numa_free(buffer, sizeof(Node));
    } else {
      free(buffer);
    }
  }

  void print();
  void print(stringstream& ss);
#ifdef MULTITHREAD
  bool lock() {
    int32_t zero = 0;
    return CAS(&loc, &zero, 1);
  }
  bool unlock() {
    int32_t one = 1;
    return CAS(&loc, &one, 0);
  }
#endif

 private:
  Key splitKey;
  Node* sibling;
  Type type;
#ifdef MULTITHREAD
  int32_t loc = 0;
#endif

  friend class BTree;
  friend class iNode;
  friend class lNode;
};

struct Split {
  Node* original;
  Node* left;
  Node* right;
  Key splitKey;

  ~Split(){
    delete original;
  }
};


struct Merge {
  Node* _left; // Orignal
  Node* _right;
  Node* left;
  Node* right;
};

class lNode : public Node {
 public:
  constexpr static int32_t CARDINALITY
      = (1024-sizeof(Node))/sizeof(LeafEntry);
  // = 2+4*10;
  // = 4;

  // Core
  lNode();
  ~lNode() override;
  void insert(Key, void*);
  void sInsert(int32_t, Key, void*);
  Split* split(Key, void*);
  Merge* merge();
  void remove(Key);
  void* search(Key);
  void* update(Key, void*);

  void *operator new(size_t size) {
    if (is_numa) {
      return numa_alloc_onnode(size, 1);
    } else {
      void* ret;
      return posix_memalign(&ret, 64, size) == 0 ? ret : nullptr;
    }
  }

  void operator delete (void* buffer) {
    if (is_numa) {
      numa_free(buffer, sizeof(lNode));
    } else {
      free(buffer);
    }
  }

  inline LeafEntry& operator[](uint32_t idx) {
      return entry[idx];
  }
  
  // Helper
  bool overflow();
  int32_t count();

  // Debug
  int print();
  int print(stringstream&);
  void sort();

 private:
  array<LeafEntry,CARDINALITY> entry;
};

class iNode : public Node {
 public:
  enum Direction : int32_t { None = -1, Left = 0, Right = 1 };
  constexpr static int32_t CARDINALITY
      = (PAGESIZE-sizeof(Node)-sizeof(int32_t)*3)/sizeof(InternalEntry);

  // Core
  iNode();
  iNode(Split*);
  ~iNode();
  bool overflow();
  int32_t insert(Key, Node*, Node*);
  int32_t sInsert(Key, Node*, Node*);
  Split* split(Key, Node*, Node*);
  void remove(Key, Node*);
  void remove(int32_t, int32_t, Node*, Node*);
  void update(int32_t, int32_t, Node*, Node*, Node*);
  Merge* merge();
  Node* search(Key);

  void *operator new(size_t size) {
    if (is_numa) {
      return numa_alloc_onnode(size, 1);
    } else {
      void* ret;
      return posix_memalign(&ret, 64, size) == 0 ? ret : nullptr;
    }
  }

  // Helper
  Node* getLeftmostPtr();
  Node* getLeftmostPtr(int32_t);
  Node* getRightmostPtr();
  Node* getRightmostPtr(int32_t);
  LeafEntry* transform(int32_t&, Node*&);
  void transform(LeafEntry*, int32_t&, int32_t&, Node*&, bool);
  bool balancedInsert(LeafEntry*, int32_t, int32_t, int32_t&, Node*);
  void defragmentation(iNode* l, iNode* r, int16_t, Key);
  void block(int32_t, Direction);
  Node* getLeftSibling(int32_t, iNode::Direction);
  int32_t getParent(int32_t);
  int32_t getParent(Node*, Direction&);
  int32_t getCommonAncestor(int32_t);
  int32_t count();
  void rebalance();

  // Debug
  void print();
  void print(int32_t loc);
  void print(stringstream&);
  void print(int32_t loc, stringstream&);
  int32_t getCnt() {
    return cnt;
  }


  // Test functions
  Node* test_getLeftSibling() {
    int32_t cur = root;
    while (entry[cur].left != -1) {
      cur = entry[cur].left;
    }
    return getLeftSibling(cur, iNode::Left);
  }
  Node* test_getLeftSibling2() {
    int32_t cur = root;
    while (entry[cur].right != -1) {
      cur = entry[cur].right;
    }
    return getLeftSibling(cur, iNode::Right);
  }
  Key test_getCommonAncestor() {
    int32_t cur = root;
    cur = entry[cur].right;
    cur = entry[cur].left;
    return entry[getCommonAncestor(cur)].key;
  }
  Node* test_block() {
    int32_t cur = root;
    cur = entry[cur].right;
    cur = entry[cur].left;
    block(cur, iNode::Right);
    return entry[cur].rPtr;
  }
  Key test_getParent() {
    int32_t cur = root;
    cur = entry[cur].left;
    cur = entry[cur].right;
    cur = entry[cur].left;
    return entry[getParent(cur)].key;
  }
  void test_remove() {
    Node* lSib = (Node*)0x1;
    remove(10, 10, (Node*)111, lSib);
    cout << (int64_t)lSib << endl;
    print();
    remove(9, 7, (Node*)111, lSib);
    cout << (int64_t)lSib << endl;
    print();
  }

 private:
  int32_t root;
  int32_t cnt;
  int32_t deleteCnt;
  array<InternalEntry,CARDINALITY> entry;

  friend class BTree;
};

class BTree {
 private:
  Node* root;
  struct timespec start, end;
  int64_t elapsed = 0;

 public:
  BTree();
  void* search(Key);
  void* insert(Key, void*);
  void* update(Key, void*);
  void remove(Key);
  vector<LeafEntry*> range(Key, Key);

  // Helper
  iNode* findParent(Node*);
  Node* findLeftSibling(Node*);
  void time() {
    cout << elapsed/1000 << "\tusec" << endl;
  }

  // DEBUG
  void sanityCheck();
  void sanityCheck(Node*);

  // Perf test
  void sort();
  void rebal();

  int64_t failedSearch;
};

#endif
