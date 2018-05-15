// Ref: https://www.usenix.org/system/files/conference/fast18/fast18-hwang.pdf


#ifndef STORAGE_LEVELDB_INDEX_FAST_PM_BTREE_H_
#define STORAGE_LEVELDB_INDEX_FAST_PM_BTREE_H_

#include <unistd.h>
#include <stdio.h>
#include <stdint.h>
#include <time.h>
#include <stdlib.h>
#include <math.h>
#include <iostream>
#include <fstream>
#include <vector>
#include <string.h>
#include <cassert>
#include <climits>
#include <future>
#include <mutex>
#include <cstdint>
#include "util/persist.h"

#define PAGESIZE 512
#define CACHE_LINE_SIZE 64
#define IS_FORWARD(c) (c % 2 == 0)

using entry_key_t = int64_t;

class Page;

class BTree{
private:
  int height;
  char* root;

public:
  BTree();
  void SetNewRoot(char*);
  void GetNumberOfNodes();
  // insert the key in the leaf node
  void Insert(entry_key_t, char*);
  // store the key into the node at the given level
  void InsertInternal(char*, entry_key_t, char*, uint32_t);
  void Delete(entry_key_t);
  void DeleteInternal(entry_key_t, char*, uint32_t, entry_key_t*, bool*, Page**);
  char* Search(entry_key_t);
  // Function to search keys from "min" to "max"
  void RangeSearch(entry_key_t, entry_key_t, unsigned long*);

  friend class Page;
};

class Header{
private:
  Page* leftmost_ptr;         // 8 bytes
  Page* sibling_ptr;          // 8 bytes
  uint32_t level;             // 4 bytes
  uint8_t switch_counter;     // 1 bytes
  uint8_t is_deleted;         // 1 bytes
  int16_t last_index;         // 2 bytes
  std::mutex *mtx;            // 8 bytes

  friend class Page;
  friend class BTree;

public:
  Header() {
    mtx = new std::mutex();
    leftmost_ptr = NULL;
    sibling_ptr = NULL;
    switch_counter = 0;
    last_index = -1;
    is_deleted = false;
  }

  ~Header() {
    delete mtx;
  }
};

class Entry{
private:
  entry_key_t key; // 8 bytes
  char* ptr; // 8 bytes

public :
  Entry(){
    key = LONG_MAX;
    ptr = NULL;
  }

  friend class Page;
  friend class BTree;
};

const int cardinality = (PAGESIZE-sizeof(Header))/sizeof(Entry);
const int count_in_line = CACHE_LINE_SIZE / sizeof(Entry);

class Page{
private:
  Header hdr;  // header in persistent memory, 16 bytes
  Entry records[cardinality]; // slots in persistent memory, 16 bytes * n

public:
  friend class BTree;

  void *operator new(size_t size) {
    void *ret;
    posix_memalign(&ret,64,size);
    return ret;
  }

  Page(uint32_t level = 0);
  Page(Page* left, entry_key_t key, Page* right, uint32_t level = 0);
  inline int Count();
  inline bool RemoveKey(entry_key_t key);
  bool Remove(BTree* bt, entry_key_t key, bool only_rebalance = false, bool with_lock = true);
  /*
   * Although we implemented the rebalancing of B+-Tree, it is currently blocked for the performance.
   * Please refer to the follow.
   * Chi, P., Lee, W. C., & Xie, Y. (2014, August).
   * Making B+-tree efficient in PCM-based main memory. In Proceedings of the 2014
   * international symposium on Low power electronics and design (pp. 69-74). ACM.
   */
  bool RemoveRebalancing(BTree* bt, entry_key_t key, bool only_rebalance = false, bool with_lock = true);
  inline void InsertKey(entry_key_t key, char* ptr, int* num_entries, bool flush = true, bool update_last_index = true);
  // Insert a new key - FAST and FAIR
  Page* Store(BTree* bt, char* left, entry_key_t key, char* right, bool flush, bool with_lock, Page* invalid_sibling = nullptr);

  // Search keys with linear search
  void LinearRangeSearch(entry_key_t min, entry_key_t max, unsigned long* buf);
  char* LinearSearch(entry_key_t key);

};

/*
 * class BTree
 */

#endif // STORAGE_LEVELDB_INDEX_FAST_PM_BTREE_H_
