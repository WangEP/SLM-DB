#include "ff_btree.h"
#include "ff_btree_iterator.h"

namespace leveldb {

/*
 *  class btree
 */
FFBtree::FFBtree(){
  root = new Page();
  height = 1;
}

void FFBtree::setNewRoot(void* new_root) {
  this->root = new_root;
  clflush((char*)&(this->root),sizeof(void*));
  ++height;
}

void* FFBtree::Search(entry_key_t key){
  Page* p = (Page*)root;

  while(p->hdr.leftmost_ptr != NULL) {
    p = (Page *)p->linear_search(key);
  }

  Page *t;
  while((t = (Page *)p->linear_search(key)) == p->hdr.sibling_ptr) {
    p = t;
    if(!p) {
      break;
    }
  }

//  if(!t || (char *)t != (char *)key) {
//    // printf("NOT FOUND %lu, t = %x\n", key, t);
//    return NULL;
//  }

  return (char *)t;
}

void FFBtree::Insert(entry_key_t key, void* right){ //need to be string
  Page* p = (Page*)root;

  while(p->hdr.leftmost_ptr != NULL) {
    p = (Page*)p->linear_search(key);
  }

  if(!p->store(this, NULL, key, right, true)) { // store
    Insert(key, right);
  }
}

void FFBtree::InsertInternal(void* left, entry_key_t key,
                             void* right, uint32_t level) {
  if(level > ((Page *)root)->hdr.level)
    return;

  Page *p = (Page *)this->root;

  while(p->hdr.level > level)
    p = (Page *)p->linear_search(key);

  if(!p->store(this, NULL, key, right, true)) {
    InsertInternal(left, key, right, level);
  }
}

void FFBtree::Remove(entry_key_t key) {
  Page* p = (Page*)root;

  while(p->hdr.leftmost_ptr != NULL){
    p = (Page*) p->linear_search(key);
  }

  Page *t;
  while((t = (Page *)p->linear_search(key)) == p->hdr.sibling_ptr) {
    p = t;
    if(!p)
      break;
  }

  if(p) {
    if(!p->remove(this, key)) {
      Remove(key);
    }
  }
  else {
    // printf("not found the key to delete %lu\n", key);
  }
}

void FFBtree::RemoveInternal(entry_key_t key, void* ptr, uint32_t level,
                             entry_key_t* deleted_key, bool* is_leftmost_node,
                             Page** left_sibling) {
  if(level > ((Page *)this->root)->hdr.level)
  return;

  Page *p = (Page *)this->root;

  while(p->hdr.level > level) {
    p = (Page *)p->linear_search(key);
  }

  if((char *)p->hdr.leftmost_ptr == ptr) {
    *is_leftmost_node = true;
    return;
  }

  *is_leftmost_node = false;

  for(int i=0; p->records[i].ptr != NULL; ++i) {
    if(p->records[i].ptr == ptr) {
      if(i == 0) {
        if((char *)p->hdr.leftmost_ptr != p->records[i].ptr) {
          *deleted_key = p->records[i].key;
          *left_sibling = p->hdr.leftmost_ptr;
          p->remove(this, *deleted_key, false, false);
          break;
        }
      }
      else {
        if(p->records[i - 1].ptr != p->records[i].ptr) {
          *deleted_key = p->records[i].key;
          *left_sibling = (Page *)p->records[i - 1].ptr;
          p->remove(this, *deleted_key, false, false);
          break;
        }
      }
    }
  }
}

void FFBtree::Range(entry_key_t min, entry_key_t max, unsigned long* buf) {
  Page *p = (Page *)root;

  while(p) {
    if(p->hdr.leftmost_ptr != NULL) {
      // The current page is internal
      p = (Page *)p->linear_search(min);
    }
    else {
      // Found a leaf
      p->linear_search_range(min, max, buf);

      break;
    }
  }
}

FFBtreeIterator* FFBtree::GetIterator() {
  return new FFBtreeIterator(this);
}

}
