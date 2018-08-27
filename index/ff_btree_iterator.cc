#include "ff_btree_iterator.h"
#include "util/coding.h"

namespace leveldb {

FFBtreeIterator::FFBtreeIterator(FFBtree* b) : valid(true), last(false) {
  btree = b;
  SeekToFirst();
}

bool FFBtreeIterator::Valid() const {
  return valid;
}

void FFBtreeIterator::SeekToFirst() {
  Page* page = (Page*)btree->root;
  while(page->hdr.leftmost_ptr != NULL) {
    page = page->hdr.leftmost_ptr;
  }
  cur_page = page;
  cur = &(page->records[0]);
  index = 0;
  valid = cur != NULL;
}

void FFBtreeIterator::SeekToLast() {
  Page* page = (Page*)btree->root;
  while(page->hdr.leftmost_ptr != NULL) {
    page = page->hdr.leftmost_ptr;
  }
  while(page->hdr.sibling_ptr != NULL) {
    page = page->hdr.sibling_ptr;
  }
  cur_page = page;
  cur = &(cur_page->records[cur_page->count()-1]);
  index = cur_page->count()-1;
  valid = cur != NULL;
}

void FFBtreeIterator::Seek(const entry_key_t& key) {
  Page* page = (Page*)btree->root;
  // search until leaf node
  while(page->hdr.leftmost_ptr!=NULL) {
    page = (Page*)page->linear_search(key);
  }
  uint8_t previous_switch_counter;
  int i;
  Entry* ret = nullptr;
  do {
    int record_count = page->count();
    do {
      previous_switch_counter = page->hdr.switch_counter;
      ret = nullptr;
      if (IS_FORWARD(previous_switch_counter)) {
        for (i = 0; page->records[i].ptr != nullptr && i < record_count; ++i) {
          if (page->records[i].key >= key && page->records[i].ptr != nullptr) {
            ret = &page->records[i];
            index = i;
            break;
          }
        }
      } else {
        for (i = record_count - 1; i >= 0; --i) {
          if (page->records[i].key >= key && page->records[i].ptr != nullptr) {
            ret = &page->records[i];
            index = i;
            break;
          }
        }
      }
    } while (page->hdr.switch_counter != previous_switch_counter);

    if (ret) {
      cur = ret;
      cur_page = page;
    }
  } while (page != page->hdr.sibling_ptr
           && (page = page->hdr.sibling_ptr)
           && !ret);

  valid = ret != nullptr;
}

void FFBtreeIterator::Next() {
  if (last) {
    valid = false;
    return;
  }
  if (cur_page->records[index+1].ptr == nullptr) {
    if (cur_page->hdr.sibling_ptr != nullptr) {
      cur_page = cur_page->hdr.sibling_ptr;
      cur = &(cur_page->records[0]);
      index = 0;
    }
  } else {
    index = index+1;
    cur = &(cur_page->records[index]);
  }
  // if this is a last key, make invalid
  if (cur_page->records[index+1].ptr == nullptr && cur_page->hdr.sibling_ptr == nullptr) {
    last = true;
  }
}

void FFBtreeIterator::Prev() {
  if(index == 0) {
  } else {
    index = index-1;
    cur = &(cur_page->records[index]);
  }
}

entry_key_t FFBtreeIterator::key() const {
  return cur->key;
}

void* FFBtreeIterator::value() const {
  return cur->ptr;
}

} // namespace leveldb