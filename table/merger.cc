// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <utility>

#include <utility>

#include "table/merger.h"

#include "leveldb/comparator.h"
#include "leveldb/iterator.h"
#include "table/iterator_wrapper.h"

namespace leveldb {

namespace {
class MergingIterator : public Iterator {
 public:
  MergingIterator(const Comparator* comparator, Iterator** children, int n)
      : comparator_(comparator),
        children_(new IteratorWrapper[n]),
        n_(n),
        current_(nullptr),
        direction_(kForward) {
    for (int i = 0; i < n; i++) {
      children_[i].Set(children[i]);
    }
  }

  virtual ~MergingIterator() {
    delete[] children_;
  }

  virtual bool Valid() const {
    return (current_ != nullptr);
  }

  virtual void SeekToFirst() {
    for (int i = 0; i < n_; i++) {
      children_[i].SeekToFirst();
    }
    FindSmallest();
    direction_ = kForward;
  }

  virtual void SeekToLast() {
    for (int i = 0; i < n_; i++) {
      children_[i].SeekToLast();
    }
    FindLargest();
    direction_ = kReverse;
  }

  virtual void Seek(const Slice& target) {
    for (int i = 0; i < n_; i++) {
      children_[i].Seek(target);
    }
    FindSmallest();
    direction_ = kForward;
  }

  virtual void Next() {
    assert(Valid());

    // Ensure that all children are positioned after key().
    // If we are moving in the forward direction, it is already
    // true for all of the non-current_ children since current_ is
    // the smallest child and key() == current_->key().  Otherwise,
    // we explicitly position the non-current_ children.
    if (direction_ != kForward) {
      for (int i = 0; i < n_; i++) {
        IteratorWrapper* child = &children_[i];
        if (child != current_) {
          child->Seek(key());
          if (child->Valid() &&
              comparator_->Compare(key(), child->key()) == 0) {
            child->Next();
          }
        }
      }
      direction_ = kForward;
    }

    current_->Next();
    FindSmallest();
  }

  virtual void Prev() {
    assert(Valid());

    // Ensure that all children are positioned before key().
    // If we are moving in the reverse direction, it is already
    // true for all of the non-current_ children since current_ is
    // the largest child and key() == current_->key().  Otherwise,
    // we explicitly position the non-current_ children.
    if (direction_ != kReverse) {
      for (int i = 0; i < n_; i++) {
        IteratorWrapper* child = &children_[i];
        if (child != current_) {
          child->Seek(key());
          if (child->Valid()) {
            // Child is at first entry >= key().  Step back one to be < key()
            child->Prev();
          } else {
            // Child has no entries >= key().  Position at last entry.
            child->SeekToLast();
          }
        }
      }
      direction_ = kReverse;
    }

    current_->Prev();
    FindLargest();
  }

  virtual Slice key() const {
    assert(Valid());
    return current_->key();
  }

  virtual Slice value() const {
    assert(Valid());
    return current_->value();
  }

  virtual Status status() const {
    Status status;
    for (int i = 0; i < n_; i++) {
      status = children_[i].status();
      if (!status.ok()) {
        break;
      }
    }
    return status;
  }

 private:
  void FindSmallest();
  void FindLargest();

  // We might want to use a heap in case there are lots of children.
  // For now we use a simple array since we expect a very small number
  // of children in leveldb.
  const Comparator* comparator_;
  IteratorWrapper* children_;
  int n_;
  IteratorWrapper* current_;

  // Which direction is the iterator moving?
  enum Direction {
    kForward,
    kReverse
  };
  Direction direction_;
};

void MergingIterator::FindSmallest() {
  IteratorWrapper* smallest = nullptr;
  for (int i = 0; i < n_; i++) {
    IteratorWrapper* child = &children_[i];
    if (child->Valid()) {
      if (smallest == nullptr) {
        smallest = child;
      } else if (comparator_->Compare(child->key(), smallest->key()) < 0) {
        smallest = child;
      }
    }
  }
  current_ = smallest;
}

void MergingIterator::FindLargest() {
  IteratorWrapper* largest = nullptr;
  for (int i = n_-1; i >= 0; i--) {
    IteratorWrapper* child = &children_[i];
    if (child->Valid()) {
      if (largest == nullptr) {
        largest = child;
      } else if (comparator_->Compare(child->key(), largest->key()) > 0) {
        largest = child;
      }
    }
  }
  current_ = largest;
}
}  // namespace

namespace {

class RangeIterator : public Iterator {
 public:
  RangeIterator(const Comparator* comparator, std::vector<Iterator*> iterators, int n);
  ~RangeIterator();

  virtual bool Valid() const;
  virtual void SeekToFirst();
  virtual void SeekToLast();
  virtual void Seek(const Slice& target);
  virtual void Next();
  virtual void Prev();
  virtual Slice key() const;
  virtual Slice value() const;
  virtual Status status() const;
 private:
  std::vector<Iterator*> iterators_;
  const Comparator* comparator_;
  int size_;
  int target_;
};

RangeIterator::RangeIterator(const Comparator* comparator,
                             std::vector<Iterator*> iterators, int n)
    : comparator_(comparator), iterators_(std::move(std::move(iterators))), size_(n) {
  Slice key;
  for (int i = 0; i < size_; i++) {
    if (key.empty() || comparator_->Compare(key, iterators_[i]->key()) > 0) {
      target_ = i;
    }
  }
}

RangeIterator::~RangeIterator() {
  for (auto iter : iterators_) {
    delete iter;
  }
};

bool RangeIterator::Valid() const {
  for (auto iter : iterators_)
    if (!iter->Valid())
      return false;
  return true;
}

void RangeIterator::SeekToFirst() {
  for (auto iter : iterators_) {
    iter->SeekToFirst();
  }
}

void RangeIterator::SeekToLast() {
  for (auto iter : iterators_) {
    iter->SeekToLast();
  }
}

void RangeIterator::Seek(const Slice& target) {
  return; // skip
  Slice key;
  for (int i = 0; i < size_; i++) {
    iterators_[i]->Seek(target);
    if (key.empty() || comparator_->Compare(key, iterators_[i]->key()) > 0) {
      target_ = i;
    }
  }
}

void RangeIterator::Next() {
  for (int i = 0; i < size_; i++)
    if (i != target_ && comparator_->Compare(iterators_[i]->key(), iterators_[target_]->key()) == 0)
      iterators_[i]->Next();
  iterators_[target_]->Next();
  for (int i = 0; i < size_; i++)
    if (comparator_->Compare(iterators_[i]->key(), iterators_[target_]->key()) < 0) {
      target_ = i;
    }
}

void RangeIterator::Prev() {
  // skip for now
}

Slice RangeIterator::key() const {
  return iterators_[target_]->key();
}

Slice RangeIterator::value() const {
  return iterators_[target_]->value();
}

Status RangeIterator::status() const {
  for (auto iter : iterators_)
    if (!iter->status().ok()) {
      return iter->status();
    }
  return Status::OK();
}

} // namespace

Iterator* NewMergingIterator(const Comparator* cmp, Iterator** list, int n) {
  assert(n >= 0);
  if (n == 0) {
    return NewEmptyIterator();
  } else if (n == 1) {
    return list[0];
  } else {
    return new MergingIterator(cmp, list, n);
  }
}

Iterator* NewRangeIterator(const Comparator* cmp, std::vector<Iterator*> list, int n) {
  assert(n >= 0);
  if (n == 0) {
    return NewEmptyIterator();
  } else if (n == 1) {
    return list[0];
  } else {
    return new RangeIterator(cmp, list, n);
  }
  
}

}  // namespace leveldb
