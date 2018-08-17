#include <iostream>
#include <string>
#include <algorithm>
#include "btree_index.h"
#include "nvm_btree.h"

using namespace std;

// BTree //
BTree::BTree() {
  root = new lNode();
  failedSearch = 0;
}

void* BTree::insert(int64_t key, void* ptr) {
  Node* node = root, *lSib = nullptr;
  Split* split = nullptr;
  iNodeSearch:
  while (node->type == Node::Internal) {
    node = ((iNode*)node)->search(key);
    if (node == nullptr) node = root;
  }
#ifdef MULTITHREAD
  if (!node->lock()) {
    node = root;
    goto iNodeSearch;
  }
#endif

  lNode* leaf = (lNode*)node;
  // update if key exists
  if (leaf->search(key)!= nullptr) {
    return leaf->update(key, ptr);
  }
  if (!leaf->overflow()) {
#ifdef WritingTime
    clock_gettime(CLOCK_MONOTONIC, &start);
#endif
    leaf->insert(key, ptr);
#ifdef WritingTime
    clock_gettime(CLOCK_MONOTONIC, &end);
    elapsed += (end.tv_nsec - start.tv_nsec)
      + (end.tv_sec - start.tv_sec)*1000000000;
#endif
#ifdef MULTITHREAD
    node->unlock();
#endif
  } else {
#ifdef SplitTime
    clock_gettime(CLOCK_MONOTONIC, &start);
#endif
    split = leaf->split(key, ptr);
#ifdef SplitTime
    clock_gettime(CLOCK_MONOTONIC, &end);
    elapsed += (end.tv_nsec - start.tv_nsec)
      + (end.tv_sec - start.tv_sec)*1000000000;
#endif
    // We don't have to unlock as we replace the page.
  }

  while (split) {
    lSibAgain:
    iNode* parent = findParent(split->original);
    if (parent != nullptr) {
#ifdef MULTITHREAD
      if (!parent->lock()) goto lSibAgain;
#endif
      iNode::Direction dir;
      int32_t pIdx = parent->getParent(split->original, dir);
      lSib = parent->getLeftSibling(pIdx, dir);
      if (lSib == nullptr) {
        lSib = findLeftSibling(split->original);
      }
      if (lSib != nullptr) {
#ifdef MULTITHREAD
        if (!lSib->lock()) {
          parent->unlock();
          goto lSibAgain;
          // To avoid deadlock.
        }
        if ( lSib->sibling != split->original ) {
          parent->unlock();
          lSib->unlock();
          goto lSibAgain;
        }
#endif
#ifdef SplitTime
        clock_gettime(CLOCK_MONOTONIC, &start);
#endif
        parent->block(pIdx, dir);
        lSib->sibling = split->left;
        clflush((char*)&lSib->sibling, sizeof(void*));
#ifdef SplitTime
        clock_gettime(CLOCK_MONOTONIC, &end);
    elapsed += (end.tv_nsec - start.tv_nsec)
      + (end.tv_sec - start.tv_sec)*1000000000;
#endif
#ifdef MULTITHREAD
        lSib->unlock();
#endif
      } else {
#ifdef MULTITHREAD
        if (split->original->splitKey != -1) {
          parent->unlock();
          goto lSibAgain;
        }
#endif
      }

      if (!parent->overflow()) {
#ifdef WritingTime
        clock_gettime(CLOCK_MONOTONIC, &start);
#endif
        parent->insert(split->splitKey, split->left, split->right);
#ifdef WritingTime
        clock_gettime(CLOCK_MONOTONIC, &end);
    elapsed += (end.tv_nsec - start.tv_nsec)
      + (end.tv_sec - start.tv_sec)*1000000000;
#endif
#ifdef MULTITHREAD
        parent->unlock();
#endif
        delete split;
        split = nullptr;
      } else {
#ifdef SplitTime
        clock_gettime(CLOCK_MONOTONIC, &start);
#endif
        Split* tmp = split;
        split = parent->split(split->splitKey, split->left, split->right);
        delete tmp;
#ifdef SplitTime
        clock_gettime(CLOCK_MONOTONIC, &end);
    elapsed += (end.tv_nsec - start.tv_nsec)
      + (end.tv_sec - start.tv_sec)*1000000000;
#endif
      }
    } else { // Height increased
      if (split->original != root) continue;
      root = (Node*) new iNode(split);
      clflush((char*)&root, sizeof(void*));
      delete split;
      split = nullptr;
    }
  }
  return nullptr;
}

void* BTree::update(int64_t key, void *ptr) {
  Node *p = root;
  while (p->type == Node::Internal) {
    p = ((iNode*) p)->search(key);
    if (p == nullptr) p = root;
  }
  return ((lNode *) p)->update(key, ptr);
}

void *BTree::search(int64_t key) {
  Node* p = root;
  while (p->type == Node::Internal) {
    p = ((iNode*)p)->search(key);
    if (p == nullptr) p = root;
  }

  void* result = ((lNode*)p)->search(key);
  if (result == nullptr) {
    failedSearch++;
  }
  return result;
}

vector<LeafEntry*> BTree::range(int64_t min, int64_t max) {
    vector<lNode*> leaves;
    Node *p = root;
    while (p->type == Node::Internal) {
        p = (Node*)((iNode*)p)->search(min);
        if (p == nullptr) p = root;
    }
    lNode* l = (lNode*)p;
    leaves.push_back(l);
    while (l->sibling != nullptr) {
        l = (lNode*)l->sibling;
        if (l->splitKey < max) leaves.push_back(l);
        else break;
    } // To ensure transactional property and efficient memory allocation.
    vector<LeafEntry*> ret;
    ret.reserve(leaves.size()*lNode::CARDINALITY);
    for (int i = 0; i < leaves.size(); i++) {
        if (i == 0) {
            for (int j = 0; j < lNode::CARDINALITY; j++) {
                if ((*leaves[i])[j].ptr != nullptr
                    && (*leaves[i])[j].key >= min
                        && (*leaves[i])[j].key <= max) {
                    ret.push_back(&(*leaves[i])[j]);
                }
            }
        } else if (i != leaves.size()-1) {
            for (int j = 0; j < lNode::CARDINALITY; j++) {
                // if i != 0, we don't have to check the min condition
                if ((*leaves[i])[j].ptr != nullptr
                    && (*leaves[i])[j].key <= max) {
                    ret.push_back(&(*leaves[i])[j]);
                }
            }
        } else {
            for (int j = 0; j < lNode::CARDINALITY; j++) {
                if ((*leaves[i])[j].ptr != nullptr) {
                    ret.push_back(&(*leaves[i])[j]);
                }
            }
        }
    }
    std::sort(std::begin(ret), std::end(ret), [](LeafEntry* a, LeafEntry* b){
                return a->key < b->key;
            });
    return std::move(ret);
}

vector<LeafEntry*> BTree::range(int64_t min, size_t n) {
  vector<lNode*> leaves;
  size_t cnt = 0;
  Node *p = root;
  while (p->type == Node::Internal) {
    p = (Node*)((iNode*)p)->search(min);
    if (p == NULL) p == root;
  }
  lNode* l = (lNode*)p;
  leaves.push_back(l);
  cnt += l->count();
  while (l->sibling != NULL) {
    l = (lNode*)l->sibling;
    cnt += l->count();
    leaves.push_back(l);
    if (n + lNode::CARDINALITY < cnt) break;
  } // To ensure transactional property and efficient memory allocation.
  vector<LeafEntry*> ret;
  ret.reserve(leaves.size()*lNode::CARDINALITY);
  for (int i = 0; i < leaves.size(); i++) {
    if (i == 0) {
      for (int j = 0; j < lNode::CARDINALITY; j++) {
        if ((*leaves[i])[j].ptr != NULL
            && (*leaves[i])[j].key >= min) {
          ret.push_back(&(*leaves[i])[j]);
        }
      }
    } else {
      for (int j = 0; j < lNode::CARDINALITY; j++) {
        if ((*leaves[i])[j].ptr != NULL) {
          ret.push_back(&(*leaves[i])[j]);
        }
      }
    }
  }
  std::sort(std::begin(ret), std::end(ret), [](LeafEntry* a, LeafEntry* b){
    return a->key < b->key;
  });
  ret.resize(n);
  return std::move(ret);
}

void BTree::remove(int64_t key) {
  Node* p = root, *lSib = nullptr;
  iNode* parent = nullptr;
  iNode::Direction dir = iNode::None;
  int32_t pIdx = -1;
  Merge* m = nullptr;

  while (p->type == Node::Internal) {
    p = (Node*)((iNode*)p)->search(key);
    if (p == nullptr) p = root;
  }

  lNode* l = (lNode*)p;
#ifdef RemoveTime
  clock_gettime(CLOCK_MONOTONIC, &start);
#endif
  l->remove(key);
#ifdef RemoveTime
  clock_gettime(CLOCK_MONOTONIC, &end);
    elapsed += (end.tv_nsec - start.tv_nsec)
      + (end.tv_sec - start.tv_sec)*1000000000;
#endif

  if (l != root && l->count() < (lNode::CARDINALITY/2)) {
    parent = findParent(l);
    pIdx   = parent->getParent(l, dir);
    if (dir == iNode::Right) {
      l = (lNode*)parent->getLeftSibling(pIdx, dir);
      pIdx = parent->getParent(l, dir);
    }
#ifdef MergeTime
    clock_gettime(CLOCK_MONOTONIC, &start);
#endif
    m = l->merge();
#ifdef MergeTime
    clock_gettime(CLOCK_MONOTONIC, &end);
    elapsed += (end.tv_nsec - start.tv_nsec)
      + (end.tv_sec - start.tv_sec)*1000000000;
#endif
  }

  while (m != nullptr) {
    lSib = parent->getLeftSibling(pIdx, dir);
    int32_t lpIdx = pIdx;
    int32_t rpIdx = parent->getParent(m->_right, dir);
    if (lSib == nullptr) {
      lSib = findLeftSibling(m->_left);
    }
    if (m->right == nullptr) {
      if (parent == root && parent->count() == 1) {
        root = m->left;
        clflush((char*)&root, sizeof(void*));
        break;
      } else {
#ifdef RemoveTime
        clock_gettime(CLOCK_MONOTONIC, &start);
#endif
        parent->remove(lpIdx, rpIdx, m->left, lSib);
#ifdef RemoveTime
        clock_gettime(CLOCK_MONOTONIC, &end);
    elapsed += (end.tv_nsec - start.tv_nsec)
      + (end.tv_sec - start.tv_sec)*1000000000;
#endif
      }
    } else {
#ifdef RemoveTime
      clock_gettime(CLOCK_MONOTONIC, &start);
#endif
      parent->update(lpIdx, rpIdx, m->left, m->right, lSib);
#ifdef RemoveTime
      clock_gettime(CLOCK_MONOTONIC, &end);
    elapsed += (end.tv_nsec - start.tv_nsec)
      + (end.tv_sec - start.tv_sec)*1000000000;
#endif
    }
    // delete m->_left;
    // delete m->_right;
    // delete m;
    m = nullptr;
    if (parent != root && parent->count() < (iNode::CARDINALITY/2)) {
      iNode* i = parent;
      parent = findParent(i);
      pIdx = parent->getParent(i, dir);
      if (dir == iNode::Right) {
        i = (iNode*)parent->getLeftSibling(pIdx, dir);
        pIdx = parent->getParent(i, dir);
      }
#ifdef MergeTime
      clock_gettime(CLOCK_MONOTONIC, &start);
#endif
      m = i->merge();
#ifdef MergeTime
      clock_gettime(CLOCK_MONOTONIC, &end);
    elapsed += (end.tv_nsec - start.tv_nsec)
      + (end.tv_sec - start.tv_sec)*1000000000;
#endif
    }
  }
}

iNode* BTree::findParent(Node* child) {
  Node* p = root, *parent = nullptr;
  if (root == child) return nullptr;
  while (p != child) {
    if (p->type == Node::Leaf) {
      return nullptr;
    }
    parent = p;
    p = ((iNode*)p)->search(child->splitKey);
    if (p == nullptr) p = root;
  }
  return (iNode*)parent;
}

Node* BTree::findLeftSibling(Node* target) {
  if (target == root) return nullptr;
  Node* lSib = nullptr;
  iNode* parent = findParent(target);
  if (!parent) return nullptr;
  iNode::Direction dir = iNode::None;
  int32_t pIdx = parent->getParent(target, dir);
  lSib = parent->getLeftSibling(pIdx, dir);
  if (lSib == nullptr) {
    Node* lpSib = findLeftSibling(parent);
    if (lpSib) lSib = ((iNode*)lpSib)->getRightmostPtr();
  }
  return lSib;
}

void lNode::sort() {
  std::sort(entry.begin(), entry.end(),
            [](LeafEntry a, LeafEntry b) {
              return a.key < b.key;
            } );
}

void BTree::sort() {
  Node* p = root;
  while (p->type == Node::Internal) {
    p = (Node*)((iNode*)p)->getLeftmostPtr();
  }
  lNode* l = (lNode*)p;
  while (l) {
    l->sort();
    l = (lNode*)l->sibling;
  }
}

void BTree::rebal() {
  Node* p = root;
  vector<Node*> nodes;
  while (p->type == Node::Internal) {
    nodes.push_back(p);
    p = (Node*)((iNode*)p)->getLeftmostPtr();
  }

  for (auto& left : nodes) {
    iNode* i = (iNode*)left;
    while (i) {
      i->rebalance();
      i = (iNode*)i->sibling;
    }
  }
}

void BTree::sanityCheck() {
  if (root->sibling != nullptr) {
    cout << "ROOT HAS SIBLING!" << endl;
    root->print();
    root->sibling->print();
    exit(1);
  }
  sanityCheck(root);
}

void BTree::sanityCheck(Node* node) {
  if (node->type == Node::Leaf) return;
  vector<Node*> nodes;
  iNode* p = (iNode*)node;

  int32_t stack[iNode::CARDINALITY] = {-1};
  int32_t top = 1;
  int32_t cur = p->root;
  int32_t last = -1;

  while (cur != -1) {
    if (last == stack[top-1]) {
      last = cur;
      if (p->entry[cur].left != -1) {
        stack[top++] = cur;
        cur = p->entry[cur].left;
      } else if (p->entry[cur].right != -1) {
        nodes.push_back(p->entry[cur].lPtr);
        stack[top++] = cur;
        cur = p->entry[cur].right;
      } else {
        nodes.push_back(p->entry[cur].lPtr);
        nodes.push_back(p->entry[cur].rPtr);
        cur = stack[--top];
      }
    } else if (last == p->entry[cur].left) {
      last = cur;
      if (p->entry[cur].right != -1) {
        stack[top++] = cur;
        cur = p->entry[cur].right;
      } else {
        nodes.push_back(p->entry[cur].rPtr);
        cur = stack[--top];
      }
    } else if (last == p->entry[cur].right) {
      last = cur;
      cur = stack[--top];
    }
  }

  Node* n = p->getLeftmostPtr();
  stringstream ss;
  for (auto e : nodes) {
    if (e != n) {
      ss << e << "\t" << n << endl;
      node->print(ss);
      n = p->getLeftmostPtr();
      for (auto f : nodes) {
        if (f == n)
          ss << f << "\t==\t" << n << "\t" << f->splitKey << "\t" << n->splitKey << endl;
        else
          ss << f << "\t!=\t" << n << "\t" << f->splitKey << "\t" << n->splitKey << endl;
        n = n->sibling;
      }
      ss << endl;
      cout << ss.str();
      exit(1);
    }
    n = n->sibling;
  }

  if (p->sibling && p->getRightmostPtr()->sibling != ((iNode*)p->sibling)->getLeftmostPtr()) {
    ss << "WEIRD SIBLING" << endl;
    ss << "p" << endl;
    p->print(ss);
    ss << "p->sibling" << endl;
    p->sibling->print(ss);
    ss << "p->getRightmostPtr()" << endl;
    p->getRightmostPtr()->print(ss);
    ss << "p->getRightmostPtr()->sibling" << endl;
    p->getRightmostPtr()->sibling->print(ss);
    ss << "p->sibling->getLeftmostPtr()" << endl;
    ((iNode*)p->sibling)->getLeftmostPtr()->print(ss);
    cout << ss.str();
    exit(1);
  }

  for (auto p : nodes) {
    sanityCheck(p);
  }
}

// Node //
Node::Node(Type _type, int64_t _splitKey, Node* _sibling) {
  type = _type;
  splitKey = _splitKey;
  sibling = _sibling;
}

Node::Node(Type _type, int64_t _splitKey) {
  type = _type;
  splitKey = _splitKey;
  sibling = nullptr;
}

Node::Node(Type _type, Node* _sibling) {
  type = _type;
  splitKey = -1;
  sibling = _sibling;
}

Node::Node(Type _type) {
  type = _type;
  splitKey = -1;
  sibling = nullptr;
}

Node::~Node() = default;

void Node::print() {
  if (type == Node::Leaf) {
    ((lNode*)this)->print();
  } else {
    ((iNode*)this)->print();
  }
}
void Node::print(stringstream& ss) {
  if (type == Node::Leaf) {
    ((lNode*)this)->print(ss);
  } else {
    ((iNode*)this)->print(ss);
  }
}


// Leaf Node //
lNode::lNode() : Node(Node::Leaf) {
  for (auto &e : entry) {
    e.key = -1;
    e.ptr = nullptr;
  }
}

lNode::~lNode() = default;

bool lNode::overflow() {
  for (int32_t i = 0; i < CARDINALITY; i++ ) {
    if (entry[i].ptr == nullptr) return false;
  }
  return true;
}

void lNode::insert(int64_t key, void* ptr) {
  int32_t loc;
  for (loc = 0; loc < CARDINALITY; loc++) {
    if (entry[loc].ptr == nullptr) break;
  }

  entry[loc].key = key;
  entry[loc].ptr = ptr;
  clflush((char*)&entry[loc], sizeof(LeafEntry));
}

void lNode::sInsert(int32_t idx, int64_t key, void* ptr) {
  entry[idx].key = key;
  entry[idx].ptr = ptr;
}

Split* lNode::split(int64_t key, void* ptr) {
  Split* split = new Split;
  array<LeafEntry, CARDINALITY> temp(entry);
  std::sort(temp.begin(), temp.end(),
            [](LeafEntry a, LeafEntry b) {
              if (b.ptr == nullptr) return true;
              return a.key < b.key;
            } );

  int32_t mIdx   = CARDINALITY/2;
  int64_t median = temp[mIdx].key;

  lNode* left = new lNode();
  lNode* right = new lNode();

  left->sibling   = right;
  right->sibling  = this->sibling;
  left->splitKey  = splitKey;
  right->splitKey = median;

  for (int32_t i = 0; i < CARDINALITY; i++) {
    if (i < mIdx)
      left->sInsert(i, temp[i].key, temp[i].ptr);
    else
      right->sInsert(i-mIdx, temp[i].key, temp[i].ptr);
  }
  if (key < median) left->sInsert(mIdx, key, ptr);
  else right->sInsert(CARDINALITY-mIdx, key, ptr);

  clflush((char*)left, sizeof(lNode));
  clflush((char*)right, sizeof(lNode));

  split->original = this;
  split->left = left;
  split->right = right;
  split->splitKey = median;

  return split;
}

Merge* lNode::merge() {
  Merge* m      = new Merge;
  lNode* _left  = this;
  lNode* _right = (lNode*)sibling;
  array<LeafEntry, CARDINALITY*2> temp {};

  int32_t cnt = 0;
  auto pred = [&cnt](LeafEntry e) {
    if (e.ptr == nullptr) return false;
    cnt++;
    return true;
  };
  copy_if(_left->entry.begin(), _left->entry.end(), temp.begin(), pred);
  copy_if(_right->entry.begin(), _right->entry.end(), temp.begin()+cnt, pred);

  std::sort(temp.begin(), temp.begin()+cnt,
            [](LeafEntry a, LeafEntry b) {
              return a.key < b.key;
            } );

  if (cnt <= CARDINALITY) { // Merge
    lNode* merged = new lNode();
    copy(temp.begin(), temp.begin()+cnt, merged->entry.begin());
    merged->splitKey = _left->splitKey;
    merged->sibling  = _right->sibling;
    clflush((char*)&merged, sizeof(lNode));

    m->_left  = _left;
    m->_right = _right;
    m->left   = merged;
    m->right  = nullptr;
  } else{
    lNode* left    = new lNode();
    lNode* right   = new lNode();
    int32_t mIdx   = cnt/2;
    int64_t median = temp[mIdx].key;

    copy(temp.begin(), temp.begin()+mIdx, left->entry.begin());
    copy(temp.begin()+mIdx, temp.begin()+cnt, right->entry.begin());

    left->splitKey  = _left->splitKey;
    left->sibling   = right;
    right->splitKey = median;
    right->sibling  = _right->sibling;
    clflush((char*)&left, sizeof(lNode));
    clflush((char*)&right, sizeof(lNode));

    m->_left  = _left;
    m->_right = _right;
    m->left   = left;
    m->right  = right;
  }
  return m;
}

void lNode::remove(int64_t key) {
  for (int32_t i = 0; i < CARDINALITY; i++) {
    if (entry[i].key == key && entry[i].ptr != nullptr) {
      entry[i].ptr = nullptr;
      clflush((char*)&entry[i].ptr, sizeof(char*));
      return;
    }
  }
}

void* lNode::update(int64_t key, void *ptr) {
  for (int32_t i = 0; i < CARDINALITY; i++) {
    if (entry[i].key == key && entry[i].ptr != nullptr) {
      void *p = entry[i].ptr;
      entry[i].ptr = ptr;
      clflush((char*) &entry[i].ptr, sizeof(void*));
      return p;
    }
  }
  if (sibling && sibling->splitKey < key) {
    return ((lNode*) sibling)->update(key, ptr);
  }
  return nullptr;
}

void* lNode::search(int64_t key) {
  for (int32_t i = 0; i < CARDINALITY; i++) {
    if (entry[i].key == key && entry[i].ptr != nullptr) {
      return entry[i].ptr;
    }
  }
  if (sibling && sibling->splitKey < key)  {
    return ((lNode*)sibling)->search(key);
  }
  return nullptr;
}

int32_t lNode::count() {
  int32_t cnt = 0;
  for (int i = 0; i < CARDINALITY; i++) {
    if (entry[i].ptr != nullptr) cnt++;
  }
  return cnt;
}

int lNode::print() {
#ifdef MULTITHREAD
  if (loc == 1)
    cout << "LOCEKD" << endl;
#endif
  cout << "Leaf" << endl;
  cout << "sibling: " << sibling << endl;
  cout << "splitKey: " << splitKey << endl;
  int cnt = 0;
  for (int i = 0; i < CARDINALITY; i++) {
    if (entry[i].ptr != nullptr) {
      cout << entry[i].key << "-[p]" << entry[i].ptr << "\t";
      cnt++;
    }
  }
  cout << endl;
  return cnt;
}

int lNode::print(stringstream& ss) {
#ifdef MULTITHREAD
  if (loc == 1)
    ss << "LOCEKD" << endl;
#endif
  ss << "Leaf" << endl;
  ss << "sibling: " << sibling << endl;
  ss << "splitKey: " << splitKey << endl;
  int cnt = 0;
  for (int i = 0; i < CARDINALITY; i++) {
    if (entry[i].ptr != nullptr) {
      ss << entry[i].key << "-[p]" << entry[i].ptr << "\t";
      cnt++;
    }
  }
  ss << endl;
  return cnt;
}

// Internal Node //
iNode::iNode() : Node(Node::Internal) {
  root = -1;
  cnt = 0;
  deleteCnt = 0;
  for (auto &e : entry) {
    e.key = -1;
    e.left = -1;
    e.right = -1;
    e.lPtr = nullptr;
    e.rPtr = nullptr;
  }
}

iNode::iNode(Split* s) : Node(Node::Internal) {
  root = -1;
  cnt = 0;
  deleteCnt = 0;
  for (auto &e : entry) {
    e.key = -1;
    e.left = -1;
    e.right = -1;
    e.lPtr = nullptr;
    e.rPtr = nullptr;
  }
  insert(s->splitKey, s->left, s->right);
}

iNode::~iNode() = default;

bool iNode::overflow() {
  return cnt >= CARDINALITY;
}

Node* iNode::search(int64_t key) {
  if (sibling && sibling->splitKey < key) {
    return ((iNode*)sibling)->search(key);
  }
  int32_t cur = root;
  while (cur >= 0) {
    if (key < entry[cur].key) {
      if (entry[cur].left < 0) {
        return entry[cur].lPtr;
      }
      cur = entry[cur].left;
    } else {
      if (entry[cur].right < 0) {
        return entry[cur].rPtr;
      }
      cur = entry[cur].right;
    }
  }
}

int32_t iNode::insert(int64_t key, Node* _left, Node* _right) {
  int32_t loc = cnt++;
  clflush((char*)&cnt, sizeof(int32_t));

  entry[loc].key = key;
  entry[loc].left = -1;
  entry[loc].right = -1;
  entry[loc].lPtr = _left;
  entry[loc].rPtr = _right;
  clflush((char*)&entry[loc], sizeof(InternalEntry));

  if (root == -1) {
    root = loc;
    clflush((char*)&root, sizeof(int32_t));
    return loc;
  }

  int32_t parent = root;
  while (parent >= 0) {
    if (key < entry[parent].key) {
      if (entry[parent].left == -1) {
        entry[parent].left = loc;
        Node* tmp = entry[parent].lPtr;
        entry[parent].lPtr = nullptr;
        clflush((char*)&entry[parent].left, 16);
        // delete tmp;
        return loc;
      }
      parent = entry[parent].left;
    } else {
      if (entry[parent].right == -1) {
        entry[parent].right = loc;
        Node* tmp = entry[parent].rPtr;
        entry[parent].rPtr = nullptr;
        clflush((char*)&entry[parent].right, 12);
        // delete tmp;
        return loc;
      }
      parent = entry[parent].right;
    }
  }
}

int32_t iNode::sInsert(int64_t key, Node* _left, Node* _right) {
  int32_t loc = cnt++;

  entry[loc].key = key;
  entry[loc].left = -1;
  entry[loc].right = -1;
  entry[loc].lPtr = _left;
  entry[loc].rPtr = _right;

  if (root == -1) {
    root = loc;
    return loc;
  }

  int32_t parent = root;
  while (parent >= 0) {
    if (key < entry[parent].key) {
      if (entry[parent].left == -1) {
        entry[parent].left = loc;
        Node* tmp = entry[parent].lPtr;
        entry[parent].lPtr = nullptr;
        // delete tmp;
        return loc;
      }
      parent = entry[parent].left;
    } else {
      if (entry[parent].right == -1) {
        entry[parent].right = loc;
        Node* tmp = entry[parent].rPtr;
        entry[parent].rPtr = nullptr;
        // delete tmp;
        return loc;
      }
      parent = entry[parent].right;
    }
  }
}

Split* iNode::split(int64_t key, Node* _left, Node* _right) {
  int32_t cnt = 0;
  Node* leftmost = nullptr;
  LeafEntry* sorted = transform(cnt, leftmost);

  if (cnt == CARDINALITY) { // split
    Split *s     = new Split;
    iNode* left  = new iNode();
    iNode* right = new iNode();

    int32_t ptrIdx = -1;
    int32_t mIdx   = cnt/2;
    int64_t median = sorted[mIdx].key;

    // defragmentation(left, right, root, median);
    left->balancedInsert(sorted, 0, mIdx-1, ptrIdx, leftmost);
    right->balancedInsert(sorted, mIdx+1, CARDINALITY-1, ptrIdx, (Node*)sorted[mIdx].ptr);

    if (key < median) {
      left->sInsert(key, _left, _right);
    } else {
      right->sInsert(key, _left, _right);
    }

    left->sibling   = right;
    left->splitKey  = splitKey;
    right->sibling  = this->sibling;
    right->splitKey = median;

    s->original = this;
    s->left     = left;
    s->right    = right;
    s->splitKey = median;

    clflush((char*)left, 32+16+sizeof(InternalEntry)*mIdx);
    clflush((char*)right, 32+16+sizeof(InternalEntry)*mIdx);

    return s;
  } else { // rebalance TODO
    return nullptr;
  }
}

// Both nodes share same parent entry
void iNode::remove(int64_t key, Node* replacement) {
  int32_t cur = root;
  int32_t parent = root;
  iNode::Direction dir= iNode::None; // 0-left, 1-right
  while (key != entry[cur].key) {
    parent = cur;
    if (key < entry[cur].key) {
      dir = iNode::Left;
      cur = entry[cur].left;
    } else {
      dir = iNode::Right;
      cur = entry[cur].right;
    }
  }

  if (parent == -1) {
    // Height is reduced.
  } else {
    if (dir == iNode::Left) {
      entry[parent].lPtr = replacement;
      entry[parent].left = -1;
      clflush((char*)&entry[parent].left, 16);
    } else {
      entry[parent].rPtr = replacement;
      entry[parent].right = -1;
      clflush((char*)&entry[parent].right, 12);
    }
  }
}

void iNode::remove(int32_t leftParent, int32_t rightParent, Node* replacement, Node* lSib) {
  if (leftParent == rightParent) {
    block(leftParent, iNode::Left);
    block(leftParent, iNode::Right); // FIXME LATER
    if (lSib) {
      lSib->sibling = replacement;
      clflush((char*)&lSib->sibling, sizeof(void*));
    }
    remove(entry[leftParent].key, replacement);
  } else {
    if (entry[leftParent].right != -1) { // left is higher
      block(rightParent, iNode::Left);
      int32_t parent = getParent(leftParent);
      if (lSib == nullptr) { // leftmost
        entry[leftParent].lPtr->sibling = replacement;
        clflush((char*)&entry[leftParent].lPtr->sibling, sizeof(void*));
        entry[rightParent].lPtr = replacement;
        clflush((char*)&entry[rightParent].lPtr, sizeof(void*));
        // blocking left node
        if (parent == -1) { // leftParent is root
          root = entry[leftParent].right;
          clflush((char*)&root, 4);
        } else { // because it's leftmost
          entry[parent].left = entry[leftParent].right;
          clflush((char*)&entry[parent].left, 4);
        }
      } else {
        // blocking left node
        if (parent == -1) { // leftParent is root
          root = entry[leftParent].right;
          clflush((char*)&root, 4);
        } else if (entry[parent].left == leftParent) {
          entry[parent].left = entry[leftParent].right;
          clflush((char*)&entry[parent].left, 4);
        } else {
          entry[parent].right = entry[leftParent].right;
          clflush((char*)&entry[parent].right, 4);
        }
        lSib->sibling = replacement;
        clflush((char*)&lSib->sibling, sizeof(void*));
        entry[rightParent].lPtr = replacement;
        clflush((char*)&entry[rightParent].lPtr, sizeof(void*));
      }
    } else { // right is higher
      block(leftParent, iNode::Right);
      int32_t parent = getParent(rightParent);
      if (parent == -1) { // rightParent is root
        root = entry[rightParent].left;
        clflush((char*)&root, 4);
      } else if (entry[parent].left == rightParent) {
        entry[parent].left = entry[rightParent].left;
        clflush((char*)&entry[parent].left, 4);
      } else {
        entry[parent].right = entry[rightParent].left;
        clflush((char*)&entry[parent].right, 4);
      }
      lSib->sibling = replacement;
      clflush((char*)&lSib->sibling, sizeof(void*));
      entry[leftParent].rPtr = replacement;
      clflush((char*)&entry[leftParent].rPtr, sizeof(void*));
    }
  }
  deleteCnt++;
}

void iNode::update(int32_t leftParent, int32_t rightParent, Node* left, Node* right, Node* lSib) {
  if (leftParent == rightParent) {
    block(leftParent, iNode::Right);
    if (lSib != nullptr) {
      block(leftParent, iNode::Left);
      lSib->sibling = left;
      clflush((char*)&lSib->sibling, sizeof(void*));
    }
    entry[leftParent].key = right->splitKey;
    entry[leftParent].lPtr = left;
    entry[leftParent].rPtr = right;
    clflush((char*)&entry[leftParent], sizeof(InternalEntry));
  } else {
    if (entry[leftParent].right != -1) { // left is higher
      block(rightParent, iNode::Left);
      if (lSib != nullptr) { // leftmost
        block(leftParent, iNode::Left);
        lSib->sibling = left;
        clflush((char*)&lSib->sibling, sizeof(void*));
      }
      entry[leftParent].key = right->splitKey;
      entry[leftParent].lPtr = left;
      clflush((char*)&entry[leftParent], sizeof(InternalEntry));
      entry[rightParent].lPtr = right;
      clflush((char*)&entry[rightParent].lPtr, sizeof(void*));
    } else { // right is higher
      block(leftParent, iNode::Right);
      block(rightParent, iNode::Right);
      lSib->sibling = left;
      clflush((char*)&lSib, sizeof(void*));
      entry[rightParent].key = right->splitKey;
      entry[rightParent].rPtr = right;
      clflush((char*)&entry[rightParent], sizeof(InternalEntry));
      entry[leftParent].rPtr = left;
      clflush((char*)&entry[leftParent].rPtr, sizeof(void*));
    }
  }
}

void iNode::block(int32_t parent, iNode::Direction dir) {
  if (dir == iNode::Left) { // blocking left child
    entry[parent].lPtr = nullptr;
    clflush((char*)&entry[parent].lPtr,sizeof(void*));
  } else { // blocking right child
    entry[parent].rPtr = nullptr;
    clflush((char*)&entry[parent].rPtr,sizeof(void*));
  }
}

Node* iNode::getLeftSibling(int32_t parent, iNode::Direction dir) {
  if (dir == iNode::Right) {
    if (entry[parent].left == -1) return entry[parent].lPtr;
    else return getRightmostPtr(entry[parent].left);
  } else {
    int32_t ancestor = getCommonAncestor(parent);
    if (ancestor == -1) return nullptr;
    else {
      if (entry[ancestor].left != -1)
        return getRightmostPtr(entry[ancestor].left);
      else
        return entry[ancestor].lPtr;
    }
  }
}

// return -1 if the entry is root
int32_t iNode::getParent(int32_t childIdx) {
  int32_t cur = root;
  int32_t parent = -1;
  while (entry[childIdx].key != entry[cur].key) {
    parent = cur;
    if (entry[childIdx].key < entry[cur].key) {
      cur = entry[cur].left;
    } else {
      cur = entry[cur].right;
    }
  }
  return parent;
}

int32_t iNode::getParent(Node* childPtr, iNode::Direction& dir) {
  int32_t cur = root;
  int32_t parent = cur;
  while (cur != -1) {
    parent = cur;
    if (childPtr->splitKey < entry[cur].key) {
      dir = iNode::Left;
      cur = entry[cur].left;
    } else {
      dir = iNode::Right;
      cur = entry[cur].right;
    }
  }
  return parent;
}

// return -1 if the entry is root or leftmost entry
int32_t iNode::getCommonAncestor(int32_t childIdx) {
  int32_t cur = root;
  int32_t ancestor = -1;
  while (entry[childIdx].key != entry[cur].key) {
    if (entry[childIdx].key < entry[cur].key) {
      cur = entry[cur].left;
    } else {
      ancestor = cur;
      cur = entry[cur].right;
    }
    if (cur < 0) break;
  }
  return ancestor;
}

int32_t iNode::count() {
  return cnt - deleteCnt;
}

void iNode::rebalance() {
  int32_t cnt = 0;
  Node* leftmost = nullptr;
  LeafEntry* sorted = transform(cnt, leftmost);

  int32_t ptrIdx = -1;
  this->root = -1;
  this->cnt = 0;
  balancedInsert(sorted, 0, cnt-1, ptrIdx, leftmost);
}

// Merge function should be called from the left page.
Merge* iNode::merge() {
  iNode* _left  = (iNode*)this;
  iNode* _right = (iNode*)this->sibling;
  Merge* m = new Merge;

  LeafEntry* sorted = new LeafEntry[CARDINALITY*2];
  Node* leftmost = nullptr;
  int32_t cnt = 0;
  int32_t ptrIdx = 0;
  _left-> transform(sorted, cnt, ptrIdx, leftmost, true);
  sorted[cnt].key = _right->splitKey;
  sorted[cnt].ptr = _right->getLeftmostPtr();
  cnt++;
  _right->transform(sorted, cnt, ptrIdx, leftmost, false);

  ptrIdx = -1;

  if (cnt <= CARDINALITY) { // MERGE
    iNode* left = new iNode();
    left->balancedInsert(sorted, 0, cnt-1, ptrIdx, leftmost);
    left->sibling = _right->sibling;
    left->splitKey = splitKey;
    clflush((char*)left, sizeof(iNode));

    m->_left  = this;
    m->_right = _right;
    m->left   = left;
    m->right  = nullptr;
  } else { // REDISTRIBUTION
    iNode* left  = new iNode();
    iNode* right = new iNode();
    int32_t mIdx   = cnt/2;
    int64_t median = sorted[mIdx].key;

    left->balancedInsert(sorted, 0, mIdx-1, ptrIdx, leftmost);
    right->balancedInsert(sorted, mIdx+1, cnt-1, ptrIdx, (Node*)sorted[mIdx].ptr);

    left->sibling   = right;
    left->splitKey  = splitKey;
    right->sibling  = _right->sibling;
    right->splitKey = median;

    m->_left  = this;
    m->_right = _right;
    m->left   = left;
    m->right  = right;

    clflush((char*)left, sizeof(iNode));
    clflush((char*)right, sizeof(iNode));
  }
  delete sorted;
  return m;
}

Node* iNode::getLeftmostPtr() {
  return getLeftmostPtr(root);
}

Node* iNode::getLeftmostPtr(int32_t loc) {
  int32_t cur = loc;
  while ( entry[cur].left != -1 ) {
    cur = entry[cur].left;
  }
  return entry[cur].lPtr;
}

Node* iNode::getRightmostPtr() {
  return getRightmostPtr(root);
}

Node* iNode::getRightmostPtr(int32_t loc) {
  int32_t cur = loc;
  while ( entry[cur].right != -1 ) {
    cur = entry[cur].right;
  }
  return entry[cur].rPtr;
}

LeafEntry* iNode::transform(int32_t& cnt, Node*& leftmost) {
  LeafEntry* transformed = new LeafEntry[CARDINALITY];
  cnt = 0;
  int32_t ptrIdx = 0;
  transform(transformed, cnt, ptrIdx, leftmost, true);
  return transformed;
}

void iNode::transform(LeafEntry* transformed, int32_t &cnt, int32_t& ptrIdx, Node*& leftmost, bool first) {
  int32_t stack[CARDINALITY] = {-1};
  int32_t top = 1;
  int32_t cur = root;
  int32_t last = -1;

  while (cur != -1) {
    if (last == stack[top-1]) {
      last = cur;
      if (entry[cur].left != -1) {
        stack[top++] = cur;
        cur = entry[cur].left;
      } else if (entry[cur].right != -1) {
        transformed[cnt++].key = entry[cur].key;
        if (!first) {
          transformed[ptrIdx++].ptr = (void*)entry[cur].lPtr;
        } else {
          leftmost = entry[cur].lPtr;
          first = false;
        }
        stack[top++] = cur;
        cur = entry[cur].right;
      } else {
        transformed[cnt++].key = entry[cur].key;
        if (!first) {
          transformed[ptrIdx++].ptr = (void*)entry[cur].lPtr;
        } else {
          leftmost = entry[cur].lPtr;
          first = false;
        }
        transformed[ptrIdx++].ptr = (void*)entry[cur].rPtr;
        cur = stack[--top];
      }
    } else if (last == entry[cur].left) {
      last = cur;
      transformed[cnt++].key = entry[cur].key;
      if (entry[cur].right != -1) {
        stack[top++] = cur;
        cur = entry[cur].right;
      } else {
        transformed[ptrIdx++].ptr = (void*)entry[cur].rPtr;
        cur = stack[--top];
      }
    } else if (last == entry[cur].right) {
      last = cur;
      cur = stack[--top];
    }
  } // while
}

bool iNode::balancedInsert(LeafEntry* arr, int32_t from, int32_t to, int32_t& ptrIdx, Node* leftmost) {
  if (from > to) return true;
  int32_t mIdx = (from + to)/2;
  int32_t loc = sInsert(arr[mIdx].key, nullptr, nullptr);
  if (balancedInsert(arr, from, mIdx-1, ptrIdx, leftmost)) {
    entry[loc].left = -1;
    if (ptrIdx == -1) {
      entry[loc].lPtr = leftmost;
      ptrIdx++;
    } else {
      entry[loc].lPtr = (Node*) arr[ptrIdx++].ptr;
    }
  }
  if (balancedInsert(arr, mIdx+1, to, ptrIdx, leftmost)) {
    entry[loc].right = -1;
    entry[loc].rPtr = (Node*) arr[ptrIdx++].ptr;
  }
  return false;
}

void iNode::defragmentation(iNode* left, iNode* right, int16_t idx, int64_t median) {
  if (idx == -1) return;
  if (entry[idx].key < median) left->sInsert(entry[idx].key, entry[idx].lPtr, entry[idx].rPtr);
  else if (entry[idx].key > median) right->sInsert(entry[idx].key, entry[idx].lPtr, entry[idx].rPtr);
  else {
    if (entry[idx].left == -1) {
      int32_t cur = left->root;
      while (left->entry[cur].right != -1) {
        cur = left->entry[cur].right;
      }
      if (left->entry[cur].rPtr == nullptr)
        left->entry[cur].rPtr = entry[idx].lPtr;
    }
    if (entry[idx].right == -1) {
      int32_t cur = right->root;
      while (right->entry[cur].left != -1) {
        cur = right->entry[cur].left;
      }
      if (right->entry[cur].lPtr == nullptr)
        right->entry[cur].lPtr = entry[idx].rPtr;
    }
  }
  defragmentation(left, right, entry[idx].left, median);
  defragmentation(left, right, entry[idx].right, median);
}

void iNode::print() {
#ifdef MULTITHREAD
  if (loc == 1)
    cout << "LOCEKD" << endl;
#endif
  cout << "Internal" << endl;
  cout << "root: " << root << endl;
  cout << "rootKey: " << entry[root].key << endl;
  cout << "sibling: " << sibling << endl;
  cout << "cnt: " << cnt << endl;
  cout << "deleteCnt: " << deleteCnt << endl;
  cout << "splitKey: " << splitKey << endl;
  print(root);
  cout << endl;
}

void iNode::print(stringstream& ss) {
#ifdef MULTITHREAD
  if (loc == 1)
    ss << "LOCEKD" << endl;
#endif
  ss << "Internal" << endl;
  ss << "root: " << root << endl;
  ss << "rootKey: " << entry[root].key << endl;
  ss << "sibling: " << sibling << endl;
  ss << "cnt: " << cnt << endl;
  ss << "deleteCnt: " << deleteCnt << endl;
  ss << "splitKey: " << splitKey << endl;
  print(root, ss);
  ss << endl;
}

void iNode::print(int32_t pos) {
  if (pos == -1) return;
  if (entry[pos].left != -1) {
    print(entry[pos].left);
  }
  cout << "<" << pos << ">";
  if (entry[pos].left == -1)
    cout << "[p][" << entry[pos].lPtr << "]-";
  else
    cout << "[" << entry[pos].left << "]-";

  cout << entry[pos].key;
  if (entry[pos].right == -1)
    cout << "-[p][" << entry[pos].rPtr << "]";
  else
    cout << "-[" << entry[pos].right << "]";
  // cout << "\t";
  cout << endl;
  if (entry[pos].right != -1) {
    print(entry[pos].right);
  }
}

void iNode::print(int32_t pos, stringstream& ss) {
  if (pos == -1) return;
  if (entry[pos].left != -1) {
    print(entry[pos].left, ss);
  }
  ss << "<" << pos << ">";
  if (entry[pos].left == -1)
    ss << "[p][" << entry[pos].lPtr << "]-";
  else
    ss << "[" << entry[pos].left << "]-";

  ss << entry[pos].key;
  if (entry[pos].right == -1)
    ss << "-[p][" << entry[pos].rPtr << "]";
  else
    ss << "-[" << entry[pos].right << "]";
  // ss << "\t";
  ss << endl;
  if (entry[pos].right != -1) {
    print(entry[pos].right, ss);
  }
}
