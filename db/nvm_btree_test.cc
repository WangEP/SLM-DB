#include <iostream>
#include <vector>
#include "nvm_btree.h"

using namespace std;

uint64_t WRITE_LATENCY_IN_NS = 0;
uint64_t clflush_cnt = 0;


int main () {
    const size_t kNumKeys = 10000;
    BTree bt;
    vector<int64_t> keys(kNumKeys);
    for (size_t i = 0; i < kNumKeys; i++) {
        keys[i] = i;
    }
    for (size_t i = 0; i < kNumKeys; i++) {
        bt.insert(keys[i], &keys[i]);
    }
    for (auto& r : bt.range(40, (size_t)5)) {
        cout << r->key << " " << r->ptr << endl;
    }
    return 0;
}
