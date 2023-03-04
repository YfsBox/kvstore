//
// Created by 杨丰硕 on 2023/3/4.
//
#include "BPlusTree.h"

using namespace kvstore;

template<class KEY, class VALUE>
BPlusTree<KEY, VALUE>::BPlusTree() {}

template<class KEY, class VALUE>
BPlusTree<KEY, VALUE>::~BPlusTree() {}

template<class KEY, class VALUE>
bool BPlusTree<KEY, VALUE>::Put(const KEY &key, const VALUE &value) {
    return false;
}

template<class KEY, class VALUE>
bool BPlusTree<KEY, VALUE>::Delete(const KEY &key) {
    return false;
}

template<class KEY, class VALUE>
VALUE *BPlusTree<KEY, VALUE>::Get(const KEY &key) const {
    return nullptr;
}




