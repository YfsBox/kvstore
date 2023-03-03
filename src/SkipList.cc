//
// Created by 杨丰硕 on 2023/3/1.
//
#include "SkipList.h"
using namespace kvstore;

template<class KEY, class VALUE>
SkipList<KEY, VALUE>::SkipList(Comparator comparator):
    compare_(std::move(comparator)), random_(RandomMin, RandomMax) {}

template<class KEY, class VALUE>
bool SkipList<KEY, VALUE>::Put(const KEY &key, const VALUE &value) {

}

template<class KEY, class VALUE>
bool SkipList<KEY, VALUE>::Delete(const KEY &key) {

}

template<class KEY, class VALUE>
VALUE *SkipList<KEY, VALUE>::Get(const KEY &key) const {
    auto findnode = FindGreaterOrEqual(key);
    if (findnode && Equal(findnode->key_, key)) {
        return findnode->value_;
    }
    return nullptr;
}

template<class KEY, class VALUE>
Node<KEY,VALUE>* SkipList<KEY, VALUE>::FindGreaterOrEqual(const KEY &key) const {
    auto curr_node = header_;
    int curr_level = curr_height_ - 1;

    while (true) {
        auto next_node = curr_node->GetNext(curr_level);
        if (next_node && Less(next_node->key_, key)) {
            curr_node = next_node;
        } else {
            if (curr_level == 0) {
                return curr_node;
            }
            --curr_level;
        }
    }
}

template<class KEY, class VALUE>
Node<KEY,VALUE>* SkipList<KEY, VALUE>::FindLast() const {
    // 从header的顶层出发,一直调用next并下降
    auto curr_node = header_;
    int curr_level = curr_height_ - 1;

    while (true) {
        auto next_node = curr_node->GetNext(curr_level);
        if (next_node) {
            curr_node = next_node;
        } else {
            if (curr_level == 0) {
                return curr_node;
            }
            --curr_level;
        }
    }
}

template<class KEY, class VALUE>
Node<KEY, VALUE>* SkipList<KEY, VALUE>::FindLessThan(const KEY &key) const {
    auto curr_node = header_;       // 第一个header一般没有实际的key
    int curr_level = curr_height_ - 1;

    while (true) {
        auto next_node = curr_node->GetNext(curr_level);
        if (next_node == nullptr || GreaterOrEqual(next_node->key_, key)) {
            if (curr_level == 0) {
                return curr_node;
            }
            --curr_level;
        } else {
            curr_node = next_node;
        }
    }
}



