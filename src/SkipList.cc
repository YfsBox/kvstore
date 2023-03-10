//
// Created by 杨丰硕 on 2023/3/1.
//
#include "SkipList.h"
using namespace kvstore;

template<class KEY, class VALUE>
bool SkipList<KEY, VALUE>::Put(const KEY &key, const VALUE &value) {

    NodePtr prenodes[kMaxHeight] = {nullptr};

    auto newnode = FindGreaterOrEqual(key, prenodes);
    if (newnode != nullptr && Equal(newnode->key_, key)) {      // 该key已经存在
        newnode->value_ = value;
        return true;
    }

    int newheight = GetRandomHeight();
    if (newheight > curr_height_) {     // 需要更新最大高度
        for (int i = curr_height_; i < newheight; ++i) {
            prenodes[i] = header_;
        }
        curr_height_ = newheight;
    }
    // 创建一个新结点
    newnode = new Node<KEY, VALUE>(key, value, kMaxHeight);
    newnode->ResizeNext(newheight);

    for (int i = 0; i < newheight; ++i) {
        newnode->SetNext(i, prenodes[i]->GetNext(i));
        prenodes[i]->SetNext(i, newnode);
    }

    return true;
}

template<class KEY, class VALUE>
bool SkipList<KEY, VALUE>::Delete(const KEY &key) {
    NodePtr prenodes[kMaxHeight] = {nullptr};
    auto findnode = FindGreaterOrEqual(key, prenodes);
    if (findnode == nullptr || !Equal(findnode->key_, key)) {
        return false;       // 不存在，也就不删除
    }

    for (int i = kMaxHeight - 1; i >= 0; --i) {
        if (prenodes[i] && prenodes[i]->GetNext(i) == findnode) {
           prenodes[i]->SetNext(i, findnode->GetNext(i));
        }
    }
    delete findnode;

    while (curr_height_ > 1 && !header_->GetNext(curr_height_ - 1)) {
        --curr_height_;
    }
    return true;
}

template<class KEY, class VALUE>
void SkipList<KEY, VALUE>::Dump() {
    // 首先打印header
    for (int i = 0; i < kMaxHeight; ++i) {
        std::cout << "header,";
    }
    std::cout << '\n';
    auto curr_node = header_->GetNext(0);
    while (curr_node) {
        int next_len = curr_node->GetNextSize();
        for (int i = 0; i < next_len; ++i) {
            std::cout << curr_node->key_ << ',';
        }
        std::cout << '\n';
        curr_node = curr_node->GetNext(0);
    }
}

template<class KEY, class VALUE>
bool SkipList<KEY, VALUE>::Get(const KEY &key, VALUE *value) const {
    auto findnode = FindGreaterOrEqual(key, nullptr);
    if (findnode && Equal(findnode->key_, key)) {
        *value = findnode->value_;
        return true;
    }
    return false;
}

template<class KEY, class VALUE>
Node<KEY,VALUE>* SkipList<KEY, VALUE>::FindGreaterOrEqual(const KEY &key, NodePtr *prenodes) const {
    auto curr_node = header_;
    int curr_level = curr_height_ - 1;

    while (true) {
        auto next_node = curr_node->GetNext(curr_level);
        if (next_node && Less(next_node->key_, key)) {
            curr_node = next_node;
        } else {        // 如果next是空的，或者大于等于key，就往下降一层，不过此时curr还是小于key的
            if (prenodes) {
                prenodes[curr_level] = curr_node;
            }
            if (curr_level == 0) {
                return next_node;
            }
            --curr_level;
        }
    }
}

template<class KEY, class VALUE>
int SkipList<KEY, VALUE>::GetRandomHeight() {
    int height = 1;
    while (height < kMaxHeight && random_.GetRandom() == 0) {
        height++;
    }
    return height;
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



