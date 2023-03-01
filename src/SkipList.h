//
// Created by 杨丰硕 on 2023/3/1.
//

#ifndef KVSTORE_SKIPLIST_H
#define KVSTORE_SKIPLIST_H

#include <cassert>
#include <iostream>
#include <vector>
#include "KvContainer.h"

namespace kvstore {

    template<class KEY, class VALUE>
    struct Node {
        using NodePtr = Node*;

        const KEY key_;

        VALUE value_;

        Node(const KEY &key, const VALUE &value, int level): key_(key), value_(value) {
            nexts_.reserve(level);
        }

        ~Node() = default;

        NodePtr GetNext(int n) {
            assert(n >= 0);
            return nexts_[n];
        }

        void SetNext(int n, const NodePtr node) {
            assert(n >= 0);
            nexts_[n] = node;
        }

    private:
        std::vector<NodePtr> nexts_;
    };

    template<class KEY, class VALUE>
    class SkipList: public KvContainer<KEY, VALUE> {
    public:
        using NodePtr = Node<KEY, VALUE>;

        SkipList();

        ~SkipList();

        bool Put(const KEY &key, const VALUE & value) override;

        VALUE * Get(const KEY &key) override;

        bool Delete(const KEY &key) override;

        ContainType GetType() override {
            return SKIPLIST_CTYPE;
        }

    private:
        static constexpr uint8_t kMaxHeight = 12;

        NodePtr header_;
        uint8_t curr_height_{1};
    };



}


#endif //KVSTORE_SKIPLIST_H
