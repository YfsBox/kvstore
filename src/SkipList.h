//
// Created by 杨丰硕 on 2023/3/1.
//

#ifndef KVSTORE_SKIPLIST_H
#define KVSTORE_SKIPLIST_H

#include <cassert>
#include <iostream>
#include <functional>
#include <vector>
#include "Utils.h"
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

        using NodePtr = Node<KEY, VALUE>*;

        using Comparator = std::function<int(const KEY&, const KEY&)>;

        enum CompareCode {

        };

        explicit SkipList(Comparator comparator);

        ~SkipList() = default;

        SkipList(const SkipList &skipList) = delete;

        SkipList& operator=(const SkipList &skipList) = delete;

        bool Put(const KEY &key, const VALUE & value) override;

        VALUE *Get(const KEY &key) const override;

        bool Delete(const KEY &key) override;

        ContainType GetType() override {
            return SKIPLIST_CTYPE;
        }

        int GetCurrHeight() const {
            return curr_height_;
        }

    private:

        static constexpr uint8_t kMaxHeight = 12;

        static constexpr int RandomMin = 0;

        static constexpr int RandomMax = 3;

        bool Equal(const KEY &a, const KEY &b) const {
            assert(compare_);
            return compare_(a, b) == 0;
        }

        bool Less(const KEY &a, const KEY &b) const {
            assert(compare_);
            return compare_(a, b) < 0;
        }

        bool GreaterOrEqual(const KEY &a, const KEY &b) const {
            assert(compare_);
            return compare_(a, b) >= 0;
        }

        int GetRandomHeight();

        NodePtr FindGreaterOrEqual(const KEY& key, std::vector<NodePtr> *prenodes) const;

        NodePtr FindLessThan(const KEY& key) const;

        NodePtr FindLast() const;

        NodePtr header_{nullptr};
        Comparator compare_;
        Random random_;
        int curr_height_{1};
    };

}


#endif //KVSTORE_SKIPLIST_H
