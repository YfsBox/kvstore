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

        const KEY key_{0};

        VALUE value_{0};

        Node(const KEY &key, const VALUE &value, int level): key_(key), value_(value) {
            nexts_.reserve(level);
        }

        explicit Node(int level) {
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

        void ResizeNext(int n) {
            nexts_.resize(n, nullptr);
        }

        int GetNextSize() const {
            return nexts_.size();
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

        explicit SkipList(Comparator comparator):
                compare_(std::move(comparator)), random_(RandomMin, RandomMax) {
                header_ = new Node<KEY, VALUE>(kMaxHeight);
                header_->ResizeNext(kMaxHeight);
                for (int i = 0; i < kMaxHeight; ++i) {
                    header_->SetNext(i, nullptr);
                }
        }

        ~SkipList() {     // 释放内存
            NodePtr curr_node = header_;
            while (curr_node) {
                auto next_node = curr_node->GetNext(0);
                delete curr_node;
                curr_node = next_node;
            }
        }

        SkipList(const SkipList &skipList) = delete;

        SkipList& operator=(const SkipList &skipList) = delete;

        bool Put(const KEY &key, const VALUE & value) override;

        bool Get(const KEY &key, VALUE *value) const override;

        bool Delete(const KEY &key) override;

        void Dump() override;

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

        NodePtr FindGreaterOrEqual(const KEY& key, NodePtr *prenodes) const;

        NodePtr FindLessThan(const KEY& key) const;

        NodePtr FindLast() const;

        NodePtr header_{nullptr};
        Comparator compare_;
        Random random_;
        int curr_height_{1};
    };

}


#endif //KVSTORE_SKIPLIST_H
