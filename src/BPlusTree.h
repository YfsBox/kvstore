//
// Created by 杨丰硕 on 2023/3/4.
//

#ifndef KVSTORE_BPLUSTREE_H
#define KVSTORE_BPLUSTREE_H

#include "KvContainer.h"
namespace kvstore {

    template<class KEY, class VALUE>
    class BPlusTree: public KvContainer<KEY, VALUE> {

    public:
        BPlusTree();

        ~BPlusTree();

        BPlusTree(const BPlusTree& bptree) = delete;

        BPlusTree& operator=(const BPlusTree& bptree) = delete;

        bool Put(const KEY &key, const VALUE &value) override;

        bool Get(const KEY &key, VALUE *value) const override;

        bool Delete(const KEY &key) override;

        ContainType GetType() override {
            return BPLUSTREE_CTYPE;
        }

    private:

    };

}

#endif //KVSTORE_BPLUSTREE_H
