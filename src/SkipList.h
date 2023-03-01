//
// Created by 杨丰硕 on 2023/3/1.
//

#ifndef KVSTORE_SKIPLIST_H
#define KVSTORE_SKIPLIST_H

#include "KvContainer.h"

namespace kvstore {
    template<class KEY, class VALUE>
    class SkipList: public KvContainer<KEY, VALUE> {
    public:
        SkipList();

        ~SkipList();

        bool Put(const KEY &key, const VALUE & value) override;

        VALUE * Get(const KEY &key) override;

        bool Delete(const KEY &key) override;

        ContainType GetType() override {
            return SKIPLIST_CTYPE;
        }

    private:


    };



}


#endif //KVSTORE_SKIPLIST_H
