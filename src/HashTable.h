//
// Created by 杨丰硕 on 2023/3/1.
//

#ifndef KVSTORE_HASHTABLE_H
#define KVSTORE_HASHTABLE_H

#include "KvContainer.h"
namespace kvstore {

    template<class KEY, class VALUE>
    class HashTable: public KvContainer<KEY, VALUE> {
    public:
        HashTable();

        ~HashTable();

        bool Put(const KEY &key, const VALUE &value) override;

        VALUE* Get(const KEY &key) override;

        bool Delete(const KEY &key) override;

        ContainType GetType() override {
            return HASH_CTYPE;
        }

    private:


    };


}

#endif //KVSTORE_HASHTABLE_H
