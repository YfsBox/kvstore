//
// Created by 杨丰硕 on 2023/3/1.
//

#ifndef KVSTORE_KVCONTAINER_H
#define KVSTORE_KVCONTAINER_H

namespace kvstore {

    enum ContainType {
        HASH_CTYPE,
        SKIPLIST_CTYPE,
        BPLUSTREE_CTYPE,
        NOTVALID_CTYPE,
    };

    template<class KEY, class VALUE>
    class KvContainer {
    public:
        KvContainer() {}

        virtual ~KvContainer() {}

        virtual bool Put(const KEY &key, const VALUE & value) = 0;

        virtual VALUE* Get(const KEY &key) const = 0;

        virtual bool Delete(const KEY &key) = 0;

        virtual ContainType GetType() = 0;

        virtual void Dump() = 0;        // 打印

    private:
    };

}

#endif //KVSTORE_KVCONTAINER_H
