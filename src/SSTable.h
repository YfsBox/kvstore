//
// Created by 杨丰硕 on 2023/3/9.
//
#ifndef KVSTORE_SSTABLE_H
#define KVSTORE_SSTABLE_H

#include <memory>
#include "SkipList.h"

namespace kvstore {
    class DiskStore;

    struct SSTableId {
        uint64_t table_id_;
        std::string path_;
    };

    class SSTable {
    public:

        using KvContainer = SkipList<uint64_t, std::string>;

        using KvIterator = SkipListIterator<uint64_t, std::string>;

        using MemStore = std::unique_ptr<KvContainer>;

        explicit SSTable(const SSTableId &tableId);

        explicit SSTable(const KvContainer &sklist, const SSTableId &tableId);

        bool Get(uint64_t key, std::string *value, bool load) const;

        bool Insert(uint64_t key, const std::string &value);

        bool LoadBlock(size_t blockno, std::string *value) const;

    private:
        void Save(const std::string &content);

        SSTableId table_id_;
        size_t entry_cnt_{0};
        size_t block_cnt_{0};
        std::vector<uint64_t> keys_;
        std::vector<uint64_t> offsets_;
        std::vector<uint64_t> block_offsets_;
    };
}

#endif //KVSTORE_SSTABLE_H
