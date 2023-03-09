//
// Created by 杨丰硕 on 2023/3/9.
//
#include "SSTable.h"
#include "DiskStorage.h"

using namespace kvstore;
// 将文件中的SSTable进行反序列化
SSTable::SSTable(const SSTableId &tableId):
        table_id_(tableId){
    std::ifstream ifs(table_id_.path_, std::ios::binary);
    ReadUint64(ifs, entry_cnt_);
    keys_.resize(entry_cnt_);
    offsets_.resize(entry_cnt_);

    for (size_t i = 0; i < entry_cnt_; ++i) {
        ReadUint64(ifs, keys_[i]);
        ReadUint64(ifs, offsets_[i]);
    }

    ReadUint64(ifs, block_cnt_);
    block_offsets_.resize(block_cnt_);
    for (size_t i = 0; i < block_cnt_; ++i) {
        ReadUint64(ifs, block_offsets_[i]);
    }
    ifs.close();
}
// 从跳表中解析出SSTable的内容
SSTable::SSTable(const KvContainer &sklist, const SSTableId &tableId):
        table_id_(tableId){
    KvIterator kvIterator(sklist);
    int entry_cnt_inblock = 0;
    uint64_t offset = 0;
    uint64_t block_offset = 0;
    std::string block_content;
    block_content.reserve(DiskStorage::BLOCK_SIZE);
    std::string sstable_content;

    while (kvIterator.HasNext()) {
        auto curr_node = kvIterator.Next();
        keys_.push_back(curr_node->key_);
        offsets_.push_back(offset);
        offset += curr_node->value_.size();
        block_content += curr_node->value_;
        ++entry_cnt_;
        ++entry_cnt_inblock;
        if (block_content.size() >= DiskStorage::BLOCK_SIZE) {
            block_offsets_.push_back(block_offset);
            sstable_content += block_content;
            block_offset += block_content.size();
            block_content.clear();
            entry_cnt_inblock = 0;
            ++block_cnt_;
        }
    }

    if (entry_cnt_inblock > 0) {
        block_offsets_.push_back(block_offset);
        sstable_content += block_content;
        block_offset += block_content.size();
        block_content.clear();
        ++block_cnt_;
    }

    block_offsets_.push_back(block_offset);
    Save(sstable_content);
}

bool SSTable::Get(uint64_t key, std::string *value, bool load) const {
    auto findit = std::lower_bound(keys_.begin(), keys_.end(), key);
    if (*findit != key) {
        return false;
    }
    if (load) {     // 表示的是是否需要将value加载出来

    }
    return true;
}

bool SSTable::Insert(uint64_t key, const std::string &value) {
    return false;
}

bool SSTable::LoadBlock(size_t blockno, std::string *value) const {
    return false;
}

void SSTable::Save(const std::string &content) {
    std::ofstream ofs(table_id_.path_, std::ios::binary);
    WriteUint64(ofs, entry_cnt_);
    for (size_t i = 0; i < entry_cnt_; ++i) {       // 写到文件中
        WriteUint64(ofs, keys_[i]);
        WriteUint64(ofs, offsets_[i]);
    }
    WriteUint64(ofs, block_cnt_);
    for (size_t i = 0; i < block_cnt_; ++i) {
        WriteUint64(ofs, block_offsets_[i]);
    }
    WriteString(ofs, content);
    ofs.close();
}
