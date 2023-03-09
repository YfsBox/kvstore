//
// Created by 杨丰硕 on 2023/3/9.
//

#ifndef KVSTORE_DISKSTORAGE_H
#define KVSTORE_DISKSTORAGE_H

#include <iostream>
#include <fstream>

namespace kvstore {

    inline void ReadUint64(std::ifstream &ifs, uint64_t &value) {
        ifs.read(reinterpret_cast<char*>(value), sizeof(uint64_t));
    }

    inline void WriteUint64(std::ofstream &ofs, uint64_t value) {
        ofs.write(reinterpret_cast<char *>(&value), sizeof(uint64_t));
    }

    inline void WriteString(std::ofstream &ofs, const std::string &str) {
        ofs.write(str.c_str(), str.size());
    }

    class DiskStorage {
    public:
        static constexpr const uint64_t BLOCK_SIZE = 4 * 1024;
    private:
    };


}

#endif //KVSTORE_DISKSTORAGE_H
