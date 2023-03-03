//
// Created by 杨丰硕 on 2023/3/2.
//

#ifndef KVSTORE_UTILS_H
#define KVSTORE_UTILS_H

#include <random>
#include <iostream>

namespace kvstore {

    class Random {
    public:
        Random(int min, int max): dis_(min, max), engine_(static_cast<int>(time(nullptr))) {}

        int GetRandom() {
            return dis_(engine_);
        }

    private:
        std::uniform_int_distribution<int> dis_;
        std::default_random_engine engine_;
    };

}

#endif //KVSTORE_UTILS_H
