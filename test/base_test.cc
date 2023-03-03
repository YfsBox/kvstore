//
// Created by 杨丰硕 on 2023/3/1.
//
#include <iostream>
#include <vector>
#include <unordered_map>
#include <gtest/gtest.h>
#include "../src/Utils.h"

using namespace kvstore;

TEST(RANDOM_TEST, BASE_RANDOM_TEST) {

    Random random1(0, 3);
    std::vector<int> number_cnt;
    number_cnt.resize(3, 0);

    for (int i = 0; i < 10000; ++i) {
        number_cnt[random1.GetRandom()]++;
    }

    for (int i = 0; i < 4; ++i) {
        printf("The %u is %u cnt\n", i, number_cnt[i]);
    }

}

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    RUN_ALL_TESTS();
    return 0;
}

