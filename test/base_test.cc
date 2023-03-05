//
// Created by 杨丰硕 on 2023/3/1.
//
#include <iostream>
#include <vector>
#include <memory>
#include <unordered_map>
#include <gtest/gtest.h>
#include "../src/Utils.h"
#include "../src/SkipList.h"
#include "../src/SkipList.cc"

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

TEST(PUTANDGET_TEST, SIMPLE_TEST) {
    auto compare = [](const int &a,const int &b) ->int {
        if (a < b) {
            return -1;
        } else if (a == b) {
            return 0;
        } else {
            return 1;
        }
    };
    auto sklist = std::make_unique<SkipList<int, int>>(compare);
    sklist->Put(1, 1);
    sklist->Put(2, 2);
    sklist->Put(4, 1);
    sklist->Dump();
}

TEST(PUTANDGET_TEST, MUTI_TEST) {

    auto compare = [](const std::string &a,const std::string &b) ->int {
        if (a < b) {
            return -1;
        } else if (a == b) {
            return 0;
        } else {
            return 1;
        }
    };
    const int max_range = 20;
    Random random_gene(0, max_range);
    // auto sklist = std::make_unique<SkipList<std::string, std::string>>();
    auto sklist = std::make_unique<SkipList<std::string , std::string>>(compare);
    // insert numbers
    std::vector<int> numbers;

    for (int i = 0; i < max_range; ++i) {
        auto number = random_gene.GetRandom();
        numbers.push_back(number);
        sklist->Put(std::to_string(number), std::to_string(number));
    }
    sklist->Dump();
    // 检查是否插入成功
    for (auto num : numbers) {
        std::string value;
        ASSERT_TRUE(sklist->Get(std::to_string(num), &value));
        ASSERT_TRUE(value == std::to_string(num));
    }

}

int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    RUN_ALL_TESTS();
    return 0;
}

