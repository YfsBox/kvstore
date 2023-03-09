//
// Created by 杨丰硕 on 2023/3/1.
//
#include <iostream>
#include <algorithm>
#include <vector>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <gtest/gtest.h>
#include "../src/Utils.h"
#include "../src/SkipList.h"
#include "../src/SkipList.cc"
#include "test_utils.h"

using namespace kvstore;

template<class KEY, class VALUE>
class STLMapKv: public KvContainer<KEY, VALUE> {
public:

    STLMapKv() = default;

    ~STLMapKv() = default;

    STLMapKv(const STLMapKv &kvmap) = delete;

    STLMapKv& operator=(const STLMapKv &kvmap) = delete;

    bool Put(const KEY &key, const VALUE &value) override {
        map_[key] = value;
        return true;
    }

    bool Get(const KEY &key, VALUE *value) const override {
        auto findit = map_.find(key);
        if (findit != map_.end()) {
            *value = findit->second;
            return true;
        }
        return false;
    }

    bool Delete(const KEY &key) override {
        auto findit = map_.find(key);
        if (findit != map_.end()) {
            map_.erase(findit);
            return true;
        }
        return false;
    }

    ContainType GetType() override {
        return kvstore::OTHER_CTYPE;
    }

    void Dump() override {}

private:
    std::unordered_map<KEY, VALUE> map_;
};

template<class KEY, class VALUE>
class BinarySearchKv: public KvContainer<KEY, VALUE> {
public:
    using Kv = std::pair<KEY, VALUE>;

    explicit BinarySearchKv(const std::vector<Kv> &sorted_vec) {
        for (auto kv: sorted_vec) {
            sorted_vec_.push_back(kv);
        }
    }

    ~BinarySearchKv() = default;

    BinarySearchKv(const BinarySearchKv &bskv) = delete;

    BinarySearchKv& operator=(const BinarySearchKv &bskv) = delete;

    bool Put(const KEY &key, const VALUE & value) override {    return false;  }

    bool Get(const KEY &key, VALUE *value) const override {
        int left = 0, right = sorted_vec_.size() - 1;
        while (left <= right) {
            int mid = (left + right) >> 1;
            if (sorted_vec_[mid].first == key) {
                *value = sorted_vec_[mid].second;
                return true;
            } else if (sorted_vec_[mid].first < key) {
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }
        return false;
    }

    bool Delete(const KEY &key) override { return false; }

    void Dump() override {
        for (auto &kv : sorted_vec_) {
            std::cout << "(" << kv.first << "," << kv.second << ") ";
        }
        std::cout << '\n';
    }

    ContainType GetType() override {
        return kvstore::OTHER_CTYPE;
    }

private:
    std::vector<Kv> sorted_vec_;
};

int CompareString(const std::string &a, const std::string &b) {
    if (a < b) {
        return -1;
    } else if (a == b) {
        return 0;
    } else {
        return 1;
    }
}

int CompareInt(const int &a, const int &b) {
    if (a < b) {
        return -1;
    } else if (a == b) {
        return 0;
    } else {
        return 1;
    }
}

TEST(SKLIST_TEST, BASE_RANDOM_TEST) {

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

TEST(SKLIST_TEST, SIMPLE_TEST) {
    auto sklist = std::make_unique<SkipList<int, int>>(CompareInt);
    sklist->Put(1, 1);
    sklist->Put(2, 2);
    sklist->Put(4, 1);
    sklist->Dump();
}

TEST(SKLIST_TEST, MUTI_TEST) {
    const int max_range = 130;
    Random random_gene(0, max_range);
    // auto sklist = std::make_unique<SkipList<std::string, std::string>>();
    auto sklist = std::make_unique<SkipList<std::string , std::string>>(CompareString);
    // insert numbers
    std::vector<int> numbers;
    std::unordered_set<int> number_set;

    for (int i = 0; i < max_range; ++i) {
        auto number = random_gene.GetRandom();
        numbers.push_back(number);
        number_set.insert(number);
        sklist->Put(std::to_string(number), std::to_string(number));
    }
    sklist->Dump();
    // 检查是否插入成功
    for (auto num : numbers) {
        std::string value;
        ASSERT_TRUE(sklist->Get(std::to_string(num), &value));
        ASSERT_TRUE(value == std::to_string(num));
    }
    printf("The number size is %lu, and the max height is %d\n", number_set.size(), sklist->GetCurrHeight());
    // 测试利用PUT进行修改
    for (int i = 0; i < max_range / 3; ++i) {
        auto number = random_gene.GetRandom();
        if (number_set.count(number)) {
            ASSERT_TRUE(sklist->Put(std::to_string(number), ""));
            std::string get_val;
            ASSERT_TRUE(sklist->Get(std::to_string(number), &get_val));
            ASSERT_EQ(get_val, "");
        }
    }

}

TEST(SKLIST_TEST, DELETE_TEST) {

    const int max_range = 500;
    Random random_gene(0, max_range);

    auto sklist = std::make_unique<SkipList<int, int>>(CompareInt);
    std::vector<int> numbers;
    std::unordered_set<int> number_set;
    std::unordered_set<int> delete_set;

    for (int i = 0; i < max_range; ++i) {
        auto number = random_gene.GetRandom();
        numbers.push_back(number);
        number_set.insert(number);
        sklist->Put(number, number);
    }

    printf("The number size is %lu, and the max height is %d\n", number_set.size(), sklist->GetCurrHeight());
    sklist->Dump();


    for (int i = 0; i < max_range / 3; ++i) {
        auto del_number = random_gene.GetRandom();
        if (number_set.count(del_number)) {
            delete_set.insert(del_number);
        }
    }

    printf("The del number size is %lu\n", delete_set.size());

    for (auto key : delete_set) {
        ASSERT_TRUE(sklist->Delete(key));
    }

    for (auto key : delete_set) {
        int val;
        ASSERT_FALSE(sklist->Get(key, &val));
    }

}

TEST(SKLIST_TEST, COMPARE_WITH_OTHERS) {
    using IntKv = std::pair<int, int>;
    const int max_size = 10000;
    printf("The test size is %d\n", max_size);
    std::unordered_set<int> lookup_set;
    std::unordered_set<int> test_set;
    std::vector<IntKv> sorted_vac;
    Random random_gene(0, max_size * 2);
    SkipList<int, int> sklist(CompareInt);
    STLMapKv<int, int> stlmap;
    // 构造数据源
    while (lookup_set.size() != max_size) {
        auto number = random_gene.GetRandom();
        if (!lookup_set.count(number)) {
            lookup_set.insert(number);
            sorted_vac.emplace_back(number, number);
        }
    }
    auto cmp = [](const IntKv &a, const IntKv &b) -> bool {
        return a.first < b.first;
    };
    std::sort(sorted_vac.begin(), sorted_vac.end(), cmp);
    // 填充容器
    for (auto &kv : sorted_vac) {
        sklist.Put(kv.first, kv.second);
        stlmap.Put(kv.first, kv.second);
    }
    BinarySearchKv<int, int> bskv(sorted_vac);
    // 选出一个测试集合
    while (test_set.size() < max_size / 3) {
        auto number = random_gene.GetRandom();
        if (lookup_set.count(number)) {
            test_set.insert(number);
        }
    }
    // bskv.Dump();
    // 分别计算出他们的耗时
    double sklist_cost, stlmap_cost, bskv_cost;
    int get_val;
    // 对stl测试时间
    {
        testutils::TimeCounter stlmap_counter(stlmap_cost);
        for (auto number : test_set) {
            ASSERT_TRUE(stlmap.Get(number, &get_val));
        }
    }
    printf("The STL map cost is %lf\n", stlmap_cost);
    // 对跳表测试时间
    {
        testutils::TimeCounter sklist_counter(sklist_cost);
        for (auto number: test_set) {
            ASSERT_TRUE(sklist.Get(number, &get_val));
        }
    }
    printf("The Skip list cost is %lf\n", sklist_cost);

    // 测试bskv的耗时
    {
        testutils::TimeCounter bskv_counter(bskv_cost);
        for (auto number : test_set) {
            ASSERT_TRUE(bskv.Get(number, &get_val));
        }
    }
    printf("The binary search cost is %lf\n", bskv_cost);

}

TEST(SKLIST_TEST, ITERATOR_TEST) {

    const int max_range = 5000;
    const int max_count = 5000;
    Random random_gene(0, max_range);
    // auto sklist = std::make_unique<SkipList<int, int>>(CompareInt);
    SkipList<int, int> sklist(CompareInt);
    SkipListIterator<int, int> sklistIt(sklist);
    // 插入数值
    std::unordered_set<int> insert_keys;
    while (insert_keys.size() != max_count) {
        auto random_number = random_gene.GetRandom();
        if (!insert_keys.count(random_number)) {
            sklist.Put(random_number, random_number);
            insert_keys.insert(random_number);
        }
    }
    // 利用迭代器遍历，检查是否递增，并且数量足够
    sklistIt.Init();
    SkipListIterator<int, int>::ConstNodePtr pre_node = nullptr;
    size_t it_node_cnt = 0;
    while (sklistIt.HasNext()) {
        auto curr_node = sklistIt.Next();
        ASSERT_TRUE(insert_keys.count(curr_node->key_));
        if (pre_node) {
            ASSERT_TRUE(pre_node->key_ < curr_node->key_);
        }
        it_node_cnt++;
        pre_node = curr_node;
    }

    ASSERT_EQ(it_node_cnt, max_count);
}


int main(int argc, char* argv[]) {
    testing::InitGoogleTest(&argc, argv);
    RUN_ALL_TESTS();
    return 0;
}

