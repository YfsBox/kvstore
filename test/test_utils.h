//
// Created by 杨丰硕 on 2023/3/5.
//

#ifndef KVSTORE_TEST_UTILS_H
#define KVSTORE_TEST_UTILS_H

#include <chrono>

namespace testutils {

    class TimeCounter {
    public:
        using TimePoint = std::chrono::steady_clock::time_point;

        explicit TimeCounter(double &cost): cost_time_(cost) {
            begin_ = std::chrono::steady_clock::now();
        }

        ~TimeCounter() {
            auto end_point = std::chrono::steady_clock::now();
            std::chrono::duration<double> temp =
                    std::chrono::duration_cast<std::chrono::duration<double>>(end_point - begin_);
            cost_time_ = temp.count();
        }

    private:
        TimePoint begin_;
        double &cost_time_;
    };
}

#endif //KVSTORE_TEST_UTILS_H
