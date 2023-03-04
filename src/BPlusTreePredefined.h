#ifndef PREDEFINED_H
#define PREDEFINED_H

#include <cstring>

namespace kvstore {

#define BP_ORDER 20

    //自定义value
    typedef int value_t;

    //自定义key
    struct key_t {
        char k[16];

        key_t(const char *str = "") {
            memset(k, 0, sizeof(k));
            strcpy(k, str);
        }

        operator bool() const {
            return strcmp(k, "");
        }
    };

    inline int keyCmp(const key_t &a, const key_t &b) {
        int x = strlen(a.k) - strlen(b.k);
        return x == 0 ? strcmp(a.k, b.k) : x;
    }

#define OPERATOR_KEYCMP(type) \
    bool operator< (const key_t &l, const type &r) {\
        return keyCmp(l, r.key) < 0;\
    }\
    bool operator< (const type &l, const key_t &r) {\
        return keyCmp(l.key, r) < 0;\
    }\
    bool operator== (const key_t &l, const type &r) {\
        return keyCmp(l, r.key) == 0;\
    }\
    bool operator== (const type &l, const key_t &r) {\
        return keyCmp(l.key, r) == 0;\
    }

}

#endif
