//
// Created by 杨丰硕 on 2023/3/4.
//

#ifndef KVSTORE_BPLUSTREE_H
#define KVSTORE_BPLUSTREE_H

#include "KvContainer.h"
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cassert>
#include "BPlusTreePredefined.h"

namespace kvstore {

#define OFFSET_META 0
#define OFFSET_BLOCK OFFSET_META + sizeof(meta_t)
#define SIZE_NO_CHILDREN sizeof(leaf_node_t) - BP_ORDER * sizeof(record_t)

    //B+树的元信息
    typedef struct {
        //B+树的阶
        size_t order;
        //value的大小
        size_t value_size;
        //key的大小
        size_t key_size;
        //中间节点个数
        size_t internal_node_num;
        //叶子结点个数
        size_t leaf_node_num;
        //树的高度
        size_t height;
        //新块的存储位置
        off_t slot;
        //根节点的位置
        off_t root_offset;
        //第一个叶子结点的位置
        off_t leaf_offset;
    } meta_t;

    //中间节点的元素key-offset
    struct index_t {
        key_t key;
        off_t child;
    };

    //中间节点
    struct internal_node_t {
        typedef index_t *child_t;
        off_t parent;
        off_t next;
        off_t prev;
        //中间节点保存的元素个数
        size_t n;
        //内部节点中保存的key-offset
        index_t children[BP_ORDER];
    };

    //叶子结点的record
    struct record_t {
        key_t key;
        value_t value;
    };

    //叶子结点
    struct leaf_node_t {
        typedef record_t *child_t;
        //父节点的位置
        off_t parent;
        //下一个节点
        off_t next;
        //上一个节点
        off_t prev;
        //叶子结点保存的record的个数
        size_t n;
        //叶子结点保存的record
        record_t children[BP_ORDER];
    };


    class BPlusTree : public KvContainer<key_t, value_t> {

    public:
        BPlusTree();

        BPlusTree(const char *path, bool force_empty = false);

        ~BPlusTree();

        BPlusTree(const BPlusTree &bptree) = delete;

        BPlusTree &operator=(const BPlusTree &bptree) = delete;

        bool Put(const key_t &key, const value_t &value) override;

        bool Get(const key_t &key, value_t *value) const override;

        bool Get_range(key_t *left, const key_t &right, value_t *values, size_t max, bool *next = NULL) const;

        bool Delete(const key_t &key) override;

        ContainType GetType() override {
            return BPLUSTREE_CTYPE;
        }

        meta_t get_meta() const {
            return meta;
        };

    private:
        char path[512]{};
        meta_t meta{};

        //初始化空树
        void init_from_empty();

        //寻找索引
        off_t search_index(const key_t &key) const;

        //寻找叶子结点
        off_t search_leaf(off_t index, const key_t &key) const;

        //寻找叶子结点
        off_t search_leaf(const key_t &key) const {
            return search_leaf(search_index(key), key);
        }

        //去除中间节点
        void remove_from_index(off_t offset, internal_node_t &node,
                               const key_t &key);

        //从其他中间节点中选取一个key（分裂）
        bool borrow_key(bool from_right, internal_node_t &borrower,
                        off_t offset);

        //从其他叶子结点中选取一个key（分裂）
        bool borrow_key(bool from_right, leaf_node_t &borrower);

        //改变某节点的父节点（分裂）
        void change_parent_child(off_t parent, const key_t &o, const key_t &n);

        //合并叶子结点 right->left
        void merge_leafs(leaf_node_t *left, leaf_node_t *right);

        //合并key
        void merge_keys(index_t *where, internal_node_t &left,
                        internal_node_t &right, bool change_where_key = false);

        //插入新的叶子结点（无需分裂）
        void insert_record_no_split(leaf_node_t *leaf,
                                    const key_t &key, const value_t &value);

        //向中间节点添加key
        void insert_key_to_index(off_t offset, const key_t &key,
                                 off_t value, off_t after);

        //向中间节点添加key（无需分裂）
        void insert_key_to_index_no_split(internal_node_t &node, const key_t &key,
                                          off_t value);

        //改变子节点的父节点
        void reset_index_children_parent(index_t *begin, index_t *end,
                                         off_t parent);

        //创造节点
        template<class T>
        void node_create(off_t offset, T *node, T *next);

        //删除节点
        template<class T>
        void node_remove(T *prev, T *node);

        //文件打开/关闭
        mutable FILE *fp;
        mutable int fp_level;

        void open_file(const char *mode = "rb+") const {
            // `rb+` will make sure we can write everywhere without truncating file
            if (fp_level == 0)
                fp = fopen(path, mode);

            ++fp_level;
        }

        void close_file() const {
            if (fp_level == 1)
                fclose(fp);

            --fp_level;
        }

        //磁盘获取空间
        off_t alloc(size_t size) {
            off_t slot = meta.slot;
            meta.slot += size;
            return slot;
        }

        off_t alloc(leaf_node_t *leaf) {
            leaf->n = 0;
            meta.leaf_node_num++;
            return alloc(sizeof(leaf_node_t));
        }

        off_t alloc(internal_node_t *node) {
            node->n = 1;
            meta.internal_node_num++;
            return alloc(sizeof(internal_node_t));
        }

        //磁盘释放空间
        void unalloc(leaf_node_t *leaf, off_t offset) {
            --meta.leaf_node_num;
        }

        void unalloc(internal_node_t *node, off_t offset) {
            --meta.internal_node_num;
        }

        //从磁盘读取块
        int map(void *block, off_t offset, size_t size) const {
            open_file();
            fseek(fp, offset, SEEK_SET);
            size_t rd = fread(block, size, 1, fp);
            close_file();

            return rd - 1;
        }

        template<class T>
        int map(T *block, off_t offset) const {
            return map(block, offset, sizeof(T));
        }

        //向磁盘写入块
        int unmap(void *block, off_t offset, size_t size) const {
            open_file();
            fseek(fp, offset, SEEK_SET);
            size_t wd = fwrite(block, size, 1, fp);
            close_file();

            return wd - 1;
        }

        template<class T>
        int unmap(T *block, off_t offset) const {
            return unmap(block, offset, sizeof(T));
        }

    };

}

#endif //KVSTORE_BPLUSTREE_H
