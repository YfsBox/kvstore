//
// Created by 杨丰硕 on 2023/3/4.
//
#include "BPlusTree.h"
#include <cstdlib>
#include "BPlusTreePredefined.h"
#include <list>
#include <algorithm>

using std::swap;
using std::binary_search;
using std::lower_bound;
using std::upper_bound;

using namespace kvstore;

//重载运算符便于<algorithm>函数调用
OPERATOR_KEYCMP(index_t)

OPERATOR_KEYCMP(record_t)

//方便获取叶子结点头尾
template<class T>
inline typename T::child_t begin(T &node) {
    return node.children;
}

template<class T>
inline typename T::child_t end(T &node) {
    return node.children + node.n;
}

//根据key寻找中间节点的index或叶子结点的record
//中间节点
inline index_t *find(internal_node_t &node, const key_t &key) {
    if (key) {
        return upper_bound(begin(node), end(node) - 1, key);
    }
    //因为索引范围的结尾是一个空字符串，所以如果我们搜索空键（当合并内部节点时），我们需要返回倒数第二个
    if (node.n > 1) {
        return node.children + node.n - 2;
    }
    return begin(node);
}

//叶子结点
inline record_t *find(leaf_node_t &node, const key_t &key) {
    return lower_bound(begin(node), end(node), key);
}

//初始化
BPlusTree::BPlusTree(const char *p, bool force_empty) : fp(nullptr), fp_level(0) {
    memset(path, 0, sizeof(path));
    strcpy(path, p);
    if (!force_empty)
        //从磁盘映射出来
        if (map(&meta, OFFSET_META) != 0)
            force_empty = true;
    if (force_empty) {
        open_file("w+");
        //如果没有的话需要新建
        init_from_empty();
        close_file();
    }
}

BPlusTree::BPlusTree() {}

BPlusTree::~BPlusTree() {}

bool BPlusTree::Put(const key_t &key, const value_t &value) {
    return false;
}

bool BPlusTree::Delete(const key_t &key) {
    return false;
}

bool BPlusTree::Get(const key_t &key, value_t *value) const {
    return false;
}

//根据key删除某节点中的key-index
void BPlusTree::remove_from_index(off_t offset, internal_node_t &node, const key_t &key) {
    //计算节点最小承载数 m/2
    size_t min_n = meta.root_offset == offset ? 1 : meta.order / 2;
    assert(node.n >= min_n && node.n <= meta.order);

    //删除key
    key_t index_key = begin(node)->key;
    index_t *to_delete = find(node, key);
    if (to_delete != end(node)) {
        (to_delete + 1)->child = to_delete->child;
        std::copy(to_delete + 1, end(node), to_delete);
    }
    node.n--;

    // remove to only one key
    if (node.n == 1 && meta.root_offset == offset &&
        meta.internal_node_num != 1) {
        unalloc(&node, meta.root_offset);
        meta.height--;
        meta.root_offset = node.children[0].child;
        unmap(&meta, OFFSET_META);
        return;
    }

    // merge or borrow
    if (node.n < min_n) {
        internal_node_t parent;
        map(&parent, node.parent);

        // first borrow from left
        bool borrowed = false;
        if (offset != begin(parent)->child)
            borrowed = borrow_key(false, node, offset);

        // then borrow from right
        if (!borrowed && offset != (end(parent) - 1)->child)
            borrowed = borrow_key(true, node, offset);

        // finally we merge
        if (!borrowed) {
            assert(node.next != 0 || node.prev != 0);

            if (offset == (end(parent) - 1)->child) {
                // if leaf is last element then merge | prev | leaf |
                assert(node.prev != 0);
                internal_node_t prev;
                map(&prev, node.prev);

                // merge
                index_t *where = find(parent, begin(prev)->key);
                reset_index_children_parent(begin(node), end(node), node.prev);
                merge_keys(where, prev, node, true);
                unmap(&prev, node.prev);
            } else {
                // else merge | leaf | next |
                assert(node.next != 0);
                internal_node_t next;
                map(&next, node.next);

                // merge
                index_t *where = find(parent, index_key);
                reset_index_children_parent(begin(next), end(next), offset);
                merge_keys(where, node, next);
                unmap(&node, offset);
            }

            // remove parent's key
            remove_from_index(node.parent, parent, index_key);
        } else {
            unmap(&node, offset);
        }
    } else {
        unmap(&node, offset);
    }
}

bool BPlusTree::borrow_key(bool from_right, internal_node_t &borrower,
                           off_t offset) {
    typedef typename internal_node_t::child_t child_t;

    off_t lender_off = from_right ? borrower.next : borrower.prev;
    internal_node_t lender;
    map(&lender, lender_off);

    assert(lender.n >= meta.order / 2);
    if (lender.n != meta.order / 2) {
        child_t where_to_lend, where_to_put;

        internal_node_t parent;

        // swap keys, draw on paper to see why
        if (from_right) {
            where_to_lend = begin(lender);
            where_to_put = end(borrower);

            map(&parent, borrower.parent);
            child_t where = lower_bound(begin(parent), end(parent) - 1,
                                        (end(borrower) - 1)->key);
            where->key = where_to_lend->key;
            unmap(&parent, borrower.parent);
        } else {
            where_to_lend = end(lender) - 1;
            where_to_put = begin(borrower);

            map(&parent, lender.parent);
            child_t where = find(parent, begin(lender)->key);
            // where_to_put->key = where->key;  // We shouldn't change where_to_put->key, because it just records the largest info but we only changes a new one which have been the smallest one
            where->key = (where_to_lend - 1)->key;
            unmap(&parent, lender.parent);
        }

        // store
        std::copy_backward(where_to_put, end(borrower), end(borrower) + 1);
        *where_to_put = *where_to_lend;
        borrower.n++;

        // erase
        reset_index_children_parent(where_to_lend, where_to_lend + 1, offset);
        std::copy(where_to_lend + 1, end(lender), where_to_lend);
        lender.n--;
        unmap(&lender, lender_off);
        return true;
    }

    return false;
}

bool BPlusTree::borrow_key(bool from_right, leaf_node_t &borrower) {
    off_t lender_off = from_right ? borrower.next : borrower.prev;
    leaf_node_t lender;
    map(&lender, lender_off);

    assert(lender.n >= meta.order / 2);
    if (lender.n != meta.order / 2) {
        typename leaf_node_t::child_t where_to_lend, where_to_put;

        // decide offset and update parent's index key
        if (from_right) {
            where_to_lend = begin(lender);
            where_to_put = end(borrower);
            change_parent_child(borrower.parent, begin(borrower)->key,
                                lender.children[1].key);
        } else {
            where_to_lend = end(lender) - 1;
            where_to_put = begin(borrower);
            change_parent_child(lender.parent, begin(lender)->key,
                                where_to_lend->key);
        }

        // store
        std::copy_backward(where_to_put, end(borrower), end(borrower) + 1);
        *where_to_put = *where_to_lend;
        borrower.n++;

        // erase
        std::copy(where_to_lend + 1, end(lender), where_to_lend);
        lender.n--;
        unmap(&lender, lender_off);
        return true;
    }

    return false;
}

void BPlusTree::change_parent_child(off_t parent, const key_t &o,
                                    const key_t &n) {
    internal_node_t node;
    map(&node, parent);

    index_t *w = find(node, o);
    assert(w != node.children + node.n);

    w->key = n;
    unmap(&node, parent);
    if (w == node.children + node.n - 1) {
        change_parent_child(node.parent, o, n);
    }
}

void BPlusTree::merge_leafs(leaf_node_t *left, leaf_node_t *right) {
    std::copy(begin(*right), end(*right), end(*left));
    left->n += right->n;
}

void BPlusTree::merge_keys(index_t *where,
                           internal_node_t &node, internal_node_t &next, bool change_where_key) {
    //(end(node) - 1)->key = where->key;
    if (change_where_key) {
        where->key = (end(next) - 1)->key;
    }
    std::copy(begin(next), end(next), end(node));
    node.n += next.n;
    node_remove(&node, &next);
}

void BPlusTree::insert_record_no_split(leaf_node_t *leaf,
                                       const key_t &key, const value_t &value) {
    record_t *where = upper_bound(begin(*leaf), end(*leaf), key);
    std::copy_backward(where, end(*leaf), end(*leaf) + 1);

    where->key = key;
    where->value = value;
    leaf->n++;
}

void BPlusTree::insert_key_to_index(off_t offset, const key_t &key,
                                    off_t old, off_t after) {
    if (offset == 0) {
        // create new root node
        internal_node_t root;
        root.next = root.prev = root.parent = 0;
        meta.root_offset = alloc(&root);
        meta.height++;

        // insert `old` and `after`
        root.n = 2;
        root.children[0].key = key;
        root.children[0].child = old;
        root.children[1].child = after;

        unmap(&meta, OFFSET_META);
        unmap(&root, meta.root_offset);

        // update children's parent
        reset_index_children_parent(begin(root), end(root),
                                    meta.root_offset);
        return;
    }

    internal_node_t node;
    map(&node, offset);
    assert(node.n <= meta.order);

    if (node.n == meta.order) {
        // split when full

        internal_node_t new_node;
        node_create(offset, &node, &new_node);

        // find even split point
        size_t point = (node.n - 1) / 2;
        bool place_right = keycmp(key, node.children[point].key) > 0;
        if (place_right)
            ++point;

        // prevent the `key` being the right `middle_key`
        // example: insert 48 into |42|45| 6|  |
        if (place_right && keycmp(key, node.children[point].key) < 0)
            point--;

        key_t middle_key = node.children[point].key;

        // split
        std::copy(begin(node) + point + 1, end(node), begin(new_node));
        new_node.n = node.n - point - 1;
        node.n = point + 1;

        // put the new key
        if (place_right)
            insert_key_to_index_no_split(new_node, key, after);
        else
            insert_key_to_index_no_split(node, key, after);

        unmap(&node, offset);
        unmap(&new_node, node.next);

        // update children's parent
        reset_index_children_parent(begin(new_node), end(new_node), node.next);

        // give the middle key to the parent
        // note: middle key's child is reserved
        insert_key_to_index(node.parent, middle_key, offset, node.next);
    } else {
        insert_key_to_index_no_split(node, key, after);
        unmap(&node, offset);
    }
}

void BPlusTree::insert_key_to_index_no_split(internal_node_t &node,
                                             const key_t &key, off_t value) {
    index_t *where = upper_bound(begin(node), end(node) - 1, key);

    // move later index forward
    std::copy_backward(where, end(node), end(node) + 1);

    // insert this key
    where->key = key;
    where->child = (where + 1)->child;
    (where + 1)->child = value;

    node.n++;
}

void BPlusTree::reset_index_children_parent(index_t *begin, index_t *end,
                                            off_t parent) {
    // this function can change both internal_node_t and leaf_node_t's parent
    // field, but we should ensure that:
    // 1. sizeof(internal_node_t) <= sizeof(leaf_node_t)
    // 2. parent field is placed in the beginning and have same size
    internal_node_t node;
    while (begin != end) {
        map(&node, begin->child);
        node.parent = parent;
        unmap(&node, begin->child, SIZE_NO_CHILDREN);
        ++begin;
    }
}

off_t BPlusTree::search_index(const key_t &key) const {
    off_t org = meta.root_offset;
    int height = meta.height;
    while (height > 1) {
        internal_node_t node;
        map(&node, org);

        index_t *i = upper_bound(begin(node), end(node) - 1, key);
        org = i->child;
        --height;
    }

    return org;
}

off_t BPlusTree::search_leaf(off_t index, const key_t &key) const {
    internal_node_t node;
    map(&node, index);

    index_t *i = upper_bound(begin(node), end(node) - 1, key);
    return i->child;
}

template<class T>
void BPlusTree::node_create(off_t offset, T *node, T *next) {
    // new sibling node
    next->parent = node->parent;
    next->next = node->next;
    next->prev = offset;
    node->next = alloc(next);
    // update next node's prev
    if (next->next != 0) {
        T old_next;
        map(&old_next, next->next, SIZE_NO_CHILDREN);
        old_next.prev = node->next;
        unmap(&old_next, next->next, SIZE_NO_CHILDREN);
    }
    unmap(&meta, OFFSET_META);
}

template<class T>
void BPlusTree::node_remove(T *prev, T *node) {
    unalloc(node, prev->next);
    prev->next = node->next;
    if (node->next != 0) {
        T next;
        map(&next, node->next, SIZE_NO_CHILDREN);
        next.prev = node->prev;
        unmap(&next, node->next, SIZE_NO_CHILDREN);
    }
    unmap(&meta, OFFSET_META);
}

void BPlusTree::init_from_empty() {
    // init default meta
    bzero(&meta, sizeof(meta_t));
    meta.order = BP_ORDER;
    meta.value_size = sizeof(value_t);
    meta.key_size = sizeof(key_t);
    meta.height = 1;
    meta.slot = OFFSET_BLOCK;

    // init root node
    internal_node_t root;
    root.next = root.prev = root.parent = 0;
    meta.root_offset = alloc(&root);

    // init empty leaf
    leaf_node_t leaf;
    leaf.next = leaf.prev = 0;
    leaf.parent = meta.root_offset;
    meta.leaf_offset = root.children[0].child = alloc(&leaf);

    // save
    unmap(&meta, OFFSET_META);
    unmap(&root, meta.root_offset);
    unmap(&leaf, root.children[0].child);
}




