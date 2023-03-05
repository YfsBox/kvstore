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

//获取元素
bool BPlusTree::Get(const key_t &key, value_t *value) const {
    leaf_node_t leaf;
    //通过key寻找叶子结点并从磁盘读取
    map(&leaf, search_leaf(key));
    //找到record
    record_t *record = find(leaf, key);
    //比对数据是否为key
    if (record != leaf.children + leaf.n) {
        *value = record->value;
        //若为key则获取成功
        //TODO 修改判断逻辑
        return keyCmp(record->key, key);
    } else {
        return false;
    }
}

//范围查询
bool BPlusTree::Get_range(key_t *left, const key_t &right, value_t *values, size_t max, bool *next) const {
    //如果左边为空或者右边大于左边则返回失败
    if (left == nullptr || keyCmp(*left, right) > 0)
        return -1;

    //获取的叶子结点的位置
    off_t off_left = search_leaf(*left);
    off_t off_right = search_leaf(right);
    //从left->right
    off_t off = off_left;
    //index
    size_t i = 0;
    record_t *beg, *ed;

    leaf_node_t leaf;
    //通过offset进行遍历
    while (off != off_right && off != 0 && i < max) {
        map(&leaf, off);

        if (off_left == off) {
            //如果第一次遍历则需要找到该位置
            beg = find(leaf, *left);
        } else {
            //如果是之后的叶子节点，则直接获取整个节点的第一个元素的位置即可
            beg = begin(leaf);
        }

        //拓展到给定数组
        ed = leaf.children + leaf.n;
        for (; beg != ed && i < max; ++beg, i++)
            values[i] = beg->value;

        //往后遍历
        off = leaf.next;
    }

    //如果到了最后一个叶子结点
    if (i < max) {
        map(&leaf, off_right);
        beg = find(leaf, *left);
        ed = upper_bound(begin(leaf), end(leaf), right);
        for (; beg != ed && i < max; ++beg, ++i)
            values[i] = beg->value;
    }

    //判断是否有后继
    if (next != nullptr) {
        if (i == max && beg != ed) {
            *next = true;
            *left = beg->key;
        } else {
            *next = false;
        }
    }

    return i;
}

//根据key删除元素
bool BPlusTree::Delete(const key_t &key) {
    internal_node_t parent_of_node;
    leaf_node_t leaf;

    //找到需要删除元素所在的叶子结点节点的父节点的父节点
    off_t parent_off = search_index(key);
    map(&parent_of_node, parent_off);

    //找到需要删除元素所在的叶子结点
    index_t *current_node = find(parent_of_node, key);
    off_t offset = current_node->child;
    map(&leaf, offset);

    //判断key是否存在
    if (!binary_search(begin(leaf), end(leaf), key))
        return false;

    //计算节点最少元素数 m/2
    size_t min_n = meta.leaf_node_num == 1 ? 0 : meta.order / 2;
    assert(leaf.n >= min_n && leaf.n <= meta.order);

    //在叶子结点中找到需要删除的元素
    record_t *to_delete = find(leaf, key);
    std::copy(to_delete + 1, end(leaf), to_delete);
    leaf.n--;

    //是否需要合并或分裂
    if (leaf.n < min_n) {
        //先从左边节点借一个元素
        bool borrowed = false;
        if (leaf.prev != 0)
            borrowed = borrow_key(false, leaf);

        //如果借不到就从右边借
        if (!borrowed && leaf.next != 0)
            borrowed = borrow_key(true, leaf);

        //如果借不到就需要合并
        if (!borrowed) {
            assert(leaf.next != 0 || leaf.prev != 0);
            key_t index_key;

            if (current_node == end(parent_of_node) - 1) {
                //如果当前节点是父节点的最后一个 则进行merge(prev+leaf)
                assert(leaf.prev != 0);
                leaf_node_t prev;
                map(&prev, leaf.prev);
                index_key = begin(prev)->key;
                //合并 right->left
                merge_leafs(&prev, &leaf);
                node_remove(&prev, &leaf);
                unmap(&prev, leaf.prev);
            } else {
                //否则merge(leaf+next)
                assert(leaf.next != 0);
                leaf_node_t next;
                map(&next, leaf.next);
                index_key = begin(leaf)->key;
                //合并 right->left
                merge_leafs(&leaf, &next);
                node_remove(&leaf, &next);
                unmap(&leaf, offset);
            }

            //递归往上修改中间节点
            remove_from_index(parent_off, parent_of_node, index_key);
        } else {
            unmap(&leaf, offset);
        }
    } else {
        unmap(&leaf, offset);
    }

    return 0;
}

bool BPlusTree::Put(const key_t &key, const value_t &value) {
    //找到当前节点
    off_t parent = search_index(key);
    //找到key所在的叶子结点
    off_t offset = search_leaf(parent, key);
    leaf_node_t leaf;
    map(&leaf, offset);

    //如果已经存在该key则直接返回
    if (binary_search(begin(leaf), end(leaf), key))
        return true;

    //如果当前叶子结点已经满了则需要先分裂
    if (leaf.n == meta.order) {
        //新建一个节点
        leaf_node_t new_leaf;
        node_create(offset, &leaf, &new_leaf);

        //找到分割点
        size_t point = leaf.n / 2;
        bool place_right = keyCmp(key, leaf.children[point].key) > 0;
        if (place_right) {
            point++;
        }

        //分裂
        std::copy(leaf.children + point, leaf.children + leaf.n, new_leaf.children);
        new_leaf.n = leaf.n - point;
        leaf.n = point;

        //选择一个节点进行插入
        if (place_right)
            insert_record_no_split(&new_leaf, key, value);
        else
            insert_record_no_split(&leaf, key, value);

        //保存新建节点与新插入元素的节点
        unmap(&leaf, offset);
        unmap(&new_leaf, leaf.next);

        //向上更新插入节点引起的变化
        insert_key_to_index(parent, new_leaf.children[0].key, offset, leaf.next);
    } else {
        insert_record_no_split(&leaf, key, value);
        unmap(&leaf, offset);
    }

    return false;
}

//根据key删除某节点中的元素
void BPlusTree::remove_from_index(off_t offset, internal_node_t &node, const key_t &key) {

    //计算节点最少元素数 m/2
    size_t min_n = meta.root_offset == offset ? 1 : meta.order / 2;
    assert(node.n >= min_n && node.n <= meta.order);

    //按照key寻找要删除的元素然后删除
    key_t index_key = begin(node)->key;
    index_t *to_delete = find(node, key);
    if (to_delete != end(node)) {
        (to_delete + 1)->child = to_delete->child;
        std::copy(to_delete + 1, end(node), to_delete);
    }
    node.n--;

    //如果只剩下一个节点
    if (node.n == 1 && meta.root_offset == offset && meta.internal_node_num != 1) {
        unalloc(&node, meta.root_offset);
        meta.height--;
        meta.root_offset = node.children[0].child;
        unmap(&meta, OFFSET_META);
        return;
    }

    //如果该节点的元素数小于指定的最少元素数 需要合并与分裂
    if (node.n < min_n) {
        //父节点从磁盘读取出来
        internal_node_t parent;
        map(&parent, node.parent);


        bool borrowed = false;
        //先从左边节点借元素
        if (offset != begin(parent)->child)
            borrowed = borrow_key(false, node, offset);

        //如果借不到就从右边借
        if (!borrowed && offset != (end(parent) - 1)->child)
            borrowed = borrow_key(true, node, offset);

        //如果都借不到就合并
        if (!borrowed) {
            assert(node.next != 0 || node.prev != 0);

            if (offset == (end(parent) - 1)->child) {
                //先合并左边
                assert(node.prev != 0);
                internal_node_t prev;
                map(&prev, node.prev);
                //合并
                index_t *where = find(parent, begin(prev)->key);
                reset_index_children_parent(begin(node), end(node), node.prev);
                merge_keys(where, prev, node, true);
                unmap(&prev, node.prev);
            } else {
                //再尝试合并右边
                assert(node.next != 0);
                internal_node_t next;
                map(&next, node.next);
                // merge
                index_t *where = find(parent, index_key);
                reset_index_children_parent(begin(next), end(next), offset);
                merge_keys(where, node, next);
                unmap(&node, offset);
            }

            //递归往上删除
            remove_from_index(node.parent, parent, index_key);
        } else {
            unmap(&node, offset);
        }
    } else {
        unmap(&node, offset);
    }
}

bool BPlusTree::borrow_key(bool from_right, internal_node_t &borrower, off_t offset) {
    typedef typename internal_node_t::child_t child_t;

    //取出需要被借元素的节点
    off_t lender_off = from_right ? borrower.next : borrower.prev;
    internal_node_t lender;
    map(&lender, lender_off);

    //判断是否可以借出元素
    assert(lender.n >= meta.order / 2);
    if (lender.n != meta.order / 2) {
        child_t where_to_lend, where_to_put;

        internal_node_t parent;

        //借出元素
        if (from_right) {
            //如果是从右边节点借元素
            where_to_lend = begin(lender);
            where_to_put = end(borrower);

            map(&parent, borrower.parent);
            child_t where = lower_bound(begin(parent), end(parent) - 1, (end(borrower) - 1)->key);
            where->key = where_to_lend->key;
            unmap(&parent, borrower.parent);
        } else {
            //如果是从左边节点借元素
            where_to_lend = end(lender) - 1;
            where_to_put = begin(borrower);

            map(&parent, lender.parent);
            child_t where = find(parent, begin(lender)->key);
            where->key = (where_to_lend - 1)->key;
            unmap(&parent, lender.parent);
        }

        //存储
        std::copy_backward(where_to_put, end(borrower), end(borrower) + 1);
        *where_to_put = *where_to_lend;
        borrower.n++;

        //被借出的元素脱离原节点并更新父节点信息
        reset_index_children_parent(where_to_lend, where_to_lend + 1, offset);
        std::copy(where_to_lend + 1, end(lender), where_to_lend);
        lender.n--;
        unmap(&lender, lender_off);
        return true;
    }

    return false;
}

bool BPlusTree::borrow_key(bool from_right, leaf_node_t &borrower) {
    //取出需要被借元素的节点
    off_t lender_off = from_right ? borrower.next : borrower.prev;
    leaf_node_t lender;
    map(&lender, lender_off);

    //判断是否可以借出元素
    assert(lender.n >= meta.order / 2);
    if (lender.n != meta.order / 2) {
        typename leaf_node_t::child_t where_to_lend, where_to_put;


        if (from_right) {
            //如果是从右边节点借元素
            where_to_lend = begin(lender);
            where_to_put = end(borrower);
            change_parent_child(borrower.parent, begin(borrower)->key, lender.children[1].key);
        } else {
            //如果是从左边节点借元素
            where_to_lend = end(lender) - 1;
            where_to_put = begin(borrower);
            change_parent_child(lender.parent, begin(lender)->key, where_to_lend->key);
        }

        //被借出的元素更新到当前节点
        std::copy_backward(where_to_put, end(borrower), end(borrower) + 1);
        *where_to_put = *where_to_lend;
        borrower.n++;

        //被借出的元素脱离原节点并更新父节点信息
        std::copy(where_to_lend + 1, end(lender), where_to_lend);
        lender.n--;
        unmap(&lender, lender_off);
        return true;
    }
    return false;
}

void BPlusTree::change_parent_child(off_t parent, const key_t &o, const key_t &n) {
    internal_node_t node;
    map(&node, parent);

    //父节点中找到
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

void BPlusTree::merge_keys(index_t *where, internal_node_t &node, internal_node_t &next, bool change_where_key) {
    //(end(node) - 1)->key = where->key;
    if (change_where_key) {
        where->key = (end(next) - 1)->key;
    }
    //下一个节点的元素全部放到当前节点
    std::copy(begin(next), end(next), end(node));
    node.n += next.n;
    //移除下一个节点
    node_remove(&node, &next);
}

void BPlusTree::insert_record_no_split(leaf_node_t *leaf, const key_t &key, const value_t &value) {
    //插入元素进叶子结点  不考虑分裂的情况
    record_t *where = upper_bound(begin(*leaf), end(*leaf), key);
    std::copy_backward(where, end(*leaf), end(*leaf) + 1);
    where->key = key;
    where->value = value;
    leaf->n++;
}

void BPlusTree::insert_key_to_index(off_t offset, const key_t &key, off_t old, off_t after) {
    if (offset == 0) {
        //如果是新树 则创建新节点
        internal_node_t root;
        root.next = root.prev = root.parent = 0;
        meta.root_offset = alloc(&root);
        meta.height++;

        //将叶子节点的变动传递到根节点
        root.n = 2;
        root.children[0].key = key;
        root.children[0].child = old;
        root.children[1].child = after;

        unmap(&meta, OFFSET_META);
        unmap(&root, meta.root_offset);

        //改变子节点的父节点状态
        reset_index_children_parent(begin(root), end(root),
                                    meta.root_offset);
        return;
    }

    internal_node_t node;
    map(&node, offset);
    assert(node.n <= meta.order);

    //如果插入后超过B+树的阶则需要分裂
    if (node.n == meta.order) {
        internal_node_t new_node;
        node_create(offset, &node, &new_node);

        //找到分裂点
        size_t point = (node.n - 1) / 2;
        bool place_right = keyCmp(key, node.children[point].key) > 0;
        if (place_right) {
            ++point;
        }
        if (place_right && keyCmp(key, node.children[point].key) < 0)
            point--;
        key_t middle_key = node.children[point].key;

        //分裂
        std::copy(begin(node) + point + 1, end(node), begin(new_node));
        new_node.n = node.n - point - 1;
        node.n = point + 1;

        //将新的元素插入节点
        if (place_right)
            insert_key_to_index_no_split(new_node, key, after);
        else
            insert_key_to_index_no_split(node, key, after);

        unmap(&node, offset);
        unmap(&new_node, node.next);

        //更新子节点的父节点状态
        reset_index_children_parent(begin(new_node), end(new_node), node.next);

        //递归向上改变
        insert_key_to_index(node.parent, middle_key, offset, node.next);
    } else {
        insert_key_to_index_no_split(node, key, after);
        unmap(&node, offset);
    }
}

void BPlusTree::insert_key_to_index_no_split(internal_node_t &node, const key_t &key, off_t value) {
    //插入元素进中间节点 不考虑分裂
    index_t *where = upper_bound(begin(node), end(node) - 1, key);
    std::copy_backward(where, end(node), end(node) + 1);
    where->key = key;
    where->child = (where + 1)->child;
    (where + 1)->child = value;
    node.n++;
}

void BPlusTree::reset_index_children_parent(index_t *begin, index_t *end,
                                            off_t parent) {
    //改变节点的父节点信息
    internal_node_t node;
    while (begin != end) {
        map(&node, begin->child);
        node.parent = parent;
        unmap(&node, begin->child, SIZE_NO_CHILDREN);
        ++begin;
    }
}

//根据key获取节点并返回offset
off_t BPlusTree::search_index(const key_t &key) const {
    off_t org = meta.root_offset;
    int height = meta.height;
    //从第一层一直往下找
    while (height > 1) {
        internal_node_t node;
        map(&node, org);

        index_t *i = upper_bound(begin(node), end(node) - 1, key);
        org = i->child;
        --height;
    }

    return org;
}

//通过key寻找叶子结点并返回位置
off_t BPlusTree::search_leaf(off_t index, const key_t &key) const {
    internal_node_t node;
    map(&node, index);
    index_t *i = upper_bound(begin(node), end(node) - 1, key);
    return i->child;
}

template<class T>
void BPlusTree::node_create(off_t offset, T *node, T *next) {
    //新节点
    next->parent = node->parent;
    next->next = node->next;
    next->prev = offset;
    node->next = alloc(next);
    //更新新增节点的前驱节点
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
    //删除一个节点并更新前驱后继节点
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
    //初始化元信息
    memset(&meta, 0, sizeof(meta_t));
    meta.order = BP_ORDER;
    meta.value_size = sizeof(value_t);
    meta.key_size = sizeof(key_t);
    meta.height = 1;
    meta.slot = OFFSET_BLOCK;

    //初始化根节点
    internal_node_t root;
    root.next = root.prev = root.parent = 0;
    meta.root_offset = alloc(&root);

    //初始化叶子结点
    leaf_node_t leaf;
    leaf.next = leaf.prev = 0;
    leaf.parent = meta.root_offset;
    meta.leaf_offset = root.children[0].child = alloc(&leaf);

    //持久化
    unmap(&meta, OFFSET_META);
    unmap(&root, meta.root_offset);
    unmap(&leaf, root.children[0].child);
}




