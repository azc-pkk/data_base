use std::{
    cell::RefCell, collections::btree_map::Keys, rc::Rc
};
use crate::error::BPlusTreeError;

/// 用一个序列化字符串存数据
#[derive(Debug, Clone)]
struct SerializedData(String);

impl SerializedData {
    // TODO: 实现数据的序列化
    fn new(data: String) -> Self {
        Self(data)
    }
    // TODO: 实现数据的反序列化
    fn unserialize(&self) -> String {
        String::from(&self.0)
    }
}

impl From<String> for SerializedData {
    // TODO: 实现数据的序列化
    fn from(data: String) -> Self {
        Self(data)
    }
}

#[derive(Debug, Clone)]
struct DataEntry<K>
where
    K: Ord + Clone,
{
    key: K,
    data: SerializedData,
}

impl<K: Ord + Clone> PartialEq for DataEntry<K> {
    fn eq(&self, other: &Self) -> bool {
        return self.key == other.key;
    }
}

impl<K: Ord + Clone> Eq for DataEntry<K> {}

impl<K: Ord + Clone> PartialOrd for DataEntry<K> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.key.cmp(&other.key))
    }
}

impl<K: Ord + Clone> Ord for DataEntry<K> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.key.cmp(&other.key)
    }
}

#[derive(Debug, Clone)]
enum BPlusTreeNode<K>
where
    K: Ord + Clone,
{
    Internal(InternalNode<K>),
    Leaf(LeafNode<K>)
}

#[derive(Debug, Clone)]
struct InternalNode<K>
where
    K: Ord + Clone,
{
    /// key 为左边子树最大元素
    keys: Vec<K>,
    /// children 与 keys 数量相同
    children: Vec<Rc<RefCell<BPlusTreeNode<K>>>>
}

/// 存数据的叶子结点。每个节点存多项数据。
#[derive(Debug, Clone)]
struct LeafNode<K>
where
    K: Ord + Clone,
{
    entries: Vec<DataEntry<K>>,
    next: Option<Rc<RefCell<BPlusTreeNode<K>>>>
}

#[derive(Debug, Clone)]
struct BPlusTree<K>
where
    K: Ord + Clone,
{
    root: Rc<RefCell<BPlusTreeNode<K>>>,
    // TODO: 增加指向最小的叶子结点的指针
    // 预计需要重写 get_all_entries、insert 函数

    /// 叶子结点最多存的数据条数
    /// 
    /// 预计把一个节点映射为硬盘上的一页，所以需要根据数据条目大小动态调整
    leaf_max_entries: usize,

    /// 中间结点最多存的数据条数
    max_children_cnt: usize,
}

impl<K> BPlusTree<K>
where
    K: Ord + Clone,
{
    /// 尝试根据 `DataEntry` 数组构建 B+ 树
    /// 
    /// `max_data_length` 表示一条 entry 的最大大小
    /// 
    /// `max_key_size` 表示作为 key 的最大大小
    fn try_new(data_set: Vec<DataEntry<K>>, max_data_length: usize, key_size: usize) -> Result<Self, BPlusTreeError> {
        if data_set.is_empty() {
            return Err(BPlusTreeError::BPlusTreeBuildError);
        }
        return Ok(Self::new(data_set, max_data_length, key_size));
    }

    fn new(data_set: Vec<DataEntry<K>>, max_data_length: usize, key_size: usize) -> Self {
        // FIXME: 设定页大小，改叶子结点最大数据条数
        let leaf_max_entries = 10;
        // 构建叶子结点
        let leaves = Self::build_leaf_nodes(data_set, leaf_max_entries);
        // FIXME: 设定页大小，改中间结点最大孩子数
        let max_children_cnt = 10;
        // 构建中间节点
        let mut internals = Self::build_internal_nodes(leaves, max_children_cnt);
        // 只有一个节点时可以直接把它当根节点
        while internals.len() != 1 {
            internals = Self::build_internal_nodes(internals, max_children_cnt);
        }
        Self {
            root: Rc::clone(&internals[0]),
            leaf_max_entries,
            max_children_cnt
        }
    }

    fn get_max_key_of_node(node: Rc<RefCell<BPlusTreeNode<K>>>) -> K {
        let node_borrow = node.borrow();
        match &*node_borrow {
            BPlusTreeNode::Internal(internal) => {
                internal.keys.last().unwrap().clone()
            },
            BPlusTreeNode::Leaf(leaf) => {
                leaf.entries.last().unwrap().key.clone()
            }
        }
    }

    // TODO: 并行构建中间节点
    fn build_internal_nodes(mut node_list: Vec<Rc<RefCell<BPlusTreeNode<K>>>>, max_children_cnt: usize) -> Vec<Rc<RefCell<BPlusTreeNode<K>>>> {
        let mut out: Vec<Rc<RefCell<BPlusTreeNode<K>>>> = vec![];
        while !node_list.is_empty() {
            let rest = match node_list.len() {
                x if x >= max_children_cnt => {
                    node_list.split_off(max_children_cnt)
                }
                _ => {
                    vec![]
                }
            };
            let keys: Vec<K> = node_list.iter()
                                        .map(|node| Self::get_max_key_of_node(Rc::clone(node)))
                                        .collect();
            let children: Vec<Rc<RefCell<BPlusTreeNode<K>>>> = node_list.iter()
                                                                        .map(|node| Rc::clone(node))
                                                                        .collect();
            let internal = Rc::new(RefCell::new(BPlusTreeNode::Internal(InternalNode {
                keys,
                children,
            })));
            out.push(internal);
            node_list = rest;
        }
        out
    }

    // TODO: 多线程构建叶子节点
    fn build_leaf_nodes(mut data_set: Vec<DataEntry<K>>, leaf_max_entries: usize) -> Vec<Rc<RefCell<BPlusTreeNode<K>>>> {
        let mut out: Vec<Rc<RefCell<BPlusTreeNode<K>>>> = vec![];
        while !data_set.is_empty() {
            // 先分块，得到一个叶子节点
            let rest = match data_set.len() {
                x if x >= leaf_max_entries => {
                    data_set.split_off(leaf_max_entries)
                },
                _ => {
                    vec![]
                }
            };
            let leaf = Rc::new(RefCell::new(BPlusTreeNode::Leaf(LeafNode {
                entries: data_set,
                next: None,
            })));
            if let Some(prev) = out.last() {
                let mut prev_borrow = prev.borrow_mut();
                if let BPlusTreeNode::Leaf(prev_inner) = &mut *prev_borrow {
                    prev_inner.next = Some(Rc::clone(&leaf));
                }
            }
            out.push(leaf);
            data_set = rest;
        }
        out
    }

    /// 返回序列化的数据数组
    fn get_all_entries(&self) -> Vec<SerializedData> {
        // TODO: 修改 BPlusTree 定义后重写这一段
        // 先找到最小叶子结点
        let mut node = self.root.clone();
        let mut current_leaf = loop {
            let maybe_next = {
                let node_borrow = node.borrow();
                match &*node_borrow {
                    BPlusTreeNode::Internal(internal) => Some(internal.children[0].clone()),
                    BPlusTreeNode::Leaf(_) => None
                }
            };
            match maybe_next {
                None => break node,
                Some(next) => node = next
            }
        };

        let mut out = vec![];

        loop {
            let maybe_next = {
                let leaf_borrow = current_leaf.borrow();
                if let BPlusTreeNode::Leaf(leaf) = &*leaf_borrow {
                    for entry in leaf.entries.iter() {
                        out.push(entry.data.clone());
                    }
                    leaf.next.as_ref().map(|next| next.clone())
                } else {
                    None
                }
            };
            if let Some(next) = maybe_next {
                current_leaf = next;
            } else {
                break;
            }
        }
        out
    }

    /// 返回序列化的数据。
    fn get_entry_by_key(&self, key: K) -> Option<SerializedData> {
        let mut node = self.root.clone();
        loop {
            let maybe_next = {
                let node_borrow = node.borrow();
                match &*node_borrow {
                    BPlusTreeNode::Internal(internal) => {
                        let index = internal.keys.partition_point(|k| *k < key);
                        // keys 与 children 大小一样，大于最大 key 说明没有
                        if index == internal.keys.len() {
                            return None;
                        }
                        internal.children[index].clone()
                    },
                    BPlusTreeNode::Leaf(leaf) => {
                        for entry in leaf.entries.iter() {
                            if entry.key == key {
                                return Some(entry.data.clone());
                            }
                        }
                        return None;
                    }
                }
            };
            node = maybe_next;
        }
    }


    // /// 需要处理一种情况：子节点的最大值改变了？插入时好像不可能出现这种情况。这样有一个问题是可能一个节点会

    // /// 插入函数。
    // /// 插入成功时，如果不需要分裂，返回 `Ok(None)`
    // /// 插入成功时，如果需要分裂，返回新产生的两个 key 和它们中间的节点
    // /// 插入失败时，返回一个 `BPlusTreeError`
    // /// 递归插入，返回时处理需要分裂的情况
    // /// FIXME: 修改返回值类型
    // fn insert(&self, current: Rc<RefCell<BPlusTreeNode<K>>>, entry: DataEntry<K>) -> Result<Option<(K, Rc<RefCell<BPlusTreeNode<K>>>, K)>, BPlusTreeError> {
    //     let mut current_borrow = current.borrow_mut();
    //     match &mut *current_borrow {
    //         BPlusTreeNode::Leaf(leaf) => {
    //             // 插入数据
    //             let insert_pos = leaf.entries.partition_point(|x| *x < entry);
    //             leaf.entries.insert(insert_pos, entry);
    //             // 分裂叶子节点
    //             if leaf.entries.len() > self.leaf_max_entries {
    //                 let right_entries = leaf.entries.split_off(leaf.entries.len() / 2);
    //                 let left_key = leaf.entries.last().unwrap().key.clone();
    //                 let right_key = right_entries.last().unwrap().key.clone();
    //                 let new_leaf = Rc::new(RefCell::new(BPlusTreeNode::Leaf(LeafNode {
    //                     entries: right_entries,
    //                     next: leaf.next.clone(),
    //                 })));
    //                 leaf.next = Some(new_leaf.clone());
    //                 return Ok(Some((left_key, new_leaf, right_key)));
    //             }
    //         },
    //         BPlusTreeNode::Internal(internal) => {
    //             // 递归插入
    //             let insert_pos = internal.keys.partition_point(|x| *x < entry.key);
    //             match self.insert(Rc::clone(&internal.children[insert_pos]), entry) {
    //                 Ok(None) => return Ok(None),
    //                 // FIXME
    //                 Ok(Some((left_key, new_node, right_key))) => {
    //                     // 用新的 key 代替原来的 key，再在后面插入新节点 key 的最大值
    //                 },
    //                 Err(err) => return Err(err),
    //             }
    //             // 插入新的 key 和 child
    //             // 分裂中间节点
    //             // 特判是否
    //             // 特判根节点
    //         }
    //     }
        
    //     Ok(None)
    // }
}

#[cfg(test)]
mod test {
    use rand::{distr::Alphanumeric, Rng};

    use crate::storage::b_plus_tree::{BPlusTree, DataEntry, SerializedData};

    fn get_random_string(len: usize) -> String {
        // 生成一个随机字符串
        rand::rng()
            .sample_iter(&Alphanumeric)
            .take(len)
            .map(char::from)
            .collect()
    }

    #[test]
    fn new_test() {
        let entries: Vec<DataEntry<i32>> = (0..20).into_iter().map(|i| DataEntry { key: i, data: SerializedData::from(get_random_string(10))}).collect();
        let supposed_result = entries.clone();
        let tree = BPlusTree::try_new(entries, 0, 0).unwrap();
        let result = tree.get_all_entries();
        for i in 0..20 {
            println!("result: {:?}", result[i]);
            println!("supposed result: {:?}", supposed_result[i]);
            assert_eq!(supposed_result[i].data.unserialize(), result[i].unserialize());
        }
        assert_eq!(tree.get_entry_by_key(12).unwrap().unserialize(), supposed_result[12].data.unserialize());
        assert_eq!(tree.get_entry_by_key(-1).unwrap_or(SerializedData(String::from("failed"))).unserialize(), String::from("failed"));
    }
}