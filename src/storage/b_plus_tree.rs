use std::{
    cell::{RefCell}, rc::Rc
};
use rand::rand_core::le;

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

impl<K> BPlusTreeNode<K>
where
    K: Ord + Clone,
{
    fn as_leaf(&self) -> Option<&LeafNode<K>> {
        match self {
            BPlusTreeNode::Leaf(leaf) => Some(leaf),
            _ => None,
        }
    }

    fn as_leaf_mut(&mut self) -> Option<&mut LeafNode<K>> {
        match self {
            BPlusTreeNode::Leaf(leaf) => Some(leaf),
            _ => None
        }
    }

    fn as_internal(&self) -> Option<&InternalNode<K>> {
        match self {
            BPlusTreeNode::Internal(internal) => Some(internal),
            _ => None,
        }
    }

    fn as_internal_mut(&mut self) -> Option<&mut InternalNode<K>> {
        match self {
            BPlusTreeNode::Internal(internal) => Some(internal),
            _ => None,
        }
    }
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


#[derive(Debug, Clone)]
enum InsertState<K>
where
    K: Ord + Clone,
{
    Success,
    Splited(Rc<RefCell<BPlusTreeNode<K>>>),
    Failed(BPlusTreeError)
}


#[derive(Debug, Clone)]
enum RemoveState {
    Success,
    Failed(BPlusTreeError)
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
    /// 
    /// `data_set` **必须有序**！
    pub fn try_new(data_set: Vec<DataEntry<K>>, max_data_length: usize, key_size: usize) -> Result<Self, BPlusTreeError> {
        if data_set.is_empty() {
            return Err(BPlusTreeError::BPlusTreeBuildError);
        }
        return Ok(Self::new(data_set, max_data_length, key_size));
    }

    fn new(data_set: Vec<DataEntry<K>>, max_data_length: usize, key_size: usize) -> Self {
        // FIXME: 设定页大小，改叶子结点最大数据条数
        let leaf_max_entries = 4;
        // 构建叶子结点
        let leaves = Self::build_leaf_nodes(data_set, leaf_max_entries);
        // FIXME: 设定页大小，改中间结点最大孩子数
        let max_children_cnt = 4;
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

    fn get_node_length(node: Rc<RefCell<BPlusTreeNode<K>>>) -> usize {
        let node_borrow = node.borrow();
        match &*node_borrow {
            BPlusTreeNode::Internal(internal) => {
                internal.children.len()
            },
            BPlusTreeNode::Leaf(leaf) => {
                leaf.entries.len()
            }
        }
    }

    fn get_max_key_of_node(node: Rc<RefCell<BPlusTreeNode<K>>>) -> K {
        // 要保证前面没有 borrow_mut
        let node_borrow = node.borrow();
        match &*node_borrow {
            BPlusTreeNode::Internal(internal) => {
                internal.keys.last().cloned().expect("internal node is empty.")
            },
            BPlusTreeNode::Leaf(leaf) => {
                leaf.entries.last().cloned().map(|x| x.key.clone()).expect("leaf node is empty.")
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

    /// 返回 `DataEntry` 数组
    pub fn get_all_entries(&self) -> Vec<DataEntry<K>> {
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
                        out.push(entry.clone());
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

    /// 返回`DataEntry`。
    pub fn get_entry_by_key(&self, key: K) -> Option<DataEntry<K>> {
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
                                return Some(entry.clone());
                            }
                        }
                        return None;
                    }
                }
            };
            node = maybe_next;
        }
    }

    pub fn insert(&mut self, data: DataEntry<K>) -> Result<(), BPlusTreeError> {
        let result = self.insert_recursive(Rc::clone(&self.root), data);
        match result {
            InsertState::Failed(error) => Err(error),
            InsertState::Splited(new_node) => {
                self.root = Rc::new(RefCell::new(BPlusTreeNode::Internal(InternalNode {
                    keys: vec![Self::get_max_key_of_node(Rc::clone(&self.root)), Self::get_max_key_of_node(Rc::clone(&new_node))],
                    children: vec![Rc::clone(&self.root), Rc::clone(&new_node)]
                })));
                Ok(())
            }
            _ => Ok(())
        }
    }

    fn insert_recursive(&self, node: Rc<RefCell<BPlusTreeNode<K>>>, data: DataEntry<K>) -> InsertState<K> {
        let (insert_pos, child) = {
            let mut node_borrow = node.borrow_mut();
            match &mut *node_borrow {
                BPlusTreeNode::Internal(internal) => {
                    let insert_pos = match internal.keys.partition_point(|k| *k < data.key) {
                        x if x == internal.keys.len() => x-1,
                        x => x
                    };
                    (insert_pos, Rc::clone(&internal.children[insert_pos]))
                },
                BPlusTreeNode::Leaf(leaf) => {
                    let insert_pos = leaf.entries.partition_point(|x| *x < data);
                    leaf.entries.insert(insert_pos, data);
                    if leaf.entries.len() > self.leaf_max_entries {
                        drop(node_borrow);
                        let new_node = Self::spilt(Rc::clone(&node));
                        return InsertState::Splited(new_node);
                    }
                    return InsertState::Success;
                }
            }
        };

        let result = self.insert_recursive(Rc::clone(&child), data);
        match result {
                InsertState::Splited(new_node) => {
                // 使用被分裂的 child 节点的最大键作为 old_key（而不是 parent node）
                let old_key = Self::get_max_key_of_node(Rc::clone(&child));
                let new_key = Self::get_max_key_of_node(Rc::clone(&new_node));

                let mut node_borrow = node.borrow_mut();
                if let BPlusTreeNode::Internal(internal) = &mut *node_borrow {
                    internal.keys[insert_pos] = old_key;
                    internal.keys.insert(insert_pos + 1, new_key);
                    internal.children.insert(insert_pos + 1, Rc::clone(&new_node));

                    if internal.keys.len() > self.max_children_cnt {
                        drop(node_borrow);
                        InsertState::Splited(Self::spilt(node))
                    } else {
                        InsertState::Success
                    }
                } else {
                    unreachable!()
                }
            }
            other => other,
        }
    }

    pub fn remove(&self, key: K) -> RemoveState {
        // 使用栈来存储父节点。
        let mut stack: Vec<Rc<RefCell<BPlusTreeNode<K>>>> = vec![];
        let result = self.remove_recursive(Rc::clone(&self.root), key, &mut stack);
        todo!()
    }

    /// 递归删除，用栈来保存经过的节点。
    /// 需要平衡时每次取三个节点合并后分裂
    /// 
    fn remove_recursive(&self, node: Rc<RefCell<BPlusTreeNode<K>>>, key: K, stack: &mut Vec<Rc<RefCell<BPlusTreeNode<K>>>>) -> RemoveState {
        let mut node_borrow_mut = node.borrow_mut();
        let balance_required = {
            match &mut *node_borrow_mut {
                BPlusTreeNode::Internal(internal) => {
                    let remove_pos = internal.keys.partition_point(|x| *x < key);
                    if remove_pos == internal.keys.len() {
                        return RemoveState::Failed(BPlusTreeError::BPlusTreeRemoveError);
                    }

                    // 递归调用前先入栈
                    stack.push(Rc::clone(&node));

                    let child = Rc::clone(&internal.children[remove_pos]);
                    let result = self.remove_recursive(child, key, stack);
                    if matches!(&result, RemoveState::Failed(_)) {
                        return result;
                    }

                    internal.keys.len() < self.max_children_cnt / 2
                },
                BPlusTreeNode::Leaf(leaf) => {
                    let remove_pos = leaf.entries.partition_point(|x| x.key < key);
                    if remove_pos == leaf.entries.len() {
                        return RemoveState::Failed(BPlusTreeError::BPlusTreeRemoveError);
                    }

                    leaf.entries.remove(remove_pos);
                    leaf.entries.len() < self.leaf_max_entries / 2
                }
            }
        };

        // 找父节点
        let parent_borrow = stack[stack.len() - 1].borrow();
        let inner_parent = parent_borrow.as_internal().unwrap();
        // 获得当前节点在父节点孩子列表中的位置
        let max_key = Self::get_max_key_of_node(Rc::clone(&node));
        let child_pos = inner_parent.keys.partition_point(|x| *x < max_key);
        // 找用于合并的兄弟节点
        // FIXME:考虑节点不够的情况
        let left_sibling = if child_pos == 0 { Rc::clone(&inner_parent.children[child_pos+2])} else { Rc:: clone(&inner_parent.children[child_pos-1]) };
        let right_sibling = if child_pos == inner_parent.keys.len() - 1 { Rc::clone(&inner_parent.children[child_pos-2]) } else { Rc::clone(&inner_parent.children[child_pos+1]) };
        Self::merge(Rc::clone(&node), left_sibling);
        Self::merge(Rc::clone(&node), right_sibling);
        
        let split_required = {
            let len = Self::get_node_length(Rc::clone(&node));
            let node_borrow = node.borrow();
            if matches!(&*node_borrow, BPlusTreeNode::Internal(_)) {
                len > self.max_children_cnt
            }
            else {
                len > self.leaf_max_entries
            }
        };
        if split_required {
            let new_node = Self::spilt(Rc::clone(&node));
        }

        todo!()
    }

    /// 将 `node2` 合并到 `node1` 中，**需要用到可变引用**
    fn merge(node1: Rc<RefCell<BPlusTreeNode<K>>>, node2: Rc<RefCell<BPlusTreeNode<K>>>) {
        // 中间节点和叶子结点分情况讨论
        let is_internal = if matches!(*node1.borrow(), BPlusTreeNode::Internal(_)) { true } else { false };
        let mut node1_borrow_mut = node1.borrow_mut();
        let mut node2_borrow_mut = node2.borrow_mut();
        if is_internal {
            let internal1 = node1_borrow_mut.as_internal_mut().unwrap();
            let internal2 = node2_borrow_mut.as_internal_mut().unwrap();
            internal1.children.append(&mut internal2.children);
            internal1.keys.append(&mut internal2.keys);
        }
        else {
            let leaf1 = node1_borrow_mut.as_leaf_mut().unwrap();
            let leaf2 = node2_borrow_mut.as_leaf_mut().unwrap();
            leaf1.entries.append(&mut leaf2.entries);
            leaf1.next = leaf2.next.clone();
        }
    }

    /// 返回分裂出的右边节点。左边节点保存在原本的变量中。
    fn spilt(node: Rc<RefCell<BPlusTreeNode<K>>>) -> Rc<RefCell<BPlusTreeNode<K>>> {
        let mut node_borrow = node.borrow_mut();
        match &mut *node_borrow {
            BPlusTreeNode::Internal(internal) => {
                let mid = internal.keys.len() / 2;
                Rc::new(RefCell::new(BPlusTreeNode::Internal(InternalNode {
                    keys: internal.keys.split_off(mid),
                    children: internal.children.split_off(mid),
                })))
            },
            BPlusTreeNode::Leaf(leaf) => {
                let mid = leaf.entries.len() / 2;
                let new_node = Rc::new(RefCell::new(BPlusTreeNode::Leaf(LeafNode {
                    entries: leaf.entries.split_off(mid),
                    next: leaf.next.clone()
                })));
                leaf.next = Some(Rc::clone(&new_node));
                new_node
            }
        }
    }
}

#[cfg(test)]
mod test {
    use rand::{distr::Alphanumeric, seq::SliceRandom, Rng};

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
            assert_eq!(supposed_result[i].data.unserialize(), result[i].data.unserialize());
        }
        assert_eq!(tree.get_entry_by_key(12).unwrap().data.unserialize(), supposed_result[12].data.unserialize());
        assert_eq!(tree.get_entry_by_key(-1), None);
    }

    #[test]
    fn ordered_insert_test() {
        // 生成 50 个 DataEntry
        let raw_entries: Vec<DataEntry<i32>> = (0..50).into_iter().map(|i| DataEntry { key: i, data: SerializedData::from(get_random_string(10)) }).collect();
        let mut build_entries = raw_entries.clone();
        let test_entries = build_entries.split_off(1);

        let mut tree = BPlusTree::new(build_entries, 0, 0);
        for entry in test_entries.iter() {
            tree.insert(entry.to_owned()).expect("something wrong");
        }

        let result = tree.get_all_entries();
        for i in 0..50 {
            assert_eq!(result[i].data.unserialize(), raw_entries[i].data.unserialize());
        }
    }

    #[test]
    fn random_insert_test() {
        // 生成 50 个 DataEntry
        let raw_entries: Vec<DataEntry<i32>> = (0..50).into_iter().map(|i| DataEntry { key: i, data: SerializedData::from(get_random_string(10)) }).collect();
        let mut build_entries = raw_entries.clone();
        let mut test_entries = build_entries.split_off(20);
        let mut rng = rand::rng();
        test_entries.shuffle(&mut rng);

        println!("build_entries:");
        for entry in build_entries.iter() {
            println!("DataEntry{{ key: {}, data: {:?} }},", entry.key, entry.data);
        }
        println!("insert_entries:");
        for entry in test_entries.iter() {
            println!("DataEntry{{ key: {}, data: {:?} }},", entry.key, entry.data);
        }

        let mut tree = BPlusTree::new(build_entries, 0, 0);
        for entry in test_entries.iter() {
            tree.insert(entry.to_owned()).expect("something wrong");
        }
        let result = tree.get_all_entries();
        for i in 0..50 {
            println!("round {i}, supposed result: {}, actual result: {}", raw_entries[i].data.unserialize(), result[i].data.unserialize());
            println!("  supposed key: {}, actual key: {}", raw_entries[i].key, result[i].key);
            assert_eq!(result[i].data.unserialize(), raw_entries[i].data.unserialize());
        }
    }
}