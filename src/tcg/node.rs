use serde::Serialize;
use std::collections::HashMap;
use std::mem::size_of_val;

// Pending nodes are created before their call executes, so concurrent rollouts
// that reach the same call wait for one execution instead of repeating it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeStatus {
    #[default]
    Ready,
    Pending,
    Failed,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct Node {
    pub children: HashMap<String, Node>,
    pub node_id: u64, // unique within a TCG and never reused; the root is 0
    pub status: NodeStatus,
    pub generation: u32, // bumped on every re-claim so stale owners cannot publish
    #[serde(skip)]
    pub owner_connection_id: Option<u64>, // set only while Pending

    pub sandbox_id: Option<String>,
    pub value: Option<String>,
    pub runtime: u32, // milliseconds
    pub ttl_present: bool,
    pub ttl: u32, // seconds
    pub inserted_at_seconds: u64,
    pub number_of_cache_hits: u64,
    pub number_of_lpm_hits: u64,
    pub size_in_bytes: u32, // A single entry can't be more than 4GB, need to update this if we remove sandbox id later.
}

impl Node {
    pub fn new_child(node_id: u64) -> Self {
        Self {
            node_id,
            generation: 1,
            ..Self::default()
        }
    }

    pub fn walk_subtcg<T, F>(&self, initial: T, f: &mut F) -> T
    where
        F: FnMut(T, &Node) -> T,
    {
        let mut acc = f(initial, self);

        for child in self.children.values() {
            acc = child.walk_subtcg(acc, f);
        }

        return acc;
    }

    // Methods on Node (not on the TCG) so callers borrow only the root field
    // and can still update the TCG's counters while holding the result.
    pub fn descendant(&self, path: &[String]) -> Option<&Node> {
        let mut curr = self;

        for path_entry in path {
            curr = curr.children.get(path_entry)?;
        }

        Some(curr)
    }

    pub fn descendant_mut(&mut self, path: &[String]) -> Option<&mut Node> {
        let mut curr = self;

        for path_entry in path {
            curr = curr.children.get_mut(path_entry)?;
        }

        Some(curr)
    }

    pub fn subtree_has_pending(&self) -> bool {
        self.walk_subtcg(false, &mut |acc, node| acc || node.status == NodeStatus::Pending)
    }

    // An empty string is a valid result, so readiness is value presence, not content.
    pub fn is_ready(&self) -> bool {
        self.status == NodeStatus::Ready && self.value.is_some()
    }

    pub fn update_size_in_bytes(&mut self, path_entry_size_in_bytes: u32) {
        let value_size_in_bytes = self.value.as_ref().map_or(0, |value| value.len() as u32);

        let sandbox_id_size_in_bytes = self
            .sandbox_id
            .as_ref()
            .map_or(0, |sandbox_id| sandbox_id.len() as u32);

        self.size_in_bytes = path_entry_size_in_bytes
            + value_size_in_bytes
            + sandbox_id_size_in_bytes
            + size_of_val(&self.node_id) as u32
            + size_of_val(&self.status) as u32
            + size_of_val(&self.generation) as u32
            + size_of_val(&self.runtime) as u32
            + size_of_val(&self.ttl_present) as u32
            + size_of_val(&self.ttl) as u32
            + size_of_val(&self.inserted_at_seconds) as u32
            + size_of_val(&self.number_of_cache_hits) as u32
            + size_of_val(&self.number_of_lpm_hits) as u32;
    }
}
