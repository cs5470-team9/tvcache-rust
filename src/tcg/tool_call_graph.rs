use crate::tcg::node::NodeStatus;
use crate::tcg::Node;
use crate::types::SandboxCandidate;
use crate::utils;
use serde::Serialize;
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use tokio::sync::Notify;

// Wire values of the claim/inspect status byte.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClaimStatus {
    Missing = 0,
    Owner = 1,
    Pending = 2,
    Ready = 3,
    Failed = 4,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClaimOutcome {
    pub status: ClaimStatus,
    pub node_id: u64,
    pub generation: u32,
    pub value: Option<String>, // only for Ready
}

impl ClaimOutcome {
    pub fn missing() -> Self {
        Self {
            status: ClaimStatus::Missing,
            node_id: 0,
            generation: 0,
            value: None,
        }
    }

    // What a non-claiming observer sees; a node without a value is not a result yet.
    fn observe(node: &Node) -> Self {
        let status = match node.status {
            NodeStatus::Pending => ClaimStatus::Pending,
            NodeStatus::Failed => ClaimStatus::Failed,
            NodeStatus::Ready if node.value.is_some() => ClaimStatus::Ready,
            NodeStatus::Ready => ClaimStatus::Missing,
        };

        Self {
            status,
            node_id: node.node_id,
            generation: node.generation,
            value: if status == ClaimStatus::Ready {
                node.value.clone()
            } else {
                None
            },
        }
    }
}

// The right to complete one pending node. The connection id is server-side
// only, so a waiter cannot publish; the generation rejects a stale owner.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Ownership {
    pub connection_id: u64,
    pub generation: u32,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct ToolCallGraph {
    pub root: Node,
    pub number_of_nodes: u32,
    pub number_of_sandboxes: u32,
    pub size_in_bytes: u64,
    // sandbox_id -> reference count; only positive counts are stored, an
    // absent entry means a count of zero.
    pub sandbox_references: BTreeMap<String, u32>,
    pub reclaimable_sandboxes: BTreeSet<String>, // Sandboxes marked for eviction (by the sandbox manager or the internal eviction loop) that are waiting to be handed to the deletion loop
    pub fork_cost_ema_ms: u32, // EMA of client-reported sandbox fork times for this task, in milliseconds, u32 is ok here because, it can store up to 4 million seconds which is > 40 days.
    pub next_node_id: u64, // last assigned node id; ids are never reused, even after eviction
    // Signalled whenever a pending node settles, so waiters can re-inspect.
    #[serde(skip)]
    pub completions: Arc<Notify>,
}

impl ToolCallGraph {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(
        &mut self,
        path: Vec<String>,
        value: Option<String>,
        sandbox_id: Option<String>,
        runtime: u32,
        ttl: Option<u32>,
        ttl_exists: bool,
    ) -> (bool, bool) {
        if path.is_empty() {
            return (false, false);
        }

        let last_idx = path.len() - 1;
        let mut curr: &mut Node = &mut self.root;

        for (idx, p) in path.into_iter().enumerate() {
            if idx < last_idx {
                if let Some(next) = curr.children.get_mut(&p) {
                    curr = next;
                } else {
                    // Not inserting, so no need to call update size.
                    return (false, false);
                }
            } else {
                let path_entry_size_in_bytes = p.len() as u32;
                let mut sandbox_id_consumed = false;

                let (new_node, old_size_in_bytes, is_new_node) = match curr.children.entry(p) {
                    Entry::Occupied(entry) => {
                        let node = entry.into_mut();
                        let old_size_in_bytes = node.size_in_bytes;

                        (node, old_size_in_bytes, false)
                    }
                    Entry::Vacant(entry) => {
                        self.number_of_nodes += 1;
                        self.next_node_id += 1;

                        (entry.insert(Node::new_child(self.next_node_id)), 0, true)
                    }
                };

                // Only the owner completes a pending node (through publish), so a
                // plain insert never writes a value into one.
                let value_blocked = new_node.status == NodeStatus::Pending && value.is_some();

                // Merge semantics: absent fields leave existing node state
                // untouched, so an insert with only sandbox cannot destroy a cached
                // value or clear a TTL. New nodes start from Node::new_child(),
                // so the same code will fully initialize them.
                if !value_blocked && (is_new_node || value.is_some()) {
                    if value.is_some() {
                        new_node.status = NodeStatus::Ready;
                    }

                    new_node.inserted_at_seconds = utils::current_utc_seconds();
                    new_node.value = value;
                    new_node.runtime = runtime;
                }

                if ttl_exists {
                    new_node.ttl_present = true;

                    if let Some(ttl) = ttl {
                        new_node.ttl = ttl;
                    }
                }

                if sandbox_id.is_some() && new_node.sandbox_id.is_none() {
                    new_node.sandbox_id = sandbox_id;
                    self.number_of_sandboxes += 1;
                    sandbox_id_consumed = true;
                }

                new_node.update_size_in_bytes(path_entry_size_in_bytes);
                self.size_in_bytes =
                    self.size_in_bytes - old_size_in_bytes as u64 + new_node.size_in_bytes as u64;

                return (!value_blocked, sandbox_id_consumed);
            }
        }

        (false, false)
    }

    pub fn find(
        &mut self,
        path: Vec<String>,
    ) -> (Option<String>, (Option<Vec<String>>, Option<String>)) {
        // this function tries to find the exact path in the tcg and return the value of the last node or return the longest prefix match with a sandbox id

        let (sandbox_path, sandbox_id) = self.deepest_sandbox(&path);
        let mut value_of_path: Option<String> = None;

        if !path.is_empty() {
            if let Some(node) = self.root.descendant_mut(&path) {
                node.number_of_cache_hits += 1;
                value_of_path = node.value.clone();
            }
        }

        if value_of_path.is_none() {
            if let Some(sandbox_path) = &sandbox_path {
                self.record_lpm_hit(sandbox_path);
            }
        }

        return (value_of_path, (sandbox_path, sandbox_id));
    }

    // The deepest node along path (the root excluded) that holds a sandbox.
    pub fn deepest_sandbox(&self, path: &[String]) -> (Option<Vec<String>>, Option<String>) {
        let mut curr = &self.root;
        let mut sandbox_path: Option<Vec<String>> = None;
        let mut sandbox_id: Option<String> = None;

        for (idx, p) in path.iter().enumerate() {
            let Some(next) = curr.children.get(p) else {
                // The path has diverged from the tcg, so return what you have seen so far
                break;
            };
            curr = next;

            if let Some(id) = &curr.sandbox_id {
                sandbox_id = Some(id.clone());
                sandbox_path = Some(path[..=idx].to_vec());
            }
        }

        (sandbox_path, sandbox_id)
    }

    pub fn record_lpm_hit(&mut self, sandbox_path: &[String]) {
        if let Some(sandbox_node) = self.root.descendant_mut(sandbox_path) {
            sandbox_node.number_of_lpm_hits += 1;
        }
    }

    // Atomically resolves every entry under prefix: a cached result, someone
    // else's pending execution, or a new claim for this connection to execute.
    pub fn claim_entries(
        &mut self,
        prefix: &[String],
        entries: &[String],
        connection_id: u64,
    ) -> Vec<ClaimOutcome> {
        let Some(parent) = self.root.descendant_mut(prefix) else {
            return entries.iter().map(|_| ClaimOutcome::missing()).collect();
        };

        let mut outcomes = Vec::with_capacity(entries.len());

        for entry in entries {
            let (node, is_new_node) = match parent.children.entry(entry.clone()) {
                Entry::Occupied(occupied) => (occupied.into_mut(), false),
                Entry::Vacant(vacant) => {
                    self.number_of_nodes += 1;
                    self.next_node_id += 1;

                    let node = vacant.insert(Node::new_child(self.next_node_id));
                    node.update_size_in_bytes(entry.len() as u32);
                    self.size_in_bytes += node.size_in_bytes as u64;

                    (node, true)
                }
            };

            if node.is_ready() || node.status == NodeStatus::Pending {
                // Both are served by the cache, so both count once as a hit.
                node.number_of_cache_hits += 1;
                outcomes.push(ClaimOutcome::observe(node));
                continue;
            }

            if !is_new_node {
                node.generation += 1;
            }

            node.status = NodeStatus::Pending;
            node.owner_connection_id = Some(connection_id);

            outcomes.push(ClaimOutcome {
                status: ClaimStatus::Owner,
                node_id: node.node_id,
                generation: node.generation,
                value: None,
            });
        }

        outcomes
    }

    // Read-only: never creates nodes or changes counters, so polling is free.
    pub fn inspect_entries(&self, prefix: &[String], entries: &[String]) -> Vec<ClaimOutcome> {
        let parent = self.root.descendant(prefix);

        entries
            .iter()
            .map(|entry| match parent.and_then(|parent| parent.children.get(entry)) {
                Some(node) => ClaimOutcome::observe(node),
                None => ClaimOutcome::missing(),
            })
            .collect()
    }

    // Completes a claim: a value makes the node ready, no value marks it failed.
    pub fn publish(
        &mut self,
        path: &[String],
        ownership: Ownership,
        value: Option<String>,
        sandbox_id: Option<String>,
        runtime: u32,
        ttl: Option<u32>,
    ) -> (bool, bool) {
        let Some(value) = value else {
            return (self.fail_claim(path, ownership), false);
        };

        let Some(node) = self.owned_node_mut(path, ownership) else {
            return (false, false);
        };

        node.status = NodeStatus::Ready;
        node.owner_connection_id = None;

        let result = self.insert(path.to_vec(), Some(value), sandbox_id, runtime, ttl, ttl.is_some());
        self.completions.notify_waiters();

        result
    }

    pub fn fail_claim(&mut self, path: &[String], ownership: Ownership) -> bool {
        let Some(node) = self.owned_node_mut(path, ownership) else {
            return false;
        };

        node.status = NodeStatus::Failed;
        node.owner_connection_id = None;
        self.completions.notify_waiters();

        true
    }

    fn owned_node_mut(&mut self, path: &[String], ownership: Ownership) -> Option<&mut Node> {
        self.root.descendant_mut(path).filter(|node| {
            node.status == NodeStatus::Pending
                && node.generation == ownership.generation
                && node.owner_connection_id == Some(ownership.connection_id)
        })
    }

    pub fn collect_sandbox_candidates(&self, task_id: &str) -> Vec<SandboxCandidate> {
        let mut candidates = Vec::new();
        let mut current_path = Vec::new();

        collect_sandbox_candidates_recursive(
            task_id,
            &self.root,
            &mut current_path,
            &mut candidates,
        );

        candidates
    }

    pub fn increment_sandbox_reference(&mut self, sandbox_id: &str) -> u32 {
        let reference_count = self
            .sandbox_references
            .entry(sandbox_id.to_string())
            .or_insert(0);
        *reference_count += 1;
        *reference_count
    }

    pub fn decrement_sandbox_reference(&mut self, sandbox_id: &str) -> u32 {
        let Some(reference_count) = self.sandbox_references.get_mut(sandbox_id) else {
            return 0;
        };

        *reference_count -= 1;

        if *reference_count == 0 {
            self.sandbox_references.remove(sandbox_id);
            return 0;
        }

        *reference_count
    }

    pub fn add_reclaimable_sandboxes(&mut self, sandbox_ids: &[String]) {
        for sandbox_id in sandbox_ids {
            self.reclaimable_sandboxes.insert(sandbox_id.clone());
        }
    }

    pub fn mark_sandbox_evicted(&mut self, sandbox_path: &[String], sandbox_id: &str) {
        // Always record the victim, even if the node is gone or mismatched,
        // so the id flows back to the deletion loop through drain instead of leaking.
        self.reclaimable_sandboxes.insert(sandbox_id.to_string());

        let mut curr: &mut Node = &mut self.root;

        for p in sandbox_path {
            if let Some(next) = curr.children.get_mut(p) {
                curr = next;
            } else {
                // Node already removed (this can happen by TTL eviction or TCG size based eviction that runs in the background).
                return;
            }
        }

        // as_deref turns Option<String> into Option<&str> for an allocation-free compare.
        if sandbox_path.is_empty() || curr.sandbox_id.as_deref() != Some(sandbox_id) {
            return;
        }

        curr.sandbox_id = None;
        self.number_of_sandboxes -= 1;

        let path_entry_size_in_bytes = sandbox_path
            .last()
            .map_or(0, |path_entry| path_entry.len() as u32);

        let old_size_in_bytes = curr.size_in_bytes;

        curr.update_size_in_bytes(path_entry_size_in_bytes);

        self.size_in_bytes =
            self.size_in_bytes - old_size_in_bytes as u64 + curr.size_in_bytes as u64;
    }

    pub fn drain_reclaimable_sandboxes(&mut self) -> Vec<String> {
        // Victims still referenced by a rollout stay in the set for a later drain.
        let sandbox_references = &self.sandbox_references;
        let mut ready = Vec::new();

        self.reclaimable_sandboxes.retain(|sandbox_id| {
            if sandbox_references.contains_key(sandbox_id) {
                true
            } else {
                ready.push(sandbox_id.clone());
                false
            }
        });

        ready
    }

    pub fn record_fork_cost(&mut self, sample_ms: u32) -> u32 {
        // Integer EMA with alpha = 0.3; the first sample seeds it. u64
        // intermediate so 7 * ema cannot overflow u32.
        // EMA means exponential moving average.
        if self.fork_cost_ema_ms == 0 {
            self.fork_cost_ema_ms = sample_ms;
        } else {
            self.fork_cost_ema_ms =
                ((7 * self.fork_cost_ema_ms as u64 + 3 * sample_ms as u64) / 10) as u32;
        }

        self.fork_cost_ema_ms
    }
}

fn collect_sandbox_candidates_recursive(
    task_id: &str,
    node: &Node,
    current_path: &mut Vec<String>,
    candidates: &mut Vec<SandboxCandidate>,
) {
    if let Some(sandbox_id) = &node.sandbox_id {
        candidates.push(SandboxCandidate {
            task_id: task_id.to_string(),
            sandbox_path: current_path.clone(),
            sandbox_id: sandbox_id.clone(),
            score: node.number_of_lpm_hits,
        });
    }

    for (path_entry, child) in &node.children {
        current_path.push(path_entry.clone());
        collect_sandbox_candidates_recursive(task_id, child, current_path, candidates);
        current_path.pop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn path(entries: &[&str]) -> Vec<String> {
        entries.iter().map(|entry| entry.to_string()).collect()
    }

    fn insert_value(tcg: &mut ToolCallGraph, entries: &[&str], value: &str) -> (bool, bool) {
        tcg.insert(path(entries), Some(value.to_string()), None, 1, None, false)
    }

    fn node<'a>(tcg: &'a ToolCallGraph, entries: &[&str]) -> &'a Node {
        let mut curr = &tcg.root;
        for entry in entries {
            curr = &curr.children[*entry];
        }
        curr
    }

    fn recomputed_size(tcg: &ToolCallGraph) -> u64 {
        tcg.root
            .walk_subtcg(0, &mut |acc, node| acc + node.size_in_bytes as u64)
    }

    #[test]
    fn insert_requires_existing_parent() {
        let mut tcg = ToolCallGraph::new();

        assert_eq!(insert_value(&mut tcg, &["a", "b"], "b"), (false, false));
        assert_eq!(insert_value(&mut tcg, &["a"], "a"), (true, false));
        assert_eq!(insert_value(&mut tcg, &["a", "b"], "b"), (true, false));
        assert_eq!(tcg.number_of_nodes, 2);
    }

    #[test]
    fn insert_merges_without_destroying_state() {
        let mut tcg = ToolCallGraph::new();
        tcg.insert(path(&["a"]), Some("a".into()), None, 5, Some(60), true);

        // A sandbox-only insert keeps the cached value and TTL.
        let (value_inserted, sandbox_inserted) =
            tcg.insert(path(&["a"]), None, Some("sb-1".into()), 0, None, false);
        assert!(value_inserted && sandbox_inserted);
        let a = node(&tcg, &["a"]);
        assert_eq!(a.value.as_deref(), Some("a"));
        assert_eq!(a.runtime, 5);
        assert!(a.ttl_present);
        assert_eq!(a.ttl, 60);

        // A second sandbox is never attached over the first.
        let (_, sandbox_inserted) =
            tcg.insert(path(&["a"]), None, Some("sb-2".into()), 0, None, false);
        assert!(!sandbox_inserted);
        assert_eq!(node(&tcg, &["a"]).sandbox_id.as_deref(), Some("sb-1"));
        assert_eq!(tcg.number_of_sandboxes, 1);
    }

    #[test]
    fn find_exact_hit_counts_cache_hit() {
        let mut tcg = ToolCallGraph::new();
        insert_value(&mut tcg, &["a"], "a");
        insert_value(&mut tcg, &["a", "b"], "b");

        let (value, (sandbox_path, sandbox_id)) = tcg.find(path(&["a", "b"]));
        assert_eq!(value.as_deref(), Some("b"));
        assert_eq!((sandbox_path, sandbox_id), (None, None));
        assert_eq!(node(&tcg, &["a", "b"]).number_of_cache_hits, 1);
        assert_eq!(node(&tcg, &["a"]).number_of_cache_hits, 0);
    }

    #[test]
    fn find_miss_returns_deepest_sandbox_and_counts_lpm_hit() {
        let mut tcg = ToolCallGraph::new();
        tcg.insert(path(&["a"]), Some("a".into()), Some("sb-a".into()), 1, None, false);
        tcg.insert(path(&["a", "b"]), Some("b".into()), Some("sb-b".into()), 1, None, false);

        let (value, (sandbox_path, sandbox_id)) = tcg.find(path(&["a", "b", "missing"]));
        assert_eq!(value, None);
        assert_eq!(sandbox_path, Some(path(&["a", "b"])));
        assert_eq!(sandbox_id.as_deref(), Some("sb-b"));
        assert_eq!(node(&tcg, &["a", "b"]).number_of_lpm_hits, 1);
        assert_eq!(node(&tcg, &["a"]).number_of_lpm_hits, 0);

        // A hit still reports the deepest sandbox but is not an lpm hit.
        let (value, (_, sandbox_id)) = tcg.find(path(&["a", "b"]));
        assert_eq!(value.as_deref(), Some("b"));
        assert_eq!(sandbox_id.as_deref(), Some("sb-b"));
        assert_eq!(node(&tcg, &["a", "b"]).number_of_lpm_hits, 1);
    }

    #[test]
    fn counters_and_size_stay_balanced() {
        let mut tcg = ToolCallGraph::new();
        tcg.insert(path(&["a"]), Some("a".into()), Some("sb-a".into()), 1, None, false);
        insert_value(&mut tcg, &["a", "b"], "b");
        insert_value(&mut tcg, &["a", "b"], "a longer replacement value");

        assert_eq!(tcg.number_of_nodes, 2);
        assert_eq!(tcg.number_of_sandboxes, 1);
        assert_eq!(tcg.size_in_bytes, recomputed_size(&tcg));

        tcg.mark_sandbox_evicted(&path(&["a"]), "sb-a");
        assert_eq!(tcg.number_of_sandboxes, 0);
        assert_eq!(tcg.size_in_bytes, recomputed_size(&tcg));
        assert_eq!(tcg.drain_reclaimable_sandboxes(), vec!["sb-a".to_string()]);
    }

    const OWNER: u64 = 1;
    const OTHER: u64 = 2;

    fn claim_one(tcg: &mut ToolCallGraph, entries: &[&str], connection_id: u64) -> ClaimOutcome {
        let (prefix, entry) = entries.split_at(entries.len() - 1);
        tcg.claim_entries(&path(prefix), &path(entry), connection_id).remove(0)
    }

    fn publish_value(
        tcg: &mut ToolCallGraph,
        entries: &[&str],
        connection_id: u64,
        generation: u32,
        value: &str,
    ) -> (bool, bool) {
        let ownership = Ownership {
            connection_id,
            generation,
        };
        tcg.publish(&path(entries), ownership, Some(value.to_string()), None, 1, None)
    }

    #[test]
    fn claim_absent_node_makes_caller_owner() {
        let mut tcg = ToolCallGraph::new();

        let outcome = claim_one(&mut tcg, &["a"], OWNER);

        assert_eq!(outcome.status, ClaimStatus::Owner);
        assert_eq!(outcome.generation, 1);
        assert_eq!(outcome.node_id, 1);
        assert_eq!(tcg.number_of_nodes, 1);
        assert_eq!(node(&tcg, &["a"]).status, NodeStatus::Pending);
        assert_eq!(tcg.size_in_bytes, recomputed_size(&tcg));
    }

    #[test]
    fn second_claim_waits_on_same_node() {
        let mut tcg = ToolCallGraph::new();
        let owner = claim_one(&mut tcg, &["a"], OWNER);

        let waiter = claim_one(&mut tcg, &["a"], OTHER);

        assert_eq!(waiter.status, ClaimStatus::Pending);
        assert_eq!((waiter.node_id, waiter.generation), (owner.node_id, owner.generation));
        assert_eq!(waiter.value, None);
        assert_eq!(node(&tcg, &["a"]).number_of_cache_hits, 1);
        assert_eq!(tcg.number_of_nodes, 1);
    }

    #[test]
    fn only_current_owner_can_publish() {
        let mut tcg = ToolCallGraph::new();
        claim_one(&mut tcg, &["a"], OWNER);

        assert_eq!(publish_value(&mut tcg, &["a"], OTHER, 1, "x"), (false, false));
        assert_eq!(publish_value(&mut tcg, &["a"], OWNER, 2, "x"), (false, false));
        assert_eq!(node(&tcg, &["a"]).status, NodeStatus::Pending);
        assert_eq!(node(&tcg, &["a"]).value, None);

        assert_eq!(publish_value(&mut tcg, &["a"], OWNER, 1, "x"), (true, false));
        assert!(node(&tcg, &["a"]).is_ready());
        assert_eq!(publish_value(&mut tcg, &["a"], OWNER, 1, "y"), (false, false));
        assert_eq!(node(&tcg, &["a"]).value.as_deref(), Some("x"));
    }

    #[test]
    fn claim_after_publish_is_a_hit() {
        let mut tcg = ToolCallGraph::new();
        let owner = claim_one(&mut tcg, &["a"], OWNER);
        publish_value(&mut tcg, &["a"], OWNER, owner.generation, "x");

        let hit = claim_one(&mut tcg, &["a"], OTHER);

        assert_eq!(hit.status, ClaimStatus::Ready);
        assert_eq!(hit.value.as_deref(), Some("x"));
        assert_eq!((hit.node_id, hit.generation), (owner.node_id, owner.generation));
        assert_eq!(tcg.number_of_nodes, 1);
        assert_eq!(tcg.size_in_bytes, recomputed_size(&tcg));
    }

    #[test]
    fn failed_node_is_reclaimed_with_new_generation() {
        let mut tcg = ToolCallGraph::new();
        let first = claim_one(&mut tcg, &["a"], OWNER);
        let failed = Ownership {
            connection_id: OWNER,
            generation: first.generation,
        };
        assert!(tcg.fail_claim(&path(&["a"]), failed));
        assert_eq!(tcg.inspect_entries(&[], &path(&["a"]))[0].status, ClaimStatus::Failed);

        let second = claim_one(&mut tcg, &["a"], OTHER);

        assert_eq!(second.status, ClaimStatus::Owner);
        assert_eq!(second.generation, 2);
        assert_eq!(second.node_id, first.node_id);
        // The old owner can neither fail nor complete the new claim.
        assert!(!tcg.fail_claim(&path(&["a"]), failed));
        assert_eq!(publish_value(&mut tcg, &["a"], OWNER, 1, "stale"), (false, false));
        assert_eq!(publish_value(&mut tcg, &["a"], OTHER, 2, "fresh"), (true, false));
        assert_eq!(node(&tcg, &["a"]).value.as_deref(), Some("fresh"));
    }

    #[test]
    fn publish_without_value_fails_the_claim() {
        let mut tcg = ToolCallGraph::new();
        let owner = claim_one(&mut tcg, &["a"], OWNER);
        let ownership = Ownership {
            connection_id: OWNER,
            generation: owner.generation,
        };

        assert_eq!(tcg.publish(&path(&["a"]), ownership, None, None, 1, None), (true, false));
        assert_eq!(node(&tcg, &["a"]).status, NodeStatus::Failed);
    }

    #[test]
    fn missing_prefix_claims_nothing() {
        let mut tcg = ToolCallGraph::new();

        let outcomes = tcg.claim_entries(&path(&["absent"]), &path(&["a", "b"]), OWNER);

        assert!(outcomes.iter().all(|outcome| outcome.status == ClaimStatus::Missing));
        assert_eq!(tcg.number_of_nodes, 0);
    }

    #[test]
    fn batch_claim_resolves_each_entry() {
        let mut tcg = ToolCallGraph::new();
        insert_value(&mut tcg, &["p"], "p");
        insert_value(&mut tcg, &["p", "ready"], "r");
        claim_one(&mut tcg, &["p", "pending"], OTHER);

        let outcomes = tcg.claim_entries(&path(&["p"]), &path(&["ready", "pending", "absent"]), OWNER);

        let statuses: Vec<ClaimStatus> = outcomes.iter().map(|outcome| outcome.status).collect();
        assert_eq!(statuses, vec![ClaimStatus::Ready, ClaimStatus::Pending, ClaimStatus::Owner]);
        assert_eq!(outcomes[0].value.as_deref(), Some("r"));
    }

    #[test]
    fn legacy_insert_respects_pending_and_revives_failed() {
        let mut tcg = ToolCallGraph::new();
        let owner = claim_one(&mut tcg, &["a"], OWNER);

        assert_eq!(insert_value(&mut tcg, &["a"], "racing"), (false, false));
        assert_eq!(node(&tcg, &["a"]).value, None);

        tcg.fail_claim(
            &path(&["a"]),
            Ownership {
                connection_id: OWNER,
                generation: owner.generation,
            },
        );
        assert_eq!(insert_value(&mut tcg, &["a"], "fallback"), (true, false));
        assert!(node(&tcg, &["a"]).is_ready());
    }

    #[test]
    fn every_created_node_starts_at_generation_one() {
        let mut tcg = ToolCallGraph::new();
        insert_value(&mut tcg, &["a"], "a");
        claim_one(&mut tcg, &["b"], OWNER);

        assert_eq!(node(&tcg, &["a"]).generation, 1);
        assert_eq!(node(&tcg, &["b"]).generation, 1);
        assert_eq!(tcg.root.generation, 0);
        assert_eq!(tcg.root.node_id, 0);
    }

    #[test]
    fn empty_string_is_a_ready_result() {
        let mut tcg = ToolCallGraph::new();
        let owner = claim_one(&mut tcg, &["a"], OWNER);
        publish_value(&mut tcg, &["a"], OWNER, owner.generation, "");

        let hit = claim_one(&mut tcg, &["a"], OTHER);

        assert_eq!(hit.status, ClaimStatus::Ready);
        assert_eq!(hit.value.as_deref(), Some(""));
    }

    #[test]
    fn valueless_node_is_claimable() {
        let mut tcg = ToolCallGraph::new();
        tcg.insert(path(&["a"]), None, Some("sb".into()), 1, None, false);
        assert_eq!(tcg.inspect_entries(&[], &path(&["a"]))[0].status, ClaimStatus::Missing);

        let outcome = claim_one(&mut tcg, &["a"], OWNER);

        assert_eq!(outcome.status, ClaimStatus::Owner);
        assert_eq!(outcome.generation, 2);
    }

    #[test]
    fn node_ids_are_unique_and_never_reused() {
        let mut tcg = ToolCallGraph::new();
        insert_value(&mut tcg, &["a"], "a");
        claim_one(&mut tcg, &["b"], OWNER);
        let removed_id = node(&tcg, &["b"]).node_id;
        tcg.root.children.remove("b");

        claim_one(&mut tcg, &["b"], OWNER);

        assert_eq!(node(&tcg, &["a"]).node_id, 1);
        assert_eq!(removed_id, 2);
        assert_eq!(node(&tcg, &["b"]).node_id, 3);
    }

    #[test]
    fn inspect_is_read_only() {
        let mut tcg = ToolCallGraph::new();
        claim_one(&mut tcg, &["a"], OWNER);

        let outcomes = tcg.inspect_entries(&[], &path(&["a", "absent"]));

        assert_eq!(outcomes[0].status, ClaimStatus::Pending);
        assert_eq!(outcomes[1].status, ClaimStatus::Missing);
        assert_eq!(node(&tcg, &["a"]).number_of_cache_hits, 0);
        assert_eq!(tcg.number_of_nodes, 1);
    }

    #[test]
    fn dump_exposes_lifecycle_but_not_owner() {
        let mut tcg = ToolCallGraph::new();
        claim_one(&mut tcg, &["a"], OWNER);

        let json = serde_json::to_value(&tcg).unwrap();
        let a = &json["root"]["children"]["a"];

        assert_eq!(a["node_id"], 1);
        assert_eq!(a["status"], "pending");
        assert_eq!(a["generation"], 1);
        assert!(a.get("owner_connection_id").is_none());
        assert!(json.get("completions").is_none());
    }
}
