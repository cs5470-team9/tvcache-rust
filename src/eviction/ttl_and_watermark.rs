use crate::{
    eviction::{EvictionPolicyConfig, EvictionResult},
    tcg::Node,
    tvcache::TVCache,
    types::{CacheSharedObject, CacheSnapshotObject},
    utils,
};

#[derive(Debug)]
struct EvictionCandidate {
    task_id: String,
    parent_path: Vec<String>,
    child_key: String,
    total_hits: u64,
}

fn get_stats_of_removed_node(node: &Node) -> EvictionResult {
    let mut result = EvictionResult {
        nodes_removed: 1,
        sandboxes_removed: if node.sandbox_id.is_some() { 1 } else { 0 },
        size_removed_in_bytes: node.size_in_bytes as u64,
        sandbox_ids_removed: Vec::new(),
    };

    if let Some(sandbox_id) = &node.sandbox_id {
        result.sandbox_ids_removed.push(sandbox_id.clone());
    }

    // Node will not be none because it is not optional, if there is a key, there is a node

    for (_, child) in node.children.iter() {
        let child_count = get_stats_of_removed_node(child);
        result.update(child_count);
    }

    return result;
}

fn has_node_expired(node: &Node) -> bool {
    let now_seconds = utils::current_utc_seconds();
    return node.ttl_present && (node.inserted_at_seconds + (node.ttl as u64) < now_seconds);
}

fn prune_expired_recursive(node: &mut Node, eviction_result: &mut EvictionResult) {
    node.children.retain(|_, child| {
        // A pending subtree has an owner executing into it, so it is never evicted.
        if has_node_expired(child) && !child.subtree_has_pending() {
            let eviction_result_child = get_stats_of_removed_node(child);

            eviction_result.update(eviction_result_child);

            false
        } else {
            prune_expired_recursive(child, eviction_result);
            true
        }
    });
}

fn get_high_watermark_size_in_bytes(config: &EvictionPolicyConfig) -> u64 {
    config.high_watermark_mb as u64 * 1024 * 1024
}

async fn get_total_cache_size_in_bytes(entries: &CacheSnapshotObject) -> u64 {
    let mut total_size_in_bytes = 0;

    for (_task_id, tcg) in entries {
        let tcg = tcg.lock().await;
        total_size_in_bytes += tcg.size_in_bytes;
    }

    total_size_in_bytes
}

fn populate_tcg_eviction_candidates(
    task_id: &str,
    node: &Node,
    parent_path: &mut Vec<String>,
    candidates: &mut Vec<EvictionCandidate>,
) {
    let mut children = Vec::<(&String, &Node)>::new();

    for (node_key, child) in node.children.iter() {
        children.push((node_key, child));
    }

    for (node_key, child) in children {
        // Pending nodes have no hits yet, so without this they would be evicted first.
        if !child.subtree_has_pending() {
            let total_hits = child.walk_subtcg(0, &mut |acc, node| acc + node.number_of_cache_hits);

            candidates.push(EvictionCandidate {
                task_id: task_id.to_string(),
                parent_path: parent_path.clone(),
                child_key: node_key.clone(),
                total_hits,
            });
        }

        parent_path.push(node_key.clone());
        populate_tcg_eviction_candidates(task_id, child, parent_path, candidates);
        parent_path.pop();
    }
}

fn find_parent_node_mut<'a>(root: &'a mut Node, parent_path: &[String]) -> Option<&'a mut Node> {
    let mut curr = root;

    for path_entry in parent_path {
        curr = curr.children.get_mut(path_entry)?;
    }

    Some(curr)
}

async fn prune_cache_to_fit(
    cache: &mut TVCache,
    required_size: u64,
    eviction_result: &mut EvictionResult,
) {
    // go over every node in every tcg
    // get the total hits in that subtcg starting at a node
    // make a list with all the visited nodes and their parent
    let mut candidates = Vec::<EvictionCandidate>::new();

    for (task_id, tcg) in cache.tcgs.iter() {
        let tcg = tcg.lock().await;
        populate_tcg_eviction_candidates(task_id, &tcg.root, &mut Vec::new(), &mut candidates);
    }

    // sort it in ascending order of total hits
    candidates.sort_by_key(|candidate| candidate.total_hits);

    prune_candidates(cache, candidates, required_size, eviction_result).await;
}

// Removes candidates in order until required_size bytes are evicted.
async fn prune_candidates(
    cache: &mut TVCache,
    candidates: Vec<EvictionCandidate>,
    required_size: u64,
    eviction_result: &mut EvictionResult,
) {
    // evict nodes until required_size amount of bytes are evicted
    let mut size_evicted_in_bytes = 0;

    for candidate in candidates {
        if size_evicted_in_bytes >= required_size {
            break;
        }

        let Some(tcg) = cache.tcgs.get(&candidate.task_id) else {
            continue;
        };

        let mut tcg = tcg.lock().await;
        let Some(parent_node) = find_parent_node_mut(&mut tcg.root, &candidate.parent_path) else {
            continue;
        };

        // Candidates were collected before this TCG was re-locked, so a claim may
        // have made the subtree pending since.
        if parent_node
            .children
            .get(&candidate.child_key)
            .is_none_or(|child| child.subtree_has_pending())
        {
            continue;
        }

        let Some(removed_node) = parent_node.children.remove(&candidate.child_key) else {
            continue;
        };

        let candidate_result = get_stats_of_removed_node(&removed_node);
        size_evicted_in_bytes += candidate_result.size_removed_in_bytes;

        tcg.number_of_nodes -= candidate_result.nodes_removed;
        tcg.number_of_sandboxes -= candidate_result.sandboxes_removed;
        tcg.size_in_bytes -= candidate_result.size_removed_in_bytes;

        // The trie node is gone, but the actual sandbox is only deleted once the
        // deletion loop drains it with a zero reference count.
        tcg.add_reclaimable_sandboxes(&candidate_result.sandbox_ids_removed);

        eviction_result.update(candidate_result);
    }

    // while evicting, update the parent tcg's total bytes value
}

pub async fn enforce(cache: CacheSharedObject, config: &EvictionPolicyConfig) -> EvictionResult {
    let mut result = EvictionResult::default();
    let entries = {
        let cache = cache.lock().await;
        cache.entries_snapshot()
    };

    for (_task_id, tcg) in &entries {
        let mut tcg = tcg.lock().await;
        let mut tcg_result = EvictionResult::default();

        prune_expired_recursive(&mut tcg.root, &mut tcg_result);

        tcg.number_of_nodes -= tcg_result.nodes_removed;
        tcg.number_of_sandboxes -= tcg_result.sandboxes_removed;
        tcg.size_in_bytes -= tcg_result.size_removed_in_bytes;

        // The trie nodes are gone, but the actual sandboxes are only deleted once
        // the deletion loop drains them with a zero reference count.
        tcg.add_reclaimable_sandboxes(&tcg_result.sandbox_ids_removed);

        result.update(tcg_result);
    }

    let high_watermark_size_in_bytes = get_high_watermark_size_in_bytes(config);
    let total_cache_size_in_bytes = get_total_cache_size_in_bytes(&entries).await;

    if total_cache_size_in_bytes > high_watermark_size_in_bytes {
        let required_size = total_cache_size_in_bytes - high_watermark_size_in_bytes;
        let mut cache = cache.lock().await;
        prune_cache_to_fit(&mut cache, required_size, &mut result).await;
    }

    let _low_watermark_mb = config.low_watermark_mb;
    let _total_tcgs = entries.len();

    result
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tokio::sync::Mutex;

    use super::*;
    use crate::eviction::EvictionPolicyName;

    fn path(entries: &[&str]) -> Vec<String> {
        entries.iter().map(|entry| entry.to_string()).collect()
    }

    fn config(high_watermark_mb: u32) -> EvictionPolicyConfig {
        EvictionPolicyConfig::new(EvictionPolicyName::TtlAndWatermark, high_watermark_mb, 0)
    }

    #[tokio::test]
    async fn ttl_expired_subtree_is_removed_and_sandboxes_reclaimable() {
        let cache: CacheSharedObject = Arc::new(Mutex::new(TVCache::new()));
        let tcg = cache.lock().await.get_tcg("task".into());
        {
            let mut tcg = tcg.lock().await;
            tcg.insert(path(&["a"]), Some("a".into()), Some("sb-a".into()), 1, None, false);
            tcg.insert(path(&["a", "b"]), Some("b".into()), Some("sb-b".into()), 1, Some(0), true);
            // inserted_at + ttl must be strictly in the past.
            tcg.root.children.get_mut("a").unwrap().children.get_mut("b").unwrap().inserted_at_seconds -= 10;
        }

        let result = enforce(Arc::clone(&cache), &config(1024)).await;

        assert_eq!(result.nodes_removed, 1);
        let mut tcg = tcg.lock().await;
        assert!(tcg.root.children["a"].children.is_empty());
        assert_eq!(tcg.number_of_nodes, 1);
        assert_eq!(tcg.drain_reclaimable_sandboxes(), vec!["sb-b".to_string()]);
    }

    #[tokio::test]
    async fn ttl_never_removes_a_pending_subtree() {
        let cache: CacheSharedObject = Arc::new(Mutex::new(TVCache::new()));
        let tcg = cache.lock().await.get_tcg("task".into());
        {
            let mut tcg = tcg.lock().await;
            tcg.insert(path(&["a"]), Some("a".into()), None, 1, Some(0), true);
            tcg.root.children.get_mut("a").unwrap().inserted_at_seconds -= 10;
            tcg.claim_entries(&path(&["a"]), &path(&["running"]), 1);
        }

        enforce(Arc::clone(&cache), &config(1024)).await;

        assert!(tcg.lock().await.root.children.contains_key("a"));
    }

    #[tokio::test]
    async fn watermark_skips_pending_subtrees() {
        let cache: CacheSharedObject = Arc::new(Mutex::new(TVCache::new()));
        let tcg = cache.lock().await.get_tcg("task".into());
        {
            let mut tcg = tcg.lock().await;
            let big = "x".repeat(700 * 1024);
            tcg.insert(path(&["hot"]), Some(big.clone()), None, 1, None, false);
            tcg.insert(path(&["cold"]), Some(big), None, 1, None, false);
            tcg.find(path(&["hot"]));
            tcg.claim_entries(&path(&["cold"]), &path(&["running"]), 1);
        }

        enforce(Arc::clone(&cache), &config(1)).await;

        // The zero-hit "cold" subtree holds a pending node, so "hot" goes instead.
        let tcg = tcg.lock().await;
        assert!(tcg.root.children.contains_key("cold"));
        assert!(!tcg.root.children.contains_key("hot"));
    }

    #[tokio::test]
    async fn watermark_rechecks_pending_at_removal() {
        let cache: CacheSharedObject = Arc::new(Mutex::new(TVCache::new()));
        let tcg = cache.lock().await.get_tcg("task".into());
        let mut candidates = Vec::new();
        {
            let mut tcg = tcg.lock().await;
            tcg.insert(path(&["cold"]), Some("x".repeat(1024)), None, 1, None, false);
            populate_tcg_eviction_candidates("task", &tcg.root, &mut Vec::new(), &mut candidates);
            // A claim lands after candidates were collected.
            tcg.claim_entries(&path(&["cold"]), &path(&["running"]), 1);
        }
        assert_eq!(candidates.len(), 1);

        let mut result = EvictionResult::default();
        prune_candidates(&mut *cache.lock().await, candidates, u64::MAX, &mut result).await;

        assert_eq!(result.nodes_removed, 0);
        assert!(tcg.lock().await.root.children.contains_key("cold"));
    }

    #[tokio::test]
    async fn watermark_evicts_lowest_hit_subtree_first() {
        let cache: CacheSharedObject = Arc::new(Mutex::new(TVCache::new()));
        let tcg = cache.lock().await.get_tcg("task".into());
        {
            let mut tcg = tcg.lock().await;
            let big = "x".repeat(700 * 1024);
            tcg.insert(path(&["hot"]), Some(big.clone()), None, 1, None, false);
            tcg.insert(path(&["cold"]), Some(big), None, 1, None, false);
            tcg.find(path(&["hot"]));
        }

        // ~1.4 MB stored against a 1 MB high watermark: one subtree must go.
        enforce(Arc::clone(&cache), &config(1)).await;

        let tcg = tcg.lock().await;
        assert!(tcg.root.children.contains_key("hot"));
        assert!(!tcg.root.children.contains_key("cold"));
    }
}
