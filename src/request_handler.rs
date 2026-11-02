use std::{collections::HashSet, io, sync::Arc, time::Duration};

use tokio::{net::TcpStream, time::Instant};
use tracing::error;

use crate::{
    protocol_primitives,
    tcg::tool_call_graph::{ClaimOutcome, ClaimStatus, Ownership},
    tvcache::tcg_entries_to_json,
    types::{
        CacheSharedObject, ReclaimableSandbox, Request, Response, SandboxCandidate,
        SandboxEvictionRecord, TcgSharedObject,
    },
};

// Upper bound on one Inspect long poll, so a waiter's connection is never held indefinitely.
const MAX_INSPECT_WAIT_MS: u32 = 30_000;

pub struct RequestHandler {
    cache: CacheSharedObject,
    connection_id: u64,
    // Claims this connection still owns: (task_id, path, ownership). Whatever is
    // left when the connection closes is failed so its waiters are not stranded.
    owned_claims: Vec<(String, Vec<String>, Ownership)>,
}

impl RequestHandler {
    pub fn new(cache: CacheSharedObject, connection_id: u64) -> Self {
        return Self {
            cache: cache,
            connection_id,
            owned_claims: Vec::new(),
        };
    }

    pub async fn handle_stream(&mut self, mut stream: TcpStream) {
        loop {
            match self.parse_request(&mut stream).await {
                Ok(request) => {
                    let response = self.execute_request(request).await;
                    if let Err(err) = self.write_response(&mut stream, response).await {
                        error!(error = %err, "error while writing response");
                    }
                }
                Err(_err) => {
                    break;
                }
            }
        }

        // The connection is gone, so its owner can never publish.
        self.fail_owned_claims().await;
    }

    async fn fail_owned_claims(&mut self) {
        for (task_id, path, ownership) in std::mem::take(&mut self.owned_claims) {
            let tcg = {
                let cache = self.cache.lock().await;
                cache.peek_tcg(&task_id)
            };

            if let Some(tcg) = tcg {
                tcg.lock().await.fail_claim(&path, ownership);
            }
        }
    }

    async fn parse_request(&self, stream: &mut TcpStream) -> Result<Request, io::Error> {
        let request_type = protocol_primitives::read_u8(stream).await?;

        match request_type {
            1 => {
                let task_id = protocol_primitives::read_string_u16(stream).await?;
                let path = protocol_primitives::read_path(stream).await?;
                let (runtime, value, sandbox_id, ttl) = self.read_insert_fields(stream).await?;

                Ok(Request::Insert {
                    task_id,
                    path,
                    value,
                    sandbox_id,
                    runtime,
                    ttl,
                    ttl_present: ttl.is_some(),
                })
            }

            2 => {
                let task_id = protocol_primitives::read_string_u16(stream).await?;
                let path = protocol_primitives::read_path(stream).await?;

                Ok(Request::Find { task_id, path })
            }

            3 => Ok(Request::DumpAll),

            4 => {
                let task_id = protocol_primitives::read_string_u16(stream).await?;
                let sandbox_id = protocol_primitives::read_string_u16(stream).await?;

                Ok(Request::UnrefSandbox {
                    task_id,
                    sandbox_id,
                })
            }

            5 => {
                let sandbox_eviction_records = self.read_sandbox_eviction_records(stream).await?;

                Ok(Request::MarkSandboxesEvicted {
                    sandbox_eviction_records,
                })
            }

            6 => {
                let limit = protocol_primitives::read_u16(stream).await?;

                Ok(Request::ListSandboxCandidates { limit })
            }

            7 => Ok(Request::DrainReclaimableSandboxes),

            8 => {
                let limit = protocol_primitives::read_u16(stream).await?;

                Ok(Request::ListHotSandboxes { limit })
            }

            9 => {
                let task_id = protocol_primitives::read_string_u16(stream).await?;
                let fork_cost_ms = protocol_primitives::read_u32(stream).await?;

                Ok(Request::ReportForkCost {
                    task_id,
                    fork_cost_ms,
                })
            }

            10 => {
                let task_id = protocol_primitives::read_string_u16(stream).await?;

                Ok(Request::GetForkCost { task_id })
            }

            11 => {
                let task_id = protocol_primitives::read_string_u16(stream).await?;
                let prefix = protocol_primitives::read_path(stream).await?;
                let entries = protocol_primitives::read_path(stream).await?;

                Ok(Request::Claim {
                    task_id,
                    prefix,
                    entries,
                })
            }

            12 => {
                let task_id = protocol_primitives::read_string_u16(stream).await?;
                let prefix = protocol_primitives::read_path(stream).await?;
                let entries = protocol_primitives::read_path(stream).await?;
                let wait_ms = protocol_primitives::read_u32(stream).await?;

                Ok(Request::Inspect {
                    task_id,
                    prefix,
                    entries,
                    wait_ms,
                })
            }

            13 => {
                // Publish is an Insert carrying the claim's generation.
                let task_id = protocol_primitives::read_string_u16(stream).await?;
                let path = protocol_primitives::read_path(stream).await?;
                let generation = protocol_primitives::read_u32(stream).await?;
                let (runtime, value, sandbox_id, ttl) = self.read_insert_fields(stream).await?;

                Ok(Request::Publish {
                    task_id,
                    path,
                    generation,
                    runtime,
                    value,
                    sandbox_id,
                    ttl,
                })
            }

            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown request type: {request_type}"),
            )),
        }
    }

    async fn read_insert_fields(
        &self,
        stream: &mut TcpStream,
    ) -> Result<(u32, Option<String>, Option<String>, Option<u32>), io::Error> {
        let runtime = protocol_primitives::read_u32(stream).await?;
        let value = protocol_primitives::read_optional_string_u32(stream, "value").await?;
        let sandbox_id = protocol_primitives::read_optional_string_u16(stream, "sandbox id").await?;
        let ttl = protocol_primitives::read_optional_u32(stream, "ttl").await?;

        Ok((runtime, value, sandbox_id, ttl))
    }

    async fn execute_request(&mut self, request: Request) -> Response {
        match request {
            Request::Insert {
                task_id,
                path,
                value,
                sandbox_id,
                runtime,
                ttl_present,
                ttl,
            } => {
                let tcg = {
                    let mut cache = self.cache.lock().await;
                    cache.get_tcg(task_id)
                };

                let (value_inserted, sandbox_inserted) = {
                    let mut tcg = tcg.lock().await;
                    tcg.insert(path, value, sandbox_id, runtime, ttl, ttl_present)
                };

                Response::InsertResult {
                    value_inserted,
                    sandbox_inserted,
                }
            }

            Request::Find { task_id, path } => {
                let tcg = {
                    let mut cache = self.cache.lock().await;
                    cache.get_tcg(task_id)
                };

                {
                    let mut tcg = tcg.lock().await;
                    let (value, (sandbox_path, sandbox_id)) = { tcg.find(path) };

                    // The returned sandbox may now be in use by the rollout, so count
                    // the reference under the same per-TCG lock (no cross-TCG lock).
                    if let Some(id) = &sandbox_id {
                        tcg.increment_sandbox_reference(id);
                    }

                    Response::FindResult {
                        value,
                        sandbox_path,
                        sandbox_id,
                    }
                }
            }

            Request::DumpAll => {
                let entries = {
                    let cache = self.cache.lock().await;
                    cache.entries_snapshot()
                };

                let json_result = tcg_entries_to_json(entries).await;

                match json_result {
                    Ok(json) => Response::Json { json },
                    Err(error) => Response::Error {
                        message: error.to_string(),
                    },
                }
            }

            Request::UnrefSandbox {
                task_id,
                sandbox_id,
            } => {
                let tcg = {
                    let cache = self.cache.lock().await;
                    cache.peek_tcg(&task_id)
                };

                let remaining_references = match tcg {
                    Some(tcg) => {
                        let mut tcg = tcg.lock().await;
                        tcg.decrement_sandbox_reference(&sandbox_id)
                    }
                    None => 0,
                };

                Response::UnrefSandboxResult {
                    remaining_references,
                }
            }

            Request::MarkSandboxesEvicted {
                sandbox_eviction_records,
            } => {
                let sandbox_ids_recorded = sandbox_eviction_records.len() as u32;

                // Make a list of tuples, (sandbox eviction record, tcg). Using this list, the next loop will lock the tcg and mark the sandbox as evicted. I am using the idiomatic rust syntax to learn
                // if we would use traditional style, we would use the code below.

                // let mut records_with_tcgs = Vec::<(TcgSharedObject, SandboxEvictionRecord)>::new();

                // let mut cache = self.cache.lock().await;

                // for record in sandbox_eviction_records {
                //     records_with_tcgs.push((cache.get_tcg(record.task_id.clone()), record));
                // }

                let records_with_tcgs = {
                    let mut cache = self.cache.lock().await;

                    sandbox_eviction_records
                        .into_iter()
                        .map(|record| (cache.get_tcg(record.task_id.clone()), record))
                        .collect::<Vec<_>>()
                };

                for (tcg, record) in records_with_tcgs {
                    let mut tcg = tcg.lock().await;
                    tcg.mark_sandbox_evicted(&record.sandbox_path, &record.sandbox_id);
                }

                Response::MarkSandboxesEvictedResult {
                    sandbox_ids_recorded,
                }
            }

            Request::ListSandboxCandidates { limit } => {
                // Ascending lpm-hit score: the least useful sandboxes first, for eviction.
                let candidates = self.collect_sorted_candidates(limit, false).await;

                Response::SandboxCandidates { candidates }
            }

            Request::ListHotSandboxes { limit } => {
                // Descending lpm-hit score: the most useful sandboxes first, for
                // proactive forking before a training iteration.
                let candidates = self.collect_sorted_candidates(limit, true).await;

                Response::SandboxCandidates { candidates }
            }

            Request::DrainReclaimableSandboxes => {
                let entries = {
                    let cache = self.cache.lock().await;
                    cache.entries_snapshot()
                };

                let mut sandboxes = Vec::new();

                for (task_id, tcg) in entries {
                    let mut tcg = tcg.lock().await;
                    let drained = tcg.drain_reclaimable_sandboxes();

                    sandboxes.extend(drained.into_iter().map(|sandbox_id| ReclaimableSandbox {
                        task_id: task_id.clone(),
                        sandbox_id,
                    }));
                }

                Response::ReclaimableSandboxes { sandboxes }
            }

            Request::ReportForkCost {
                task_id,
                fork_cost_ms,
            } => {
                // get_tcg (creating) so the first samples for a task (e.g. fork
                // bank warm-up) are kept even before any insert.
                let tcg = {
                    let mut cache = self.cache.lock().await;
                    cache.get_tcg(task_id)
                };

                let updated_ema = {
                    let mut tcg = tcg.lock().await;
                    tcg.record_fork_cost(fork_cost_ms)
                };

                Response::ForkCost {
                    fork_cost_ms: updated_ema,
                }
            }

            Request::GetForkCost { task_id } => {
                let tcg = {
                    let cache = self.cache.lock().await;
                    cache.peek_tcg(&task_id)
                };

                let fork_cost_ms = match tcg {
                    Some(tcg) => {
                        let tcg = tcg.lock().await;
                        tcg.fork_cost_ema_ms
                    }
                    None => 0,
                };

                Response::ForkCost { fork_cost_ms }
            }

            Request::Claim {
                task_id,
                prefix,
                entries,
            } => {
                if let Some(message) = invalid_entries_message(&entries) {
                    return Response::Error { message };
                }

                let tcg = {
                    let mut cache = self.cache.lock().await;
                    cache.get_tcg(task_id.clone())
                };

                let mut tcg = tcg.lock().await;
                let outcomes = tcg.claim_entries(&prefix, &entries, self.connection_id);

                // Only a caller that will execute needs a sandbox. The lease is on
                // the prefix: a sandbox at an entry's own node already contains
                // that call, so it can never be used to execute it.
                let (mut sandbox_path, mut sandbox_id) = (None, None);

                if outcomes
                    .iter()
                    .any(|outcome| matches!(outcome.status, ClaimStatus::Owner | ClaimStatus::Missing))
                {
                    (sandbox_path, sandbox_id) = tcg.deepest_sandbox(&prefix);

                    if let (Some(path), Some(id)) = (&sandbox_path, &sandbox_id) {
                        tcg.record_lpm_hit(path);
                        tcg.increment_sandbox_reference(id);
                    }
                }

                for (entry, outcome) in entries.iter().zip(&outcomes) {
                    if outcome.status == ClaimStatus::Owner {
                        let mut path = prefix.clone();
                        path.push(entry.clone());

                        self.owned_claims.push((
                            task_id.clone(),
                            path,
                            Ownership {
                                connection_id: self.connection_id,
                                generation: outcome.generation,
                            },
                        ));
                    }
                }

                Response::ClaimResults {
                    sandbox_path,
                    sandbox_id,
                    outcomes,
                }
            }

            Request::Inspect {
                task_id,
                prefix,
                entries,
                wait_ms,
            } => {
                if let Some(message) = invalid_entries_message(&entries) {
                    return Response::Error { message };
                }

                // peek_tcg (non-creating) so polling can never allocate a TCG.
                let tcg = {
                    let cache = self.cache.lock().await;
                    cache.peek_tcg(&task_id)
                };

                let outcomes = match tcg {
                    Some(tcg) => {
                        let wait = Duration::from_millis(wait_ms.min(MAX_INSPECT_WAIT_MS) as u64);
                        inspect_until_settled(&tcg, &prefix, &entries, wait).await
                    }
                    None => entries.iter().map(|_| ClaimOutcome::missing()).collect(),
                };

                Response::ClaimResults {
                    sandbox_path: None,
                    sandbox_id: None,
                    outcomes,
                }
            }

            Request::Publish {
                task_id,
                path,
                generation,
                runtime,
                value,
                sandbox_id,
                ttl,
            } => {
                let ownership = Ownership {
                    connection_id: self.connection_id,
                    generation,
                };

                // Accepted or not, this claim no longer needs failing on disconnect.
                self.owned_claims
                    .retain(|claim| !(claim.0 == task_id && claim.1 == path && claim.2 == ownership));

                let tcg = {
                    let cache = self.cache.lock().await;
                    cache.peek_tcg(&task_id)
                };

                let (value_inserted, sandbox_inserted) = match tcg {
                    Some(tcg) => {
                        let mut tcg = tcg.lock().await;
                        tcg.publish(&path, ownership, value, sandbox_id, runtime, ttl)
                    }
                    None => (false, false),
                };

                Response::InsertResult {
                    value_inserted,
                    sandbox_inserted,
                }
            }
        }
    }

    async fn write_response(
        &self,
        stream: &mut TcpStream,
        response: Response,
    ) -> Result<(), io::Error> {
        match response {
            Response::InsertResult {
                value_inserted,
                sandbox_inserted,
            } => {
                protocol_primitives::write_u8(stream, 1).await?;
                protocol_primitives::write_bool(stream, value_inserted).await?;
                protocol_primitives::write_bool(stream, sandbox_inserted).await?;
            }

            Response::FindResult {
                value,
                sandbox_path,
                sandbox_id,
            } => {
                protocol_primitives::write_u8(stream, 2).await?;

                if let Some(value) = value {
                    protocol_primitives::write_bool(stream, true).await?;
                    protocol_primitives::write_string_u32(stream, &value).await?;
                } else {
                    protocol_primitives::write_bool(stream, false).await?;
                }

                if let Some(sandbox_path) = sandbox_path {
                    protocol_primitives::write_bool(stream, true).await?;
                    protocol_primitives::write_path(stream, &sandbox_path).await?;
                } else {
                    protocol_primitives::write_bool(stream, false).await?;
                }

                if let Some(sandbox_id) = sandbox_id {
                    protocol_primitives::write_bool(stream, true).await?;
                    protocol_primitives::write_string_u16(stream, &sandbox_id).await?;
                } else {
                    protocol_primitives::write_bool(stream, false).await?;
                }
            }

            Response::UnrefSandboxResult {
                remaining_references,
            } => {
                protocol_primitives::write_u8(stream, 5).await?;
                protocol_primitives::write_u32(stream, remaining_references).await?;
            }

            Response::MarkSandboxesEvictedResult {
                sandbox_ids_recorded,
            } => {
                protocol_primitives::write_u8(stream, 6).await?;
                protocol_primitives::write_u32(stream, sandbox_ids_recorded).await?;
            }

            Response::SandboxCandidates { candidates } => {
                protocol_primitives::write_u8(stream, 7).await?;
                self.write_sandbox_candidates(stream, &candidates).await?;
            }

            Response::ReclaimableSandboxes { sandboxes } => {
                protocol_primitives::write_u8(stream, 8).await?;

                // u32 length: a large TTL sweep can produce more than u16::MAX victims.
                let sandbox_count = u32::try_from(sandboxes.len()).map_err(|error| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("too many reclaimable sandboxes for u32 length: {error}"),
                    )
                })?;

                protocol_primitives::write_u32(stream, sandbox_count).await?;

                for sandbox in &sandboxes {
                    protocol_primitives::write_string_u16(stream, &sandbox.task_id).await?;
                    protocol_primitives::write_string_u16(stream, &sandbox.sandbox_id).await?;
                }
            }

            Response::ForkCost { fork_cost_ms } => {
                protocol_primitives::write_u8(stream, 9).await?;
                protocol_primitives::write_u32(stream, fork_cost_ms).await?;
            }

            Response::Error { message } => {
                protocol_primitives::write_u8(stream, 3).await?;
                protocol_primitives::write_string_u32(stream, &message).await?;
            }

            Response::Json { json } => {
                protocol_primitives::write_u8(stream, 4).await?;
                protocol_primitives::write_string_u32(stream, &json).await?;
            }

            Response::ClaimResults {
                sandbox_path,
                sandbox_id,
                outcomes,
            } => {
                protocol_primitives::write_u8(stream, 10).await?;
                protocol_primitives::write_optional_path(stream, &sandbox_path).await?;
                protocol_primitives::write_optional_string_u16(stream, &sandbox_id).await?;

                // Matches the u16 entry count of the request.
                let outcome_count = u16::try_from(outcomes.len()).map_err(|error| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("too many claim outcomes for u16 length: {error}"),
                    )
                })?;

                protocol_primitives::write_u16(stream, outcome_count).await?;

                for outcome in &outcomes {
                    protocol_primitives::write_u8(stream, outcome.status as u8).await?;
                    protocol_primitives::write_u64(stream, outcome.node_id).await?;
                    protocol_primitives::write_u32(stream, outcome.generation).await?;
                    protocol_primitives::write_optional_string_u32(stream, &outcome.value).await?;
                }
            }
        }

        Ok(())
    }

    async fn collect_sorted_candidates(
        &self,
        limit: u16,
        descending: bool,
    ) -> Vec<SandboxCandidate> {
        let limit = if limit == 0 { 10 } else { limit } as usize;

        let mut candidates = Vec::<SandboxCandidate>::new();
        let entries = {
            let cache = self.cache.lock().await;
            cache.entries_snapshot()
        };

        for (task_id, tcg) in entries {
            let tcg = tcg.lock().await;
            candidates.extend(tcg.collect_sandbox_candidates(&task_id));
        }

        candidates.sort_by_key(|candidate| candidate.score);
        if descending {
            candidates.reverse();
        }
        candidates.truncate(limit);

        candidates
    }

    async fn read_sandbox_eviction_records(
        &self,
        stream: &mut TcpStream,
    ) -> Result<Vec<SandboxEvictionRecord>, io::Error> {
        let record_count = protocol_primitives::read_u16(stream).await? as usize;
        let mut records = Vec::with_capacity(record_count);

        for _ in 0..record_count {
            let task_id = protocol_primitives::read_string_u16(stream).await?;
            let sandbox_path = protocol_primitives::read_path(stream).await?;
            let sandbox_id = protocol_primitives::read_string_u16(stream).await?;

            records.push(SandboxEvictionRecord {
                task_id,
                sandbox_path,
                sandbox_id,
            });
        }

        Ok(records)
    }

    async fn write_sandbox_candidates(
        &self,
        stream: &mut TcpStream,
        candidates: &[SandboxCandidate],
    ) -> Result<(), io::Error> {
        let candidate_count = u16::try_from(candidates.len()).map_err(|error| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("too many sandbox candidates for u16 length: {error}"),
            )
        })?;

        protocol_primitives::write_u16(stream, candidate_count).await?;

        for candidate in candidates {
            protocol_primitives::write_string_u16(stream, &candidate.task_id).await?;
            protocol_primitives::write_path(stream, &candidate.sandbox_path).await?;
            protocol_primitives::write_string_u16(stream, &candidate.sandbox_id).await?;
            protocol_primitives::write_u64(stream, candidate.score).await?;
        }

        Ok(())
    }
}

// A batch with a repeated entry would wait on its own claim, so it is rejected.
fn invalid_entries_message(entries: &[String]) -> Option<String> {
    if entries.is_empty() {
        return Some("claim and inspect need at least one entry".to_string());
    }

    let unique_entries: HashSet<&String> = entries.iter().collect();

    if unique_entries.len() != entries.len() {
        return Some("duplicate entries in one request".to_string());
    }

    None
}

// Long poll: returns as soon as no entry is pending, or at the deadline.
async fn inspect_until_settled(
    tcg: &TcgSharedObject,
    prefix: &[String],
    entries: &[String],
    wait: Duration,
) -> Vec<ClaimOutcome> {
    let deadline = Instant::now() + wait;

    loop {
        let completions;
        let notified;

        {
            let tcg = tcg.lock().await;
            let outcomes = tcg.inspect_entries(prefix, entries);

            if outcomes.iter().all(|outcome| outcome.status != ClaimStatus::Pending)
                || Instant::now() >= deadline
            {
                return outcomes;
            }

            // Created under the TCG lock, so a publish after the check above
            // cannot be missed: Notified receives notify_waiters() from creation.
            completions = Arc::clone(&tcg.completions);
            notified = completions.notified();
        }

        let _ = tokio::time::timeout_at(deadline, notified).await;
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::{net::TcpListener, sync::Mutex};

    use super::*;
    use crate::{tcg::ToolCallGraph, tvcache::TVCache};

    fn path(entries: &[&str]) -> Vec<String> {
        entries.iter().map(|entry| entry.to_string()).collect()
    }

    fn ownership(generation: u32) -> Ownership {
        Ownership {
            connection_id: 1,
            generation,
        }
    }

    fn pending_tcg(entries: &[&str]) -> TcgSharedObject {
        let mut tcg = ToolCallGraph::new();
        tcg.claim_entries(&[], &path(entries), 1);
        Arc::new(tokio::sync::Mutex::new(tcg))
    }

    async fn publish_after(tcg: &TcgSharedObject, entry: &str, delay: Duration) {
        tokio::time::sleep(delay).await;
        let mut tcg = tcg.lock().await;
        tcg.publish(&path(&[entry]), ownership(1), Some(entry.to_string()), None, 1, None);
    }

    #[tokio::test]
    async fn inspect_returns_immediately_when_published_first() {
        let tcg = pending_tcg(&["a"]);
        publish_after(&tcg, "a", Duration::ZERO).await;

        let start = Instant::now();
        let outcomes = inspect_until_settled(&tcg, &[], &path(&["a"]), Duration::from_secs(5)).await;

        assert_eq!(outcomes[0].status, ClaimStatus::Ready);
        assert!(start.elapsed() < Duration::from_millis(100));
    }

    #[tokio::test]
    async fn inspect_wakes_on_publish() {
        let tcg = pending_tcg(&["a"]);
        let publisher = Arc::clone(&tcg);
        tokio::spawn(async move { publish_after(&publisher, "a", Duration::from_millis(50)).await });

        let start = Instant::now();
        let outcomes = inspect_until_settled(&tcg, &[], &path(&["a"]), Duration::from_secs(5)).await;

        assert_eq!(outcomes[0].status, ClaimStatus::Ready);
        assert_eq!(outcomes[0].value.as_deref(), Some("a"));
        assert!(start.elapsed() < Duration::from_secs(1));
    }

    #[tokio::test]
    async fn inspect_times_out_while_pending() {
        let tcg = pending_tcg(&["a"]);

        let start = Instant::now();
        let outcomes = inspect_until_settled(&tcg, &[], &path(&["a"]), Duration::from_millis(100)).await;

        assert_eq!(outcomes[0].status, ClaimStatus::Pending);
        assert!(start.elapsed() >= Duration::from_millis(100));
    }

    #[tokio::test]
    async fn inspect_batch_waits_for_every_entry() {
        let tcg = pending_tcg(&["a", "b"]);
        let publisher = Arc::clone(&tcg);
        tokio::spawn(async move {
            publish_after(&publisher, "a", Duration::from_millis(20)).await;
            publish_after(&publisher, "b", Duration::from_millis(100)).await;
        });

        let start = Instant::now();
        let outcomes = inspect_until_settled(&tcg, &[], &path(&["a", "b"]), Duration::from_secs(5)).await;

        assert!(outcomes.iter().all(|outcome| outcome.status == ClaimStatus::Ready));
        assert!(start.elapsed() >= Duration::from_millis(120));
    }

    // Wire-level tests against a real in-process server.

    struct ClaimReply {
        sandbox_path: Option<Vec<String>>,
        sandbox_id: Option<String>,
        outcomes: Vec<(u8, u64, u32, Option<String>)>,
    }

    async fn start_server() -> (std::net::SocketAddr, CacheSharedObject) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let cache: CacheSharedObject = Arc::new(Mutex::new(TVCache::new()));
        tokio::spawn(crate::serve(listener, Arc::clone(&cache)));
        (address, cache)
    }

    async fn read_optional<T>(
        stream: &mut TcpStream,
        read: impl AsyncFnOnce(&mut TcpStream) -> io::Result<T>,
    ) -> Option<T> {
        match protocol_primitives::read_u8(stream).await.unwrap() {
            1 => Some(read(stream).await.unwrap()),
            _ => None,
        }
    }

    async fn read_claim_reply(stream: &mut TcpStream) -> ClaimReply {
        let tag = protocol_primitives::read_u8(stream).await.unwrap();
        assert_eq!(tag, 10, "expected ClaimResults");
        let sandbox_path = read_optional(stream, protocol_primitives::read_path).await;
        let sandbox_id = read_optional(stream, protocol_primitives::read_string_u16).await;
        let count = protocol_primitives::read_u16(stream).await.unwrap();
        let mut outcomes = Vec::new();

        for _ in 0..count {
            let status = protocol_primitives::read_u8(stream).await.unwrap();
            let mut node_id = [0; 8];
            tokio::io::AsyncReadExt::read_exact(stream, &mut node_id).await.unwrap();
            let generation = protocol_primitives::read_u32(stream).await.unwrap();
            let value = read_optional(stream, protocol_primitives::read_string_u32).await;
            outcomes.push((status, u64::from_be_bytes(node_id), generation, value));
        }

        ClaimReply {
            sandbox_path,
            sandbox_id,
            outcomes,
        }
    }

    async fn send_claim(stream: &mut TcpStream, tag: u8, prefix: &[&str], entries: &[&str]) {
        protocol_primitives::write_u8(stream, tag).await.unwrap();
        protocol_primitives::write_string_u16(stream, "task").await.unwrap();
        protocol_primitives::write_path(stream, &path(prefix)).await.unwrap();
        protocol_primitives::write_path(stream, &path(entries)).await.unwrap();
    }

    async fn claim(stream: &mut TcpStream, prefix: &[&str], entries: &[&str]) -> ClaimReply {
        send_claim(stream, 11, prefix, entries).await;
        read_claim_reply(stream).await
    }

    async fn inspect(stream: &mut TcpStream, prefix: &[&str], entries: &[&str], wait_ms: u32) -> ClaimReply {
        send_claim(stream, 12, prefix, entries).await;
        protocol_primitives::write_u32(stream, wait_ms).await.unwrap();
        read_claim_reply(stream).await
    }

    async fn publish(stream: &mut TcpStream, full_path: &[&str], generation: u32, value: Option<&str>) -> (bool, bool) {
        protocol_primitives::write_u8(stream, 13).await.unwrap();
        protocol_primitives::write_string_u16(stream, "task").await.unwrap();
        protocol_primitives::write_path(stream, &path(full_path)).await.unwrap();
        protocol_primitives::write_u32(stream, generation).await.unwrap();
        protocol_primitives::write_u32(stream, 1).await.unwrap(); // runtime
        protocol_primitives::write_optional_string_u32(stream, &value.map(str::to_string)).await.unwrap();
        protocol_primitives::write_bool(stream, false).await.unwrap(); // sandbox id
        protocol_primitives::write_bool(stream, false).await.unwrap(); // ttl

        assert_eq!(protocol_primitives::read_u8(stream).await.unwrap(), 1, "expected InsertResult");
        let value_inserted = protocol_primitives::read_u8(stream).await.unwrap() == 1;
        let sandbox_inserted = protocol_primitives::read_u8(stream).await.unwrap() == 1;
        (value_inserted, sandbox_inserted)
    }

    const OWNER: u8 = ClaimStatus::Owner as u8;
    const PENDING: u8 = ClaimStatus::Pending as u8;
    const READY: u8 = ClaimStatus::Ready as u8;
    const FAILED: u8 = ClaimStatus::Failed as u8;
    const MISSING: u8 = ClaimStatus::Missing as u8;

    #[tokio::test]
    async fn concurrent_claimers_share_one_execution() {
        let (address, _cache) = start_server().await;
        let mut owner = TcpStream::connect(address).await.unwrap();
        let mut waiter = TcpStream::connect(address).await.unwrap();

        let owned = claim(&mut owner, &[], &["call"]).await.outcomes[0].clone();
        let waiting = claim(&mut waiter, &[], &["call"]).await.outcomes[0].clone();
        assert_eq!(owned.0, OWNER);
        assert_eq!(waiting.0, PENDING);
        assert_eq!((waiting.1, waiting.2), (owned.1, owned.2));

        let poll = tokio::spawn(async move { inspect(&mut waiter, &[], &["call"], 5_000).await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(publish(&mut owner, &["call"], owned.2, Some("result")).await, (true, false));

        let settled = poll.await.unwrap().outcomes[0].clone();
        assert_eq!(settled.0, READY);
        assert_eq!(settled.3.as_deref(), Some("result"));
        assert_eq!(settled.1, owned.1);
    }

    #[tokio::test]
    async fn owner_disconnect_fails_claim_and_allows_reclaim() {
        let (address, _cache) = start_server().await;
        let mut owner = TcpStream::connect(address).await.unwrap();
        let mut waiter = TcpStream::connect(address).await.unwrap();

        let owned = claim(&mut owner, &[], &["call"]).await.outcomes[0].clone();
        assert_eq!(claim(&mut waiter, &[], &["call"]).await.outcomes[0].0, PENDING);

        let poll = tokio::spawn(async move {
            let reply = inspect(&mut waiter, &[], &["call"], 5_000).await;
            (reply, waiter)
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        drop(owner);

        let (reply, mut waiter) = poll.await.unwrap();
        assert_eq!(reply.outcomes[0].0, FAILED);

        let reclaimed = claim(&mut waiter, &[], &["call"]).await.outcomes[0].clone();
        assert_eq!(reclaimed.0, OWNER);
        assert_eq!(reclaimed.1, owned.1);
        assert_eq!(reclaimed.2, owned.2 + 1);
    }

    #[tokio::test]
    async fn waiter_cannot_publish_and_failure_is_reported() {
        let (address, _cache) = start_server().await;
        let mut owner = TcpStream::connect(address).await.unwrap();
        let mut waiter = TcpStream::connect(address).await.unwrap();

        let owned = claim(&mut owner, &[], &["call"]).await.outcomes[0].clone();
        claim(&mut waiter, &[], &["call"]).await;

        assert_eq!(publish(&mut waiter, &["call"], owned.2, Some("forged")).await, (false, false));
        assert_eq!(publish(&mut owner, &["call"], owned.2, None).await, (true, false));
        assert_eq!(inspect(&mut waiter, &[], &["call"], 0).await.outcomes[0].0, FAILED);
    }

    #[tokio::test]
    async fn invalid_entry_lists_are_rejected() {
        let (address, _cache) = start_server().await;
        let mut stream = TcpStream::connect(address).await.unwrap();

        for entries in [&[][..], &["a", "a"][..]] {
            send_claim(&mut stream, 11, &[], entries).await;
            assert_eq!(protocol_primitives::read_u8(&mut stream).await.unwrap(), 3, "expected Error");
            protocol_primitives::read_string_u32(&mut stream).await.unwrap();
        }
    }

    #[tokio::test]
    async fn missing_prefix_and_unknown_task_report_missing() {
        let (address, _cache) = start_server().await;
        let mut stream = TcpStream::connect(address).await.unwrap();

        assert_eq!(inspect(&mut stream, &[], &["call"], 0).await.outcomes[0].0, MISSING);
        assert_eq!(claim(&mut stream, &["absent"], &["call"]).await.outcomes[0].0, MISSING);
    }

    #[tokio::test]
    async fn only_executing_callers_lease_the_prefix_sandbox() {
        let (address, cache) = start_server().await;
        let tcg = cache.lock().await.get_tcg("task".into());
        tcg.lock().await.insert(path(&["p"]), Some("p".into()), Some("sb".into()), 1, None, false);
        let mut owner = TcpStream::connect(address).await.unwrap();
        let mut waiter = TcpStream::connect(address).await.unwrap();

        let owned = claim(&mut owner, &["p"], &["call"]).await;
        let waiting = claim(&mut waiter, &["p"], &["call"]).await;

        assert_eq!(owned.sandbox_path, Some(path(&["p"])));
        assert_eq!(owned.sandbox_id.as_deref(), Some("sb"));
        assert_eq!((waiting.sandbox_path, waiting.sandbox_id), (None, None));
        let tcg = tcg.lock().await;
        assert_eq!(tcg.sandbox_references.get("sb"), Some(&1));
        assert_eq!(tcg.root.children["p"].number_of_lpm_hits, 1);
    }
}
