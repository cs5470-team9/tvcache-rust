# Pending nodes: implementation contract

Arjun Saini · Nov 1, 2026 · Status: implemented on branch `pending-nodes` of `cs5470-team9/tvcache-rust` and `cs5470-team9/tvclient`

This describes what the server and client now do, so the correctness testbench (`tvcache-tests`) can connect to them. It answers the questions in `arjun_interface_checklist.md`. Where the testbench proposal and the implementation differ, the differences are listed in §Adapter.

## Behavior in one paragraph

A rollout that misses on a call **claims** it. The claim creates the call's node in the tool-call graph *before* the call runs, marked `pending`, and makes that rollout's connection the **owner**. Any other rollout that claims the same task + path while it is pending gets `pending` back. It waits with a long-poll **inspect** until the owner **publishes** the result, so the call runs once. If the owner reports failure or its connection closes, the node becomes `failed`; waiters stop waiting and run the call themselves. The next claim takes the node over with a higher generation. Coalescing is opt-in in the executor (`TvcacheToolExecutor(coalesce=True)`), and the legacy `Insert`/`Find` path is unchanged, so the sequential-cache baseline is the same code with the flag off.

## Key and node identity

- **Key:** task id + path, as before. The path is the serialized stateful calls so far plus the current call. Coalescing never merges different state histories: the same call under a different prefix is a different node.
- **Node id:** `node_id: u64`, unique within a task graph and never reused, even after eviction. The root is 0. It stays the same through pending → ready → failed → re-claimed.
- **Generation:** `generation: u32`. It is 1 for every newly created node, whether `Claim` or legacy `Insert` created it, and increases by 1 on each re-claim. The root is 0.
- **Entries are opaque.** One entry is one serialized call today. A whole parallel turn can later be one entry without server changes.

## Node states

```
(absent) --Claim--> Pending(gen=1, owner=connection)
Pending  --Publish(value, owner connection, same gen)--> Ready
Pending  --Publish(no value) | owner connection closes--> Failed
Failed or Ready-without-value --Claim--> Pending(gen+1)
Ready-with-value --Claim--> hit (no change)
```

A node is `ready` only if it has a value. An empty string is a valid ready value. A legacy `Insert` never writes a value into a pending node; a legacy `Insert` with a value turns a failed node into a ready one.

## Wire protocol

New tags, in the existing encoding: big-endian, length-prefixed strings, and a 0/1 flag byte before each optional field. `entries` uses the same encoding as a path.

| Tag | Request | Fields | Response |
|---|---|---|---|
| 11 | `Claim` | `task_id` str16, `prefix` path, `entries` path | `ClaimResults` (10) |
| 12 | `Inspect` | `task_id` str16, `prefix` path, `entries` path, `wait_ms` u32 | `ClaimResults` (10) |
| 13 | `Publish` | `task_id` str16, `path`, `generation` u32, `runtime` u32, opt `value` str32, opt `sandbox_id` str16, opt `ttl` u32 | `InsertResult` (1) |

**`ClaimResults` (tag 10):** opt `sandbox_path`, opt `sandbox_id`, u16 count, then per entry: `status` u8, `node_id` u64, `generation` u32, opt `value` str32.

**Status codes:**

| Code | Name | Meaning |
|---|---|---|
| 0 | missing | The prefix isn't in the graph, or the node exists but has no result (`node_id` is 0 if absent) |
| 1 | owner | This connection created or took over the claim and must execute and publish it (`Claim` only) |
| 2 | pending | Another connection owns the execution |
| 3 | ready | Completed; `value` holds the result |
| 4 | failed | The owner reported failure or disconnected (`Inspect` only; a `Claim` on a failed node re-claims it) |

**Errors:** an empty entry list or a repeated entry in one request returns the existing `Error` response (tag 3).

**Sandbox lease:** returned only when some entry is `owner` or `missing`, i.e. when the caller will execute. It is the deepest snapshot on the **prefix**, and its reference count is incremented exactly as `Find` does. `ready` and `pending` callers receive no lease. The tvclient `ClaimResult` releases it on context exit.

**Counters:** a `Claim` that returns `ready` or `pending` adds 1 to the node's `number_of_cache_hits`. `Inspect` never changes counters, never creates nodes, and never creates a task graph.

## Answers to the checklist

| Question | Answer |
|---|---|
| Atomic claim/lookup distinguishing owner, waiter and hit? | `Claim`. All entries are resolved under one lock on the task graph, so two callers can never both become owner. |
| How does the owner publish, and how do waiters get it without missing a wake-up? | The owner sends `Publish`. Waiters send `Inspect` with `wait_ms > 0`: the server re-checks the node, and if it's still pending, subscribes to a per-task notification *while holding the graph lock*, then waits. A publish after the check therefore always wakes the waiter. A publish *before* the waiter's first poll is visible as `ready` immediately (T04). |
| Polling vs notification? | Both: `wait_ms = 0` is a plain read-only poll; `wait_ms > 0` is a long poll, capped by the server at 30 s. tvclient uses 2 s slices because its request timeout is 5 s. |
| Owner identity and generation? | The owner is the server-side connection that claimed, and the claim's generation. No owner token is sent on the wire, so a waiter cannot publish: its connection differs. |
| Owner failure? | `Publish` with no value → `failed`. In tvclient, the executor does this when the call raises, its sandbox fork fails, or the tool reports `Failed to execute`. |
| Owner disconnect? | When a connection closes, the server marks every claim it still owns `failed` and wakes their waiters (T07). If the client reconnects, it has a new connection id, so a late publish from the old claim is rejected. |
| Timeout? | No server-side claim expiry, because legitimate builds can run for tens of minutes. Waiters give up after `max_wait_seconds` (executor default 1800) and execute the call themselves; this changes no server state. |
| Retries / takeover? Stale owners? | The next `Claim` on a failed node takes over with `generation + 1` and the same `node_id`. A publish with an old generation, or from any connection other than the current owner's, is rejected (`value_inserted = false`). Takeover while the old owner is still alive and connected is not supported. |
| One call or a whole turn per pending key? | One entry per key. v1 always sends one entry, the current call. A turn can be sent as a batch of entries under one prefix; the layout for parallel turns (per-call, whole-turn or hybrid) is decided in the client later. |
| What forms the key? | Task id + the serialized stateful calls so far (prefix) + the current call (entry). |
| Result and state-reference format; call IDs? | The value is the stored string. Call IDs never reach the server or tvclient: the executor returns results in input order, and the harness maps them to its call IDs. |
| Does result availability guarantee snapshot readiness? | If the owner took a snapshot (decided by its snapshot policy), the snapshot is attached in the same `Publish`, so it's available when the result is. Otherwise a waiter's later miss rebuilds the state by forking an earlier snapshot and replaying, exactly as after an ordinary cache hit. Tested with the mock sandbox both ways. |
| Events to confirm callers are waiting before releasing the gate? | Each `pending` claim response, plus the node's `number_of_cache_hits` in `DumpAll` (one per pending claim). `test_coalescing.py` waits until it reaches N − 1 before opening its gate. |
| IDs and lifecycle in the dump? | Every node in `DumpAll` now has `node_id`, `status` (`"ready"`, `"pending"`, `"failed"`) and `generation`. The owner's connection id and the notification handle are never serialized. |

## Adapter for the testbench's `PendingClient`

Each simulated caller must use its own `TVCacheClient`, because ownership is per connection. The testbench's `factory()` already does this.

| `PendingClient` method | tvclient call |
|---|---|
| `lookup_or_claim(task_id, path, attempt_id)` | `async with await client.claim(task_id, list(path[:-1]), [path[-1]]) as claimed:`, then map `claimed.outcomes[0]`. Releasing the lease straight away is fine; fixtures don't fork. |
| `inspect(task_id, path)` | `client.inspect(task_id, list(path[:-1]), [path[-1]], wait_ms=0)` |
| `publish(task_id, path, *, node_id, generation, owner_token, value)` | `client.publish(task_id, list(path), generation, value, None, runtime=1)`; raise if `value_inserted` is false. The server identifies the node by its path; `node_id` and `owner_token` are not sent. |
| `dump_all()` | `client.dump_all()` |

**Mapping to `Outcome`:**
- status codes → `"missing"`, `"owner"`, `"pending"`, `"ready"`, `"failed"`
- `node_id = None` when the status is missing
- `owner_token` = any local non-`None` object for `owner` (for example the client instance), `None` otherwise

**Differences from the proposal:**
- **No `"prefix"` status.** Absent or value-less nodes report `missing`.
- **The root serializes `"status": "ready"`, generation 0,** not `"root"`. Please accept it, or treat node id 0 as the root.
- **Owner token:** ownership is enforced by the server-side connection, not a token, so the adapter's token is only a local marker.

## Running against it

```bash
cd tvcache-rust && cargo build
target/debug/tvcache --port=7879 --eviction-policy=none     # dedicated CPU-only test server
pip install -e ../tvclient                                  # or load_real_client(<path to tvclient>)
```

Pin the testbench to the commit SHA of `pending-nodes` it was run against. `scripts/test.sh` in `tvcache-rust` runs the implementation's own tests: 35 Rust tests, plus Python tests for the protocol, coalescing, failure, disconnect, state catch-up and a randomized concurrency check.

## Known limits

- **Waiters wait for the owner's snapshot fork too,** because the snapshot is attached in the same `Publish`. Publishing the value before forking is a planned follow-up.
- **A state-changing call whose result starts with `"Failed to execute"` is never cached** (upstream behavior). Later calls in that rollout then have a missing prefix: `Claim` returns `missing` and they run uncoalesced.
- **One notification per task:** a publish wakes every waiter in that task, and each re-checks its own entries. That's cheap at about 10 rollouts per task.
