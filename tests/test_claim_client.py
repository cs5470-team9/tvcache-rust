#!/usr/bin/env python3
"""Claim/Inspect/Publish round trips through tvclient against a live server.

Run the server first: cargo run
Then: python tests/test_claim_client.py (or scripts/test.sh)
"""

import asyncio
import os
import time

from tvclient import (
    CLAIM_FAILED,
    CLAIM_MISSING,
    CLAIM_OWNER,
    CLAIM_PENDING,
    CLAIM_READY,
    TVCacheClient,
)

HOST = "127.0.0.1"
PORT = int(os.environ.get("TVCACHE_PORT", "7878"))


async def sandbox_references(client: TVCacheClient, task_id: str) -> dict:
    return (await client.dump_all())[task_id]["sandbox_references"]


async def test_round_trip(prefix: str) -> None:
    task_id = f"{prefix}-round-trip"

    async with TVCacheClient(HOST, PORT) as owner, TVCacheClient(HOST, PORT) as waiter:
        async with await owner.claim(task_id, [], ["call"]) as claimed:
            owned = claimed.outcomes[0]
        assert owned.status == CLAIM_OWNER, owned
        assert owned.generation == 1 and owned.node_id > 0 and owned.value is None, owned

        async with await waiter.claim(task_id, [], ["call"]) as claimed:
            waiting = claimed.outcomes[0]
        assert waiting.status == CLAIM_PENDING, waiting
        assert (waiting.node_id, waiting.generation) == (owned.node_id, owned.generation)

        polled = (await waiter.inspect(task_id, [], ["call"])).outcomes[0]
        assert polled.status == CLAIM_PENDING, polled

        # A waiter cannot complete someone else's claim.
        forged = await waiter.publish(task_id, ["call"], owned.generation, "forged", None, runtime=1)
        assert not forged.value_inserted

        published = await owner.publish(task_id, ["call"], owned.generation, "", None, runtime=1)
        assert published.value_inserted

        # An empty string is a valid, ready result.
        ready = (await waiter.inspect(task_id, [], ["call"], wait_ms=1000)).outcomes[0]
        assert (ready.status, ready.value, ready.node_id) == (CLAIM_READY, "", owned.node_id), ready

        async with await waiter.claim(task_id, [], ["call"]) as claimed:
            hit = claimed.outcomes[0]
        assert (hit.status, hit.value) == (CLAIM_READY, ""), hit

        # Publishing no value reports failure; the next claim takes over.
        async with await owner.claim(task_id, [], ["broken"]) as claimed:
            broken = claimed.outcomes[0]
        await owner.publish(task_id, ["broken"], broken.generation, None, None, runtime=1)
        assert (await waiter.inspect(task_id, [], ["broken"])).outcomes[0].status == CLAIM_FAILED
        async with await waiter.claim(task_id, [], ["broken"]) as claimed:
            retaken = claimed.outcomes[0]
        assert (retaken.status, retaken.generation) == (CLAIM_OWNER, broken.generation + 1), retaken

        missing = (await waiter.inspect(task_id, ["absent"], ["call"])).outcomes[0]
        assert missing.status == CLAIM_MISSING, missing

    print("round trip leg passed")


async def test_lease(prefix: str) -> None:
    task_id = f"{prefix}-lease"

    async with TVCacheClient(HOST, PORT) as client:
        await client.insert(task_id, ["p"], "p", f"{task_id}-sb", runtime=1)

        async with await client.claim(task_id, ["p"], ["a"]) as claimed:
            assert claimed.outcomes[0].status == CLAIM_OWNER
            assert claimed.sandbox_path == ["p"] and claimed.sandbox_id == f"{task_id}-sb"
            assert await sandbox_references(client, task_id) == {f"{task_id}-sb": 1}
        assert await sandbox_references(client, task_id) == {}

        # The lease is released even when the body raises.
        try:
            async with await client.claim(task_id, ["p"], ["b"]):
                raise RuntimeError("rollout crashed")
        except RuntimeError:
            pass
        assert await sandbox_references(client, task_id) == {}

        # Callers that will not execute get no lease.
        async with TVCacheClient(HOST, PORT) as other:
            async with await other.claim(task_id, ["p"], ["a"]) as claimed:
                assert claimed.outcomes[0].status == CLAIM_PENDING
                assert claimed.sandbox_id is None
        assert await sandbox_references(client, task_id) == {}

    print("lease leg passed")


async def test_concurrent_requests_on_one_client(prefix: str) -> None:
    task_id = f"{prefix}-shared-connection"

    async with TVCacheClient(HOST, PORT) as client:
        for index in range(10):
            await client.insert(task_id, [f"k{index}"], f"v{index}", None, runtime=1)

        # Interleaved coroutines on one connection must each get their own response.
        async def inspect(index: int) -> None:
            outcome = (await client.inspect(task_id, [], [f"k{index}"])).outcomes[0]
            assert outcome.value == f"v{index}", (index, outcome)

        async def fork_cost(index: int) -> None:
            reported = await client.report_fork_cost(f"{task_id}-{index}", 1000 + index)
            assert reported == 1000 + index, (index, reported)

        await asyncio.gather(*[inspect(index) for index in range(10)],
                             *[fork_cost(index) for index in range(10)])

    print("shared connection leg passed")


async def run_test() -> None:
    prefix = f"claim-client-{time.time_ns()}"
    await test_round_trip(prefix)
    await test_lease(prefix)
    await test_concurrent_requests_on_one_client(prefix)
    print("all claim client tests passed")


def main() -> None:
    asyncio.run(run_test())


if __name__ == "__main__":
    main()
