#!/usr/bin/env python3
"""Randomized coalescing test: many rollouts walk random call sequences with
random delays, and the cache must neither change any result nor execute any
(task, path) more than once as an owner.

Run the server first: cargo run --eviction-policy=none
Then: python tests/test_coalescing_random.py (or scripts/test.sh)
"""

import asyncio
import json
import os
import random
import time
from typing import List, Tuple

from mock_sandbox import CommandToolCall, MockSandbox, cat
from tvclient import ToolCall, TvcacheToolExecutor, TVCacheClient

HOST = "127.0.0.1"
PORT = int(os.environ.get("TVCACHE_PORT", "7878"))
SEEDS = range(5)
ROLLOUTS = 16
DEPTH = 4
TIMEOUT_SECONDS = 30

# A small alphabet so rollouts share prefixes and collide often; slow calls
# keep executions in flight long enough for others to arrive.
CALLS = [
    CommandToolCall("write-fast"),
    CommandToolCall("write-slow", slow=True),
    CommandToolCall("read-slow", mutates=False, slow=True),
    cat(),
]


def expected_value(history: List[ToolCall]) -> str:
    """What an uncached rollout would return for its last call."""
    current = history[-1]
    if current.command == "cat":
        state = [call.serialize() for call in history[:-1] if call.will_mutate_state()]
        return f"state:{json.dumps(state)}"
    return f"result:{current.serialize()}"


def cache_key(history: List[ToolCall]) -> Tuple[str, ...]:
    stateful = [call.serialize() for call in history[:-1] if call.will_mutate_state()]
    return tuple(stateful) + (history[-1].serialize(),)


async def run_seed(client: TVCacheClient, task_id: str, seed: int) -> int:
    rng = random.Random(seed)
    sequences = [[rng.choice(CALLS) for _ in range(DEPTH)] for _ in range(ROLLOUTS)]
    delays = [[rng.uniform(0, 0.03) for _ in range(DEPTH)] for _ in range(ROLLOUTS)]
    executors = [
        TvcacheToolExecutor(
            MockSandbox,
            CommandToolCall,
            task_id,
            host=HOST,
            port=PORT,
            adaptive=False,
            fixed_threshold_seconds=0.05,
            coalesce=True,
        )
        for _ in range(ROLLOUTS)
    ]

    async def rollout(index: int) -> List[Tuple[List[ToolCall], str]]:
        observed = []
        async with executors[index] as executor:
            for step in range(1, DEPTH + 1):
                await asyncio.sleep(delays[index][step - 1])
                history = sequences[index][:step]
                observed.append((history, await executor.lookup_or_execute(history)))
        return observed

    results = await asyncio.wait_for(
        asyncio.gather(*[rollout(index) for index in range(ROLLOUTS)]), TIMEOUT_SECONDS
    )

    for observed in results:
        for history, value in observed:
            assert value == expected_value(history), (seed, [c.command for c in history], value)

    keys = {cache_key(sequence[:step]) for sequence in sequences for step in range(1, DEPTH + 1)}
    owners = sum(executor.total_owned for executor in executors)
    assert owners == len(keys), f"seed {seed}: {owners} owners for {len(keys)} distinct paths"
    graph = (await client.dump_all())[task_id]
    assert graph["number_of_nodes"] == len(keys), (seed, graph["number_of_nodes"], len(keys))
    assert graph["sandbox_references"] == {}, graph["sandbox_references"]
    return sum(executor.total_waits for executor in executors)


async def run_test() -> None:
    prefix = f"coalescing-random-{time.time_ns()}"
    total_waits = 0

    async with TVCacheClient(HOST, PORT) as client:
        for seed in SEEDS:
            total_waits += await run_seed(client, f"{prefix}-{seed}", seed)

    # Guards against a schedule so sparse that nothing was ever coalesced.
    assert total_waits > 0, "no rollout ever waited on a pending node"
    print(f"randomized leg passed: {len(SEEDS)} seeds, {total_waits} coalesced waits")
    print("all randomized coalescing tests passed")


def main() -> None:
    asyncio.run(run_test())


if __name__ == "__main__":
    main()
