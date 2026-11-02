#!/usr/bin/env python3
"""Coalescing executor tests: concurrent rollouts that reach the same call run it
once, and the rest wait on the pending node instead of re-executing.

Run the server first: cargo run
Then: python tests/test_coalescing.py (or scripts/test.sh)
"""

import asyncio
import json
import os
import time
from typing import List

from mock_sandbox import CommandToolCall, MockSandbox, cat
from tvclient import ToolCall, TvcacheToolExecutor, TVCacheClient

HOST = "127.0.0.1"
PORT = int(os.environ.get("TVCACHE_PORT", "7878"))
FORK_THRESHOLD_SECONDS = 0.05
ROLLOUTS = 8
TIMEOUT_SECONDS = 10


def make_executor(task_id: str, coalesce: bool = True) -> TvcacheToolExecutor:
    return TvcacheToolExecutor(
        MockSandbox,
        CommandToolCall,
        task_id,
        host=HOST,
        port=PORT,
        adaptive=False,
        fixed_threshold_seconds=FORK_THRESHOLD_SECONDS,
        coalesce=coalesce,
    )


def state_of(*calls: ToolCall) -> str:
    return f"state:{json.dumps([call.serialize() for call in calls])}"


def gate(call: ToolCall) -> asyncio.Event:
    """Blocks every execution of call until the returned event is set; create
    it before the rollouts start."""
    return MockSandbox.gates.setdefault(call.serialize(), asyncio.Event())


async def wait_until(condition, what: str, timeout: float = TIMEOUT_SECONDS) -> None:
    deadline = time.monotonic() + timeout
    while not await condition():
        if time.monotonic() > deadline:
            raise AssertionError(f"timed out waiting for {what}")
        await asyncio.sleep(0.005)


async def wait_for_waiters(client: TVCacheClient, task_id: str, path: List[ToolCall], count: int) -> None:
    """Every claim of a pending node counts one hit, so this observes that all
    other rollouts have claimed and are waiting before the owner is released."""

    async def claimed() -> bool:
        node = (await client.dump_all()).get(task_id, {}).get("root")
        for call in path:
            node = (node or {}).get("children", {}).get(call.serialize())
        return node is not None and node["number_of_cache_hits"] >= count

    await wait_until(claimed, f"{count} waiters on {path[-1].serialize()}")


async def run_rollouts(executors: List[TvcacheToolExecutor], history: List[ToolCall]) -> list:
    return await asyncio.wait_for(
        asyncio.gather(
            *[executor.lookup_or_execute(history) for executor in executors],
            return_exceptions=True,
        ),
        TIMEOUT_SECONDS,
    )


async def open_executors(task_id: str, coalesce: bool = True) -> List[TvcacheToolExecutor]:
    executors = [make_executor(task_id, coalesce) for _ in range(ROLLOUTS)]
    for executor in executors:
        await executor.client.connect()
    return executors


async def close_executors(executors: List[TvcacheToolExecutor]) -> None:
    for executor in executors:
        await executor.close()


async def test_identical_calls_execute_once(client: TVCacheClient, prefix: str) -> None:
    task_id = f"{prefix}-identical"
    setup = CommandToolCall(f"{task_id}-setup", slow=True)
    executors = await open_executors(task_id)

    release = gate(setup)
    rollouts = asyncio.create_task(run_rollouts(executors, [setup]))
    await wait_for_waiters(client, task_id, [setup], ROLLOUTS - 1)
    assert MockSandbox.executions[setup.serialize()] == 1, "only the owner may start executing"
    release.set()
    values = await rollouts

    assert values == [f"result:{setup.serialize()}"] * ROLLOUTS, values
    assert MockSandbox.executions[setup.serialize()] == 1, MockSandbox.executions[setup.serialize()]
    assert sum(executor.total_owned for executor in executors) == 1
    assert sum(executor.total_waits for executor in executors) == ROLLOUTS - 1
    await close_executors(executors)
    print("identical calls leg passed")


async def test_without_coalescing_every_rollout_executes(prefix: str) -> None:
    task_id = f"{prefix}-uncoalesced"
    setup = CommandToolCall(f"{task_id}-setup", slow=True)
    executors = await open_executors(task_id, coalesce=False)

    release = gate(setup)
    rollouts = asyncio.create_task(run_rollouts(executors, [setup]))

    async def all_executing() -> bool:
        return MockSandbox.executions[setup.serialize()] == ROLLOUTS

    await wait_until(all_executing, "every rollout to execute")
    release.set()
    await rollouts

    assert sum(executor.total_waits for executor in executors) == 0
    await close_executors(executors)
    print("uncoalesced baseline leg passed")


async def test_different_histories_do_not_coalesce(prefix: str) -> None:
    task_id = f"{prefix}-histories"
    branch_a = CommandToolCall(f"{task_id}-a")
    branch_b = CommandToolCall(f"{task_id}-b")
    read = cat()
    executors = await open_executors(task_id)
    group_a, group_b = executors[: ROLLOUTS // 2], executors[ROLLOUTS // 2 :]

    await asyncio.gather(run_rollouts(group_a, [branch_a]), run_rollouts(group_b, [branch_b]))
    values_a, values_b = await asyncio.gather(
        run_rollouts(group_a, [branch_a, read]), run_rollouts(group_b, [branch_b, read])
    )

    # The same call text under different state histories is a different node.
    assert values_a == [state_of(branch_a)] * len(group_a), values_a
    assert values_b == [state_of(branch_b)] * len(group_b), values_b
    assert MockSandbox.executions[read.serialize()] == 2, MockSandbox.executions[read.serialize()]
    await close_executors(executors)
    print("different histories leg passed")


async def test_owner_failure_releases_waiters(client: TVCacheClient, prefix: str) -> None:
    task_id = f"{prefix}-owner-raises"
    setup = CommandToolCall(f"{task_id}-setup")
    executors = await open_executors(task_id)

    release = gate(setup)
    MockSandbox.raise_once.add(setup.serialize())
    rollouts = asyncio.create_task(run_rollouts(executors, [setup]))
    await wait_for_waiters(client, task_id, [setup], ROLLOUTS - 1)
    release.set()
    results = await rollouts

    errors = [result for result in results if isinstance(result, Exception)]
    values = [result for result in results if not isinstance(result, Exception)]
    assert len(errors) == 1 and isinstance(errors[0], RuntimeError), results
    # Waiters fall back to executing the call themselves.
    assert values == [f"result:{setup.serialize()}"] * (ROLLOUTS - 1), values
    assert MockSandbox.executions[setup.serialize()] == ROLLOUTS
    async with await client.claim(task_id, [], [setup.serialize()]) as claimed:
        assert claimed.outcomes[0].value == f"result:{setup.serialize()}", claimed.outcomes
    await close_executors(executors)
    print("owner failure leg passed")


async def test_owner_disconnect_wakes_waiters(client: TVCacheClient, prefix: str) -> None:
    task_id = f"{prefix}-owner-disconnects"
    setup = CommandToolCall(f"{task_id}-setup")
    executors = await open_executors(task_id)

    release = gate(setup)
    rollouts = asyncio.create_task(run_rollouts(executors, [setup]))
    await wait_for_waiters(client, task_id, [setup], ROLLOUTS - 1)
    owner = next(executor for executor in executors if executor.total_owned == 1)
    started = time.monotonic()
    await owner.client.close()

    async def waiters_executing() -> bool:
        return MockSandbox.executions[setup.serialize()] == ROLLOUTS

    await wait_until(waiters_executing, "waiters to fall back after the owner disconnects")
    # Woken by the server's notification, not by the next long-poll round.
    assert time.monotonic() - started < 1.0, time.monotonic() - started
    release.set()
    values = await rollouts

    # The owner reconnects to publish, but its claim is gone; it keeps its own result.
    assert values == [f"result:{setup.serialize()}"] * ROLLOUTS, values
    await close_executors(executors)
    print("owner disconnect leg passed")


async def test_waiter_catches_up_skipped_state(client: TVCacheClient, prefix: str) -> None:
    # Slow writes are snapshotted by the owner, so the waiter forks that
    # snapshot; fast writes are not, so the waiter replays the skipped write.
    for label, slow in (("fork", True), ("replay", False)):
        task_id = f"{prefix}-catch-up-{label}"
        write = CommandToolCall(f"{task_id}-write", slow=slow)
        read = cat()
        executors = await open_executors(task_id)

        release = gate(write)
        rollouts = asyncio.create_task(run_rollouts(executors, [write]))
        await wait_for_waiters(client, task_id, [write], ROLLOUTS - 1)
        release.set()
        await rollouts
        writes_before_read = MockSandbox.executions[write.serialize()]

        # A waiter never applied the write to a sandbox of its own; its next
        # miss must still see the written state.
        waiter = next(executor for executor in executors if executor.total_waits == 1)
        assert waiter.rollout_sandbox is None
        value = await asyncio.wait_for(waiter.lookup_or_execute([write, read]), TIMEOUT_SECONDS)

        assert value == state_of(write), (label, value)
        replays = MockSandbox.executions[write.serialize()] - writes_before_read
        assert replays == (0 if slow else 1), (label, replays)
        await close_executors(executors)
    print("waiter state catch-up leg passed")


async def run_test() -> None:
    prefix = f"coalescing-{time.time_ns()}"

    async with TVCacheClient(HOST, PORT) as client:
        await test_identical_calls_execute_once(client, prefix)
        await test_without_coalescing_every_rollout_executes(prefix)
        await test_different_histories_do_not_coalesce(prefix)
        await test_owner_failure_releases_waiters(client, prefix)
        await test_owner_disconnect_wakes_waiters(client, prefix)
        await test_waiter_catches_up_skipped_state(client, prefix)

    print("all coalescing tests passed")


def main() -> None:
    asyncio.run(run_test())


if __name__ == "__main__":
    main()
