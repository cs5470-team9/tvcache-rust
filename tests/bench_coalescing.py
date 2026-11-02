#!/usr/bin/env python3
"""Tool-wait benchmark with mock tool latencies: no cache vs the sequential
cache vs the coalescing cache.

Each task runs a group of rollouts, as in a GRPO step: two heavy setup calls,
a few reads drawn from a small pool, then the test. Rollout start times are
spread uniformly over --stagger seconds; tool-wait excludes that offset.

The mock sandbox has no resource contention, so with --stagger 0 a waiter
waits as long as the owner executes: coalescing then saves executions, not
latency. Latency is saved when rollouts arrive while a call is in flight.

Run the server first: cargo run -- --eviction-policy=none
Then: python tests/bench_coalescing.py [--tasks 2] [--rollouts 8] [--stagger 1.0]
"""

import argparse
import asyncio
import os
import random
import time
from typing import List

from mock_sandbox import CommandToolCall, MockSandbox
from tvclient import ToolCall, TvcacheToolExecutor

HOST = "127.0.0.1"
PORT = int(os.environ.get("TVCACHE_PORT", "7878"))
READ_POOL = 6
READS_PER_ROLLOUT = 3


class TimedCall(CommandToolCall):
    def __init__(self, command: str, seconds: float, mutates: bool = True):
        super().__init__(command, mutates=mutates)
        self.seconds = seconds


class TimedSandbox(MockSandbox):
    async def execute(self, tool_call: ToolCall) -> str:
        await asyncio.sleep(getattr(tool_call, "seconds", 0))
        return await super().execute(tool_call)


def rollout_calls(task: int, rng: random.Random) -> List[ToolCall]:
    reads = rng.sample(range(READ_POOL), READS_PER_ROLLOUT)
    return (
        [TimedCall(f"t{task}-clone-and-build", 1.0), TimedCall(f"t{task}-install-deps", 0.5)]
        + [TimedCall(f"t{task}-read-{read}", 0.05, mutates=False) for read in reads]
        + [TimedCall(f"t{task}-run-tests", 0.5, mutates=False)]
    )


async def run_without_cache(calls: List[ToolCall]) -> float:
    sandbox = TimedSandbox(task_name="no-cache")
    await sandbox.start()
    started = time.perf_counter()
    for call in calls:
        await sandbox.execute(call)
    waited = time.perf_counter() - started
    await sandbox.stop()
    return waited


async def run_with_cache(task_id: str, calls: List[ToolCall], coalesce: bool) -> float:
    async with TvcacheToolExecutor(
        TimedSandbox,
        TimedCall,
        task_id,
        host=HOST,
        port=PORT,
        adaptive=False,
        fixed_threshold_seconds=0.2,
        coalesce=coalesce,
    ) as executor:
        started = time.perf_counter()
        for step in range(1, len(calls) + 1):
            await executor.lookup_or_execute(calls[:step])
        return time.perf_counter() - started


async def run_config(name: str, args: argparse.Namespace, run_id: str) -> dict:
    rng = random.Random(0)  # the same workload for every configuration
    workloads = [
        [rollout_calls(task, rng) for _ in range(args.rollouts)] for task in range(args.tasks)
    ]
    offsets = [[rng.uniform(0, args.stagger) for _ in group] for group in workloads]
    executions_before = sum(MockSandbox.executions.values())
    started = time.perf_counter()

    async def rollout(task: int, calls: List[ToolCall], offset: float) -> float:
        await asyncio.sleep(offset)
        if name == "no cache":
            return await run_without_cache(calls)
        return await run_with_cache(f"{run_id}-{name}-{task}", calls, coalesce=name == "coalescing")

    waits = await asyncio.gather(
        *[
            rollout(task, calls, offset)
            for task, group in enumerate(workloads)
            for calls, offset in zip(group, offsets[task])
        ]
    )
    return {
        "name": name,
        "wall": time.perf_counter() - started,
        "mean_wait": sum(waits) / len(waits),
        "total_wait": sum(waits),
        "executions": sum(MockSandbox.executions.values()) - executions_before,
        "calls": sum(len(calls) for group in workloads for calls in group),
    }


async def run_benchmark(args: argparse.Namespace) -> None:
    run_id = f"bench-{time.time_ns()}"
    results = [await run_config(name, args, run_id) for name in ("no cache", "sequential", "coalescing")]

    print(f"{args.tasks} tasks x {args.rollouts} rollouts, starts spread over {args.stagger:.1f} s")
    print(f"{'config':<12} {'wall s':>7} {'tool-wait/rollout s':>20} {'total wait s':>13} {'executions':>11}")
    for result in results:
        print(
            f"{result['name']:<12} {result['wall']:>7.2f} {result['mean_wait']:>20.2f} "
            f"{result['total_wait']:>13.2f} {result['executions']:>7}/{result['calls']}"
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tasks", default=2, type=int)
    parser.add_argument("--rollouts", default=8, type=int)
    parser.add_argument("--stagger", default=1.0, type=float)
    asyncio.run(run_benchmark(parser.parse_args()))


if __name__ == "__main__":
    main()
