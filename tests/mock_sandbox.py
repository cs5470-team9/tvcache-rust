"""In-memory tool call and sandbox shared by the tvclient integration tests."""

import asyncio
import itertools
import json
from collections import Counter
from typing import Dict, List, Set, Tuple

from tvclient import Sandbox, ToolCall

SLOW_CALL_SECONDS = 0.08
FAST_CALL_SECONDS = 0.001


class CommandToolCall(ToolCall):
    def __init__(self, command: str, mutates: bool = True, slow: bool = False):
        self.command = command
        self.mutates = mutates
        self.slow = slow

    def to_dict(self) -> dict:
        return {"command": self.command}

    def will_mutate_state(self) -> bool:
        return self.mutates


def cat() -> CommandToolCall:
    """A stateless read whose result is the sandbox state, so a sandbox that
    missed a mutation returns a visibly wrong value."""
    return CommandToolCall("cat", mutates=False)


class MockSandbox(Sandbox):
    """The state is the list of serialized stateful calls applied so far.

    Follows the Sandbox contract: fork() stores a cold snapshot handle, and
    start() with an env_id restores that snapshot into a new live sandbox.
    """

    registry: Dict[str, List[str]] = {}
    execute_log: List[Tuple[str, str]] = []
    executions: Counter = Counter()  # serialized call -> number of executions
    stop_log: List[str] = []
    # serialized call -> event the call blocks on until a test sets it.
    gates: Dict[str, asyncio.Event] = {}
    # serialized call -> event set as soon as an execution of it starts.
    entered: Dict[str, asyncio.Event] = {}
    # serialized calls whose next execution raises (after passing its gate).
    raise_once: Set[str] = set()
    _counter = itertools.count()

    async def start(self) -> None:
        # .get tolerates foreign snapshot ids left in the cache by other runs.
        state = list(MockSandbox.registry.get(self.env_id, [])) if self.env_id else []
        self.env_id = f"env-{next(MockSandbox._counter)}"
        MockSandbox.registry[self.env_id] = state

    async def fork(self) -> "MockSandbox":
        handle = MockSandbox(task_name=self.task_name, env_id=f"env-{next(MockSandbox._counter)}")
        MockSandbox.registry[handle.env_id] = list(MockSandbox.registry.get(self.env_id, []))
        return handle

    async def execute(self, tool_call: ToolCall) -> str:
        serialized = tool_call.serialize()
        MockSandbox.executions[serialized] += 1
        MockSandbox.entered.setdefault(serialized, asyncio.Event()).set()
        if serialized in MockSandbox.gates:
            await MockSandbox.gates[serialized].wait()
        if serialized in MockSandbox.raise_once:
            MockSandbox.raise_once.discard(serialized)
            raise RuntimeError(f"sandbox crashed while executing {serialized}")
        await asyncio.sleep(
            SLOW_CALL_SECONDS if getattr(tool_call, "slow", False) else FAST_CALL_SECONDS
        )
        MockSandbox.execute_log.append((self.env_id, serialized))
        state = MockSandbox.registry[self.env_id]
        if getattr(tool_call, "command", None) == "cat":
            return f"state:{json.dumps(state)}"
        if tool_call.will_mutate_state():
            state.append(serialized)
        return f"result:{serialized}"

    async def stop(self) -> None:
        MockSandbox.stop_log.append(self.env_id)
        MockSandbox.registry.pop(self.env_id, None)
