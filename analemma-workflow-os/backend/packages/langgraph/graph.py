"""A small, test-friendly stub of langgraph.graph.StateGraph.

This implements the minimal interface that `backend/main.py` expects
from langgraph.graph: add_node, add_edge, add_conditional_edges,
set_entry_point, set_finish_point, compile and a compiled graph with
an `invoke`-compatible method. It's intentionally simple: nodes are
executed in insertion order when invoked which is sufficient for
unit-testing prompt propagation and templating logic in the repo.
"""

from typing import Any, Dict, List, Callable, Optional

END = object()


class CompiledGraph:
    def __init__(self, nodes: Dict[str, Callable[[Dict[str, Any]], Any]], entry_point: Optional[str]):
        self.nodes = nodes
        self.entry_point = entry_point

    def invoke(self, input: Dict[str, Any] | None = None, configurable: Dict[str, Any] | None = None, callbacks: List[Any] | None = None):
        # Simple execution model: call every registered node in insertion order
        state = dict(input) if input else {}
        outputs = {}
        for nid, fn in list(self.nodes.items()):
            try:
                # Node call conventions in backend expect (state) or (state, config)
                try:
                    out = fn(state)
                except TypeError:
                    out = fn(state, None)
                if out:
                    outputs[nid] = out
                    # [Fix] Merge node output into state so subsequent nodes can access it
                    # This is critical for operator_official outputs like input_items
                    if isinstance(out, dict):
                        state.update(out)
            except Exception as e:
                # Propagate exceptions properly instead of swallowing them
                # This allows FAIL workflows to actually fail as expected
                raise e
        # merge outputs into state for convenience (legacy compatibility)
        state.update({"node_outputs": outputs})
        return state


class StateGraph:
    def __init__(self, cfg: Any = None):
        self.cfg = cfg
        self._nodes: Dict[str, Callable] = {}
        self._edges: List[Dict[str, Any]] = []
        self._entry: Optional[str] = None
        self._finish: Optional[str] = None

    def add_node(self, node_id: str, func: Callable) -> None:
        self._nodes[node_id] = func

    def add_edge(self, src: str, target: str) -> None:
        self._edges.append({"type": "edge", "source": src, "target": target})

    def add_conditional_edges(self, src: str, router: Callable[[Dict[str, Any]], str], mapping: Dict[str, Any]) -> None:
        # store conditional edge for completeness; router isn't executed in stub
        self._edges.append({"type": "conditional", "source": src, "mapping": mapping, "router": router})

    def set_entry_point(self, node_id: str) -> None:
        self._entry = node_id

    def set_finish_point(self, node_id: str) -> None:
        self._finish = node_id

    def compile(self, checkpointer: Any | None = None):
        # Return a CompiledGraph that will simply invoke registered nodes
        return CompiledGraph(self._nodes, self._entry)

    # Legacy convenience
    def invoke(self, state: Dict[str, Any]):
        return self.compile().invoke(input=state)

