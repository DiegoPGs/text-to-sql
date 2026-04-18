"""LangGraph agent graph — Phase 5 with safety, HITL, and self-correction.

Adds three control-flow concerns on top of the Phase-4 happy path:

1. ``classify_intent`` routes to ``clarify`` (HITL) for ambiguous questions
   and ``refuse`` (terminal) for out-of-scope questions.
2. ``validate_sql`` routes back to ``draft_sql`` on failure (capped by
   ``MAX_RETRIES``), or to ``terminate_failure`` once the cap is hit.
3. ``execute_sql`` routes back to ``draft_sql`` on a database error
   (same cap), or to ``terminate_failure`` on the final attempt.

The compiled graph uses a checkpointer so the ``clarify`` node's
``interrupt`` primitive can pause and resume.

Usage::

    import asyncpg
    from langgraph.types import Command
    from voyage.agent.client import WarehouseClient
    from voyage.agent.graph import build_graph
    from voyage.agent.state import initial_state

    pool = await asyncpg.create_pool(RO_DATABASE_URL)
    client = WarehouseClient(pool)
    graph = build_graph()
    cfg = {"configurable": {"client": client, "thread_id": "demo"}}

    state = await graph.ainvoke(initial_state("..."), config=cfg)
    if "__interrupt__" in state:
        state = await graph.ainvoke(Command(resume="..."), config=cfg)
"""

from __future__ import annotations

from typing import Any

from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, StateGraph

from voyage.agent.nodes.clarify import clarify
from voyage.agent.nodes.classify import classify_intent
from voyage.agent.nodes.draft import draft_sql
from voyage.agent.nodes.execute import execute_sql
from voyage.agent.nodes.failure import terminate_failure
from voyage.agent.nodes.interpret import interpret_result
from voyage.agent.nodes.refuse import refuse
from voyage.agent.nodes.retrieve import retrieve_context
from voyage.agent.nodes.validate import validate_sql
from voyage.agent.state import AgentState, IntentEnum

MAX_RETRIES = 3


# ---------------------------------------------------------------------------
# Conditional routers
# ---------------------------------------------------------------------------


def route_after_classify(state: AgentState) -> str:
    """Pick the next node based on the classified intent."""
    intent = state["intent"]
    if intent is None or intent.value == IntentEnum.DATA:
        return "retrieve_context"
    if intent.value == IntentEnum.AMBIGUOUS:
        return "clarify"
    return "refuse"


def route_after_validate(state: AgentState) -> str:
    """Pass to execute on success; retry draft until cap; else terminate."""
    val = state["validation_result"]
    if val is not None and val.ok:
        return "execute_sql"
    if state["retry_count"] < MAX_RETRIES:
        return "draft_sql"
    return "terminate_failure"


def route_after_execute(state: AgentState) -> str:
    """Interpret on success; retry draft until cap; else terminate."""
    if state["query_result"] is not None:
        return "interpret_result"
    if state["retry_count"] < MAX_RETRIES:
        return "draft_sql"
    return "terminate_failure"


# ---------------------------------------------------------------------------
# Graph builder
# ---------------------------------------------------------------------------


def build_graph(checkpointer: Any | None = None) -> Any:
    """Build and compile the Phase-5 agent graph.

    Args:
        checkpointer: Optional LangGraph checkpointer. Defaults to an
            in-memory ``MemorySaver`` so HITL ``interrupt`` works out of
            the box.

    Returns:
        A compiled LangGraph ``CompiledStateGraph`` ready for ``ainvoke``.
    """
    g: StateGraph[AgentState] = StateGraph(AgentState)

    g.add_node("classify_intent", classify_intent)
    g.add_node("clarify", clarify)
    g.add_node("refuse", refuse)
    g.add_node("retrieve_context", retrieve_context)
    g.add_node("draft_sql", draft_sql)
    g.add_node("validate_sql", validate_sql)
    g.add_node("execute_sql", execute_sql)
    g.add_node("interpret_result", interpret_result)
    g.add_node("terminate_failure", terminate_failure)

    g.set_entry_point("classify_intent")

    # classify → {retrieve | clarify | refuse}
    g.add_conditional_edges(
        "classify_intent",
        route_after_classify,
        {
            "retrieve_context": "retrieve_context",
            "clarify": "clarify",
            "refuse": "refuse",
        },
    )
    g.add_edge("clarify", "retrieve_context")
    g.add_edge("refuse", END)

    # retrieve → draft → validate
    g.add_edge("retrieve_context", "draft_sql")
    g.add_edge("draft_sql", "validate_sql")

    # validate → {execute | draft | terminate}
    g.add_conditional_edges(
        "validate_sql",
        route_after_validate,
        {
            "execute_sql": "execute_sql",
            "draft_sql": "draft_sql",
            "terminate_failure": "terminate_failure",
        },
    )

    # execute → {interpret | draft | terminate}
    g.add_conditional_edges(
        "execute_sql",
        route_after_execute,
        {
            "interpret_result": "interpret_result",
            "draft_sql": "draft_sql",
            "terminate_failure": "terminate_failure",
        },
    )

    g.add_edge("interpret_result", END)
    g.add_edge("terminate_failure", END)

    return g.compile(checkpointer=checkpointer if checkpointer is not None else MemorySaver())
