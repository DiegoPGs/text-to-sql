"""LangGraph agent graph — Phase 4 happy path.

Wires all nodes into a linear StateGraph:

    classify_intent → retrieve_context → draft_sql
        → validate_sql → execute_sql → interpret_result → END

Conditional edges (clarify / refuse / retry) are added in Phase 5.

Usage::

    import asyncpg
    from voyage.agent.client import WarehouseClient
    from voyage.agent.graph import build_graph
    from voyage.agent.state import initial_state

    pool = await asyncpg.create_pool(RO_DATABASE_URL)
    client = WarehouseClient(pool)
    graph = build_graph()
    result = await graph.ainvoke(
        initial_state("What is total revenue last month?"),
        config={"configurable": {"client": client}},
    )
"""

from __future__ import annotations

from typing import Any

from langgraph.graph import END, StateGraph

from voyage.agent.nodes.classify import classify_intent
from voyage.agent.nodes.draft import draft_sql
from voyage.agent.nodes.execute import execute_sql
from voyage.agent.nodes.interpret import interpret_result
from voyage.agent.nodes.retrieve import retrieve_context
from voyage.agent.nodes.validate import validate_sql
from voyage.agent.state import AgentState


def build_graph() -> Any:
    """Build and compile the Phase-4 happy-path agent graph.

    Returns:
        A compiled LangGraph ``CompiledStateGraph`` ready for ``ainvoke``.
    """
    g: StateGraph[AgentState] = StateGraph(AgentState)

    g.add_node("classify_intent", classify_intent)
    g.add_node("retrieve_context", retrieve_context)
    g.add_node("draft_sql", draft_sql)
    g.add_node("validate_sql", validate_sql)
    g.add_node("execute_sql", execute_sql)
    g.add_node("interpret_result", interpret_result)

    # Happy-path linear flow — conditional edges added in Phase 5.
    g.set_entry_point("classify_intent")
    g.add_edge("classify_intent", "retrieve_context")
    g.add_edge("retrieve_context", "draft_sql")
    g.add_edge("draft_sql", "validate_sql")
    g.add_edge("validate_sql", "execute_sql")
    g.add_edge("execute_sql", "interpret_result")
    g.add_edge("interpret_result", END)

    return g.compile()
