"""Adaptive tool selection: query type classification and conditional routing."""
from __future__ import annotations

import pytest

from src.agents.query_agent import (
    QUERY_TYPE_EXPLORATORY,
    QUERY_TYPE_FACTUAL,
    QUERY_TYPE_SEMANTIC,
    classify_query_type,
    _route_after_semantic,
)


def test_classify_factual():
    assert classify_query_type("What is the total comprehensive income?") == QUERY_TYPE_FACTUAL
    assert classify_query_type("Revenue for the year") == QUERY_TYPE_FACTUAL
    assert classify_query_type("Proclamation number 1186") == QUERY_TYPE_FACTUAL
    assert classify_query_type("Give me the exact value of EBITDA") == QUERY_TYPE_FACTUAL


def test_classify_exploratory():
    assert classify_query_type("Summarize this document") == QUERY_TYPE_EXPLORATORY
    assert classify_query_type("Give me an overview of the report") == QUERY_TYPE_EXPLORATORY
    assert classify_query_type("List the main sections") == QUERY_TYPE_EXPLORATORY
    assert classify_query_type("What are the main topics?") == QUERY_TYPE_EXPLORATORY


def test_classify_semantic():
    assert classify_query_type("How does the tax system work?") == QUERY_TYPE_SEMANTIC
    assert classify_query_type("") == QUERY_TYPE_SEMANTIC


def test_route_after_semantic_exploratory_skips_structured():
    state = {
        "tool_sequence": ["pageindex_navigate", "semantic_search", "structured_query"],
        "query_type": QUERY_TYPE_EXPLORATORY,
    }
    assert _route_after_semantic(state) == "prepare_synthesize"


def test_route_after_semantic_factual_runs_structured():
    state = {
        "tool_sequence": ["pageindex_navigate", "semantic_search", "structured_query"],
        "query_type": QUERY_TYPE_FACTUAL,
    }
    assert _route_after_semantic(state) == "structured_query"


def test_route_after_semantic_no_structured_in_sequence():
    state = {
        "tool_sequence": ["pageindex_navigate", "semantic_search"],
        "query_type": QUERY_TYPE_EXPLORATORY,
    }
    assert _route_after_semantic(state) == "prepare_synthesize"
