"""Ranking weights and malformed heading safeguards for PageIndex builder."""
from __future__ import annotations

from src.agents.indexer import (
    MAX_HEADING_LEVEL,
    _build_hierarchy,
    _heading_level,
    _normalize_heading_level,
    build_pageindex_from_ldus,
)
from src.models.pageindex import PageIndex, PageIndexSection


def test_heading_level_clamps_deep_numbering():
    assert _heading_level("1.2.3.4.5.6.7") == MAX_HEADING_LEVEL
    assert _heading_level("1.2.3.4.5.5.5.5") == MAX_HEADING_LEVEL


def test_heading_level_empty_or_too_long_returns_one():
    assert _heading_level("") == 1
    assert _heading_level("   ") == 1
    assert _heading_level("x" * 501) == 1


def test_normalize_heading_level_no_skip():
    assert _normalize_heading_level(5, 1) == 2
    assert _normalize_heading_level(3, 2) == 3
    assert _normalize_heading_level(1, 0) == 1


def test_build_hierarchy_handles_malformed_levels():
    flat = [
        PageIndexSection(section_id="s1", title="1. First", page_start=1, page_end=1),
        PageIndexSection(section_id="s2", title="1.2.3.4.5.6.7. Deep", page_start=1, page_end=1),
        PageIndexSection(section_id="s3", title="2. Back", page_start=2, page_end=2),
    ]
    root_list = _build_hierarchy(flat)
    assert len(root_list) >= 1
    for node in root_list:
        assert node.title
        assert node.page_end >= node.page_start


def test_build_hierarchy_empty_title_becomes_unnamed():
    flat = [
        PageIndexSection(section_id="s1", title="   ", page_start=1, page_end=1),
    ]
    root_list = _build_hierarchy(flat)
    assert len(root_list) == 1
    assert root_list[0].title == "(unnamed)"


def test_ranking_title_weight_dominates():
    index = PageIndex(
        doc_id="doc1",
        root=PageIndexSection(
            section_id="root",
            title="Doc",
            page_start=1,
            page_end=2,
            child_sections=[
                PageIndexSection(
                    section_id="sec-1",
                    title="Other",
                    page_start=1,
                    page_end=1,
                    summary="revenue profit",
                    key_entities=[],
                ),
                PageIndexSection(
                    section_id="sec-2",
                    title="revenue profit",
                    page_start=2,
                    page_end=2,
                    summary="Other content.",
                    key_entities=[],
                ),
            ],
        ),
    )
    ranked_default = index.top_sections_for_topic("revenue profit", k=1)
    ranked_title_heavy = index.top_sections_for_topic(
        "revenue profit", k=1, title_weight=3.0, summary_weight=0.5
    )
    assert ranked_title_heavy[0].section_id == "sec-2"
    assert ranked_default[0].section_id == "sec-2"


def test_ranking_key_entities_weight():
    index = PageIndex(
        doc_id="doc1",
        root=PageIndexSection(
            section_id="root",
            title="Doc",
            page_start=1,
            page_end=2,
            child_sections=[
                PageIndexSection(
                    section_id="sec-1",
                    title="Section A",
                    page_start=1,
                    page_end=1,
                    summary="General overview.",
                    key_entities=["EBITDA", "revenue", "margin"],
                ),
                PageIndexSection(
                    section_id="sec-2",
                    title="revenue",
                    page_start=2,
                    page_end=2,
                    summary="Other.",
                    key_entities=[],
                ),
            ],
        ),
    )
    ranked_entity_heavy = index.top_sections_for_topic(
        "revenue EBITDA", k=1, title_weight=1.0, key_entities_weight=2.0, summary_weight=0.5
    )
    assert ranked_entity_heavy[0].section_id == "sec-1"


def test_build_pageindex_from_ldus_produces_valid_hierarchy():
    chunks = [
        {"parent_section": "1. Intro", "page_refs": [1], "text": "Hello"},
        {"parent_section": "1.1. Sub", "page_refs": [1], "text": "World"},
        {"parent_section": "2. Next", "page_refs": [2], "text": "End"},
    ]
    index = build_pageindex_from_ldus("doc1", "Doc", chunks)
    assert index.root
    assert index.root.page_start >= 1
    assert index.root.page_end >= index.root.page_start
    all_secs = index._all_sections(index.root)
    for s in all_secs:
        assert s.title
        assert s.page_end >= s.page_start
