from src.models.pageindex import PageIndex, PageIndexSection


def test_pageindex_topic_ranking_prefers_matching_titles():
    index = PageIndex(
        doc_id="doc-1",
        root=PageIndexSection(
            section_id="root",
            title="Annual Report",
            page_start=1,
            page_end=3,
            child_sections=[
                PageIndexSection(section_id="sec-1", title="Revenue Analysis", page_start=1, page_end=1),
                PageIndexSection(section_id="sec-2", title="Risk Factors", page_start=2, page_end=2),
            ],
        ),
    )

    ranked = index.top_sections_for_topic("revenue trend", k=1)
    assert ranked[0].section_id == "sec-1"


def test_pageindex_ranks_nested_descendants():
    nested = PageIndexSection(
        section_id="sec-nested",
        title="Capital expenditure projections",
        page_start=2,
        page_end=2,
    )
    index = PageIndex(
        doc_id="doc-1",
        root=PageIndexSection(
            section_id="root",
            title="Report",
            page_start=1,
            page_end=3,
            child_sections=[
                PageIndexSection(section_id="sec-1", title="Overview", page_start=1, page_end=1),
                PageIndexSection(
                    section_id="sec-2",
                    title="Risk Factors",
                    page_start=2,
                    page_end=2,
                    child_sections=[nested],
                ),
            ],
        ),
    )
    ranked = index.top_sections_for_topic("capital expenditure", k=2)
    assert "sec-nested" in [s.section_id for s in ranked]
    assert ranked[0].section_id == "sec-nested"


def test_pageindex_ranking_uses_summary_and_key_entities():
    index = PageIndex(
        doc_id="doc-1",
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
                    summary="Quarterly revenue and profit margins.",
                    key_entities=["revenue", "EBITDA"],
                ),
                PageIndexSection(
                    section_id="sec-2",
                    title="Section B",
                    page_start=2,
                    page_end=2,
                    summary="Risk factors.",
                    key_entities=[],
                ),
            ],
        ),
    )
    ranked = index.top_sections_for_topic("revenue profit", k=1)
    assert ranked[0].section_id == "sec-1"
