from src.agents.indexer import _parse_enrichment_response, section_texts_from_ldus
from src.models.pageindex import PageIndex, PageIndexSection


def test_parse_enrichment_response():
    text = """Summary: This section covers revenue growth and EBITDA margins in Q3.

Key entities: Acme Corp, $4.2B, EBITDA, Q3 2024

Data types: tables, narrative"""
    summary, entities, types = _parse_enrichment_response(text)
    assert "revenue" in summary.lower()
    assert "Acme Corp" in entities or "4.2B" in entities
    assert "tables" in types or "narrative" in types


def test_parse_enrichment_response_empty():
    summary, entities, types = _parse_enrichment_response("")
    assert summary == ""
    assert entities == []
    assert types == []


def test_section_texts_from_ldus():
    index = PageIndex(
        doc_id="doc-1",
        root=PageIndexSection(
            section_id="root-doc-1",
            title="Doc",
            page_start=1,
            page_end=2,
            child_sections=[
                PageIndexSection(section_id="sec-1", title="S1", page_start=1, page_end=1),
                PageIndexSection(section_id="sec-2", title="S2", page_start=2, page_end=2),
            ],
        ),
    )
    chunks = [
        {"id": "c1", "text": "Revenue increased.", "page_refs": [1]},
        {"id": "c2", "text": "Risk factors.", "page_refs": [2]},
    ]
    section_texts = section_texts_from_ldus(index, chunks)
    assert "sec-1" in section_texts
    assert "Revenue" in section_texts["sec-1"]
    assert "sec-2" in section_texts
    assert "Risk" in section_texts["sec-2"]
