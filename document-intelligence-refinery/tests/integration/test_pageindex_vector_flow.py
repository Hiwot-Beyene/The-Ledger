from src.agents.indexer import build_pageindex
from src.services.vector_store import InMemoryVectorStore


def test_vector_ingestion_and_section_first_retrieval():
    store = InMemoryVectorStore()
    store.ingest(
        doc_id="doc-1",
        chunks=[
            {"id": "ldu-1", "text": "Revenue increased in Q4"},
            {"id": "ldu-2", "text": "Operational risk remained stable"},
        ],
    )

    pageindex = build_pageindex(
        doc_id="doc-1",
        document_name="report.pdf",
        pages=[1, 2],
        headings=["Revenue Analysis", "Risk Factors"],
    )

    top_sections = pageindex.top_sections_for_topic("revenue", k=1)
    hits = store.semantic_search(doc_ids=["doc-1"], query="revenue", k=1)

    assert top_sections[0].title == "Revenue Analysis"
    assert hits[0].chunk_id == "ldu-1"
