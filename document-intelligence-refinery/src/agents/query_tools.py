from __future__ import annotations

from src.models.pageindex import PageIndex
from src.services.fact_table import structured_query as run_structured_query
from src.services.fact_table import structured_query_multi as run_structured_query_multi
from src.services.vector_store import BaseVectorStore


def pageindex_navigate(pageindex: PageIndex, topic: str, k: int = 3) -> list[dict]:
    sections = pageindex.top_sections_for_topic(topic, k=k)
    return [section.model_dump() for section in sections]


def semantic_search(vector_store: BaseVectorStore, doc_ids: list[str], query: str, k: int = 5) -> list[dict]:
    records = vector_store.semantic_search(doc_ids=doc_ids, query=query, k=k)
    return [record.__dict__ for record in records]


def structured_query(db_path: str, doc_ids: list[str], key: str) -> list[dict]:
    return run_structured_query(db_path=db_path, doc_ids=doc_ids, key=key)


def structured_query_multi(db_path: str, doc_ids: list[str], keys: list[str]) -> list[dict]:
    return run_structured_query_multi(db_path=db_path, doc_ids=doc_ids, keys=keys)
