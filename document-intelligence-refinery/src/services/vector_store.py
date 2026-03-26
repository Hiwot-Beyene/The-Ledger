"""
Vector store implementations. The project uses ChromaDB as specified in the docs.
InMemoryVectorStore remains for tests and fallback.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Union


@dataclass
class VectorRecord:
    doc_id: str
    chunk_id: str
    text: str
    page_number: int | None = None
    content_hash: str | None = None
    document_title: str | None = None
    chunk_type: str | None = None
    parent_section: str | None = None


class BaseVectorStore(ABC):
    @abstractmethod
    def ingest(self, doc_id: str, chunks: list[dict], document_title: str | None = None) -> None:
        pass

    @abstractmethod
    def semantic_search(self, doc_ids: list[str], query: str, k: int = 3) -> list[VectorRecord]:
        pass

    def delete_by_doc_id(self, doc_id: str) -> None:
        pass

    def get_chunk_metadata(self, doc_id: str) -> list[dict]:
        """Return list of {chunk_id, content_hash} for audit/verification."""
        return []


class InMemoryVectorStore(BaseVectorStore):
    def __init__(self) -> None:
        self.records: list[VectorRecord] = []

    def ingest(self, doc_id: str, chunks: list[dict], document_title: str | None = None) -> None:
        for chunk in chunks:
            page_refs = chunk.get("page_refs") or []
            page_number = page_refs[0] if page_refs else 1
            self.records.append(
                VectorRecord(
                    doc_id=doc_id,
                    chunk_id=str(chunk.get("id", "")),
                    text=str(chunk.get("text", "")),
                    page_number=page_number,
                    content_hash=chunk.get("content_hash"),
                    document_title=document_title,
                    chunk_type=chunk.get("chunk_type"),
                    parent_section=chunk.get("parent_section"),
                )
            )

    def semantic_search(self, doc_ids: list[str], query: str, k: int = 3) -> list[VectorRecord]:
        query_tokens = set((query or "").lower().split())

        def score(record: VectorRecord) -> int:
            return len(query_tokens.intersection(set(record.text.lower().split())))

        filtered = [r for r in self.records if not doc_ids or r.doc_id in doc_ids]
        ranked = sorted(filtered, key=score, reverse=True)
        return ranked[:k]

    def count(self) -> int:
        return len(self.records)

    def get_all(self, doc_id: str | None = None, limit: int = 100) -> list[dict]:
        filtered = [r for r in self.records if doc_id is None or r.doc_id == doc_id]
        return [{"doc_id": r.doc_id, "chunk_id": r.chunk_id, "text": (r.text or "")[:200], "document_title": r.document_title} for r in filtered[:limit]]

    def get_chunk_metadata(self, doc_id: str) -> list[dict]:
        out = []
        for r in self.records:
            if r.doc_id != doc_id:
                continue
            out.append({"chunk_id": r.chunk_id, "content_hash": r.content_hash or ""})
        return out

    def delete_by_doc_id(self, doc_id: str) -> None:
        self.records = [r for r in self.records if r.doc_id != doc_id]


def _get_embedding_function():
    from chromadb.utils import embedding_functions
    return embedding_functions.SentenceTransformerEmbeddingFunction(model_name="all-MiniLM-L6-v2")


class ChromaVectorStore(BaseVectorStore):
    """ChromaDB-backed vector store. Persists under persist_dir. Uses local sentence-transformers embeddings."""

    COLLECTION_NAME = "refinery_ldus"

    def __init__(self, persist_dir: str | Path = ".refinery/chroma") -> None:
        self.persist_dir = Path(persist_dir)
        self.persist_dir.mkdir(parents=True, exist_ok=True)
        import chromadb
        self._client = chromadb.PersistentClient(path=str(self.persist_dir))
        self._ef = _get_embedding_function()
        self._collection = self._client.get_or_create_collection(
            name=self.COLLECTION_NAME,
            embedding_function=self._ef,
            metadata={"hnsw:space": "cosine"},
        )

    def ingest(self, doc_id: str, chunks: list[dict], document_title: str | None = None) -> None:
        if not chunks:
            return
        self._collection.delete(where={"doc_id": doc_id})
        ids = []
        documents = []
        metadatas = []
        title = (document_title or "")[:512]
        for chunk in chunks:
            cid = str(chunk.get("id", ""))
            text = str(chunk.get("text", ""))
            page_refs = chunk.get("page_refs") or []
            page_number = page_refs[0] if page_refs else 1
            content_hash = chunk.get("content_hash") or ""
            chunk_type = chunk.get("chunk_type") or ""
            parent_section = (chunk.get("parent_section") or "")[:500]
            ids.append(f"{doc_id}_{cid}")
            documents.append(text)
            metadatas.append({
                "doc_id": doc_id,
                "chunk_id": cid,
                "page_number": page_number,
                "content_hash": content_hash[:64] if content_hash else "",
                "document_title": title,
                "chunk_type": chunk_type[:64] if chunk_type else "",
                "parent_section": parent_section,
            })
        self._collection.add(ids=ids, documents=documents, metadatas=metadatas)

    def semantic_search(self, doc_ids: list[str], query: str, k: int = 3) -> list[VectorRecord]:
        n = self._collection.count()
        if n == 0:
            return []
        where = None
        if doc_ids:
            where = {"doc_id": {"$in": doc_ids}}
        result = self._collection.query(
            query_texts=[query],
            n_results=min(k, n),
            where=where,
        )
        records = []
        if result and result.get("ids") and result["ids"][0]:
            ids_list = result["ids"][0]
            docs_list = (result.get("documents") or [None])[0] or []
            metas_list = (result.get("metadatas") or [None])[0] or []
            for i, cid in enumerate(ids_list):
                meta = metas_list[i] if i < len(metas_list) else {}
                doc_text = docs_list[i] if i < len(docs_list) else ""
                records.append(
                    VectorRecord(
                        doc_id=meta.get("doc_id", ""),
                        chunk_id=meta.get("chunk_id", cid),
                        text=doc_text,
                        page_number=meta.get("page_number") or 1,
                        content_hash=meta.get("content_hash"),
                        document_title=meta.get("document_title"),
                        chunk_type=meta.get("chunk_type"),
                        parent_section=meta.get("parent_section"),
                    )
                )
        return records

    def count(self) -> int:
        return self._collection.count()

    def get_all(self, doc_id: str | None = None, limit: int = 100) -> list[dict]:
        """Preview: fetch documents from the collection. Optional filter by doc_id."""
        where = {"doc_id": doc_id} if doc_id else None
        result = self._collection.get(where=where, limit=limit, include=["documents", "metadatas"])
        out = []
        for i, meta in enumerate(result.get("metadatas") or []):
            doc = (result.get("documents") or [])[i] if i < len(result.get("documents") or []) else ""
            out.append({
                "doc_id": meta.get("doc_id"),
                "chunk_id": meta.get("chunk_id"),
                "text": (doc or "")[:200],
                "document_title": meta.get("document_title"),
            })
        return out

    def get_chunk_metadata(self, doc_id: str) -> list[dict]:
        result = self._collection.get(where={"doc_id": doc_id}, include=["metadatas"])
        out = []
        for meta in (result.get("metadatas") or []):
            out.append({"chunk_id": meta.get("chunk_id", ""), "content_hash": meta.get("content_hash") or ""})
        return out

    def delete_by_doc_id(self, doc_id: str) -> None:
        try:
            self._collection.delete(where={"doc_id": doc_id})
        except Exception:
            pass


def get_vector_store(persist_dir: str | Path = ".refinery/chroma", use_chroma: bool = True) -> Union[ChromaVectorStore, InMemoryVectorStore]:
    """Return ChromaVectorStore if use_chroma else InMemoryVectorStore."""
    if use_chroma:
        return ChromaVectorStore(persist_dir=persist_dir)
    return InMemoryVectorStore()
