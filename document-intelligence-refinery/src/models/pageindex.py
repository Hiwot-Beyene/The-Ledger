from __future__ import annotations

from pydantic import BaseModel, Field


class PageIndexSection(BaseModel):
    section_id: str = Field(min_length=1)
    title: str = Field(min_length=1)
    page_start: int = Field(ge=1)
    page_end: int = Field(ge=1)
    summary: str = ""
    key_entities: list[str] = Field(default_factory=list)
    data_types_present: list[str] = Field(default_factory=list)
    child_sections: list["PageIndexSection"] = Field(default_factory=list)


class PageIndex(BaseModel):
    doc_id: str = Field(min_length=1)
    root: PageIndexSection

    def _all_sections(self, section: PageIndexSection | None = None) -> list[PageIndexSection]:
        section = section or self.root
        out: list[PageIndexSection] = [section]
        for child in section.child_sections or []:
            out.extend(self._all_sections(child))
        return out

    def top_sections_for_topic(
        self,
        topic: str,
        k: int = 3,
        *,
        title_weight: float = 2.0,
        key_entities_weight: float = 1.5,
        summary_weight: float = 1.0,
    ) -> list[PageIndexSection]:
        topic_tokens = set((topic or "").lower().split())
        if not topic_tokens:
            candidates = self._all_sections(self.root)[1:]
            return sorted(candidates, key=lambda s: s.page_start)[:k]

        candidates = self._all_sections(self.root)[1:]

        def score(section: PageIndexSection) -> float:
            title_tokens = set((section.title or "").lower().split())
            summary_tokens = set((section.summary or "").lower().split())
            entity_tokens: set[str] = set()
            for e in section.key_entities or []:
                entity_tokens.update((e or "").lower().split())
            title_hits = len(topic_tokens.intersection(title_tokens))
            summary_hits = len(topic_tokens.intersection(summary_tokens))
            entity_hits = len(topic_tokens.intersection(entity_tokens))
            return (
                title_hits * title_weight
                + summary_hits * summary_weight
                + entity_hits * key_entities_weight
            )

        ranked = sorted(candidates, key=lambda s: (-score(s), s.page_start))
        return ranked[:k]
