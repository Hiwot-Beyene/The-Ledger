"use client";

import React from "react";

export type ProvenanceCitation = {
  document_name: string;
  page_number: number;
  bbox: [number, number, number, number];
  content_hash: string;
};

export function ProvenanceCard({ citation }: { citation: ProvenanceCitation }) {
  const bboxStr = citation.bbox && citation.bbox.length >= 4
    ? `[${citation.bbox[0].toFixed(0)}, ${citation.bbox[1].toFixed(0)}, ${citation.bbox[2].toFixed(0)}, ${citation.bbox[3].toFixed(0)}]`
    : null;
  return (
    <article className="rounded-lg border border-slate-200 bg-slate-50/80 p-3">
      <div className="font-medium text-slate-900">{citation.document_name}</div>
      <div className="mt-1 text-xs font-medium uppercase tracking-wide text-slate-500">Page {citation.page_number}</div>
      {bboxStr && <div className="mt-0.5 font-mono text-xs text-slate-500">Bbox {bboxStr}</div>}
      <div className="mt-1 font-mono text-xs text-slate-600">Hash {citation.content_hash}</div>
    </article>
  );
}
