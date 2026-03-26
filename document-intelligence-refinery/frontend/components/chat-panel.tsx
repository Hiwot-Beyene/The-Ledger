"use client";

import React, { useState } from "react";
import ReactMarkdown from "react-markdown";
import remarkGfm from "remark-gfm";

import { ProvenanceCard, type ProvenanceCitation } from "./provenance-card";

type Props = {
  onAsk: (query: string) => Promise<{ answer: string; provenance: ProvenanceCitation[] }>;
};

const markdownClasses = {
  p: "mb-2 last:mb-0",
  table: "min-w-full border-collapse border border-slate-300 text-sm my-3",
  thead: "bg-slate-100",
  th: "border border-slate-300 px-3 py-2 text-left font-semibold",
  td: "border border-slate-300 px-3 py-2",
  tr: "border-b border-slate-200",
  ul: "list-disc list-inside mb-2 space-y-0.5",
  ol: "list-decimal list-inside mb-2 space-y-0.5",
  li: "text-slate-800",
  strong: "font-semibold",
  code: "bg-slate-100 px-1 py-0.5 rounded text-sm font-mono",
  pre: "bg-slate-100 p-3 rounded-lg overflow-x-auto text-sm my-2",
  h1: "text-lg font-bold mt-3 mb-1",
  h2: "text-base font-bold mt-3 mb-1",
  h3: "text-sm font-bold mt-2 mb-1",
  blockquote: "border-l-4 border-slate-300 pl-3 my-2 text-slate-600",
};

export function ChatPanel({ onAsk }: Props) {
  const [answer, setAnswer] = useState("");
  const [provenance, setProvenance] = useState<ProvenanceCitation[]>([]);
  const [query, setQuery] = useState("");
  const [isAnswering, setIsAnswering] = useState(false);

  return (
    <section>
      <h2 className="text-sm font-semibold text-slate-800">Chat</h2>
      <p className="mt-1 mb-3 text-xs text-slate-500">Ask a question and inspect source-grounded evidence.</p>
      <div className="rounded-lg border border-slate-200 bg-white p-3">
        <div className="flex flex-col gap-2 sm:flex-row">
          <input
            className="w-full rounded-lg border border-slate-200 px-3 py-2 text-sm placeholder:text-slate-400 focus:border-slate-400 focus:outline-none focus:ring-1 focus:ring-slate-400"
            aria-label="Question"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="Ask about key figures, clauses, tables..."
            disabled={isAnswering}
          />
          <button
            type="button"
            className="rounded-lg bg-slate-900 px-4 py-2 text-sm font-semibold text-white transition hover:bg-slate-800 focus:outline-none focus:ring-2 focus:ring-slate-500 focus:ring-offset-2 disabled:opacity-70 disabled:cursor-not-allowed"
            disabled={isAnswering}
            onClick={async () => {
              if (!query.trim() || isAnswering) return;
              setIsAnswering(true);
              setAnswer("");
              setProvenance([]);
              try {
                const result = await onAsk(query);
                setAnswer(result?.answer ?? "");
                setProvenance(Array.isArray(result?.provenance) ? result.provenance : []);
              } catch (e) {
                setAnswer(e instanceof Error ? e.message : "Request failed.");
                setProvenance([]);
              } finally {
                setIsAnswering(false);
              }
            }}
          >
            {isAnswering ? "Answering…" : "Ask"}
          </button>
        </div>
      </div>

      <div className="mt-4 rounded-lg border border-slate-200 bg-slate-50/60 p-4">
        <h3 className="text-[11px] font-semibold uppercase tracking-wide text-slate-500">Answer</h3>
        <div className="mt-2 min-h-[2rem] max-h-[50vh] overflow-y-auto break-words text-sm leading-relaxed text-slate-800">
          {isAnswering ? (
            "Answering…"
          ) : answer ? (
            <ReactMarkdown
              remarkPlugins={[remarkGfm]}
              components={{
                p: ({ children }) => <p className={markdownClasses.p}>{children}</p>,
                table: ({ children }) => (
                  <div className="my-3 w-full overflow-x-auto">
                    <table className={markdownClasses.table}>{children}</table>
                  </div>
                ),
                thead: ({ children }) => <thead className={markdownClasses.thead}>{children}</thead>,
                th: ({ children }) => <th className={markdownClasses.th}>{children}</th>,
                td: ({ children }) => <td className={markdownClasses.td}>{children}</td>,
                tr: ({ children }) => <tr className={markdownClasses.tr}>{children}</tr>,
                ul: ({ children }) => <ul className={markdownClasses.ul}>{children}</ul>,
                ol: ({ children }) => <ol className={markdownClasses.ol}>{children}</ol>,
                li: ({ children }) => <li className={markdownClasses.li}>{children}</li>,
                strong: ({ children }) => <strong className={markdownClasses.strong}>{children}</strong>,
                code: ({ children }) => <code className={markdownClasses.code}>{children}</code>,
                pre: ({ children }) => <pre className={markdownClasses.pre}>{children}</pre>,
                h1: ({ children }) => <h1 className={markdownClasses.h1}>{children}</h1>,
                h2: ({ children }) => <h2 className={markdownClasses.h2}>{children}</h2>,
                h3: ({ children }) => <h3 className={markdownClasses.h3}>{children}</h3>,
                blockquote: ({ children }) => <blockquote className={markdownClasses.blockquote}>{children}</blockquote>,
              }}
            >
              {answer}
            </ReactMarkdown>
          ) : (
            "No answer yet. Run a query to see the response."
          )}
        </div>
      </div>

      <div className="mt-4">
        <h3 className="mb-2 text-[11px] font-semibold uppercase tracking-wide text-slate-500">Provenance</h3>
        {provenance.length === 0 ? (
          <p className="text-sm text-slate-500">No citations yet.</p>
        ) : (
          <div className="max-h-[40vh] space-y-2 overflow-y-auto">
            {provenance.map((item, index) => (
              <ProvenanceCard key={`${index}-${item.content_hash}-${item.page_number}`} citation={item} />
            ))}
          </div>
        )}
      </div>
    </section>
  );
}
