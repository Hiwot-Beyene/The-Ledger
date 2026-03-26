"use client";

import React, { useState } from "react";

export type DocumentListItem = {
  doc_id: string;
  document_name: string;
  status: string;
};

type Props = {
  documents: DocumentListItem[];
  selectedDocId: string | null;
  onSelect: (docId: string) => void;
  onDelete: (docId: string, documentName: string) => void;
};

const statusStyles: Record<string, string> = {
  ready: "bg-emerald-50 text-emerald-700 border-emerald-200/80",
  processing: "bg-sky-50 text-sky-700 border-sky-200/80",
  uploaded: "bg-amber-50 text-amber-700 border-amber-200/80",
  queued: "bg-slate-100 text-slate-600 border-slate-200/80",
  error: "bg-red-50 text-red-700 border-red-200/80",
  approval_required: "bg-amber-50 text-amber-700 border-amber-200/80",
};

export function DocumentList({ documents, selectedDocId, onSelect, onDelete }: Props) {
  const [deleteConfirm, setDeleteConfirm] = useState<{ doc_id: string; document_name: string } | null>(null);

  const handleDeleteClick = (e: React.MouseEvent, item: DocumentListItem) => {
    e.stopPropagation();
    setDeleteConfirm({ doc_id: item.doc_id, document_name: item.document_name });
  };

  const handleConfirmDelete = () => {
    if (!deleteConfirm) return;
    onDelete(deleteConfirm.doc_id, deleteConfirm.document_name);
    setDeleteConfirm(null);
  };

  return (
    <>
      <section className="flex flex-1 flex-col">
        {documents.length === 0 ? (
          <div className="flex flex-1 flex-col items-center justify-center rounded-lg border border-dashed border-slate-200 bg-slate-50/50 py-12 text-center">
            <p className="text-sm font-medium text-slate-500">No documents yet</p>
            <p className="mt-1 text-xs text-slate-400">Upload a PDF above to get started.</p>
          </div>
        ) : (
          <ul className="space-y-2">
            {documents.map((item) => {
              const statusClass = statusStyles[item.status] ?? "bg-slate-100 text-slate-600 border-slate-200/80";
              const isSelected = selectedDocId === item.doc_id;
              return (
                <li key={item.doc_id}>
                  <div
                    className={`flex items-center gap-2 rounded-lg border px-3 py-2.5 transition-all ${
                      isSelected
                        ? "border-slate-900 bg-slate-900 text-white shadow-sm"
                        : "border-slate-200 bg-white hover:border-slate-300 hover:bg-slate-50"
                    }`}
                  >
                    <button
                      type="button"
                      className="min-w-0 flex-1 text-left"
                      onClick={() => onSelect(item.doc_id)}
                    >
                      <div className="flex items-center justify-between gap-2">
                        <span className="truncate text-sm font-medium">{item.document_name}</span>
                        <span
                          className={`shrink-0 rounded-md border px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide ${
                            isSelected ? "border-white/30 bg-white/20" : statusClass
                          }`}
                        >
                          {item.status}
                        </span>
                      </div>
                    </button>
                    <button
                      type="button"
                      aria-label={`Delete ${item.document_name}`}
                      className={`shrink-0 rounded p-1.5 transition-colors ${
                        isSelected
                          ? "text-white hover:bg-white/20"
                          : "text-slate-400 hover:bg-red-50 hover:text-red-600"
                      }`}
                      onClick={(e) => handleDeleteClick(e, item)}
                    >
                      <svg className="h-4 w-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" />
                      </svg>
                    </button>
                  </div>
                </li>
              );
            })}
          </ul>
        )}
      </section>

      {deleteConfirm && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4" onClick={() => setDeleteConfirm(null)}>
          <div
            className="w-full max-w-sm rounded-xl border border-slate-200 bg-white p-5 shadow-xl"
            onClick={(e) => e.stopPropagation()}
          >
            <h3 className="font-semibold text-slate-900">Delete document?</h3>
            <p className="mt-2 text-sm text-slate-600">
              Remove &quot;{deleteConfirm.document_name}&quot; and all associated data (chunks, index, facts). This cannot be undone.
            </p>
            <div className="mt-5 flex justify-end gap-2">
              <button
                type="button"
                className="rounded-lg border border-slate-200 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-50"
                onClick={() => setDeleteConfirm(null)}
              >
                Cancel
              </button>
              <button
                type="button"
                className="rounded-lg bg-red-600 px-4 py-2 text-sm font-medium text-white hover:bg-red-700"
                onClick={handleConfirmDelete}
              >
                Delete
              </button>
            </div>
          </div>
        </div>
      )}
    </>
  );
}
