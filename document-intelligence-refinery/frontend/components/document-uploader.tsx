"use client";

import React, { useState } from "react";

type Props = {
  onUpload: (file: File) => Promise<void>;
};

export function DocumentUploader({ onUpload }: Props) {
  const [status, setStatus] = useState("idle");

  return (
    <section className="mb-5">
      <h2 className="text-sm font-semibold text-slate-800">Upload</h2>
      <p className="mt-1 mb-2 text-xs text-slate-500">Add a PDF to start triage and extraction.</p>
      <input
        className="block w-full rounded-lg border border-slate-200 bg-slate-50/80 px-3 py-2.5 text-sm file:mr-3 file:rounded-md file:border-0 file:bg-slate-900 file:px-3 file:py-1.5 file:text-xs file:font-semibold file:text-white file:transition hover:file:bg-slate-800"
        aria-label="Choose PDF file"
        type="file"
        accept="application/pdf"
        onChange={async (e) => {
          const file = e.target.files?.[0];
          if (!file) return;
          setStatus("uploading");
          await onUpload(file);
          setStatus("done");
        }}
      />
      <p className="mt-2 text-[11px] font-medium uppercase tracking-wide text-slate-400">State: {status}</p>
    </section>
  );
}
