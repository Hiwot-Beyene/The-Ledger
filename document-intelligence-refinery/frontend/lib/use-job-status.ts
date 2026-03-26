"use client";

import { useEffect, useState } from "react";

import { fetchJobStatus, type JobStatus } from "./api-client";

export function useJobStatus(docId: string | null, intervalMs = 2000) {
  const [status, setStatus] = useState<JobStatus | null>(null);

  useEffect(() => {
    if (!docId) {
      setStatus(null);
      return;
    }

    let active = true;
    const tick = async () => {
      const current = await fetchJobStatus(docId);
      if (active) {
        setStatus(current);
      }
    };

    void tick();
    const timer = window.setInterval(() => {
      void tick();
    }, intervalMs);

    return () => {
      active = false;
      window.clearInterval(timer);
    };
  }, [docId, intervalMs]);

  return status;
}
