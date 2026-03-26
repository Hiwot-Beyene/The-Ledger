import {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
  type ReactNode,
} from "react";
import {
  APPLICANT_PROFILES as SEED_APPLICANTS,
  PIPELINE_APPLICATIONS as SEED_PIPELINE,
  type ApplicantProfile,
  type PipelineApplication,
} from "@/data/ledgerSeed";
import { isLiveApiMode, resolveApiBase } from "@/lib/apiBase";

type Source = "api" | "seed";

export type LedgerDataState = {
  applicants: ApplicantProfile[];
  pipeline: PipelineApplication[];
  source: Source;
  loading: boolean;
  error: string | null;
  refetch: () => Promise<void>;
};

const Ctx = createContext<LedgerDataState | null>(null);

async function loadFromApi(
  apiBase: string,
): Promise<{ applicants: ApplicantProfile[]; pipeline: PipelineApplication[] }> {
  const [ra, rp] = await Promise.all([
    fetch(`${apiBase}/api/registry/companies`),
    fetch(`${apiBase}/api/applications/pipeline`),
  ]);
  if (!ra.ok) throw new Error(`registry ${ra.status}: ${await ra.text()}`);
  if (!rp.ok) throw new Error(`pipeline ${rp.status}: ${await rp.text()}`);
  const applicants = (await ra.json()) as ApplicantProfile[];
  const body = (await rp.json()) as { applications: PipelineApplication[] };
  return { applicants, pipeline: body.applications ?? [] };
}

export function LedgerDataProvider({ children }: { children: ReactNode }) {
  const [applicants, setApplicants] = useState<ApplicantProfile[]>([]);
  const [pipeline, setPipeline] = useState<PipelineApplication[]>([]);
  const [source, setSource] = useState<Source>("seed");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const refetch = useCallback(async () => {
    if (!isLiveApiMode()) {
      setApplicants(SEED_APPLICANTS);
      setPipeline(SEED_PIPELINE);
      setSource("seed");
      setError(null);
      setLoading(false);
      return;
    }
    const apiBase = resolveApiBase();
    setLoading(true);
    setError(null);
    try {
      const d = await loadFromApi(apiBase);
      setApplicants(d.applicants);
      setPipeline(d.pipeline);
      setSource("api");
    } catch (e) {
      setApplicants([]);
      setPipeline([]);
      setSource("api");
      setError(String(e));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void refetch();
  }, [refetch]);

  const value = useMemo(
    () => ({ applicants, pipeline, source, loading, error, refetch }),
    [applicants, pipeline, source, loading, error, refetch],
  );

  return <Ctx.Provider value={value}>{children}</Ctx.Provider>;
}

export function useLedgerData(): LedgerDataState {
  const v = useContext(Ctx);
  if (!v) throw new Error("useLedgerData must be used within LedgerDataProvider");
  return v;
}

export function getApiBase(): string {
  return resolveApiBase();
}
