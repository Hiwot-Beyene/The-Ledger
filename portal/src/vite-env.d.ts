/// <reference types="vite/client" />

declare module "@repo-data/applicant_profiles.json" {
  const value: {
    company_id: string;
    name: string;
    industry: string;
    jurisdiction: string;
    legal_type: string;
    trajectory: string;
    risk_segment: string;
    compliance_flags: unknown[];
  }[];
  export default value;
}

declare module "@repo-data/seed_events.jsonl?raw" {
  const raw: string;
  export default raw;
}
