import React from "react";
import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { DocumentList } from "../../components/document-list";
import { DocumentUploader } from "../../components/document-uploader";
import { ModelConfigPanel } from "../../components/model-config-panel";

describe("left panel components", () => {
  it("renders uploader and document list", () => {
    render(
      <>
        <DocumentUploader onUpload={vi.fn(async () => {})} />
        <DocumentList
          documents={[{ doc_id: "d1", document_name: "sample.pdf", status: "ready" }]}
          selectedDocId={null}
          onSelect={vi.fn()}
          onDelete={vi.fn()}
        />
      </>
    );

    expect(screen.getByText("Upload")).toBeInTheDocument();
    expect(screen.getByText(/sample\.pdf/)).toBeInTheDocument();
  });

  it("renders model config controls", () => {
    render(
      <ModelConfigPanel
        initial={{
          auto_select: true,
          query_provider: "ollama",
          query_model_name: "llama3.1:8b",
          vision_provider: "ollama",
          vision_model_name: "llava:7b",
          summary_provider: "ollama",
          summary_model_name: "llama3.1:8b",
        }}
        providers={[
          { provider: "ollama", paid: false, requires_api_key: false, key_configured: true, models: ["llama3.1:8b"] },
          { provider: "openrouter", paid: true, requires_api_key: true, key_configured: false, models: [] },
          { provider: "openai", paid: true, requires_api_key: true, key_configured: false, models: [] },
        ]}
        onSave={vi.fn(async () => {})}
      />
    );

    expect(screen.getByRole("heading", { name: "Model" })).toBeInTheDocument();
    expect(screen.getByText("Auto select")).toBeInTheDocument();
  });
});
