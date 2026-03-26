import React from "react";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it } from "vitest";

import { ChatPanel } from "../../components/chat-panel";

describe("chat provenance integration", () => {
  it("renders answer and provenance cards", async () => {
    render(
      <ChatPanel
        onAsk={async () => ({
          answer: "Revenue grew 18%.",
          provenance: [
            {
              document_name: "report.pdf",
              page_number: 4,
              bbox: [0, 0, 50, 50],
              content_hash: "hash1234",
            },
          ],
        })}
      />
    );

    fireEvent.change(screen.getByLabelText("Question"), {
      target: { value: "What is revenue growth?" },
    });
    fireEvent.click(screen.getByText("Ask"));

    await waitFor(() => {
      expect(screen.getByText("Revenue grew 18%." )).toBeInTheDocument();
      expect(screen.getByText("report.pdf")).toBeInTheDocument();
      expect(screen.getByText(/Hash hash1234/)).toBeInTheDocument();
    });
  });
});
