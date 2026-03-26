import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { BrowserRouter } from "react-router-dom";
import { LedgerDataProvider } from "./contexts/LedgerDataContext";
import App from "./App";
import "./index.css";

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <BrowserRouter>
      <LedgerDataProvider>
        <App />
      </LedgerDataProvider>
    </BrowserRouter>
  </StrictMode>
);
