import type { ReactNode } from "react";
import "./globals.css";
import { AppFooter } from "../components/app-footer";
import { NavBar } from "../components/nav-bar";

export const metadata = {
  title: "DocForge – documents refined into structured data",
};

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en">
      <body className="flex min-h-screen flex-col">
        <NavBar />
        <div className="flex flex-1 flex-col">{children}</div>
        <AppFooter />
      </body>
    </html>
  );
}
