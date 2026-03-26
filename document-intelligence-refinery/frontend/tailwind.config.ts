import type { Config } from "tailwindcss";

const config: Config = {
  content: [
    "./app/**/*.{js,ts,jsx,tsx,mdx}",
    "./components/**/*.{js,ts,jsx,tsx,mdx}",
    "./lib/**/*.{js,ts,jsx,tsx,mdx}",
    "./tests/**/*.{js,ts,jsx,tsx,mdx}",
  ],
  theme: {
    extend: {
      colors: {
        ink: "rgb(var(--ink-rgb) / <alpha-value>)",
        sand: "rgb(var(--sand-rgb) / <alpha-value>)",
        mint: "rgb(var(--mint-rgb) / <alpha-value>)",
        coral: "rgb(var(--coral-rgb) / <alpha-value>)",
        sky: "rgb(var(--sky-rgb) / <alpha-value>)",
      },
      boxShadow: {
        panel: "0 20px 45px -25px rgba(15, 31, 30, 0.45)",
      },
      borderRadius: {
        xl2: "1.25rem",
      },
      fontFamily: {
        display: ["'Space Grotesk'", "sans-serif"],
        body: ["'Manrope'", "sans-serif"],
        mono: ["'IBM Plex Mono'", "monospace"],
      },
    },
  },
  plugins: [],
};

export default config;
