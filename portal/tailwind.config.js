/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{js,ts,jsx,tsx}"],
  theme: {
    extend: {
      colors: {
        apex: {
          void: "#0a0b0f",
          night: "#12141c",
          slate: "#1a1d28",
          mist: "#8b919d",
          champagne: "#c9a962",
          gold: "#d4af37",
          golddim: "#8a7340",
          ivory: "#f4f1ea",
          cream: "#e8e4dc",
          wine: "#2c1810",
        },
      },
      fontFamily: {
        display: ["'Cormorant Garamond'", "Georgia", "serif"],
        sans: ["'Outfit'", "system-ui", "sans-serif"],
      },
      backgroundImage: {
        "lux-gradient":
          "radial-gradient(ellipse 120% 80% at 50% -20%, rgba(201,169,98,0.15), transparent 50%), radial-gradient(ellipse 80% 50% at 100% 50%, rgba(42,45,58,0.8), transparent), linear-gradient(180deg, #0a0b0f 0%, #12141c 40%, #0a0b0f 100%)",
        "gold-shine":
          "linear-gradient(105deg, transparent 40%, rgba(212,175,55,0.12) 50%, transparent 60%)",
      },
      animation: {
        shimmer: "shimmer 2.5s ease-in-out infinite",
      },
      keyframes: {
        shimmer: {
          "0%, 100%": { backgroundPosition: "200% 0" },
          "50%": { backgroundPosition: "-200% 0" },
        },
      },
    },
  },
  plugins: [],
};
