import type { Config } from "tailwindcss";

export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        ink: "#080911",
        panel: "rgba(12, 12, 21, 0.84)",
        panelSoft: "#19182a",
        sidebar: "rgba(9, 9, 18, 0.94)",
        border: "rgba(181, 167, 255, 0.13)",
        text: "#f8fbff",
        muted: "#a6acc7",
        accent: "#a78bfa",
        accentSoft: "rgba(167, 139, 250, 0.2)",
        success: "#6ee7b7",
        warning: "#fbbf24",
        danger: "#f87171"
      },
      backgroundImage: {
        "app": "linear-gradient(180deg, #05060d 0%, #090814 28%, #0a0d18 58%, #0b0f19 100%)",
        "hero-panel": "linear-gradient(180deg, rgba(13,12,24,0.96) 0%, rgba(10,11,20,0.98) 100%)",
        "card-surface": "linear-gradient(180deg, rgba(16,15,28,0.96) 0%, rgba(12,13,23,0.97) 100%)"
      },
      boxShadow: {
        panel: "0 28px 70px rgba(0, 0, 0, 0.38), 0 0 0 1px rgba(255,255,255,0.02)",
        shell: "0 36px 90px rgba(0, 0, 0, 0.45), 0 0 80px rgba(109,40,217,0.10)",
        sidebar: "0 34px 90px rgba(0, 0, 0, 0.42), 0 0 70px rgba(109,40,217,0.10)"
      },
      borderRadius: {
        xl2: "1.5rem"
      },
      screens: {
        "2xl": "1440px"
      }
    }
  },
  plugins: []
} satisfies Config;
