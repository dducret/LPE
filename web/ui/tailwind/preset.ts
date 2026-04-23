import type { Config } from "tailwindcss";
import { lpeColors, lpeRadii, lpeShadows } from "./tokens";

const preset: Config = {
  theme: {
    extend: {
      colors: lpeColors,
      borderRadius: lpeRadii,
      boxShadow: lpeShadows,
      transitionTimingFunction: {
        spring: "var(--lpe-spring)",
      },
      fontFamily: {
        sans: [
          "\"IBM Plex Sans\"",
          "\"Segoe UI\"",
          "\"Helvetica Neue\"",
          "Arial",
          "sans-serif",
        ],
      },
      backdropBlur: {
        premium: "18px",
      },
    },
  },
};

export default preset;
