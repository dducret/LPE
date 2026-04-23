import type { Config } from "tailwindcss";
import sharedPreset from "../ui/tailwind/preset";

export default {
  presets: [sharedPreset],
  content: [
    "./index.html",
    "./src/**/*.{ts,tsx}",
    "../shared/src/**/*.{ts,tsx,css}",
    "../ui/src/**/*.{ts,tsx}",
    "../ui/tailwind/**/*.{ts,css}",
  ],
} satisfies Config;
