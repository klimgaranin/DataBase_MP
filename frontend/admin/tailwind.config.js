/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        ink: "#17212b",
        muted: "#667085",
        line: "#d9e0e7",
        primary: "#315f8c",
        plum: "#685786",
        sun: "#9a6b25",
      },
      borderRadius: {
        ui: "8px",
      },
      boxShadow: {
        panel: "0 18px 48px rgba(21, 27, 35, 0.11)",
        soft: "0 8px 24px rgba(21, 27, 35, 0.07)",
      },
    },
  },
  plugins: [],
};
