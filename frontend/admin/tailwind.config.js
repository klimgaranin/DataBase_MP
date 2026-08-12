/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        ink: "#151b23",
        muted: "#64707d",
        line: "#dbe2e8",
        teal: "#0f766e",
        berry: "#6d28d9",
        amber: "#b45309",
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
