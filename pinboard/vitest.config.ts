import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    // `npm run build` compiles test/ into dist/test/, which vitest would also collect.
    include: ["test/**/*.test.ts"],
    coverage: {
      include: ["src/**/*.ts"],
      thresholds: {
        lines: 67,
        functions: 62,
        branches: 60,
        statements: 66,
        autoUpdate: (threshold, previous) =>
          Math.max(previous, Math.floor(threshold) - 1),
      },
    },
    environment: "node",
    globals: true,
  },
});
