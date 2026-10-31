import { defineConfig } from 'vitest/config';

// `base: './'` keeps every emitted asset URL relative, so the same bundle works
// from `book/output/play/` on GitHub Pages, from a `file://` open, and from the
// dev server.
export default defineConfig({
  base: './',
  // The shared tokens, components and fonts live in website/shared/.
  server: {
    fs: { allow: ['..'] },
  },
  build: {
    target: 'es2022',
    outDir: 'dist',
    emptyOutDir: true,
  },
  test: {
    environment: 'node',
    include: ['src/**/*.test.ts'],
  },
});
