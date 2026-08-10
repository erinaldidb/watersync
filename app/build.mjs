/**
 * Build script: compiles the server (esbuild) and client (vite).
 * Run: node build.mjs
 */
import { build } from 'esbuild';
import { execSync } from 'child_process';

// Build server
await build({
  entryPoints: ['src/server/index.ts'],
  bundle: true,
  platform: 'node',
  target: 'node20',
  format: 'esm',
  outfile: 'dist/server/index.js',
  external: ['express'],
  banner: {
    js: "import { createRequire } from 'module'; const require = createRequire(import.meta.url);",
  },
});

console.log('[build] Server compiled to dist/server/index.js');

// Build client
execSync('npx vite build', { stdio: 'inherit' });
console.log('[build] Client compiled to dist/client/');
