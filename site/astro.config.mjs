import { defineConfig } from 'astro/config';

// Fully static output: Vercel serves ./dist with zero configuration.
export default defineConfig({ output: 'static' });
