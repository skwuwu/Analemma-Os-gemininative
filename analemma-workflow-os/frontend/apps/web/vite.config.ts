import { defineConfig } from "vite";
import react from "@vitejs/plugin-react-swc";
import path from "path";
import { componentTagger } from "lovable-tagger";

// https://vitejs.dev/config/
export default defineConfig(({ mode }) => ({
  server: {
    host: "::",
    port: 8080,
  },
  plugins: [react(), mode === "development" && componentTagger()].filter(Boolean),
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
  build: {
    // Chunk size warning threshold (KB)
    chunkSizeWarningLimit: 800,
    // Use esbuild minifier
    minify: 'esbuild',
    // Optimize dependencies
    commonjsOptions: {
      transformMixedEsModules: true,
    },
    rollupOptions: {
      onwarn(warning, warn) {
        // Suppress circular dependency warnings from node_modules
        if (warning.code === 'CIRCULAR_DEPENDENCY' && warning.message?.includes('node_modules')) {
          return;
        }
        // Suppress eval warnings
        if (warning.code === 'EVAL') return;
        warn(warning);
      },
      output: {
        manualChunks(id) {
          // 0. d3 libraries - CRITICAL: separate to avoid circular dependency issues
          // d3 packages have internal circular deps that cause runtime initialization errors
          if (id.includes('node_modules/d3-selection') ||
              id.includes('node_modules/d3-transition') ||
              id.includes('node_modules/d3-interpolate') ||
              id.includes('node_modules/d3-dispatch') ||
              id.includes('node_modules/d3-timer') ||
              id.includes('node_modules/d3-ease')) {
            return 'd3-vendor';
          }
          
          // JSON viewer - also has circular dep issues
          if (id.includes('node_modules/@uiw/react-json-view')) {
            return 'json-viewer';
          }
          
          // 1. Zustand stores - must load before app code
          if (id.includes('src/lib/workflowStore') || 
              id.includes('src/lib/codesignStore') ||
              id.includes('src/lib/streamingFetch') ||
              id.includes('src/lib/jsonlParser')) {
            return 'stores';
          }
          
          // 2. React ecosystem - core dependency
          if (id.includes('node_modules/react-dom')) {
            return 'react-vendor';
          }
          if (id.includes('node_modules/react/') || id.includes('node_modules/react-is')) {
            return 'react-vendor';
          }
          
          // 3. Zustand library
          if (id.includes('node_modules/zustand')) {
            return 'zustand-vendor';
          }
          
          // 4. XYFlow - separate heavy library (may use d3 internally)
          if (id.includes('node_modules/@xyflow')) {
            return 'xyflow';
          }
          
          // 5. TanStack Query
          if (id.includes('node_modules/@tanstack')) {
            return 'tanstack';
          }
          
          // Let Rollup handle remaining app code
        },
      },
    },
  },
}));
