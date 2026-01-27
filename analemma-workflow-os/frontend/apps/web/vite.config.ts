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
    chunkSizeWarningLimit: 500,
    // Use esbuild (default, faster than terser)
    minify: 'esbuild',
    // Optimize dependencies to prevent circular dependency crashes
    commonjsOptions: {
      transformMixedEsModules: true,
    },
    rollupOptions: {
      // Prevent circular dependency errors during build
      onwarn(warning, warn) {
        // Suppress circular dependency warnings
        if (warning.code === 'CIRCULAR_DEPENDENCY') return;
        // Suppress eval warnings
        if (warning.code === 'EVAL') return;
        warn(warning);
      },
      output: {
        // More aggressive code splitting to prevent circular deps
        manualChunks(id) {
          // React ecosystem
          if (id.includes('node_modules/react') || id.includes('node_modules/react-dom')) {
            return 'react-vendor';
          }
          // XYFlow
          if (id.includes('node_modules/@xyflow')) {
            return 'xyflow';
          }
          // TanStack Query
          if (id.includes('node_modules/@tanstack')) {
            return 'tanstack';
          }
          // UI Libraries
          if (id.includes('node_modules/lucide-react') || id.includes('node_modules/sonner')) {
            return 'ui-vendor';
          }
          // Framer Motion (heavy animation library)
          if (id.includes('node_modules/framer-motion')) {
            return 'framer-motion';
          }
          // Zustand stores (separate to avoid circular deps)
          if (id.includes('/lib/workflowStore') || id.includes('/lib/codesignStore')) {
            return 'stores';
          }
          // Workflow components (separate chunk to break circular deps)
          if (id.includes('/components/WorkflowCanvas') || 
              id.includes('/components/nodes/') ||
              id.includes('/components/edges/')) {
            return 'workflow-canvas';
          }
        },
      },
    },
  },
}));
