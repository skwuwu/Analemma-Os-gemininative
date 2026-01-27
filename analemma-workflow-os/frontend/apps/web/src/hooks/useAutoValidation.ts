/**
 * useAutoValidation: Background Linter for Workflow Canvas
 * =========================================================
 * 
 * Automatically validates workflow logic as users design, similar to:
 * - VSCode's real-time linter
 * - Figma's auto-layout validation
 * - Circuit design DRC (Design Rule Check)
 * 
 * Eliminates the need for manual "Simulate Run" button.
 * 
 * NOTE: This hook receives store values via props to avoid module-level
 * store imports that can cause bundler initialization order issues.
 */
import { useEffect, useRef } from 'react';
import { Node, Edge } from '@xyflow/react';

export interface AuditIssue {
  level: 'error' | 'warning' | 'info';
  type: string;
  message: string;
  affectedNodes: string[];
  suggestion?: string;
}

interface UseAutoValidationOptions {
  enabled?: boolean;
  debounceMs?: number;
  onValidationComplete?: (issueCount: number) => void;
  // Store values passed from component to avoid circular import issues
  nodes: Node[];
  edges: Edge[];
  auditIssues: AuditIssue[];
  requestAudit: (workflow: { nodes: Node[]; edges: Edge[] }, authToken?: string) => Promise<void>;
}

export function useAutoValidation(options: UseAutoValidationOptions) {
  const { 
    enabled = true, 
    debounceMs = 1500, 
    onValidationComplete,
    nodes,
    edges,
    auditIssues,
    requestAudit
  } = options;
  
  const timeoutRef = useRef<NodeJS.Timeout>();
  const prevWorkflowHashRef = useRef<string>('');

  useEffect(() => {
    if (!enabled || nodes.length === 0) {
      return;
    }

    // Generate workflow hash to detect changes
    const workflowHash = JSON.stringify({
      nodeCount: nodes.length,
      edgeCount: edges.length,
      nodeIds: nodes.map(n => n.id).sort(),
      edgeIds: edges.map(e => `${e.source}-${e.target}`).sort()
    });

    // Skip if workflow hasn't changed
    if (workflowHash === prevWorkflowHashRef.current) {
      return;
    }

    prevWorkflowHashRef.current = workflowHash;

    // Debounce validation to avoid excessive calls during rapid edits
    if (timeoutRef.current) {
      clearTimeout(timeoutRef.current);
    }

    timeoutRef.current = setTimeout(async () => {
      console.log('[AutoValidation] Running background audit...');
      
      await requestAudit(
        { nodes, edges },
        undefined // authToken handled by codesignStore
      );

      if (onValidationComplete) {
        onValidationComplete(auditIssues.length);
      }
    }, debounceMs);

    return () => {
      if (timeoutRef.current) {
        clearTimeout(timeoutRef.current);
      }
    };
  }, [nodes, edges, enabled, debounceMs, requestAudit, onValidationComplete, auditIssues.length]);

  return {
    issueCount: auditIssues.length,
    issues: auditIssues,
    hasErrors: auditIssues.some(issue => issue.level === 'error'),
    hasWarnings: auditIssues.some(issue => issue.level === 'warning'),
  };
}
