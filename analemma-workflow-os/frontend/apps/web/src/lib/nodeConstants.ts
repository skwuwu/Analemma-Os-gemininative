/**
 * Node Constants
 * 
 * PURE DATA ONLY - No imports, no React components, no circular dependencies.
 * This file sits at the absolute bottom of the dependency tree.
 * Icon names are strings that will be resolved by node components.
 */

// Operator Node Config
export const OPERATOR_CONFIG = {
  custom: { iconName: 'Globe', color: '25 95% 60%', label: 'Custom' },
  api_call: { iconName: 'Globe', color: '200 100% 50%', label: 'API Call' },
  database: { iconName: 'Database', color: '190 100% 28%', label: 'Database' },
  db_query: { iconName: 'Database', color: '190 100% 28%', label: 'Database' },
  safe_operator: { iconName: 'CheckCircle2', color: '142 76% 36%', label: 'Safe Transform' },
  operator_official: { iconName: 'CheckCircle2', color: '142 76% 36%', label: 'Safe Transform' },
  default: { iconName: 'Globe', color: '25 95% 60%', label: 'Operator' }
} as const;

// Trigger Node Config
export const TRIGGER_CONFIG = {
  time: {
    iconName: 'Clock',
    label: 'Schedule',
    color: '142 76% 36%'
  },
  request: {
    iconName: 'Webhook',
    label: 'Webhook',
    color: '217 91% 60%'
  },
  event: {
    iconName: 'Zap',
    label: 'Event',
    color: '45 93% 47%'
  },
  default: {
    iconName: 'Zap',
    label: 'Trigger',
    color: '142 76% 36%'
  }
} as const;

// Control Node Config
export const CONTROL_CONFIG = {
  conditional: { iconName: 'GitBranch', color: '280 100% 70%', label: 'Conditional' },
  parallel: { iconName: 'GitBranch', color: '280 100% 70%', label: 'Parallel' },
  default: { iconName: 'GitBranch', color: '280 100% 70%', label: 'Control' }
} as const;

// Type exports for type safety (pure types, no runtime impact)
export type OperatorType = keyof typeof OPERATOR_CONFIG;
export type TriggerType = keyof typeof TRIGGER_CONFIG;
export type ControlType = keyof typeof CONTROL_CONFIG;
export type NodeStatus = 'idle' | 'running' | 'failed' | 'completed';

