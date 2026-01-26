/**
 * Skills API Client - CRUD operations for Skills and Subgraphs
 * 
 * Endpoints:
 * - POST   /skills           - Create a new skill
 * - GET    /skills           - List skills for authenticated user
 * - GET    /skills/{id}      - Get a specific skill
 * - PUT    /skills/{id}      - Update a skill
 * - DELETE /skills/{id}      - Delete a skill
 * - GET    /skills/public    - List public/shared skills (marketplace)
 * - POST   /skills/subgraph  - Save a subgraph as a skill
 */

import { makeAuthenticatedRequest, parseApiResponse, getOwnerId } from './api';

// API Base URL from environment (injected via SSM Parameter Store in CI/CD)
const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || '';

// -----------------------------------------------------------------------------
// Types
// -----------------------------------------------------------------------------

export interface ToolDefinition {
  name: string;
  description: string;
  parameters?: Record<string, unknown>;
  required_api_keys?: string[];
  handler_type?: string;
  handler_config?: Record<string, unknown>;
}

export interface SkillDependency {
  skill_id: string;
  version?: string;
  alias?: string;
}

export interface Skill {
  skill_id: string;
  version: string;
  owner_id: string;
  visibility: 'private' | 'public' | 'organization';
  name: string;
  description: string;
  category: string;
  tags: string[];
  tool_definitions: ToolDefinition[];
  system_instructions: string;
  dependencies: SkillDependency[];
  required_api_keys: string[];
  timeout_seconds: number;
  created_at: string;
  updated_at: string;
  status: 'active' | 'deprecated' | 'archived';
  // Subgraph-based skills
  skill_type?: 'tool_based' | 'subgraph_based';
  subgraph_config?: SubgraphDefinition;
  input_schema?: Record<string, SchemaField>;
  output_schema?: Record<string, SchemaField>;
}

export interface SchemaField {
  type: 'string' | 'number' | 'object' | 'array' | 'boolean' | 'any';
  description?: string;
  required?: boolean;
  default?: unknown;
}

export interface SubgraphMetadata {
  name: string;
  description?: string;
  icon?: string;
  color?: string;
}

export interface SubgraphDefinition {
  nodes: unknown[];
  edges: unknown[];
  subgraphs?: Record<string, SubgraphDefinition>;
  input_schema?: Record<string, SchemaField>;
  output_schema?: Record<string, SchemaField>;
  metadata: SubgraphMetadata;
}

export interface CreateSkillRequest {
  name: string;
  description?: string;
  category?: string;
  tags?: string[];
  tool_definitions?: ToolDefinition[];
  system_instructions?: string;
  visibility?: 'private' | 'public' | 'organization';
}

export interface UpdateSkillRequest {
  name?: string;
  description?: string;
  category?: string;
  tags?: string[];
  tool_definitions?: ToolDefinition[];
  system_instructions?: string;
  visibility?: 'private' | 'public' | 'organization';
  status?: 'active' | 'deprecated' | 'archived';
}

export interface SaveSubgraphAsSkillRequest {
  name: string;
  description?: string;
  category?: string;
  tags?: string[];
  subgraph: SubgraphDefinition;
  visibility?: 'private' | 'public' | 'organization';
}

export interface SkillListResponse {
  items: Skill[];
  next_token?: string;
}

// -----------------------------------------------------------------------------
// API Functions
// -----------------------------------------------------------------------------

/**
 * Create a new skill
 */
export async function createSkill(request: CreateSkillRequest): Promise<Skill> {
  const response = await makeAuthenticatedRequest(`${API_BASE_URL}/skills`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(request),
  });

  return parseApiResponse<Skill>(response);
}

/**
 * List skills for the authenticated user
 */
export async function listSkills(options?: {
  category?: string;
  limit?: number;
  nextToken?: string;
}): Promise<SkillListResponse> {
  const params = new URLSearchParams();
  if (options?.category) params.append('category', options.category);
  if (options?.limit) params.append('limit', options.limit.toString());
  if (options?.nextToken) params.append('next_token', options.nextToken);

  const queryString = params.toString();
  const url = `${API_BASE_URL}/skills${queryString ? `?${queryString}` : ''}`;

  const response = await makeAuthenticatedRequest(url, {
    method: 'GET',
  });

  return parseApiResponse<SkillListResponse>(response);
}

/**
 * Get a specific skill by ID
 */
export async function getSkill(skillId: string, version?: string): Promise<Skill> {
  const params = version ? `?version=${encodeURIComponent(version)}` : '';
  const response = await makeAuthenticatedRequest(
    `${API_BASE_URL}/skills/${encodeURIComponent(skillId)}${params}`,
    { method: 'GET' }
  );

  return parseApiResponse<Skill>(response);
}

/**
 * Update an existing skill
 */
export async function updateSkill(skillId: string, request: UpdateSkillRequest): Promise<Skill> {
  const response = await makeAuthenticatedRequest(
    `${API_BASE_URL}/skills/${encodeURIComponent(skillId)}`,
    {
      method: 'PUT',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(request),
    }
  );

  return parseApiResponse<Skill>(response);
}

/**
 * Delete a skill
 */
export async function deleteSkill(skillId: string): Promise<void> {
  await makeAuthenticatedRequest(
    `${API_BASE_URL}/skills/${encodeURIComponent(skillId)}`,
    { method: 'DELETE' }
  );
}

/**
 * List public/shared skills (marketplace)
 */
export async function listPublicSkills(options?: {
  category?: string;
  search?: string;
  limit?: number;
  nextToken?: string;
}): Promise<SkillListResponse> {
  const params = new URLSearchParams();
  if (options?.category) params.append('category', options.category);
  if (options?.search) params.append('search', options.search);
  if (options?.limit) params.append('limit', options.limit.toString());
  if (options?.nextToken) params.append('next_token', options.nextToken);

  const queryString = params.toString();
  const url = `${API_BASE_URL}/skills/public${queryString ? `?${queryString}` : ''}`;

  const response = await makeAuthenticatedRequest(url, {
    method: 'GET',
  });

  return parseApiResponse<SkillListResponse>(response);
}

/**
 * Save a subgraph as a reusable skill
 * Note: owner_id is extracted from JWT on backend to prevent IDOR
 */
export async function saveSubgraphAsSkill(request: SaveSubgraphAsSkillRequest): Promise<Skill> {
  // owner_id is intentionally NOT sent from client - backend extracts from JWT
  const skillPayload = {
    name: request.name,
    description: request.description,
    category: request.category,
    tags: request.tags,
    visibility: request.visibility,
    skill_type: 'subgraph_based',
    subgraph_config: request.subgraph,
    input_schema: request.subgraph.input_schema || {},
    output_schema: request.subgraph.output_schema || {},
  };

  const response = await makeAuthenticatedRequest(`${API_BASE_URL}/skills`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(skillPayload),
  });

  return parseApiResponse<Skill>(response);
}

/**
 * Load a skill as a subgraph definition for use in workflows
 */
export async function loadSkillAsSubgraph(skillId: string): Promise<SubgraphDefinition | null> {
  try {
    const skill = await getSkill(skillId);

    if (skill.skill_type !== 'subgraph_based' || !skill.subgraph_config) {
      console.warn(`Skill ${skillId} is not a subgraph-based skill`);
      return null;
    }

    return skill.subgraph_config;
  } catch (error) {
    console.error(`Failed to load skill ${skillId} as subgraph:`, error);
    return null;
  }
}

// -----------------------------------------------------------------------------
// React Query Hooks (Optional - if using TanStack Query)
// -----------------------------------------------------------------------------

// If you're using TanStack Query, you can uncomment and use these hooks:
/*
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';

export const skillsQueryKeys = {
  all: ['skills'] as const,
  list: (options?: { category?: string }) => [...skillsQueryKeys.all, 'list', options] as const,
  detail: (skillId: string) => [...skillsQueryKeys.all, 'detail', skillId] as const,
  public: (options?: { category?: string; search?: string }) => 
    [...skillsQueryKeys.all, 'public', options] as const,
};

export function useSkills(options?: { category?: string }) {
  return useQuery({
    queryKey: skillsQueryKeys.list(options),
    queryFn: () => listSkills(options),
  });
}

export function useSkill(skillId: string) {
  return useQuery({
    queryKey: skillsQueryKeys.detail(skillId),
    queryFn: () => getSkill(skillId),
    enabled: !!skillId,
  });
}

export function usePublicSkills(options?: { category?: string; search?: string }) {
  return useQuery({
    queryKey: skillsQueryKeys.public(options),
    queryFn: () => listPublicSkills(options),
  });
}

export function useCreateSkill() {
  const queryClient = useQueryClient();
  
  return useMutation({
    mutationFn: createSkill,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: skillsQueryKeys.all });
    },
  });
}

export function useUpdateSkill() {
  const queryClient = useQueryClient();
  
  return useMutation({
    mutationFn: ({ skillId, request }: { skillId: string; request: UpdateSkillRequest }) =>
      updateSkill(skillId, request),
    onSuccess: (_, { skillId }) => {
      queryClient.invalidateQueries({ queryKey: skillsQueryKeys.detail(skillId) });
      queryClient.invalidateQueries({ queryKey: skillsQueryKeys.all });
    },
  });
}

export function useDeleteSkill() {
  const queryClient = useQueryClient();
  
  return useMutation({
    mutationFn: deleteSkill,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: skillsQueryKeys.all });
    },
  });
}

export function useSaveSubgraphAsSkill() {
  const queryClient = useQueryClient();
  
  return useMutation({
    mutationFn: saveSubgraphAsSkill,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: skillsQueryKeys.all });
    },
  });
}
*/
