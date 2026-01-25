"""
Token aggregation utilities for workflow cost tracking.

This module provides standardized token usage aggregation across all workflow execution patterns,
ensuring consistent cost tracking and guardrail functionality.
"""

import logging
from typing import Dict, Any, List, Optional, Union
from decimal import Decimal, ROUND_HALF_UP

logger = logging.getLogger(__name__)

# Standard token usage structure
TokenUsage = Dict[str, Union[int, float]]

def extract_token_usage(data: Dict[str, Any]) -> TokenUsage:
    """
    Extract token usage from various data structures with fallback support.

    Supports both 'usage' (from llm_chat_runner) and 'token_usage' (legacy) keys.

    Args:
        data: Dictionary that may contain token usage information

    Returns:
        Normalized token usage dictionary with input_tokens, output_tokens, total_tokens
    """
    if not isinstance(data, dict):
        return {'input_tokens': 0, 'output_tokens': 0, 'total_tokens': 0}

    # Try both possible keys for backward compatibility
    token_data = data.get('usage') or data.get('token_usage') or {}

    if not isinstance(token_data, dict):
        return {'input_tokens': 0, 'output_tokens': 0, 'total_tokens': 0}

    # Extract and normalize token values
    input_tokens = int(token_data.get('input_tokens', 0))
    output_tokens = int(token_data.get('output_tokens', 0))
    total_tokens = int(token_data.get('total_tokens', input_tokens + output_tokens))

    return {
        'input_tokens': input_tokens,
        'output_tokens': output_tokens,
        'total_tokens': total_tokens
    }

def aggregate_tokens_from_branches(branches: Dict[str, Any]) -> TokenUsage:
    """
    Aggregate token usage from parallel branches.

    Args:
        branches: Dictionary of branch results

    Returns:
        Aggregated token usage
    """
    total_input = 0
    total_output = 0

    for branch_id, branch_data in branches.items():
        if isinstance(branch_data, dict):
            usage = extract_token_usage(branch_data)
            total_input += usage['input_tokens']
            total_output += usage['output_tokens']

    total_tokens = total_input + total_output
    return {
        'input_tokens': total_input,
        'output_tokens': total_output,
        'total_tokens': total_tokens
    }

def aggregate_tokens_from_iterations(iterations: List[Dict[str, Any]]) -> TokenUsage:
    """
    Aggregate token usage from iteration results (for_each, map).

    Args:
        iterations: List of iteration results

    Returns:
        Aggregated token usage
    """
    total_input = 0
    total_output = 0

    for item_result in iterations:
        if isinstance(item_result, dict):
            usage = extract_token_usage(item_result)
            total_input += usage['input_tokens']
            total_output += usage['output_tokens']

    total_tokens = total_input + total_output
    return {
        'input_tokens': total_input,
        'output_tokens': total_output,
        'total_tokens': total_tokens
    }

def aggregate_tokens_from_nested(nested_results: List[Dict[str, Any]]) -> TokenUsage:
    """
    Recursively aggregate token usage from nested structures.

    Handles arbitrary depth nesting for hyper-stress scenarios.

    Args:
        nested_results: List of nested results with potential recursive structure

    Returns:
        Aggregated token usage
    """
    total_input = 0
    total_output = 0

    def _extract_recursive(data: Any) -> TokenUsage:
        """Recursively extract tokens from nested structures."""
        if isinstance(data, dict):
            # Direct token usage
            usage = extract_token_usage(data)
            if usage['total_tokens'] > 0:
                return usage

            # Check for nested results
            inner_total = {'input_tokens': 0, 'output_tokens': 0, 'total_tokens': 0}

            # Handle various nested result keys
            nested_keys = ['inner_results', 'results', 'branches', 'iterations']
            for key in nested_keys:
                nested_data = data.get(key)
                if isinstance(nested_data, list):
                    for item in nested_data:
                        item_usage = _extract_recursive(item)
                        inner_total['input_tokens'] += item_usage['input_tokens']
                        inner_total['output_tokens'] += item_usage['output_tokens']
                        inner_total['total_tokens'] += item_usage['total_tokens']
                elif isinstance(nested_data, dict):
                    item_usage = _extract_recursive(nested_data)
                    inner_total['input_tokens'] += item_usage['input_tokens']
                    inner_total['output_tokens'] += item_usage['output_tokens']
                    inner_total['total_tokens'] += item_usage['total_tokens']

            if inner_total['total_tokens'] > 0:
                return inner_total

        elif isinstance(data, list):
            # List of results
            list_total = {'input_tokens': 0, 'output_tokens': 0, 'total_tokens': 0}
            for item in data:
                item_usage = _extract_recursive(item)
                list_total['input_tokens'] += item_usage['input_tokens']
                list_total['output_tokens'] += item_usage['output_tokens']
                list_total['total_tokens'] += item_usage['total_tokens']
            return list_total

        return {'input_tokens': 0, 'output_tokens': 0, 'total_tokens': 0}

    for result in nested_results:
        usage = _extract_recursive(result)
        total_input += usage['input_tokens']
        total_output += usage['output_tokens']

    total_tokens = total_input + total_output
    return {
        'input_tokens': total_input,
        'output_tokens': total_output,
        'total_tokens': total_tokens
    }

def accumulate_tokens_in_state(current_result: Dict[str, Any], state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Accumulate token usage by adding current result to existing state values.

    Prevents overwrite issues in complex workflows with multiple aggregation points.

    Args:
        current_result: Current aggregation result with token fields
        state: Workflow state that may contain previous token accumulations

    Returns:
        Updated result with accumulated token values
    """
    # Get previous accumulated values
    prev_input = state.get('total_input_tokens', 0)
    prev_output = state.get('total_output_tokens', 0)
    prev_total = state.get('total_tokens', 0)

    # Get current values from result
    curr_input = current_result.get('total_input_tokens', 0)
    curr_output = current_result.get('total_output_tokens', 0)
    curr_total = current_result.get('total_tokens', 0)

    # Accumulate
    accumulated_input = prev_input + curr_input
    accumulated_output = prev_output + curr_output
    accumulated_total = prev_total + curr_total

    # Update result
    current_result['total_input_tokens'] = accumulated_input
    current_result['total_output_tokens'] = accumulated_output
    current_result['total_tokens'] = accumulated_total

    logger.info(f"Token accumulation: {prev_total} + {curr_total} = {accumulated_total} total tokens")

    return current_result

def calculate_cost_usd(token_usage: TokenUsage, input_cost_per_token: float = 0.00015,
                      output_cost_per_token: float = 0.0006) -> float:
    """
    Calculate estimated cost in USD with high precision to avoid rounding errors.

    Uses Decimal for precision in high-volume scenarios.

    Args:
        token_usage: Token usage dictionary
        input_cost_per_token: Cost per input token (default: GPT-4o-mini input rate)
        output_cost_per_token: Cost per output token (default: GPT-4o-mini output rate)

    Returns:
        Estimated cost in USD
    """
    input_tokens = Decimal(str(token_usage.get('input_tokens', 0)))
    output_tokens = Decimal(str(token_usage.get('output_tokens', 0)))

    input_cost = input_tokens * Decimal(str(input_cost_per_token))
    output_cost = output_tokens * Decimal(str(output_cost_per_token))

    total_cost = input_cost + output_cost

    # Round to 6 decimal places to match typical currency precision
    return float(total_cost.quantize(Decimal('0.000001'), rounding=ROUND_HALF_UP))