"""
Safe Expression Evaluator for Operator Strategies.

Provides a secure way to evaluate simple expressions without using eval() or exec().
Supports JSONPath-style access and basic comparison operators.

Security:
- No code execution (no eval/exec)
- Whitelist-only operators
- No function calls in expressions
- Maximum expression depth to prevent DoS
"""

import re
import operator
from typing import Any, Dict, List, Optional, Union, Callable
from functools import reduce
import logging

logger = logging.getLogger(__name__)

# Maximum depth for nested access to prevent DoS
MAX_PATH_DEPTH = 20


class SafeExpressionEvaluator:
    """
    Evaluates safe expressions in JSONPath-like syntax.
    
    Supported syntax:
    - $.field - Direct field access
    - $.field.nested - Nested access
    - $.field[0] - Array index access
    - $.field == 'value' - Equality comparison
    - $.field != 'value' - Inequality
    - $.field > 10 - Greater than
    - $.field >= 10 - Greater or equal
    - $.field < 10 - Less than
    - $.field <= 10 - Less or equal
    - $.field in ['a', 'b'] - Membership test
    - $.field not in ['a', 'b'] - Non-membership
    - $.field and $.other - Logical AND
    - $.field or $.other - Logical OR
    - not $.field - Logical NOT
    """
    
    # Comparison operators whitelist
    COMPARISON_OPS: Dict[str, Callable] = {
        "==": operator.eq,
        "!=": operator.ne,
        ">": operator.gt,
        ">=": operator.ge,
        "<": operator.lt,
        "<=": operator.le,
    }
    
    # Regex patterns for parsing
    PATH_PATTERN = re.compile(r"^\$\.([a-zA-Z_][a-zA-Z0-9_]*(?:\.[a-zA-Z_][a-zA-Z0-9_]*|\[\d+\])*)$")
    COMPARISON_PATTERN = re.compile(
        r"^\$\.([a-zA-Z_][a-zA-Z0-9_\.\[\]]*)\s*(==|!=|>=|<=|>|<)\s*(.+)$"
    )
    MEMBERSHIP_PATTERN = re.compile(
        r"^\$\.([a-zA-Z_][a-zA-Z0-9_\.\[\]]*)\s+(not\s+)?in\s+\[(.+)\]$"
    )
    LITERAL_PATTERN = re.compile(r"^'([^']*)'$|^\"([^\"]*)\"$|^(\d+\.?\d*)$|^(true|false|null)$", re.IGNORECASE)
    
    def __init__(self, context: Dict[str, Any]):
        """
        Initialize evaluator with context data.
        
        Args:
            context: Dictionary containing the data to evaluate against.
        """
        self.context = context
    
    def get_path(self, path: str, default: Any = None) -> Any:
        """
        Get value from context using dot-notation path.
        
        Args:
            path: Dot-separated path (e.g., "user.profile.name")
            default: Default value if path not found
            
        Returns:
            Value at path or default
        """
        if not path:
            return default
            
        parts = self._parse_path(path)
        if len(parts) > MAX_PATH_DEPTH:
            logger.warning(f"Path depth exceeds maximum ({MAX_PATH_DEPTH}): {path}")
            return default
            
        current = self.context
        for part in parts:
            if current is None:
                return default
                
            if isinstance(part, int):
                # Array index access
                if isinstance(current, (list, tuple)) and 0 <= part < len(current):
                    current = current[part]
                else:
                    return default
            elif isinstance(current, dict):
                current = current.get(part, default)
                if current is default and part not in current if isinstance(current, dict) else True:
                    # Check if we should continue or return default
                    if current is default:
                        return default
            else:
                # Try attribute access for objects
                try:
                    current = getattr(current, part, default)
                except Exception:
                    return default
                    
        return current
    
    def _parse_path(self, path: str) -> List[Union[str, int]]:
        """Parse path into list of keys and indices."""
        parts = []
        # Handle both dot notation and bracket notation
        tokens = re.split(r'\.|\[|\]', path)
        for token in tokens:
            if not token:
                continue
            if token.isdigit():
                parts.append(int(token))
            else:
                parts.append(token)
        return parts
    
    def _parse_literal(self, value_str: str, resolve_path: bool = False) -> Any:
        """Parse a literal value from string.
        
        Args:
            value_str: The value string to parse
            resolve_path: If True, resolve $.path expressions to actual values
        """
        value_str = value_str.strip()
        
        # Path reference: $.field.nested - resolve to actual value
        if resolve_path and value_str.startswith("$."):
            path = value_str[2:]  # Remove $. prefix
            return self.get_path(path)
        
        # String literals
        if (value_str.startswith("'") and value_str.endswith("'")) or \
           (value_str.startswith('"') and value_str.endswith('"')):
            return value_str[1:-1]
        
        # Boolean literals
        if value_str.lower() == "true":
            return True
        if value_str.lower() == "false":
            return False
        if value_str.lower() == "null" or value_str.lower() == "none":
            return None
            
        # Number literals
        try:
            if "." in value_str:
                return float(value_str)
            return int(value_str)
        except ValueError:
            pass
            
        # If nothing matches, return as string
        return value_str
    
    def _parse_list_literal(self, list_str: str) -> List[Any]:
        """Parse a list literal from string."""
        items = []
        # Simple CSV parsing (doesn't handle nested structures)
        for item in list_str.split(","):
            items.append(self._parse_literal(item.strip()))
        return items
    
    def evaluate(self, expression: str) -> Any:
        """
        Evaluate an expression against the context.
        
        Args:
            expression: Expression to evaluate (e.g., "$.name == 'John'")
            
        Returns:
            Result of expression evaluation
        """
        expression = expression.strip()
        
        # Handle empty expression
        if not expression:
            return None
            
        # Simple path access: $.field or $.field.nested
        path_match = self.PATH_PATTERN.match(expression)
        if path_match:
            return self.get_path(path_match.group(1))
        
        # Comparison: $.field == 'value' OR $.field == $.other_field
        comp_match = self.COMPARISON_PATTERN.match(expression)
        if comp_match:
            path, op, value_str = comp_match.groups()
            left_value = self.get_path(path)
            # resolve_path=True allows right side to be $.path reference
            right_value = self._parse_literal(value_str, resolve_path=True)
            op_func = self.COMPARISON_OPS.get(op)
            if op_func:
                try:
                    return op_func(left_value, right_value)
                except TypeError:
                    # Can't compare incompatible types
                    return False
        
        # Membership: $.field in ['a', 'b']
        mem_match = self.MEMBERSHIP_PATTERN.match(expression)
        if mem_match:
            path, negated, list_str = mem_match.groups()
            value = self.get_path(path)
            items = self._parse_list_literal(list_str)
            result = value in items
            return not result if negated else result
        
        # Handle logical operators (and, or, not)
        if " and " in expression:
            parts = expression.split(" and ", 1)
            return self.evaluate(parts[0]) and self.evaluate(parts[1])
            
        if " or " in expression:
            parts = expression.split(" or ", 1)
            return self.evaluate(parts[0]) or self.evaluate(parts[1])
            
        if expression.startswith("not "):
            return not self.evaluate(expression[4:])
        
        # Literal value
        return self._parse_literal(expression)


def evaluate_expression(expression: str, context: Dict[str, Any]) -> Any:
    """
    Convenience function to evaluate an expression.
    
    Args:
        expression: Expression string
        context: Data context
        
    Returns:
        Evaluation result
    """
    evaluator = SafeExpressionEvaluator(context)
    return evaluator.evaluate(expression)


def get_nested_value(data: Any, path: str, default: Any = None) -> Any:
    """
    Get a nested value using dot notation.
    
    Args:
        data: Source data (dict or object)
        path: Dot-separated path (e.g., "user.profile.name")
        default: Default if not found
        
    Returns:
        Value at path or default
    """
    if not path:
        return data
    
    # Strip $. prefix if present
    if path.startswith("$."):
        path = path[2:]
    
    # Parse and traverse
    parts = []
    tokens = re.split(r'\.|\[|\]', path)
    for token in tokens:
        if not token:
            continue
        if token.isdigit():
            parts.append(int(token))
        else:
            parts.append(token)
    
    if len(parts) > MAX_PATH_DEPTH:
        logger.warning(f"Path depth exceeds maximum ({MAX_PATH_DEPTH}): {path}")
        return default
    
    current = data
    for part in parts:
        if current is None:
            return default
            
        if isinstance(part, int):
            # Array index access
            if isinstance(current, (list, tuple)) and 0 <= part < len(current):
                current = current[part]
            else:
                return default
        elif isinstance(current, dict):
            if part in current:
                current = current[part]
            else:
                return default
        else:
            # Try attribute access for objects
            try:
                current = getattr(current, part, default)
                if current is default:
                    return default
            except Exception:
                return default
                
    return current


def set_nested_value(data: Dict[str, Any], path: str, value: Any) -> Dict[str, Any]:
    """
    Set a nested value using dot notation.
    
    Args:
        data: Target dictionary (will be modified)
        path: Dot-separated path
        value: Value to set
        
    Returns:
        Modified data dictionary
    """
    if not path:
        return data
        
    parts = path.replace("$.", "").split(".")
    current = data
    
    for i, part in enumerate(parts[:-1]):
        # Handle array index
        if "[" in part:
            key, idx_str = part.split("[")
            idx = int(idx_str.rstrip("]"))
            if key not in current:
                current[key] = []
            while len(current[key]) <= idx:
                current[key].append({})
            current = current[key][idx]
        else:
            if part not in current:
                current[part] = {}
            current = current[part]
    
    # Set the final value
    final_key = parts[-1]
    if "[" in final_key:
        key, idx_str = final_key.split("[")
        idx = int(idx_str.rstrip("]"))
        if key not in current:
            current[key] = []
        while len(current[key]) <= idx:
            current[key].append(None)
        current[key][idx] = value
    else:
        current[final_key] = value
        
    return data
