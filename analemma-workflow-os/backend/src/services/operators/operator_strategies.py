"""
Operator Strategies - Safe Built-in Transformations for Workflows.

This module provides production-ready data transformation strategies
that can be used in workflows without requiring exec() or eval().

All strategies are pure functions with no side effects.
"""

import json
import re
import base64
import hashlib
import uuid
import math
from enum import Enum
from typing import Any, Dict, List, Optional, Callable, Union, Tuple
from datetime import datetime, timezone, timedelta
from functools import reduce
from urllib.parse import quote, unquote, urlencode, parse_qs
import logging

from .expression_evaluator import (
    SafeExpressionEvaluator,
    evaluate_expression,
    get_nested_value,
    set_nested_value,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Strategy Enum - All supported transformation types
# =============================================================================

class OperatorStrategy(str, Enum):
    """Enumeration of all supported operator strategies."""
    
    # JSON/Object Manipulation
    JSON_PARSE = "json_parse"
    JSON_STRINGIFY = "json_stringify"
    DEEP_GET = "deep_get"
    DEEP_SET = "deep_set"
    PICK_FIELDS = "pick_fields"
    OMIT_FIELDS = "omit_fields"
    MERGE_OBJECTS = "merge_objects"
    FLATTEN = "flatten"
    UNFLATTEN = "unflatten"
    
    # List/Array Operations
    LIST_MAP = "list_map"
    LIST_FILTER = "list_filter"
    LIST_REDUCE = "list_reduce"
    LIST_SORT = "list_sort"
    LIST_GROUP_BY = "list_group_by"
    LIST_UNIQUE = "list_unique"
    LIST_FIRST = "list_first"
    LIST_LAST = "list_last"
    LIST_SLICE = "list_slice"
    LIST_CONCAT = "list_concat"
    LIST_ZIP = "list_zip"
    LIST_FIND = "list_find"
    LIST_COUNT = "list_count"
    LIST_REVERSE = "list_reverse"
    
    # String Manipulation
    STRING_TEMPLATE = "string_template"
    STRING_SPLIT = "string_split"
    STRING_JOIN = "string_join"
    REGEX_EXTRACT = "regex_extract"
    REGEX_REPLACE = "regex_replace"
    REGEX_MATCH = "regex_match"
    STRING_TRIM = "string_trim"
    STRING_CASE = "string_case"
    STRING_TRUNCATE = "string_truncate"
    STRING_PAD = "string_pad"
    
    # Type Conversion
    TO_INT = "to_int"
    TO_FLOAT = "to_float"
    TO_BOOL = "to_bool"
    TO_STRING = "to_string"
    TO_DATE = "to_date"
    FORMAT_DATE = "format_date"
    COERCE_TYPE = "coerce_type"
    
    # Control Flow / Conditional
    IF_ELSE = "if_else"
    SWITCH_CASE = "switch_case"
    DEFAULT_VALUE = "default_value"
    TRY_CATCH = "try_catch"
    ASSERT = "assert"
    
    # Encoding / Cryptography (Safe subset)
    BASE64_ENCODE = "base64_encode"
    BASE64_DECODE = "base64_decode"
    URL_ENCODE = "url_encode"
    URL_DECODE = "url_decode"
    HASH_SHA256 = "hash_sha256"
    HASH_MD5 = "hash_md5"
    UUID_GENERATE = "uuid_generate"
    
    # Math Operations
    MATH_ROUND = "math_round"
    MATH_FLOOR = "math_floor"
    MATH_CEIL = "math_ceil"
    MATH_ABS = "math_abs"
    MATH_CLAMP = "math_clamp"
    MATH_PERCENT = "math_percent"
    MATH_EXPRESSION = "math_expression"
    
    # Utility
    ECHO = "echo"  # Pass-through for testing
    LOG = "log"    # Debug logging
    TIMESTAMP = "timestamp"  # Current timestamp
    

# =============================================================================
# Strategy Implementations
# =============================================================================

def _json_parse(input_val: Any, params: Dict[str, Any]) -> Any:
    """Parse JSON string to object.
    
    Handles common LLM output formats:
    - Plain JSON: {"key": "value"}
    - Markdown code blocks: ```json\n{"key": "value"}\n```
    - Mixed content with JSON embedded
    """
    import re
    
    if input_val is None:
        return params.get("default", None)
    if isinstance(input_val, (dict, list)):
        return input_val  # Already parsed
    
    text = str(input_val).strip()
    
    # Try to extract JSON from markdown code blocks (```json ... ```)
    # This handles LLM responses that wrap JSON in markdown
    code_block_match = re.search(r'```(?:json)?\s*([\s\S]*?)\s*```', text)
    if code_block_match:
        text = code_block_match.group(1).strip()
    
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        if params.get("strict", True):
            raise ValueError(f"Invalid JSON: {e}")
        return params.get("default", None)


def _json_stringify(input_val: Any, params: Dict[str, Any]) -> str:
    """Convert object to JSON string."""
    indent = params.get("indent", None)
    ensure_ascii = params.get("ensure_ascii", False)
    return json.dumps(input_val, indent=indent, ensure_ascii=ensure_ascii, default=str)


def _deep_get(input_val: Any, params: Dict[str, Any]) -> Any:
    """Get nested value using dot-path."""
    path = params.get("path", "")
    default = params.get("default", None)
    return get_nested_value(input_val, path, default)


def _deep_set(input_val: Any, params: Dict[str, Any]) -> Any:
    """Set nested value using dot-path."""
    if not isinstance(input_val, dict):
        input_val = {}
    path = params.get("path", "")
    value = params.get("value")
    return set_nested_value(dict(input_val), path, value)


def _pick_fields(input_val: Any, params: Dict[str, Any]) -> Dict[str, Any]:
    """Extract only specified fields from object."""
    if not isinstance(input_val, dict):
        return {}
    fields = params.get("fields", [])
    return {k: v for k, v in input_val.items() if k in fields}


def _omit_fields(input_val: Any, params: Dict[str, Any]) -> Dict[str, Any]:
    """Remove specified fields from object."""
    if not isinstance(input_val, dict):
        return {}
    fields = params.get("fields", [])
    return {k: v for k, v in input_val.items() if k not in fields}


def _merge_objects(input_val: Any, params: Dict[str, Any]) -> Dict[str, Any]:
    """Merge multiple objects into one."""
    result = {}
    objects = params.get("objects", [])
    
    # Start with input if it's a dict
    if isinstance(input_val, dict):
        result.update(input_val)
    
    # Merge additional objects
    for obj in objects:
        if isinstance(obj, dict):
            result.update(obj)
    
    return result


def _flatten(input_val: Any, params: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten nested object to single level with dot-notation keys."""
    if not isinstance(input_val, dict):
        return {"_value": input_val}
    
    separator = params.get("separator", ".")
    max_depth = min(params.get("max_depth", 10), 20)  # Cap at 20
    
    def _do_flatten(obj: Any, prefix: str = "", depth: int = 0) -> Dict[str, Any]:
        items = {}
        if depth >= max_depth:
            items[prefix.rstrip(separator)] = obj
            return items
            
        if isinstance(obj, dict):
            for k, v in obj.items():
                new_key = f"{prefix}{k}{separator}" if prefix else f"{k}{separator}"
                items.update(_do_flatten(v, new_key, depth + 1))
        elif isinstance(obj, list):
            for i, v in enumerate(obj):
                new_key = f"{prefix}{i}{separator}"
                items.update(_do_flatten(v, new_key, depth + 1))
        else:
            items[prefix.rstrip(separator)] = obj
        return items
    
    return _do_flatten(input_val)


def _unflatten(input_val: Any, params: Dict[str, Any]) -> Dict[str, Any]:
    """Convert flattened object with dot-notation keys back to nested."""
    if not isinstance(input_val, dict):
        return {}
    
    separator = params.get("separator", ".")
    result: Dict[str, Any] = {}
    
    for key, value in input_val.items():
        parts = key.split(separator)
        current = result
        for i, part in enumerate(parts[:-1]):
            if part.isdigit():
                # This should be an array index - complex case, skip for now
                continue
            if part not in current:
                current[part] = {}
            current = current[part]
        
        final_part = parts[-1]
        current[final_part] = value
    
    return result


# --- List Operations ---

def _list_map(input_val: Any, params: Dict[str, Any]) -> List[Any]:
    """Transform each element in a list."""
    if not isinstance(input_val, list):
        return []
    
    expression = params.get("expression", "$.")
    field = params.get("field", None)  # Shorthand: extract specific field
    
    result = []
    for item in input_val:
        if field and isinstance(item, dict):
            # Support nested field paths like "branch_final_result.summary"
            result.append(get_nested_value(item, field, None))
        else:
            evaluator = SafeExpressionEvaluator({"item": item, "$": item})
            mapped = evaluator.evaluate(expression.replace("$.", "item.") if expression.startswith("$.") else expression)
            result.append(mapped if mapped is not None else item)
    
    return result


def _list_filter(input_val: Any, params: Dict[str, Any]) -> List[Any]:
    """Filter list by condition."""
    if not isinstance(input_val, list):
        return []
    
    condition = params.get("condition", "")
    if not condition:
        return input_val
    
    result = []
    for item in input_val:
        evaluator = SafeExpressionEvaluator(item if isinstance(item, dict) else {"$": item})
        if evaluator.evaluate(condition):
            result.append(item)
    
    return result


def _list_reduce(input_val: Any, params: Dict[str, Any]) -> Any:
    """Aggregate list to single value."""
    if not isinstance(input_val, list) or len(input_val) == 0:
        return params.get("initial", None)
    
    operation = params.get("operation", "sum")
    field = params.get("field", None)
    
    # Extract field values if specified
    values = input_val
    if field:
        values = [get_nested_value(item, field, 0) if isinstance(item, dict) else item for item in input_val]
    
    # Numeric operations
    numeric_values = []
    for v in values:
        try:
            if isinstance(v, (int, float)):
                numeric_values.append(v)
            elif v is not None:
                numeric_values.append(float(v))
        except (ValueError, TypeError):
            continue
    
    if operation == "sum":
        return sum(numeric_values) if numeric_values else 0
    elif operation == "avg" or operation == "average":
        return sum(numeric_values) / len(numeric_values) if numeric_values else 0
    elif operation == "min":
        return min(numeric_values) if numeric_values else None
    elif operation == "max":
        return max(numeric_values) if numeric_values else None
    elif operation == "count":
        return len(input_val)
    elif operation == "concat":
        return "".join(str(v) for v in values)
    elif operation == "first":
        return input_val[0] if input_val else None
    elif operation == "last":
        return input_val[-1] if input_val else None
    else:
        return sum(numeric_values) if numeric_values else 0


def _list_sort(input_val: Any, params: Dict[str, Any]) -> List[Any]:
    """Sort list by field or value."""
    if not isinstance(input_val, list):
        return []
    
    field = params.get("field", None)
    order = params.get("order", "asc")
    reverse = order.lower() in ("desc", "descending", "reverse")
    
    try:
        if field and all(isinstance(item, dict) for item in input_val):
            return sorted(input_val, key=lambda x: x.get(field, ""), reverse=reverse)
        else:
            return sorted(input_val, reverse=reverse)
    except TypeError:
        # Can't sort mixed types
        return input_val


def _list_group_by(input_val: Any, params: Dict[str, Any]) -> Dict[str, List[Any]]:
    """Group list items by a field value."""
    if not isinstance(input_val, list):
        return {}
    
    field = params.get("field", "")
    if not field:
        return {"_all": input_val}
    
    result: Dict[str, List[Any]] = {}
    for item in input_val:
        if isinstance(item, dict):
            key = str(item.get(field, "_unknown"))
        else:
            key = "_unknown"
        if key not in result:
            result[key] = []
        result[key].append(item)
    
    return result


def _list_unique(input_val: Any, params: Dict[str, Any]) -> List[Any]:
    """Remove duplicates from list."""
    if not isinstance(input_val, list):
        return []
    
    field = params.get("field", None)
    
    if field:
        # Unique by field value
        seen = set()
        result = []
        for item in input_val:
            if isinstance(item, dict):
                key = item.get(field)
            else:
                key = item
            if key not in seen:
                seen.add(key)
                result.append(item)
        return result
    else:
        # Simple unique (preserves order)
        seen = set()
        result = []
        for item in input_val:
            key = json.dumps(item, sort_keys=True, default=str) if isinstance(item, (dict, list)) else item
            if key not in seen:
                seen.add(key)
                result.append(item)
        return result


def _list_first(input_val: Any, params: Dict[str, Any]) -> Any:
    """Get first element (optionally matching condition)."""
    if not isinstance(input_val, list) or len(input_val) == 0:
        return params.get("default", None)
    
    condition = params.get("condition", None)
    if condition:
        for item in input_val:
            evaluator = SafeExpressionEvaluator(item if isinstance(item, dict) else {"$": item})
            if evaluator.evaluate(condition):
                return item
        return params.get("default", None)
    
    return input_val[0]


def _list_last(input_val: Any, params: Dict[str, Any]) -> Any:
    """Get last element (optionally matching condition)."""
    if not isinstance(input_val, list) or len(input_val) == 0:
        return params.get("default", None)
    
    condition = params.get("condition", None)
    if condition:
        for item in reversed(input_val):
            evaluator = SafeExpressionEvaluator(item if isinstance(item, dict) else {"$": item})
            if evaluator.evaluate(condition):
                return item
        return params.get("default", None)
    
    return input_val[-1]


def _list_slice(input_val: Any, params: Dict[str, Any]) -> List[Any]:
    """Slice list by start/end indices."""
    if not isinstance(input_val, list):
        return []
    
    start = params.get("start", 0)
    end = params.get("end", None)
    step = params.get("step", 1)
    
    return input_val[start:end:step]


def _list_concat(input_val: Any, params: Dict[str, Any]) -> List[Any]:
    """Concatenate multiple lists."""
    result = []
    
    if isinstance(input_val, list):
        result.extend(input_val)
    
    for arr in params.get("arrays", []):
        if isinstance(arr, list):
            result.extend(arr)
    
    return result


def _list_zip(input_val: Any, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Zip multiple arrays into list of objects."""
    keys = params.get("keys", [])
    arrays = params.get("arrays", [])
    
    if isinstance(input_val, list):
        arrays = [input_val] + arrays
    
    if not arrays or not keys:
        return []
    
    result = []
    max_len = max(len(arr) for arr in arrays if isinstance(arr, list))
    
    for i in range(max_len):
        obj = {}
        for j, key in enumerate(keys):
            if j < len(arrays) and isinstance(arrays[j], list) and i < len(arrays[j]):
                obj[key] = arrays[j][i]
            else:
                obj[key] = None
        result.append(obj)
    
    return result


def _list_find(input_val: Any, params: Dict[str, Any]) -> Any:
    """Find first item matching condition (alias for list_first with condition)."""
    return _list_first(input_val, params)


def _list_count(input_val: Any, params: Dict[str, Any]) -> int:
    """Count items (optionally matching condition)."""
    if not isinstance(input_val, list):
        return 0
    
    condition = params.get("condition", None)
    if not condition:
        return len(input_val)
    
    count = 0
    for item in input_val:
        evaluator = SafeExpressionEvaluator(item if isinstance(item, dict) else {"$": item})
        if evaluator.evaluate(condition):
            count += 1
    return count


def _list_reverse(input_val: Any, params: Dict[str, Any]) -> List[Any]:
    """Reverse list order."""
    if not isinstance(input_val, list):
        return []
    return list(reversed(input_val))


# --- String Operations ---

def _string_template(input_val: Any, params: Dict[str, Any]) -> str:
    """Render template with variable substitution."""
    template = params.get("template", str(input_val) if input_val else "")
    context = params.get("context", {})
    
    if isinstance(input_val, dict):
        context = {**input_val, **context}
    
    # Simple {{variable}} substitution
    result = template
    for key, value in context.items():
        result = result.replace(f"{{{{{key}}}}}", str(value) if value is not None else "")
    
    return result


def _string_split(input_val: Any, params: Dict[str, Any]) -> List[str]:
    """Split string by delimiter."""
    if input_val is None:
        return []
    
    delimiter = params.get("delimiter", ",")
    max_split = params.get("max_split", -1)
    trim = params.get("trim", True)
    
    parts = str(input_val).split(delimiter, max_split)
    if trim:
        parts = [p.strip() for p in parts]
    
    return parts


def _string_join(input_val: Any, params: Dict[str, Any]) -> str:
    """Join list to string with delimiter."""
    if not isinstance(input_val, list):
        return str(input_val) if input_val else ""
    
    delimiter = params.get("delimiter", ", ")
    return delimiter.join(str(item) for item in input_val)


def _regex_extract(input_val: Any, params: Dict[str, Any]) -> Optional[Union[str, List[str]]]:
    """Extract text matching regex pattern."""
    if input_val is None:
        return None
    
    pattern = params.get("pattern", "")
    group = params.get("group", 0)
    all_matches = params.get("all", False)
    
    try:
        if all_matches:
            matches = re.findall(pattern, str(input_val))
            return matches
        else:
            match = re.search(pattern, str(input_val))
            if match:
                return match.group(group) if group <= len(match.groups()) else match.group(0)
            return None
    except re.error:
        return None


def _regex_replace(input_val: Any, params: Dict[str, Any]) -> str:
    """Replace text matching regex pattern."""
    if input_val is None:
        return ""
    
    pattern = params.get("pattern", "")
    replacement = params.get("replacement", "")
    count = params.get("count", 0)  # 0 = all
    
    try:
        return re.sub(pattern, replacement, str(input_val), count=count)
    except re.error:
        return str(input_val)


def _regex_match(input_val: Any, params: Dict[str, Any]) -> bool:
    """Check if string matches regex pattern."""
    if input_val is None:
        return False
    
    pattern = params.get("pattern", "")
    full_match = params.get("full_match", False)
    
    try:
        if full_match:
            return bool(re.fullmatch(pattern, str(input_val)))
        else:
            return bool(re.search(pattern, str(input_val)))
    except re.error:
        return False


def _string_trim(input_val: Any, params: Dict[str, Any]) -> str:
    """Trim whitespace from string."""
    if input_val is None:
        return ""
    
    chars = params.get("chars", None)
    side = params.get("side", "both")  # left, right, both
    
    s = str(input_val)
    if side == "left":
        return s.lstrip(chars)
    elif side == "right":
        return s.rstrip(chars)
    else:
        return s.strip(chars)


def _string_case(input_val: Any, params: Dict[str, Any]) -> str:
    """Convert string case."""
    if input_val is None:
        return ""
    
    case = params.get("case", "lower")
    s = str(input_val)
    
    if case == "lower":
        return s.lower()
    elif case == "upper":
        return s.upper()
    elif case == "title":
        return s.title()
    elif case == "capitalize":
        return s.capitalize()
    elif case == "snake_case":
        # CamelCase to snake_case
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', s)
        return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
    elif case == "camel_case":
        # snake_case to camelCase
        parts = s.split("_")
        return parts[0].lower() + "".join(p.capitalize() for p in parts[1:])
    elif case == "pascal_case":
        # snake_case to PascalCase
        return "".join(p.capitalize() for p in s.split("_"))
    elif case == "kebab_case":
        # CamelCase to kebab-case
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1-\2', s)
        return re.sub('([a-z0-9])([A-Z])', r'\1-\2', s1).lower()
    else:
        return s


def _string_truncate(input_val: Any, params: Dict[str, Any]) -> str:
    """Truncate string to max length."""
    if input_val is None:
        return ""
    
    max_length = params.get("max_length", 100)
    suffix = params.get("suffix", "...")
    
    s = str(input_val)
    if len(s) <= max_length:
        return s
    
    return s[:max_length - len(suffix)] + suffix


def _string_pad(input_val: Any, params: Dict[str, Any]) -> str:
    """Pad string to target length."""
    if input_val is None:
        return ""
    
    length = params.get("length", 10)
    char = params.get("char", " ")
    side = params.get("side", "left")  # left, right, both
    
    s = str(input_val)
    
    if side == "left":
        return s.rjust(length, char[0])
    elif side == "right":
        return s.ljust(length, char[0])
    else:
        return s.center(length, char[0])


# --- Type Conversion ---

def _to_int(input_val: Any, params: Dict[str, Any]) -> int:
    """Convert to integer."""
    default = params.get("default", 0)
    try:
        if isinstance(input_val, bool):
            return 1 if input_val else 0
        return int(float(input_val))
    except (ValueError, TypeError):
        return default


def _to_float(input_val: Any, params: Dict[str, Any]) -> float:
    """Convert to float."""
    default = params.get("default", 0.0)
    try:
        return float(input_val)
    except (ValueError, TypeError):
        return default


def _to_bool(input_val: Any, params: Dict[str, Any]) -> bool:
    """Convert to boolean."""
    if isinstance(input_val, bool):
        return input_val
    if isinstance(input_val, str):
        return input_val.lower() in ("true", "1", "yes", "on", "t", "y")
    if isinstance(input_val, (int, float)):
        return input_val != 0
    return bool(input_val)


def _to_string(input_val: Any, params: Dict[str, Any]) -> str:
    """Convert to string."""
    if input_val is None:
        return params.get("default", "")
    if isinstance(input_val, (dict, list)):
        return json.dumps(input_val, ensure_ascii=False, default=str)
    return str(input_val)


def _to_date(input_val: Any, params: Dict[str, Any]) -> Optional[datetime]:
    """Parse string to datetime."""
    if input_val is None:
        return None
    if isinstance(input_val, datetime):
        return input_val
    
    format_str = params.get("format", None)
    
    try:
        if format_str:
            return datetime.strptime(str(input_val), format_str)
        else:
            # Try ISO format first
            return datetime.fromisoformat(str(input_val).replace("Z", "+00:00"))
    except ValueError:
        return None


def _format_date(input_val: Any, params: Dict[str, Any]) -> str:
    """Format datetime to string."""
    if input_val is None:
        return ""
    
    format_str = params.get("format", "%Y-%m-%d %H:%M:%S")
    
    if isinstance(input_val, str):
        input_val = _to_date(input_val, {})
    
    if isinstance(input_val, datetime):
        if format_str.upper() == "ISO" or format_str.upper() == "ISO8601":
            return input_val.isoformat()
        return input_val.strftime(format_str)
    
    return str(input_val)


def _coerce_type(input_val: Any, params: Dict[str, Any]) -> Any:
    """Coerce value to specified type."""
    target_type = params.get("type", "string")
    
    type_map = {
        "string": _to_string,
        "int": _to_int,
        "integer": _to_int,
        "float": _to_float,
        "number": _to_float,
        "bool": _to_bool,
        "boolean": _to_bool,
        "date": _to_date,
        "datetime": _to_date,
    }
    
    handler = type_map.get(target_type.lower())
    if handler:
        return handler(input_val, params)
    return input_val


# --- Control Flow ---

def _if_else(input_val: Any, params: Dict[str, Any]) -> Any:
    """Conditional value selection."""
    condition = params.get("condition", "")
    then_value = params.get("then", None)
    else_value = params.get("else", None)
    
    context = {"$": input_val}
    if isinstance(input_val, dict):
        context.update(input_val)
    
    evaluator = SafeExpressionEvaluator(context)
    
    if evaluator.evaluate(condition):
        return then_value
    return else_value


def _switch_case(input_val: Any, params: Dict[str, Any]) -> Any:
    """Multi-way conditional."""
    cases = params.get("cases", {})
    default = params.get("default", None)
    field = params.get("field", None)
    
    # Get the value to switch on
    if field and isinstance(input_val, dict):
        switch_value = input_val.get(field)
    else:
        switch_value = input_val
    
    # Convert to string for matching
    switch_key = str(switch_value) if switch_value is not None else ""
    
    return cases.get(switch_key, default)


def _default_value(input_val: Any, params: Dict[str, Any]) -> Any:
    """Return default if input is null/empty."""
    default = params.get("default", None)
    treat_empty_as_null = params.get("treat_empty_as_null", True)
    
    if input_val is None:
        return default
    
    if treat_empty_as_null:
        if input_val == "" or input_val == [] or input_val == {}:
            return default
    
    return input_val


def _try_catch(input_val: Any, params: Dict[str, Any]) -> Any:
    """Return fallback on error."""
    # This is a structural strategy - just returns input as-is
    # Actual error handling is done at the executor level
    fallback = params.get("fallback", None)
    return input_val if input_val is not None else fallback


def _assert(input_val: Any, params: Dict[str, Any]) -> Any:
    """Assert condition, raise error if false."""
    condition = params.get("condition", "")
    message = params.get("message", "Assertion failed")
    
    context = {"$": input_val}
    if isinstance(input_val, dict):
        context.update(input_val)
    
    evaluator = SafeExpressionEvaluator(context)
    
    if not evaluator.evaluate(condition):
        raise AssertionError(message)
    
    return input_val


# --- Encoding / Cryptography ---

def _base64_encode(input_val: Any, params: Dict[str, Any]) -> str:
    """Encode value to Base64."""
    if input_val is None:
        return ""
    encoding = params.get("encoding", "utf-8")
    data = str(input_val).encode(encoding)
    return base64.b64encode(data).decode("ascii")


def _base64_decode(input_val: Any, params: Dict[str, Any]) -> str:
    """Decode Base64 to string."""
    if input_val is None:
        return ""
    encoding = params.get("encoding", "utf-8")
    try:
        data = base64.b64decode(str(input_val))
        return data.decode(encoding)
    except Exception:
        return ""


def _url_encode(input_val: Any, params: Dict[str, Any]) -> str:
    """URL-encode string."""
    if input_val is None:
        return ""
    
    if isinstance(input_val, dict):
        return urlencode(input_val)
    
    return quote(str(input_val), safe=params.get("safe", ""))


def _url_decode(input_val: Any, params: Dict[str, Any]) -> Union[str, Dict[str, List[str]]]:
    """URL-decode string."""
    if input_val is None:
        return ""
    
    s = str(input_val)
    
    if "=" in s:
        # Parse as query string
        return parse_qs(s)
    
    return unquote(s)


def _hash_sha256(input_val: Any, params: Dict[str, Any]) -> str:
    """Generate SHA256 hash."""
    if input_val is None:
        return ""
    encoding = params.get("encoding", "utf-8")
    data = str(input_val).encode(encoding)
    return hashlib.sha256(data).hexdigest()


def _hash_md5(input_val: Any, params: Dict[str, Any]) -> str:
    """Generate MD5 hash (for checksums, not security)."""
    if input_val is None:
        return ""
    encoding = params.get("encoding", "utf-8")
    data = str(input_val).encode(encoding)
    return hashlib.md5(data).hexdigest()


def _uuid_generate(input_val: Any, params: Dict[str, Any]) -> str:
    """Generate a new UUID."""
    version = params.get("version", 4)
    if version == 1:
        return str(uuid.uuid1())
    return str(uuid.uuid4())


# --- Math Operations ---

def _math_round(input_val: Any, params: Dict[str, Any]) -> float:
    """Round number to decimal places."""
    decimals = params.get("decimals", 0)
    try:
        return round(float(input_val), decimals)
    except (ValueError, TypeError):
        return 0.0


def _math_floor(input_val: Any, params: Dict[str, Any]) -> int:
    """Floor (round down) number."""
    try:
        return math.floor(float(input_val))
    except (ValueError, TypeError):
        return 0


def _math_ceil(input_val: Any, params: Dict[str, Any]) -> int:
    """Ceiling (round up) number."""
    try:
        return math.ceil(float(input_val))
    except (ValueError, TypeError):
        return 0


def _math_abs(input_val: Any, params: Dict[str, Any]) -> float:
    """Absolute value."""
    try:
        return abs(float(input_val))
    except (ValueError, TypeError):
        return 0.0


def _math_clamp(input_val: Any, params: Dict[str, Any]) -> float:
    """Clamp value to range."""
    min_val = params.get("min", float("-inf"))
    max_val = params.get("max", float("inf"))
    try:
        val = float(input_val)
        return max(min_val, min(max_val, val))
    except (ValueError, TypeError):
        return min_val


def _math_percent(input_val: Any, params: Dict[str, Any]) -> float:
    """Calculate percentage."""
    total = params.get("total", 100)
    decimals = params.get("decimals", 2)
    try:
        val = float(input_val)
        total = float(total)
        if total == 0:
            return 0.0
        return round((val / total) * 100, decimals)
    except (ValueError, TypeError):
        return 0.0


def _math_expression(input_val: Any, params: Dict[str, Any]) -> float:
    """Evaluate a safe math expression."""
    expression = params.get("expression", "")
    variables = params.get("variables", {})
    
    if isinstance(input_val, dict):
        variables = {**input_val, **variables}
    elif isinstance(input_val, (int, float)):
        variables["x"] = input_val
    
    # Safe math expression evaluator
    # Only allows: numbers, +, -, *, /, (), and variable names
    allowed_chars = re.compile(r'^[0-9a-zA-Z_\.\+\-\*\/\(\)\s]+$')
    
    if not allowed_chars.match(expression):
        raise ValueError(f"Invalid math expression: {expression}")
    
    # Replace variables with values
    for var, val in variables.items():
        expression = re.sub(rf'\b{var}\b', str(val), expression)
    
    # Evaluate using a simple recursive parser (no eval)
    try:
        return _safe_math_eval(expression)
    except Exception as e:
        logger.warning(f"Math expression error: {e}")
        return 0.0


def _safe_math_eval(expr: str) -> float:
    """
    Safely evaluate a mathematical expression without eval().
    Supports: +, -, *, /, parentheses, and numbers.
    """
    expr = expr.replace(" ", "")
    
    def parse_number(s: str, i: int) -> Tuple[float, int]:
        start = i
        while i < len(s) and (s[i].isdigit() or s[i] == '.'):
            i += 1
        return float(s[start:i] or 0), i
    
    def parse_factor(s: str, i: int) -> Tuple[float, int]:
        if i >= len(s):
            return 0, i
        if s[i] == '(':
            val, i = parse_expr(s, i + 1)
            if i < len(s) and s[i] == ')':
                i += 1
            return val, i
        elif s[i] == '-':
            val, i = parse_factor(s, i + 1)
            return -val, i
        else:
            return parse_number(s, i)
    
    def parse_term(s: str, i: int) -> Tuple[float, int]:
        val, i = parse_factor(s, i)
        while i < len(s) and s[i] in '*/':
            op = s[i]
            right, i = parse_factor(s, i + 1)
            if op == '*':
                val *= right
            elif op == '/' and right != 0:
                val /= right
        return val, i
    
    def parse_expr(s: str, i: int) -> Tuple[float, int]:
        val, i = parse_term(s, i)
        while i < len(s) and s[i] in '+-':
            op = s[i]
            right, i = parse_term(s, i + 1)
            if op == '+':
                val += right
            else:
                val -= right
        return val, i
    
    result, _ = parse_expr(expr, 0)
    return result


# --- Utility ---

def _echo(input_val: Any, params: Dict[str, Any]) -> Any:
    """Pass-through (for testing/debugging)."""
    return input_val


def _log(input_val: Any, params: Dict[str, Any]) -> Any:
    """Log value and pass through."""
    level = params.get("level", "info")
    message = params.get("message", "")
    
    log_func = getattr(logger, level, logger.info)
    log_func(f"[Operator Log] {message}: {input_val}")
    
    return input_val


def _timestamp(input_val: Any, params: Dict[str, Any]) -> Union[str, int, float]:
    """Get current timestamp."""
    format_type = params.get("format", "iso")
    
    now = datetime.now(timezone.utc)
    
    if format_type == "iso":
        return now.isoformat()
    elif format_type == "unix":
        return int(now.timestamp())
    elif format_type in ("unix_ms", "epoch_ms", "ms"):
        # epoch_ms, ms 별칭 지원
        return int(now.timestamp() * 1000)
    elif format_type == "epoch":
        return now.timestamp()
    else:
        # Custom strftime format (e.g., "%Y-%m-%d")
        try:
            return now.strftime(format_type)
        except ValueError:
            # Invalid format string - fallback to ISO
            return now.isoformat()


# =============================================================================
# Strategy Registry - Maps strategy names to handler functions
# =============================================================================

STRATEGY_REGISTRY: Dict[str, Callable[[Any, Dict[str, Any]], Any]] = {
    # JSON/Object
    OperatorStrategy.JSON_PARSE: _json_parse,
    OperatorStrategy.JSON_STRINGIFY: _json_stringify,
    OperatorStrategy.DEEP_GET: _deep_get,
    OperatorStrategy.DEEP_SET: _deep_set,
    OperatorStrategy.PICK_FIELDS: _pick_fields,
    OperatorStrategy.OMIT_FIELDS: _omit_fields,
    OperatorStrategy.MERGE_OBJECTS: _merge_objects,
    OperatorStrategy.FLATTEN: _flatten,
    OperatorStrategy.UNFLATTEN: _unflatten,
    
    # List
    OperatorStrategy.LIST_MAP: _list_map,
    OperatorStrategy.LIST_FILTER: _list_filter,
    OperatorStrategy.LIST_REDUCE: _list_reduce,
    OperatorStrategy.LIST_SORT: _list_sort,
    OperatorStrategy.LIST_GROUP_BY: _list_group_by,
    OperatorStrategy.LIST_UNIQUE: _list_unique,
    OperatorStrategy.LIST_FIRST: _list_first,
    OperatorStrategy.LIST_LAST: _list_last,
    OperatorStrategy.LIST_SLICE: _list_slice,
    OperatorStrategy.LIST_CONCAT: _list_concat,
    OperatorStrategy.LIST_ZIP: _list_zip,
    OperatorStrategy.LIST_FIND: _list_find,
    OperatorStrategy.LIST_COUNT: _list_count,
    OperatorStrategy.LIST_REVERSE: _list_reverse,
    
    # String
    OperatorStrategy.STRING_TEMPLATE: _string_template,
    OperatorStrategy.STRING_SPLIT: _string_split,
    OperatorStrategy.STRING_JOIN: _string_join,
    OperatorStrategy.REGEX_EXTRACT: _regex_extract,
    OperatorStrategy.REGEX_REPLACE: _regex_replace,
    OperatorStrategy.REGEX_MATCH: _regex_match,
    OperatorStrategy.STRING_TRIM: _string_trim,
    OperatorStrategy.STRING_CASE: _string_case,
    OperatorStrategy.STRING_TRUNCATE: _string_truncate,
    OperatorStrategy.STRING_PAD: _string_pad,
    
    # Type
    OperatorStrategy.TO_INT: _to_int,
    OperatorStrategy.TO_FLOAT: _to_float,
    OperatorStrategy.TO_BOOL: _to_bool,
    OperatorStrategy.TO_STRING: _to_string,
    OperatorStrategy.TO_DATE: _to_date,
    OperatorStrategy.FORMAT_DATE: _format_date,
    OperatorStrategy.COERCE_TYPE: _coerce_type,
    
    # Control Flow
    OperatorStrategy.IF_ELSE: _if_else,
    OperatorStrategy.SWITCH_CASE: _switch_case,
    OperatorStrategy.DEFAULT_VALUE: _default_value,
    OperatorStrategy.TRY_CATCH: _try_catch,
    OperatorStrategy.ASSERT: _assert,
    
    # Encoding/Crypto
    OperatorStrategy.BASE64_ENCODE: _base64_encode,
    OperatorStrategy.BASE64_DECODE: _base64_decode,
    OperatorStrategy.URL_ENCODE: _url_encode,
    OperatorStrategy.URL_DECODE: _url_decode,
    OperatorStrategy.HASH_SHA256: _hash_sha256,
    OperatorStrategy.HASH_MD5: _hash_md5,
    OperatorStrategy.UUID_GENERATE: _uuid_generate,
    
    # Math
    OperatorStrategy.MATH_ROUND: _math_round,
    OperatorStrategy.MATH_FLOOR: _math_floor,
    OperatorStrategy.MATH_CEIL: _math_ceil,
    OperatorStrategy.MATH_ABS: _math_abs,
    OperatorStrategy.MATH_CLAMP: _math_clamp,
    OperatorStrategy.MATH_PERCENT: _math_percent,
    OperatorStrategy.MATH_EXPRESSION: _math_expression,
    
    # Utility
    OperatorStrategy.ECHO: _echo,
    OperatorStrategy.LOG: _log,
    OperatorStrategy.TIMESTAMP: _timestamp,
}

# Also register by string values for easier lookup
for strategy in OperatorStrategy:
    STRATEGY_REGISTRY[strategy.value] = STRATEGY_REGISTRY[strategy]


# =============================================================================
# Main Execution Function
# =============================================================================

def execute_strategy(
    strategy: Union[str, OperatorStrategy],
    input_value: Any,
    params: Optional[Dict[str, Any]] = None,
    state: Optional[Dict[str, Any]] = None,
) -> Any:
    """
    Execute a strategy transformation.
    
    Args:
        strategy: Strategy name or enum value
        input_value: Input value to transform
        params: Strategy-specific parameters
        state: Full workflow state (for template rendering)
        
    Returns:
        Transformed value
        
    Raises:
        ValueError: If strategy is not found
    """
    if params is None:
        params = {}
    
    # Resolve strategy handler
    if isinstance(strategy, OperatorStrategy):
        strategy_key = strategy
    else:
        strategy_key = strategy.lower()
    
    handler = STRATEGY_REGISTRY.get(strategy_key)
    
    if not handler:
        available = [s.value for s in OperatorStrategy]
        raise ValueError(f"Unknown strategy: {strategy}. Available: {available}")
    
    # Execute with error handling
    try:
        result = handler(input_value, params)
        logger.debug(f"Strategy {strategy} executed: {type(input_value).__name__} -> {type(result).__name__}")
        return result
    except Exception as e:
        logger.error(f"Strategy {strategy} failed: {e}")
        raise


def get_available_strategies() -> List[str]:
    """Get list of all available strategy names."""
    return [s.value for s in OperatorStrategy]


def get_strategy_info(strategy: Union[str, OperatorStrategy]) -> Dict[str, Any]:
    """Get information about a specific strategy."""
    if isinstance(strategy, str):
        try:
            strategy = OperatorStrategy(strategy)
        except ValueError:
            return {"error": f"Unknown strategy: {strategy}"}
    
    handler = STRATEGY_REGISTRY.get(strategy)
    if not handler:
        return {"error": f"No handler for strategy: {strategy}"}
    
    return {
        "name": strategy.value,
        "description": handler.__doc__ or "No description",
        "category": strategy.value.split("_")[0] if "_" in strategy.value else "utility",
    }
