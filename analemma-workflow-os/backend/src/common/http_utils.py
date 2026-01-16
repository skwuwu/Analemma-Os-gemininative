"""
HTTP utilities for Lambda API responses.

Consolidates common headers and response helpers used across Lambda handlers.
"""

import json
import os
from typing import Any, Dict, Optional

from src.common.json_utils import DecimalEncoder


def get_cors_headers(origin: Optional[str] = None) -> Dict[str, str]:
    """
    Get full CORS headers for API responses.
    
    Args:
        origin: Override the allowed origin. Defaults to CLOUDFRONT_DOMAIN env var or "*"
        
    Returns:
        Dictionary with CORS headers
    """
    return {
        "Content-Type": "application/json",
        "Access-Control-Allow-Origin": origin or os.environ.get("CLOUDFRONT_DOMAIN", "*"),
        "Access-Control-Allow-Headers": "Authorization, Content-Type",
        "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
        "Access-Control-Allow-Credentials": "true"
    }


def get_json_headers() -> Dict[str, str]:
    """
    Get minimal JSON content-type headers.
    
    Use this for internal responses or when CORS is handled elsewhere.
    
    Returns:
        Dictionary with Content-Type header
    """
    return {"Content-Type": "application/json"}


# Pre-built header constants for common use cases
JSON_HEADERS = get_json_headers()
CORS_HEADERS = get_cors_headers()


def build_response(
    status_code: int,
    body: Any,
    headers: Optional[Dict[str, str]] = None,
    use_cors: bool = True
) -> Dict[str, Any]:
    """
    Build a standard API Gateway Lambda response.
    
    Args:
        status_code: HTTP status code
        body: Response body (will be JSON-serialized)
        headers: Additional headers to include
        use_cors: Whether to include CORS headers (default: True)
        
    Returns:
        API Gateway compatible response dictionary
    """
    response_headers = get_cors_headers() if use_cors else get_json_headers()
    if headers:
        response_headers.update(headers)
    
    return {
        "statusCode": status_code,
        "headers": response_headers,
        "body": json.dumps(body, cls=DecimalEncoder, ensure_ascii=False)
    }


def success_response(body: Any, headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    """Build a 200 OK response."""
    return build_response(200, body, headers)


def created_response(body: Any, headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    """Build a 201 Created response."""
    return build_response(201, body, headers)


def bad_request_response(message: str, details: Optional[Dict] = None) -> Dict[str, Any]:
    """Build a 400 Bad Request response."""
    body = {"error": message}
    if details:
        body["details"] = details
    return build_response(400, body)


def unauthorized_response(message: str = "Unauthorized") -> Dict[str, Any]:
    """Build a 401 Unauthorized response."""
    return build_response(401, {"error": message})


def forbidden_response(message: str = "Forbidden") -> Dict[str, Any]:
    """Build a 403 Forbidden response."""
    return build_response(403, {"error": message})


def not_found_response(message: str = "Not found") -> Dict[str, Any]:
    """Build a 404 Not Found response."""
    return build_response(404, {"error": message})


def internal_error_response(message: str = "Internal server error") -> Dict[str, Any]:
    """Build a 500 Internal Server Error response."""
    return build_response(500, {"error": message})
