"""
Common utility functions related to authentication
Integrate JWT token verification and user ID extraction logic to improve reusability
"""

import os
import logging
import time
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

# JWKS cache global variables
_JWKS_CACHE: Optional[Dict[str, Any]] = None
_JWKS_CACHE_FETCHED_AT: float = 0.0
_JWKS_CACHE_TTL = int(os.getenv("JWKS_CACHE_TTL_SECONDS", "3600"))


def _fetch_jwks(jwks_url: str) -> Dict[str, Any]:
    """
    Fetch JWKS from the given URL. (Minimize dependencies by using urllib.request)

    Args:
        jwks_url: JWKS endpoint URL

    Returns:
        JWKS data

    Raises:
        RuntimeError: If HTTP request fails
    """
    try:
        import urllib.request
        import json

        with urllib.request.urlopen(jwks_url, timeout=5) as response:
            data = json.loads(response.read().decode('utf-8'))
            return data
    except Exception as e:
        logger.error(f"Failed to fetch JWKS from {jwks_url}: {e}")
        raise RuntimeError(f"Failed to fetch JWKS: {e}")


def _get_cached_jwks(jwks_url: str) -> Dict[str, Any]:
    """
    Returns cached JWKS and refreshes if TTL has expired.

    Args:
        jwks_url: JWKS endpoint URL

    Returns:
        Cached JWKS data (kid->key mapping)
    """
    global _JWKS_CACHE, _JWKS_CACHE_FETCHED_AT
    now = time.time()
    if _JWKS_CACHE is None or (now - _JWKS_CACHE_FETCHED_AT) > _JWKS_CACHE_TTL:
        jwks = _fetch_jwks(jwks_url)
        # Index by kid for fast lookup
        key_map: Dict[str, Any] = {}
        for k in jwks.get("keys", []):
            kid = k.get("kid")
            if kid:
                key_map[kid] = k
        _JWKS_CACHE = {"keys": key_map}
        _JWKS_CACHE_FETCHED_AT = now
    return _JWKS_CACHE


def validate_token(token: str) -> Dict[str, Any]:
    """
    Validates JWT token using Cognito JWKS. (Lightweight using PyJWT)

    Args:
        token: JWT token to validate

    Returns:
        Decoded token claims

    Raises:
        RuntimeError: If required libraries or environment variables are missing
        ValueError: If token validation fails
    """
    try:
        import jwt
        from jwt.algorithms import RSAAlgorithm
    except Exception:
        raise RuntimeError("'PyJWT' is required for JWT validation. Please add 'PyJWT' to your Lambda dependencies.")

    # Determine issuer URL (environment variable takes priority, otherwise assemble from region and pool ID)
    issuer_url = os.environ.get("COGNITO_ISSUER_URL")

    if issuer_url:
        # If URL exists in environment variable, use as is (safest)
        issuer = issuer_url.rstrip('/')
    else:
        # If URL doesn't exist, assemble directly from Region and Pool ID
        region = os.environ.get("COGNITO_REGION")
        user_pool = os.environ.get("USER_POOL_ID")

        if not region or not user_pool:
            raise RuntimeError("COGNITO_ISSUER_URL or (COGNITO_REGION + USER_POOL_ID) must be set.")

        issuer = f"https://cognito-idp.{region}.amazonaws.com/{user_pool}"

    app_client = os.environ.get("APP_CLIENT_ID")
    if not app_client:
        raise RuntimeError("APP_CLIENT_ID environment variable must be set.")

    jwks_url = f"{issuer}/.well-known/jwks.json"

    # Extract kid from token header (use get_unverified_header in PyJWT)
    try:
        unverified_header = jwt.get_unverified_header(token)
        kid = unverified_header.get("kid")
    except Exception as e:
        raise ValueError(f"Invalid token header: {e}")

    # Retrieve JWKS (using cache)
    jwks = _get_cached_jwks(jwks_url)
    key_map = jwks.get("keys", {})
    key_data = key_map.get(kid)

    if not key_data:
        # Retry after cache refresh (once)
        global _JWKS_CACHE_FETCHED_AT
        _JWKS_CACHE_FETCHED_AT = 0
        jwks = _get_cached_jwks(jwks_url)
        key_map = jwks.get("keys", {})
        key_data = key_map.get(kid)
        if not key_data:
            raise ValueError("Public key not found in JWKS for kid: %s" % kid)

    # In PyJWT, RSA public key must be constructed directly
    try:
        # Create RSA public key
        public_key = RSAAlgorithm.from_jwk(key_data)

        # Validate and decode token
        decoded = jwt.decode(
            token,
            public_key,
            algorithms=["RS256"],
            issuer=issuer,
            audience=app_client,
        )
        return decoded
    except Exception as e:
        raise ValueError(f"Token validation failed: {e}")


def extract_owner_id_from_token(token: str) -> str:
    """
    Extract owner_id from JWT token.

    Args:
        token: JWT token

    Returns:
        User ID (sub claim)

    Raises:
        ValueError: If token is invalid or sub claim is missing
    """
    token_claims = validate_token(token)
    owner_id = token_claims.get('sub')
    if not owner_id:
        raise ValueError("Token does not contain required 'sub' claim")
    return owner_id


def extract_owner_id_from_event(event: Dict[str, Any]) -> Optional[str]:
    """
    Extract owner_id from API Gateway event.
    
    1. Use authorizer claims from requestContext (already verified by API Gateway) as priority
    2. If not available, parse and verify Authorization header (Fallback)

    Args:
        event: API Gateway event

    Returns:
        User ID or None (if authentication fails)
    """
    # Fast Path: Use Claims already verified by API Gateway
    try:
        request_context = event.get("requestContext", {})
        authorizer = request_context.get("authorizer", {})
        
        # HTTP API (JWT Authorizer) structure: authorizer -> jwt -> claims -> sub
        jwt_auth = authorizer.get("jwt", {})
        unique_id = jwt_auth.get("claims", {}).get("sub")
        
        # REST API (Cognito Authorizer) or simple Lambda Authorizer structure: authorizer -> claims -> sub
        if not unique_id:
            claims = authorizer.get("claims", {})
            unique_id = claims.get("sub")
            
        # Direct authorizer output (Lambda Authorizer returning context)
        if not unique_id:
            unique_id = authorizer.get("sub") or authorizer.get("principalId")

        if unique_id:
            # logger.debug(f"Using owner_id from requestContext: {unique_id}")
            return unique_id

    except Exception as e:
        logger.warning(f"Failed to extract owner_id from requestContext: {e}")
        # Fallback continues below

    # Slow Path: Parse and verify signature directly from header (for local test compatibility, etc.)
    try:
        headers = event.get("headers") or {}
        # Lowercase header names to accommodate various variations
        headers_low = {k.lower(): v for k, v in headers.items()} if isinstance(headers, dict) else {}
        auth_header = headers_low.get("authorization")

        if not auth_header or not isinstance(auth_header, str) or not auth_header.lower().startswith("bearer "):
            logger.warning("Missing or invalid Authorization header")
            return None

        token = auth_header.split()[1]
        return extract_owner_id_from_token(token)
    except Exception as e:
        logger.error(f"Failed to extract owner_id from src.event headers: {e}")
        return None


def extract_owner_id_from_fastapi_request(request) -> Optional[str]:
    """
    Extract owner_id from FastAPI Request object.
    
    Args:
        request: FastAPI Request object
        
    Returns:
        User ID or None (if authentication fails)
    """
    try:
        # Convert FastAPI headers to dict format expected by extract_owner_id_from_event
        headers = dict(request.headers)
        fake_event = {"headers": headers}
        return extract_owner_id_from_event(fake_event)
    except Exception as e:
        logger.error(f"Failed to extract owner_id from FastAPI request: {e}")
        return None


def require_authentication(event: Dict[str, Any]) -> str:
    """
    Extract and require owner_id from API Gateway event.
    
    This function enforces authentication by raising a ValueError if 
    the owner_id cannot be extracted from the event.
    
    Args:
        event: API Gateway event
        
    Returns:
        User ID (owner_id)
        
    Raises:
        ValueError: If authentication fails or owner_id cannot be extracted
    """
    owner_id = extract_owner_id_from_event(event)
    
    if not owner_id:
        logger.error("Authentication required but no valid credentials found")
        raise ValueError("Authentication required: No valid credentials provided")
    
    return owner_id