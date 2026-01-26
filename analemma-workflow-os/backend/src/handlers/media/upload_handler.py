"""
Media Upload Handler
====================

이미지/영상 파일을 S3에 업로드하고 URI를 반환하는 Lambda 핸들러.
워크플로우 실행 전 미디어 파일을 안전하게 업로드하는 전용 엔드포인트.

Endpoints:
    POST /api/media/upload - 직접 업로드 (base64 또는 multipart)
    POST /api/media/presign - Presigned URL 발급 (대용량 파일용)

Architecture:
    ┌─────────────┐    ┌──────────────────┐    ┌─────────┐
    │  Frontend   │───▶│ MediaUploadLambda │───▶│   S3    │
    └─────────────┘    └──────────────────┘    └─────────┘
           │                    │
           │                    ▼
           │           { "media_uri": "s3://..." }
           │                    │
           ▼                    ▼
    ┌─────────────────────────────────────────┐
    │  RunWorkflow (initial_state.image_uri)  │
    └─────────────────────────────────────────┘
"""

import json
import os
import base64
import uuid
import hashlib
import mimetypes
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional, Tuple
import boto3
from botocore.exceptions import ClientError

# Logger setup
import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# AWS Clients (lazy init)
_s3_client = None
_dynamodb = None

def get_s3_client():
    global _s3_client
    if _s3_client is None:
        _s3_client = boto3.client('s3', region_name=os.environ.get('AWS_REGION', 'ap-northeast-2'))
    return _s3_client

def get_dynamodb():
    global _dynamodb
    if _dynamodb is None:
        _dynamodb = boto3.resource('dynamodb', region_name=os.environ.get('AWS_REGION', 'ap-northeast-2'))
    return _dynamodb

# Configuration
MEDIA_BUCKET = os.environ.get('MEDIA_BUCKET') or os.environ.get('WORKFLOW_STATE_BUCKET') or 'analemma-workflows-dev'
MAX_FILE_SIZE_MB = int(os.environ.get('MAX_UPLOAD_SIZE_MB', '50'))  # 50MB default
MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024
PRESIGN_EXPIRY_SECONDS = int(os.environ.get('PRESIGN_EXPIRY_SECONDS', '3600'))  # 1 hour

# Allowed MIME types
ALLOWED_MIME_TYPES = {
    # Images
    'image/jpeg': '.jpg',
    'image/png': '.png',
    'image/gif': '.gif',
    'image/webp': '.webp',
    'image/heic': '.heic',
    'image/heif': '.heif',
    'image/bmp': '.bmp',
    # Videos
    'video/mp4': '.mp4',
    'video/quicktime': '.mov',
    'video/x-msvideo': '.avi',
    'video/webm': '.webm',
    'video/x-matroska': '.mkv',
    # Documents (for Gemini 1.5+)
    'application/pdf': '.pdf',
}


def _cors_headers() -> Dict[str, str]:
    """Standard CORS headers"""
    return {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type,Authorization,X-Owner-Id',
        'Access-Control-Allow-Methods': 'POST,OPTIONS',
        'Content-Type': 'application/json'
    }


def _error_response(status_code: int, message: str, error_code: str = None) -> Dict[str, Any]:
    """Standard error response"""
    body = {
        'success': False,
        'error': message,
        'error_code': error_code or 'UPLOAD_ERROR'
    }
    return {
        'statusCode': status_code,
        'headers': _cors_headers(),
        'body': json.dumps(body)
    }


def _success_response(data: Dict[str, Any]) -> Dict[str, Any]:
    """Standard success response"""
    return {
        'statusCode': 200,
        'headers': _cors_headers(),
        'body': json.dumps({
            'success': True,
            **data
        })
    }


def _get_content_type(filename: str, provided_type: str = None) -> str:
    """Determine content type from filename or provided type"""
    if provided_type and provided_type in ALLOWED_MIME_TYPES:
        return provided_type
    
    # Guess from filename
    guessed_type, _ = mimetypes.guess_type(filename)
    if guessed_type and guessed_type in ALLOWED_MIME_TYPES:
        return guessed_type
    
    # Default based on extension
    ext = os.path.splitext(filename)[1].lower()
    ext_to_mime = {v: k for k, v in ALLOWED_MIME_TYPES.items()}
    return ext_to_mime.get(ext, 'application/octet-stream')


def _generate_s3_key(owner_id: str, filename: str, content_type: str, content_hash: str = None) -> str:
    """
    Generate S3 key for uploaded file.
    
    Structure: media/{owner_id}/{date}/{hash}_{uuid}.{ext}
    """
    ext = ALLOWED_MIME_TYPES.get(content_type, '.bin')
    date_prefix = datetime.now(timezone.utc).strftime('%Y/%m/%d')
    
    # Include content hash for deduplication
    hash_prefix = content_hash[:8] if content_hash else uuid.uuid4().hex[:8]
    unique_id = uuid.uuid4().hex[:8]
    
    # Sanitize owner_id
    safe_owner = owner_id.replace('/', '_').replace('..', '_')[:64]
    
    return f"media/{safe_owner}/{date_prefix}/{hash_prefix}_{unique_id}{ext}"


def _compute_content_hash(data: bytes) -> str:
    """Compute SHA-256 hash of content for deduplication"""
    return hashlib.sha256(data).hexdigest()


def _decode_base64_data(data_str: str) -> Tuple[bytes, str]:
    """
    Decode base64 data, handling data URI format.
    
    Returns: (bytes, content_type)
    """
    content_type = 'application/octet-stream'
    
    # Handle data URI format: data:image/png;base64,xxxxx
    if data_str.startswith('data:'):
        try:
            header, encoded = data_str.split(',', 1)
            # Extract MIME type
            mime_part = header.split(';')[0]  # data:image/png
            content_type = mime_part.replace('data:', '')
            data_str = encoded
        except ValueError:
            pass
    
    # Decode base64
    try:
        data_bytes = base64.b64decode(data_str)
        return data_bytes, content_type
    except Exception as e:
        raise ValueError(f"Invalid base64 encoding: {e}")


def handle_direct_upload(event: Dict[str, Any], owner_id: str) -> Dict[str, Any]:
    """
    Handle direct file upload (base64 encoded).
    
    Request body:
    {
        "file_data": "base64-encoded-data or data:image/png;base64,xxx",
        "filename": "receipt.jpg",  // optional
        "content_type": "image/jpeg"  // optional
    }
    
    Response:
    {
        "success": true,
        "media_uri": "s3://bucket/media/owner/2026/01/26/abc123.jpg",
        "media_url": "https://bucket.s3.region.amazonaws.com/...",  // public URL if enabled
        "content_type": "image/jpeg",
        "size_bytes": 123456,
        "content_hash": "sha256:abc123..."
    }
    """
    try:
        # Parse body
        body = event.get('body', '{}')
        if isinstance(body, str):
            # Check if base64 encoded body (API Gateway binary)
            if event.get('isBase64Encoded'):
                body = base64.b64decode(body).decode('utf-8')
            body = json.loads(body)
        
        file_data = body.get('file_data') or body.get('data') or body.get('image_data')
        filename = body.get('filename', 'upload')
        provided_content_type = body.get('content_type') or body.get('mime_type')
        
        if not file_data:
            return _error_response(400, "Missing 'file_data' in request body", "MISSING_DATA")
        
        # Decode base64
        try:
            data_bytes, detected_type = _decode_base64_data(file_data)
        except ValueError as e:
            return _error_response(400, str(e), "INVALID_BASE64")
        
        # Determine content type
        content_type = provided_content_type or detected_type
        if content_type not in ALLOWED_MIME_TYPES:
            return _error_response(
                400, 
                f"Unsupported content type: {content_type}. Allowed: {list(ALLOWED_MIME_TYPES.keys())}",
                "UNSUPPORTED_TYPE"
            )
        
        # Check file size
        if len(data_bytes) > MAX_FILE_SIZE_BYTES:
            return _error_response(
                400,
                f"File too large: {len(data_bytes)} bytes. Maximum: {MAX_FILE_SIZE_BYTES} bytes ({MAX_FILE_SIZE_MB}MB)",
                "FILE_TOO_LARGE"
            )
        
        # Compute hash for deduplication
        content_hash = _compute_content_hash(data_bytes)
        
        # Generate S3 key
        s3_key = _generate_s3_key(owner_id, filename, content_type, content_hash)
        
        # Upload to S3
        s3 = get_s3_client()
        try:
            s3.put_object(
                Bucket=MEDIA_BUCKET,
                Key=s3_key,
                Body=data_bytes,
                ContentType=content_type,
                Metadata={
                    'owner_id': owner_id,
                    'original_filename': filename[:256],
                    'content_hash': content_hash,
                    'uploaded_at': datetime.now(timezone.utc).isoformat()
                }
            )
        except ClientError as e:
            logger.error(f"S3 upload failed: {e}")
            return _error_response(500, "Failed to upload file to storage", "S3_ERROR")
        
        # Build response
        s3_uri = f"s3://{MEDIA_BUCKET}/{s3_key}"
        
        logger.info(f"✅ Media uploaded: {s3_uri} ({len(data_bytes)} bytes) by {owner_id}")
        
        return _success_response({
            'media_uri': s3_uri,
            's3_bucket': MEDIA_BUCKET,
            's3_key': s3_key,
            'content_type': content_type,
            'size_bytes': len(data_bytes),
            'content_hash': f"sha256:{content_hash}"
        })
        
    except json.JSONDecodeError as e:
        return _error_response(400, f"Invalid JSON body: {e}", "INVALID_JSON")
    except Exception as e:
        logger.exception(f"Upload error: {e}")
        return _error_response(500, f"Internal error: {str(e)}", "INTERNAL_ERROR")


def handle_presign_request(event: Dict[str, Any], owner_id: str) -> Dict[str, Any]:
    """
    Generate presigned URL for direct S3 upload (large files).
    
    Request body:
    {
        "filename": "large_video.mp4",
        "content_type": "video/mp4",
        "size_bytes": 104857600  // optional, for validation
    }
    
    Response:
    {
        "success": true,
        "upload_url": "https://bucket.s3.amazonaws.com/...?X-Amz-Signature=...",
        "media_uri": "s3://bucket/media/owner/2026/01/26/abc123.mp4",
        "expires_in_seconds": 3600,
        "fields": {}  // for POST-based upload
    }
    """
    try:
        # Parse body
        body = event.get('body', '{}')
        if isinstance(body, str):
            body = json.loads(body)
        
        filename = body.get('filename', 'upload')
        content_type = body.get('content_type') or _get_content_type(filename)
        size_bytes = body.get('size_bytes')
        
        # Validate content type
        if content_type not in ALLOWED_MIME_TYPES:
            return _error_response(
                400,
                f"Unsupported content type: {content_type}",
                "UNSUPPORTED_TYPE"
            )
        
        # Validate size if provided
        if size_bytes and size_bytes > MAX_FILE_SIZE_BYTES:
            return _error_response(
                400,
                f"File too large: {size_bytes} bytes. Maximum: {MAX_FILE_SIZE_BYTES} bytes",
                "FILE_TOO_LARGE"
            )
        
        # Generate S3 key
        s3_key = _generate_s3_key(owner_id, filename, content_type)
        
        # Generate presigned URL
        s3 = get_s3_client()
        try:
            presigned_url = s3.generate_presigned_url(
                'put_object',
                Params={
                    'Bucket': MEDIA_BUCKET,
                    'Key': s3_key,
                    'ContentType': content_type,
                    'Metadata': {
                        'owner_id': owner_id,
                        'original_filename': filename[:256]
                    }
                },
                ExpiresIn=PRESIGN_EXPIRY_SECONDS
            )
        except ClientError as e:
            logger.error(f"Presign generation failed: {e}")
            return _error_response(500, "Failed to generate upload URL", "PRESIGN_ERROR")
        
        s3_uri = f"s3://{MEDIA_BUCKET}/{s3_key}"
        
        logger.info(f"✅ Presigned URL generated: {s3_uri} for {owner_id}")
        
        return _success_response({
            'upload_url': presigned_url,
            'media_uri': s3_uri,
            's3_bucket': MEDIA_BUCKET,
            's3_key': s3_key,
            'content_type': content_type,
            'expires_in_seconds': PRESIGN_EXPIRY_SECONDS,
            'http_method': 'PUT',
            'required_headers': {
                'Content-Type': content_type
            }
        })
        
    except json.JSONDecodeError as e:
        return _error_response(400, f"Invalid JSON body: {e}", "INVALID_JSON")
    except Exception as e:
        logger.exception(f"Presign error: {e}")
        return _error_response(500, f"Internal error: {str(e)}", "INTERNAL_ERROR")


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Media Upload Lambda Handler
    
    Routes:
        POST /api/media/upload  - Direct upload (base64)
        POST /api/media/presign - Get presigned URL
        OPTIONS *               - CORS preflight
    """
    # Handle CORS preflight
    http_method = event.get('httpMethod') or event.get('requestContext', {}).get('http', {}).get('method', 'POST')
    
    if http_method == 'OPTIONS':
        return {
            'statusCode': 200,
            'headers': _cors_headers(),
            'body': ''
        }
    
    # Extract path
    path = event.get('path') or event.get('rawPath') or '/upload'
    
    # Extract owner_id from headers or auth context
    headers = event.get('headers', {})
    # Normalize header keys (API Gateway may lowercase them)
    headers_lower = {k.lower(): v for k, v in headers.items()}
    
    owner_id = (
        headers_lower.get('x-owner-id') or
        headers.get('X-Owner-Id') or
        event.get('requestContext', {}).get('authorizer', {}).get('ownerId') or
        event.get('requestContext', {}).get('authorizer', {}).get('claims', {}).get('sub') or
        'anonymous'
    )
    
    logger.info(f"📤 Media upload request: {path} by {owner_id}")
    
    # Route to handler
    if '/presign' in path:
        return handle_presign_request(event, owner_id)
    else:
        return handle_direct_upload(event, owner_id)
