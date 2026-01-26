"""
Media Handlers Package
======================

미디어 파일 업로드 및 처리를 위한 핸들러 패키지.
"""

from .upload_handler import lambda_handler as upload_lambda_handler

__all__ = [
    'upload_lambda_handler',
]
