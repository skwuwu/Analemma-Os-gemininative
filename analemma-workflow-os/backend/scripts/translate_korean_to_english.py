#!/usr/bin/env python3
"""
🌐 Analemma OS - Korean to English Translation Script (v2.0)
=============================================================

High-performance batch translation script for converting Korean comments 
and docstrings to English while preserving code integrity.

v2.0 Enhancements:
- Full-file context translation (no unit splitting)
- Async parallel processing with asyncio
- Post-translation integrity verification (AST + diff)
- Linux Kernel coding style for comments

Features:
- Extracts only comments and docstrings (code logic untouched)
- Preserves Ring Protection tokens ([RING-0], [RING-3], etc.)
- Preserves variable placeholders ({variable}, $.path, etc.)
- Enforces Analemma-specific terminology glossary
- Uses Gemini API for high-quality technical translation

Usage:
    python translate_korean_to_english.py [options]

    Options:
        --dry-run       Preview changes without modifying files
        --file PATH     Translate a specific file only
        --dir PATH      Translate all Python files in directory
        --verbose       Show detailed translation progress
        --concurrency N Number of parallel translations (default: 5)
        --skip-verify   Skip AST verification after translation

Author: Analemma OS Team
License: BSL 1.1
"""

import os
import re
import sys
import ast
import json
import asyncio
import argparse
import time
import difflib
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Set
from dataclasses import dataclass, field
from concurrent.futures import ThreadPoolExecutor
import hashlib

# ============================================================================
# Configuration
# ============================================================================

# Analemma-specific terminology glossary (Korean -> English)
GLOSSARY = {
    # Core OS Concepts
    "커널": "Kernel",
    "세그먼트": "Segment",
    "워크플로우": "Workflow",
    "상태": "State",
    "노드": "Node",
    "브랜치": "Branch",
    "병렬": "Parallel",
    "직렬": "Sequential",
    "동시성": "Concurrency",
    
    # Ring Protection
    "링 보호": "Ring Protection",
    "권한 분리": "Privilege Isolation",
    "프롬프트 주입": "Prompt Injection",
    "보안 위반": "Security Violation",
    "위험 도구": "Dangerous Tool",
    "신뢰 불가": "Untrusted",
    "불변": "Immutable",
    
    # Execution & Scheduling
    "실행": "Execution",
    "스케줄러": "Scheduler",
    "스케줄링": "Scheduling",
    "재시도": "Retry",
    "오프로딩": "Offloading",
    "분할": "Split",
    "병합": "Merge",
    "집계": "Aggregation",
    
    # State Management
    "상태 관리": "State Management",
    "상태 백": "State Bag",
    "히스토리": "History",
    "체크포인트": "Checkpoint",
    "복구": "Recovery",
    "자가 치유": "Self-Healing",
    
    # Infrastructure
    "람다": "Lambda",
    "분산": "Distributed",
    "맵리듀스": "Map-Reduce",
    "배치": "Batch",
    "비동기": "Async",
    "동기": "Sync",
    
    # Error Handling
    "예외": "Exception",
    "에러": "Error",
    "실패": "Failure",
    "성공": "Success",
    "대기": "Wait",
    "시간 초과": "Timeout",
    
    # HITP (Human-in-the-Process)
    "사용자 입력": "User Input",
    "콜백": "Callback",
    "일시 중지": "Pause",
    "재개": "Resume",
    
    # Metrics & Monitoring
    "메트릭": "Metrics",
    "로깅": "Logging",
    "모니터링": "Monitoring",
    "추적": "Tracing",
    
    # Common Terms
    "초기화": "Initialize",
    "설정": "Configuration",
    "옵션": "Option",
    "기본값": "Default",
    "최대": "Maximum",
    "최소": "Minimum",
    "임계값": "Threshold",
    "제한": "Limit",
    "활성화": "Enable",
    "비활성화": "Disable",
    "검증": "Validation",
    "정화": "Sanitization",
    "추정": "Estimation",
    "계산": "Calculation",
    "처리": "Processing",
    "반환": "Return",
    "인자": "Argument",
    "매개변수": "Parameter",
    "호출": "Call",
    "응답": "Response",
    "요청": "Request",
    "저장": "Save",
    "로드": "Load",
    "삭제": "Delete",
    "생성": "Create",
    "수정": "Modify",
    "업데이트": "Update",
    "확인": "Check",
    "탐지": "Detection",
    "차단": "Block",
    "허용": "Allow",
    "거부": "Deny",
    "접근": "Access",
    "권한": "Permission",
    "인증": "Authentication",
    "인가": "Authorization",
}

# Patterns to preserve (never translate)
PRESERVE_PATTERNS = [
    r'\[RING-\d[^\]]*\]',           # Ring Protection tags
    r'\[KERNEL\]',                   # Kernel tags
    r'\[IMMUTABLE\]',                # Immutable tags
    r'\{[a-zA-Z_][a-zA-Z0-9_]*\}',   # Variable placeholders {var}
    r'\$\.[a-zA-Z_.]+',              # JSONPath expressions $.path
    r'\$\{[^}]+\}',                  # CloudFormation references ${Ref}
    r'arn:aws:[^\s]+',               # AWS ARNs
    r'https?://[^\s]+',              # URLs
]

# Korean character detection regex
KOREAN_PATTERN = re.compile(r'[가-힣]+')

# ============================================================================
# Full-File Translation Prompt (v2.0 Enhancement)
# ============================================================================

FULL_FILE_TRANSLATION_PROMPT = """You are a professional software localization engineer specializing in translating Korean code comments to English.

## YOUR TASK
Translate ALL Korean text (comments, docstrings, string literals in error messages/logs) in the following Python code to English.

## CRITICAL RULES - VIOLATION WILL CORRUPT THE CODEBASE

### 1. CODE INTEGRITY (ABSOLUTE)
- DO NOT modify any Python code logic, variable names, function names, class names
- DO NOT change indentation, spacing, or line structure
- DO NOT add or remove any lines of code
- DO NOT modify import statements
- Preserve ALL special tokens: [RING-0], [RING-3], {variable}, $.path, ARNs, URLs

### 2. TRANSLATION STYLE (Linux Kernel Style)
- Use imperative mood: "Initialize state" not "Initializes the state"
- Be concise: Remove unnecessary adjectives and filler words
- Technical precision: "State" vs "Status" must match the context
- Consistent terminology: Use the glossary below

### 3. GLOSSARY (MUST USE)
{glossary_text}

### 4. WHAT TO TRANSLATE
- Single-line comments starting with #
- Multi-line docstrings (triple quotes)
- Korean text in error messages, log strings, and user-facing strings
- Korean text in f-strings (translate the Korean parts only)

### 5. WHAT TO PRESERVE EXACTLY
- All code structure and logic
- Variable names, function names, class names
- Import statements
- Special tokens: [RING-0], [RING-3], [KERNEL], [IMMUTABLE]
- Placeholders: {variable_name}, $.json.path, ${CloudFormation}
- AWS ARNs, URLs, file paths
- Emoji characters (🛡️, ✅, ❌, etc.)

## OUTPUT FORMAT
Return ONLY the translated Python code. No explanations, no markdown code blocks, no additional text.
The output must be valid Python that can be parsed by ast.parse().

---
## PYTHON CODE TO TRANSLATE:

{code_content}
"""


@dataclass
class TranslationResult:
    """Result of translating a file"""
    file_path: str
    success: bool = True
    error_message: Optional[str] = None
    original_content: str = ""
    translated_content: str = ""
    korean_count_before: int = 0
    korean_count_after: int = 0
    ast_valid: bool = True
    code_diff_clean: bool = True
    diff_report: str = ""


@dataclass
class IntegrityReport:
    """Report of post-translation integrity check"""
    ast_valid: bool
    ast_error: Optional[str] = None
    code_unchanged: bool = True
    changed_code_lines: List[str] = field(default_factory=list)
    non_comment_changes: int = 0


class CodeIntegrityChecker:
    """
    Post-translation integrity verification.
    
    Ensures:
    1. AST is valid (syntax check)
    2. Only comments/docstrings changed (diff analysis)
    """
    
    @staticmethod
    def verify_ast(code: str) -> Tuple[bool, Optional[str]]:
        """
        Verify Python code is syntactically valid.
        
        Returns:
            (is_valid, error_message)
        """
        try:
            ast.parse(code)
            return True, None
        except SyntaxError as e:
            return False, f"Line {e.lineno}: {e.msg}"
    
    @staticmethod
    def extract_code_structure(code: str) -> str:
        """
        Extract code structure (remove comments/docstrings) for comparison.
        
        This creates a 'skeleton' of the code for diff comparison.
        """
        lines = []
        in_docstring = False
        docstring_char = None
        
        for line in code.split('\n'):
            stripped = line.strip()
            
            # Track docstring state
            if not in_docstring:
                if stripped.startswith('"""') or stripped.startswith("'''"):
                    docstring_char = stripped[:3]
                    if stripped.count(docstring_char) >= 2 and len(stripped) > 3:
                        # Single-line docstring
                        lines.append(line.split(docstring_char)[0] + docstring_char + "..." + docstring_char)
                        continue
                    in_docstring = True
                    lines.append(line.split(docstring_char)[0] + docstring_char + "...")
                    continue
            else:
                if docstring_char in stripped:
                    in_docstring = False
                    lines.append("..." + docstring_char)
                continue
            
            # Remove inline comments for comparison
            if '#' in line and not stripped.startswith('#'):
                code_part = line.split('#')[0]
                lines.append(code_part.rstrip())
            elif stripped.startswith('#'):
                lines.append('')  # Comment-only line
            else:
                lines.append(line)
        
        return '\n'.join(lines)
    
    @classmethod
    def verify_code_unchanged(cls, original: str, translated: str) -> Tuple[bool, List[str]]:
        """
        Verify that only comments/docstrings changed, not code logic.
        
        Returns:
            (is_unchanged, list_of_changed_code_lines)
        """
        original_skeleton = cls.extract_code_structure(original)
        translated_skeleton = cls.extract_code_structure(translated)
        
        if original_skeleton == translated_skeleton:
            return True, []
        
        # Find differences
        diff = list(difflib.unified_diff(
            original_skeleton.split('\n'),
            translated_skeleton.split('\n'),
            lineterm='',
            n=0
        ))
        
        changed_lines = [
            line for line in diff 
            if line.startswith('+') or line.startswith('-')
            if not line.startswith('+++') and not line.startswith('---')
        ]
        
        return len(changed_lines) == 0, changed_lines
    
    @classmethod
    def full_check(cls, original: str, translated: str) -> IntegrityReport:
        """Perform full integrity check"""
        report = IntegrityReport(ast_valid=True)
        
        # 1. AST validation
        ast_valid, ast_error = cls.verify_ast(translated)
        report.ast_valid = ast_valid
        report.ast_error = ast_error
        
        if not ast_valid:
            return report
        
        # 2. Code structure comparison
        code_unchanged, changed_lines = cls.verify_code_unchanged(original, translated)
        report.code_unchanged = code_unchanged
        report.changed_code_lines = changed_lines
        report.non_comment_changes = len(changed_lines)
        
        return report


class AnalemmaTranslator:
    """
    Korean to English translator for Analemma OS codebase (v2.0).
    
    Enhancements:
    - Full-file context translation
    - Async parallel processing
    - Post-translation integrity verification
    """
    
    def __init__(
        self, 
        api_key: Optional[str] = None, 
        dry_run: bool = False, 
        verbose: bool = False,
        concurrency: int = 5,
        skip_verify: bool = False
    ):
        self.api_key = api_key or os.environ.get("GOOGLE_API_KEY") or os.environ.get("GEMINI_API_KEY")
        self.dry_run = dry_run
        self.verbose = verbose
        self.concurrency = concurrency
        self.skip_verify = skip_verify
        self._genai = None
        self._model = None
        
        # Compile preserve patterns
        self._preserve_regex = re.compile('|'.join(f'({p})' for p in PRESERVE_PATTERNS))
        
        # Build glossary text for prompt
        self._glossary_text = self._build_glossary_text()
        
        # Statistics
        self.stats = {
            "files_processed": 0,
            "files_modified": 0,
            "files_skipped": 0,
            "api_calls": 0,
            "errors": 0,
            "ast_failures": 0,
            "integrity_failures": 0,
            "total_korean_before": 0,
            "total_korean_after": 0,
        }
        
        # Thread pool for sync API calls in async context
        self._executor = ThreadPoolExecutor(max_workers=concurrency)
    
    def _build_glossary_text(self) -> str:
        """Build formatted glossary for prompt"""
        lines = []
        for korean, english in sorted(GLOSSARY.items(), key=lambda x: -len(x[0])):
            lines.append(f"- {korean} → {english}")
        return '\n'.join(lines)
    
    def _init_gemini(self):
        """Initialize Gemini API client"""
        if self._genai is not None:
            return
        
        try:
            import google.generativeai as genai
            
            if not self.api_key:
                raise ValueError(
                    "Gemini API key not found. Set GOOGLE_API_KEY or GEMINI_API_KEY environment variable."
                )
            
            genai.configure(api_key=self.api_key)
            self._genai = genai
            # Use Gemini 1.5 Pro for large context
            self._model = genai.GenerativeModel('gemini-1.5-pro')
            
            if self.verbose:
                print("✅ Gemini 1.5 Pro initialized (2M token context)")
                
        except ImportError:
            raise ImportError(
                "google-generativeai package not installed. "
                "Run: pip install google-generativeai"
            )
    
    def _contains_korean(self, text: str) -> bool:
        """Check if text contains Korean characters"""
        return bool(KOREAN_PATTERN.search(text))
    
    def _count_korean_chars(self, text: str) -> int:
        """Count Korean characters in text"""
        return len(KOREAN_PATTERN.findall(text))
    
    def _apply_glossary(self, text: str) -> str:
        """Apply terminology glossary (for dry-run mode)"""
        result = text
        for korean, english in sorted(GLOSSARY.items(), key=lambda x: -len(x[0])):
            result = result.replace(korean, english)
        return result
    
    def _translate_full_file(self, content: str) -> str:
        """
        Translate entire file content using full-context approach.
        
        v2.0: Send whole file to Gemini for context-aware translation.
        """
        self._init_gemini()
        
        prompt = FULL_FILE_TRANSLATION_PROMPT.format(
            glossary_text=self._glossary_text,
            code_content=content
        )
        
        try:
            response = self._model.generate_content(
                prompt,
                generation_config={
                    "temperature": 0.1,  # Low temperature for consistency
                    "max_output_tokens": 65536,  # Large output for full files
                }
            )
            self.stats["api_calls"] += 1
            
            translated = response.text.strip()
            
            # Remove markdown code blocks if present
            if translated.startswith("```python"):
                translated = translated[9:]
            if translated.startswith("```"):
                translated = translated[3:]
            if translated.endswith("```"):
                translated = translated[:-3]
            
            return translated.strip()
            
        except Exception as e:
            self.stats["errors"] += 1
            if self.verbose:
                print(f"  ❌ Gemini API error: {e}")
            raise
    
    def _generate_diff_report(self, original: str, translated: str) -> str:
        """Generate a human-readable diff report"""
        diff = list(difflib.unified_diff(
            original.split('\n'),
            translated.split('\n'),
            fromfile='original',
            tofile='translated',
            lineterm='',
            n=1
        ))
        return '\n'.join(diff[:100])  # Limit to first 100 lines
    
    def translate_file_sync(self, file_path: str) -> TranslationResult:
        """Translate a single Python file (synchronous)"""
        result = TranslationResult(file_path=file_path)
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            result.original_content = content
            
            # Count Korean before
            result.korean_count_before = self._count_korean_chars(content)
            self.stats["total_korean_before"] += result.korean_count_before
            
            # Skip if no Korean
            if not self._contains_korean(content):
                if self.verbose:
                    print(f"  ⏭️ No Korean text: {file_path}")
                result.translated_content = content
                self.stats["files_skipped"] += 1
                return result
            
            if self.verbose:
                print(f"  📝 Translating: {file_path} ({result.korean_count_before} Korean chars)")
            
            # Translate
            if self.dry_run:
                # Dry run: only apply glossary
                translated = self._apply_glossary(content)
            else:
                # Full translation
                translated = self._translate_full_file(content)
            
            result.translated_content = translated
            result.korean_count_after = self._count_korean_chars(translated)
            self.stats["total_korean_after"] += result.korean_count_after
            
            # Integrity verification
            if not self.skip_verify and not self.dry_run:
                integrity = CodeIntegrityChecker.full_check(content, translated)
                result.ast_valid = integrity.ast_valid
                result.code_diff_clean = integrity.code_unchanged
                
                if not integrity.ast_valid:
                    result.success = False
                    result.error_message = f"AST Error: {integrity.ast_error}"
                    self.stats["ast_failures"] += 1
                    if self.verbose:
                        print(f"  ❌ AST validation failed: {integrity.ast_error}")
                    return result
                
                if not integrity.code_unchanged:
                    self.stats["integrity_failures"] += 1
                    if self.verbose:
                        print(f"  ⚠️ Code structure changed ({integrity.non_comment_changes} lines)")
                        for line in integrity.changed_code_lines[:5]:
                            print(f"      {line}")
            
            # Generate diff report
            result.diff_report = self._generate_diff_report(content, translated)
            
            # Write if not dry run and content changed
            if not self.dry_run and translated != content and result.ast_valid:
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(translated)
                self.stats["files_modified"] += 1
                if self.verbose:
                    print(f"  ✅ Updated: {file_path}")
            
        except Exception as e:
            result.success = False
            result.error_message = str(e)
            self.stats["errors"] += 1
            if self.verbose:
                print(f"  ❌ Error: {file_path}: {e}")
        
        self.stats["files_processed"] += 1
        return result
    
    async def translate_file_async(self, file_path: str) -> TranslationResult:
        """Translate a single Python file (async wrapper)"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._executor,
            self.translate_file_sync,
            file_path
        )
    
    async def translate_directory_async(self, dir_path: str, recursive: bool = True) -> List[TranslationResult]:
        """Translate all Python files in a directory (async)"""
        path = Path(dir_path)
        
        if not path.exists():
            raise FileNotFoundError(f"Directory not found: {dir_path}")
        
        # Find Python files
        pattern = "**/*.py" if recursive else "*.py"
        python_files = list(path.glob(pattern))
        
        # Exclude certain directories
        exclude_dirs = {'__pycache__', '.git', 'node_modules', 'venv', '.venv', 'env', 'build', 'dist'}
        python_files = [
            f for f in python_files 
            if not any(ex in f.parts for ex in exclude_dirs)
        ]
        
        print(f"\n🔍 Found {len(python_files)} Python files to process")
        print(f"⚡ Using {self.concurrency} concurrent workers\n")
        
        # Process in batches for controlled concurrency
        results = []
        semaphore = asyncio.Semaphore(self.concurrency)
        
        async def translate_with_semaphore(file_path: str) -> TranslationResult:
            async with semaphore:
                return await self.translate_file_async(str(file_path))
        
        # Create tasks
        tasks = [translate_with_semaphore(f) for f in python_files]
        
        # Execute with progress
        for i, coro in enumerate(asyncio.as_completed(tasks), 1):
            result = await coro
            results.append(result)
            
            if not self.verbose:
                # Progress indicator
                progress = i / len(python_files) * 100
                print(f"\r  Progress: {i}/{len(python_files)} ({progress:.1f}%)", end="", flush=True)
        
        if not self.verbose:
            print()  # Newline after progress
        
        return results
    
    def translate_directory(self, dir_path: str, recursive: bool = True) -> List[TranslationResult]:
        """Translate all Python files in a directory (sync wrapper)"""
        return asyncio.run(self.translate_directory_async(dir_path, recursive))
    
    def print_stats(self):
        """Print translation statistics"""
        korean_reduction = 0
        if self.stats["total_korean_before"] > 0:
            korean_reduction = (1 - self.stats["total_korean_after"] / self.stats["total_korean_before"]) * 100
        
        print("\n" + "=" * 70)
        print("📊 Translation Statistics")
        print("=" * 70)
        print(f"  Files processed:       {self.stats['files_processed']}")
        print(f"  Files modified:        {self.stats['files_modified']}")
        print(f"  Files skipped:         {self.stats['files_skipped']}")
        print(f"  API calls made:        {self.stats['api_calls']}")
        print(f"  Errors:                {self.stats['errors']}")
        print("-" * 70)
        print(f"  Korean chars before:   {self.stats['total_korean_before']}")
        print(f"  Korean chars after:    {self.stats['total_korean_after']}")
        print(f"  Translation coverage:  {korean_reduction:.1f}%")
        print("-" * 70)
        print(f"  AST failures:          {self.stats['ast_failures']}")
        print(f"  Integrity warnings:    {self.stats['integrity_failures']}")
        print("=" * 70)


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Translate Korean comments/docstrings to English in Analemma OS codebase"
    )
    parser.add_argument(
        "--dry-run", "-n",
        action="store_true",
        help="Preview changes without modifying files (glossary-only)"
    )
    parser.add_argument(
        "--file", "-f",
        type=str,
        help="Translate a specific file only"
    )
    parser.add_argument(
        "--dir", "-d",
        type=str,
        default="src",
        help="Directory to translate (default: src)"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show detailed progress"
    )
    parser.add_argument(
        "--concurrency", "-c",
        type=int,
        default=5,
        help="Number of parallel translations (default: 5)"
    )
    parser.add_argument(
        "--skip-verify",
        action="store_true",
        help="Skip AST and integrity verification"
    )
    
    args = parser.parse_args()
    
    print("""
╔═══════════════════════════════════════════════════════════════════════╗
║  🌐 Analemma OS - Korean to English Translation Tool v2.0             ║
║                                                                       ║
║  v2.0 Enhancements:                                                   ║
║  • Full-file context translation (Gemini 1.5 Pro 2M tokens)           ║
║  • Async parallel processing (5-10x faster)                           ║
║  • Post-translation integrity verification (AST + diff)               ║
║  • Linux Kernel coding style for comments                             ║
╚═══════════════════════════════════════════════════════════════════════╝
""")
    
    if args.dry_run:
        print("🔍 DRY RUN MODE - No files will be modified (glossary-only)\n")
    
    translator = AnalemmaTranslator(
        dry_run=args.dry_run,
        verbose=args.verbose,
        concurrency=args.concurrency,
        skip_verify=args.skip_verify
    )
    
    start_time = time.time()
    
    try:
        if args.file:
            # Translate single file
            result = translator.translate_file_sync(args.file)
            if result.success:
                print(f"\n✅ Translated: {args.file}")
                print(f"   Korean: {result.korean_count_before} → {result.korean_count_after}")
                if not result.ast_valid:
                    print(f"   ⚠️ AST Error: {result.error_message}")
            else:
                print(f"\n❌ Failed: {result.error_message}")
        else:
            # Translate directory
            results = translator.translate_directory(args.dir)
            
            # Summary
            successful = sum(1 for r in results if r.success)
            modified = sum(1 for r in results if r.translated_content != r.original_content)
            ast_errors = sum(1 for r in results if not r.ast_valid)
            
            print(f"\n✅ Successfully processed: {successful}/{len(results)} files")
            print(f"📝 Files with changes: {modified}")
            if ast_errors:
                print(f"⚠️ AST validation errors: {ast_errors}")
        
        elapsed = time.time() - start_time
        translator.print_stats()
        print(f"\n⏱️ Total time: {elapsed:.1f} seconds")
        
    except KeyboardInterrupt:
        print("\n\n⏹️ Translation cancelled by user")
        translator.print_stats()
        sys.exit(130)
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
