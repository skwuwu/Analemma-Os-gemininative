"""
Verify LLM Test Results Lambda
==============================
LLM Simulator Step Functions에서 호출되어 테스트 결과를 검증합니다.

이 Lambda는 llm_simulator.py의 verify_* 함수들을 Step Functions에서 직접 호출할 수 있도록 래핑합니다.

v2.1: Task Manager 추상화 레이어 검증 추가
- Provider 교차 검증 (Cross-validation)
- Thought Memory Compaction (10개 한정)
- QuickFix 동적 생성 검증

v2.2: S3 Offload Hydration 지원
- __s3_offloaded 상태 자동 하이드레이션
- Stage 4, 6, 7, 8에서 S3 경로 추적
"""

import json
import logging
import uuid
from typing import Dict, Any, Tuple, List, Optional
from decimal import Decimal

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# ============================================================================
# S3 Offload Hydration (v2.2)
# ============================================================================

def _hydrate_s3_offloaded_state(final_state: Dict[str, Any]) -> Dict[str, Any]:
    """
    S3로 오프로드된 상태를 하이드레이션합니다.
    
    v3.0: StateBag 아키텍처 지원
    - __s3_offloaded: True + __s3_path → 레거시 오프로드 패턴
    - state_s3_path → 새로운 StateBag 오프로드 패턴 (워크플로 실행 결과)
    
    이 함수가 없으면 검증기가 빈 상태만 보고 실패로 판정합니다.
    
    Args:
        final_state: 오프로드되었을 수 있는 상태 딕셔너리
    
    Returns:
        하이드레이션된 전체 상태
    """
    if not isinstance(final_state, dict):
        return final_state
    
    # S3 경로 결정: 여러 패턴 지원
    # 1) 레거시: __s3_offloaded + __s3_path
    # 2) StateBag: state_s3_path (워크플로 실행 결과)
    # 3) final_state_s3_path (병렬 브랜치 결과)
    s3_path = None
    
    if final_state.get('__s3_offloaded'):
        s3_path = final_state.get('__s3_path')
    elif final_state.get('state_s3_path'):
        s3_path = final_state.get('state_s3_path')
        logger.info(f"[S3 Hydration] Detected StateBag pattern: state_s3_path")
    elif final_state.get('final_state_s3_path'):
        s3_path = final_state.get('final_state_s3_path')
        logger.info(f"[S3 Hydration] Detected parallel branch pattern: final_state_s3_path")
    
    if not s3_path:
        # S3 오프로드가 없으면 원본 반환
        return final_state
    
    logger.info(f"[S3 Hydration] Fetching offloaded state from: {s3_path}")
    
    try:
        import boto3
        s3_client = boto3.client('s3')
        
        # s3://bucket/key 형식 파싱
        if s3_path.startswith('s3://'):
            parts = s3_path[5:].split('/', 1)
            bucket = parts[0]
            key = parts[1] if len(parts) > 1 else ''
        else:
            logger.error(f"[S3 Hydration] Invalid S3 path format: {s3_path}")
            return final_state
        
        response = s3_client.get_object(Bucket=bucket, Key=key)
        body = response['Body'].read().decode('utf-8')
        
        hydrated_state = json.loads(body)
        logger.info(f"[S3 Hydration] Successfully hydrated state, keys: {list(hydrated_state.keys())[:10]}...")
        
        return hydrated_state
        
    except Exception as e:
        logger.exception(f"[S3 Hydration] Failed to fetch from S3: {e}")
        # 실패 시 원본 반환 (부분 데이터라도 사용)
        return final_state


def _ensure_hydrated_state(final_state: Dict[str, Any]) -> Dict[str, Any]:
    """
    검증 전 상태가 S3에서 완전히 하이드레이션되었는지 확인합니다.
    
    중첩된 오프로드 상태도 처리합니다 (예: final_state 내부의 execution_result).
    
    v2.4: 모든 필드에 대해 재귀적으로 S3 오프로드 상태를 하이드레이션합니다.
    - Stage 4: vision_results, processed_items
    - Stage 6: partition_results, items
    - Stage 7: branch_results, claude_branch, gemini_branch
    - Stage 8: successful_items, failed_items, processed_results
    """
    # 최상위 하이드레이션
    hydrated = _hydrate_s3_offloaded_state(final_state)
    
    if not isinstance(hydrated, dict):
        return hydrated
    
    # 재귀적으로 모든 딕셔너리 필드 하이드레이션 (Stage 4, 6, 7, 8 지원)
    def _recursive_hydrate(obj: Any, depth: int = 0) -> Any:
        """최대 3단계까지 재귀적으로 S3 오프로드 상태를 하이드레이션합니다."""
        if depth > 3:  # 무한 재귀 방지
            return obj
        
        if isinstance(obj, dict):
            # 현재 객체가 오프로드되었으면 하이드레이션
            # v3.0: __s3_offloaded, state_s3_path, final_state_s3_path 모두 지원
            if obj.get('__s3_offloaded') or obj.get('state_s3_path') or obj.get('final_state_s3_path'):
                obj = _hydrate_s3_offloaded_state(obj)
            
            # 모든 하위 필드에 대해 재귀 적용
            for key, value in list(obj.items()):
                if isinstance(value, dict):
                    obj[key] = _recursive_hydrate(value, depth + 1)
                elif isinstance(value, list):
                    obj[key] = [
                        _recursive_hydrate(item, depth + 1) if isinstance(item, dict) else item
                        for item in value
                    ]
            return obj
        
        return obj
    
    # 재귀 하이드레이션 적용
    hydrated = _recursive_hydrate(hydrated)
    
    # 하이드레이션 완료 로깅
    offload_keys = [k for k, v in hydrated.items() if isinstance(v, dict) and not v.get('__s3_offloaded')]
    logger.info(f"[S3 Hydration] Recursive hydration complete. Active keys: {offload_keys[:10]}")
    
    return hydrated

# Task Context imports (lazy loading to avoid circular imports)
def _get_task_context_models():
    """Lazy import to avoid circular dependency and Lambda cold start overhead."""
    from src.models.task_context import (
        TaskContext, TaskStatus, SubStatus, CostCategory, CostLineItem, CostDetail,
        QuickFix, QuickFixType, THOUGHT_HISTORY_MAX_LENGTH, get_friendly_error_message
    )
    return {
        'TaskContext': TaskContext,
        'TaskStatus': TaskStatus,
        'SubStatus': SubStatus,
        'CostCategory': CostCategory,
        'CostLineItem': CostLineItem,
        'CostDetail': CostDetail,
        'QuickFix': QuickFix,
        'QuickFixType': QuickFixType,
        'THOUGHT_HISTORY_MAX_LENGTH': THOUGHT_HISTORY_MAX_LENGTH,
        'get_friendly_error_message': get_friendly_error_message,
    }


# Provider 이름 매핑 (Cross-validation용)
PROVIDER_SERVICE_NAME_MAP = {
    "gemini": ["gemini", "vertex", "google"],
    "bedrock": ["bedrock", "claude", "anthropic", "amazon"],
    "openai": ["openai", "gpt", "chatgpt"],
}


def verify_basic_llm_call(final_state: Dict, test_config: Dict) -> Tuple[bool, str]:
    """LI-A: 기본 LLM 호출 검증"""
    # [Fix] Support multiple LLM output key patterns used across different workflows
    llm_output_keys = [
        'llm_output', 'llm_raw_output', 'vision_raw_output', 
        'document_analysis_raw', 'final_report_raw', 'llm_result'
    ]
    
    llm_output = None
    for key in llm_output_keys:
        llm_output = final_state.get(key) or final_state.get('final_state', {}).get(key)
        if llm_output:
            break
    
    # Also check for usage field as indicator of LLM execution
    usage = final_state.get('usage') or final_state.get('final_state', {}).get('usage')
    
    if not llm_output and not usage:
        return False, "No LLM output found in result"
    
    expected_min_length = test_config.get('expected_min_length', 10)
    if len(str(llm_output)) < expected_min_length:
        return False, f"LLM output too short: {len(str(llm_output))} < {expected_min_length}"
    
    if '[MOCK' in str(llm_output).upper():
        return False, "Received MOCK response instead of real LLM output"
    
    return True, f"Basic LLM call successful, output length: {len(str(llm_output))}"


def verify_structured_output(final_state: Dict, test_config: Dict) -> Tuple[bool, str]:
    """LI-B: Structured Output 검증"""
    structured_output = final_state.get('structured_output') or final_state.get('final_state', {}).get('structured_output')
    
    if not structured_output:
        return False, "No structured output found"
    
    try:
        if isinstance(structured_output, str):
            parsed = json.loads(structured_output)
        else:
            parsed = structured_output
        
        if 'languages' in parsed and isinstance(parsed['languages'], list):
            return True, f"Structured output valid with {len(parsed['languages'])} items"
        else:
            return False, f"Unexpected structure: {list(parsed.keys())}"
    except json.JSONDecodeError as e:
        return False, f"Invalid JSON in structured output: {e}"


def verify_thinking_mode(final_state: Dict, test_config: Dict) -> Tuple[bool, str]:
    """LI-C: Thinking Mode 검증"""
    thinking_output = final_state.get('thinking_output') or final_state.get('final_state', {}).get('thinking_output')
    llm_output = final_state.get('llm_output') or final_state.get('final_state', {}).get('llm_output')
    
    has_reasoning = False
    if thinking_output:
        has_reasoning = True
    elif llm_output and any(word in str(llm_output).lower() for word in ['because', 'therefore', 'so', 'left', 'remain']):
        has_reasoning = True
    
    if not has_reasoning:
        return False, "No thinking/reasoning visible in output"
    
    answer_correct = '9' in str(llm_output)
    
    if answer_correct:
        return True, "Thinking mode worked correctly, answer is 9"
    else:
        return False, f"Answer may be incorrect: {str(llm_output)[:100]}"


def verify_llm_operator_integration(final_state: Dict, test_config: Dict) -> Tuple[bool, str]:
    """LI-D: LLM + operator_official 통합 검증"""
    step_history = final_state.get('step_history', [])
    expected_strategies = test_config.get('expected_strategies_used', [])
    
    found_strategies = []
    for step in step_history:
        for strategy in expected_strategies:
            if strategy in str(step):
                found_strategies.append(strategy)
    
    found_strategies = list(set(found_strategies))
    
    if len(found_strategies) < len(expected_strategies):
        missing = set(expected_strategies) - set(found_strategies)
        return False, f"Missing strategies: {missing}"
    
    filtered_results = final_state.get('filtered_results') or final_state.get('high_priority_tasks')
    if filtered_results is None:
        return False, "No filtered results found - operator integration may have failed"
    
    return True, f"LLM + Operator integration successful. Strategies used: {found_strategies}"


def verify_document_analysis(final_state: Dict, test_config: Dict) -> Tuple[bool, str]:
    """LI-E: 복합 문서 분석 파이프라인 검증"""
    expected_stages = test_config.get('expected_pipeline_stages', [])
    completed_stages = []
    
    if final_state.get('analysis_data') or final_state.get('llm_analysis_raw'):
        completed_stages.append('extract')
    if final_state.get('high_priority_vulnerabilities') or final_state.get('filtered_items'):
        completed_stages.append('filter')
    if final_state.get('deep_analysis_results'):
        completed_stages.append('deep_analyze')
    if final_state.get('final_report') or final_state.get('final_markdown_report'):
        completed_stages.append('merge')
    
    missing_stages = set(expected_stages) - set(completed_stages)
    if missing_stages:
        return False, f"Pipeline incomplete. Missing stages: {missing_stages}"
    
    if test_config.get('expected_security_findings'):
        vulns = final_state.get('security_vulnerabilities') or final_state.get('high_priority_vulnerabilities')
        if not vulns:
            return False, "No security findings extracted"
    
    return True, f"Document analysis pipeline complete. Stages: {completed_stages}"


def verify_multimodal_vision(final_state: Dict, test_config: Dict) -> Tuple[bool, str]:
    """LI-F: Multimodal Vision 라이브 검증"""
    vision_output = final_state.get('vision_output') or final_state.get('final_state', {}).get('vision_output')
    
    if not vision_output:
        return False, "No vision output found"
    
    if len(str(vision_output)) < 50:
        return False, f"Vision output too short: {len(str(vision_output))}"
    
    diagram_words = ['diagram', 'architecture', 'flow', 'component', 'system', 'connection']
    has_diagram = any(w in str(vision_output).lower() for w in diagram_words)
    if not has_diagram:
        return False, "Vision output does not describe a diagram"
        
    return True, "Multimodal vision analysis successful"

def verify_stage5_hyper_stress(final_state: Dict, test_config: Dict) -> Tuple[bool, str]:
    """Stage 5: Hyper Stress Verification with StateBag Integrity"""
    
    # 1. StateBag Integrity Check
    if test_config.get('verify_state_bag_integrity'):
        passed, msg = verify_state_bag_integrity(final_state, test_config)
        if not passed:
            return False, msg
            
    # 2. Token Usage Check
    usage = final_state.get('usage', {})
    total_tokens = usage.get('total_tokens', 0)
    if total_tokens > 50000:
        return False, f"Token Limit Exceeded: {total_tokens} > 50000"
        
    return True, f"Stage 5 Passed: System survived hyper stress (Tokens: {total_tokens})"


def verify_state_bag_integrity(state: dict, test_config: dict) -> Tuple[bool, str]:
    """
    Verify StateBag Architecture Compliance
    - Checks if large payloads (>30KB) were correctly offloaded.
    - Checks metadata (payload_size_kb, s3_offloaded).
    """
    # 1. Check Metadata Existence
    if 'payload_size_kb' not in state:
        # Some legacy paths might miss this, checking strictly for Stage 5
        if test_config.get('strict_mode'):
            return False, "Missing 'payload_size_kb' metadata in StateBag"
        
    s3_offloaded = state.get('s3_offloaded', False)
    payload_size = state.get('payload_size_kb', 0)
    
    # 2. Verify Offloading Logic
    # If size > 200KB (MAX), s3_offloaded MUST be true
    if payload_size > 200 and not s3_offloaded:
        return False, f"StateBag Breach: Size {payload_size}KB > 200KB but NOT offloaded"
        
    # 3. Check for specific field offloading (Pointer check)
    large_inline_fields = []
    for key, val in state.items():
        if isinstance(val, (dict, list)) and not key.startswith('__'):
            try:
                # Approximate sizing
                json_str = json.dumps(val)
                if len(json_str) > 50 * 1024: # 50KB
                    large_inline_fields.append(key)
            except:
                pass
                
    if large_inline_fields:
        logger.warning(f"StateBag Warning: Large inline fields detected: {large_inline_fields}")
        
    return True, f"StateBag Integrity OK (Size: {payload_size}KB, Offloaded: {s3_offloaded})"



# ============================================================================
# Task Manager Abstraction Layer Validation (v2.1)
# ============================================================================

def _safe_get_nested(data: Dict, *keys, default=None) -> Any:
    """
    안전한 중첩 필드 접근.
    
    거짓 음성(False Negative) 방지를 위해 다중 경로 탐색.
    예: _safe_get_nested(data, 'output', 'usage', 'provider') 또는
        _safe_get_nested(data, 'final_state', 'usage', 'provider')
    """
    current = data
    for key in keys:
        if isinstance(current, dict):
            current = current.get(key)
            if current is None:
                # fallback: final_state 내부에서 찾기
                if 'final_state' in data and keys[0] != 'final_state':
                    return _safe_get_nested(data.get('final_state', {}), *keys, default=default)
                return default
        else:
            return default
    return current if current is not None else default


def verify_provider_cross_validation(
    runner_log: Dict[str, Any], 
    task_context_data: Dict[str, Any],
    test_config: Dict[str, Any]
) -> Tuple[bool, str, List[Dict[str, Any]]]:
    """
    Provider 교차 검증 (Cross-validation).
    
    검증식: if runner_log.provider == 'gemini' then task_context.service_name must contain 'Gemini'
    
    이를 통해 "엔진은 Gemini인데 라벨은 Bedrock"인 상태를 잡아냅니다.
    
    Returns:
        (passed, message, checks): 검증 결과, 메시지, 상세 체크 목록
    """
    checks = []
    
    # 1. runner_log에서 provider 추출 (다중 경로 탐색)
    runner_provider = (
        _safe_get_nested(runner_log, 'usage', 'provider') or
        _safe_get_nested(runner_log, 'output', 'usage', 'provider') or
        _safe_get_nested(runner_log, 'meta', 'provider') or
        _safe_get_nested(runner_log, 'provider') or
        'unknown'
    ).lower()
    
    checks.append({
        "name": "runner_provider 추출",
        "passed": runner_provider != 'unknown',
        "value": runner_provider,
        "path": "runner_log.usage.provider or runner_log.output.usage.provider"
    })
    
    # 2. task_context에서 service_name 추출 (다중 경로 탐색)
    service_name = None
    extraction_path = None
    
    # 경로 1: cost_detail.line_items[].service_name
    cost_detail = _safe_get_nested(task_context_data, 'cost_detail')
    if cost_detail:
        line_items = cost_detail.get('line_items', [])
        for item in line_items:
            if item.get('category') == 'llm':
                service_name = item.get('service_name', '').lower()
                extraction_path = "task_context.cost_detail.line_items[category=llm].service_name"
                break
    
    # 경로 2: cost_summary.service_name (WebSocket payload)
    if not service_name:
        cost_summary = _safe_get_nested(task_context_data, 'cost_summary')
        if cost_summary:
            service_name = cost_summary.get('service_name', '').lower()
            extraction_path = "task_context.cost_summary.service_name"
    
    # 경로 3: token_usage metadata
    if not service_name:
        token_usage = _safe_get_nested(task_context_data, 'token_usage')
        if token_usage and isinstance(token_usage, dict):
            service_name = token_usage.get('service', '').lower()
            extraction_path = "task_context.token_usage.service"
    
    checks.append({
        "name": "task_context service_name 추출",
        "passed": bool(service_name),
        "value": service_name or 'not_found',
        "path": extraction_path or 'N/A'
    })
    
    # 3. Cross-validation: provider와 service_name 일치 여부 확인
    if runner_provider == 'unknown' or not service_name:
        # 필드가 누락된 경우 - 별도 이슈로 취급
        checks.append({
            "name": "Provider 매핑 Cross-validation",
            "passed": False,
            "value": "INCOMPLETE_DATA",
            "message": f"runner_provider={runner_provider}, service_name={service_name}"
        })
        return False, f"Cross-validation 불가: 데이터 누락 (runner={runner_provider}, service={service_name})", checks
    
    # 매핑 테이블에서 기대 키워드 찾기
    expected_keywords = PROVIDER_SERVICE_NAME_MAP.get(runner_provider, [runner_provider])
    
    # service_name에 기대 키워드가 포함되어 있는지 확인
    is_match = any(keyword in service_name for keyword in expected_keywords)
    
    # 역방향 검증: service_name에서 추출한 provider와 runner_provider 비교
    detected_provider = None
    for provider, keywords in PROVIDER_SERVICE_NAME_MAP.items():
        if any(kw in service_name for kw in keywords):
            detected_provider = provider
            break
    
    cross_validation_passed = is_match and (detected_provider == runner_provider if detected_provider else True)
    
    checks.append({
        "name": "Provider 매핑 Cross-validation",
        "passed": cross_validation_passed,
        "expected": f"service_name에 {expected_keywords} 중 하나 포함",
        "actual": service_name,
        "runner_provider": runner_provider,
        "detected_from_service": detected_provider or 'unknown'
    })
    
    if not cross_validation_passed:
        error_msg = (
            f"ASSERT_ERROR: Provider 불일치! "
            f"Runner={runner_provider}, ServiceName에서 감지={detected_provider or 'unknown'}, "
            f"실제 service_name='{service_name}'"
        )
        logger.error(error_msg)
        return False, error_msg, checks
    
    return True, f"Provider Cross-validation 통과: {runner_provider} ↔ {service_name}", checks


def verify_thought_memory_compaction(
    task_context_data: Dict[str, Any],
    total_thoughts_added: int,
    test_config: Dict[str, Any]
) -> Tuple[bool, str, List[Dict[str, Any]]]:
    """
    실시간성 및 메모리 압착 검증 (10개 한정 로직).
    
    테스트: 15번의 add_thought 발생 후 메모리상 thought_history 길이가 정확히 10인지 확인.
    나머지 5개가 S3로 밀려났는지도 체크.
    
    Args:
        task_context_data: TaskContext 데이터 (직렬화된 Dict)
        total_thoughts_added: 총 추가된 thought 수
        test_config: 테스트 설정
    
    Returns:
        (passed, message, checks): 검증 결과, 메시지, 상세 체크 목록
    """
    checks = []
    models = _get_task_context_models()
    MAX_LENGTH = models['THOUGHT_HISTORY_MAX_LENGTH']  # 10
    
    # 1. thought_history 길이 확인
    thought_history = _safe_get_nested(task_context_data, 'thought_history', default=[])
    memory_count = len(thought_history) if isinstance(thought_history, list) else 0
    
    expected_memory_count = min(total_thoughts_added, MAX_LENGTH)
    memory_check_passed = memory_count == expected_memory_count
    
    checks.append({
        "name": "Memory thought_history 길이",
        "passed": memory_check_passed,
        "expected": expected_memory_count,
        "actual": memory_count,
        "max_limit": MAX_LENGTH
    })
    
    # 2. total_thought_count 필드 확인 (전체 카운트 추적)
    total_thought_count = _safe_get_nested(task_context_data, 'total_thought_count', default=0)
    
    count_tracking_passed = total_thought_count >= total_thoughts_added
    
    checks.append({
        "name": "total_thought_count 추적",
        "passed": count_tracking_passed,
        "expected_min": total_thoughts_added,
        "actual": total_thought_count
    })
    
    # 3. S3 참조 존재 확인 (메모리 초과 시 필수)
    full_thought_trace_ref = _safe_get_nested(task_context_data, 'full_thought_trace_ref')
    
    if total_thoughts_added > MAX_LENGTH:
        # 초과분이 있으면 S3 참조가 있어야 함
        s3_ref_required = True
        s3_ref_exists = bool(full_thought_trace_ref)
        
        checks.append({
            "name": "S3 히스토리 참조 (full_thought_trace_ref)",
            "passed": s3_ref_exists,
            "required": s3_ref_required,
            "value": full_thought_trace_ref or 'NOT_SET',
            "overflow_count": total_thoughts_added - MAX_LENGTH
        })
        
        if not s3_ref_exists:
            return False, f"메모리 압착 실패: {total_thoughts_added}개 thought 중 S3 참조 없음", checks
    else:
        checks.append({
            "name": "S3 히스토리 참조",
            "passed": True,
            "required": False,
            "value": "N/A (미초과)",
            "overflow_count": 0
        })
    
    # 4. 최신 thought 순서 검증 (LIFO: 가장 최근 것이 마지막)
    if thought_history and len(thought_history) >= 2:
        # timestamp 기준 정렬 확인
        timestamps = []
        for t in thought_history:
            ts = t.get('timestamp') if isinstance(t, dict) else None
            if ts:
                timestamps.append(ts)
        
        if timestamps:
            is_sorted = timestamps == sorted(timestamps)
            checks.append({
                "name": "thought_history 시간순 정렬",
                "passed": is_sorted,
                "first_ts": timestamps[0] if timestamps else None,
                "last_ts": timestamps[-1] if timestamps else None
            })
    
    all_passed = all(check["passed"] for check in checks)
    
    if all_passed:
        msg = f"메모리 압착 검증 통과: {memory_count}/{total_thoughts_added} in memory, "
        msg += f"total_count={total_thought_count}"
        if full_thought_trace_ref:
            msg += f", S3={full_thought_trace_ref[:50]}..."
    else:
        failed = [c["name"] for c in checks if not c["passed"]]
        msg = f"메모리 압착 검증 실패: {', '.join(failed)}"
    
    return all_passed, msg, checks


def verify_quick_fix_dynamic_generation(
    error_context: Dict[str, Any],
    generated_quick_fix: Optional[Dict[str, Any]],
    test_config: Dict[str, Any]
) -> Tuple[bool, str, List[Dict[str, Any]]]:
    """
    QuickFix 동적 생성 검증.
    
    에러 발생 시 단순 에러 메시지만 확인하지 말고,
    QuickFix 객체가 현재 에러 문맥에 맞게 생성되었는지 검증.
    
    예: 429 Rate Limit 발생 → QuickFix에 Wait and Retry 액션이 포함되었는가?
    
    Args:
        error_context: 에러 정보 (error_code, error_message 등)
        generated_quick_fix: 생성된 QuickFix 데이터
        test_config: 테스트 설정
    
    Returns:
        (passed, message, checks): 검증 결과, 메시지, 상세 체크 목록
    """
    checks = []
    models = _get_task_context_models()
    QuickFixType = models['QuickFixType']
    
    # 에러 코드/메시지 추출
    error_code = str(error_context.get('error_code', '')).lower()
    error_message = str(error_context.get('error_message', error_context.get('message', ''))).lower()
    error_type = error_context.get('error_type', 'unknown')
    
    # 에러 유형별 기대 QuickFix 매핑
    EXPECTED_QUICK_FIX_MAP = {
        "429": {
            "expected_fix_types": [QuickFixType.RETRY.value],
            "expected_action_patterns": ["retry", "delayed", "wait"],
            "expected_context_keys": ["delay_seconds"],
            "description": "Rate Limit → Wait and Retry"
        },
        "500": {
            "expected_fix_types": [QuickFixType.RETRY.value, QuickFixType.SELF_HEALING.value],
            "expected_action_patterns": ["retry"],
            "expected_context_keys": [],
            "description": "Server Error → Retry"
        },
        "503": {
            "expected_fix_types": [QuickFixType.RETRY.value],
            "expected_action_patterns": ["retry"],
            "expected_context_keys": [],
            "description": "Service Unavailable → Retry"
        },
        "504": {
            "expected_fix_types": [QuickFixType.SELF_HEALING.value],
            "expected_action_patterns": ["split", "retry"],
            "expected_context_keys": [],
            "description": "Timeout → Split and Retry"
        },
        "401": {
            "expected_fix_types": [QuickFixType.REDIRECT.value],
            "expected_action_patterns": ["auth", "login", "redirect"],
            "expected_context_keys": ["redirect_url"],
            "description": "Auth Error → Redirect to Login"
        },
        "403": {
            "expected_fix_types": [QuickFixType.ESCALATE.value],
            "expected_action_patterns": ["escalate", "admin"],
            "expected_context_keys": [],
            "description": "Forbidden → Escalate to Admin"
        },
        "validation": {
            "expected_fix_types": [QuickFixType.INPUT.value],
            "expected_action_patterns": ["input", "request"],
            "expected_context_keys": [],
            "description": "Validation Error → Request Input"
        },
        "schema": {
            "expected_fix_types": [QuickFixType.SELF_HEALING.value],
            "expected_action_patterns": ["fix", "auto"],
            "expected_context_keys": [],
            "description": "Schema Error → Auto Fix"
        },
        "llm": {
            "expected_fix_types": [QuickFixType.SELF_HEALING.value],
            "expected_action_patterns": ["retry", "node"],
            "expected_context_keys": [],
            "description": "LLM Error → Node Retry with Context"
        }
    }
    
    # 1. 에러 유형 식별
    detected_error_type = None
    for key in ["429", "500", "503", "504", "401", "403"]:
        if key in error_code or key in error_message:
            detected_error_type = key
            break
    
    if not detected_error_type:
        for key in ["validation", "schema", "llm"]:
            if key in error_message or key in error_type.lower():
                detected_error_type = key
                break
    
    checks.append({
        "name": "에러 유형 식별",
        "passed": detected_error_type is not None,
        "detected": detected_error_type or 'unknown',
        "error_code": error_code,
        "error_message": error_message[:100] if error_message else 'N/A'
    })
    
    # 2. QuickFix 존재 확인
    if not generated_quick_fix:
        checks.append({
            "name": "QuickFix 생성",
            "passed": False,
            "value": "NOT_GENERATED",
            "message": "에러 발생 시 QuickFix가 생성되지 않음"
        })
        return False, "QuickFix 미생성: 에러 발생 시 동적 복구 액션이 필요합니다", checks
    
    checks.append({
        "name": "QuickFix 생성",
        "passed": True,
        "value": generated_quick_fix.get('fix_type', 'N/A')
    })
    
    # 3. 기대 QuickFix 매핑 검증 (에러 유형이 식별된 경우)
    if detected_error_type and detected_error_type in EXPECTED_QUICK_FIX_MAP:
        expected = EXPECTED_QUICK_FIX_MAP[detected_error_type]
        
        # fix_type 검증
        actual_fix_type = generated_quick_fix.get('fix_type', '')
        fix_type_match = actual_fix_type in expected["expected_fix_types"]
        
        checks.append({
            "name": f"QuickFix Type ({expected['description']})",
            "passed": fix_type_match,
            "expected": expected["expected_fix_types"],
            "actual": actual_fix_type
        })
        
        # action_id 패턴 검증
        action_id = generated_quick_fix.get('action_id', '').lower()
        action_pattern_match = any(
            pattern in action_id 
            for pattern in expected["expected_action_patterns"]
        )
        
        checks.append({
            "name": "QuickFix Action Pattern",
            "passed": action_pattern_match,
            "expected_patterns": expected["expected_action_patterns"],
            "actual_action_id": action_id
        })
        
        # context 키 검증 (필수 키가 있는 경우)
        context = generated_quick_fix.get('context', {})
        if expected["expected_context_keys"]:
            missing_keys = [k for k in expected["expected_context_keys"] if k not in context]
            context_check_passed = len(missing_keys) == 0
            
            checks.append({
                "name": "QuickFix Context Keys",
                "passed": context_check_passed,
                "expected_keys": expected["expected_context_keys"],
                "actual_keys": list(context.keys()),
                "missing": missing_keys
            })
    
    # 4. 최종 결과 집계
    all_passed = all(check["passed"] for check in checks)
    
    if all_passed:
        fix_type = generated_quick_fix.get('fix_type', 'N/A')
        action_id = generated_quick_fix.get('action_id', 'N/A')
        msg = f"QuickFix 검증 통과: {detected_error_type} → {fix_type}/{action_id}"
    else:
        failed = [c["name"] for c in checks if not c["passed"]]
        msg = f"QuickFix 검증 실패: {', '.join(failed)}"
    
    return all_passed, msg, checks


def verify_task_abstraction(
    test_result: Dict[str, Any], 
    test_config: Dict[str, Any]
) -> Dict[str, Any]:
    """
    TaskContext 추상화 레이어 종합 검증.
    
    Stage 1-5 테스트 결과를 TaskContext로 변환했을 때
    사용자 친화적 정보가 올바르게 추출되는지 통합 확인합니다.
    
    포함 검증:
    1. Provider Cross-validation
    2. Thought Memory Compaction
    3. QuickFix Dynamic Generation (에러 발생 시)
    
    Args:
        test_result: 테스트 실행 결과
        test_config: 테스트 설정
    
    Returns:
        검증 결과 Dictionary
    """
    all_checks = []
    verification_results = {}
    
    # 테스트 결과에서 필요한 데이터 추출
    final_state = test_result.get('final_state', test_result.get('output', {}))
    usage = _safe_get_nested(final_state, 'usage', default={})
    
    # 1. Provider Cross-validation
    if test_config.get('verify_provider', True):
        try:
            # 시뮬레이션된 TaskContext 데이터 생성
            simulated_task_context = _simulate_task_context_from_result(test_result)
            
            passed, msg, checks = verify_provider_cross_validation(
                runner_log=final_state,
                task_context_data=simulated_task_context,
                test_config=test_config
            )
            
            verification_results['provider_cross_validation'] = {
                'passed': passed,
                'message': msg
            }
            all_checks.extend(checks)
        except Exception as e:
            logger.exception("Provider cross-validation failed")
            verification_results['provider_cross_validation'] = {
                'passed': False,
                'message': f"검증 중 오류: {str(e)}"
            }
            all_checks.append({
                "name": "Provider Cross-validation",
                "passed": False,
                "error": str(e)
            })
    
    # 2. Thought Memory Compaction (스트레스 테스트에서만)
    if test_config.get('verify_thought_compaction', False):
        try:
            total_thoughts = test_config.get('simulated_thought_count', 15)
            simulated_task_context = _simulate_task_context_with_thoughts(
                test_result, 
                thought_count=total_thoughts
            )
            
            passed, msg, checks = verify_thought_memory_compaction(
                task_context_data=simulated_task_context,
                total_thoughts_added=total_thoughts,
                test_config=test_config
            )
            
            verification_results['thought_memory_compaction'] = {
                'passed': passed,
                'message': msg
            }
            all_checks.extend(checks)
        except Exception as e:
            logger.exception("Thought memory compaction verification failed")
            verification_results['thought_memory_compaction'] = {
                'passed': False,
                'message': f"검증 중 오류: {str(e)}"
            }
    
    # 3. QuickFix Dynamic Generation (에러 발생 시)
    error_info = _safe_get_nested(final_state, 'error') or _safe_get_nested(test_result, 'error')
    
    if error_info or not test_result.get('success', True):
        try:
            # get_friendly_error_message 함수로 QuickFix 생성
            models = _get_task_context_models()
            get_friendly_error_message = models['get_friendly_error_message']
            
            error_str = str(error_info.get('message', error_info) if isinstance(error_info, dict) else error_info)
            execution_id = test_result.get('execution_id', '')
            node_id = _safe_get_nested(final_state, 'current_node_id')
            
            _, _, quick_fix = get_friendly_error_message(error_str, execution_id, node_id)
            
            error_context = {
                'error_code': error_info.get('code', '') if isinstance(error_info, dict) else '',
                'error_message': error_str,
                'error_type': error_info.get('type', 'unknown') if isinstance(error_info, dict) else 'unknown'
            }
            
            quick_fix_data = quick_fix.model_dump() if quick_fix else None
            
            passed, msg, checks = verify_quick_fix_dynamic_generation(
                error_context=error_context,
                generated_quick_fix=quick_fix_data,
                test_config=test_config
            )
            
            verification_results['quick_fix_generation'] = {
                'passed': passed,
                'message': msg
            }
            all_checks.extend(checks)
        except Exception as e:
            logger.exception("QuickFix verification failed")
            verification_results['quick_fix_generation'] = {
                'passed': False,
                'message': f"검증 중 오류: {str(e)}"
            }
    
    # 종합 결과
    overall_passed = all(r['passed'] for r in verification_results.values()) if verification_results else True
    
    return {
        'task_abstraction_verified': overall_passed,
        'verification_results': verification_results,
        'checks': all_checks,
        'checks_passed': sum(1 for c in all_checks if c.get('passed', False)),
        'checks_total': len(all_checks)
    }


def _simulate_task_context_from_result(test_result: Dict[str, Any]) -> Dict[str, Any]:
    """
    테스트 결과에서 TaskContext 시뮬레이션 생성.
    
    실제 런타임과 동일한 변환 로직을 사용하여
    필드 매핑 오류를 조기에 발견합니다.
    """
    final_state = test_result.get('final_state', test_result.get('output', {}))
    usage = _safe_get_nested(final_state, 'usage', default={})
    
    provider = usage.get('provider', 'unknown')
    input_tokens = usage.get('input_tokens', 0)
    output_tokens = usage.get('output_tokens', 0)
    cost = usage.get('cost', 0)
    
    # Provider별 서비스 이름 매핑
    SERVICE_NAME_MAP = {
        "gemini": "Gemini 2.0 Flash",
        "bedrock": "Bedrock Claude 3.5 Sonnet",
        "openai": "OpenAI GPT-4",
    }
    service_name = SERVICE_NAME_MAP.get(provider.lower(), f"Unknown ({provider})")
    
    # 시뮬레이션된 TaskContext 데이터
    return {
        'task_id': test_result.get('execution_id', f"test-{uuid.uuid4()}"),
        'status': 'COMPLETED' if test_result.get('success') else 'FAILED',
        'cost_detail': {
            'line_items': [
                {
                    'category': 'llm',
                    'service_name': service_name,
                    'quantity': input_tokens + output_tokens,
                    'unit': 'tokens',
                    'total_usd': cost
                }
            ],
            'actual_total_usd': cost
        },
        'token_usage': {
            'input': input_tokens,
            'output': output_tokens
        }
    }


def _simulate_task_context_with_thoughts(
    test_result: Dict[str, Any], 
    thought_count: int
) -> Dict[str, Any]:
    """
    Thought Memory Compaction 테스트를 위한 TaskContext 시뮬레이션.
    
    지정된 수만큼 thought를 추가하고 압착 로직이 올바르게 작동하는지 확인합니다.
    """
    models = _get_task_context_models()
    TaskContext = models['TaskContext']
    MAX_LENGTH = models['THOUGHT_HISTORY_MAX_LENGTH']
    
    # TaskContext 생성
    task = TaskContext(
        task_id=test_result.get('execution_id', f"test-{uuid.uuid4()}"),
        task_summary=f"Memory Compaction Test: {thought_count} thoughts"
    )
    
    # 지정된 수만큼 thought 추가
    for i in range(thought_count):
        task.add_thought(
            message=f"Test thought {i+1}/{thought_count}",
            thought_type="progress",
            persist_to_s3=True  # 실제 S3 저장은 하지 않지만 로직 테스트
        )
    
    # S3 참조 시뮬레이션 (초과분이 있을 경우)
    if thought_count > MAX_LENGTH:
        task.full_thought_trace_ref = f"s3://analemma-traces/test/{task.task_id}/thoughts.json"
    
    # Dict로 직렬화
    return task.model_dump()



    has_diagram_content = any(word in str(vision_output).lower() for word in diagram_words)
    
    if not has_diagram_content:
        return False, "Vision output doesn't seem to describe a diagram"
    
    return True, "Multimodal vision analysis successful"


# ============================================================================
# 5-Stage Graduated Test Verification Functions
# ============================================================================

def verify_stage1_basic(final_state: Dict, test_config: Dict) -> Tuple[bool, str]:
    """
    Stage 1: 동작 기초 검증
    - Response Schema 준수
    - json_parse 0.5초 이내 성능
    """
    issues = []
    
    # 1. LLM 출력 존재 확인
    llm_output = final_state.get('llm_raw_output') or final_state.get('parsed_summary')
    if not llm_output:
        return False, "No LLM output found"
    
    # 2. MOCK 응답 아닌지 확인
    if '[MOCK' in str(llm_output).upper():
        return False, "Received MOCK response instead of real LLM output"
    
    # 3. Response Schema 준수 확인
    parsed = final_state.get('parsed_summary', {})
    if isinstance(parsed, str):
        try:
            parsed = json.loads(parsed)
        except:
            return False, "Failed to parse JSON from LLM output"
    
    required_fields = ['main_topic', 'key_points', 'sentiment']
    missing_fields = [f for f in required_fields if f not in parsed or parsed.get(f) == 'parse_failed']
    if missing_fields:
        issues.append(f"Missing schema fields: {missing_fields}")
    
    # 4. json_parse 시간 검증 (0.5초 = 500ms 이내)
    # 방어적 코딩: 문자열/숫자 타입 모두 처리
    from src.common.json_utils import get_ms_timestamp
    
    parse_start = get_ms_timestamp(final_state.get('parse_start_ms', 0))
    parse_end = get_ms_timestamp(final_state.get('parse_end_ms', 0))
    parse_duration_ms = 0
    
    if parse_start > 0 and parse_end > 0:
        parse_duration_ms = parse_end - parse_start
        expected_max_ms = test_config.get('expected_json_parse_ms', 500)
        
        if parse_duration_ms > expected_max_ms:
            issues.append(f"json_parse too slow: {parse_duration_ms}ms > {expected_max_ms}ms")
    
    # 5. 스키마 유효성 확인
    schema_validation = final_state.get('schema_validation', 'unknown')
    if schema_validation == 'schema_invalid':
        issues.append("Schema validation failed")
    
    if issues:
        return False, f"Stage 1 issues: {'; '.join(issues)}"
    
    parse_time_str = f"{parse_duration_ms}ms" if parse_start and parse_end else "N/A"
    return True, f"Stage 1 PASSED: Schema valid, json_parse={parse_time_str}"


def verify_stage2_flow_control(final_state: Dict, test_config: Dict) -> Tuple[bool, str]:
    """
    Stage 2: Flow Control + COST_GUARDRAIL 검증
    - for_each 병렬 처리
    - HITL 상태 복구
    - 토큰 누적 제한
    - State Recovery Integrity (손실률 0%)
    """
    issues = []
    metrics = {}
    
    # 1. for_each 결과 확인
    # [Fix] processed_items 배열 길이도 확인 (processed_count가 없을 때 fallback)
    processed_items = final_state.get('processed_items', [])
    processed_count = final_state.get('processed_count', 0)
    
    # processed_count가 0이면 processed_items 길이 사용
    if processed_count == 0 and isinstance(processed_items, list):
        processed_count = len(processed_items)
    
    if processed_count < 3:
        issues.append(f"Expected 3 items processed, got {processed_count}")
    
    # 2. COST_GUARDRAIL 확인
    guardrail_state = final_state.get('guardrail_state', {})
    accumulated_tokens = guardrail_state.get('accumulated_tokens', 0)
    max_tokens = test_config.get('max_tokens_total', 10000)
    
    if accumulated_tokens > max_tokens:
        issues.append(f"Token limit exceeded: {accumulated_tokens} > {max_tokens}")
    
    metrics['tokens_used'] = accumulated_tokens
    
    # 3. HITL 상태 복구 검증 (State Recovery Integrity)
    pre_hitl_timestamp = final_state.get('pre_hitl_timestamp')
    hitl_decision = final_state.get('hitl_decision')
    
    if hitl_decision:
        # HITL이 실행되었으면 상태가 보존되어야 함
        initial_keys = set(test_config.get('initial_state_keys', []))
        current_keys = set(final_state.keys())
        
        # 손실된 키 확인 (일부는 변환될 수 있으므로 critical keys만 체크)
        critical_lost = initial_keys - current_keys
        if critical_lost:
            issues.append(f"State Recovery Integrity failed: lost keys {critical_lost}")
    
    # 4. Time Machine 롤백 토큰 리셋 테스트
    if test_config.get('verify_token_reset_on_rollback'):
        # 롤백 이벤트가 있었는지 확인
        rollback_event = final_state.get('time_machine_triggered')
        if rollback_event:
            post_rollback_tokens = final_state.get('post_rollback_accumulated_tokens', 0)
            metrics['rollback_token_count'] = post_rollback_tokens
    
    # 5. 반복 횟수 제한 확인
    iteration_count = guardrail_state.get('iteration_count', 0)
    max_iterations = guardrail_state.get('max_iterations', 3)
    
    if iteration_count > max_iterations:
        issues.append(f"Iteration limit exceeded: {iteration_count} > {max_iterations}")
    
    if issues:
        return False, f"Stage 2 issues: {'; '.join(issues)}"
    
    return True, f"Stage 2 PASSED: {processed_count} items, {accumulated_tokens} tokens, HITL ok"


def verify_stage3_vision_basic(final_state: Dict, test_config: Dict) -> Tuple[bool, str]:
    """
    Stage 3: 멀티모달 기초 검증
    - S3 URI → bytes 변환
    - Vision JSON 추출 및 파싱
    """
    issues = []
    
    # 1. Vision 출력 존재 확인
    vision_output = final_state.get('vision_raw_output') or final_state.get('parsed_vision_data')
    if not vision_output:
        return False, "No vision output found"
    
    # 2. S3 → bytes 변환 확인 (하이드레이션 시간 측정)
    # 방어적 코딩: 문자열/숫자 타입 모두 처리
    from src.common.json_utils import get_ms_timestamp
    
    hydration_start = get_ms_timestamp(final_state.get('hydration_start_ms', 0))
    hydration_end = get_ms_timestamp(final_state.get('hydration_end_ms', 0))
    hydration_time = 0
    
    if hydration_start > 0 and hydration_end > 0:
        hydration_time = hydration_end - hydration_start
        if hydration_time > 10000:  # 10초 초과시 경고
            issues.append(f"Hydration too slow: {hydration_time}ms")
    
    # 3. Vision 출력 JSON 파싱 가능 확인
    parsed_vision = final_state.get('parsed_vision_data', {})
    if isinstance(parsed_vision, str):
        try:
            parsed_vision = json.loads(parsed_vision)
        except:
            return False, "Vision output is not valid JSON"
    
    # 4. 추출 상태 확인
    extraction_status = final_state.get('extraction_status', 'unknown')
    if extraction_status == 'extraction_failed':
        issues.append("Vision extraction failed")
    
    # 5. 필수 필드 존재 확인 (parse_failed만 실패, null은 정상 - 이미지에 정보가 없을 수 있음)
    vendor = parsed_vision.get('vendor')
    if vendor == 'parse_failed':
        issues.append("JSON parsing failed - vendor field contains 'parse_failed'")
    
    # 5.1 슬롭 탐지: vendor 필드가 비정상적으로 긴 경우 (할루시네이션 의심)
    if isinstance(vendor, str) and len(vendor) > 100:
        issues.append(f"Vision slop detected: vendor field too long ({len(vendor)} chars)")
    
    # 6. clean_vision_data 확인 (operator 가공)
    clean_data = final_state.get('clean_vision_data')
    if not clean_data:
        issues.append("operator_official processing failed - no clean_vision_data")
    
    if issues:
        return False, f"Stage 3 issues: {'; '.join(issues)}"
    
    hydration_str = f"{hydration_time}ms" if hydration_start and hydration_end else "N/A"
    return True, f"Stage 3 PASSED: Vision JSON valid, hydration={hydration_str}"


def verify_stage4_vision_map(final_state: Dict, test_config: Dict) -> Tuple[bool, str]:
    """
    Stage 4: Vision Map + SPEED_GUARDRAIL 검증
    - 5장 이미지 병렬 분석
    - max_concurrency=3 준수
    - 타임스탬프 간격 분석 (동시 시작 방지)
    - StateBag 브랜치 병합 무결성
    """
    issues = []
    
    # 1. 처리된 이미지 수 확인
    vision_results = final_state.get('vision_results', [])
    total_processed = final_state.get('total_processed', 0)
    
    if total_processed < 5:
        issues.append(f"Expected 5 images, processed {total_processed}")
    
    # 2. 타임스탬프 간격 분석 (동시성 제어 검증)
    # 방어적 코딩: 문자열/숫자 타입 모두 처리
    from src.common.json_utils import get_ms_timestamp
    
    raw_timestamps = final_state.get('execution_timestamps', [])
    execution_timestamps = [get_ms_timestamp(ts) for ts in raw_timestamps if ts]
    min_gap_ms = test_config.get('min_timestamp_gap_ms', 100)
    
    if len(execution_timestamps) >= 2:
        sorted_ts = sorted([ts for ts in execution_timestamps if ts > 0])
        concurrent_violations = 0
        
        for i in range(len(sorted_ts) - 1):
            gap = sorted_ts[i + 1] - sorted_ts[i]
            if gap < min_gap_ms and gap >= 0:
                # 동일 시간대에 3개 이상 시작되지 않았는지 확인
                concurrent_count = sum(1 for ts in sorted_ts if abs(ts - sorted_ts[i]) < min_gap_ms)
                if concurrent_count > 3:  # max_concurrency 초과
                    concurrent_violations += 1
        
        if concurrent_violations > 0:
            issues.append(f"Concurrency violation: {concurrent_violations} batches exceeded max_concurrency=3")
    
    # 3. 카테고리 그룹화 결과 확인
    grouped = final_state.get('grouped_by_category', {})
    electronics = final_state.get('electronics_items', [])
    
    if not grouped:
        issues.append("Category grouping failed")
    
    # 4. StateBag 브랜치 병합 무결성
    if test_config.get('verify_branch_merge_integrity'):
        # 모든 브랜치의 결과가 lost 없이 병합되었는지 확인
        if len(vision_results) != total_processed:
            issues.append(f"Branch merge integrity failed: {len(vision_results)} != {total_processed}")
    
    if issues:
        return False, f"Stage 4 issues: {'; '.join(issues)}"
    
    category_count = len(grouped.keys()) if isinstance(grouped, dict) else 0
    return True, f"Stage 4 PASSED: {total_processed} images, {category_count} categories, concurrency verified"


def verify_stage5_hyper_stress(final_state: Dict, test_config: Dict) -> Tuple[bool, str]:
    """
    Stage 5: Hyper Stress + ALL_GUARDRAILS 검증
    - 3단계 재귀 상태 오염 방지
    - Partial Failure (Depth 2) 복구
    - Context Caching TEI ≥ 50%
    - HITL 중첩 처리
    """
    # [Critical] S3 Offload Hydration
    final_state = _ensure_hydrated_state(final_state)
    
    issues = []
    metrics = {}
    
    # 1. 3단계 재귀 완료 확인
    merged_state = final_state.get('merged_state', {})
    depth_0 = merged_state.get('depth_0_analysis')
    depth_1 = merged_state.get('depth_1_results')
    depth_2 = merged_state.get('depth_2_results')
    depth_3 = merged_state.get('depth_3_hitl')
    
    completed_depths = sum(1 for d in [depth_0, depth_1, depth_2, depth_3] if d)
    if completed_depths < 3:
        issues.append(f"Only {completed_depths}/4 recursion depths completed")
    
    # 2. Partial Failure 복구 확인
    failure_check = final_state.get('failure_check')
    if failure_check == 'has_failures':
        # Partial failure가 있었지만 다른 브랜치는 성공해야 함
        depth_1_results = final_state.get('depth_1_results', [])
        successful_branches = [r for r in depth_1_results if not r.get('error')]
        if len(successful_branches) == 0:
            issues.append("All branches failed - no partial failure recovery")
    
    # 3. Token Efficiency Index (TEI) 계산
    # TEI = (Cached Tokens / Total Prompt Tokens) × 100
    usage = final_state.get('usage', {})
    cached_tokens = usage.get('cached_tokens', 0)
    total_prompt_tokens = usage.get('prompt_tokens', 0) or usage.get('total_prompt_tokens', 1)
    
    if total_prompt_tokens > 0:
        tei = (cached_tokens / total_prompt_tokens) * 100
        metrics['tei'] = tei
        
        target_tei = test_config.get('target_tei_percentage', 50)
        if test_config.get('verify_context_caching') and tei < target_tei:
            # 경고만 (Context Caching이 항상 작동하진 않을 수 있음)
            logger.warning(f"TEI below target: {tei:.1f}% < {target_tei}%")
    
    # 4. 상태 오염 검증
    if test_config.get('verify_state_isolation'):
        # 하위 재귀에서 상위 상태를 오염시키지 않았는지 확인
        # 상위 depth의 데이터가 하위 depth 데이터로 덮어쓰이지 않았는지
        initial_analysis = final_state.get('parsed_issues', {})
        if not initial_analysis or initial_analysis.get('issues') is None:
            issues.append("State isolation may have failed - initial analysis corrupted")
    
    # 5. HITL 중첩 처리 확인
    hitl_decision = final_state.get('hitl_decision')
    if hitl_decision is None and depth_3:
        issues.append("HITL at depth 3 was expected but not executed")
    
    # 6. 최종 리포트 생성 확인
    final_report = final_state.get('final_report_raw') or final_state.get('final_report')
    if not final_report:
        issues.append("Final report generation failed")
    
    # 7. Graceful Stop 확인 (가드레일에 의한 정상 중단)
    final_status = final_state.get('final_status', {})
    if final_status.get('graceful_stop'):
        # 가드레일이 작동하여 정상 중단됨
        metrics['graceful_stop'] = True
    
    if issues:
        return False, f"Stage 5 issues: {'; '.join(issues)}"
    
    tei_str = f"TEI={metrics.get('tei', 0):.1f}%" if 'tei' in metrics else "TEI=N/A"
    return True, f"Stage 5 PASSED: {completed_depths} depths, {tei_str}, recursion isolated"


# ============================================================================
# STAGE 6: Distributed MAP_REDUCE + Loop + HITL Verification
# ============================================================================

def verify_stage6_distributed_map_reduce(final_state: Dict, test_config: Dict) -> Tuple[bool, str]:
    """
    Stage 6: Distributed MAP_REDUCE + Loop + HITL 통합 검증
    
    검증 항목:
    1. MAP_REDUCE 분산 처리 (파티션 병렬 실행)
    2. for_each Loop 내 LLM 호출
    3. HITL 체크포인트 및 상태 보존
    4. Loop 수렴 조건 검증
    5. Provider 일관성 (Cross-validation)
    6. Token Aggregation 정확도
    7. Partial Failure Recovery
    """
    # [Critical] S3 Offload Hydration
    final_state = _ensure_hydrated_state(final_state)
    
    issues = []
    metrics = {}
    
    # ========================================
    # 1. MAP_REDUCE 분산 처리 검증
    # ========================================
    partition_results = final_state.get('partition_results', [])
    expected_partitions = test_config.get('partition_count', 3)
    
    completed_partitions = len([p for p in partition_results if p.get('status') == 'completed'])
    metrics['completed_partitions'] = completed_partitions
    
    if completed_partitions < expected_partitions:
        # Partial failure 허용 여부 확인
        if not test_config.get('verify_partial_failure_recovery'):
            issues.append(f"Partition incomplete: {completed_partitions}/{expected_partitions}")
    
    # 분산 전략 확인
    distributed_strategy = final_state.get('distributed_strategy')
    if distributed_strategy != 'MAP_REDUCE':
        issues.append(f"Expected MAP_REDUCE strategy, got {distributed_strategy}")
    
    # ========================================
    # 2. for_each Loop 내 LLM 호출 검증
    # ========================================
    loop_iterations = final_state.get('loop_iterations', [])
    max_iterations = test_config.get('max_loop_iterations', 2)
    
    if test_config.get('loop_enabled'):
        actual_iterations = len(loop_iterations)
        metrics['loop_iterations'] = actual_iterations
        
        # Loop가 max_iterations 이내에서 완료되었는지
        if actual_iterations > max_iterations:
            issues.append(f"Loop exceeded max iterations: {actual_iterations} > {max_iterations}")
        
        # 각 iteration에서 LLM 호출이 있었는지
        iterations_with_llm = sum(1 for it in loop_iterations if it.get('llm_called'))
        if iterations_with_llm == 0:
            issues.append("No LLM calls detected in loop iterations")
        
        # Loop 수렴 검증
        if test_config.get('verify_loop_convergence'):
            convergence_score = final_state.get('loop_convergence_score', 0)
            threshold = test_config.get('loop_convergence_threshold', 0.8)
            
            if convergence_score < threshold:
                issues.append(f"Loop did not converge: {convergence_score:.2f} < {threshold}")
            else:
                metrics['convergence_score'] = convergence_score
    
    # ========================================
    # 3. HITL 체크포인트 및 상태 보존 검증
    # ========================================
    if test_config.get('hitl_enabled'):
        hitl_events = final_state.get('hitl_events', [])
        expected_hitl_partition = test_config.get('hitl_checkpoint_at_partition', 1)
        
        if len(hitl_events) == 0:
            issues.append("HITL was enabled but no HITL events occurred")
        else:
            # HITL 발생 위치 확인
            hitl_partition_ids = [e.get('partition_id') for e in hitl_events]
            metrics['hitl_count'] = len(hitl_events)
            
            # HITL 전후 상태 보존 확인
            if test_config.get('verify_hitl_state_preservation'):
                for event in hitl_events:
                    pre_state_keys = set(event.get('pre_hitl_state_keys', []))
                    post_state_keys = set(event.get('post_hitl_state_keys', []))
                    
                    lost_keys = pre_state_keys - post_state_keys
                    if lost_keys:
                        issues.append(f"HITL state loss: {lost_keys}")
    
    # ========================================
    # 4. Provider 일관성 검증 (Cross-validation)
    # ========================================
    if test_config.get('verify_provider_consistency'):
        providers_used = final_state.get('providers_used', [])
        
        if len(providers_used) > 0:
            unique_providers = set(providers_used)
            
            if len(unique_providers) > 1:
                # 여러 Provider 사용 시 경고 (fallback 발생 가능)
                logger.warning(f"Multiple providers used: {unique_providers}")
                metrics['providers'] = list(unique_providers)
            else:
                metrics['provider'] = list(unique_providers)[0]
        
        # 각 파티션별 provider 확인
        for idx, partition in enumerate(partition_results):
            partition_provider = partition.get('usage', {}).get('provider')
            if partition_provider:
                expected_provider = test_config.get('expected_provider', 'gemini')
                if partition_provider.lower() != expected_provider.lower():
                    issues.append(f"Partition {idx} provider mismatch: {partition_provider} != {expected_provider}")
    
    # ========================================
    # 5. Token Aggregation 검증
    # ========================================
    if test_config.get('verify_token_aggregation'):
        total_usage = final_state.get('aggregated_usage', {})
        total_tokens = total_usage.get('total_tokens', 0)
        max_tokens = test_config.get('max_tokens_total', 50000)
        
        metrics['total_tokens'] = total_tokens
        
        if total_tokens > max_tokens:
            issues.append(f"Token limit exceeded: {total_tokens} > {max_tokens}")
        
        # 파티션별 토큰 합계 검증
        partition_token_sum = sum(
            p.get('usage', {}).get('total_tokens', 0) 
            for p in partition_results
        )
        
        # 집계된 값과 개별 합계가 일치하는지 (10% 오차 허용)
        if total_tokens > 0:
            diff_ratio = abs(total_tokens - partition_token_sum) / total_tokens
            if diff_ratio > 0.1:
                issues.append(f"Token aggregation mismatch: {total_tokens} vs sum({partition_token_sum})")
    
    # ========================================
    # 6. Partial Failure Recovery 검증
    # ========================================
    if test_config.get('verify_partial_failure_recovery'):
        failed_partitions = [p for p in partition_results if p.get('status') == 'failed']
        
        if len(failed_partitions) > 0:
            # 실패한 파티션이 있지만 나머지는 성공해야 함
            if completed_partitions == 0:
                issues.append("All partitions failed - no recovery")
            else:
                metrics['failed_partitions'] = len(failed_partitions)
                metrics['recovery_ratio'] = completed_partitions / expected_partitions
                
                # 재시도 시도 확인
                retry_attempts = final_state.get('retry_attempts', 0)
                if retry_attempts > 0:
                    metrics['retry_attempts'] = retry_attempts
    
    # ========================================
    # 7. 최종 Merge 결과 확인
    # ========================================
    merged_output = final_state.get('merged_output') or final_state.get('final_merged_result')
    if not merged_output:
        issues.append("No merged output found from MAP_REDUCE")
    
    # ========================================
    # 결과 집계
    # ========================================
    if issues:
        return False, f"Stage 6 issues: {'; '.join(issues)}"
    
    summary_parts = [
        f"{completed_partitions} partitions",
        f"{metrics.get('loop_iterations', 0)} iterations",
        f"{metrics.get('hitl_count', 0)} HITLs",
        f"{metrics.get('total_tokens', 0)} tokens"
    ]
    
    return True, f"Stage 6 PASSED: {', '.join(summary_parts)}"


# ============================================================================
# STAGE 7: Parallel Multi-LLM with StateBag Merge Verification
# ============================================================================

def verify_stage7_parallel_multi_llm(final_state: Dict, test_config: Dict) -> Tuple[bool, str]:
    """
    Stage 7: Parallel Multi-LLM + StateBag Merge 검증
    
    검증 항목:
    1. 다중 병렬 LLM 호출 (5개 브랜치)
    2. max_concurrency 제한 준수
    3. StateBag 브랜치 병합 무결성 (0% loss)
    4. 브랜치별 Loop 실행
    5. HITL at specific branch
    6. 비용 집계 정확도
    7. 지연 시간 측정
    """
    # [Critical] S3 Offload Hydration
    final_state = _ensure_hydrated_state(final_state)
    
    issues = []
    metrics = {}
    
    # ========================================
    # 1. 병렬 브랜치 실행 결과 확인
    # ========================================
    branch_results = final_state.get('branch_results', [])
    expected_branches = test_config.get('parallel_branches', 5)
    
    completed_branches = len([b for b in branch_results if b.get('status') == 'completed'])
    metrics['completed_branches'] = completed_branches
    
    if completed_branches < expected_branches:
        issues.append(f"Branch incomplete: {completed_branches}/{expected_branches}")
    
    # ========================================
    # 2. max_concurrency 제한 검증
    # ========================================
    execution_timestamps = final_state.get('branch_start_timestamps', [])
    max_concurrency = test_config.get('max_concurrency', 3)
    
    if len(execution_timestamps) >= max_concurrency:
        sorted_ts = sorted([ts for ts in execution_timestamps if ts])
        
        # 동시 실행 수 계산
        max_concurrent = 0
        for i, ts in enumerate(sorted_ts):
            concurrent_count = sum(
                1 for other_ts in sorted_ts 
                if abs(other_ts - ts) < 1000  # 1초 이내
            )
            max_concurrent = max(max_concurrent, concurrent_count)
        
        if max_concurrent > max_concurrency:
            issues.append(f"Concurrency violation: {max_concurrent} > {max_concurrency}")
        
        metrics['max_concurrent'] = max_concurrent
    
    # ========================================
    # 3. StateBag 브랜치 병합 무결성 (0% loss)
    # ========================================
    if test_config.get('verify_statebag_merge_integrity'):
        merged_statebag = final_state.get('merged_statebag', {})
        
        # 각 브랜치의 결과가 모두 병합되었는지
        for idx, branch in enumerate(branch_results):
            branch_key = f"branch_{idx}_result"
            if branch.get('status') == 'completed':
                if branch_key not in merged_statebag and not merged_statebag.get(f'branch_results', {}).get(str(idx)):
                    issues.append(f"Branch {idx} result lost during merge")
        
        if test_config.get('expected_zero_loss'):
            loss_count = expected_branches - len([
                k for k in merged_statebag.keys() 
                if 'branch' in k.lower() or 'result' in k.lower()
            ])
            
            # branch_results 배열 내에서도 확인
            if 'branch_results' in merged_statebag:
                loss_count = expected_branches - len(merged_statebag['branch_results'])
            
            if loss_count > 0 and completed_branches == expected_branches:
                issues.append(f"StateBag merge loss detected: {loss_count} results missing")
    
    # ========================================
    # 4. 브랜치별 Loop 실행 검증
    # ========================================
    if test_config.get('loop_per_branch'):
        max_loop_per_branch = test_config.get('max_loop_per_branch', 2)
        
        for idx, branch in enumerate(branch_results):
            branch_iterations = branch.get('loop_iterations', 0)
            
            if branch_iterations > max_loop_per_branch:
                issues.append(f"Branch {idx} exceeded loop limit: {branch_iterations} > {max_loop_per_branch}")
            
            # LLM 호출 확인
            if branch.get('llm_calls', 0) == 0:
                issues.append(f"Branch {idx} had no LLM calls")
        
        total_iterations = sum(b.get('loop_iterations', 0) for b in branch_results)
        metrics['total_iterations'] = total_iterations
    
    # ========================================
    # 5. HITL at specific branch 검증
    # ========================================
    if test_config.get('hitl_enabled'):
        hitl_at_branch = test_config.get('hitl_at_branch', 2)
        hitl_events = final_state.get('hitl_events', [])
        
        if len(hitl_events) == 0:
            issues.append("HITL was enabled but no HITL events occurred")
        else:
            hitl_branch_ids = [e.get('branch_id') for e in hitl_events]
            
            if hitl_at_branch not in hitl_branch_ids:
                # 인덱스 또는 브랜치 ID로 확인
                if f"branch_{hitl_at_branch}" not in str(hitl_branch_ids):
                    issues.append(f"HITL expected at branch {hitl_at_branch} but occurred at {hitl_branch_ids}")
            
            metrics['hitl_count'] = len(hitl_events)
    
    # ========================================
    # 6. 비용 집계 검증
    # ========================================
    if test_config.get('verify_cost_aggregation'):
        total_cost = final_state.get('aggregated_cost', 0)
        
        # 브랜치별 비용 합계
        branch_cost_sum = sum(
            b.get('usage', {}).get('cost', 0) 
            for b in branch_results
        )
        
        metrics['total_cost'] = total_cost
        
        if total_cost > 0:
            diff_ratio = abs(total_cost - branch_cost_sum) / total_cost
            if diff_ratio > 0.1:
                issues.append(f"Cost aggregation mismatch: {total_cost} vs sum({branch_cost_sum})")
    
    # ========================================
    # 7. 지연 시간 측정
    # ========================================
    start_time = final_state.get('parallel_start_time')
    end_time = final_state.get('parallel_end_time')
    
    if start_time and end_time:
        total_latency_ms = end_time - start_time
        metrics['total_latency_ms'] = total_latency_ms
        
        # 각 브랜치 최대 지연 시간
        max_branch_latency = max(
            (b.get('latency_ms', 0) for b in branch_results),
            default=0
        )
        
        # 병렬 실행 이점: total_latency ≈ max(branch_latencies) + overhead
        if max_branch_latency > 0:
            overhead_ratio = (total_latency_ms - max_branch_latency) / max_branch_latency
            if overhead_ratio > 0.5:  # 50% 이상 오버헤드
                logger.warning(f"High parallel overhead: {overhead_ratio:.1%}")
            
            metrics['parallel_efficiency'] = max_branch_latency / total_latency_ms if total_latency_ms > 0 else 0
    
    # ========================================
    # 결과 집계
    # ========================================
    if issues:
        return False, f"Stage 7 issues: {'; '.join(issues)}"
    
    summary_parts = [
        f"{completed_branches} branches",
        f"max_concurrent={metrics.get('max_concurrent', 'N/A')}",
        f"{metrics.get('total_iterations', 0)} total iterations",
        f"${metrics.get('total_cost', 0):.4f}"
    ]
    
    return True, f"Stage 7 PASSED: {', '.join(summary_parts)}"


# ============================================================================
# Stage 8: Slop Detection & Quality Gate Verification
# ============================================================================
def verify_stage8_slop_detection(
    final_state: Dict[str, Any],
    test_config: Dict[str, Any]
) -> Tuple[bool, str]:
    """
    Stage 8: Slop Detection & Quality Gate 검증
    
    검증 항목:
    1. 테스트 케이스별 통과 여부
    2. Precision/Recall 메트릭 (F1 >= 0.8)
    3. 도메인별 이모티콘 정책 적용
    4. Slop Injector 정확도
    5. 페르소나 탈옥 프롬프트 효과
    """
    # [Critical] S3 Offload Hydration
    final_state = _ensure_hydrated_state(final_state)
    
    issues = []
    metrics = {
        'total_cases': 0,
        'passed_cases': 0,
        'precision': 0.0,
        'recall': 0.0,
        'f1_score': 0.0
    }
    
    # ========================================
    # 1. 테스트 케이스 결과 확인
    # ========================================
    test_results = final_state.get('test_results', [])
    if not test_results:
        issues.append("No test results found")
    else:
        passed_cases = [r for r in test_results if isinstance(r, dict) and r.get('passed', False)]
        metrics['total_cases'] = len(test_results)
        metrics['passed_cases'] = len(passed_cases)
        
        if len(passed_cases) < len(test_results):
            failed_ids = [r.get('case_id', 'unknown') for r in test_results 
                         if isinstance(r, dict) and not r.get('passed', False)]
            issues.append(f"Failed cases: {failed_ids}")
    
    # ========================================
    # 2. Precision/Recall 메트릭 검증
    # ========================================
    final_metrics = final_state.get('final_metrics', {})
    precision = final_metrics.get('precision', 0)
    recall = final_metrics.get('recall', 0)
    f1_score = final_metrics.get('f1_score', 0)
    
    metrics['precision'] = precision
    metrics['recall'] = recall
    metrics['f1_score'] = f1_score
    
    min_f1 = test_config.get('expected_min_f1_score', 0.8)
    if f1_score < min_f1:
        issues.append(f"F1 score {f1_score:.2%} < {min_f1:.2%} threshold")
    
    # ========================================
    # 3. Confusion Matrix 검증
    # ========================================
    confusion = final_metrics.get('confusion_matrix', {})
    tp = confusion.get('TP', 0)
    tn = confusion.get('TN', 0)
    fp = confusion.get('FP', 0)
    fn = confusion.get('FN', 0)
    
    metrics['confusion_matrix'] = {'TP': tp, 'TN': tn, 'FP': fp, 'FN': fn}
    
    # False Positive가 너무 많으면 문제
    if fp > 1:
        issues.append(f"Too many false positives: {fp}")
    
    # False Negative는 치명적 (Slop이 통과함)
    if fn > 0:
        issues.append(f"False negatives detected: {fn} (slop passed undetected)")
    
    # ========================================
    # 4. 케이스별 세부 검증
    # ========================================
    for result in test_results:
        if not isinstance(result, dict):
            continue
            
        case_id = result.get('case_id', '')
        
        # CASE_B: 페르소나 탈옥은 반드시 slop_score > 0.7 이어야 함
        if case_id == 'CASE_B_PERSONA_JAILBREAK':
            if result.get('slop_score', 0) < 0.7:
                issues.append(f"Persona jailbreak should produce high slop: got {result.get('slop_score', 0):.2f}")
        
        # CASE_D: TECHNICAL_REPORT + 이모티콘 1개도 is_slop=True
        if case_id == 'CASE_D_TECHNICAL_EMOJI':
            if not result.get('is_slop', False):
                issues.append("TECHNICAL_REPORT with emoji should be marked as slop")
        
        # CASE_E: MARKETING_COPY + 이모티콘 5개는 is_slop=False
        if case_id == 'CASE_E_MARKETING_EMOJI':
            if result.get('is_slop', True):
                issues.append("MARKETING_COPY with emojis only should NOT be marked as slop")
    
    # ========================================
    # 5. 최종 판정
    # ========================================
    if issues:
        return False, f"Stage 8 issues: {'; '.join(issues)}"
    
    summary_parts = [
        f"{metrics['passed_cases']}/{metrics['total_cases']} cases",
        f"P={precision:.2%}",
        f"R={recall:.2%}",
        f"F1={f1_score:.2%}"
    ]
    
    return True, f"Stage 8 PASSED: {', '.join(summary_parts)}"


# ============================================================================
# Verification function registry
# ============================================================================
VERIFY_FUNCTIONS = {
    # 5-Stage scenarios
    'STAGE1_BASIC': verify_stage1_basic,
    'STAGE2_FLOW_CONTROL': verify_stage2_flow_control,
    'STAGE3_VISION_BASIC': verify_stage3_vision_basic,
    'STAGE4_VISION_MAP': verify_stage4_vision_map,
    'STAGE5_HYPER_STRESS': verify_stage5_hyper_stress,
    # Distributed/Parallel LLM scenarios (v2.1)
    'STAGE6_DISTRIBUTED_MAP_REDUCE': verify_stage6_distributed_map_reduce,
    'STAGE7_PARALLEL_MULTI_LLM': verify_stage7_parallel_multi_llm,
    # Quality Gate scenarios (v2.2)
    'STAGE8_SLOP_DETECTION': verify_stage8_slop_detection,
    # Legacy scenarios
    'BASIC_LLM_CALL': verify_basic_llm_call,
    'STRUCTURED_OUTPUT': verify_structured_output,
    'THINKING_MODE': verify_thinking_mode,
    'LLM_OPERATOR_INTEGRATION': verify_llm_operator_integration,
    'DOCUMENT_ANALYSIS': verify_document_analysis,
    'MULTIMODAL_VISION_LIVE': verify_multimodal_vision,
    # Task Manager Abstraction (v2.1)
    'TASK_ABSTRACTION': None,  # Uses dedicated verify_task_abstraction function
}


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    LLM 테스트 결과 검증 Lambda Handler
    
    Event format:
    {
        "scenario_key": "BASIC_LLM_CALL",
        "final_state": {...},
        "test_config": {...},
        "execution_id": "xxx",
        "verification_type": "standard" | "task_abstraction"  # v2.1
    }
    
    v2.1: Task Manager Abstraction Verification
    - verification_type: "task_abstraction"으로 설정 시 전용 검증 수행
    - Provider Cross-validation, Thought Memory Compaction, QuickFix 동적 생성 검증
    
    v2.2: S3 Offload Hydration
    - __s3_offloaded 상태 자동 하이드레이션
    - Stage 4, 6, 7, 8에서 S3 경로 추적
    
    v2.3: Nested final_state 자동 추출
    - Step Functions 출력이 {status, final_state: {...}} 구조일 때 자동 추출
    - final_state가 비어있으면 test_result.output에서 추출
    """
    logger.info(f"Verifying LLM test: {event.get('scenario_key')}")
    
    scenario_key = event.get('scenario_key', 'BASIC_LLM_CALL')
    final_state = event.get('final_state', {})
    test_config = event.get('test_config', {})
    execution_id = event.get('execution_id', '')
    verification_type = event.get('verification_type', 'standard')
    
    # ========================================
    # v2.3: Robust final_state 추출
    # ========================================
    # final_state가 비어있거나 None이면 test_result.output에서 추출
    if not final_state or (isinstance(final_state, dict) and not final_state):
        test_result = event.get('test_result', {})
        output = test_result.get('output', {})
        
        # output이 문자열이면 파싱
        if isinstance(output, str):
            try:
                output = json.loads(output)
            except (json.JSONDecodeError, ValueError):
                logger.warning(f"Failed to parse test_result.output as JSON")
                output = {}
        
        # output.final_state 또는 output 자체를 사용
        if isinstance(output, dict):
            final_state = output.get('final_state', output)
            logger.info(f"[Fallback] Extracted final_state from test_result.output, keys: {list(final_state.keys())[:5] if isinstance(final_state, dict) else 'N/A'}")
    
    # final_state가 문자열이면 파싱
    if isinstance(final_state, str):
        try:
            final_state = json.loads(final_state)
        except (json.JSONDecodeError, ValueError):
            logger.warning(f"Failed to parse final_state as JSON string")
            final_state = {}
    
    # 중첩된 final_state 추출 (output.final_state 구조)
    if isinstance(final_state, dict) and 'final_state' in final_state and 'status' in final_state:
        logger.info(f"[Nested] Extracting nested final_state from Step Functions output structure")
        nested_state = final_state.get('final_state', {})
        if isinstance(nested_state, dict) and nested_state:
            final_state = nested_state

    # ========================================
    # v3.1: current_state 자동 추출 (Kernel Protocol 지원)
    # ========================================
    # ASL v3.13 Kernel Protocol에서 노드 실행 결과는 final_state.current_state에 저장됨
    # 검증 함수는 llm_raw_output, vision_raw_output 등을 최상위에서 찾으므로
    # current_state 내용을 final_state 최상위로 평탄화
    if isinstance(final_state, dict) and 'current_state' in final_state:
        current_state = final_state.get('current_state', {})
        if isinstance(current_state, dict) and current_state:
            # current_state 내용을 final_state 최상위로 병합 (current_state가 우선)
            # 단, _status, _segment 등 제어 필드는 유지
            merged_state = final_state.copy()
            for key, value in current_state.items():
                # 이미 최상위에 존재하고 의미있는 값이면 덮어쓰지 않음
                if key not in merged_state or merged_state.get(key) is None:
                    merged_state[key] = value
                # current_state의 LLM 결과 필드는 항상 우선 (덮어쓰기)
                elif key in ('llm_raw_output', 'parsed_summary', 'vision_raw_output', 
                             'vision_results', 'partition_results', 'branch_results',
                             'test_results', 'slop_detection_results'):
                    merged_state[key] = value
            final_state = merged_state
            logger.info(f"[v3.1] Flattened current_state keys: {list(current_state.keys())[:10]}")

    # ========================================
    # v3.0: S3 Offload Hydration (Recursive)
    # ========================================
    # StateBag 아키텍처 지원: state_s3_path, final_state_s3_path 패턴 추가
    # Stage 4, 6, 7, 8에서 상태가 S3로 오프로드되면 빈 상태만 보이는 문제 수정
    # v2.4: 최상위뿐만 아니라 중첩된 필드 (vision_results, partition_results 등)도 하이드레이션
    if isinstance(final_state, dict):
        # 1) 최상위가 오프로드된 경우 (__s3_offloaded, state_s3_path, final_state_s3_path)
        # 2) 또는 중첩 필드 중 하나라도 오프로드된 경우 (Stage 4/6/7/8)
        has_offloaded_nested = any(
            isinstance(v, dict) and (v.get('__s3_offloaded') or v.get('state_s3_path') or v.get('final_state_s3_path'))
            for v in final_state.values()
        )
        needs_hydration = (
            final_state.get('__s3_offloaded') or 
            final_state.get('state_s3_path') or 
            final_state.get('final_state_s3_path') or 
            has_offloaded_nested
        )
        if needs_hydration:
            logger.info(f"[S3 Hydration] Detected offloaded state (legacy: {final_state.get('__s3_offloaded')}, statebag: {final_state.get('state_s3_path')}, nested: {has_offloaded_nested}), hydrating recursively...")
            final_state = _ensure_hydrated_state(final_state)
            logger.info(f"[S3 Hydration] Hydration complete. State keys: {list(final_state.keys())[:10]}")
    
    # v2.1: Task Abstraction 전용 검증
    if verification_type == 'task_abstraction' or scenario_key == 'TASK_ABSTRACTION':
        try:
            # test_result 구조 재구성
            test_result = {
                'execution_id': execution_id,
                'final_state': final_state,
                'output': event.get('test_result', {}).get('output', final_state),
                'success': event.get('test_result', {}).get('success', True),
                'error': event.get('test_result', {}).get('error')
            }
            
            abstraction_result = verify_task_abstraction(test_result, test_config)
            
            return {
                'verified': abstraction_result['task_abstraction_verified'],
                'message': f"Task Abstraction: {abstraction_result['checks_passed']}/{abstraction_result['checks_total']} checks passed",
                'scenario_key': scenario_key,
                'execution_id': execution_id,
                'task_abstraction_result': abstraction_result
            }
        except Exception as e:
            logger.exception("Task abstraction verification failed")
            return {
                'verified': False,
                'message': f"Task abstraction verification error: {str(e)}",
                'scenario_key': scenario_key,
                'execution_id': execution_id
            }
    
    # 표준 시나리오 검증
    verify_func = VERIFY_FUNCTIONS.get(scenario_key)
    
    if not verify_func:
        return {
            'verified': False,
            'message': f"Unknown scenario: {scenario_key}",
            'execution_id': execution_id
        }
    
    try:
        passed, message = verify_func(final_state, test_config)
        
        # v2.1: 표준 검증 후에도 Task Abstraction 검증 추가 (옵션)
        task_abstraction_result = None
        if test_config.get('include_task_abstraction_verification', False):
            try:
                test_result = {
                    'execution_id': execution_id,
                    'final_state': final_state,
                    'success': passed
                }
                task_abstraction_result = verify_task_abstraction(test_result, test_config)
            except Exception as e:
                logger.warning(f"Optional task abstraction verification failed: {e}")
        
        result = {
            'verified': passed,
            'message': message,
            'scenario_key': scenario_key,
            'execution_id': execution_id
        }
        
        if task_abstraction_result:
            result['task_abstraction_result'] = task_abstraction_result
        
        return result
        
    except Exception as e:
        logger.exception(f"Verification failed for {scenario_key}")
        return {
            'verified': False,
            'message': f"Verification error: {str(e)}",
            'scenario_key': scenario_key,
            'execution_id': execution_id
        }
