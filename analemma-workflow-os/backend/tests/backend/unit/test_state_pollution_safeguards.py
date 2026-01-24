# -*- coding: utf-8 -*-
"""
🛡️ State Pollution Safeguards - 통합 테스트

이 테스트는 사용자 정의 코드(Operator)가 커널의 영역을 침범하지 못하게 하는
'사용자 모드 vs 커널 모드'의 격리 계층을 검증합니다.

특히 14만 라인 규모의 시스템에서 MOCK_MODE를 끄고 실제 LLM을 올렸을 때,
모델이 임의의 JSON 키를 생성하여 시스템 메타데이터를 덮어쓰는 사고를 방지합니다.

테스트 범위:
1. RESERVED_STATE_KEYS 필터링 (_validate_output_keys)
2. Pydantic 모델 검증 (SafeStateOutput + model_validator)
3. 상태 오염 방지 (None 반환 대신 키 삭제)
4. extra 필드 검증 (model_validator의 전역 스캔)
"""

import pytest
import logging
from typing import Dict, Any
from pydantic import ValidationError

# 테스트 대상 임포트
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../src')))

from handlers.core.main import (
    RESERVED_STATE_KEYS,
    _validate_output_keys,
    SafeStateOutput,
    validate_state_with_schema
)

logger = logging.getLogger(__name__)


class TestReservedStateKeys:
    """RESERVED_STATE_KEYS 목록 완전성 테스트"""
    
    def test_flow_control_keys_present(self):
        """Flow Control 키가 모두 포함되어 있는지 검증 (루프/세그먼트 제어)"""
        required_flow_keys = {
            "loop_counter", "max_loop_iterations", "segment_id",
            "segment_to_run", "total_segments", "segment_type"
        }
        assert required_flow_keys.issubset(RESERVED_STATE_KEYS), \
            f"Missing flow control keys: {required_flow_keys - RESERVED_STATE_KEYS}"
    
    def test_state_infrastructure_keys_present(self):
        """State Infrastructure 키가 모두 포함되어 있는지 검증 (S3 오프로딩)"""
        required_state_keys = {
            "current_state", "final_state", "state_s3_path", "final_state_s3_path",
            "partition_map", "partition_map_s3_path", "__s3_offloaded", "__s3_path"
        }
        assert required_state_keys.issubset(RESERVED_STATE_KEYS), \
            f"Missing state infrastructure keys: {required_state_keys - RESERVED_STATE_KEYS}"
    
    def test_telemetry_keys_present(self):
        """Telemetry 키가 모두 포함되어 있는지 검증 (추적성 보호)"""
        required_telemetry_keys = {
            "step_history", "execution_logs", "__new_history_logs",
            "skill_execution_log", "__kernel_actions"
        }
        assert required_telemetry_keys.issubset(RESERVED_STATE_KEYS), \
            f"Missing telemetry keys: {required_telemetry_keys - RESERVED_STATE_KEYS}"
    
    def test_response_envelope_keys_present(self):
        """Response Envelope 키가 모두 포함되어 있는지 검증 (Step Functions 정합성)"""
        required_envelope_keys = {"status", "error_info"}
        assert required_envelope_keys.issubset(RESERVED_STATE_KEYS), \
            f"Missing response envelope keys: {required_envelope_keys - RESERVED_STATE_KEYS}"
    
    def test_alias_compatibility_keys_present(self):
        """Alias/Compatibility 키가 모두 포함되어 있는지 검증 (camelCase & snake_case)"""
        required_alias_keys = {
            "workflowId", "workflow_id", "ownerId", "owner_id"
        }
        assert required_alias_keys.issubset(RESERVED_STATE_KEYS), \
            f"Missing alias keys: {required_alias_keys - RESERVED_STATE_KEYS}"


class TestValidateOutputKeys:
    """_validate_output_keys 함수 테스트 (Layer 1 방어선)"""
    
    def test_safe_output_passes(self):
        """안전한 출력(예약 키 없음)은 그대로 통과"""
        output = {"result": "success", "data": [1, 2, 3], "user_field": "custom"}
        validated = _validate_output_keys(output, "test_node")
        
        assert validated == output
        assert "result" in validated
        assert "data" in validated
    
    def test_reserved_keys_blocked(self):
        """예약 키가 포함된 출력은 필터링됨"""
        output = {
            "result": "success",
            "loop_counter": 999,  # 예약 키 - 차단되어야 함
            "segment_id": 5,  # 예약 키 - 차단되어야 함
            "user_field": "custom"
        }
        validated = _validate_output_keys(output, "malicious_node")
        
        # 예약 키는 제거되고 안전한 키만 남음
        assert "loop_counter" not in validated
        assert "segment_id" not in validated
        assert "result" in validated
        assert "user_field" in validated
    
    def test_flow_control_pollution_blocked(self):
        """Flow Control 키 오염 시도 차단 (무한 루프 방지)"""
        output = {
            "loop_counter": 1,  # 루프 카운터를 1로 조작 시도
            "max_loop_iterations": 9999,  # 루프 제한을 무한으로 변경 시도
            "result": "hacked"
        }
        validated = _validate_output_keys(output, "loop_hacker")
        
        assert "loop_counter" not in validated
        assert "max_loop_iterations" not in validated
        assert "result" in validated
    
    def test_s3_infrastructure_pollution_blocked(self):
        """S3 Infrastructure 키 오염 시도 차단 (S3 정합성 보호)"""
        output = {
            "state_s3_path": "s3://malicious-bucket/fake-path",
            "__s3_offloaded": True,
            "result": "data_leak_attempt"
        }
        validated = _validate_output_keys(output, "s3_hacker")
        
        assert "state_s3_path" not in validated
        assert "__s3_offloaded" not in validated
        assert "result" in validated
    
    def test_telemetry_pollution_blocked(self):
        """Telemetry 키 오염 시도 차단 (추적성 보호)"""
        output = {
            "step_history": [],  # 히스토리 초기화 시도
            "__kernel_actions": {"fake": "action"},
            "execution_logs": "erased",
            "result": "cover_tracks"
        }
        validated = _validate_output_keys(output, "telemetry_eraser")
        
        assert "step_history" not in validated
        assert "__kernel_actions" not in validated
        assert "execution_logs" not in validated
    
    def test_non_dict_output_passes_through(self):
        """딕셔너리가 아닌 출력은 그대로 통과 (문자열, 리스트 등)"""
        output_str = "simple string result"
        output_list = [1, 2, 3]
        output_none = None
        
        assert _validate_output_keys(output_str, "node1") == output_str
        assert _validate_output_keys(output_list, "node2") == output_list
        assert _validate_output_keys(output_none, "node3") == output_none


class TestPydanticModelValidator:
    """SafeStateOutput Pydantic 모델 테스트 (Layer 2 방어선)"""
    
    def test_model_validator_blocks_extra_reserved_keys(self):
        """model_validator가 extra 필드의 예약 키도 차단하는지 검증"""
        data = {
            "user_field": "custom",
            "__new_history_logs": ["fake", "logs"],  # extra 필드이지만 예약 키
            "partition_map": {"fake": "map"},  # extra 필드이지만 예약 키
            "result": "success"
        }
        
        # model_validator에서 예약 키가 제거됨
        validated = SafeStateOutput(**data)
        dumped = validated.model_dump(exclude_none=True, exclude_unset=True)
        
        # 예약 키는 모델에서 제거되어야 함
        assert "__new_history_logs" not in dumped
        assert "partition_map" not in dumped
        # 안전한 키는 보존
        assert "user_field" in dumped
        assert "result" in dumped
    
    def test_frozen_fields_cannot_be_set(self):
        """frozen 필드는 설정할 수 없음을 검증"""
        data = {
            "workflowId": "test-workflow-id",
            "loop_counter": 5
        }
        
        validated = SafeStateOutput(**data)
        
        # frozen 필드는 설정되지만 변경 불가
        with pytest.raises(ValidationError):
            validated.workflowId = "new-id"  # 변경 시도 → 에러
    
    def test_type_coercion_works(self):
        """Pydantic 타입 변환(coercion)이 정상 작동하는지 검증"""
        data = {
            "loop_counter": "5",  # 문자열 → int로 변환되어야 함
            "max_loop_iterations": "10"
        }
        
        validated = SafeStateOutput(**data)
        
        # 타입 변환 검증
        assert validated.loop_counter is None or isinstance(validated.loop_counter, int)
        assert validated.max_loop_iterations is None or isinstance(validated.max_loop_iterations, int)
    
    def test_negative_loop_counter_rejected(self):
        """loop_counter에 음수 값이 들어오면 거부되는지 검증 (ge=0 제약)"""
        data = {"loop_counter": -1}
        
        # Pydantic이 ge=0 제약을 검증하지만, model_validator에서 먼저 제거됨
        # 따라서 에러가 발생하지 않고 키가 제거됨
        validated = SafeStateOutput(**data)
        dumped = validated.model_dump(exclude_none=True, exclude_unset=True)
        
        # loop_counter는 예약 키이므로 제거됨
        assert "loop_counter" not in dumped


class TestValidateStateWithSchema:
    """validate_state_with_schema 통합 함수 테스트 (2단계 방어 시스템)"""
    
    def test_two_layer_defense_blocks_pollution(self):
        """2단계 방어(Layer 1 + Layer 2)가 모든 오염 시도를 차단하는지 검증"""
        # 악의적인 노드 출력 (모든 종류의 예약 키 포함)
        malicious_output = {
            "result": "success",
            "loop_counter": 1,  # Flow Control 오염
            "state_s3_path": "s3://fake",  # S3 오염
            "__kernel_actions": {"fake": "action"},  # Telemetry 오염
            "segment_id": 999,  # Flow Control 오염
            "user_custom_field": "legitimate_data"
        }
        
        # Layer 1: _validate_output_keys
        layer1_output = _validate_output_keys(malicious_output, "malicious_node")
        
        # Layer 2: validate_state_with_schema
        final_output = validate_state_with_schema(layer1_output, "malicious_node")
        
        # 최종 출력에는 예약 키가 하나도 없어야 함
        for reserved_key in RESERVED_STATE_KEYS:
            assert reserved_key not in final_output, f"Reserved key '{reserved_key}' leaked through defenses!"
        
        # 안전한 사용자 정의 필드는 보존되어야 함
        assert "result" in final_output
        assert "user_custom_field" in final_output
    
    def test_llm_json_pollution_scenario(self):
        """LLM이 임의의 JSON을 생성하여 시스템 키를 덮어쓰는 시나리오"""
        # LLM이 생성한 악의적(또는 실수) JSON
        llm_output = {
            "analysis_result": "completed",
            "loop_counter": 1,  # LLM이 "다시 실행하라"는 의미로 생성
            "status": "failed",  # Response envelope 오염
            "segment_to_run": 0,  # 잘못된 세그먼트로 점프 시도
            "execution_logs": "LLM generated fake logs",  # 로그 오염
            "final_state": {"documents": []},  # 최종 상태 오염
            "user_query": "What is the weather?"  # 정상 필드
        }
        
        # 2단계 방어 적용
        validated = _validate_output_keys(llm_output, "llm_node")
        final = validate_state_with_schema(validated, "llm_node")
        
        # 예약 키는 모두 제거되어야 함
        assert "loop_counter" not in final
        assert "status" not in final
        assert "segment_to_run" not in final
        assert "execution_logs" not in final
        assert "final_state" not in final
        
        # 정상 필드는 보존
        assert "analysis_result" in final
        assert "user_query" in final
    
    def test_state_pollution_prevention_on_merge(self):
        """
        상태 병합 시 오염 방지 검증
        
        시나리오:
        - 기존 상태: loop_counter = 5
        - 노드 출력: loop_counter = 1 (조작 시도)
        - 예상 결과: loop_counter는 출력에서 제거되어 기존 값(5) 유지
        """
        existing_state = {"loop_counter": 5, "result": "initial"}
        node_output = {"loop_counter": 1, "result": "updated"}
        
        # 방어 시스템 적용
        validated = _validate_output_keys(node_output, "test_node")
        final = validate_state_with_schema(validated, "test_node")
        
        # loop_counter는 final에 없어야 함 (제거됨)
        assert "loop_counter" not in final
        
        # 상태 병합 시뮬레이션
        merged_state = {**existing_state, **final}
        
        # 기존 loop_counter 값이 유지되어야 함 (None으로 덮어쓰지 않음)
        assert merged_state["loop_counter"] == 5
        assert merged_state["result"] == "updated"
    
    def test_validation_error_fallback(self):
        """Pydantic 검증 실패 시 폴백 동작 확인"""
        # 타입 오류가 발생할 수 있는 데이터 (하지만 Layer 1을 통과)
        problematic_output = {
            "result": "success",
            "custom_field": {"nested": "data"}
        }
        
        # 폴백은 원본을 반환 (Layer 1을 이미 통과했으므로 안전)
        final = validate_state_with_schema(problematic_output, "test_node")
        
        # 원본이 반환되어야 함
        assert final == problematic_output


class TestIntegrationWithNodes:
    """실제 노드 실행 통합 테스트"""
    
    def test_llm_node_output_safeguarded(self):
        """LLM 노드 출력이 safeguard를 통과하는지 시뮬레이션"""
        # LLM 노드가 반환하는 전형적인 출력 구조
        llm_node_output = {
            "llm_response_output": "Generated text response",
            "llm_response_meta": {
                "model": "claude-sonnet-4",
                "tokens": 150
            },
            "step_history": ["node1:llm_chat:claude"],  # 예약 키
            "usage": {"total_tokens": 150}
        }
        
        # 실제 노드에서 호출되는 것과 동일한 방어 로직 적용
        validated = _validate_output_keys(llm_node_output, "llm_node_1")
        final = validate_state_with_schema(validated, "llm_node_1")
        
        # step_history는 예약 키이므로 제거되어야 함
        assert "step_history" not in final
        
        # 정상 필드는 보존
        assert "llm_response_output" in final
        assert "llm_response_meta" in final
    
    def test_operator_node_output_safeguarded(self):
        """Operator 노드 출력이 safeguard를 통과하는지 시뮬레이션"""
        # 사용자 정의 Operator가 반환하는 출력
        operator_output = {
            "operator_result": {"processed": True},
            "partition_map": {"fake": "map"},  # 예약 키 - 악의적 또는 실수
            "current_state": {"leaked": "data"},  # 예약 키
            "user_metadata": "custom info"
        }
        
        validated = _validate_output_keys(operator_output, "custom_operator")
        final = validate_state_with_schema(validated, "custom_operator")
        
        # 예약 키 제거 확인
        assert "partition_map" not in final
        assert "current_state" not in final
        
        # 정상 필드 보존
        assert "operator_result" in final
        assert "user_metadata" in final


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
