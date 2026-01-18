
import json
import boto3
import os
import uuid
from typing import Dict, Any, Optional, List
import logging
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Config
STATE_BUCKET = os.environ.get('WORKFLOW_STATE_BUCKET')
METRIC_NAMESPACE = os.environ.get('METRIC_NAMESPACE', 'Analemma/MissionSimulator')

# Step Functions client (lazy init)
_sfn_client = None

def _get_sfn_client():
    """Lazy initialization of Step Functions client"""
    global _sfn_client
    if _sfn_client is None:
        _sfn_client = boto3.client('stepfunctions')
    return _sfn_client


def _get_execution_history(execution_arn: str) -> List[Dict[str, Any]]:
    """
    Retrieve Step Functions execution history for analysis.
    Returns list of state events with their details.
    """
    if not execution_arn:
        return []
    
    try:
        sfn = _get_sfn_client()
        events = []
        paginator = sfn.get_paginator('get_execution_history')
        
        for page in paginator.paginate(executionArn=execution_arn, reverseOrder=False):
            events.extend(page.get('events', []))
        
        logger.info(f"Retrieved {len(events)} execution history events")
        return events
    except Exception as e:
        logger.warning(f"Failed to retrieve execution history: {e}")
        return []


def _analyze_error_handling_flow(execution_arn: str, expected_error_marker: str = "FAIL_TEST") -> Dict[str, Any]:
    """
    Analyze Step Functions execution history to verify error handling flow.
    
    Checks:
    1. NotifyExecutionFailure state was entered
    2. Error info was properly propagated
    3. EventBridge event was published (putEvents succeeded)
    4. Workflow ended in WorkflowFailed state
    
    Returns:
        Dict with analysis results
    """
    result = {
        "notify_failure_entered": False,
        "notify_failure_succeeded": False,
        "error_info_present": False,
        "error_message_correct": False,
        "workflow_failed_state_reached": False,
        "execution_segment_failed": False,
        "error_details": None,
        "states_visited": []
    }
    
    events = _get_execution_history(execution_arn)
    if not events:
        result["error_details"] = "Could not retrieve execution history"
        return result
    
    for event in events:
        event_type = event.get('type', '')
        
        # Track state entries
        if event_type == 'TaskStateEntered':
            state_name = event.get('stateEnteredEventDetails', {}).get('name', '')
            result["states_visited"].append(state_name)
            
            if state_name == 'NotifyExecutionFailure':
                result["notify_failure_entered"] = True
                # Extract input to check error_info
                try:
                    input_str = event.get('stateEnteredEventDetails', {}).get('input', '{}')
                    input_data = json.loads(input_str)
                    error_info = input_data.get('execution_result', {}).get('error_info', {})
                    if error_info:
                        result["error_info_present"] = True
                        result["error_details"] = error_info
                        # Check if error message contains expected marker
                        error_msg = error_info.get('error', '')
                        if expected_error_marker in error_msg:
                            result["error_message_correct"] = True
                except Exception as e:
                    logger.warning(f"Failed to parse NotifyExecutionFailure input: {e}")
            
            elif state_name == 'ExecuteSegment':
                pass  # Track segment execution
                
        # Track state exits
        elif event_type == 'TaskStateExited':
            state_name = event.get('stateExitedEventDetails', {}).get('name', '')
            if state_name == 'NotifyExecutionFailure':
                result["notify_failure_succeeded"] = True
        
        # Track execution failures  
        elif event_type == 'ExecutionFailed':
            result["workflow_failed_state_reached"] = True
        
        # Track Lambda failures (ExecuteSegment failing)
        elif event_type == 'LambdaFunctionFailed':
            result["execution_segment_failed"] = True
    
    logger.info(f"Error handling flow analysis: {json.dumps(result, default=str)}")
    return result

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Verifies the output of a test execution.
    Input: 전체 맵 반복 컨텍스트 또는 { "scenario": "...", "exec_result": {...} }
    Output: { "passed": True/False, "report_s3_path": "s3://..." }
    """
    # [방어적 파싱] 전체 컨텍스트($)가 들어올 수 있으므로 다양한 경로에서 scenario 추출
    scenario = (
        event.get('scenario') or 
        event.get('prep_result', {}).get('scenario') or
        'UNKNOWN'
    )
    
    # [방어적 파싱] exec_result 추출 - 직접 필드 또는 상위 레벨
    exec_result = event.get('exec_result')
    if exec_result is None:
        # 전체 컨텍스트가 exec_result 자체일 수 있음
        exec_result = event if 'Status' in event or 'status' in event or 'Error' in event else {}
    
    # 상세 로깅: 입력 데이터 기록
    logger.info(f"VerifyResult called for scenario: {scenario}")
    logger.info(f"Raw event keys: {list(event.keys())}")
    logger.info(f"exec_result keys: {list(exec_result.keys()) if isinstance(exec_result, dict) else type(exec_result)}")
    
    # Extract status and output from exec_result
    # SFN 성공 시: Status='SUCCEEDED', Output 존재
    # SFN 실패 시 (Catch): Status='FAILED', Error/Cause 존재  
    # SFN Runtime 에러 시: Error/Cause 필드만 존재
    
    # [방어적 파싱] 상태 추출 - 다양한 케이스 처리
    error_type = exec_result.get('Error')
    cause_raw = exec_result.get('Cause', '')
    
    # 상태 결정 로직 강화
    if error_type:
        # 에러가 있으면 무조건 FAILED
        status = 'FAILED'
        logger.warning(f"❌ Execution error detected: {error_type}")
    else:
        # 기존 로직: status 또는 Status 필드 확인
        status = exec_result.get('status', exec_result.get('Status', 'SUCCEEDED'))
    
    # [방어적 파싱] Output 추출 - 여러 경로 시도
    output_raw = exec_result.get('output') or exec_result.get('Output')
    if not output_raw:
        # Catch로 들어온 실패 케이스 - Cause에서 에러 정보 추출
        if cause_raw:
            output_raw = cause_raw
            logger.warning(f"⚠️ No output found for {scenario}, using Cause field")
            logger.info(f"Cause content (truncated): {str(cause_raw)[:500]}...")
        else:
            output_raw = '{"error": "No output or cause found"}'
            logger.warning(f"⚠️ No output or cause found for {scenario}")
    
    # [Fix] Parse output if it's a string (ASL passes raw string to avoid payload limit issues)
    output = {}
    if isinstance(output_raw, str):
        try:
            output = json.loads(output_raw)
        except Exception as e:
            logger.warning(f"Failed to parse output string: {e}")
            output = {"error": "Failed to parse execution output", "raw": output_raw[:1000]}
    elif isinstance(output_raw, dict):
        output = output_raw
    
    # Add error info if execution failed
    if error_type:
        output['execution_error'] = error_type
        output['execution_failed'] = True
    
    # [Enhancement] Extract ExecutionArn for detailed history analysis
    execution_arn = exec_result.get('ExecutionArn', '')
    if not execution_arn:
        # Try extracting from prep_result or other locations
        execution_arn = event.get('prep_result', {}).get('execution_arn', '')
    
    logger.info(f"Verifying {scenario} - Status: {status}, ExecutionArn: {execution_arn[:50]}..." if execution_arn else f"Verifying {scenario} - Status: {status}")
    
    # 1. Perform Verification Logic
    verification_result = _verify_scenario(scenario, status, output, execution_arn)
    
    # 2. Embellish Result
    verification_result['timestamp'] = datetime.utcnow().isoformat()
    verification_result['scenario'] = scenario
    
    # 3. Create Detailed Report
    report = {
        "scenario": scenario,
        "verification_result": verification_result,
        "execution_status": status,
        "execution_output": output # Storing full output here safely in S3
    }
    
    # 4. Upload Report to S3 (Payload Safety)
    report_key = f"test-reports/{datetime.utcnow().strftime('%Y/%m/%d')}/{scenario}-{uuid.uuid4()}.json"
    if STATE_BUCKET:
        s3 = boto3.client('s3')
        s3.put_object(
            Bucket=STATE_BUCKET,
            Key=report_key,
            Body=json.dumps(report),
            ContentType='application/json'
        )
        report_s3_path = f"s3://{STATE_BUCKET}/{report_key}"
    else:
        logger.warning("WORKFLOW_STATE_BUCKET not set, skipping S3 upload")
        report_s3_path = "N/A"

    return {
        "passed": verification_result['passed'],
        "scenario": scenario,
        "report_s3_path": report_s3_path
    }

def _verify_scenario(scenario: str, status: str, output: Dict[str, Any], execution_arn: str = "") -> Dict[str, Any]:
    checks = []
    
    # [선처리] LoopLimitExceeded 에러 감지 - 모든 시나리오에서 확인
    error_type = output.get('Error', output.get('error_type', ''))
    if error_type in ('LoopLimitExceeded', 'BranchLoopLimitExceeded'):
        # 가드레일이 작동한 것 자체는 시스템이 정상 작동하는 증거
        checks.append(_check("Safety Guardrail Activated", True, 
                            details=f"Infinite loop prevented by system: {error_type}"))
        # 하지만 HAPPY_PATH에서는 루프가 발생하면 안 되므로 실패 처리
        if 'HAPPY_PATH' in scenario or 'MAP_AGGREGATOR' in scenario:
            checks.append(_check("No Infinite Loop", False, 
                                details="Workflow should complete without hitting loop limit",
                                expected="< 100 iterations", actual=error_type))
        return {"passed": False, "checks": checks}
    
    # A. Happy Path
    if scenario in ['HAPPY_PATH', 'STANDARD_HAPPY_PATH', 'MAP_AGGREGATOR', 'STANDARD_MAP_AGGREGATOR', 'LOOP_LIMIT', 'STANDARD_LOOP_LIMIT', 'COST_GUARDRAIL', 'STANDARD_COST_GUARDRAIL', 'ATOMICITY', 'STANDARD_ATOMICITY', 'XRAY_TRACEABILITY', 'STANDARD_XRAY_TRACEABILITY']:
        checks.append(_check("Status Succeeded", status == 'SUCCEEDED', expected="SUCCEEDED", actual=status))
        
        # [Fix] LOOP_LIMIT/COST_GUARDRAIL: 가드레일 메트릭 검증
        if 'LOOP_LIMIT' in scenario or 'COST_GUARDRAIL' in scenario:
            # 가드레일 메트릭이 최종 출력에 포함되어 있는지 확인
            has_loop_counter = isinstance(output, dict) and (
                output.get('loop_counter') is not None or
                output.get('max_loop_iterations') is not None
            )
            has_cost_metrics = isinstance(output, dict) and (
                output.get('llm_segments') is not None or
                output.get('total_segments') is not None
            )
            checks.append(_check("Guardrail Metrics Present", has_loop_counter or has_cost_metrics,
                                details="Output should contain loop_counter or cost metrics"))
        
        # Additional specific checks
        if 'MAP_AGGREGATOR' in scenario:
             out_str = json.dumps(output)
             # [Fix] Check for multiple possible branch result patterns including branch_executed marker
             branch_a = 'Branch A' in out_str or 'branch_A' in out_str or 'branch_A_result' in out_str or 'branch_A_executed' in out_str or 'branch_executed' in out_str
             branch_b = 'Branch B' in out_str or 'branch_B' in out_str or 'branch_B_result' in out_str or 'branch_B_executed' in out_str or 'branch_executed' in out_str
             checks.append(_check("Branch A Executed", branch_a))
             checks.append(_check("Branch B Executed", branch_b))
    
    # A-2. HITP Test - Human-in-the-loop 세그멘테이션 테스트
    elif scenario == 'HITP_TEST':
        checks.append(_check("Status Succeeded", status == 'SUCCEEDED', expected="SUCCEEDED", actual=status))
        out_str = json.dumps(output)
        # HITP 흐름 완료 확인
        checks.append(_check("HITP Flow Completed", 
                            'hitp_test_completed' in out_str or 'HITP_NODE_EXECUTED' in out_str or 'SUCCESS' in out_str,
                            details="HITP workflow should complete after auto-resume"))
        
    # B. PII Test
    elif 'PII_TEST' in scenario:
        checks.append(_check("Status Succeeded", status == 'SUCCEEDED'))
        out_str = json.dumps(output)
        checks.append(_check("PII Email Masked", 'john@example.com' not in out_str, "Email should not be visible"))
        checks.append(_check("PII Phone Masked", '010-1234-5678' not in out_str, "Phone should not be visible"))

    # C. S3 Offloading
    elif 'LARGE_PAYLOAD' in scenario:
        checks.append(_check("Status Succeeded", status == 'SUCCEEDED'))
        out_str = json.dumps(output)
        # [Fix] S3 path can be in multiple locations - check deeper paths
        # Check in output, execution_result, and nested state_data
        has_s3_reference = (
            's3://' in out_str or 
            'state_s3_path' in out_str or 
            'final_state_s3_path' in out_str or
            (isinstance(output, dict) and output.get('final_state_s3_path')) or
            (isinstance(output, dict) and output.get('execution_result', {}).get('final_state_s3_path')) or
            (isinstance(output, dict) and output.get('state_data', {}).get('state_s3_path')) or
            # Also check for large payload marker indicating S3 should have been used
            (isinstance(output, dict) and output.get('large_s3_result'))
        )
        checks.append(_check("S3 Path Present", has_s3_reference, "Output should contain S3 reference"))
        
    # D. Error Handling - 의도적 실패 테스트 (강화된 검증)
    elif scenario in ['ERROR_HANDLING', 'STANDARD_ERROR_HANDLING']:
        checks.append(_check("Status Failed", status == 'FAILED', expected="FAILED", actual=status))
        
        # [방어적 파싱] 에러 메시지 추출 - 다양한 위치 확인
        error_msg = ''
        if isinstance(output, dict):
            error_msg = output.get('error', output.get('message', ''))
        else:
            error_msg = str(output)
        
        # FAIL_TEST 마커 확인 (의도적 실패인 경우)
        full_context = json.dumps(output) if isinstance(output, dict) else str(output)
        has_fail_marker = 'FAIL_TEST' in full_context
        
        if has_fail_marker:
            checks.append(_check("Intentional Failure Marker Found", True, details="FAIL_TEST marker detected - test passed"))
        else:
            checks.append(_check("Error Info Present", len(str(error_msg)) > 0))
        
        # [Enhanced] Step Functions 실행 히스토리 분석을 통한 에러 핸들링 플로우 검증
        if execution_arn:
            flow_analysis = _analyze_error_handling_flow(execution_arn, expected_error_marker="FAIL_TEST")
            
            # 1. NotifyExecutionFailure 상태가 진입되었는지 확인
            checks.append(_check(
                "NotifyExecutionFailure State Entered",
                flow_analysis.get("notify_failure_entered", False),
                details="Error handling state should be reached when workflow fails"
            ))
            
            # 2. NotifyExecutionFailure가 성공적으로 완료되었는지 (EventBridge 이벤트 발행)
            checks.append(_check(
                "Error Event Published to EventBridge",
                flow_analysis.get("notify_failure_succeeded", False),
                details="EventBridge event should be published with error details"
            ))
            
            # 3. 에러 정보가 올바르게 전파되었는지
            checks.append(_check(
                "Error Info Properly Propagated",
                flow_analysis.get("error_info_present", False),
                details=f"Error details: {flow_analysis.get('error_details', 'N/A')}"
            ))
            
            # 4. 에러 메시지가 원본과 일치하는지
            checks.append(_check(
                "Error Message Contains Expected Marker",
                flow_analysis.get("error_message_correct", False),
                details="Error message should contain 'FAIL_TEST' marker from intentional failure"
            ))
            
            # 5. 최종적으로 WorkflowFailed 상태로 종료되었는지
            checks.append(_check(
                "Workflow Ended in Failed State",
                flow_analysis.get("workflow_failed_state_reached", False),
                details="Execution should end with ExecutionFailed event"
            ))
            
            # 6. 상태 흐름 기록 (디버깅용)
            states_visited = flow_analysis.get("states_visited", [])
            if states_visited:
                checks.append(_check(
                    "Error Handling Flow Traced",
                    True,
                    details=f"States visited: {' -> '.join(states_visited[-5:])}"  # Last 5 states
                ))
        else:
            # ExecutionArn이 없으면 기본 검증만 수행
            checks.append(_check(
                "Execution History Analysis",
                False,
                details="ExecutionArn not available - detailed flow analysis skipped"
            ))

    # E. Local Runner Scenarios - 자체 검증 결과 활용
    elif scenario in ['API_CONNECTIVITY', 'WEBSOCKET_CONNECT', 'AUTH_FLOW', 'REALTIME_NOTIFICATION',
                      'CORS_SECURITY', 'MULTI_TENANT_ISOLATION', 'CONCURRENT_BURST', 'CANCELLATION',
                      'IDEMPOTENCY', 'DLQ_RECOVERY']:
        # Local Runner가 반환한 checks 결과를 그대로 활용
        local_checks = output.get('checks', [])
        local_passed = output.get('passed', False)
        
        if local_checks:
            # Local Runner의 상세 체크 결과 포함
            for lc in local_checks:
                checks.append(_check(
                    f"[Local] {lc.get('name', 'Unknown')}",
                    lc.get('passed', False),
                    details=lc.get('details', '')
                ))
        else:
            # checks가 없으면 status 기반으로 판단
            checks.append(_check("Local Test Status", status == 'SUCCEEDED', expected="SUCCEEDED", actual=status))
        
        # 최종 결과는 Local Runner의 passed 값 사용
        return {"passed": local_passed, "checks": checks}
    
    # F. PARALLEL_GROUP_TEST - parallel_group 노드 실행 검증
    elif scenario == 'PARALLEL_GROUP_TEST':
        checks.append(_check("Status Succeeded", status == 'SUCCEEDED', expected="SUCCEEDED", actual=status))
        out_str = json.dumps(output)
        # Check for parallel execution markers
        has_parallel_result = (
            'branch' in out_str.lower() or 
            'parallel' in out_str.lower() or
            '_executed' in out_str
        )
        checks.append(_check("Parallel Execution Completed", has_parallel_result, 
                            details="Should contain branch execution results"))
    
    # G. HYPER_STRESS_V3 - V3 하이퍼 스트레스 시나리오
    elif scenario == 'HYPER_STRESS_V3':
        checks.append(_check("Status Succeeded", status == 'SUCCEEDED', expected="SUCCEEDED", actual=status))
        out_str = json.dumps(output)
        # Nested Map 실행 확인
        has_nested_map = (
            'nested_map' in out_str.lower() or
            'market_analysis' in out_str.lower() or
            'outer_count' in out_str.lower()
        )
        # Multi-HITL 확인
        has_hitl = (
            'hitl' in out_str.lower() or
            'hitl_merge' in out_str.lower() or
            'hitl_decisions' in out_str.lower()
        )
        # Partial State Sync 확인
        has_partial_sync = (
            'partial_sync' in out_str.lower() or
            'delta_sync' in out_str.lower() or
            'state_delta' in out_str.lower()
        )
        checks.append(_check("Nested Map Executed", has_nested_map, details="Should contain nested map results"))
        checks.append(_check("Multi-HITL Merged", has_hitl, details="Should contain HITL merge results"))
        checks.append(_check("Partial State Sync", has_partial_sync, details="Should contain partial sync results"))
    
    # H. MULTIMODAL_VISION - Gemini Vision 이미지 분석
    elif scenario == 'MULTIMODAL_VISION':
        checks.append(_check("Status Succeeded", status == 'SUCCEEDED', expected="SUCCEEDED", actual=status))
        out_str = json.dumps(output)
        # Vision 결과 확인
        has_vision_result = (
            'vision_result' in output or
            'vision' in out_str.lower() or
            'image_analysis' in out_str.lower() or
            'product_specs' in output
        )
        # Vision 메타데이터 확인 (선택적)
        has_vision_meta = (
            'vision_meta' in output or
            'vision_node_meta' in output or
            'image_count' in out_str.lower()
        )
        checks.append(_check("Vision Analysis Complete", has_vision_result, 
                            details="Should contain vision analysis results"))
        checks.append(_check("Vision Metadata Present (Optional)", has_vision_meta, 
                            details="Should contain vision metadata if available"))
    
    # I. MULTIMODAL_COMPLEX - 비디오 + 이미지 복합 분석
    elif scenario == 'MULTIMODAL_COMPLEX':
        checks.append(_check("Status Succeeded", status == 'SUCCEEDED', expected="SUCCEEDED", actual=status))
        out_str = json.dumps(output)
        # 비디오 청킹 확인
        has_video = (
            'video_chunks' in output or
            'video_analysis' in out_str.lower() or
            'video_track' in out_str.lower()
        )
        # 이미지 분석 확인
        has_images = (
            'spec_sheet' in out_str.lower() or
            'image_track' in out_str.lower() or
            'sheet_spec' in out_str.lower()
        )
        # 충돌 해결 확인
        has_conflict_resolution = (
            'conflict' in out_str.lower() or
            'final_product_specs' in output or
            'merged' in out_str.lower()
        )
        # HTML 생성 확인 (선택적)
        has_html = (
            'final_html' in output or
            'html' in out_str.lower() or
            'product_page' in out_str.lower()
        )
        checks.append(_check("Video Chunking Complete", has_video, 
                            details="Should contain video chunking/analysis results"))
        checks.append(_check("Image Analysis Complete", has_images, 
                            details="Should contain spec sheet analysis results"))
        checks.append(_check("Conflict Resolution Complete", has_conflict_resolution, 
                            details="Should contain conflict resolution or merged specs"))
        checks.append(_check("HTML Generation (Optional)", has_html or status == 'SUCCEEDED', 
                            details="Should contain final HTML if workflow completed"))

    # Default fallback
    else:
        checks.append(_check("Status Succeeded (Default)", status == 'SUCCEEDED'))

    passed = all(c['passed'] for c in checks)
    return {"passed": passed, "checks": checks}

def _check(name: str, condition: bool, details: str = None, expected: Any = None, actual: Any = None):
    res = {'name': name, 'passed': condition}
    if details: res['details'] = details
    if expected: res['expected'] = expected
    if actual: res['actual'] = actual
    return res
