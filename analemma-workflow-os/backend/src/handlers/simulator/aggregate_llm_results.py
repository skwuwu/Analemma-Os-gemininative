"""
LLM Test Results Aggregation Lambda
여러 LLM 테스트 결과를 집계하고 CloudWatch 메트릭을 발행합니다.
"""

import json
import logging
import boto3
import os
from typing import Dict, Any, List
from datetime import datetime, timezone

logger = logging.getLogger()
logger.setLevel(logging.INFO)

METRIC_NAMESPACE = os.environ.get('METRIC_NAMESPACE', 'Analemma/LLMSimulator')


def _extract_payload(result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Lambda invoke 응답에서 실제 Payload를 추출합니다.
    
    Map 상태의 결과는 Lambda invoke 응답 형식일 수 있음:
    { "ExecutedVersion": "$LATEST", "Payload": {...}, "StatusCode": 200 }
    """
    if not isinstance(result, dict):
        return {}
    
    # Lambda invoke 응답 형식인 경우 Payload 추출
    if 'Payload' in result and 'StatusCode' in result:
        payload = result.get('Payload', {})
        if isinstance(payload, str):
            try:
                return json.loads(payload)
            except (json.JSONDecodeError, ValueError):
                return {}
        return payload if isinstance(payload, dict) else {}
    
    # 이미 직접 결과 형식인 경우
    return result


def _publish_metrics(results: List[Dict[str, Any]], passed_count: int, total: int):
    """CloudWatch 메트릭을 발행합니다."""
    try:
        cw = boto3.client('cloudwatch')
        
        metrics = [
            {
                'MetricName': 'LLMTestsPassed',
                'Value': passed_count,
                'Unit': 'Count',
                'Timestamp': datetime.now(timezone.utc)
            },
            {
                'MetricName': 'LLMTestsFailed',
                'Value': total - passed_count,
                'Unit': 'Count',
                'Timestamp': datetime.now(timezone.utc)
            },
            {
                'MetricName': 'LLMTestsTotal',
                'Value': total,
                'Unit': 'Count',
                'Timestamp': datetime.now(timezone.utc)
            },
            {
                'MetricName': 'LLMTestPassRate',
                'Value': (passed_count / total * 100) if total > 0 else 0,
                'Unit': 'Percent',
                'Timestamp': datetime.now(timezone.utc)
            }
        ]
        
        # 시나리오별 메트릭
        for result in results:
            scenario = result.get('scenario', 'unknown')
            verification = result.get('verification_result', {}).get('verification', {})
            status = verification.get('status', 'UNKNOWN')
            
            metrics.append({
                'MetricName': f'LLMTest_{scenario}',
                'Value': 1 if status == 'PASSED' else 0,
                'Unit': 'Count',
                'Timestamp': datetime.now(timezone.utc),
                'Dimensions': [
                    {'Name': 'Scenario', 'Value': scenario},
                    {'Name': 'Status', 'Value': status}
                ]
            })
        
        cw.put_metric_data(
            Namespace=METRIC_NAMESPACE,
            MetricData=metrics
        )
        
        logger.info(f"✅ Published {len(metrics)} CloudWatch metrics")
        
    except Exception as e:
        logger.warning(f"Failed to publish CloudWatch metrics: {e}")


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    LLM 테스트 결과를 집계하고 종합 리포트를 생성합니다.
    
    Input: {
        "test_results": [...],
        "simulator_execution_id": "...",
        "start_time": "..."
    }
    """
    raw_results = event.get('test_results', [])
    sim_exec_id = event.get('simulator_execution_id', 'unknown')
    start_time = event.get('start_time', '')
    
    # Lambda invoke 응답에서 실제 Payload 추출
    results = [_extract_payload(r) for r in raw_results]
    
    total = len(results)
    passed_count = 0
    failed_count = 0
    skipped_count = 0
    
    logger.info(f"🧠 Aggregating {total} LLM test results for {sim_exec_id}")
    
    scenarios = {}
    failed_scenarios = []
    
    for result in results:
        scenario = result.get('scenario', 'unknown')
        verification = result.get('verification_result', {}).get('verification', {})
        
        # [Fix] verify_llm_test.py는 'verified' boolean을 반환하므로 'status' 문자열로 변환
        # 기존: status = verification.get('status', 'UNKNOWN')
        if 'status' in verification:
            status = verification.get('status')
        elif 'verified' in verification:
            status = 'PASSED' if verification.get('verified') else 'FAILED'
        else:
            status = 'UNKNOWN'
        
        if status == 'PASSED':
            passed_count += 1
        elif status == 'FAILED':
            failed_count += 1
            failed_scenarios.append(scenario)
        else:
            skipped_count += 1
        
        # Extract provider information from test result
        # Structure: result -> test_result -> output (final_state) -> usage -> provider
        test_result = result.get('test_result', {})
        
        # Handle both direct output and nested structure
        if isinstance(test_result, dict):
            # Check for 'output' key (Step Functions format)
            final_state = test_result.get('output', test_result)
            
            # Handle case where output is a JSON string
            if isinstance(final_state, str):
                try:
                    final_state = json.loads(final_state)
                except:
                    final_state = {}
            
            # Extract usage from final_state or nested final_state
            usage = final_state.get('usage') or final_state.get('final_state', {}).get('usage', {})
            provider = usage.get('provider', 'unknown')
        else:
            usage = {}
            provider = 'unknown'
        
        # Log provider info for debugging
        if provider != 'unknown':
            logger.info(f"Scenario {scenario}: provider={provider}")
        else:
            logger.warning(f"Scenario {scenario}: provider not found in result")
        
        scenarios[scenario] = {
            'status': status,
            'message': verification.get('message', ''),
            'test_result': test_result,
            'verification': verification,
            'provider': provider,
            'usage': usage
        }
    
    overall_status = 'SUCCESS' if failed_count == 0 else 'FAILURE'
    
    # CloudWatch 메트릭 발행
    _publish_metrics(results, passed_count, total)
    
    # 종합 리포트
    report = {
        'simulator_execution_id': sim_exec_id,
        'start_time': start_time,
        'end_time': datetime.now(timezone.utc).isoformat(),
        'overall_status': overall_status,
        'summary': {
            'total': total,
            'passed': passed_count,
            'failed': failed_count,
            'skipped': skipped_count,
            'pass_rate': round((passed_count / total * 100) if total > 0 else 0, 2)
        },
        'scenarios': scenarios,
        'failed_scenarios': failed_scenarios,
        'mock_mode': 'false'
    }
    
    if failed_count > 0:
        logger.error(f"❌ LLM Simulator FAILED: {failed_count}/{total} scenarios failed")
        logger.error(f"Failed scenarios: {failed_scenarios}")
    else:
        logger.info(f"✅ LLM Simulator SUCCESS: All {total} scenarios passed")
    
    logger.info(f"Pass rate: {report['summary']['pass_rate']}%")
    
    return report
