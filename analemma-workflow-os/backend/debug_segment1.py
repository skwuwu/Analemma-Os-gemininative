#!/usr/bin/env python3
import boto3
import json

sfn = boto3.client('stepfunctions', region_name='ap-northeast-2')

# Get latest LLM Simulator execution
executions = sfn.list_executions(
    stateMachineArn='arn:aws:states:ap-northeast-2:482421580372:stateMachine:LLMSimulator-dev',
    statusFilter='SUCCEEDED',
    maxResults=1
)

if not executions['executions']:
    print('No executions found')
    exit(1)

latest_exec = executions['executions'][0]
exec_arn = latest_exec['executionArn']
print(f'Latest LLM Simulator: {latest_exec["name"]}')
print(f'Started: {latest_exec["startDate"]}')

# Get output to find Stage 1 orchestrator execution
exec_detail = sfn.describe_execution(executionArn=exec_arn)
output = json.loads(exec_detail.get('output', '{}'))

test_results = output.get('test_results', [])
if not test_results:
    print('No test results')
    exit(1)

stage1 = test_results[0]
prep_input = stage1.get('prep_result', {}).get('input', {})
stage1_exec_id = prep_input.get('idempotency_key', '')

if not stage1_exec_id:
    print('No Stage 1 execution ID found')
    exit(1)

print(f'\nStage 1 Orchestrator ID: {stage1_exec_id}')

# Find the orchestrator execution
orch_execs = sfn.list_executions(
    stateMachineArn='arn:aws:states:ap-northeast-2:482421580372:stateMachine:WorkflowDistributedOrchestrator-dev',
    maxResults=100
)

# Try partial match
matching = [e for e in orch_execs['executions'] if 'STAGE1' in e['name'] and 'llm-1b15f8b43e1e' in e['name']]
if not matching:
    print('Orchestrator execution not found')
    exit(1)

orch_exec = matching[0]
print(f'Found: {orch_exec["name"]} - {orch_exec["status"]}')

# Get detailed execution history
history = sfn.get_execution_history(
    executionArn=orch_exec['executionArn'],
    maxResults=200
)

print(f'\n=== Analyzing {len(history["events"])} events ===\n')

# Find all ExecuteSegment events
segment_executions = []
for event in history['events']:
    event_type = event.get('type', '')
    
    if 'TaskSucceeded' in event_type or 'LambdaFunctionSucceeded' in event_type:
        details = event.get('taskSucceededEventDetails', event.get('lambdaFunctionSucceededEventDetails', {}))
        output_str = details.get('output', '')
        
        if output_str and 'segment_to_run' in output_str:
            try:
                output_data = json.loads(output_str)
                payload = output_data.get('Payload', {})
                state_data = payload.get('state_data', {})
                seg_to_run = state_data.get('segment_to_run', -1)
                
                segment_executions.append({
                    'event_id': event['id'],
                    'segment_completed': seg_to_run - 1 if seg_to_run > 0 else 0,
                    'next_segment': seg_to_run,
                    'state_data': state_data
                })
            except:
                pass

print(f'Found {len(segment_executions)} segment execution results\n')

# Analyze Segment 1 (second execution result, completed segment 1)
for i, seg_result in enumerate(segment_executions):
    completed = seg_result['segment_completed']
    
    if completed == 1:  # Segment 1 completed
        print(f'=== Segment 1 Execution Result (Event {seg_result["event_id"]}) ===')
        
        state_data = seg_result['state_data']
        
        # Check current_state
        current_state = state_data.get('current_state', {})
        print(f'\ncurrent_state keys: {list(current_state.keys())}')
        
        if 'llm_raw_output' in current_state:
            llm_out = current_state['llm_raw_output']
            print(f'✓ llm_raw_output FOUND in current_state!')
            print(f'  Type: {type(llm_out)}')
            print(f'  Preview: {str(llm_out)[:200]}')
        else:
            print(f'✗ llm_raw_output NOT FOUND in current_state')
        
        # Check execution_result
        exec_result = state_data.get('execution_result', {})
        if exec_result:
            print(f'\nexecution_result keys: {list(exec_result.keys())}')
            
            final_state = exec_result.get('final_state', {})
            print(f'execution_result.final_state keys: {list(final_state.keys())[:15]}')
            
            if 'llm_raw_output' in final_state:
                print(f'✓ llm_raw_output FOUND in execution_result.final_state!')
            else:
                print(f'✗ llm_raw_output NOT FOUND in execution_result.final_state')
        else:
            print(f'\n✗ execution_result is EMPTY')
        
        # Check for S3 offload
        final_state_s3 = state_data.get('final_state_s3_path')
        if final_state_s3:
            print(f'\nS3 offload path: {final_state_s3}')
        
        # Deep search for llm_raw_output anywhere in state_data
        def find_key_recursive(obj, target_key, path=''):
            results = []
            if isinstance(obj, dict):
                for k, v in obj.items():
                    current_path = f'{path}.{k}' if path else k
                    if target_key in k.lower():
                        results.append((current_path, type(v).__name__))
                    results.extend(find_key_recursive(v, target_key, current_path))
            elif isinstance(obj, list):
                for i, item in enumerate(obj[:3]):  # Check first 3 items
                    results.extend(find_key_recursive(item, target_key, f'{path}[{i}]'))
            return results
        
        llm_keys = find_key_recursive(state_data, 'llm')
        if llm_keys:
            print(f'\nAll keys containing "llm":')
            for path, val_type in llm_keys[:10]:
                print(f'  {path} ({val_type})')
        
        break

print('\n=== Final State Check ===')
final_state = test_results[0].get('test_result', {}).get('output', {}).get('final_state', {})
final_current = final_state.get('current_state', {})
print(f'Final workflow current_state keys: {list(final_current.keys())}')
if 'llm_raw_output' in final_current:
    print('✓ llm_raw_output PRESENT in final result')
else:
    print('✗ llm_raw_output MISSING in final result')
