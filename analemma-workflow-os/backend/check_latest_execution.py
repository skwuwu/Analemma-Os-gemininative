#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import boto3
import json

# Get latest LLM Simulator execution
sfn = boto3.client('stepfunctions', region_name='ap-northeast-2')
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
print(f'Latest execution: {latest_exec["name"]}')
print(f'Start: {latest_exec["startDate"]}')

# Get output
exec_detail = sfn.describe_execution(executionArn=exec_arn)
output = json.loads(exec_detail.get('output', '{}'))

# Check first test result
test_results = output.get('test_results', [])
if not test_results:
    print('No test results found')
    exit(1)

stage1 = test_results[0]
test_result = stage1.get('test_result', {})
final_state = test_result.get('output', {}).get('final_state', {})
current_state = final_state.get('current_state', {})

print(f'\nStage 1 current_state keys: {list(current_state.keys())}')
if 'llm_raw_output' in current_state:
    llm_output = current_state['llm_raw_output']
    print(f'llm_raw_output FOUND: {str(llm_output)[:300]}')
else:
    print('llm_raw_output: NOT FOUND')

# Check verification result
verif = stage1.get('verification_result', {})
print(f'\nVerification status: {verif.get("status")}')
print(f'Message: {verif.get("message", "N/A")[:300]}')

# Get the distributed orchestrator execution for Stage 1
stage1_exec_id = stage1.get('prep_result', {}).get('idempotency_key', '')
print(f'\nLooking for WorkflowDistributedOrchestrator execution: {stage1_exec_id}')

if stage1_exec_id:
    # List recent executions
    orch_execs = sfn.list_executions(
        stateMachineArn='arn:aws:states:ap-northeast-2:482421580372:stateMachine:WorkflowDistributedOrchestrator-dev',
        maxResults=50
    )
    
    # Find matching execution
    matching = [e for e in orch_execs['executions'] if stage1_exec_id in e['name']]
    if matching:
        orch_exec = matching[0]
        print(f'Found orchestrator execution: {orch_exec["name"]} - {orch_exec["status"]}')
        
        # Get execution history to check Segment 1 output
        history = sfn.get_execution_history(
            executionArn=orch_exec['executionArn'],
            maxResults=100
        )
        
        # Find Segment 1 execution (second ExecuteSegment)
        exec_segment_count = 0
        for event in history['events']:
            event_type = event.get('type', '')
            if 'TaskSucceeded' in event_type or 'LambdaFunctionSucceeded' in event_type:
                # Check if this is ExecuteSegment
                details = event.get('taskSucceededEventDetails', event.get('lambdaFunctionSucceededEventDetails', {}))
                output_str = details.get('output', '')
                if output_str and 'segment_to_run' in output_str:
                    exec_segment_count += 1
                    if exec_segment_count == 2:  # Segment 1 (second execution)
                        output_data = json.loads(output_str)
                        payload = output_data.get('Payload', {})
                        state_data = payload.get('state_data', {})
                        seg_current_state = state_data.get('current_state', {})
                        
                        print(f'\nSegment 1 ExecuteSegment output:')
                        print(f'  current_state keys: {list(seg_current_state.keys())}')
                        if 'llm_raw_output' in seg_current_state:
                            print(f'  llm_raw_output FOUND!')
                        else:
                            print(f'  llm_raw_output: NOT FOUND')
                        
                        # Check execution_result
                        exec_result = state_data.get('execution_result', {})
                        print(f'  execution_result keys: {list(exec_result.keys())[:10]}')
                        
                        if exec_result:
                            exec_final_state = exec_result.get('final_state', {})
                            print(f'  execution_result.final_state keys: {list(exec_final_state.keys())[:15]}')
                            if 'llm_raw_output' in exec_final_state:
                                print(f'  llm_raw_output in final_state: FOUND!')
                            else:
                                print(f'  llm_raw_output in final_state: NOT FOUND')
                        
                        break
