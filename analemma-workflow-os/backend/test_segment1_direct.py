#!/usr/bin/env python3
import boto3
import json

# Get segment manifest from S3
s3 = boto3.client('s3', region_name='ap-northeast-2')
bucket = 'backend-workflow-dev-workflowstatebucketresource-duyk1qamnxhf'
manifest_key = 'workflow-manifests/system/llm-test-stage1_basic/segment_manifest.json'

print('Downloading segment manifest...')
response = s3.get_object(Bucket=bucket, Key=manifest_key)
manifest = json.loads(response['Body'].read())

print(f'Manifest has {len(manifest)} segments\n')

# Check Segment 1
seg1 = manifest[1]
print(f'=== Segment 1 Structure ===')
print(f'Top-level keys: {list(seg1.keys())}')

seg1_config = seg1.get('segment_config')
if seg1_config:
    print(f'\nsegment_config keys: {list(seg1_config.keys())}')
    print(f'nodes: {len(seg1_config.get("nodes", []))} nodes')
    
    for node in seg1_config.get('nodes', []):
        print(f'  - {node.get("id")}: {node.get("type")}')
        if node.get('type') == 'llm_chat':
            output_key = node.get('config', {}).get('output_key')
            print(f'    output_key: {output_key}')
else:
    print('segment_config NOT FOUND!')

# Now test Lambda invocation with this segment
print('\n=== Testing Lambda Invocation ===')

lambda_client = boto3.client('lambda', region_name='ap-northeast-2')

# Find SegmentRunner function
functions = lambda_client.list_functions()
seg_funcs = [f for f in functions['Functions'] if 'SegmentRunner' in f['FunctionName']]

if not seg_funcs:
    print('SegmentRunnerFunction not found!')
    exit(1)

func_name = seg_funcs[0]['FunctionName']
print(f'Using function: {func_name}')

# Prepare test event
test_event = {
    'segment_id': 1,
    'segment_to_run': 1,
    'workflowId': 'llm-test-stage1_basic',
    'ownerId': 'system',
    'partition_map': manifest,
    'workflow_config': {
        'workflow_name': 'test_llm_stage1_basic',
        'nodes': [],
        'edges': []
    },
    'current_state': {
        'input_text': 'Analemma OS is a revolutionary AI-first workflow automation platform.'
    },
    'MOCK_MODE': 'false',
    'loop_counter': 0,
    'total_segments': len(manifest)
}

print('\nInvoking Lambda with Segment 1 config...')
print(f'segment_id: 1')
print(f'partition_map length: {len(manifest)}')

try:
    response = lambda_client.invoke(
        FunctionName=func_name,
        InvocationType='RequestResponse',
        Payload=json.dumps(test_event)
    )
    
    payload = json.loads(response['Payload'].read())
    
    if 'errorMessage' in payload:
        print(f'\n✗ ERROR: {payload["errorMessage"]}')
        if 'stackTrace' in payload:
            print('\nStack trace:')
            for line in payload['stackTrace'][:10]:
                print(f'  {line}')
    else:
        print(f'\n✓ Success!')
        print(f'Response keys: {list(payload.keys())}')
        
        state_data = payload.get('state_data', {})
        print(f'state_data keys: {list(state_data.keys())[:15]}')
        
        current_state = state_data.get('current_state', {})
        print(f'\ncurrent_state keys: {list(current_state.keys())}')
        
        if 'llm_raw_output' in current_state:
            print(f'✓✓ llm_raw_output FOUND!')
            print(f'   Value: {str(current_state["llm_raw_output"])[:200]}')
        else:
            print(f'✗✗ llm_raw_output NOT FOUND')
        
        exec_result = state_data.get('execution_result', {})
        if exec_result:
            print(f'\nexecution_result keys: {list(exec_result.keys())}')
        else:
            print(f'\nexecution_result: EMPTY')

except Exception as e:
    print(f'\n✗ Invocation failed: {e}')
