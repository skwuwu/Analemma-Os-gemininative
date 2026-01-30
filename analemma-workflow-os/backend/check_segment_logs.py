#!/usr/bin/env python3
import boto3
from datetime import datetime, timedelta
import time

logs = boto3.client('logs', region_name='ap-northeast-2')

# SegmentRunnerFunction log group
log_group = '/aws/lambda/backend-workflow-dev-SegmentRunnerFunction-sQ2y38Eugfdm'

# Get logs from last 60 minutes
start_time = int((datetime.now() - timedelta(minutes=60)).timestamp() * 1000)

# Search for critical logs
query = '''
fields @timestamp, @message
| filter @message like /Segment/ or @message like /segment/ or @message like /config/
| sort @timestamp desc
| limit 100
'''

print('Starting CloudWatch Logs query...')
response = logs.start_query(
    logGroupName=log_group,
    startTime=start_time,
    endTime=int(datetime.now().timestamp() * 1000),
    queryString=query
)

query_id = response['queryId']
print(f'Query ID: {query_id}')

for i in range(15):
    time.sleep(2)
    result = logs.get_query_results(queryId=query_id)
    status = result['status']
    
    if status == 'Complete':
        print(f'\nFound {len(result["results"])} log entries')
        
        for entry in result['results'][:20]:
            timestamp = next((f['value'] for f in entry if f['field'] == '@timestamp'), '')
            msg = next((f['value'] for f in entry if f['field'] == '@message'), '')
            
            # Extract time from timestamp
            if timestamp:
                dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                time_str = dt.strftime('%H:%M:%S')
            else:
                time_str = 'N/A'
            
            print(f'\n[{time_str}] {msg[:500]}')
        
        break
    
    if status in ['Failed', 'Cancelled']:
        print(f'Query failed: {status}')
        break
    
    if i % 3 == 0:
        print(f'  Waiting... ({status})')

if status == 'Running':
    print('Query timeout')
