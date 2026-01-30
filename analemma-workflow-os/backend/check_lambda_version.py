#!/usr/bin/env python3
import boto3

lambda_client = boto3.client('lambda', region_name='ap-northeast-2')
response = lambda_client.list_functions(MaxItems=50)

seg_funcs = [f for f in response['Functions'] if 'SegmentRunner' in f['FunctionName']]

for func in seg_funcs:
    print(f"{func['FunctionName']}:")
    print(f"  Last Modified: {func['LastModified']}")
    print(f"  Runtime: {func['Runtime']}")
    print(f"  Code Size: {func['CodeSize'] / 1024 / 1024:.1f} MB")
    print()
