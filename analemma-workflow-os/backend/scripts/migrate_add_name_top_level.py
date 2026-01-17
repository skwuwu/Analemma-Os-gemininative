#!/usr/bin/env python3
"""
Safe one-time migration: ensure every item in the Workflows table has a top-level
'name' attribute derived from the stored `config` JSON when possible.

Usage:
  export AWS_PROFILE=... (optional)
  export WORKFLOWS_TABLE=Workflows
  python3 scripts/migrate_add_name_top_level.py

This script will:
  - scan the table in pages
  - for each item, if 'name' is missing and config contains a name, call UpdateItem
    with a conditional expression to avoid stomping concurrently-updated items.

Run this from a machine with AWS credentials that have permission to read/scan the
Workflows table and call UpdateItem.
"""

import os
import json
import boto3
from botocore.exceptions import ClientError

# 🚨 [Critical Fix] Match default value with template.yaml
WORKFLOWS_TABLE = os.environ.get('WORKFLOWS_TABLE', 'WorkflowsTableV3')


def extract_name_from_config(cfg_value):
    if cfg_value is None:
        return None
    try:
        if isinstance(cfg_value, str):
            parsed = json.loads(cfg_value)
        elif isinstance(cfg_value, dict):
            parsed = cfg_value
        else:
            return None
        if isinstance(parsed, dict) and isinstance(parsed.get('name'), str):
            return parsed.get('name')
    except Exception:
        return None
    return None


def migrate():
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(WORKFLOWS_TABLE)

    paginator = table.meta.client.get_paginator('scan')
    page_iter = paginator.paginate(TableName=WORKFLOWS_TABLE, ProjectionExpression='workflowId, config, #nm', ExpressionAttributeNames={'#nm': 'name'})

    updated = 0
    scanned = 0
    for page in page_iter:
        items = page.get('Items', [])
        scanned += len(items)
        for it in items:
            # skip if name already present
            if 'name' in it and it.get('name'):
                continue
            cfg = it.get('config')
            name = extract_name_from_config(cfg)
            if name:
                # safest update: only set name if the attribute doesn't already exist
                try:
                    table.update_item(
                        Key={'workflowId': it['workflowId']},
                        UpdateExpression='SET #n = :v',
                        ConditionExpression='attribute_not_exists(#n) OR #n = :empty',
                        ExpressionAttributeNames={'#n': 'name'},
                        ExpressionAttributeValues={':v': name, ':empty': ''}
                    )
                    updated += 1
                    print(f"Updated workflowId={it['workflowId']} with name={name}")
                except ClientError as e:
                    # ignore conditional failures or other errors but log them
                    print(f"Failed updating {it.get('workflowId')}: {e}")
                    continue
    print(f"Scanned {scanned} items, updated {updated} items")


if __name__ == '__main__':
    migrate()
