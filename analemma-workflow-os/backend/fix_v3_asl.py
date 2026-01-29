"""
V3 ASL 파일들의 ResultSelector 필드명 자동 수정 스크립트
- "field_name.$": "$.path" -> "field_name": "$.path"
"""

import json
import re
from pathlib import Path

def fix_result_selector_fields(obj):
    """ResultSelector 내의 필드명에서 .$ 제거"""
    if isinstance(obj, dict):
        # ResultSelector 필드 발견
        if 'ResultSelector' in obj:
            rs = obj['ResultSelector']
            if isinstance(rs, dict):
                # 필드명에서 .$ 제거
                fixed_rs = {}
                for key, value in rs.items():
                    if key.endswith('.$'):
                        new_key = key[:-2]  # .$ 제거
                        print(f'  ✓ "{key}" -> "{new_key}"')
                        fixed_rs[new_key] = value
                    else:
                        fixed_rs[key] = value
                obj['ResultSelector'] = fixed_rs
        
        # 재귀적으로 모든 하위 객체 처리
        for key, value in obj.items():
            if isinstance(value, (dict, list)):
                fix_result_selector_fields(value)
    
    elif isinstance(obj, list):
        for item in obj:
            fix_result_selector_fields(item)

def fix_payload_parameters(obj):
    """Parameters.Payload.$ -> Parameters.Payload (REMOVED - this was incorrect!)
    
    NOTE: Payload.$ is actually CORRECT for Lambda invoke in Step Functions.
    The .$ suffix tells Step Functions to evaluate the value as JSONPath.
    This function is kept for reference but does nothing.
    """
    # This function intentionally does nothing - Payload.$ is correct syntax!
    pass

def process_file(file_path: Path):
    """파일 처리"""
    print(f'\n📄 {file_path.name} 처리 중...')
    print('='*60)
    
    with open(file_path, 'r', encoding='utf-8') as f:
        sm_def = json.load(f)
    
    print('\n🔧 ResultSelector 필드 수정:')
    fix_result_selector_fields(sm_def)
    
    # Payload.$ 수정은 제거됨 (원래 문법이 맞았음)
    # print('\n🔧 Parameters.Payload.$ 수정:')
    # fix_payload_parameters(sm_def)
    
    # 파일 저장
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(sm_def, f, indent=2, ensure_ascii=False)
    
    print(f'\n✅ {file_path.name} 저장 완료')

def main():
    src_dir = Path(__file__).parent / 'src'
    v3_files = [
        src_dir / 'aws_step_functions_v3.json',
        src_dir / 'aws_step_functions_distributed_v3.json',
        src_dir / 'aws_step_functions_distributed.json'
    ]
    
    print('\n' + '='*60)
    print('🛠️  ASL 파일 자동 수정 (v3 + 분산맵)')
    print('='*60)
    
    for file_path in v3_files:
        if file_path.exists():
            process_file(file_path)
        else:
            print(f'\n❌ 파일을 찾을 수 없음: {file_path}')
    
    print('\n' + '='*60)
    print('✨ 모든 수정 완료!')
    print('='*60 + '\n')

if __name__ == '__main__':
    main()
