"""
V3 ASL 파일 정합성 검증 스크립트
- JSON 문법
- JSONPath 패턴
- ResultSelector/Parameters 필드 검증
- State 전이 검증
- ARN 참조 검증
"""

import json
import re
from pathlib import Path
from typing import Dict, List, Tuple, Any

class ASLValidator:
    def __init__(self):
        self.issues = []
        self.warnings = []
        
    def validate_jsonpath(self, path: str, context: str = '') -> Tuple[bool, str]:
        """JSONPath 및 Intrinsic Function 검증"""
        if not path or not isinstance(path, str):
            return True, ''
        
        # Intrinsic Functions
        intrinsics = [
            'States.Format', 'States.StringToJson', 'States.JsonToString', 
            'States.Array', 'States.ArrayPartition', 'States.ArrayContains',
            'States.ArrayRange', 'States.ArrayGetItem', 'States.ArrayLength',
            'States.ArrayUnique', 'States.Base64Encode', 'States.Base64Decode',
            'States.Hash', 'States.JsonMerge', 'States.MathRandom', 'States.MathAdd',
            'States.StringSplit', 'States.UUID'
        ]
        
        if any(path.startswith(f) for f in intrinsics):
            return True, ''
        
        # JSONPath 패턴
        if path.startswith('$'):
            # Root path (전체 입력 참조)
            if path == '$':
                return True, ''
            # 기본 JSONPath 패턴
            if re.match(r'^\$[\[\].a-zA-Z0-9_\*\-]+$', path):
                return True, ''
            # 배열 처리 패턴 $[*].{...}
            if re.match(r'^\$\[\*\]\.\{[^}]+\}$', path):
                return True, ''
            # Context Object $$
            if path.startswith('$$.'):
                return True, ''
            # 함수 체이닝이 있는 경우
            if 'States.' in path:
                return True, ''
        
        # 리터럴 값 (JSONPath가 아님)
        if not path.endswith('.$') and '$' not in path:
            return True, ''
        
        return False, f'Invalid JSONPath: {path} in {context}'
    
    def check_result_selector(self, state_name: str, state_def: Dict) -> List[str]:
        """ResultSelector 패턴 검증"""
        issues = []
        
        if 'ResultSelector' in state_def:
            rs = state_def['ResultSelector']
            if not isinstance(rs, dict):
                issues.append(f'{state_name}: ResultSelector must be an object')
                return issues
                
            for key, value in rs.items():
                # 필드명에 .$ 가 있으면 안됨
                if key.endswith('.$'):
                    issues.append(f'{state_name}: ResultSelector field "{key}" should not end with .$ (use "{key[:-2]}" instead)')
                
                # 값이 JSONPath인지 확인
                if isinstance(value, str):
                    valid, msg = self.validate_jsonpath(value, f'{state_name}.ResultSelector.{key}')
                    if not valid:
                        issues.append(msg)
        
        return issues
    
    def check_parameters(self, state_name: str, state_def: Dict) -> List[str]:
        """Parameters 필드 검증"""
        issues = []
        
        if 'Parameters' in state_def:
            params = state_def['Parameters']
            if not isinstance(params, dict):
                issues.append(f'{state_name}: Parameters must be an object')
                return issues
                
            for key, value in params.items():
                if key.endswith('.$'):
                    # 동적 참조 - 값이 JSONPath여야 함
                    if isinstance(value, str):
                        valid, msg = self.validate_jsonpath(value, f'{state_name}.Parameters.{key}')
                        if not valid:
                            issues.append(msg)
                elif key.endswith('.'):
                    issues.append(f'{state_name}: Invalid parameter key "{key}" (ends with single dot)')
        
        # ItemSelector도 동일하게 검증 (Distributed Map용)
        if 'ItemSelector' in state_def:
            params = state_def['ItemSelector']
            if not isinstance(params, dict):
                issues.append(f'{state_name}: ItemSelector must be an object')
                return issues
                
            for key, value in params.items():
                if key.endswith('.$'):
                    if isinstance(value, str):
                        valid, msg = self.validate_jsonpath(value, f'{state_name}.ItemSelector.{key}')
                        if not valid:
                            issues.append(msg)
        
        return issues
    
    def check_state_transitions(self, states: Dict, state_name: str, state_def: Dict) -> List[str]:
        """State 전이 검증"""
        issues = []
        
        # Next 검증
        if 'Next' in state_def:
            next_state = state_def['Next']
            if next_state not in states:
                issues.append(f'{state_name}: Next points to non-existent state "{next_state}"')
        
        # Choice 상태의 전이 검증
        if state_def.get('Type') == 'Choice':
            if 'Choices' in state_def:
                for i, choice in enumerate(state_def['Choices']):
                    if 'Next' in choice:
                        next_state = choice['Next']
                        if next_state not in states:
                            issues.append(f'{state_name}: Choice[{i}].Next points to non-existent state "{next_state}"')
            
            if 'Default' in state_def:
                default_state = state_def['Default']
                if default_state not in states:
                    issues.append(f'{state_name}: Default points to non-existent state "{default_state}"')
        
        # Catch의 Next 검증
        if 'Catch' in state_def:
            for i, catcher in enumerate(state_def['Catch']):
                if 'Next' in catcher:
                    next_state = catcher['Next']
                    if next_state not in states:
                        issues.append(f'{state_name}: Catch[{i}].Next points to non-existent state "{next_state}"')
        
        return issues
    
    def check_required_fields(self, state_name: str, state_def: Dict) -> List[str]:
        """필수 필드 검증"""
        issues = []
        
        state_type = state_def.get('Type')
        if not state_type:
            issues.append(f'{state_name}: Missing required field "Type"')
            return issues
        
        # Task 상태는 Resource 필요
        if state_type == 'Task' and 'Resource' not in state_def:
            issues.append(f'{state_name}: Task state must have "Resource" field')
        
        # Map 상태는 Iterator 또는 ItemProcessor 필요
        if state_type == 'Map':
            if 'Iterator' not in state_def and 'ItemProcessor' not in state_def:
                issues.append(f'{state_name}: Map state must have "Iterator" or "ItemProcessor" field')
        
        # Parallel 상태는 Branches 필요
        if state_type == 'Parallel' and 'Branches' not in state_def:
            issues.append(f'{state_name}: Parallel state must have "Branches" field')
        
        # Choice 상태는 Choices 필요
        if state_type == 'Choice' and 'Choices' not in state_def:
            issues.append(f'{state_name}: Choice state must have "Choices" field')
        
        # End가 없으면 Next가 있어야 함 (Choice와 종료 상태 제외)
        if not state_def.get('End') and 'Next' not in state_def and state_type not in ['Choice', 'Succeed', 'Fail']:
            issues.append(f'{state_name}: State must have either "End" or "Next" field')
        
        return issues
    
    def check_arn_references(self, content: str) -> List[str]:
        """ARN 변수 참조 검증"""
        warnings = []
        
        # ${} 패턴 찾기
        arn_refs = re.findall(r'\$\{([^}]+)\}', content)
        
        # 예상되는 ARN 변수들
        expected_arns = [
            'IdempotencyTable',
            'StateDataManagerArn',
            'ExecuteSegmentArn',
            'ReducerArn',
            'GetNextActionArn',
            'StateBagBucket',
            'InitializeStateDataArn',
            'StoreTaskTokenArn',
            'AsyncLLMHandlerArn',
            'WorkflowEventBusArn',
            'PrepareDistributedExecutionArn',
            'ProcessSegmentChunkArn',
            'StoreDistributedTaskTokenArn',
            'ExecuteBranchArn',
            'WorkflowStateBucket',
            'SaveLatestStateArn',
            'AggregateDistributedResultsArn',
            'SegmentRunnerArn',
            'LoadLatestStateArn',
            'ResumeChunkProcessingArn',
            'MergeCallbackArn'
        ]
        
        found_arns = set(arn_refs)
        
        for arn in found_arns:
            if arn not in expected_arns:
                warnings.append(f'Unknown ARN reference: ${{{arn}}} (might be valid but not in expected list)')
        
        return warnings
    
    def validate_state_machine(self, sm_def: Dict, states: Dict, name: str = '') -> None:
        """State Machine 전체 검증"""
        print(f'\n🔍 {name} 검증 중...')
        
        # StartAt 검증
        if 'StartAt' not in sm_def:
            self.issues.append(f'{name}: Missing "StartAt" field')
        elif sm_def['StartAt'] not in states:
            self.issues.append(f'{name}: StartAt points to non-existent state "{sm_def["StartAt"]}"')
        
        # 각 State 검증
        for state_name, state_def in states.items():
            self.issues.extend(self.check_required_fields(state_name, state_def))
            self.issues.extend(self.check_result_selector(state_name, state_def))
            self.issues.extend(self.check_parameters(state_name, state_def))
            self.issues.extend(self.check_state_transitions(states, state_name, state_def))
            
            # Iterator 내부 검증 (Map state)
            if 'Iterator' in state_def and 'States' in state_def['Iterator']:
                iterator = state_def['Iterator']
                for inner_name, inner_def in iterator['States'].items():
                    full_name = f'{state_name}.Iterator.{inner_name}'
                    self.issues.extend(self.check_required_fields(full_name, inner_def))
                    self.issues.extend(self.check_result_selector(full_name, inner_def))
                    self.issues.extend(self.check_parameters(full_name, inner_def))
                    self.issues.extend(self.check_state_transitions(iterator['States'], full_name, inner_def))
            
            # ItemProcessor 내부 검증 (Distributed Map state)
            if 'ItemProcessor' in state_def and 'States' in state_def['ItemProcessor']:
                item_processor = state_def['ItemProcessor']
                for inner_name, inner_def in item_processor['States'].items():
                    full_name = f'{state_name}.ItemProcessor.{inner_name}'
                    self.issues.extend(self.check_required_fields(full_name, inner_def))
                    self.issues.extend(self.check_result_selector(full_name, inner_def))
                    self.issues.extend(self.check_parameters(full_name, inner_def))
                    self.issues.extend(self.check_state_transitions(item_processor['States'], full_name, inner_def))
            
            # Branches 내부 검증 (Parallel state)
            if 'Branches' in state_def:
                for i, branch in enumerate(state_def['Branches']):
                    if 'States' in branch:
                        for branch_state_name, branch_state_def in branch['States'].items():
                            full_name = f'{state_name}.Branches[{i}].{branch_state_name}'
                            self.issues.extend(self.check_required_fields(full_name, branch_state_def))
                            self.issues.extend(self.check_result_selector(full_name, branch_state_def))
                            self.issues.extend(self.check_parameters(full_name, branch_state_def))
    
    def validate_file(self, file_path: Path) -> None:
        """파일 전체 검증"""
        print(f'\n📄 {file_path.name} 검증')
        print('=' * 60)
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                sm_def = json.loads(content)
            
            print('✅ JSON 구문 유효')
            
            # ARN 참조 검증
            self.warnings.extend(self.check_arn_references(content))
            
            # State Machine 검증
            if 'States' in sm_def:
                self.validate_state_machine(sm_def, sm_def['States'], file_path.name)
            else:
                self.issues.append(f'{file_path.name}: Missing "States" field')
                
        except json.JSONDecodeError as e:
            self.issues.append(f'{file_path.name}: JSON syntax error - {str(e)}')
        except Exception as e:
            self.issues.append(f'{file_path.name}: Validation error - {str(e)}')


def main():
    validator = ASLValidator()
    
    src_dir = Path(__file__).parent / 'src'
    v3_files = [
        src_dir / 'aws_step_functions_v3.json',
        src_dir / 'aws_step_functions_distributed_v3.json',
        src_dir / 'aws_step_functions_distributed.json'
    ]
    
    print('\n' + '='*60)
    print('🚀 Analemma OS ASL 정합성 검증 (v3 + 분산맵)')
    print('='*60)
    
    for file_path in v3_files:
        if file_path.exists():
            validator.validate_file(file_path)
        else:
            print(f'\n❌ 파일을 찾을 수 없음: {file_path}')
    
    # 결과 출력
    print('\n\n' + '='*60)
    print('📊 검증 결과')
    print('='*60)
    
    if validator.issues:
        print(f'\n❌ {len(validator.issues)}개의 이슈 발견:')
        for i, issue in enumerate(validator.issues, 1):
            print(f'  {i}. {issue}')
    else:
        print('\n✅ 모든 필수 검증 통과!')
    
    if validator.warnings:
        print(f'\n⚠️  {len(validator.warnings)}개의 경고:')
        for i, warning in enumerate(validator.warnings, 1):
            print(f'  {i}. {warning}')
    
    print('\n' + '='*60)
    
    return len(validator.issues) == 0

if __name__ == '__main__':
    import sys
    success = main()
    sys.exit(0 if success else 1)
