"""
백엔드 common 모듈의 임포트 무결성 검사
"""
import os
import sys

# 백엔드 경로 추가
backend_path = os.path.join(os.path.dirname(__file__), "analemma-workflow-os", "backend")
sys.path.insert(0, backend_path)

# common.__init__.py에서 LAZY_IMPORT_MAP 읽기
from src.common import _LAZY_IMPORT_MAP

missing_imports = []
successful_imports = []

print("=" * 80)
print("🔍 Common 모듈 임포트 무결성 검사")
print("=" * 80)

for attr_name, (module_path, actual_attr) in _LAZY_IMPORT_MAP.items():
    try:
        # 모듈 임포트 시도
        module = __import__(module_path, fromlist=[actual_attr])
        
        # 속성 존재 확인
        if hasattr(module, actual_attr):
            successful_imports.append((attr_name, module_path, actual_attr))
            print(f"✅ {attr_name:35s} <- {module_path}.{actual_attr}")
        else:
            missing_imports.append({
                'attr_name': attr_name,
                'module_path': module_path,
                'actual_attr': actual_attr,
                'error': f"Attribute '{actual_attr}' not found in module '{module_path}'"
            })
            print(f"❌ {attr_name:35s} <- {module_path}.{actual_attr} [속성 없음]")
    
    except ImportError as e:
        missing_imports.append({
            'attr_name': attr_name,
            'module_path': module_path,
            'actual_attr': actual_attr,
            'error': str(e)
        })
        print(f"❌ {attr_name:35s} <- {module_path}.{actual_attr} [모듈 없음: {e}]")

print("\n" + "=" * 80)
print(f"📊 검사 결과: {len(successful_imports)} 성공, {len(missing_imports)} 실패")
print("=" * 80)

if missing_imports:
    print("\n⚠️  누락된 임포트:")
    for item in missing_imports:
        print(f"\n  - {item['attr_name']}")
        print(f"    모듈: {item['module_path']}")
        print(f"    속성: {item['actual_attr']}")
        print(f"    오류: {item['error']}")
    sys.exit(1)
else:
    print("\n✅ 모든 임포트가 정상입니다!")
    sys.exit(0)
