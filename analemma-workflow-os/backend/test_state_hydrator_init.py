"""
StateHydrator 초기화 파라미터 검증 테스트
"""

from src.common.state_hydrator import StateHydrator

print("="*80)
print("StateHydrator 초기화 파라미터 검증 테스트")
print("="*80)

# Test 1: 파라미터 없이 초기화
print("\nTest 1: StateHydrator()")
try:
    hydrator1 = StateHydrator()
    print("  ✅ Success - hydrator initialized without parameters")
except Exception as e:
    print(f"  ❌ Failed: {e}")

# Test 2: bucket_name으로 초기화
print("\nTest 2: StateHydrator(bucket_name='test-bucket')")
try:
    hydrator2 = StateHydrator(bucket_name='test-bucket')
    print("  ✅ Success - hydrator initialized with bucket_name")
except Exception as e:
    print(f"  ❌ Failed: {e}")

# Test 3: 잘못된 파라미터로 초기화 시도 (에러 발생 예상)
print("\nTest 3: StateHydrator(s3_bucket='test-bucket') - Should fail")
try:
    hydrator3 = StateHydrator(s3_bucket='test-bucket')
    print("  ❌ Should have failed but succeeded - WRONG!")
except TypeError as e:
    print(f"  ✅ Expected TypeError caught: {e}")

# Test 4: SegmentRunnerService 초기화
print("\nTest 4: SegmentRunnerService initialization")
try:
    from src.services.execution.segment_runner_service import SegmentRunnerService
    service = SegmentRunnerService(s3_bucket='test-bucket')
    print("  ✅ Success - SegmentRunnerService initialized")
    print(f"     - State bucket: {service.state_bucket}")
    print(f"     - Hydrator: {type(service.hydrator).__name__}")
except Exception as e:
    print(f"  ❌ Failed: {e}")

print("\n" + "="*80)
print("All tests completed!")
print("="*80)
