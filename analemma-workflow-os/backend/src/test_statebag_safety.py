from src.common.statebag import StateBag
import json
import boto3
import decimal

def test_state_bag_safety():
    print("Testing StateBag Safety...")
    
    # 1. Serialization Test (Data Contamination Check)
    sb = StateBag({'key': 'value', 'nested': {'a': 1}})
    sb['new_key'] = 'new_param'
    
    try:
        json_str = json.dumps(sb)
        print(f"✅ JSON Serialization OK: {json_str}")
        
        reloaded = json.loads(json_str)
        if reloaded['key'] == 'value' and reloaded['new_key'] == 'new_param':
            print("✅ Data Integrity Verified")
        else:
            print("❌ Data LOST during serialization")
            
    except Exception as e:
        print(f"❌ JSON Serialization FAILED: {e}")

    # 2. Dict Compatibility Test
    if isinstance(sb, dict):
        print("✅ isinstance(sb, dict) is True")
    else:
        print("❌ isinstance(sb, dict) is False")
        
    # 3. None Safety Test
    print(f"None Safety Check: missing_key -> {sb.get('missing', 'SAFE')}")
    sb['explicit_none'] = None
    print(f"Explicit None Check (default='SAFE'): {sb.get('explicit_none', 'SAFE')}")
    print(f"Explicit None Check (default=None): {sb.get('explicit_none')}")

if __name__ == "__main__":
    test_state_bag_safety()
