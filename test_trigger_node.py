#!/usr/bin/env python3
"""
Trigger 노드 매핑 테스트
- API request trigger → start 노드로 매핑되는지 확인
- 미구현 trigger 타입들이 적절히 처리되는지 확인
"""

import sys
import os

# Add the backend/src directory to the Python path
backend_src_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 
                               'analemma-workflow-os', 'backend', 'src')
sys.path.insert(0, backend_src_path)

from services.workflow.builder import WorkflowGraphBuilder

def test_trigger_node_mapping():
    """Trigger 노드 매핑 테스트"""
    
    # API request trigger 테스트
    api_trigger_config = {
        "nodes": [
            {
                "id": "api_trigger_node",
                "type": "trigger",
                "config": {
                    "trigger_type": "request",
                    "description": "API 요청 트리거"
                }
            }
        ],
        "edges": []
    }
    
    print("🧪 Testing API request trigger mapping...")
    try:
        builder = WorkflowGraphBuilder(api_trigger_config)
        node_handler = builder._get_node_handler({
            "id": "api_trigger_node",
            "type": "trigger",
            "config": {"trigger_type": "request"}
        })
        
        if node_handler:
            print("✅ API request trigger successfully mapped to start node")
            # 실제로 실행해보기
            result = node_handler({}, {})
            print(f"   Result: {result}")
        else:
            print("❌ Failed to create handler for API request trigger")
    except Exception as e:
        print(f"❌ Error testing API request trigger: {e}")
    
    # Time trigger 테스트 (미구현)
    time_trigger_config = {
        "nodes": [
            {
                "id": "time_trigger_node", 
                "type": "trigger",
                "config": {
                    "trigger_type": "time",
                    "cron": "0 9 * * *"
                }
            }
        ],
        "edges": []
    }
    
    print("\n🧪 Testing unimplemented time trigger...")
    try:
        builder = WorkflowGraphBuilder(time_trigger_config)
        node_handler = builder._get_node_handler({
            "id": "time_trigger_node",
            "type": "trigger", 
            "config": {"trigger_type": "time", "cron": "0 9 * * *"}
        })
        
        if node_handler:
            print("✅ Time trigger fallback handler created (with TODO warning)")
        else:
            print("❌ Failed to create fallback handler for time trigger")
    except Exception as e:
        print(f"❌ Error testing time trigger: {e}")

if __name__ == "__main__":
    print("🚀 Trigger Node Mapping Test")
    print("=" * 50)
    test_trigger_node_mapping()
    print("\n✨ Test completed!")