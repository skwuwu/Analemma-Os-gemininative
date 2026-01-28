#!/usr/bin/env python3
"""
Test script to verify Gemini 3 Preview model availability

Usage:
    python test_gemini3_model.py

Environment variables required:
    - GCP_PROJECT_ID: Your Google Cloud project ID
    - GCP_LOCATION: Vertex AI location (default: global)
    - GCP_SERVICE_ACCOUNT_KEY: Service account JSON key (optional if using ADC)
"""

import os
import sys

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

def test_gemini3_pro():
    """Test Gemini 3 Pro Preview availability"""
    print("=" * 80)
    print("Testing Gemini 3 Pro Preview Model")
    print("=" * 80)
    
    try:
        from services.llm.gemini_service import GeminiService, GeminiConfig, GeminiModel
        
        # Initialize service with Gemini 3 Pro
        print("\n1️⃣ Initializing GeminiService with gemini-3-pro-preview...")
        config = GeminiConfig(
            model=GeminiModel.GEMINI_3_PRO,
            max_output_tokens=100,
            temperature=0.7
        )
        service = GeminiService(config)
        print(f"   ✅ Service initialized with model: {config.model.value}")
        
        # Test simple generation
        print("\n2️⃣ Testing simple text generation...")
        prompt = "What is the capital of France? Answer in one word."
        response = service.invoke_model(
            user_prompt=prompt,
            system_instruction="You are a helpful assistant."
        )
        
        text = service.extract_text(response)
        print(f"   ✅ Response received: {text[:100]}...")
        
        print("\n" + "=" * 80)
        print("✅ SUCCESS: Gemini 3 Pro Preview is working!")
        print("=" * 80)
        return True
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        print("\nTroubleshooting steps:")
        print("1. Check GCP_PROJECT_ID environment variable")
        print("2. Verify GCP_LOCATION is set to 'global' for Gemini 3 models")
        print("3. Ensure google-cloud-aiplatform SDK is updated: pip install --upgrade google-cloud-aiplatform")
        print("4. Check service account credentials (GCP_SERVICE_ACCOUNT_KEY or ADC)")
        print("5. Verify Vertex AI API is enabled in Google Cloud Console")
        print("6. Check Model Garden for Gemini 3 access")
        return False


def test_gemini3_flash():
    """Test Gemini 3 Flash Preview availability"""
    print("\n" + "=" * 80)
    print("Testing Gemini 3 Flash Preview Model")
    print("=" * 80)
    
    try:
        from services.llm.gemini_service import GeminiService, GeminiConfig, GeminiModel
        
        print("\n1️⃣ Initializing GeminiService with gemini-3-flash-preview...")
        config = GeminiConfig(
            model=GeminiModel.GEMINI_3_FLASH,
            max_output_tokens=50,
            temperature=0.5
        )
        service = GeminiService(config)
        print(f"   ✅ Service initialized with model: {config.model.value}")
        
        print("\n2️⃣ Testing simple generation...")
        prompt = "Count from 1 to 5."
        response = service.invoke_model(
            user_prompt=prompt,
            system_instruction="You are a helpful assistant."
        )
        
        text = service.extract_text(response)
        print(f"   ✅ Response received: {text[:100]}...")
        
        print("\n" + "=" * 80)
        print("✅ SUCCESS: Gemini 3 Flash Preview is working!")
        print("=" * 80)
        return True
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        return False


def test_model_enum():
    """Test that model enum values are correct"""
    print("\n" + "=" * 80)
    print("Checking Model Enum Values")
    print("=" * 80)
    
    try:
        from services.llm.gemini_service import GeminiModel
        
        print(f"\n✅ GEMINI_3_PRO = {GeminiModel.GEMINI_3_PRO.value}")
        print(f"✅ GEMINI_3_FLASH = {GeminiModel.GEMINI_3_FLASH.value}")
        
        # Verify correct IDs
        assert GeminiModel.GEMINI_3_PRO.value == "gemini-3-pro-preview", "GEMINI_3_PRO should be gemini-3-pro-preview"
        assert GeminiModel.GEMINI_3_FLASH.value == "gemini-3-flash-preview", "GEMINI_3_FLASH should be gemini-3-flash-preview"
        
        print("\n✅ Model enum values are correct!")
        return True
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        return False


def main():
    """Run all tests"""
    print("\n")
    print("╔" + "=" * 78 + "╗")
    print("║" + " " * 20 + "Gemini 3 Preview Model Test" + " " * 31 + "║")
    print("╚" + "=" * 78 + "╝")
    
    # Check environment
    project_id = os.getenv("GCP_PROJECT_ID")
    location = os.getenv("GCP_LOCATION", "global")
    
    print(f"\n📋 Configuration:")
    print(f"   GCP_PROJECT_ID: {project_id or '❌ NOT SET'}")
    print(f"   GCP_LOCATION: {location}")
    print(f"   Service Account: {'✅ Configured' if os.getenv('GCP_SERVICE_ACCOUNT_KEY') else '⚠️ Using ADC'}")
    
    if not project_id:
        print("\n❌ ERROR: GCP_PROJECT_ID environment variable is not set")
        print("\nSet it with:")
        print('   export GCP_PROJECT_ID="your-project-id"')
        print('   # or')
        print('   set GCP_PROJECT_ID=your-project-id  # Windows')
        return 1
    
    # Run tests
    results = []
    results.append(("Model Enum Check", test_model_enum()))
    results.append(("Gemini 3 Pro", test_gemini3_pro()))
    results.append(("Gemini 3 Flash", test_gemini3_flash()))
    
    # Summary
    print("\n\n")
    print("╔" + "=" * 78 + "╗")
    print("║" + " " * 32 + "Test Summary" + " " * 34 + "║")
    print("╠" + "=" * 78 + "╣")
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"║  {test_name:<50} {status:>25} ║")
    
    print("╠" + "=" * 78 + "╣")
    print(f"║  Total: {passed}/{total} tests passed{' ' * (62 - len(f'Total: {passed}/{total} tests passed'))}║")
    print("╚" + "=" * 78 + "╝")
    
    if passed == total:
        print("\n🎉 All tests passed! Gemini 3 Preview models are ready to use.")
        return 0
    else:
        print(f"\n⚠️ {total - passed} test(s) failed. Check the error messages above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
