# 🖼️ Multimodal Integration (v3.0)

## Overview

**llm_chat_runner now supports multimodal processing (image/video + text)** with explicit input arrays, combining the best features from both llm_chat_runner and vision_runner.

---

## 🎯 Key Features

### 1. **Two Input Modes**

#### **Mode 1: Explicit Inputs (NEW - Recommended)**
```json
{
  "type": "llm_chat",
  "config": {
    "prompt_content": "이 이미지들을 분석해주세요",
    "image_inputs": [
      "s3://my-bucket/product-image.jpg",
      "{{user_uploaded_image}}",
      "https://example.com/photo.png"
    ],
    "video_inputs": [
      "s3://my-bucket/demo-video.mp4"
    ],
    "model": "gemini-2.0-flash",
    "max_tokens": 2000
  }
}
```

#### **Mode 2: Legacy - S3 URIs in Prompt**
```json
{
  "type": "llm_chat",
  "config": {
    "prompt_content": "이 이미지 분석해줘 s3://bucket/image.jpg",
    "vision_enabled": true,
    "model": "gemini-1.5-flash"
  }
}
```

---

## 📊 Comparison: Before vs After

| Feature | Before (vision_runner) | After (llm_chat_runner v3.0) |
|---------|----------------------|----------------------------|
| **Multimodal Support** | ✅ Explicit arrays | ✅ Explicit arrays + Legacy |
| **Retry Logic** | ❌ None | ✅ Full exponential backoff |
| **Provider Fallback** | ❌ Gemini only | ✅ Gemini → Bedrock (text) |
| **Quality Check** | ❌ None | ✅ Kernel Middleware |
| **Cancellation Check** | ❌ None | ✅ execution_arn |
| **Lambda Timeout** | ❌ None | ✅ Early exit |
| **Response Schema** | ❌ Free text | ✅ Structured JSON |
| **Auto-prompt** | ⚠️ Basic | ✅ Workflow chain |
| **Media Count** | ✅ Tracked | ✅ Tracked |

**Result**: llm_chat_runner is now a **production-ready unified LLM runner** supporting both text and multimodal inputs.

---

## 🔧 Implementation Details

### **1. Explicit Input Processing**

```python
# llm_chat_runner (lines 1319-1368)
# Process image_inputs
raw_image_inputs = actual_config.get("image_inputs", [])
if isinstance(raw_image_inputs, str):
    raw_image_inputs = [raw_image_inputs]

for img_input in raw_image_inputs:
    resolved = _render_template(img_input, exec_state)
    # Check if state key reference
    if resolved and not resolved.startswith(("s3://", "gs://", "http://", "https://", "data:")):
        state_val = exec_state.get(resolved)
        if state_val:
            resolved = state_val
    
    if resolved:
        explicit_media_inputs.append({"type": "image", "source": resolved})
```

### **2. Multimodal Content Preparation**

```python
# prepare_multimodal_content (lines 1127-1277)
# Priority 1: Check for explicit media inputs
explicit_inputs = state.get("_explicit_media_inputs", [])
if explicit_inputs:
    logger.info(f"🖼️ [Multimodal] Processing {len(explicit_inputs)} explicit media inputs")
    
    for media_item in explicit_inputs:
        source = media_item.get("source")
        media_type = media_item.get("type", "image")
        mime_type = get_mime_type(str(source))
        
        # Handle S3 URI + hydration
        if isinstance(source, str) and source.startswith("s3://"):
            s3_key = f"hydrated_{source.replace('s3://', '').replace('/', '_')}"
            
            if s3_key in state:
                # Binary data already hydrated
                multimodal_parts.append({
                    "source": binary_data,
                    "mime_type": mime_type,
                    "source_uri": source,
                    "hydrated": True
                })
```

### **3. Media Count Tracking**

```python
# llm_chat_runner (lines 1588-1596)
meta = {
    "model": model, 
    "max_tokens": max_tokens, 
    "attempt": attempt + 1, 
    "provider": "gemini",
    "multimodal": False,  # Will be updated if multimodal_parts detected
    "image_count": 0,
    "video_count": 0
}

# llm_chat_runner (lines 1641-1650)
if multimodal_parts:
    meta["multimodal"] = True
    for part in multimodal_parts:
        mime_type = part.get("mime_type", "")
        if mime_type.startswith("image/"):
            meta["image_count"] += 1
        elif mime_type.startswith("video/"):
            meta["video_count"] += 1
```

---

## 📝 Usage Examples

### **Example 1: Product Image Analysis**
```json
{
  "nodes": [
    {
      "id": "analyze_product",
      "type": "llm_chat",
      "config": {
        "prompt_content": "Extract product specifications from these images as JSON",
        "image_inputs": [
          "{{product_front_image}}",
          "{{product_back_image}}",
          "{{spec_sheet_image}}"
        ],
        "model": "gemini-2.0-flash",
        "response_schema": {
          "type": "object",
          "properties": {
            "name": {"type": "string"},
            "dimensions": {"type": "string"},
            "weight": {"type": "string"},
            "materials": {"type": "array", "items": {"type": "string"}}
          }
        },
        "output_key": "product_specs"
      }
    }
  ]
}
```

### **Example 2: Video + Text Analysis**
```json
{
  "nodes": [
    {
      "id": "analyze_tutorial",
      "type": "llm_chat",
      "config": {
        "prompt_content": "Create step-by-step instructions from this tutorial video",
        "video_inputs": ["s3://tutorials/cooking-pasta.mp4"],
        "model": "gemini-1.5-pro",
        "max_tokens": 4000,
        "temperature": 0.3,
        "output_key": "tutorial_steps"
      }
    }
  ]
}
```

### **Example 3: Mixed Media with State References**
```json
{
  "nodes": [
    {
      "id": "upload_images",
      "type": "api_call",
      "config": {
        "url": "https://api.example.com/upload",
        "method": "POST",
        "output_key": "uploaded_image_s3_uri"
      }
    },
    {
      "id": "analyze_mixed",
      "type": "llm_chat",
      "config": {
        "prompt_content": "Compare these product images with the reference video",
        "image_inputs": [
          "{{uploaded_image_s3_uri}}",
          "s3://reference/product-standard.jpg"
        ],
        "video_inputs": ["{{reference_video_uri}}"],
        "model": "gemini-2.0-flash",
        "output_key": "comparison_result"
      }
    }
  ]
}
```

### **Example 4: Legacy Mode (Backward Compatible)**
```json
{
  "nodes": [
    {
      "id": "analyze_legacy",
      "type": "llm_chat",
      "config": {
        "prompt_content": "이 이미지 분석해줘 s3://bucket/image.jpg 그리고 이 영상도 s3://bucket/video.mp4",
        "vision_enabled": true,
        "model": "gemini-1.5-flash"
      }
    }
  ]
}
```

---

## 🛡️ Advanced Features

### **1. Retry on Gemini Rate Limits**
```json
{
  "type": "llm_chat",
  "config": {
    "image_inputs": ["s3://large-image.jpg"],
    "retry_config": {
      "max_retries": 3,
      "base_delay": 2.0
    }
  }
}
```

### **2. Quality Check Integration**
```json
{
  "type": "llm_chat",
  "config": {
    "image_inputs": ["s3://product.jpg"],
    "quality_domain": "technical_report",
    "slop_threshold": 0.5
  }
}
```

### **3. Structured Output (JSON Mode)**
```json
{
  "type": "llm_chat",
  "config": {
    "image_inputs": ["s3://invoice.jpg"],
    "response_schema": {
      "type": "object",
      "properties": {
        "invoice_number": {"type": "string"},
        "total_amount": {"type": "number"},
        "items": {"type": "array"}
      }
    }
  }
}
```

---

## 🚨 Important Notes

### **1. Bedrock Fallback Limitation**
- **Multimodal requests CANNOT fall back to Bedrock** (Bedrock doesn't support vision)
- If Gemini Vision fails with non-retryable error, the node will raise an exception
- Text-only requests can still fall back to Bedrock Claude

```python
# main.py (lines 1760-1771)
if multimodal_parts:
    logger.error(
        f"🚫 [Vision Fallback Blocked] Node: {node_id}, "
        f"Multimodal requests cannot fall back to Bedrock."
    )
    raise RuntimeError(
        f"Gemini Vision failed and Bedrock fallback not supported for multimodal requests."
    )
```

### **2. Supported File Types**

**Images**: jpg, jpeg, png, webp, gif, heic, heif, bmp  
**Videos**: mp4, mov, avi, webm, mkv  
**Documents**: pdf (Gemini 1.5+)

### **3. Input Source Types**

1. **S3 URI**: `s3://bucket/file.jpg` (auto-downloaded)
2. **HTTP URL**: `https://example.com/image.jpg` (auto-downloaded)
3. **State key**: `{{previous_image_output}}` (resolved from state)
4. **Data URI**: `data:image/png;base64,iVBORw0KG...` (inline base64)

---

## 🎯 Migration Guide

### **From vision_runner to llm_chat_runner**

**Before (vision_runner)**:
```json
{
  "type": "vision",
  "config": {
    "image_inputs": ["s3://image.jpg"],
    "prompt_content": "Analyze this image"
  }
}
```

**After (llm_chat_runner v3.0)**:
```json
{
  "type": "llm_chat",
  "config": {
    "image_inputs": ["s3://image.jpg"],
    "prompt_content": "Analyze this image"
  }
}
```

**Benefits**:
- ✅ Retry logic (3 attempts with backoff)
- ✅ Bedrock fallback for text responses
- ✅ Quality kernel integration
- ✅ Cancellation support
- ✅ Lambda timeout handling
- ✅ Structured output (response_schema)

---

## 📊 Metadata Output

```json
{
  "llm_output": "분석 결과...",
  "llm_meta": {
    "model": "gemini-2.0-flash",
    "provider": "gemini",
    "multimodal": true,
    "image_count": 3,
    "video_count": 1,
    "attempt": 1,
    "max_tokens": 2000
  },
  "usage": {
    "input_tokens": 1250,
    "output_tokens": 450,
    "total_tokens": 1700
  }
}
```

---

## 🔍 Debugging

### **Enable Detailed Logging**
```python
# Check multimodal detection
logger.info(f"🖼️ [Multimodal] {len(explicit_media_inputs)} explicit media inputs detected")

# Check media type breakdown
logger.info(f"Invoking Gemini Vision API with {len(multimodal_parts)} multimodal parts "
           f"({meta['image_count']} images, {meta['video_count']} videos)")

# Check hydration status
logger.info(f"  ✓ Hydrated image: {source} ({mime_type})")
logger.info(f"  ✓ S3 URI image: {source} ({mime_type})")
```

---

## ✅ Summary

**llm_chat_runner v3.0 is now the unified LLM runner** supporting:
- ✅ Text-only processing
- ✅ Multimodal processing (image/video + text)
- ✅ Explicit input arrays (vision_runner style)
- ✅ Legacy S3 URI extraction from prompt
- ✅ Production-ready error handling (retry, fallback, cancellation)
- ✅ Quality kernel integration
- ✅ Structured output (JSON mode)
- ✅ Media count tracking

**vision_runner is still available** for specialized use cases, but **llm_chat_runner is now recommended** for all LLM tasks (text + multimodal).
