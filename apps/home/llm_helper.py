import re

# Heuristic token estimator for SQL/JSON prompts
def estimate_tokens(text: str) -> int:
    return int(len(text) / 3.2)

def normalize_model_name(model: str | None) -> str:
    return (model or "").strip().lower()

def detect_model_family(model_llm: str | None) -> str:
    """
    Return a normalized "family key" for common Ollama model names.
    Examples: llama3, llama2, mistral, mistral3, mixtral, qwen2, qwen3, phi3, gemma2, deepseek, etc.
    """
    m = normalize_model_name(model_llm)

    # Explicit matches first
    if "llama3" in m:
        return "llama3"
    if "llama2" in m:
        return "llama2"
    if "mistral3" in m or "mistral-small3" in m or "mistral-small-3" in m:
        return "mistral3"
    if "mixtral" in m:
        return "mixtral"
    if "mistral" in m:
        return "mistral"

    if "qwen3" in m:
        return "qwen3"
    if "qwen2" in m:
        return "qwen2"
    if "qwen" in m:
        return "qwen"

    if "phi3" in m or "phi-3" in m:
        return "phi3"
    if "phi4" in m or "phi-4" in m:
        return "phi4"
    if "phi" in m:
        return "phi"

    if "gemma2" in m:
        return "gemma2"
    if "gemma" in m:
        return "gemma"

    if "deepseek" in m:
        return "deepseek"

    if "yi" in m:
        return "yi"

    if "command-r" in m or "commandr" in m or "cohere" in m:
        return "command-r"

    if "gpt-oss-20b" in m or "gtp-oss-20b" in m or "gpt-oss:20b" in m:
        return "gpt-oss-20b"

    return "unknown"

# Conservative defaults by family (most-common context windows)
# Notes:
# - Many variants exist (some higher). We pick "safe" values that usually work.
# - If you know your deployment uses long-context variants, bump specific entries.
MODEL_CTX_LIMITS = {
    # Meta Llama
    "llama3": 8192,
    "llama2": 4096,

    # Mistral
    "mistral3": 32768,   # Mistral Small 3 often 32k
    "mixtral": 32768,    # Many Mixtral builds are 32k
    "mistral": 8192,     # Base mistral builds often 8k

    # Qwen
    "qwen3": 32768,
    "qwen2": 32768,
    "qwen": 8192,

    # Microsoft Phi
    "phi4": 16384,       # conservative
    "phi3": 128000,      # Phi-3 family is often long-context (128k advertised), but heavy
    "phi": 8192,

    # Google Gemma
    "gemma2": 8192,
    "gemma": 8192,

    # DeepSeek / Yi / Command-R (varies a lot)
    "deepseek": 32768,
    "yi": 16384,
    "command-r": 128000,  # Command R often long-context (advertised), but very deployment-dependent

    # Your custom / OSS
    "gpt-oss-20b": 32768,

    "unknown": 8192,
}

def choose_ctx_and_output_budget(model_llm: str | None, prompt_tokens: int) -> tuple[int, int]:
    """
    Return (ctx_limit, out_tokens_budget).
    - ctx_limit: chosen by family table
    - out_tokens_budget: derived from remaining headroom, clamped
    """
    family = detect_model_family(model_llm)
    ctx_limit = MODEL_CTX_LIMITS.get(family, MODEL_CTX_LIMITS["unknown"])

    # Safety margin: keep room for formatting/system overhead and avoid edge truncation
    margin = 512 if ctx_limit <= 8192 else 1024

    remaining = max(0, ctx_limit - prompt_tokens - margin)

    # Practical output caps
    hard_cap = 2500 if ctx_limit <= 8192 else 4000

    # Use up to ~60% of remaining, but keep minimum viable answer size
    out_budget = min(hard_cap, int(remaining * 0.6))
    out_budget = max(600, out_budget)

    return ctx_limit, out_budget

def clamp_num_ctx(ctx_limit: int, prompt_tokens: int, out_budget: int) -> int:
    """
    Choose num_ctx to cover (prompt + output + margin), but never exceed ctx_limit.
    """
    margin = 256 if ctx_limit <= 8192 else 512
    needed = prompt_tokens + out_budget + margin

    # Clamp to ctx_limit; also enforce a minimum (avoid weird tiny contexts)
    return max(2048, min(ctx_limit, needed))