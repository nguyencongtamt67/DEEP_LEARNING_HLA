"""
LLM-based extractor for HLA-disease associations using OpenAI GPT-4o.
Handles single-paper extraction with structured JSON output.
"""

import json
import logging
import re
from datetime import datetime
from typing import Optional, Dict, Tuple

from openai import OpenAI
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# optional local HF support
try:
    from transformers import AutoTokenizer
    HF_AVAILABLE = True
except ImportError:
    HF_AVAILABLE = False

import config
from extraction.prompt_templates import SYSTEM_PROMPT, EXTRACTION_PROMPT, TABLE_EXTRACTION_PROMPT
from extraction.validator import validate_extraction, ExtractionResult

logger = logging.getLogger(__name__)

# Initialize OpenAI client
client = OpenAI(api_key=config.OPENAI_API_KEY)

# Pricing per 1M tokens (GPT-4o)
PRICING = {
    "gpt-4o": {"input": 2.50, "output": 10.00},
    "gpt-4o-mini": {"input": 0.15, "output": 0.60},
    "gpt-4-turbo": {"input": 10.00, "output": 30.00},
}


def estimate_cost(tokens_input: int, tokens_output: int, model: str = None) -> float:
    """Estimate the cost of an API call in USD."""
    if model is None:
        model = config.OPENAI_MODEL
    prices = PRICING.get(model, PRICING["gpt-4o"])
    cost = (tokens_input / 1_000_000) * prices["input"] + (tokens_output / 1_000_000) * prices["output"]
    return round(cost, 6)


def truncate_text(text: str, max_chars: int = 100_000) -> str:
    """
    Truncate text to fit within token limits.
    Rough estimate: 1 token ~ 4 characters for English text.
    max_chars=100_000 ~ 25,000 tokens, well within GPT-4o's 128K context.
    """
    if len(text) <= max_chars:
        return text

    # Keep beginning and end (most important parts of a paper)
    half = max_chars // 2
    truncated = text[:half] + "\n\n[... TEXT TRUNCATED ...]\n\n" + text[-half:]
    logger.warning(f"Text truncated from {len(text)} to {len(truncated)} characters")
    return truncated


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=60),
    retry=retry_if_exception_type((Exception,)),
    before_sleep=lambda retry_state: logger.warning(
        f"Retry attempt {retry_state.attempt_number} after error: {retry_state.outcome.exception()}"
    ),
)
def _extract_from_hf_model(text: str, model: str) -> Tuple[Dict, int, int]:
    """Extract associations with local Hugging Face model fallback."""
    if not HF_AVAILABLE:
        raise RuntimeError("transformers is not installed, cannot use Hugging Face model")

    # For now, use tokenizer to measure tokens and heuristics for extraction.
    try:
        tokenizer = AutoTokenizer.from_pretrained(model)
        tokens = tokenizer(text, truncation=True, max_length=4096)
        tokens_in = len(tokens["input_ids"])
    except Exception as e:
        logger.warning(f"HF tokenizer load failed for {model}: {e}")
        tokens_in = len(text.split())

    # Basic heuristic detection
    disease = None
    for code, info in config.DISEASES.items():
        for term in info["query_terms"]:
            if term.lower() in text.lower():
                disease = info["name"]
                break
        if disease:
            break

    if disease is None:
        disease = "Unknown"

    allele_pattern = re.compile(r"\bHLA-[A-Za-z0-9*:\\-]+")
    alleles = sorted(set(allele_pattern.findall(text)))

    hla_associations = []
    for allele in alleles:
        hla_associations.append({
            "allele": allele,
            "effect": None,
            "odds_ratio": None,
            "confidence_interval": None,
            "p_value": None,
            "frequency_cases": None,
            "frequency_controls": None,
            "context": "Detected allele mention via heuristic with BiomedBERT fallback",
        })

    data = {
        "disease": disease,
        "disease_subtype": None,
        "population": None,
        "study_type": None,
        "sample_size": {"cases": None, "controls": None},
        "hla_associations": hla_associations,
        "hla_combinations": [],
        "key_findings": "Extracted by local BiomedBERT heuristic fallback",
    }

    return data, tokens_in, 0


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=60),
    retry=retry_if_exception_type((Exception,)),
    before_sleep=lambda retry_state: logger.warning(
        f"Retry attempt {retry_state.attempt_number} after error: {retry_state.outcome.exception()}"
    ),
)
def extract_from_text(
    text: str,
    model: str = None,
    temperature: float = None,
) -> Tuple[Dict, int, int]:
    """
    Extract HLA-disease associations from a paper's text using GPT-4o or local HF model.

    Args:
        text: The paper abstract or full text
        model: OpenAI model or Hugging Face model name
        temperature: Sampling temperature

    Returns:
        Tuple of (extracted_data_dict, tokens_input, tokens_output)
    """
    if model is None:
        model = config.OPENAI_MODEL
    if temperature is None:
        temperature = config.OPENAI_TEMPERATURE

    # Truncate if needed
    text = truncate_text(text)

    if model and model.lower().startswith("microsoft/"):
        logger.info(f"Using local Hugging Face model fallback: {model}")
        return _extract_from_hf_model(text=text, model=model)

    # Build the prompt
    user_prompt = EXTRACTION_PROMPT.format(text=text)

    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        temperature=temperature,
        max_tokens=config.OPENAI_MAX_TOKENS,
        response_format={"type": "json_object"},
    )

    # Parse response
    content = response.choices[0].message.content
    tokens_in = response.usage.prompt_tokens
    tokens_out = response.usage.completion_tokens

    try:
        data = json.loads(content)
    except json.JSONDecodeError as e:
        logger.error(f"JSON parse error: {e}\nResponse content: {content[:500]}")
        raise

    return data, tokens_in, tokens_out


def extract_from_paper(
    pmid: Optional[str],
    abstract: Optional[str],
    full_text: Optional[str],
    model: str = None,
) -> Dict:
    """
    Extract HLA associations from a paper. Uses full text if available,
    otherwise falls back to abstract.

    Returns a dict with extraction results and metadata.
    """
    # Choose the best available text
    if full_text and len(full_text) > 100:
        text = full_text
        text_source = "full_text"
    elif abstract and len(abstract) > 50:
        text = abstract
        text_source = "abstract"
    else:
        return {
            "status": "skipped",
            "reason": "No text available (no abstract or full text)",
            "data": None,
            "tokens_input": 0,
            "tokens_output": 0,
            "cost_usd": 0,
        }

    try:
        data, tokens_in, tokens_out = extract_from_text(text, model=model)

        # Validate the extraction
        validation = validate_extraction(data)

        cost = estimate_cost(tokens_in, tokens_out, model or config.OPENAI_MODEL)

        return {
            "status": "completed",
            "data": data,
            "validation": {
                "is_valid": validation.is_valid,
                "errors": validation.errors,
                "warnings": validation.warnings,
            },
            "text_source": text_source,
            "tokens_input": tokens_in,
            "tokens_output": tokens_out,
            "cost_usd": cost,
            "model": model or config.OPENAI_MODEL,
            "extracted_at": datetime.utcnow().isoformat(),
        }

    except Exception as e:
        logger.error(f"Extraction failed for PMID {pmid}: {e}")
        return {
            "status": "failed",
            "error": str(e),
            "data": None,
            "tokens_input": 0,
            "tokens_output": 0,
            "cost_usd": 0,
        }
