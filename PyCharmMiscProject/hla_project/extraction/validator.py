"""
Data validation module for extracted HLA-disease association data.
Uses Pydantic models for schema validation and custom rules for domain-specific checks.
"""

import re
import logging
from typing import List, Optional, Tuple

from pydantic import BaseModel, Field, field_validator

logger = logging.getLogger(__name__)

# Valid HLA allele pattern: HLA-GENE*XX:XX (2-field resolution or higher)
HLA_ALLELE_PATTERN = re.compile(
    r"^HLA-[A-Z][A-Za-z0-9]*\*\d{1,4}:\d{1,4}(:\d{1,4})?(:\d{1,4})?[A-Z]?$"
)

VALID_DISEASES = [
    "Multiple Sclerosis",
    "NMOSD",
    "Neuromyelitis Optica",
    "Neuromyelitis Optica Spectrum Disorder",
    "Myasthenia Gravis",
    "Guillain-Barre Syndrome",
    "Guillain-Barré Syndrome",
    "Transverse Myelitis",
    "Optic Neuritis",
    "Autoimmune Encephalitis",
    "Chronic Inflammatory Demyelinating Polyneuropathy",
    "Acute Disseminated Encephalomyelitis",
    "MOG Antibody Disease",
    "AE",
    "AIE",
    "CIDP",
    "ADEM",
    "MOGAD",
    "MOG-IgG-associated disease",
]

VALID_EFFECTS = ["risk", "protective", "neutral", None]


class SampleSize(BaseModel):
    cases: Optional[int] = None
    controls: Optional[int] = None


class HLAAssociationData(BaseModel):
    allele: str
    effect: Optional[str] = None
    odds_ratio: Optional[float] = None
    confidence_interval: Optional[List[float]] = None
    p_value: Optional[float] = None
    frequency_cases: Optional[float] = None
    frequency_controls: Optional[float] = None
    context: Optional[str] = None

    @field_validator("allele")
    @classmethod
    def validate_allele(cls, v):
        # Try to normalize common variants
        v = v.strip()
        if not v.startswith("HLA-"):
            # Try to add HLA- prefix
            if re.match(r"^[A-Z][A-Za-z0-9]*\*", v):
                v = f"HLA-{v}"
        return v

    @field_validator("effect")
    @classmethod
    def validate_effect(cls, v):
        if v is not None and v not in ["risk", "protective", "neutral"]:
            logger.warning(f"Invalid effect value: {v}")
            return None
        return v

    @field_validator("p_value")
    @classmethod
    def validate_p_value(cls, v):
        if v is not None and (v < 0 or v > 1):
            logger.warning(f"Invalid p_value: {v}")
            return None
        return v

    @field_validator("odds_ratio")
    @classmethod
    def validate_odds_ratio(cls, v):
        if v is not None and v <= 0:
            logger.warning(f"Invalid odds_ratio: {v}")
            return None
        return v


class HLACombinationData(BaseModel):
    alleles: List[str]
    haplotype: Optional[str] = None
    effect: Optional[str] = None
    odds_ratio: Optional[float] = None
    confidence_interval: Optional[List[float]] = None
    p_value: Optional[float] = None
    context: Optional[str] = None


class ExtractionResult(BaseModel):
    disease: Optional[str] = None
    disease_subtype: Optional[str] = None
    population: Optional[str] = None
    study_type: Optional[str] = None
    sample_size: Optional[SampleSize] = None
    hla_associations: List[HLAAssociationData] = Field(default_factory=list)
    hla_combinations: List[HLACombinationData] = Field(default_factory=list)
    key_findings: Optional[str] = None


class ValidationResult(BaseModel):
    is_valid: bool
    errors: List[str] = Field(default_factory=list)
    warnings: List[str] = Field(default_factory=list)
    data: Optional[ExtractionResult] = None


def validate_extraction(data: dict) -> ValidationResult:
    """
    Validate extracted HLA-disease association data.
    Returns a ValidationResult with errors and warnings.
    """
    errors = []
    warnings = []

    # Parse with Pydantic
    try:
        result = ExtractionResult(**data)
    except Exception as e:
        return ValidationResult(
            is_valid=False,
            errors=[f"Schema validation failed: {str(e)}"],
        )

    # Validate disease name
    if result.disease and result.disease not in VALID_DISEASES:
        warnings.append(
            f"Disease '{result.disease}' not in standard list. "
            f"Valid: {VALID_DISEASES}"
        )

    # Validate each association
    for i, assoc in enumerate(result.hla_associations):
        assoc_errors, assoc_warnings = _validate_association(assoc, i)
        errors.extend(assoc_errors)
        warnings.extend(assoc_warnings)

    # Validate combinations
    for i, combo in enumerate(result.hla_combinations):
        for j, allele in enumerate(combo.alleles):
            if not HLA_ALLELE_PATTERN.match(allele):
                warnings.append(
                    f"Combination {i}, allele {j}: '{allele}' doesn't match standard HLA format"
                )

    is_valid = len(errors) == 0

    return ValidationResult(
        is_valid=is_valid,
        errors=errors,
        warnings=warnings,
        data=result,
    )


def _validate_association(assoc: HLAAssociationData, index: int) -> Tuple[List[str], List[str]]:
    """Validate a single HLA association entry."""
    errors = []
    warnings = []
    prefix = f"Association {index} ({assoc.allele})"

    # Check allele format
    if not HLA_ALLELE_PATTERN.match(assoc.allele):
        warnings.append(f"{prefix}: allele doesn't match standard HLA format (HLA-GENE*XX:XX)")

    # Check effect vs odds_ratio consistency
    if assoc.odds_ratio is not None and assoc.effect is not None:
        if assoc.effect == "risk" and assoc.odds_ratio < 1:
            warnings.append(
                f"{prefix}: effect is 'risk' but OR={assoc.odds_ratio} < 1"
            )
        elif assoc.effect == "protective" and assoc.odds_ratio > 1:
            warnings.append(
                f"{prefix}: effect is 'protective' but OR={assoc.odds_ratio} > 1"
            )

    # Check confidence interval
    if assoc.confidence_interval is not None:
        if len(assoc.confidence_interval) != 2:
            errors.append(f"{prefix}: confidence interval must have exactly 2 values")
        elif assoc.confidence_interval[0] > assoc.confidence_interval[1]:
            errors.append(
                f"{prefix}: CI lower ({assoc.confidence_interval[0]}) > upper ({assoc.confidence_interval[1]})"
            )
        elif assoc.odds_ratio is not None:
            ci_low, ci_high = assoc.confidence_interval
            if not (ci_low <= assoc.odds_ratio <= ci_high):
                warnings.append(
                    f"{prefix}: OR={assoc.odds_ratio} outside CI [{ci_low}, {ci_high}]"
                )

    # Flag very extreme values
    if assoc.odds_ratio is not None and assoc.odds_ratio > 100:
        warnings.append(f"{prefix}: extremely high OR={assoc.odds_ratio}, verify manually")

    if assoc.p_value is not None and assoc.p_value > 0.05:
        warnings.append(f"{prefix}: p-value={assoc.p_value} > 0.05, not statistically significant")

    return errors, warnings


def normalize_allele_name(allele: str) -> str:
    """
    Normalize an HLA allele name to standard IMGT format.
    Examples:
        DRB1*1501 -> HLA-DRB1*15:01
        HLA-DRB1*15:01 -> HLA-DRB1*15:01 (no change)
        A*0201 -> HLA-A*02:01
    """
    if allele is None or not isinstance(allele, str):
        return ""

    allele = allele.strip()
    if not allele:
        return ""

    # Add HLA- prefix if missing
    if not allele.startswith("HLA-"):
        if re.match(r"^[A-Z]", allele):
            allele = f"HLA-{allele}"

    # Insert colon in 4-digit codes without colons
    # e.g., HLA-DRB1*1501 -> HLA-DRB1*15:01
    match = re.match(r"(HLA-[A-Za-z0-9]+\*)(\d{4})$", allele)
    if match:
        prefix = match.group(1)
        digits = match.group(2)
        allele = f"{prefix}{digits[:2]}:{digits[2:]}"

    # Handle 2-digit only (e.g., HLA-DR15 -> keep as-is with warning)
    return allele
