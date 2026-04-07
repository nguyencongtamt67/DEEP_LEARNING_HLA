"""
Prompt templates for LLM-based extraction of HLA-disease associations.
Specialized prompts for biomedical named entity recognition and relation extraction.
"""

SYSTEM_PROMPT = """You are an expert biomedical researcher specializing in HLA (Human Leukocyte Antigen) genetics and autoimmune neurological diseases. Your task is to extract structured HLA-disease association data from scientific publications with high precision.

Key guidelines:
- Only extract information explicitly stated in the text
- Use IMGT/HLA nomenclature for allele names (e.g., HLA-DRB1*15:01)
- Normalize allele names to at least 4-digit (2-field) resolution when possible
- Distinguish between risk alleles (OR > 1) and protective alleles (OR < 1)
- Report exact p-values and odds ratios as stated in the paper
- If confidence intervals are given, include them
- If information is not available, use null
- For population, use the most specific ethnic/geographic descriptor given
"""

EXTRACTION_PROMPT = """Analyze the following scientific publication and extract all HLA-disease association data.

=== PUBLICATION TEXT ===
{text}
=== END OF TEXT ===

Extract the information into the following JSON structure. Be thorough but only include data explicitly stated in the paper:

{{
  "disease": "<primary disease studied: one of 'Multiple Sclerosis', 'NMOSD', 'Myasthenia Gravis', 'Guillain-Barre Syndrome', 'Transverse Myelitis', 'Optic Neuritis', 'Autoimmune Encephalitis', 'Chronic Inflammatory Demyelinating Polyneuropathy', 'Acute Disseminated Encephalomyelitis', 'MOG Antibody Disease', or exact disease name if different>",
  "disease_subtype": "<subtype if mentioned, e.g., 'RRMS', 'PPMS', 'AQP4-positive NMOSD', 'MuSK MG', 'AMAN', etc., or null>",
  "population": "<study population ethnicity/nationality, e.g., 'European', 'Japanese', 'Vietnamese', 'Han Chinese', etc., or null>",
  "study_type": "<e.g., 'GWAS', 'case-control', 'meta-analysis', 'cohort', 'systematic review', or null>",
  "sample_size": {{
    "cases": <number of cases or null>,
    "controls": <number of controls or null>
  }},
  "hla_associations": [
    {{
      "allele": "<HLA allele in IMGT format, e.g., HLA-DRB1*15:01>",
      "effect": "<'risk' if OR>1, 'protective' if OR<1, 'neutral' if no significant effect, or null>",
      "odds_ratio": <numeric OR value or null>,
      "confidence_interval": [<lower bound>, <upper bound>] or null,
      "p_value": <numeric p-value or null>,
      "frequency_cases": <allele frequency in cases or null>,
      "frequency_controls": <allele frequency in controls or null>,
      "context": "<brief description of the specific finding>"
    }}
  ],
  "hla_combinations": [
    {{
      "alleles": ["<allele1>", "<allele2>"],
      "haplotype": "<haplotype name if given, e.g., 'DR15-DQ6', or null>",
      "effect": "<'risk' or 'protective' or null>",
      "odds_ratio": <numeric OR or null>,
      "confidence_interval": [<lower>, <upper>] or null,
      "p_value": <numeric p-value or null>,
      "context": "<description>"
    }}
  ],
  "key_findings": "<1-3 sentence summary of the main HLA-related findings>"
}}

Important:
- If the paper reports multiple populations, create separate association entries for each
- If no HLA associations are found, return empty arrays for hla_associations and hla_combinations
- Convert relative risks (RR) or hazard ratios (HR) to the odds_ratio field with a note in context
- Ensure allele names follow HLA-GENE*XX:XX format (e.g., HLA-A*02:01, HLA-DRB1*15:01)
- For amino acid associations, note them in the context field

Respond with ONLY the JSON object, no additional text."""

TABLE_EXTRACTION_PROMPT = """The following text contains tables from a scientific publication about HLA-disease associations. Extract all HLA association data from these tables.

=== TABLE DATA ===
{table_text}
=== END OF TABLE DATA ===

For each row in the tables that contains HLA association data, extract:

{{
  "table_associations": [
    {{
      "allele": "<HLA allele>",
      "disease": "<disease>",
      "population": "<population or null>",
      "effect": "<risk or protective>",
      "odds_ratio": <OR value or null>,
      "confidence_interval": [<lower>, <upper>] or null,
      "p_value": <p-value or null>,
      "frequency_cases": <frequency or null>,
      "frequency_controls": <frequency or null>,
      "context": "<any additional context from the table>"
    }}
  ]
}}

Respond with ONLY the JSON object."""

VALIDATION_PROMPT = """Review the following extracted HLA-disease association data for accuracy and consistency. Flag any issues.

=== EXTRACTED DATA ===
{extracted_json}
=== END OF DATA ===

Check for:
1. Allele nomenclature correctness (should be HLA-GENE*XX:XX format)
2. Odds ratio and p-value consistency (OR > 1 should be "risk", OR < 1 should be "protective")
3. Confidence interval validity (lower < OR < upper for risk, lower < upper)
4. Any obvious data quality issues

Return a JSON object:
{{
  "is_valid": true/false,
  "issues": ["<list of issues found>"],
  "corrected_data": <corrected version of the data if issues found, or null>
}}

Respond with ONLY the JSON object."""
