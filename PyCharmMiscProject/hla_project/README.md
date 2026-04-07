# HLA Literature Mining Pipeline

Automated pipeline for extracting HLA-disease associations from published literature and building a knowledge graph.

## Target Diseases

- Multiple Sclerosis (MS)
- Neuromyelitis Optica Spectrum Disorder (NMOSD)
- Myasthenia Gravis (MG)
- Guillain-Barré syndrome (GBS)
- Transverse Myelitis (TM)
- Optic Neuritis
- Autoimmune Encephalitis (AIE / AE)
- Chronic Inflammatory Demyelinating Polyneuropathy (CIDP)
- Acute Disseminated Encephalomyelitis (ADEM)
- MOG Antibody Disease (MOGAD / MOG-IgG-associated disease)

## Architecture

```
PubMed/bioRxiv -> Download & Store (PostgreSQL) -> LLM Extraction (GPT-4o) -> Neo4j Knowledge Graph
```

## Setup

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Configure `.env` with your API keys:
   ```
   NICB_API_KEY=your_ncbi_key
   OPENAI_API_KEY=your_openai_key
   DATABASE_URL=postgresql://user:pass@localhost:5432/hla_mining
   NEO4J_URI=bolt://localhost:7687
   NEO4J_USER=neo4j
   NEO4J_PASSWORD=your_password
   ```

3. Create the PostgreSQL database:
   ```bash
   createdb hla_mining
   psql -d hla_mining -f db/schema.sql
   ```

## Usage

```bash
# Search PubMed for papers on a specific disease
python -m pipeline.cli search --disease MS --source pubmed

# Search bioRxiv
python -m pipeline.cli search --disease MS --source biorxiv

# Download abstracts and full-text
python -m pipeline.cli download --batch-size 200

# Extract HLA associations using LLM
python -m pipeline.cli extract --batch-size 100 --model gpt-4o

# Validate extracted data
python -m pipeline.cli validate

# Build Neo4j knowledge graph
python -m pipeline.cli build-graph

# Run entire pipeline
python -m pipeline.cli run-all --disease all
```

## Project Structure

```
HLA/
├── config.py               # Configuration management
├── db/                     # Database schema and models
├── search/                 # PubMed and bioRxiv search
├── download/               # Abstract/full-text/PDF download
├── extraction/             # LLM extraction and validation
├── knowledge_graph/        # Neo4j knowledge graph
├── pipeline/               # Orchestration and CLI
└── data/pdfs/              # Downloaded PDFs (gitignored)
```
