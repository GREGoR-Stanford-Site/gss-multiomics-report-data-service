# GSS Multiomics Report Data Service

A high-performance FastAPI service for storing and querying large-scale genome sequencing data with support for PostgreSQL.

## 📋 Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Installation](#installation)
- [Configuration](#configuration)
- [API Documentation](#api-documentation)
- [Data Schema](#data-schema)
- [Usage Examples](#usage-examples)
- [Development](#development)
- [Deployment](#deployment)
- [Performance](#performance)
- [Troubleshooting](#troubleshooting)

## Overview

This service provides a REST API for efficient storage and retrieval of genomic variant data from whole genome/exome sequencing. It supports:

- **Small variants** (SNVs, indels) from GSS data
- **Structural variants** (SVs) with complex annotations
- **Efficient querying** by chromosome, gene, position, and family ID
- **Backend support**: PostgreSQL (production)

### Key Capabilities

- 🧬 Query millions of variants by gene name
- 📍 Position-based range queries
- 👨‍👩‍👧‍👦 Family-based variant filtering
- 📊 Bulk data loading from TSV files
- 🚀 High-performance indexing strategies
- 🔄 Support for SQL backends

## Features

### Core Functionality

- **Multi-backend Support**
  - PostgreSQL with Cloud SQL connector (GCP)
  - Local Postgres server
  - Identical API interface for both backends

- **Advanced Querying**
  - Filter by chromosome, position range, gene names
  - Support for array columns (gene lists in SVs)
  - Pagination with size and offset
  - Custom field selection

- **Data Management**
  - Chunked loading for large datasets
  - Conflict resolution (insert/ignore/update)
  - Table partitioning by selected column
  - Hash-based deduplication

- **Performance Optimizations**
  - B-tree indexes on position columns
  - Hash indexes on gene names
  - GIN indexes for array columns
  - Efficient bulk insert operations

### Technology Stack

- **Framework**: FastAPI
- **ORM**: SQLAlchemy (PostgreSQL)
- **Database**: PostgreSQL 12+ 
- **Data Processing**: Pandas, Polars
- **Cloud**: GCP Cloud SQL Connector
- **Python**: 3.9+

## Installation

### Prerequisites

- Python 3.9 or higher
- PostgreSQL 12+ (for PostgreSQL backend)
- Access to GCP Cloud SQL (optional, for cloud deployment)

### Local Setup

1. **Clone the repository**
```bash
git clone <repository-url>
cd gss-multiomics-report-data-service
```

2. **Create virtual environment**
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. **Install dependencies**
```bash
pip install -r requirements.txt
```

4. **Set up environment variables**
```bash
cp .env_local .env
# Edit .env with your configuration
```

## Configuration

### Environment Variables

Create a `.env` file with the following variables:

```bash
# Database Selection
USE_GCP_DB=0  # 0 for local PostgreSQL, 1 for GCP Cloud SQL

# PostgreSQL Configuration
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password_here
DATABASE_URLS=postgresql://postgres:password@localhost:5432/gss_data,postgresql://postgres:password@localhost:5432/sv_data
DEFAULT_DB_NAME=gss_data

# GCP Cloud SQL (if USE_GCP_DB=1)
INSTANCE_CONNECTION_NAME=project:region:instance

# Data Directory
DATA_DIR=../data/

```

### Database URLs Format

Multiple local databases can be configured:
```bash
DATABASE_URLS=postgresql://user:pass@host:port/db1,postgresql://user:pass@host:port/db2
```

The service will parse these and create handlers for each database.

## API Documentation

### Base URL

```
http://localhost:8000
```

### Interactive API Docs

- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

---

## PostgreSQL API Endpoints

### 1. Search Variants

**Endpoint:** `POST /search`

Query genome variants with flexible filtering.

**Request Body:**
```json
{
  "filters": [
    {
      "chrom": "chr1",
      "gene": ["BRCA1", "BRCA2"],
      "pos": {
        ">=": 1000000,
        "<=": 2000000
      }
    }
  ],
  "selected_fields": ["chrom", "pos", "ref", "alt", "gene", "consequence"],
  "size": 100,
  "offset": 0,
  "return_lol": 0,
  "debug": 0
}
```

**Parameters:**
- `filters`: List of filter dictionaries (OR logic between filters)
  - `chrom`: Chromosome name (e.g., "chr1", "chrX")
  - `gene`: Gene name(s) - string or list
  - `pos`: Position or range (`{">=": start, "<=": end}`)
  - Any other schema field
- `selected_fields`: List of fields to return (default: all)
- `size`: Maximum results to return (default: 1)
- `offset`: Number of results to skip (default: 0)
- `return_lol`: Return as list of lists instead of dicts (default: 0)

**Response:**
```json
{
  "results": [
    {
      "chrom": "chr1",
      "pos": 1234567,
      "ref": "A",
      "alt": "G",
      "gene": "BRCA1",
      "consequence": "missense_variant"
    }
  ]
}
```

### 2. Load Data

**Endpoint:** `POST /load_data`

Load genome data from TSV files into the database.

**Request (File Upload):**
```bash
curl -X POST "http://localhost:8000/load_data" \
  -F "uploaded_file=@genome_data.tsv" \
  -F "params={\"chunk_size\": 1000, \"on_conflict\": \"update\"}"
```

**Request (File in DATA_DIR):**
```json
{
  "file_name": "genome_data.tsv",
  "chunk_size": 1000,
  "start_chunk": 0,
  "chunk_count": 10,
  "valid_table_name_pat": "chr.*",
  "skip_table_name_pat": "chrUn.*",
  "on_conflict": "update"
}
```

**Parameters:**
- `file_name`: Name of file in DATA_DIR
- `chunk_size`: Rows per batch insert (default: 100)
- `start_chunk`: Starting chunk index (for resuming)
- `chunk_count`: Number of chunks to load
- `valid_table_name_pat`: Regex for table names to include
- `skip_table_name_pat`: Regex for table names to skip
- `on_conflict`: `ignore` or `update` (default: update)

### 3. Execute Raw Query

**Endpoint:** `POST /query`

Execute arbitrary SQL queries.

**Request:**
```json
{
  "query": "SELECT chrom, COUNT(*) as count FROM chr1 GROUP BY chrom"
}
```

**Response:**
```json
[
  {"chrom": "chr1", "count": 12345}
]
```

### 4. Create Tables

**Endpoint:** `POST /create_tables`

Create new tables with the default schema.

**Request:**
```json
{
  "table_names": ["chr1", "chr2", "chrX"]
}
```

### 5. Drop Tables

**Endpoint:** `POST /drop_tables`

Drop tables matching a regex pattern.

**Request:**
```bash
curl -X POST "http://localhost:8000/drop_tables?table_name_pat=chr.*"
```

---

## Data Schema


### GSS multiomics report (Structural Variants)

Table: `sv_table` or family-specific tables

| Column | Type | Indexed | Description |
|--------|------|---------|-------------|
| id | INTEGER | PK | Auto-increment primary key |
| family_id | VARCHAR | ✓ | Family identifier |
| chrom | VARCHAR | ✓ | Chromosome |
| start | INTEGER | ✓ | SV start position |
| end | INTEGER | ✓ | SV end position |
| ref | VARCHAR | - | Reference sequence |
| alt | VARCHAR | - | Alternate sequence |
| length | INTEGER | - | SV length |
| type | VARCHAR | - | SV type (DEL, DUP, INV, etc.) |
| genotype | VARCHAR | - | Proband genotype |
| maternal_genotype | VARCHAR | - | Maternal genotype |
| paternal_genotype | VARCHAR | - | Paternal genotype |
| inheritance | VARCHAR | - | Inheritance pattern |
| af_all | FLOAT | - | Allele frequency (all populations) |
| af_afr | FLOAT | - | AF African |
| af_amr | FLOAT | - | AF American |
| af_eas | FLOAT | - | AF East Asian |
| af_eur | FLOAT | - | AF European |
| af_sas | FLOAT | - | AF South Asian |
| omim | VARCHAR | - | OMIM annotations |
| exonic | BOOLEAN | - | Overlaps exonic region |
| centromeric | BOOLEAN | - | Overlaps centromere |
| pericentromeric | BOOLEAN | - | Overlaps pericentromere |
| telomeric | BOOLEAN | - | Overlaps telomere |
| str | BOOLEAN | - | Short tandem repeat |
| vntr | BOOLEAN | - | Variable number tandem repeat |
| segdup | BOOLEAN | - | Segmental duplication |
| repeat | BOOLEAN | - | Repeat region |
| gap | BOOLEAN | - | Assembly gap |
| hiconf | BOOLEAN | - | High confidence region |
| alt_reads | INTEGER | - | Alternate allele read count |
| ref_reads | INTEGER | - | Reference allele read count |
| total_reads | INTEGER | - | Total read count |
| gt_homwt | INTEGER | - | Homozygous WT genotype count |
| gt_het | INTEGER | - | Heterozygous genotype count |
| gt_homvar | INTEGER | - | Homozygous variant count |
| gene | ARRAY[VARCHAR] | ✓ (GIN) | Array of gene names |
| variant_hash | VARCHAR | ✓ (unique) | MD5 hash for deduplication |

**Indexes:**
- B-tree: (chrom, start, end), (family_id, chrom, start)
- GIN: gene (array)
- Hash: variant_hash (unique)

## Usage Examples

### Note: you can try the APIs in the docs page at: localhost/IP_address:8000/docs.


### Example 1: Query by Gene Name

```bash
curl -X POST "http://localhost:8000/search" \
  -H "Content-Type: application/json" \
  -d '{
    "filters": [
      {
        "gene": ["BRCA1", "BRCA2", "TP53"]
      }
    ],
    "size": 50
  }'
```

### Example 2: Query by Chromosome and Position

```bash
curl -X POST "http://localhost:8000/search" \
  -H "Content-Type: application/json" \
  -d '{
    "filters": [
      {
        "chrom": "chr17",
        "pos": {
          ">=": 41196312,
          "<=": 41277500
        }
      }
    ],
    "selected_fields": ["pos", "ref", "alt", "gene", "consequence"],
    "size": 100
  }'
```

### Example 3: Query SV Data by Family

```bash
curl -X POST "http://localhost:8000/search" \
  -H "Content-Type: application/json" \
  -d '{
    "filters": [
      {
        "family_id": "FAM12345",
        "gene": ["BRCA1"],
        "type": "DEL"
      }
    ]
  }'
```

### Example 4.1: Load Data from File by curl

```bash
curl -X POST "http://localhost:8000/load_data" \
  -H "Content-Type: application/json" \
  -d '{
    "file_name": "chr1_variants.tsv",
    "chunk_size": 5000,
    "on_conflict": "update",
    "valid_table_name_pat": "chr1"
  }'
```
### Example 4.2: Load Data from Files in Batch

```bash
PYTHPONPATH=. python app/db_cli.ply --chunk_size 1000 --data_files raw_file1.txt raw_file2.txt ...
```

### Example 5: Multiple Filter Conditions (OR)

```bash
curl -X POST "http://localhost:8000/search" \
  -H "Content-Type: application/json" \
  -d '{
    "filters": [
      {"chrom": "chr1", "gene": "BRCA1"},
      {"chrom": "chr17", "gene": "TP53"}
    ],
    "size": 100
  }'
```

### Example 6: Statistics Query

```bash
curl -X POST "http://localhost:8000/query" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "SELECT gene, COUNT(*) as variant_count, AVG(af) as avg_frequency FROM chr1 WHERE gene IS NOT NULL GROUP BY gene ORDER BY variant_count DESC LIMIT 10"
  }'
```

## Development

### Running Locally

```bash
# Start the server with auto-reload
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

### Database Setup (PostgreSQL)

**Default: GCP Cloud SQL
Use .env file to set the credentials for the cloud sql api access

**Option 1: Local PostgreSQL**
```bash
# Install PostgreSQL
brew install postgresql  # macOS
# or
apt-get install postgresql  # Linux

# Start PostgreSQL
brew services start postgresql  # macOS
# or
sudo service postgresql start  # Linux

# Create databases
createdb gss-multiomics-report
```

**Option 2: Docker PostgreSQL**
```bash
# Using docker-compose
docker-compose up -d

# Or manually
docker run -d \
  --name postgres-gss \
  -e POSTGRES_PASSWORD=password \
  -e POSTGRES_DB=db_name \
  -p 5432:5432 \
  postgres:14
```


### Testing

```bash
# Test PostgreSQL connection
python -c "from app.db_manager import initialize_db; db = initialize_db(); print(db.get_curr_table_names())"
```

### Code Structure

```
app/
├── __init__.py           # Package initialization
├── main.py              # FastAPI application and endpoints
├── db_manager.py        # PostgreSQL database manager
├── config.py            # Configuration and settings
├── utils.py             # Utility functions
└── db_cli.py           # Command-line interface
```

## Deployment

### Docker Deployment

```bash
# Build image
docker-compse build

# Run container
docker-compose up

# Shutdown the container
docker-compose down
```

Once the container image is built locally, you can save and copy the image to any VM.

```bash
# Save the image named gss_api
docker save |gzip > gss_api.tgz

# Copy the file to the target VM
gcloud compute scp --zone ZONE --project PROJETC_ID gss_api.tgz VM_NAME:~/

# Copy the Dockerfile/docker-compose.yml 
gcloud compute scp --zone ZONE --project PROJETC_ID Dockerfile VM_NAME:~/
gcloud compute scp --zone ZONE --project PROJETC_ID docker-compose.yml VM_NAME:~/

# ssh to the VM and Load the image
docker load < gss_api.tgz

# Start the container
docker-compose up

```

### GCP Cloud Run Deployment

```bash
# Build and push to GCR
gcloud builds submit --tag gcr.io/PROJECT_ID/gss-data-service

# Deploy to Cloud Run
gcloud run deploy gss-data-service \
  --image gcr.io/PROJECT_ID/gss-data-service \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated \
  --set-env-vars USE_GCP_DB=1,INSTANCE_CONNECTION_NAME=PROJECT:REGION:INSTANCE
```

### Environment-Specific Configuration

**Development:**
```bash
USE_GCP_DB=0
DATABASE_URLS=postgresql://localhost:5432/gss_data
```

**Production:**
```bash
USE_GCP_DB=1
INSTANCE_CONNECTION_NAME=project:region:instance
POSTGRES_USER=postgres
POSTGRES_PASSWORD=<from-secret-manager>
```


### Monitoring

```sql
-- Check table sizes
SELECT
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;

-- Check index usage
SELECT
    schemaname,
    tablename,
    indexname,
    idx_scan,
    idx_tup_read,
    idx_tup_fetch
FROM pg_stat_user_indexes
ORDER BY idx_scan DESC;
```

## Troubleshooting

### Common Issues

**1. Connection Failed**
```
Error: could not connect to server
```
Solution: Check DATABASE_URLS and ensure PostgreSQL is running.

**2. Table Not Found**
```
Error: relation "chr1" does not exist
```
Solution: Create tables first using `/create_tables` endpoint.

**3. Import Error: No module named 'google.cloud.sql.connector'**
```
Solution: pip install cloud-sql-python-connector[pg8000]
```

**4. Out of Memory During Load**
```
Solution: Reduce chunk_size parameter (try 100-500)
```

### Debug Mode

Enable debug output:
```json
{
  "filters": [...],
  "debug": 1
}
```

### Logs

```bash
# View application logs
tail -f app.log

# View PostgreSQL logs
tail -f /usr/local/var/postgres/server.log  # macOS
tail -f /var/log/postgresql/postgresql-14-main.log  # Linux
```


## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request


## Support

For issues and questions:
- Create an issue in the repository
- Contact the development team

## Changelog

### Version 1.0.0
- Initial release
- PostgreSQL backend implementation
- Dual backend architecture
- Comprehensive API documentation

---

**Documentation:**
- API Docs: http://localhost/IP:8000/docs
