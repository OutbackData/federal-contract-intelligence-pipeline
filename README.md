# federal-contract-intelligence-pipeline
# Federal Contract Intelligence Pipeline

Automated Python pipeline that extracts **recent U.S. federal prime contract awards** and identifies **high-value subcontracting opportunities** using sector classification, scoring models, and enrichment.

The system pulls fresh award data from the USAspending API, analyzes industry sectors via NAICS codes, and generates ranked leads for potential subcontracting opportunities.

---

## Overview

This project builds an automated **data pipeline for federal contract intelligence**.

The script:

1. Pulls recent federal contract awards
2. Filters high-value contracts
3. Classifies industry sectors via NAICS codes
4. Scores leads based on market factors
5. Estimates subcontracting opportunity size
6. Exports structured datasets for analysis or sales outreach

The pipeline is designed to produce **fresh, actionable contract leads** with minimal manual work.

---

## Data Source

Data is retrieved from the official **USAspending.gov API**, which provides public data on U.S. federal spending and contract awards.

API Endpoint used:
https://api.usaspending.gov/api/v2/search/spending_by_award/

---

## Key Features

### Automated Data Extraction
- Pulls recent federal contract awards
- Filters by award amount and contract type
- Retrieves recipient company, agency, NAICS, and location

### Sector Classification
Contracts are categorized into major federal market sectors including:

- Cybersecurity / IT
- Defense & Aerospace
- AI / Machine Learning R&D
- Infrastructure & Construction
- Healthcare & Medical
- Logistics & Transportation
- General Government Services

Sector classification is performed using **NAICS code prefixes**.

### Lead Scoring Model

Each contract recipient is scored using multiple signals:

- Contract size
- Award recency
- Market growth sector
- Geographic federal contracting activity

Scores range from **0–100**, with high scores representing stronger subcontracting potential.

### Market Opportunity Estimation

A probabilistic model estimates potential subcontracting opportunity size using a log-normal distribution.

Outputs include:

- Estimated subcontract value
- market trend signal
- lead strength score

### Parallel Data Processing

The enrichment and scoring pipeline uses **ThreadPoolExecutor** for faster processing of large datasets.

### Resilient API Requests

The script includes:

- automatic retries
- exponential backoff
- rate limit handling

---

## Output Files

The pipeline generates three outputs:

### 1. Full Lead Dataset
Federal_PrimeContracts_Fresh_TIMESTAMP.csv


Contains all processed contracts and enrichment data.

### 2. High-Quality Leads


Federal_STRONG_70+_TIMESTAMP.csv


Filtered list of leads with **Lead Score ≥ 70**.

These represent the strongest subcontracting opportunities.

### 3. SQLite Database


federal_contract_leads.db


Stores structured lead data for querying or further analysis.

---

## Example Output Fields

| Field | Description |
|------|-------------|
| Company Name | Contract recipient |
| Award Amount | Total contract value |
| Awarding Agency | Federal agency issuing the award |
| NAICS | Industry classification |
| Sector | Classified market sector |
| Lead Score | Opportunity strength (0–100) |
| Est Subcontract Potential | Estimated subcontract value |
| State | Place of performance |
| City | Place of performance |
| Recency | Days since award |

---

## Tech Stack

- Python
- pandas
- requests
- tqdm
- scipy
- sqlite3
- concurrent.futures

---

