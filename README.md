# TransactTap

A Python-based data pipeline for automating extraction and analysis of personal financial data from multiple financial institutions.

## Overview

This project automates the collection, processing, and analysis of personal finance data:

1. Extracts transaction data from multiple bank websites
2. Handles authentication including MFA and CAPTCHA
3. Processes and categorizes transactions
4. Loads data to BigQuery for analysis

## Setup

### Prerequisites

- Python 3.9+
- Google Cloud account with BigQuery
- Chrome/Chromium browser

### Installation

1. Clone the repository
```bash
git clone https://github.com/yourusername/finance-pipeline.git
cd finance-pipeline
```

2. Create a virtual environment
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies
```bash
pip install -e .
```

4. Configure credentials
```bash
cp .env.example .env
# Edit .env with your bank credentials
```

5. Run the pipeline
```bash
python -m src.main
```

## Features

- **Multi-Bank Support**: Examples included for Wells Fargo and Chase
- **Authentication**: Handles login flows including MFA and CAPTCHA
- **Data Processing**: Deduplicates and categorizes transactions
- **BigQuery Integration**: Stores processed data for analysis
- **Configurable**: Can customize transaction categorization

## Usage

Basic extraction:
```bash
python -m src.main
```

Specific bank extraction:
```bash
python -m src.main --banks wells_fargo chase
```

Date range extraction:
```bash
python -m src.main --start-date 2025-01-01 --end-date 2025-03-31
```

## Extending

### Adding a New Bank

1. Create a new extractor class in `src/extractors/`
2. Implement required methods from `BaseExtractor`
3. Add bank-specific configuration in `config/banks/`
4. Register new extractor in `ExtractorFactory`

### Customizing Categories

Edit `config/mappings.json` to update transaction categorization rules.
