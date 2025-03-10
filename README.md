# Electric Vehicle Charging Stations Data Pipeline

A PySpark-based data ingestion pipeline that processes electric vehicle charging stations data, performs data quality checks, and applies appropriate transformations.

## Project Overview

This pipeline is designed to process CSV data containing information about electric vehicle charging stations (specifically from Electra), validate the data quality, transform it into a more usable format, and generate analytics.

## Project Structure

```
data_ingestion_pipeline/
├── config/                       # Configuration files
│   ├── pipeline_config.yaml      # General pipeline configuration
│   └── electra_pipeline_config.yaml  # Electra-specific configuration
├── data/                         # Data directories
│   ├── raw/                      # Raw input CSV files
│   │   └── bornes-de-recharge-pour-vehicules-electriques-irve-electra.csv
│   ├── processed/                # Intermediate processed data
│   └── output/                   # Final output data
│       └── electra_processed/    # Processed Electra data in Parquet format
├── src/                          # Source code
│   ├── ingestion/                # Data ingestion modules
│   ├── quality_checks/           # Data quality validation modules
│   ├── transformation/           # Data transformation modules
│   └── utils/                    # Utility functions
├── tests/                        # Unit and integration tests
├── requirements.txt              # Project dependencies
├── main.py                       # General entry point
├── run_electra_pipeline.py       # Electra-specific pipeline script
└── setup.py                      # Package setup file
```

## Features

- CSV file ingestion with configurable options
- Data quality checks:
  - Null value detection in required fields
  - Duplicate ID detection
  - Date format validation
  - Value range validation
- Data transformations:
  - Coordinate extraction (latitude/longitude)
  - Date type conversion
  - Charging type classification
  - Power category classification
  - City extraction from addresses
- Analytics generation:
  - Stations by power category
  - Stations by charging type
  - Top cities by number of stations

## Setup

1. Create a virtual environment:
```
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```
pip install -r requirements.txt
```

3. Install the package in development mode:
```
pip install -e .
```

## Usage

### Running the Electra Pipeline

```
python run_electra_pipeline.py
```

This will:
1. Read the Electra charging stations CSV file
2. Perform data quality checks
3. Transform the data
4. Save the processed data as Parquet files
5. Generate and display basic analytics

### Configuration

The pipeline behavior can be customized by modifying the YAML configuration files in the `config/` directory:

- `electra_pipeline_config.yaml`: Configuration specific to the Electra charging stations pipeline

## Output

The processed data is saved in Parquet format in the `data/output/electra_processed/` directory. This format is optimized for:

- Efficient storage (compressed)
- Fast querying
- Schema preservation
- Compatibility with big data tools

## Data Transformations

The pipeline applies the following transformations to the raw data:

1. **Coordinate Extraction**: Parses the `coordonneesXY` field to extract latitude and longitude
2. **Date Conversion**: Converts string dates to proper date types
3. **Charging Type Classification**: Creates a `charging_type` field based on available plug types
4. **Power Categorization**: Classifies stations as Low/Medium/High based on nominal power
5. **City Extraction**: Extracts city names from the station address
6. **Timestamp Addition**: Adds processing timestamp for data lineage

## Analytics

The pipeline generates the following analytics:

- Distribution of charging stations by power category
- Distribution of charging stations by charging type
- Top 10 cities by number of charging stations