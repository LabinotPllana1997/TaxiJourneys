# Taxi Journeys Data Pipeline

The Taxi Journeys Data Pipeline is a Python-based project designed to process and analyse taxi journey data efficiently using Apache Spark. This pipeline consists of five main steps, each handling a specific aspect of the data processing workflow:

1. **Data Import and Conversion**: Raw CSV files containing taxi journey data are imported and converted to Parquet format for optimized storage and processing.
    
2. **Schema Transformation**: The data undergoes schema transformation to ensure consistency across different sources. This step resolves any discrepancies in column names or data types, preparing the data for further processing.
    
3. **Data Validation and Silver Processing**: The transformed data is validated against predefined criteria to ensure data quality. Valid records are saved to the "Silver" stage of the pipeline for further analysis.
    
4. **Deduplication and Further Silver Processing**: Duplicate records are removed from the validated data to ensure data integrity. The deduplicated data is then saved back to the "Silver" stage for additional processing.
    
5. **Data Shaping and Gold Processing**: The processed data is shaped into meaningful insights and saved to the "Gold" stage of the pipeline. This final stage prepares the data for analysis and visualization.
    

## Folder Structure

```
TaxiJourneys/
├── PythonCode/
│   ├── __main__.py
│   ├── __init__.py
│   ├── step1_import_and_convert.py
│   ├── step2_transform_to_consistent_schema.py
│   ├── step3_validate_and_save.py
│   ├── step4_dedupe_and_save.py
│   └── step5_shape_and_save_to_gold.py
├── Tests/
│   ├── __init__.py
│   ├── step1_import_and_convert.py
│   ├── step2_transform_to_consistent_schema.py
│   ├── step3_validate_and_save.py
│   ├── step4_dedupe_and_save.py
│   └── step5_shape_and_save_to_gold.py
├── RawData/
│   ├── yellow_tripdata_2021-01
│   └── green_tripdata_2021-01.csv
└── PipelineData/
      ├── Bronze/
      ├── Silver/
      └── Gold/

```
## Setup

### Requirements

- Python 3.6 or higher
- Apache Spark
- Jupyter Notebook (optional)

### Installation

1. Clone this repository to your local machine:

    `git clone https://github.com/your-username/TaxiJourneys.git`

2. Install the required dependencies using pip:

    `pip install pyspark`

3. Ensure Apache Spark is installed and configured on your system. You can download Spark from the official website and follow the installation instructions provided.

4. Navigate to the project directory:
    
    bashCopy code
    
    `cd TaxiJourneys`
    

## Usage

1. Run the `__main__.py` file within the `PythonCode` folder to execute the entire data pipeline:

    `python PythonCode/__main__.py`
    
2. Alternatively, you can execute each step of the pipeline individually by importing and calling the respective functions from the `PythonCode` package.
    
3. Once the pipeline is complete, analyze the processed data in the `PipelineData/Gold` directory.

4. For testing, you can use the command `pytest -v` for running tests.
    

## Cleaning Up

To rerun the project with fresh data, you can clear the contents within the `PipelineData/Bronze`, `PipelineData/Silver`, and `PipelineData/Gold` folders before running the `__main__.py` file again.