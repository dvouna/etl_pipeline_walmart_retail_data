# Walmart Retail Data Pipeline

[![Python Version](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Overview
This project builds an **ETL (Extract, Transform, Load) data pipeline** for Walmart's retail data to analyze supply and demand patterns around the holiday seasons. By cleaning and aggregating data from multiple sources, the pipeline provides insights into seasonal sales trends, helping Walmart optimize inventory and pricing strategies.

## Features
- **Data Extraction**: Reads store sales data from CSV and additional metadata from a Parquet file.
- **Data Transformation**: Cleans missing values, filters for high-revenue sales, and extracts relevant holiday insights.
- **Aggregation**: Computes average weekly sales per month to observe holiday trends.
- **Data Loading**: Saves the cleaned and aggregated data into structured CSV files.
- **Validation**: Ensures data files are correctly created and accessible.

## Installation & Setup 
* **Python 3.8 or higher:** You can download it from [https://www.python.org/downloads/](https://www.python.org/downloads/).
* **Pandas 2.0 or higher:** You can download it from [https://pandas.pydata.org/]

## 
* CSV & Parquet file handling**
- ETL methodology

## Dataset 
The datasets used in this project have been prvided by **Datacamp** [https://datacamp.com/]

## Usage
1. Clone this repository:
   ```bash
   git clone https://github.com/dvouna/etl_pipeline_walmart_retail_data.git

## Output Files
clean_data.csv: Processed sales data with holiday-based filtering.

agg_data.csv: Aggregated monthly sales statistics.

Contributors
**David Victor** - Data Scientist 
