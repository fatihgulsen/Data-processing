
# Data Processing

This repository contains Python scripts and modules for processing and analyzing large datasets. The project leverages technologies such as Apache Spark, PostgreSQL, and other data processing tools.

## Table of Contents
- [Data Processing](#data-processing)
  - [Table of Contents](#table-of-contents)
  - [Project Overview](#project-overview)
  - [Getting Started](#getting-started)
    - [Prerequisites](#prerequisites)
    - [Installation](#installation)
  - [Usage](#usage)
  - [Directory Structure](#directory-structure)
- [Data Processing Flow](#data-processing-flow)
  - [Overview](#overview)
  - [Incoming Data](#incoming-data)
  - [Data Processing Workflow](#data-processing-workflow)
    - [1. Workflow Management with Apache Airflow](#1-workflow-management-with-apache-airflow)
    - [2. Data Processing with Apache Spark](#2-data-processing-with-apache-spark)
    - [Processed Data Handling](#processed-data-handling)
      - [2.1 Loading Processed Data into PostgreSQL](#21-loading-processed-data-into-postgresql)
      - [2.2 Transferring Data to ElasticSearch](#22-transferring-data-to-elasticsearch)
    - [Unprocessed Data Handling](#unprocessed-data-handling)
      - [2.3 Notification of Incomplete Data](#23-notification-of-incomplete-data)
      - [2.4 Remediation Actions](#24-remediation-actions)
  - [Conclusion](#conclusion)
  - [Contributing](#contributing)
  - [License](#license)

## Project Overview
The Data Processing project is designed to handle country-specific data processing tasks. It includes scripts to read data from PostgreSQL, process it using Apache Spark, and store the results back to PostgreSQL or other storage systems.

## Getting Started
To get a copy of this project up and running on your local machine, follow these steps.

### Prerequisites
- Python 3.x
- Apache Spark
- PostgreSQL

### Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/fatihgulsen/Data-processing.git
   cd Data-processing
   ```
2. Create and activate a virtual environment:
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # On Windows use `venv\Scripts\activate`
   ```
3. Install the dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage
1. Configure your PostgreSQL connection in the `config.py` file.
2. Run the data processing script:
   ```bash
   python process_data.py
   ```

## Directory Structure
```
Data-processing/
│
├── data/                   # Data files
├── src/                    # Source files
│   ├── __init__.py
│   ├── process_data.py     # Main script for data processing
│   └── config.py           # Configuration file for database connections
│
├── tests/                  # Test files
│
└── requirements.txt        # Python dependencies
```

# Data Processing Flow

## Overview
This document outlines the steps involved in our data processing workflow. The workflow is designed to efficiently process incoming data from various sources and store the processed data in a PostgreSQL database. Additionally, the workflow handles incomplete data and ensures that necessary actions are taken to address any issues.

## Incoming Data
- **Source**: Data typically comes in formats such as Excel, AccessDB, and CSV files.
- **Action**: These data files are directly loaded into the PostgreSQL database for initial storage.

## Data Processing Workflow

### 1. Workflow Management with Apache Airflow
Apache Airflow is used to manage the overall workflow. It orchestrates the sequence of tasks, ensuring that each step is executed in the correct order and handling any dependencies between tasks.

### 2. Data Processing with Apache Spark
Apache Spark is utilized for data processing due to its powerful capabilities in handling large datasets. The processing tasks in Spark include cleaning, transformation, and aggregation of data. The outcome of the processing step is divided into two categories:
- **Processed Data**: Cleaned and transformed data ready for further use.
- **Unprocessed Data**: Data that could not be fully processed due to issues such as missing values or format errors.

### Processed Data Handling
#### 2.1 Loading Processed Data into PostgreSQL
The processed data is reloaded into the PostgreSQL database. This step ensures that the cleaned and transformed data is available for querying and analysis.

#### 2.2 Transferring Data to ElasticSearch
Processed data is also transferred to ElasticSearch for indexing and search purposes. A script facilitates this transfer via FTP, ensuring that the data is available for fast search and retrieval operations.

### Unprocessed Data Handling
#### 2.3 Notification of Incomplete Data
In cases where data cannot be fully processed, notifications are sent via email to the relevant personnel. This step ensures that stakeholders are aware of any issues that need attention.

#### 2.4 Remediation Actions
A script is used to perform necessary actions on the unprocessed data. This may include cleaning, transformation, or enrichment to address the issues identified during the initial processing step.

## Conclusion
This data processing workflow ensures that incoming data is efficiently processed, stored, and made available for analysis and search operations. By handling both processed and unprocessed data, the workflow maintains data quality and provides timely notifications for any issues that require further attention.


## Contributing
Contributions are welcome! Please read the [CONTRIBUTING](CONTRIBUTING.md) file for guidelines on how to contribute to this project.

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
