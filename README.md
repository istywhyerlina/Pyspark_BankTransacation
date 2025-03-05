# README
## Project Overview

This project is designed to demonstrate the implementation of a data pipeline using Docker and PySpark.

## Prerequisites

Before you begin, ensure you have met the following requirements:
- Docker installed on your local machine

## Setup Instructions

1. Clone the repository to your local machine.
2. Navigate to the project directory.

## Running the Project

To build and run the project, execute the following commands in your terminal:

```bash
docker compose build --no-cache
docker compose up -d
```

After the containers are up and running, if the library and dependency is not instlled. you can install manual:

```bash
docker exec -it pyspark_container pip install sqlalchemy psycopg2-binary
```

To access Jupyter Notebook on the PySpark container, check the container logs to get the token:

```bash
docker logs pyspark_container
```

example
```bash
http://127.0.0.1:8888/lab?token=3fa3b1cf2c67643874054971f23ee59bdee283b373794847
```

## Dataset
1. **Source Database (`source_db` container)**  
   - A PostgreSQL database that contains structured data related to banking transactions.
   - Tables include `customers`, `transactions`, `marketing_campaign_deposit`, and others.

2. **CSV File (`/script/data/new_bank_transaction.csv`)**  
   - A large dataset containing transactional records.
   - Requires transformation before loading into the Data Warehouse.

## Data Pipeline
The scripts for the data pipeline are located in the `/script` folder. 

## Logging
All logs generated during the execution of the data pipeline are saved in the following file:

```plaintext
/script/log/log.info
```

