# SCADA System with Kafka and PySpark

## Overview

This repository contains a project showcasing the integration of a simulated SCADA system with Apache Kafka and PySpark for real-time data processing. The system is orchestrated using Apache Airflow, and Docker is used for containerization and easy deployment.

## Getting Started

### Prerequisites

Make sure you have Docker and Docker Compose installed on your machine.

- [Docker Installation Guide](https://docs.docker.com/get-docker/)
- [Docker Compose Installation Guide](https://docs.docker.com/compose/install/)

### How to Run

1. Clone this repository:

   ```bash
   git clone https://github.com/Ayoub-Talbi1/SCADA-KAFKA-PYSPARK.git
   ```

2. Initialize Airflow:

   ```bash
   docker-compose up airflow-init
   ```

3. Start the entire system:

   ```bash
   docker-compose up -d
   ```

This will bring up the entire environment, including the SCADA simulation, Kafka, Spark, and Airflow.

## Project Structure

- `SCADA-SYS/`: Simulated SCADA system using FastAPI and Pandas.
- `producer.py`: Python script to produce data from the SCADA system to Kafka.
- `consumer.py`: PySpark script for consuming and analyzing data from Kafka in real-time.

## Workflow

1. The SCADA system generates simulated data and exposes it through a FastAPI API.
2. The `producer.py` script fetches data from the SCADA API and produces it to the Kafka topic.
3. Airflow DAG schedules the `producer.py` script to run periodically.
4. Spark Structured Streaming in the `consumer.py` script consumes data from the Kafka topic, applies a schema, and performs real-time analysis.
5. Results of the analysis are displayed in the console and can be further extended for other outputs.

## Additional Notes

- Make sure to tailor the system configuration, such as Kafka server addresses or Spark configurations, according to your environment or specific needs.

Feel free to explore and enhance the project based on your requirements. If you encounter any issues or have suggestions for improvements, please create an issue or submit a pull request. Happy coding!
