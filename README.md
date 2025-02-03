# Data Pipeline for Premier League Analysis

## Overview
This project implements a data pipeline for processing and analyzing Premier League data. The pipeline involves extracting data from the web, storing it in a MySQL database, processing it with Pandas, storing intermediate data in MinIO, and loading it into PostgreSQL for further transformation using dbt. The final data is visualized using Streamlit.

## Technologies Used
- **MySQL**: Stores raw extracted data.
- **Pandas**: Processes and transforms data.
- **MinIO**: Provides object storage for intermediate data.
- **PostgreSQL**: Stores structured data for analysis.
- **dbt**: Transforms data in PostgreSQL.
- **Streamlit**: Visualizes the final processed data.
- **Docker**: Containerizes the entire pipeline.
- **Dagster**: Orchestrates the ETL workflow.

## Data Flow
![image](https://github.com/user-attachments/assets/ad747436-c1fc-4643-b447-db21ffcf9297)

## Project Directory Structure
![image](https://github.com/user-attachments/assets/b8618e65-04c3-4a7b-acc7-5b2204ab2a68)

## Docker 
![image](https://github.com/user-attachments/assets/34bbaaad-c859-43f1-801f-09947e5b5c41)

## Visualiztion
![image](https://github.com/user-attachments/assets/a904cbcc-89e4-484b-9d42-e5325d8c2533)

## Conclusion
This project demonstrates an end-to-end data pipeline for processing and analyzing Premier League data. By leveraging modern data tools such as MySQL, Pandas, MinIO, PostgreSQL, dbt, and Streamlit, we ensure efficient data handling, transformation, and visualization.

By using Docker and Dagster, we provide a scalable and maintainable workflow that can be easily deployed and monitored. The integration of dbt enables data modeling and transformation, ensuring high-quality analytics for decision-making
