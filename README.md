# spotify-end-to-end-data-engineering-project

## Introduction
In this project we will build an ETL (Extract, Transform, Load) pipeline using the Spotify API on AWS. The pipeline will retrieve data from Spotify API, transform it to desired format and load into a AWS data store.

## Implement Complete Data Pipeline Data Engineering Project using Spotify

### Architecture
![image](https://github.com/user-attachments/assets/710a9fcd-32da-4f4b-b5b7-8c07f3a9264c)

### About Dataset/ API
The API contains information about music artists, albums and songs [Spotify API](https://developer.spotify.com/documentation/web-api)

### Services Used
1. **Amazon S3 (Simple Storage Service)** is an object storage service that provides scalable, high-speed, and secure storage for any type of data. It is designed to store and retrieve large amounts of data efficiently, making it ideal for backup, archiving, big data analytics, and static website hosting.
2. **AWS Lambda** is a serverless computing service that lets you run code without provisioning or managing servers. It automatically scales and executes code in response to events, charging only for the compute time used.
3. **Amazon CloudWatch** is a monitoring and observability service for AWS resources and applications. It collects and analyzes metrics, logs, and events to provide real-time insights into system performance, resource utilization, and operational health.
4. **AWS Glue Crawler** is a metadata discovery tool that automatically scans and catalogs data stored in S3, RDS, DynamoDB, and other sources. It extracts schema and table definitions and stores them in the AWS Glue Data Catalog, making the data available for querying and transformation.
5. **AWS Glue Data Catalog** is a centralized metadata repository that stores information about databases, tables, and schemas for data stored in AWS services like Amazon S3, RDS, DynamoDB, and Redshift. It enables data discovery, governance, and query optimization for analytics and ETL processes.
6. **Amazon Athena** is a serverless, interactive query service that allows you to analyze data stored in Amazon S3 using SQL. It eliminates the need for complex infrastructure setup, automatically scales queries, and charges only for the data scanned.

   ### install packages
   ```
   pip install pandas
   pip install numpy
   pip install spotipy

   ```

### Project Execution Flow 
Extract Data from API -> Lambda trigger (every 1 hour) -> run extract code -> store raw data -> trigger transform function -> transform data and load it -> Query using Athena
