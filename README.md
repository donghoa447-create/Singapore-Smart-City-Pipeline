# Singapore Smart City Real-Time Traffic Intelligence Pipeline

An end-to-end real-time data engineering project that integrates
traffic, weather, incident, and simulated IoT data to produce
analysis-ready metrics for congestion monitoring.

## Project Goal

Build a scalable real-time data pipeline that:

- detects developing traffic congestion
- monitors whether traffic conditions are improving or worsening
- integrates contributing conditions such as rainfall, incidents,
  abnormal traffic volume, and illegal stopping
- produces data that can be queried and visualized for traffic monitoring

## Planned Architecture

Data Sources
→ Python Producers
→ Apache Kafka
→ Apache Spark Structured Streaming
→ Amazon S3
→ AWS Glue / Athena
→ Amazon Redshift
→ Power BI
