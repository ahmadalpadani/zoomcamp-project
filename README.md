# Olist E-Commerce Analytics Pipeline (Zoomcamp-Project)

## Project Overview
This project builds an End-to-End Data Pipeline to analyze the Brazilian E-Commerce Public Dataset by Olist. The primary objective is to transform fragmented transactional data into a clean, performant Star Schema to provide actionable business insights regarding logistics efficiency, seller performance, and customer loyalty.

## Problem Statement
In the rapidly evolving e-commerce landscape, businesses struggle to gain a holistic view of their operations when data is siloed across multiple domains (customers, sellers, payments, and logistics). Specifically for Olist, a major Brazilian department store marketplace, there is a need to understand the relationship between geographic location, delivery performance, and customer satisfaction.

*The Challenge:*
Manual analysis of 100,000 orders across 2016–2018 is inefficient. Stakeholders need an automated, end-to-end pipeline that transforms raw transactional data into actionable insights.

*The Solution:*
This project builds a robust data platform to answer:

- Sales Trends: How do order volumes fluctuate over time across different Brazilian states?
- Logistics Efficiency: Which regions experience the highest freight costs versus delivery speeds?
- Customer Satisfaction: What is the distribution of review scores across the top product categories?

By implementing this pipeline, Olist can identify underperforming regions and optimize their logistics partnership network.

## Architecture & Technologies

- Cloud Platform: Google Cloud Platform (GCP)
- Infrastructure as Code: Terraform
- Workflow Orchestration: Kestra
- Distributed Computing: Apache Spark (Data Processing & Format Conversion)
- Data Lake: Google Cloud Storage (GCS)
- Data Warehouse: BigQuery
- Transformation Layer: dbt (Data Build Tool)
- Visualization: Looker Studio


