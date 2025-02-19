# Fake eCommerce Data Generator

## Overview
This tool is designed to create mock record label eCommerce data that simulates real-world sales of physical music products. It generates fake users, products and orders serving as a data source for building an ETL data pipeline in Google Cloud and creating dashboards with metrics in Google Looker.

## Features

- **Generate Mock Products:** Create products with catalogue numbers, release dates and pricing. Update product prices and active status. Products are generated with popularity weighting that decreases over time.
- **User Profile Generation:** Simulate fake user profiles from multiple locales, including addresses and email addresses.
- **Order Simulation:** Mimic real-world behavior in order generation, including a high ratio of new to returning customers and increased orders during pre-order periods.
- **Data Storage:** Store data in separate tables within Cloud SQL for organized data management.
- **Data Export:** Export data to CSV files in Cloud Storage, with the option for "messy" data that includes missing values and varied date formatting.

## Technologies Used
- Google Cloud Run
- Cloud SQL
- Cloud Storage
- Cloud Secret Manager
- Python
