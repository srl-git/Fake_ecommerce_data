# Fake eCommerce Data Generator

## Overview
This project is an exercise in generating semi-randomised but realistic sales data. The eCommerce Data Generator creates mock users, products, and orders that simulate real-world record label physical music sales. The data is output to daily CSV reports and made available through an API endpoint.
The generated data serves as a foundation for building an [ETL data pipeline in Google Cloud](https://github.com/srl-git/ecommerce_data_pipeline) and developing [a dashboard with key metrics in Google Looker](https://lookerstudio.google.com/reporting/97af44b1-e064-4d94-a599-be2af56907d6/page/pWi5E). 


## Features

- **Generate Mock Products:** Create products with catalogue numbers, release dates and pricing. Products are generated with randomised popularity weighting that decreases over time.
- **User Profile Generation:** Simulate fake user profiles from multiple locales, including addresses and email addresses.
- **Order Simulation:** Mimic real-world behavior in order generation, including a high ratio of new to returning customers and increased orders during pre-order periods.
- **Data Storage:** Store data in separate tables within a PostgreSQL Cloud SQL database.
- **Data Export:** Export data to CSV files in Cloud Storage, with the option for "messy" data that includes missing values and varied date formatting.
- **API Access:** Retrieve data via an API endpoint running on Google Cloud Run.

## Technologies Used

##### Python
 - FastAPI/Uvicorn
 - SQLAlchemy
 - Faker

##### Project Tooling
 - Makefile
 - YAML
 - Docker

##### Google Cloud
- Cloud SQL
- Cloud Storage
- Cloud Build
- Cloud Run
- Cloud Logging
- Cloud Secret Manager