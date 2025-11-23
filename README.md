### City History Map – Prototype ETL Pipeline

This is a small, self-contained ETL prototype demonstrating work with pandas, SQLAlchemy, and a PostgreSQL schema.

This ETL is part of an ongoing student team project, where I am responsible for the data layer.
The larger project is a web application that visualizes the history of a city on a map.

**As this is the first prototype, it contains no error handling or validation yet - only transformation logic.**
Some patterns could technically be abstracted or extracted, but the structure is intentionally kept simple to accommodate upcoming changes.

The test_batch.csv dataset and the database schema were both created by me.

#### How to run

1. Create a PostgreSQL database using the provided db_create.sql script.
2. Adjust config.py with your own DB credentials.
3. Run etl_v2.py