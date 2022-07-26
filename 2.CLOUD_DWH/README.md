# Summary of the project:

This project contains an ETL pipeline that extracts Sparkify's data from S3, stages them in Redshift, and transform data into a set of dimension & fact tables. 

### - "How-to" run the ETL pipeline:

Run the following commands in the console to

1. (Drop &) Create the database from scratch:

```bash
python create_tables.py
```

2. Insert (or update) data in the DWH (AWS Redshift):

```
python etl.py
```


# Files in the repository:

- ``create_tables.py`` : python script used to drop (if they exist) and create all database tables. This file can be run every time you run the ETL script.


- ``etl.py`` : python script with ETL pipeline. ETL reads and processes every JSON file under ``s3://udacity-dend/song_data`` and ``s3://udacity-dend/log_data``. The content of JSON files is stagged into Redshift and finally inserted into a fact and dimension tables.


- ``sql_queries.py`` : python module with all SQL queries used to interact with the DWH. This includes queries to CREATE database tables, INSERT records, DROP tables and query (SELECT) data.


- ``test_queries.ipynb`` : jupyter notebook used to interactively query the DWH.


- ``README.md`` : markdown file with project overview.


- ``dwh.cfg``: File with AWS access keys (file not available on ``remote``).