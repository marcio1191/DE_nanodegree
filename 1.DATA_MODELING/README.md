# Sparkify new music streaming app data

The objective of this project is to store Sparkity's new music streaming app user activity and songs data into a posgres database: ``sparkifydb``.

The end user of the data is the analytics team, who are particularly interested in understanding what songs users are listening to.

### - "How-to" run the ETL pipeline:

Run the following commands in the console to

1. (Drop &) Create the database from scratch:

`python create_tables.py`

2. Insert (or update) data to (in) the database:

`python etl.py`

### - Project files:

- ``./data`` : contains JSON logs with log and song data.

- ``create_tables.py`` : python script used to drop (if they exist) and create all database tables. This file can be run each time you run the ETL scripts.

- ``etl.py`` : python script with ETL pipeline. ETL reads and processes every single file under ``./data/log_data`` and ``./data/song_data`` and loads them into the different tables.

- ``sql_queries.py`` : python module with all SQL queries used to interact with the database. This includes queries to CREATE database tables, INSERT records, DROP tables and query (SELECT) data.

- ``test.ipynb`` : jupyter notebook used to interactively test ``etl.py``. 

- ``etl.ipynb`` : jupyter notebook used when creating etl.py (not required anymore)

- ``README.md`` : markdown file with project overview.

### - Database schema design and ETL pipeline

Database schema and tables are optimized for queries on song play analysis. Therefore, it follows a star model with the following fact and dimension tables:

- Fact table: ``songplays``

- Dimension tables: ``users``, ``songs``, ``artists``, ``time``


An ETL pipeline is used to extract, transform and load data from JSON user activity logs and song metadata to the database using Python and SQL.

### - Example queries and results for song play analysis

a) which song men listen to ?

``%sql \
SELECT DISTINCT(s.title) \
FROM songs s \
JOIN songplays sp \
ON sp.song_id = s.song_id \
JOIN users u \
ON u.user_id = sp.user_id \
WHERE u.gender = 'M';
``


b) What are the most-streamed songs?

``%sql \
SELECT s.title, COUNT(s.title) \
FROM songs s \
JOIN songplays sp \
ON sp.song_id = s.song_id \
GROUP BY 1 \
ORDER BY 1 DESC;
``

| title          | count |
|----------------|-------|
| setanta matins | 1     |