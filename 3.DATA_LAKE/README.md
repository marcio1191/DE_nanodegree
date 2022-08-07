# Summary of the project:

This project contains an ELT pipeline that extracts Sparkify's data from S3, transforms them using pyspark and saves parquet tables back to S3.

### - "How-to" run the ELT pipeline:

Run the following commands in the EMR cluster:

```bash
spark-submit etl.py
```

# Files in the repository:

- `etl.py` : python script with ELT pipeline. ELT reads and processes every JSON file under `s3://udacity-dend/song_data` and `s3://udacity-dend/log_data`. The content of JSON files transformed with pyspark and finally parquet files with are written back to `s3://sparkify-tables-marcio` bucket.

- `README.md` : markdown file with project overview.

- `dl.cfg`: File with AWS access keys (file not available on `remote`).
