# spark analytical ETL miniproject

### Objective: using SparkSQL and Python to build an ETL job that calculates the following results for a given day:
- Latest trade price before the quote.
- Latest 30-minute moving average trade price, before the quote.
- The bid/ask price movement from previous day’s closing price.

### Design steps
- spark will read the parquet files based on trade date for current date from the HDFS folder
- After calculating the average moving price for last 30 minutes the result will be written to a temp hive table
- similarly the average moving price for last 30 minutes for last day is calculated and written to a temp hive table
- The quotes data are loaded from the HDFS folder to a dataframe
- The quotes data is then joined with the two temp tables and finally the results are written as parquet files to a HDFS folder
