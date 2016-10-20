Parquet S3 Backup
========
Parquet Merging, Splitting and Saving using Apache Spark to S3 (or other Spark Supported FileSystems)

ParquetS3Backup supports
* Merging - combine many parquet files to fewer parquet files
* Splitting - split fewer parquet files to many parquet files
* Backup - from a BackupManifest, save parquet files from src directory to destination directory
* Restore - from a BackupManifest, restore parquet files from destination directory to src directory

Building
========
Run ```mvn install``` from the root directory

Running via Command-Line
========
Simply call spark-submit with the necessary parameters
```spark-submit --class com.unity.analytics.spark.utils.parquet.ParquetS3Backup parquet-s3-backup-jar-with-dependencies.jar --src /source_folder --dest /dest_folder --num 2```



