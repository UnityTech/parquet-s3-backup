package com.unity.analytics.spark.utils.parquet

import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
  * Parquet Merging or Splitting from source parquet folder (e.g hdfs://foo) to destination parquet folder (e.g s3a://bar)
  * Merging uses Spark's coalesce - which avoids a shuffle
  * Splitting uses Spark's repartition - which causes a shuffle
  */
object ParquetS3Backup extends Logging{
  implicit val formats = org.json4s.DefaultFormats

  def main(args: Array[String]): Unit = {
    val config = new ParquetS3BackupConfiguration(args)
    val sparkConf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    val sqlContext = new SQLContext(new SparkContext(sparkConf))
    config.merge() match {
      case true => merge(sqlContext, config.srcDir(), config.destDir(), config.numFiles())
      case false => split(sqlContext, config.srcDir(), config.destDir(), config.numFiles())
    }
  }

  
  // Reads, then merges Parquet files and writes to destDir
  def merge(sqlContext: SQLContext, srcDir: String, destDir: String, destNumFiles: Int): Unit = {
    logInfo(s"ParquetS3Backup merge - srcDir: $srcDir, destDir: $destDir, destNumFiles: $destNumFiles")
    sqlContext.read.parquet(srcDir)
      .coalesce(destNumFiles)
      .write.mode(SaveMode.Overwrite).parquet(destDir)
  }

  // Reads, then splits Parquet files and writes to destDir
  def split(sqlContext: SQLContext, srcDir: String, destDir: String, destNumFiles: Int): Unit = {
    logInfo(s"ParquetS3Backup split - srcDir: $srcDir, destDir: $destDir, destNumFiles: $destNumFiles")
    sqlContext.read.parquet(srcDir)
      .repartition(destNumFiles)
      .write.mode(SaveMode.Overwrite).parquet(destDir)
  }

  //  Reads backupMetadata and does a Backup on each srcDir to destDir, to the set number of files
  def backup(sqlContext: SQLContext, backupMetadata: BackupMetadata): Unit = {
    backupMetadata.backupEntries.foreach(backupEntry => {
      if (backupEntry.destNumFiles <= backupEntry.srcNumFiles) {
        merge(sqlContext, backupEntry.srcDir, backupEntry.destDir, backupEntry.destNumFiles)
      } else {
        split(sqlContext, backupEntry.srcDir, backupEntry.destDir, backupEntry.destNumFiles)
      }
    })
  }

  // Reads backupMetadata and restores from destDir to the srcDir, bringing back the original number of files
  def restore(sqlContext: SQLContext, backupMetadata: BackupMetadata): Unit = {
    backupMetadata.backupEntries.foreach(backupEntry => {
      if (backupEntry.srcNumFiles <= backupEntry.destNumFiles) {
        merge(sqlContext, backupEntry.destDir, backupEntry.srcDir, backupEntry.srcNumFiles)
      } else {
        split(sqlContext, backupEntry.destDir, backupEntry.srcDir, backupEntry.srcNumFiles)
      }
    })
  }
}
