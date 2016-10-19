package com.unity.analytics.spark.utils.parquet

import org.rogach.scallop.ScallopConf

class ParquetS3BackupConfiguration(args: Seq[String]) extends ScallopConf(args) {
  val srcDir = opt[String]("src", required = true,
    descr = "Source parquet folder, e.g hdfs://foo")
  val destDir = opt[String]("dest", required = true,
    descr = "Destination  parquet folder, e.g s3a://bar")
  val numFiles = opt[Int]("num", required = true,
    descr = "Destination target number of parquet files (not including meta files)")
  val merge = opt[Boolean]("merge", descr = "pass the flag 'merge' for ParquetS3Backup to use the more efficient coalesce, otherwise this defaults to 'split' which does a partial shuffle via repartition")
}
