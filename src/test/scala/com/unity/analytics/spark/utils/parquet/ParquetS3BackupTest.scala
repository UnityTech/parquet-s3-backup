package com.unity.analytics.spark.utils.parquet

import java.io.File
import java.nio.file.Paths

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SaveMode

class ParquetS3BackupTest extends BaseTest {
  val tmpDir = Paths.get(FileUtils.getTempDirectoryPath, "ParquetS3BackupTest").toString
  val srcNumFiles1 = 20
  val srcNumFiles2 = 40

  val testInput1 = (1 to 100).toList
  val testInput2 = (1 to 300).toList

  var srcDir1 = Paths.get(tmpDir, "src_dir1").toString
  var srcDir2 = Paths.get(tmpDir, "src_dir2").toString
  var destDir1 = Paths.get(tmpDir, "dest_dir1").toString
  var destDir2 = Paths.get(tmpDir, "dest_dir2").toString

  before {
    FileUtils.deleteDirectory(new File(tmpDir))
    // Write test parquet files
    val testRDD1 = sc.parallelize(testInput1).repartition(srcNumFiles1)
    val testRDD2 = sc.parallelize(testInput2).repartition(srcNumFiles2)
    val sqlContext = getSQLContext
    import sqlContext.implicits._
    testRDD1.toDF.write.mode(SaveMode.Overwrite).parquet(srcDir1)
    testRDD2.toDF.write.mode(SaveMode.Overwrite).parquet(srcDir2)
  }

  test("Test merging parquet files") {
    // merge test parquet files
    ParquetS3Backup.merge(sqlContext, srcDir1, destDir1, 2)

    // Verify # of output parquet files match the specified number
    val filesCount = new File(destDir1).list().filter(_.endsWith(".parquet")).length
    assert(2 === filesCount)

    // Verify all items were written
    val writtenOuput = sqlContext.read.parquet(destDir1)
      .map(_.getAs[Int](0))
      .collect().sorted
    assert(testInput1.toArray.deep === writtenOuput.deep)
  }

  test("Test splitting parquet files") {
    // merge test parquet files
    ParquetS3Backup.split(sqlContext, srcDir1, destDir1, 80)

    // Verify # of output parquet files match the specified number
    assert(80 === countParquetFiles(destDir1))

    // Verify all items were written
    val writtenOuput = sqlContext.read.parquet(destDir1)
      .map(_.getAs[Int](0))
      .collect().sorted
    assert(testInput1.toArray.deep === writtenOuput.deep)
  }

  test("Backup and Restore from BackupMetadata") {
    val backupId = "dummyBackupId"
    val destNumFiles1 = 50 // from 20
    val destNumFiles2 = 20 // from 40
    val backupEntries = Array(
        BackupEntry(srcDir1, destDir1, srcNumFiles1, destNumFiles1),
        BackupEntry(srcDir2, destDir2, srcNumFiles2, destNumFiles2)
      )
    val metadata = BackupMetadata(backupId, backupEntries)
    ParquetS3Backup.backup(sqlContext, metadata)

    // Verify # of output parquet files match the specified destNumFiles number
    assert(destNumFiles1 === countParquetFiles(destDir1))
    assert(destNumFiles2 === countParquetFiles(destDir2))

    // Delete source directories and Restore from destination
    FileUtils.deleteDirectory(new File(srcDir1))
    FileUtils.deleteDirectory(new File(srcDir2))

    assert(0 === countParquetFiles(srcDir1))
    assert(0 === countParquetFiles(srcDir2))

    // Verify # of output parquet files match the specified srcNumFiles number
    ParquetS3Backup.restore(sqlContext, metadata)
    assert(srcNumFiles1 === countParquetFiles(srcDir1))
    assert(srcNumFiles2 === countParquetFiles(srcDir2))
  }

  def countParquetFiles(path: String): Int = {
    val fileList = new File(path).list()
    fileList match {
      case null => 0
      case _ => fileList.filter(_.endsWith(".parquet")).length
    }
  }
}
