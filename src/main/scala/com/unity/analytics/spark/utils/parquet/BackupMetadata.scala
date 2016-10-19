package com.unity.analytics.spark.utils.parquet

import java.sql.DriverManager

import org.apache.spark.Logging
import org.json4s.jackson.Serialization

/*
  This class contains all information needed for a backup.
  It handles writing and reading the backupMetadata to/from a jdbc-compatible database
*/
case class BackupMetadata(
                           backupId: String,
                           backupEntries: Array[BackupEntry]
                         )

case class BackupEntry(
                        srcDir: String,
                        destDir: String,
                        srcNumFiles: Int = 1,
                        destNumFiles: Int = 1
                      )

object BackupMetadata extends Logging {
  val tableName = "backup_metadata"
  implicit val formats = org.json4s.DefaultFormats

  def write(backupId: String, backupEntries: Array[BackupEntry], jdbcConfig: Map[String, String]): Unit = {
    val connection = DriverManager.getConnection(jdbcConfig.get("url").get)
    val backupEntriesJSON = Serialization.write[Array[BackupEntry]](backupEntries)
    val sql = s"""INSERT INTO ${BackupMetadata.tableName} (id, entries) VALUES ('$backupId', '$backupEntriesJSON') ON DUPLICATE KEY UPDATE entries = '$backupEntriesJSON'"""

    try {
      connection.prepareStatement(sql).execute()
    }
    finally {
      connection.close()
    }
  }

  def read(backupId: String, jdbcConfig: Map[String, String]): Option[BackupMetadata] = {
    //Read from MySQL
    val connection = DriverManager.getConnection(jdbcConfig.get("url").get)
    val sql = s"SELECT * FROM $tableName WHERE id = '$backupId'"
    try {
      val results = connection.prepareStatement(sql).executeQuery()
      while (results.next()) {
        val backupEntriesJSON = results.getString("entries")
        val backupEntries = Serialization.read[Array[BackupEntry]](backupEntriesJSON)
        return Some(BackupMetadata(backupId, backupEntries))
      }
    }
    catch {
      case e: Exception => {
        logError(s"Error loading backup BackupMetadata $backupId - ${e.getMessage}")
      }
    }
    finally {
      connection.close()
    }
    None
  }
}


