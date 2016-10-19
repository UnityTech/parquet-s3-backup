package com.unity.analytics.spark.utils.parquet

import java.io.{File, StringReader}
import java.sql.DriverManager

import org.apache.commons.io.FileUtils
import org.h2.tools.{RunScript, Server}

class BackupMetadataTest extends BaseTest {

  val dbDriver = "org.h2.Driver"
  val server = Server.createTcpServer("-tcpPort", "9999")
  val jdbcDB = "dummydb"
  val jdbcUrl = s"jdbc:h2:mem:$jdbcDB;DATABASE_TO_UPPER=FALSE;MODE=MYSQL"
  val jdbcConfig = Map(
    "url" -> jdbcUrl
  )
  val backendConnection = DriverManager.getConnection(jdbcUrl)

  val backupId = "dummyBackupId"

  override def beforeAll(): Unit = {
    super.beforeAll()
    println("INITIALIZE H2")
    // Initialize H2
    val reader = new StringReader(FileUtils.readFileToString(new File("src/test/scala/resources/backup_metadata.sql")).replace('`', '"'))
    RunScript.execute(backendConnection, reader)
  }

  test("Test reading and writing backup metadata") {
    val entries = Array(
      BackupEntry("hdfs://hello", "s3a://hello", 100, 10),
      BackupEntry("/source", "/destination", 200, 2)
    ).sortBy(_.destDir)
    BackupMetadata.write(backupId, entries, jdbcConfig)
    val metadata = BackupMetadata.read(backupId, jdbcConfig).get
    assert(metadata.backupId === backupId)
    assert(metadata.backupEntries.length === entries.length)
    val output = metadata.backupEntries.sortBy(_.destDir)
    entries.zip(output).foreach(e => {
      assert(e._1 === e._2)
    })
  }
}
