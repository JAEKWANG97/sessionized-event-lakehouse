package com.jaekwang.lakehouse.io

import java.io.File
import java.net.URI

import com.jaekwang.lakehouse.config.AppConfig
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, collect_set, count, lit, sort_array}

object LakeWriter {
  final case class WriteResult(rowCount: Long, partitions: Seq[String])

  def writePartitioned(df: DataFrame, config: AppConfig): WriteResult = {
    val spark = df.sparkSession
    val outputPath = new Path(config.output)
    val stagingPath = new Path(new Path(config.output, "_staging"), safeFileName(config.runId))
    val versionPath = new Path(new Path(config.output, "_versions"), safeFileName(config.runId))
    val fs = outputPath.getFileSystem(spark.sparkContext.hadoopConfiguration)

    if (config.enableHiveSync && fs.exists(versionPath)) {
      throw new IllegalStateException(
        s"Version path already exists for run_id=${config.runId}: $versionPath. Use a new run_id or clean up the incomplete run."
      )
    }

    deleteIfExists(fs, stagingPath)
    fs.mkdirs(outputPath)

    val outputDf =
      if (config.repartitionByDt) {
        df.repartition(col("dt"))
      } else {
        df
      }

    outputDf.write
      .mode("overwrite")
      .option("compression", "snappy")
      .partitionBy("dt")
      .parquet(stagingPath.toString)

    val staged = spark.read.parquet(stagingPath.toString)
    val result = collectOutputStats(staged)

    if (result.partitions.isEmpty) {
      throw new IllegalStateException("No dt partitions were written to the staging path.")
    }

    if (config.enableHiveSync) {
      publishVersionedPartitions(spark, fs, stagingPath, versionPath, config, result.partitions)
    } else {
      publishFileSystemPartitions(fs, stagingPath, outputPath, result.partitions)
      deleteIfExists(fs, stagingPath)
    }

    result
  }

  private def ensureHiveTable(spark: SparkSession, config: AppConfig): Unit = {
    val tableLocation = toLocationUri(config.output)

    spark.sql(s"CREATE DATABASE IF NOT EXISTS ${config.database}")
    spark.sql(
      s"""
         |CREATE EXTERNAL TABLE IF NOT EXISTS ${config.database}.${config.table} (
         |  event_time_utc timestamp,
         |  event_time_kst timestamp,
         |  event_type string,
         |  product_id bigint,
         |  category_id bigint,
         |  category_code string,
         |  brand string,
         |  price decimal(18,2),
         |  user_id bigint,
         |  source_user_session string,
         |  generated_session_id string,
         |  session_seq bigint,
         |  session_start_at_utc timestamp,
         |  session_start_at_kst timestamp,
         |  session_event_seq bigint,
         |  ingested_at timestamp,
         |  run_id string
         |)
         |PARTITIONED BY (dt string)
         |STORED AS PARQUET
         |LOCATION '$tableLocation'
         |""".stripMargin
    )
  }

  private def toLocationUri(path: String): String = {
    val uri = new URI(path)
    if (uri.getScheme != null) {
      path
    } else {
      new File(path).getAbsoluteFile.toURI.toString
    }
  }

  private def collectOutputStats(df: DataFrame): WriteResult = {
    val row = df
      .agg(
        count(lit(1)).as("row_count"),
        sort_array(collect_set(col("dt").cast("string"))).as("partitions")
      )
      .first()

    WriteResult(row.getLong(0), row.getSeq[String](1))
  }

  private def publishVersionedPartitions(
      spark: SparkSession,
      fs: FileSystem,
      stagingPath: Path,
      versionPath: Path,
      config: AppConfig,
      partitions: Seq[String]
  ): Unit = {
    ensureHiveTable(spark, config)

    if (!fs.rename(stagingPath, versionPath)) {
      throw new IllegalStateException(s"Failed to promote staging path $stagingPath to version path $versionPath")
    }

    partitions.foreach { dt =>
      val partitionName = s"dt=$dt"
      val versionedPartition = new Path(versionPath, partitionName)

      if (!fs.exists(versionedPartition)) {
        throw new IllegalStateException(s"Missing versioned partition: $versionedPartition")
      }

      val partitionLocation = toLocationUri(versionedPartition.toString)
      val tableName = s"${config.database}.${config.table}"
      val partitionSpec = s"dt='${escapeSqlString(dt)}'"

      spark.sql(s"ALTER TABLE $tableName ADD IF NOT EXISTS PARTITION ($partitionSpec) LOCATION '$partitionLocation'")
      spark.sql(s"ALTER TABLE $tableName PARTITION ($partitionSpec) SET LOCATION '$partitionLocation'")
    }
  }

  private def publishFileSystemPartitions(
      fs: FileSystem,
      stagingPath: Path,
      outputPath: Path,
      partitions: Seq[String]
  ): Unit = {
    partitions.foreach { dt =>
      val partitionName = s"dt=$dt"
      val stagedPartition = new Path(stagingPath, partitionName)
      val finalPartition = new Path(outputPath, partitionName)

      if (!fs.exists(stagedPartition)) {
        throw new IllegalStateException(s"Missing staged partition: $stagedPartition")
      }

      deleteIfExists(fs, finalPartition)

      if (!fs.rename(stagedPartition, finalPartition)) {
        throw new IllegalStateException(s"Failed to move staged partition $stagedPartition to $finalPartition")
      }
    }
  }

  private def deleteIfExists(fs: FileSystem, path: Path): Unit = {
    if (fs.exists(path)) {
      fs.delete(path, true)
    }
  }

  private def safeFileName(value: String): String =
    value.replaceAll("[^A-Za-z0-9._=-]", "_")

  private def escapeSqlString(value: String): String =
    value.replace("'", "''")
}
