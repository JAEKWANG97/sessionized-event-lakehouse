package com.jaekwang.lakehouse.io

import java.nio.charset.StandardCharsets
import java.time.Instant

import com.jaekwang.lakehouse.config.AppConfig
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

object RunManifestWriter {
  private val ApplicationName = "sessionized-event-lakehouse"

  def writeRunning(spark: SparkSession, config: AppConfig, startedAt: Instant): Unit =
    write(
      spark = spark,
      config = config,
      status = "RUNNING",
      rowCount = None,
      partitions = Seq.empty,
      startedAt = startedAt,
      completedAt = None,
      error = None
    )

  def writeSuccess(
      spark: SparkSession,
      config: AppConfig,
      rowCount: Long,
      partitions: Seq[String],
      startedAt: Instant,
      completedAt: Instant
  ): Unit =
    write(
      spark = spark,
      config = config,
      status = "SUCCESS",
      rowCount = Some(rowCount),
      partitions = partitions,
      startedAt = startedAt,
      completedAt = Some(completedAt),
      error = None
    )

  def writeFailed(
      spark: SparkSession,
      config: AppConfig,
      error: Throwable,
      startedAt: Instant,
      completedAt: Instant
  ): Unit =
    write(
      spark = spark,
      config = config,
      status = "FAILED",
      rowCount = None,
      partitions = Seq.empty,
      startedAt = startedAt,
      completedAt = Some(completedAt),
      error = Some(error)
    )

  private def write(
      spark: SparkSession,
      config: AppConfig,
      status: String,
      rowCount: Option[Long],
      partitions: Seq[String],
      startedAt: Instant,
      completedAt: Option[Instant],
      error: Option[Throwable]
  ): Unit = {
    val manifestDir = new Path(new Path(config.output), "_manifests")
    val manifestPath = new Path(manifestDir, s"${safeFileName(config.runId)}.json")
    val fs = manifestDir.getFileSystem(spark.sparkContext.hadoopConfiguration)

    if (!fs.exists(manifestDir)) {
      fs.mkdirs(manifestDir)
    }

    val stream = fs.create(manifestPath, true)
    try {
      stream.write(renderJson(config, status, rowCount, partitions, startedAt, completedAt, error).getBytes(StandardCharsets.UTF_8))
    } finally {
      stream.close()
    }
  }

  private def renderJson(
      config: AppConfig,
      status: String,
      rowCount: Option[Long],
      partitions: Seq[String],
      startedAt: Instant,
      completedAt: Option[Instant],
      error: Option[Throwable]
  ): String = {
    val fields = Seq(
      "application" -> jsonString(ApplicationName),
      "run_id" -> jsonString(config.runId),
      "status" -> jsonString(status),
      "input" -> jsonString(config.input),
      "output" -> jsonString(config.output),
      "database" -> jsonString(config.database),
      "table" -> jsonString(config.table),
      "start_date" -> jsonString(config.startDate),
      "end_date" -> jsonString(config.endDate),
      "timezone" -> jsonString(config.timezone),
      "session_gap_minutes" -> config.sessionGapMinutes.toString,
      "row_count" -> rowCount.map(_.toString).getOrElse("null"),
      "partitions" -> jsonArray(partitions),
      "started_at" -> jsonString(startedAt.toString),
      "completed_at" -> completedAt.map(value => jsonString(value.toString)).getOrElse("null"),
      "error_class" -> error.map(value => jsonString(value.getClass.getName)).getOrElse("null"),
      "error_message" -> error.map(value => jsonString(Option(value.getMessage).getOrElse(""))).getOrElse("null")
    )

    fields.map { case (key, value) => s"  ${jsonString(key)}: $value" }.mkString("{\n", ",\n", "\n}\n")
  }

  private def jsonArray(values: Seq[String]): String =
    values.sorted.map(jsonString).mkString("[", ", ", "]")

  private def jsonString(value: String): String =
    value.flatMap {
      case '"'  => "\\\""
      case '\\' => "\\\\"
      case '\b' => "\\b"
      case '\f' => "\\f"
      case '\n' => "\\n"
      case '\r' => "\\r"
      case '\t' => "\\t"
      case char if char.isControl => f"\\u${char.toInt}%04x"
      case char => char.toString
    }.prepended('"').appended('"')

  private def safeFileName(value: String): String =
    value.replaceAll("[^A-Za-z0-9._=-]", "_")
}
