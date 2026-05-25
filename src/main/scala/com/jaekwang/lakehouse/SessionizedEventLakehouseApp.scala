package com.jaekwang.lakehouse

import java.time.Instant

import com.jaekwang.lakehouse.config.AppConfig
import com.jaekwang.lakehouse.io.{LakeWriter, RunManifestWriter}
import com.jaekwang.lakehouse.schema.EventSchema
import com.jaekwang.lakehouse.transform.Sessionization
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import scala.util.control.NonFatal

object SessionizedEventLakehouseApp {
  def main(args: Array[String]): Unit = {
    val config = AppConfig.parse(args)
    val startedAt = Instant.now()

    val spark = SparkSession
      .builder()
      .appName("sessionized-event-lakehouse")
      .enableHiveSupport()
      .getOrCreate()

    spark.conf.set("spark.sql.session.timeZone", "UTC")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    try {
      RunManifestWriter.writeRunning(spark, config, startedAt)

      val rawEvents = spark.read
        .option("header", "true")
        .schema(EventSchema.rawCsvSchema)
        .csv(config.input)

      val sessionizedEvents = Sessionization
        .transform(rawEvents, config.sessionGapMinutes, config.timezone, config.runId)
        .filter(col("dt") >= config.startDate && col("dt") < config.endDate)

      val writeResult = LakeWriter.writePartitioned(sessionizedEvents, config)

      if (config.enableHiveSync) {
        LakeWriter.syncHiveTable(spark, config)
      }

      RunManifestWriter.writeSuccess(spark, config, writeResult.rowCount, writeResult.partitions, startedAt, Instant.now())
    } catch {
      case NonFatal(error) =>
        try {
          RunManifestWriter.writeFailed(spark, config, error, startedAt, Instant.now())
        } catch {
          case NonFatal(manifestError) =>
            System.err.println(s"Failed to write FAILED manifest for run_id=${config.runId}: ${manifestError.getMessage}")
        }
        throw error
    } finally {
      spark.stop()
    }
  }
}
