package com.jaekwang.lakehouse.transform

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Sessionization {
  def transform(raw: DataFrame, sessionGapMinutes: Int, timezone: String, runId: String): DataFrame = {
    val sessionGapSeconds = sessionGapMinutes * 60

    val orderedByUser = Window
      .partitionBy(col("user_id"))
      .orderBy(col("event_time_utc"), col("source_user_session"), col("product_id"))

    val sessionSeqWindow = orderedByUser.rowsBetween(Window.unboundedPreceding, Window.currentRow)

    val withParsedTime = raw
      .withColumn("event_time_utc", to_timestamp(col("event_time"), "yyyy-MM-dd HH:mm:ss 'UTC'"))
      .withColumn("event_time_kst", from_utc_timestamp(col("event_time_utc"), timezone))
      .withColumn("dt", date_format(col("event_time_kst"), "yyyy-MM-dd"))
      .withColumnRenamed("user_session", "source_user_session")
      .drop("event_time")

    val withSessionSeq = withParsedTime
      .withColumn("previous_event_time_utc", lag(col("event_time_utc"), 1).over(orderedByUser))
      .withColumn("gap_seconds", unix_timestamp(col("event_time_utc")) - unix_timestamp(col("previous_event_time_utc")))
      .withColumn(
        "is_new_session",
        when(col("previous_event_time_utc").isNull || col("gap_seconds") >= lit(sessionGapSeconds), lit(1)).otherwise(lit(0))
      )
      .withColumn("session_seq", sum(col("is_new_session")).over(sessionSeqWindow))

    val sessionWindow = Window.partitionBy(col("user_id"), col("session_seq"))
    val eventInSessionWindow = sessionWindow.orderBy(col("event_time_utc"), col("source_user_session"), col("product_id"))

    withSessionSeq
      .withColumn("session_start_at_utc", min(col("event_time_utc")).over(sessionWindow))
      .withColumn("session_start_at_kst", min(col("event_time_kst")).over(sessionWindow))
      .withColumn("session_event_seq", row_number().over(eventInSessionWindow))
      .withColumn("generated_session_id", generatedSessionId)
      .withColumn("ingested_at", current_timestamp())
      .withColumn("run_id", lit(runId))
      .drop("previous_event_time_utc", "gap_seconds", "is_new_session")
      .select(
        col("event_time_utc"),
        col("event_time_kst"),
        col("event_type"),
        col("product_id"),
        col("category_id"),
        col("category_code"),
        col("brand"),
        col("price"),
        col("user_id"),
        col("source_user_session"),
        col("generated_session_id"),
        col("session_seq"),
        col("session_start_at_utc"),
        col("session_start_at_kst"),
        col("session_event_seq"),
        col("ingested_at"),
        col("run_id"),
        col("dt")
      )
  }

  private def generatedSessionId: Column =
    sha2(
      concat_ws(
        "|",
        col("user_id").cast("string"),
        date_format(col("session_start_at_utc"), "yyyy-MM-dd HH:mm:ss"),
        col("session_seq").cast("string")
      ),
      256
    )
}
