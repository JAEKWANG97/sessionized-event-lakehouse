package com.jaekwang.lakehouse.schema

import org.apache.spark.sql.types._

object EventSchema {
  val rawCsvSchema: StructType = StructType(
    Seq(
      StructField("event_time", StringType, nullable = false),
      StructField("event_type", StringType, nullable = true),
      StructField("product_id", LongType, nullable = true),
      StructField("category_id", LongType, nullable = true),
      StructField("category_code", StringType, nullable = true),
      StructField("brand", StringType, nullable = true),
      StructField("price", DecimalType(18, 2), nullable = true),
      StructField("user_id", LongType, nullable = false),
      StructField("user_session", StringType, nullable = true)
    )
  )
}
