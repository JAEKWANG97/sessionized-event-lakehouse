package com.jaekwang.lakehouse.transform

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.date_format
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

final class SessionizationSpec extends AnyFunSuite with BeforeAndAfterAll {
  import SessionizationSpec.RawEvent

  private var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .appName("sessionization-spec")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .config("spark.sql.session.timeZone", "UTC")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  test("splits sessions on gap >= 5 minutes within each user") {
    val localSpark = spark
    import localSpark.implicits._

    val raw = Seq(
      RawEvent("2019-10-01 00:00:00 UTC", "view", 1001L, 10L, Some("electronics.phone"), Some("brand-a"), BigDecimal("100.00"), 1L, Some("source-a")),
      RawEvent("2019-10-01 00:04:59 UTC", "view", 1002L, 10L, Some("electronics.phone"), Some("brand-a"), BigDecimal("100.00"), 1L, Some("source-b")),
      RawEvent("2019-10-01 00:09:59 UTC", "cart", 1003L, 10L, Some("electronics.phone"), Some("brand-a"), BigDecimal("100.00"), 1L, Some("source-a")),
      RawEvent("2019-10-01 00:02:00 UTC", "view", 2001L, 20L, None, None, BigDecimal("50.00"), 2L, Some("source-c"))
    ).toDF()

    val rows = transform(raw)

    val userOne = rows.filter(_("user_id") == 1L).sortBy(_("product_id").asInstanceOf[Long])
    val first = userOne(0)
    val second = userOne(1)
    val third = userOne(2)

    assert(first("session_seq") == 1L)
    assert(second("session_seq") == 1L)
    assert(third("session_seq") == 2L)
    assert(first("generated_session_id") == second("generated_session_id"))
    assert(first("generated_session_id") != third("generated_session_id"))
    assert(second("source_user_session") == "source-b")

    val userTwo = rows.filter(_("user_id") == 2L)
    assert(userTwo.size == 1)
    assert(userTwo.head("session_seq") == 1L)
  }

  test("creates KST dt from UTC event_time") {
    val localSpark = spark
    import localSpark.implicits._

    val raw = Seq(
      RawEvent("2019-09-30 15:30:00 UTC", "view", 3001L, 30L, None, None, BigDecimal("10.00"), 3L, Some("source-kst"))
    ).toDF()

    val row = transform(raw).head

    assert(row("dt") == "2019-10-01")
  }

  test("keeps generated_session_id stable when earlier history changes session_seq") {
    val localSpark = spark
    import localSpark.implicits._

    val targetOnly = Seq(
      RawEvent("2019-11-01 00:00:00 UTC", "view", 7002L, 70L, None, None, BigDecimal("10.00"), 7L, Some("target"))
    ).toDF()

    val withHistory = Seq(
      RawEvent("2019-10-31 23:00:00 UTC", "view", 7001L, 70L, None, None, BigDecimal("10.00"), 7L, Some("history")),
      RawEvent("2019-11-01 00:00:00 UTC", "view", 7002L, 70L, None, None, BigDecimal("10.00"), 7L, Some("target"))
    ).toDF()

    val targetOnlyRow = transform(targetOnly).find(_("product_id") == 7002L).get
    val withHistoryRow = transform(withHistory).find(_("product_id") == 7002L).get

    assert(targetOnlyRow("session_seq") == 1L)
    assert(withHistoryRow("session_seq") == 2L)
    assert(targetOnlyRow("generated_session_id") == withHistoryRow("generated_session_id"))
  }

  test("allows lookback events to define a session that starts before the published dt") {
    val localSpark = spark
    import localSpark.implicits._

    val raw = Seq(
      RawEvent("2019-10-31 14:58:00 UTC", "view", 8001L, 80L, None, None, BigDecimal("10.00"), 8L, Some("lookback")),
      RawEvent("2019-10-31 15:01:00 UTC", "view", 8002L, 80L, None, None, BigDecimal("10.00"), 8L, Some("target"))
    ).toDF()

    val row = Sessionization
      .transform(raw, sessionGapMinutes = 5, timezone = "Asia/Seoul", runId = "test-run")
      .filter($"dt" >= "2019-11-01" && $"dt" < "2019-11-02")
      .select(
        $"product_id",
        $"session_event_seq",
        date_format($"session_start_at_kst", "yyyy-MM-dd HH:mm:ss").as("session_start_at_kst")
      )
      .head()

    assert(row.getAs[Long]("product_id") == 8002L)
    assert(row.getAs[Int]("session_event_seq") == 2)
    assert(row.getAs[String]("session_start_at_kst") == "2019-10-31 23:58:00")
  }

  private def transform(raw: DataFrame): Seq[Map[String, Any]] =
    Sessionization
      .transform(raw, sessionGapMinutes = 5, timezone = "Asia/Seoul", runId = "test-run")
      .select("user_id", "product_id", "source_user_session", "session_seq", "generated_session_id", "session_event_seq", "dt")
      .collect()
      .map(rowToMap)
      .toSeq

  private def rowToMap(row: Row): Map[String, Any] =
    row.schema.fieldNames.map(name => name -> row.getAs[Any](name)).toMap

}

private object SessionizationSpec {
  final case class RawEvent(
      event_time: String,
      event_type: String,
      product_id: Long,
      category_id: Long,
      category_code: Option[String],
      brand: Option[String],
      price: BigDecimal,
      user_id: Long,
      user_session: Option[String]
  )
}
