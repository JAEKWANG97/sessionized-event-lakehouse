package com.jaekwang.lakehouse.config

final case class AppConfig(
    input: String,
    output: String,
    startDate: String,
    endDate: String,
    runId: String,
    database: String,
    table: String,
    sessionGapMinutes: Int = 5,
    timezone: String = "Asia/Seoul",
    enableHiveSync: Boolean = true
)

object AppConfig {
  def parse(args: Array[String]): AppConfig = {
    val values = args.toList
      .sliding(2, 2)
      .collect { case key :: value :: Nil if key.startsWith("--") => key.drop(2) -> value }
      .toMap

    def required(name: String): String =
      values.getOrElse(name, throw new IllegalArgumentException(s"Missing required argument: --$name"))

    AppConfig(
      input = required("input"),
      output = required("output"),
      startDate = required("start-date"),
      endDate = required("end-date"),
      runId = required("run-id"),
      database = values.getOrElse("database", "default"),
      table = values.getOrElse("table", "sessionized_events"),
      sessionGapMinutes = values.get("session-gap-minutes").map(_.toInt).getOrElse(5),
      timezone = values.getOrElse("timezone", "Asia/Seoul"),
      enableHiveSync = values.get("enable-hive-sync").forall(_.toBoolean)
    )
  }
}
