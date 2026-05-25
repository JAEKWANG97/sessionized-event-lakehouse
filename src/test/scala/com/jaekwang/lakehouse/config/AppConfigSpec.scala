package com.jaekwang.lakehouse.config

import org.scalatest.funsuite.AnyFunSuite

final class AppConfigSpec extends AnyFunSuite {
  test("uses lookback input before target input when building CSV read paths") {
    val config = AppConfig.parse(
      Array(
        "--input",
        "data/raw/2019-Nov.csv,data/raw/2019-Dec.csv",
        "--lookback-input",
        "data/raw/2019-Oct.csv",
        "--output",
        "data/lake/events",
        "--start-date",
        "2019-11-01",
        "--end-date",
        "2019-12-01",
        "--run-id",
        "test-run"
      )
    )

    assert(config.inputPaths == Seq("data/raw/2019-Oct.csv", "data/raw/2019-Nov.csv", "data/raw/2019-Dec.csv"))
    assert(config.repartitionByDt)
  }

  test("allows dt repartitioning to be disabled explicitly") {
    val config = AppConfig.parse(
      Array(
        "--input",
        "data/raw/2019-Nov.csv",
        "--output",
        "data/lake/events",
        "--start-date",
        "2019-11-01",
        "--end-date",
        "2019-12-01",
        "--run-id",
        "test-run",
        "--repartition-by-dt",
        "false"
      )
    )

    assert(config.inputPaths == Seq("data/raw/2019-Nov.csv"))
    assert(!config.repartitionByDt)
  }
}
