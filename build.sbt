ThisBuild / scalaVersion := "2.13.17"
ThisBuild / organization := "com.jaekwang"
ThisBuild / version := "0.1.0"

lazy val root = (project in file("."))
  .settings(
    name := "sessionized-event-lakehouse",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-sql" % "4.1.2" % Provided,
      "org.scalatest" %% "scalatest" % "3.2.19" % Test
    ),
    Compile / mainClass := Some("com.jaekwang.lakehouse.SessionizedEventLakehouseApp"),
    Test / fork := true,
    Test / javaOptions ++= Seq(
      "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
    )
  )
