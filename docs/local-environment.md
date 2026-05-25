# 로컬 실행 환경

이 프로젝트는 로컬에서 `spark-submit`으로 직접 실행해 검증했습니다. Spark 애플리케이션은
컴파일할 때의 Scala binary version과 실제 실행할 Spark 런타임의 Scala version이 맞아야 하므로,
로컬 Spark 환경을 기준으로 빌드 버전을 정했습니다.

## 확인한 버전

```text
Java: OpenJDK 17.0.19
sbt project version: 1.10.7
sbt runner version: 1.12.11
Spark: 4.1.2
Spark Scala: 2.13.17
```

## build.sbt 버전 선택

처음에는 Spark 3.5.x와 Scala 2.12 조합도 생각할 수 있습니다. Spark 3.x 계열에서는 이 조합이
흔하기 때문입니다. 하지만 이 프로젝트는 로컬에 설치된 `spark-submit`으로 실행할 예정이었고,
현재 Spark 4.1.2는 Scala 2.13.17로 빌드되어 있습니다.

그래서 `build.sbt`도 실행 환경에 맞춰 다음처럼 설정했습니다.

```scala
ThisBuild / scalaVersion := "2.13.17"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "4.1.2" % Provided
)
```

`Provided`를 사용하는 이유는 Spark dependency를 jar 안에 함께 넣지 않기 위해서입니다. 실제 실행
시 Spark 라이브러리는 `spark-submit` 런타임에서 제공됩니다.

## 검증 명령

```bash
sbt compile
sbt test
sbt package
```

샘플 실행 결과는 `docs/sample-run-verification.md`에, 전체 데이터 실행 결과는 README의
`전체 데이터 실행` 섹션에 기록했습니다.
