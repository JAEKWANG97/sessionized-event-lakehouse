# 로컬 실행 환경

이 프로젝트는 로컬 Spark 환경에 맞춰 빌드 버전을 고정한다.

현재 개발 환경에서 확인한 버전은 다음과 같다.

```text
Java: OpenJDK 17.0.19
sbt project version: 1.10.7
sbt runner version: 1.12.11
Spark: 4.1.2
Spark Scala: 2.13.17
```

## build.sbt 버전 선택

처음에는 Spark 3.5.x와 Scala 2.12 조합을 생각했다. Spark 3.x 계열에서는 이 조합이 안정적이기 때문이다.

하지만 현재 로컬에 설치된 `spark-submit`은 Spark 4.1.2이고, Spark 4.1.2는 Scala 2.13.17로 빌드되어 있다.

`spark-submit`으로 실행할 애플리케이션은 Spark 런타임과 Scala binary version을 맞추는 것이 중요하다. 그래서 `build.sbt`도 로컬 Spark에 맞춰 다음처럼 설정한다.

```scala
ThisBuild / scalaVersion := "2.13.17"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "4.1.2" % Provided
)
```

`Provided`를 사용하는 이유는 Spark dependency를 fat jar 안에 넣지 않기 위해서다. 실제 실행 시 Spark 라이브러리는 `spark-submit`이 제공한다.

## 검증 명령

```bash
sbt compile
sbt test
sbt package
```

샘플과 전체 데이터 실행 결과는 `docs/sample-run-verification.md`와
README의 전체 데이터 실행 섹션에 기록했다.
