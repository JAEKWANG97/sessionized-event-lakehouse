# 샘플 실행 검증

로컬 환경에서 `sample/sample_events.csv`를 대상으로 Spark 애플리케이션을 실행했다.

## 실행 환경

```text
Java: OpenJDK 17.0.19
Spark: 4.1.2
Scala: 2.13.17
sbt project version: 1.10.7
```

## 빌드 검증

```bash
sbt compile
sbt package
```

결과:

```text
compile success
package success
```

## 샘플 실행 명령

```bash
spark-submit \
  --class com.jaekwang.lakehouse.SessionizedEventLakehouseApp \
  target/scala-2.13/sessionized-event-lakehouse_2.13-0.1.0.jar \
  --input sample/sample_events.csv \
  --output data/lake/sessionized_events \
  --start-date 2019-10-01 \
  --end-date 2019-10-03 \
  --run-id sample-001 \
  --database default \
  --table sessionized_events \
  --enable-hive-sync false
```

처음 검증에서는 Hive sync를 끄고 Parquet/Snappy output 생성부터 확인했다.

## 결과 파일

```text
data/lake/sessionized_events/dt=2019-10-01/*.snappy.parquet
data/lake/sessionized_events/dt=2019-10-02/*.snappy.parquet
```

## 검증 쿼리

```sql
SELECT
  dt,
  count(*) AS rows,
  count(DISTINCT user_id) AS users,
  count(DISTINCT generated_session_id) AS sessions
FROM parquet.`data/lake/sessionized_events`
GROUP BY dt
ORDER BY dt;
```

결과:

```text
2019-10-01  6  2  4
2019-10-02  3  1  2
```

전체 검증:

```sql
SELECT
  count(*) AS rows,
  count(DISTINCT user_id) AS users,
  count(DISTINCT generated_session_id) AS sessions,
  count(DISTINCT dt) AS partitions
FROM parquet.`data/lake/sessionized_events`;
```

결과:

```text
rows       = 9
users      = 3
sessions   = 6
partitions = 2
```

샘플 데이터 설계 당시 기대했던 값과 일치한다.
