# Sessionized Event Lakehouse

DE 개발 사전 과제 제출용 프로젝트입니다.

이 프로젝트는 ecommerce activity 로그를 Spark 애플리케이션으로 읽고,
동일 `user_id` 내 이벤트 간격이 5분 이상이면 새로운 세션으로 판단하여
세션 ID를 생성합니다. 결과는 KST 기준 일자 partition으로 나누어
Parquet/Snappy 형식으로 저장하고, Hive external table에서 조회할 수 있게
설계했습니다.

## 과제 요구사항 정리

입력 데이터:

- `2019-Oct.csv`
- `2019-Nov.csv`
- Kaggle: Ecommerce behavior data from multi category store

구현 요구사항:

- 사용자 activity 로그를 Hive table로 제공하기 위한 Spark Application 작성
- KST 기준 daily partition 처리
- 동일 `user_id` 내에서 `event_time` 간격이 5분 이상이면 새 세션 ID 생성
- 재처리 후 Parquet/Snappy 처리
- External Table 방식 설계
- 추가 기간 처리에 대응 가능하도록 구현
- 배치 장애 시 복구를 위한 장치 구현 또는 설계
- Hive external table을 이용한 WAU 계산
- `user_id` 기준 WAU 계산
- 생성된 session ID 기준 WAU 계산
- 계산에 사용한 쿼리와 결과값 제출
- Spark Application 구현 언어는 Scala 또는 Java 사용

## 요구사항 대응 요약

| 요구사항 | 반영 내용 |
|---|---|
| Spark Application 작성 | Scala 기반 Spark 애플리케이션 구현 |
| KST 기준 daily partition | UTC `event_time`을 KST로 변환한 뒤 `dt` partition 생성 |
| 5분 이상 gap 기준 세션화 | `user_id`별 이전 이벤트와 현재 이벤트의 시간 차이를 계산해 새 session 판단 |
| 새 세션 ID 생성 | `generated_session_id`를 deterministic hash로 생성 |
| Parquet/Snappy 처리 | `dt` 기준 Parquet/Snappy partition 저장 |
| External Table 설계 | Spark output path를 바라보는 Hive external table 생성 |
| 추가 기간 처리 | 입력 경로와 기간을 실행 인자로 받고, 실행 결과에 포함된 `dt` partition만 추가 또는 교체 |
| 배치 장애 복구 장치 | run manifest, 임시 저장 경로, 검증 후 최종 partition 반영 구조 구현 |
| WAU 계산 | Hive external table을 이용해 `user_id`, `generated_session_id` 기준 주간 distinct count 계산 |
| Scala/Java 제한 | Scala 선택 사유 기술 |
| AI 사용 내역 | 사용 범위, 직접 설계/검증한 부분, 프롬프트 전략 기술 |

## 프로젝트 구조

```text
src/main/scala/com/jaekwang/lakehouse/
  SessionizedEventLakehouseApp.scala
  config/AppConfig.scala
  schema/EventSchema.scala
  transform/Sessionization.scala
  io/LakeWriter.scala

sql/
  create_external_table.sql
  wau_by_user.sql
  wau_by_session.sql

docs/
  application-arguments.md
  code-structure.md
  local-environment.md
  period-extension-verification.md
  run-manifest.md
  sample-run-verification.md
  table-schema.md

sample/
  additional_period_2019_10_03.csv
  reprocess_2019_10_02.csv
  sample_events.csv
```

## 처리 흐름

애플리케이션은 특정 월이나 특정 파일명에 묶이지 않도록 구현했습니다.
입력 경로, 출력 경로, 처리 기간, Hive database/table 이름, 실행 ID를 모두
실행 인자로 받습니다.

전체 흐름은 다음과 같습니다.

```text
CSV 입력
  -> event_time을 UTC timestamp로 파싱
  -> KST timestamp 생성
  -> KST 기준 dt partition 생성
  -> user_id, event_time 기준 정렬
  -> 직전 이벤트와 5분 이상 차이 나면 새 세션 생성
  -> 임시 저장 경로에 dt 기준 Parquet/Snappy partition 저장
  -> 임시 저장 결과의 row count와 dt partition 목록 검증
  -> 검증된 dt partition만 최종 경로로 이동
  -> Hive external table 생성 또는 partition repair
  -> run manifest 저장
```

생성 session ID는 다음 값으로 만듭니다.

```text
sha2(user_id | session_start_at_utc | session_seq, 256)
```

이렇게 한 이유는 같은 데이터를 재처리해도 동일한 session ID가 생성되도록
하기 위해서입니다. 또한 기간을 나누어 처리할 때 `session_seq`만으로 ID를
만들면 충돌 가능성이 있으므로, 세션 시작 시각을 함께 사용했습니다.

추가 기간 처리를 위해 저장 결과는 `dt` 기준 partition으로 생성합니다.
애플리케이션은 실행 결과를 임시 저장 경로에 먼저 쓴 뒤 row count와
partition 목록을 확인하고, 이번 실행 결과에 포함된 `dt` partition만
최종 경로로 이동합니다. 따라서 새 기간의 partition은 추가되고, 같은 기간을
재처리하면 해당 `dt` partition만 교체되며 다른 partition은 유지됩니다.

## 빌드

현재 로컬 개발 환경은 Spark 4.1.2, Scala 2.13.17 기준입니다.

```bash
sbt compile
sbt package
```

`spark-sql` dependency는 `Provided`로 설정했습니다. 실제 실행 시 Spark
라이브러리는 `spark-submit` 런타임에서 제공되기 때문입니다.

## 샘플 실행

전체 Kaggle 데이터는 크기가 크기 때문에, 먼저 작은 샘플 데이터로 세션화
규칙과 KST partition 처리를 검증했습니다.

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

샘플 검증 결과:

```text
rows       = 9
users      = 3
sessions   = 6
partitions = 2
```

자세한 검증 내용은 `docs/sample-run-verification.md`에 정리했습니다.

## 추가 기간 처리 검증

추가 기간 처리와 동일 기간 재처리는 별도 샘플 output에서 검증했습니다.

검증용 table:

```text
default.sessionized_events_period_test
```

검증 결과:

| 단계 | 실행 내용 | 결과 |
|---|---|---|
| 1차 | `2019-10-01` ~ `2019-10-02` 샘플 적재 | `2019-10-01=6건`, `2019-10-02=3건` |
| 2차 | `2019-10-03` 추가 적재 | 기존 partition 유지, `2019-10-03=3건` 추가 |
| 3차 | `2019-10-02` 재처리 | 기존 3건에 append되지 않고 2건으로 교체 |

최종 partition별 row 수:

| dt | rows | run_id |
|---|---:|---|
| 2019-10-01 | 6 | `period-test-initial` |
| 2019-10-02 | 2 | `period-test-reprocess-20191002` |
| 2019-10-03 | 3 | `period-test-add-20191003` |

`SHOW PARTITIONS default.sessionized_events_period_test`에서도 다음 partition을 확인했습니다.

```text
dt=2019-10-01
dt=2019-10-02
dt=2019-10-03
```

자세한 실행 명령과 검증 쿼리는 `docs/period-extension-verification.md`에 정리했습니다.

## 전체 데이터 실행

실행 명령:

```bash
spark-submit \
  --master 'local[*]' \
  --driver-memory 8g \
  --class com.jaekwang.lakehouse.SessionizedEventLakehouseApp \
  target/scala-2.13/sessionized-event-lakehouse_2.13-0.1.0.jar \
  --input 'data/raw/extracted/2019-*.csv' \
  --output data/lake/sessionized_events \
  --start-date 2019-10-01 \
  --end-date 2019-12-01 \
  --run-id full-201910-201911-001 \
  --database default \
  --table sessionized_events
```

실행 결과:

```text
run_id     = full-201910-201911-001
min_dt     = 2019-10-01
max_dt     = 2019-11-30
rows       = 109,362,687
partitions = 61
output     = data/lake/sessionized_events
format     = Parquet/Snappy
size       = 5.6GB
files      = 6,100 parquet files
```

원본 10월, 11월 CSV 전체 row 수보다 결과 row 수가 적은 이유는 날짜 필터를
KST partition 기준으로 적용했기 때문입니다. `--end-date 2019-12-01`은
exclusive 조건이므로 KST 기준 `2019-12-01` partition은 제외됩니다.

## Hive External Table

결과 테이블은 Parquet output path를 바라보는 external table로 설계했습니다.

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS ${database}.${table} (
  event_time_utc timestamp,
  event_time_kst timestamp,
  event_type string,
  product_id bigint,
  category_id bigint,
  category_code string,
  brand string,
  price decimal(18,2),
  user_id bigint,
  source_user_session string,
  generated_session_id string,
  session_seq bigint,
  session_start_at_utc timestamp,
  session_start_at_kst timestamp,
  session_event_seq bigint,
  ingested_at timestamp,
  run_id string
)
PARTITIONED BY (dt string)
STORED AS PARQUET
LOCATION '${output}';
```

Hive external table을 사용할 때 주의한 점은 다음과 같습니다.

- table은 데이터 파일을 소유하지 않고 `LOCATION`의 파일을 바라봅니다.
  따라서 검증된 최종 partition만 `${output}/dt=...` 경로에 두도록 했습니다.
- 새 `dt` partition이 추가되면 `MSCK REPAIR TABLE`로 Hive metadata를 갱신합니다.
- `_staging`, `_manifests`처럼 `_`로 시작하는 보조 디렉터리는 partition repair 대상에서 제외되도록 output root 아래에 분리했습니다.
- `dt`는 원본 UTC 날짜가 아니라 KST로 변환한 날짜이며, WAU도 이 `dt`를 기준으로 계산합니다.

## WAU 계산 쿼리

WAU는 Hive external table에 등록된 `dt` partition을 기준으로 계산했습니다.
`dt`는 UTC 원본 시간이 아니라 KST로 변환한 날짜이므로, 주간 기준도 KST 날짜 기준입니다.

주 시작일은 월요일로 두었습니다.
아래 쿼리의 `1970-01-05`는 월요일이기 때문에, 각 `dt`가 속한 주의 월요일을 `week_start`로 계산합니다.

`user_id` 기준 WAU:

```sql
SELECT
  date_sub(to_date(dt), pmod(datediff(to_date(dt), '1970-01-05'), 7)) AS week_start,
  count(DISTINCT user_id) AS wau_users
FROM ${database}.${table}
GROUP BY date_sub(to_date(dt), pmod(datediff(to_date(dt), '1970-01-05'), 7))
ORDER BY week_start;
```

생성된 session ID 기준 WAU:

과제에서는 생성된 세션 ID 기준 WAU도 요구합니다.
다만 이 값은 `user_id` 기준 활성 사용자 수가 아니라, 엄밀히는 한 주 동안 발생한 distinct `generated_session_id` 수입니다.
따라서 README에서는 과제 표현을 따르되, 결과 컬럼은 `wau_sessions`로 표시했습니다.

```sql
SELECT
  date_sub(to_date(dt), pmod(datediff(to_date(dt), '1970-01-05'), 7)) AS week_start,
  count(DISTINCT generated_session_id) AS wau_sessions
FROM ${database}.${table}
GROUP BY date_sub(to_date(dt), pmod(datediff(to_date(dt), '1970-01-05'), 7))
ORDER BY week_start;
```

`user_id` 기준 WAU 결과:

| week_start | wau_users |
|---|---:|
| 2019-09-30 | 818,388 |
| 2019-10-07 | 1,057,958 |
| 2019-10-14 | 1,090,898 |
| 2019-10-21 | 1,093,146 |
| 2019-10-28 | 1,054,722 |
| 2019-11-04 | 1,321,141 |
| 2019-11-11 | 1,543,309 |
| 2019-11-18 | 1,376,755 |
| 2019-11-25 | 1,133,949 |

생성된 session ID 기준 WAU 결과:

| week_start | wau_sessions |
|---|---:|
| 2019-09-30 | 1,570,536 |
| 2019-10-07 | 2,154,180 |
| 2019-10-14 | 2,257,214 |
| 2019-10-21 | 2,153,837 |
| 2019-10-28 | 2,115,233 |
| 2019-11-04 | 2,751,842 |
| 2019-11-11 | 4,754,423 |
| 2019-11-18 | 2,876,494 |
| 2019-11-25 | 2,265,384 |

## 재처리와 장애 복구 설계

현재 구현:

- 각 배치 실행에 `run_id` 부여
- 재처리해도 동일한 session ID가 생성되도록 deterministic ID 사용
- `dt` partition 단위 최종 경로 교체
- Hive external table 방식으로 데이터 위치와 테이블 metadata 분리
- 배치 시작 시 `RUNNING` manifest 작성
- 실행 결과를 최종 경로가 아닌 임시 저장 경로에 먼저 작성
- 임시 저장 결과의 row count와 partition 목록 검증
- 검증된 `dt` partition만 Hive external table이 읽는 최종 경로로 이동
- 최종 partition 반영과 Hive sync가 모두 성공하면 `SUCCESS` manifest 작성
- catch 가능한 예외 발생 시 `FAILED` manifest 작성

Manifest는 output root 아래에 저장됩니다.

```text
{output}/_manifests/{run_id}.json
```

Manifest에는 다음 정보를 기록합니다.

```text
application, run_id, status, input, output, database, table,
start_date, end_date, timezone, session_gap_minutes,
row_count, partitions, started_at, completed_at,
error_class, error_message
```

복구 판단 기준:

```text
SUCCESS manifest 있음  => 성공한 배치
FAILED manifest 있음   => catch 가능한 실패가 발생한 배치
RUNNING만 남아 있음    => 비정상 종료 가능성이 있는 incomplete run
manifest 없음          => 성공한 배치로 보지 않음
```

실패 또는 incomplete run은 같은 기간을 다시 실행합니다. 동일 `dt` partition은
검증된 결과로 교체되므로 중복 append를 피할 수 있습니다.

임시 저장 중 실패하면 최종 partition은 변경하지 않습니다. 최종 경로로 이동하는
과정까지 끝나고 Hive sync가 완료된 뒤에만 `SUCCESS` manifest를 기록하므로,
`SUCCESS`가 없는 실행은 완료된 배치로 보지 않습니다.

자세한 내용은 `docs/run-manifest.md`에 정리했습니다.

## Scala 선택 이유

Scala를 선택한 이유는 Spark가 Scala 기반으로 만들어진 프레임워크이고,
DataFrame API와 window function 기반 변환을 간결하게 표현할 수 있기
때문입니다.

이번 과제의 핵심은 `user_id`별 이벤트 순서를 기준으로 이전 이벤트와의 시간
차이를 계산하고 누적 세션 번호를 만드는 것입니다. Scala에서는 Spark SQL
function과 window specification을 자연스럽게 조합할 수 있어, 세션화 로직을
Spark 실행 모델에 가깝게 표현할 수 있다고 판단했습니다.

## AI 도구 사용 내역

AI 도구는 개발 보조 용도로 사용했습니다.

사용 범위:

- 과제 요구사항 체크리스트 정리
- Spark 애플리케이션 구조 초안 작성 보조
- README와 설계 문서 초안 작성 보조
- 현재 구현 상태와 과제 요구사항 비교
- 제출 전 남은 작업 식별

직접 설계 및 검증한 부분:

- KST 기준 partition 설계
- 5분 이상 gap 기준 세션화 규칙 해석
- `generated_session_id` 구성 방식
- Hive external table schema
- 샘플 데이터의 경계 케이스 구성
- 샘플 실행 결과 검증

프롬프트 전략:

- 과제 요구사항을 항목별로 나누어 구현 상태와 비교했습니다.
- AI가 생성한 코드는 바로 제출하지 않고, 설명 가능한 단위로 나누어
  검토했습니다.
- run manifest와 임시 저장 경로 기반 partition 반영 흐름은 구현하고
  샘플 실행으로 검증했습니다.
- `src/test` 기반 자동화 테스트는 별도로 작성하지 않았고, 샘플 데이터 실행과
  전체 데이터 실행 결과를 기준으로 검증했습니다.
