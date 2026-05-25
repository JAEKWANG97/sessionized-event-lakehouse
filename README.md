# Sessionized Event Lakehouse

이 프로젝트는 ecommerce activity 로그를 Spark 애플리케이션으로 읽고,
동일 `user_id` 내 이벤트 간격이 5분 이상이면 새로운 세션으로 판단하여
세션 ID를 생성합니다. 결과는 KST 기준 일자 partition으로 나누어
Parquet/Snappy 형식으로 저장하고, Hive external table에서 조회할 수 있게
설계했습니다.

## 구현 범위 요약

| 요구사항 | 반영 내용 |
|---|---|
| Spark Application 작성 | Scala 기반 Spark 애플리케이션 구현 |
| KST 기준 daily partition | UTC `event_time`을 KST로 변환한 뒤 `dt` partition 생성 |
| 5분 이상 gap 기준 세션화 | `user_id`별 이전 이벤트와 현재 이벤트의 시간 차이를 계산해 새 session 판단 |
| 새 세션 ID 생성 | `generated_session_id`를 deterministic hash로 생성 |
| Parquet/Snappy 처리 | `dt` 기준 Parquet/Snappy partition 저장 |
| External Table 설계 | Spark output path를 바라보는 Hive external table 생성 |
| 추가 기간 처리 | 입력 경로와 기간을 실행 인자로 받고, 실행 결과에 포함된 `dt` partition만 추가 또는 교체 |
| 배치 장애 복구 장치 | run manifest, 임시 저장 경로, versioned path, Hive partition location 전환 구조 구현 |
| WAU 계산 | Hive external table을 이용해 `user_id`, `generated_session_id` 기준 주간 distinct count 계산 |
| Scala/Java 제한 | Scala 선택 사유 기술 |
| AI 사용 내역 | 사용 범위, 직접 설계/검증한 부분, 프롬프트 전략 기술 |

요구사항을 기준으로 한 체크리스트는
[`docs/requirements.md`](docs/requirements.md)에 따로 정리했습니다.

## 프로젝트 구조

```text
src/main/scala/com/jaekwang/lakehouse/
  SessionizedEventLakehouseApp.scala
  config/AppConfig.scala
  schema/EventSchema.scala
  transform/Sessionization.scala
  io/LakeWriter.scala
  io/RunManifestWriter.scala

src/test/scala/com/jaekwang/lakehouse/
  config/AppConfigSpec.scala
  transform/SessionizationSpec.scala

sql/
  create_external_table.sql
  wau_by_user.sql
  wau_by_session.sql

docs/
  application-arguments.md
  code-structure.md
  local-environment.md
  period-extension-verification.md
  requirements.md
  run-manifest.md
  sample-run-verification.md
  table-schema.md

sample/
  additional_period_2019_10_03.csv
  lookback_previous_period.csv
  lookback_target_period.csv
  reprocess_2019_10_02.csv
  sample_events.csv
```

## 처리 흐름

애플리케이션은 특정 월이나 특정 파일명에 묶이지 않도록 구현했습니다.
입력 경로, 출력 경로, 처리 기간, Hive database/table 이름, 실행 ID를 모두
실행 인자로 받습니다.

전체 흐름은 다음과 같습니다.

```text
CSV 입력 + optional lookback 입력
  -> event_time을 UTC timestamp로 파싱
  -> KST timestamp 생성
  -> KST 기준 dt partition 생성
  -> user_id, event_time 기준 정렬
  -> 직전 이벤트와 5분 이상 차이 나면 새 세션 생성
  -> 임시 저장 경로에 dt 기준 Parquet/Snappy partition 저장
  -> 임시 저장 결과의 row count와 dt partition 목록 검증
  -> 검증된 결과를 run_id별 versioned path로 이동
  -> Hive partition location을 versioned path로 전환
  -> run manifest 저장
```

생성 session ID는 다음 값으로 만듭니다.

```text
sha2(user_id | session_start_at_utc, 256)
```

이렇게 한 이유는 같은 데이터를 재처리해도 동일한 session ID가 생성되도록
하기 위해서입니다. `session_seq`는 입력 범위에 따라 달라질 수 있으므로
생성 ID의 재료로 사용하지 않고, 사람이 검증할 수 있는 보조 컬럼으로 남겼습니다.

추가 기간 처리를 위해 저장 결과는 `dt` 기준 partition으로 생성합니다.
애플리케이션은 실행 결과를 임시 저장 경로에 먼저 쓴 뒤 row count와
partition 목록을 확인합니다. Hive sync를 사용하는 경우 검증된 결과는
`_versions/{run_id}/dt=...` 경로에 보존하고, 이번 실행 결과에 포함된
`dt` partition의 Hive location만 새 versioned path로 전환합니다. 따라서
새 기간의 partition은 추가되고, 같은 기간을 재처리하면 해당 `dt` partition이
새 버전 데이터를 바라보며 다른 partition은 유지됩니다.

기간 경계에서 이어지는 세션은 `--lookback-input`으로 이전 기간 데이터를
함께 읽어 계산할 수 있습니다. 세션화는 `input + lookback-input` 전체에서
수행하고, 마지막 write 직전에만 `start-date <= dt < end-date` 조건을
적용합니다. 따라서 publish 대상 row라도 `session_start_at_utc`와
`session_start_at_kst`가 publish 범위 이전 시각을 가리킬 수 있습니다.
이는 경계 세션이 이전 기간에서 시작되었음을 나타내기 위한 의도된 동작입니다.

## 빌드

현재 로컬 개발 환경은 Spark 4.1.2, Scala 2.13.17 기준입니다.

```bash
sbt compile
sbt test
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

기간 경계 세션을 확인하기 위한 lookback 샘플도 별도로 실행했습니다.

```bash
spark-submit \
  --master 'local[2]' \
  --driver-memory 2g \
  --class com.jaekwang.lakehouse.SessionizedEventLakehouseApp \
  target/scala-2.13/sessionized-event-lakehouse_2.13-0.1.0.jar \
  --input sample/lookback_target_period.csv \
  --lookback-input sample/lookback_previous_period.csv \
  --output data/lake/lookback_boundary_test \
  --start-date 2019-11-01 \
  --end-date 2019-11-02 \
  --run-id lookback-boundary-001 \
  --enable-hive-sync false
```

검증 결과 publish된 row는 `dt=2019-11-01`의 1건뿐이지만, 해당 row의
`session_event_seq`는 `2`이고 `session_start_at_kst`는 `2019-10-31 23:58:00`입니다.
즉 이전 기간 이벤트를 함께 읽어 경계 세션이 끊기지 않았음을 확인했습니다.

## 전체 데이터 실행

실행 명령:

```bash
spark-submit \
  --master 'local[*]' \
  --driver-memory 8g \
  --class com.jaekwang.lakehouse.SessionizedEventLakehouseApp \
  target/scala-2.13/sessionized-event-lakehouse_2.13-0.1.0.jar \
  --input 'data/raw/extracted/2019-*.csv' \
  --output data/lake/sessionized_events_repartitioned \
  --start-date 2019-10-01 \
  --end-date 2019-12-01 \
  --run-id full-201910-201911-repartition-001 \
  --database default \
  --table sessionized_events_repartitioned \
  --repartition-by-dt true
```

실행 결과:

```text
run_id     = full-201910-201911-repartition-001
min_dt     = 2019-10-01
max_dt     = 2019-11-30
rows       = 109,362,687
partitions = 61
output     = data/lake/sessionized_events_repartitioned
format     = Parquet/Snappy
size       = 4.5GB
files      = 61 parquet files
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
  기본 table location은 `${output}`으로 두고, 각 `dt` partition location은
  검증된 `_versions/{run_id}/dt=...` 경로로 설정합니다.
- 새 `dt` partition은 `ALTER TABLE ADD IF NOT EXISTS PARTITION ... LOCATION`으로 추가하고,
  기존 `dt` partition 재처리는 `ALTER TABLE ... PARTITION ... SET LOCATION`으로 새 버전을 바라보게 합니다.
- `_staging`, `_versions`, `_manifests`처럼 `_`로 시작하는 보조 디렉터리는 output root 아래에 분리했습니다.
- `dt`는 원본 UTC 날짜가 아니라 KST로 변환한 날짜이며, WAU도 이 `dt`를 기준으로 계산합니다.

필드는 다음 기준으로 나누어 설계했습니다.

| 구분 | 컬럼 | 설계 이유 |
|---|---|---|
| 원본 이벤트 속성 | `event_type`, `product_id`, `category_id`, `category_code`, `brand`, `price`, `user_id` | 원본 로그의 분석 가능한 이벤트 정보를 보존하기 위해 유지 |
| 시간 기준 | `event_time_utc`, `event_time_kst`, `dt` | 원본 시간과 KST 기준 처리 결과를 함께 남겨 partition과 시간 변환을 검증할 수 있게 함 |
| 원본 세션 추적 | `source_user_session` | 원본 `user_session`은 기준을 알 수 없으므로 새 세션 ID와 구분해 참고용으로만 보존 |
| 생성 세션 | `generated_session_id`, `session_seq`, `session_start_at_utc`, `session_start_at_kst`, `session_event_seq` | 요구사항의 5분 gap 기준 세션을 식별하고, 사람이 세션 경계를 검증할 수 있게 함 |
| 배치 추적 | `ingested_at`, `run_id` | 어떤 실행에서 만들어진 데이터인지 추적하고 재처리/장애 확인에 사용 |

더 자세한 필드별 설명과 설계 근거는
[`docs/table-schema.md`](docs/table-schema.md)에 정리했습니다.

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

요구사항에는 생성된 세션 ID 기준 WAU도 포함되어 있습니다.
다만 이 값은 `user_id` 기준 활성 사용자 수가 아니라, 엄밀히는 한 주 동안 발생한 distinct `generated_session_id` 수입니다.
따라서 결과 컬럼은 의미가 드러나도록 `wau_sessions`로 표시했습니다.

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
- `dt` partition 단위 Hive location 전환
- Hive external table 방식으로 데이터 위치와 테이블 metadata 분리
- 배치 시작 시 `RUNNING` manifest 작성
- 실행 결과를 최종 경로가 아닌 임시 저장 경로에 먼저 작성
- 임시 저장 결과의 row count와 partition 목록 검증
- 검증된 결과를 `_versions/{run_id}` 경로로 이동
- 이번 실행에 포함된 `dt` partition의 Hive location을 versioned path로 전환
- Hive partition location 전환이 모두 성공하면 `SUCCESS` manifest 작성
- catch 가능한 예외 발생 시 `FAILED` manifest 작성

Manifest는 output root 아래에 저장됩니다.

```text
{output}/_manifests/{run_id}.json
```

Manifest에는 다음 정보를 기록합니다.

```text
application, run_id, status, input, lookback_input, input_paths,
output, database, table, start_date, end_date, timezone, session_gap_minutes,
repartition_by_dt,
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

임시 저장 중 실패하면 Hive partition location은 변경하지 않습니다. 검증된 데이터를
`_versions/{run_id}` 경로로 이동하고 Hive partition location 전환까지 끝난 뒤에만
`SUCCESS` manifest를 기록하므로, `SUCCESS`가 없는 실행은 완료된 배치로 보지 않습니다.

자세한 내용은 `docs/run-manifest.md`에 정리했습니다.

## 현재 구현 범위와 운영 확장 방향

현재 구현은 로컬 환경에서 요구사항을 검증할 수 있도록 Spark 애플리케이션,
Parquet/Snappy 저장, Hive external table, 재처리 가능한 partition location 전환
흐름에 집중했습니다. 운영 환경으로 확장할 때는 다음 지점을 추가로 보완하는
것이 좋습니다.

| 항목 | 현재 구현 | 운영 확장 방향 |
|---|---|---|
| 추가 기간의 세션 경계 | `--lookback-input`으로 이전 기간 데이터를 명시적으로 함께 읽을 수 있습니다. 세션화는 `input + lookback-input` 전체에서 수행하고, publish는 `start-date <= dt < end-date` 범위만 수행합니다. | 완전한 stateful incremental 처리가 필요하면 `user_id`별 마지막 이벤트와 세션 상태를 별도 state table로 관리합니다. |
| partition publish 중 장애 | staging write와 검증이 끝난 뒤 `_versions/{run_id}` 경로를 만들고 Hive partition location을 전환합니다. 기존 데이터를 먼저 삭제하지 않으므로 partition missing 위험은 줄였지만, 여러 `dt`를 전환하는 중 장애가 나면 일부 partition만 새 version을 바라보는 mixed 상태가 될 수 있습니다. | 여러 partition을 하나의 atomic commit으로 묶어야 한다면 Iceberg/Delta/Hudi 같은 table format을 사용합니다. |
| small file | 기본값으로 write 전에 `dt` 기준 `repartition`을 수행해 같은 날짜의 데이터가 여러 작은 파일로 과도하게 쪼개지는 문제를 줄였습니다. | 날짜별 데이터가 커지는 운영 환경에서는 target file size 기준 repartition, salt key, 또는 후속 compaction batch를 둡니다. |
| 자동화 테스트 | `SessionizationSpec`에서 Spark local mode로 세션화 핵심 경계를 검증합니다. | LakeWriter의 versioned publish까지 자동화하려면 별도 integration test를 추가할 수 있습니다. |

## Scala 선택 이유

Scala를 선택한 이유는 Spark가 Scala 기반으로 만들어진 프레임워크이고,
DataFrame API와 window function 기반 변환을 간결하게 표현할 수 있기
때문입니다.

이 프로젝트의 핵심은 `user_id`별 이벤트 순서를 기준으로 이전 이벤트와의 시간
차이를 계산하고 누적 세션 번호를 만드는 것입니다. Scala에서는 Spark SQL
function과 window specification을 자연스럽게 조합할 수 있어, 세션화 로직을
Spark 실행 모델에 가깝게 표현할 수 있다고 판단했습니다.

## AI 도구 사용 내역

사용한 도구:

- OpenAI ChatGPT/Codex

AI는 정답을 그대로 받는 용도보다, 요구사항을 쪼개고 제가 헷갈리는 개념을
확인하며 구현 초안을 빠르게 검토하는 용도로 사용했습니다.

AI를 활용한 부분:

- 요구사항을 항목별로 나누고 구현 범위 체크리스트를 정리
- Hive, external table, partition, Parquet/Snappy, manifest 개념 학습 보조
- Spark 애플리케이션의 파일 구조와 일부 Scala 코드 초안 작성 보조
- `user_id`별 window function을 이용한 세션화 로직 검토
- staging 경로에 먼저 쓰고 검증 후 Hive partition location을 전환하는 흐름 검토
- README와 `docs/` 문서 초안 작성 및 표현 정리 보조
- 실행 명령, 검증 쿼리, 남은 작업 목록 점검

직접 판단하고 검증한 부분:

- `event_time`을 KST로 변환한 뒤 `dt` daily partition을 만드는 방향
- 세션 경계 조건을 `이전 이벤트와 현재 이벤트의 차이 >= 300초`로 해석
- 원본 `user_session`은 기준을 알 수 없으므로 사용하지 않고,
  새 `generated_session_id`를 생성하기로 한 결정
- `generated_session_id`를 재처리해도 동일하게 나오도록 deterministic hash로 구성
- `--lookback-input` 도입 후 `session_seq`가 입력 범위에 따라 바뀔 수 있음을
  반영해 `generated_session_id`에서 `session_seq`를 제외한 결정
- external table 필드 구성과 `source_user_session`, `session_seq`,
  `session_start_at_*`, `run_id`를 남긴 이유
- 추가 기간 적재와 동일 기간 재처리를 partition location 전환으로 처리한 설계
- 배치 실패를 자동 복구하기보다, incomplete run을 식별하고 같은 기간을
  안전하게 재실행할 수 있게 만든 복구 방향
- 샘플 데이터 경계 케이스, lookback 경계 케이스, 전체 데이터 실행 결과 확인
- `src/test` 기반 Spark local 테스트 추가
- WAU를 `user_id` 기준 값과 `generated_session_id` 기준 값으로 분리해 해석

프롬프트 전략:

- 처음부터 코드 생성을 요청하기보다, 요구사항을 하나씩 나누어 제가 이해한
  내용을 설명하고 AI에게 빠진 부분을 지적하게 했습니다.
- 모르는 개념은 바로 구현으로 넘어가지 않고, Hive external table,
  partition, Parquet/Snappy, manifest가 각각 어떤 문제를 해결하는지
  질문했습니다.
- AI가 제안한 구현은 그대로 확정하지 않고, 재처리, 중복 append, 중간 실패,
  KST 날짜 경계처럼 실패할 수 있는 상황을 다시 질문했습니다.
- README 문서는 AI가 초안을 만들되, 공개 저장소에 맞지 않는 표현과
  제가 설명하기 어려운 문장은 제거하거나 다시 작성했습니다.

검증 방식:

- 작은 샘플 CSV로 KST partition, 5분 gap 세션화, 정확히 5분인 경계 조건을 확인
- `SessionizationSpec`에서 4분 59초 gap, 정확히 5분 gap, user별 독립 세션, KST 날짜 경계, 원본 `user_session`과 생성 세션 분리, lookback으로 인한 기간 경계 세션을 자동화 테스트로 확인
- 추가 기간 적재와 동일 기간 재처리 시 partition이 추가 또는 location 전환되는지 확인
- `AppConfigSpec`에서 `--lookback-input`과 `--repartition-by-dt` 인자 해석을 확인
- 전체 10월/11월 데이터를 실행해 row 수, partition 수, WAU 결과를 확인
- `sbt compile`, `sbt test`, `sbt package`, `spark-submit`, `spark-sql` 실행 결과를 기준으로 검증
