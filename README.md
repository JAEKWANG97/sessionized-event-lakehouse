# Sessionized Event Lakehouse

이 프로젝트에서 풀어야 했던 핵심 문제는 원본 로그를 그대로 저장하는 것이 아니라,
사용자 이벤트를 시간 순서대로 묶어 세션 단위로 해석할 수 있게 만드는 것이었습니다.

Spark 애플리케이션으로 전자상거래 activity 로그를 읽고, 같은 `user_id` 안에서
이벤트 간격이 5분 이상 벌어지면 새로운 세션으로 판단합니다. 결과는 KST 기준
`dt` partition으로 나누어 Parquet/Snappy 형식으로 저장하고, Hive external table을
통해 SQL로 조회할 수 있게 구성했습니다.

## 구현 범위 요약

| 요구사항 | 반영 내용 |
|---|---|
| Spark Application 작성 | Scala 기반 Spark 애플리케이션 구현 |
| KST 기준 daily partition | UTC `event_time`을 KST로 변환한 뒤 `dt` partition 생성 |
| 5분 이상 간격 기준 세션화 | `user_id`별 이전 이벤트와 현재 이벤트의 시간 차이를 계산해 새 세션 판단 |
| 새 세션 ID 생성 | `generated_session_id`를 `user_id + session_start_at_utc` 기반 hash로 생성 |
| Parquet/Snappy 처리 | `dt` 기준 Parquet/Snappy 저장 |
| External Table 설계 | Spark가 저장한 파일 경로를 Hive external table로 등록 |
| 추가 기간 처리 | 입력 경로와 처리 기간을 실행 인자로 받고, 실행 결과에 포함된 `dt` partition만 추가 또는 교체 |
| 배치 장애 복구 장치 | run manifest, staging path, run_id별 version path, Hive partition location 전환 구조 구현 |
| WAU 계산 | Hive external table을 이용해 `user_id`, `generated_session_id` 기준 주간 distinct count 계산 |
| Scala/Java 제한 | Scala 선택 사유 기술 |
| AI 사용 내역 | 사용 범위, 직접 설계/검증한 부분, 프롬프트 전략 기술 |

요구사항 기준 체크리스트는 [`docs/requirements.md`](docs/requirements.md)에 따로 정리했습니다.

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

애플리케이션은 특정 월이나 특정 파일명에 묶이지 않도록 만들었습니다. 입력 경로,
출력 경로, 처리 기간, Hive database/table 이름, 실행 ID를 모두 실행 인자로 받습니다.

```text
CSV 입력 + 선택 lookback 입력
  -> event_time을 UTC timestamp로 파싱
  -> KST timestamp와 KST 기준 dt partition 생성
  -> user_id, event_time 순서로 정렬
  -> 직전 이벤트와 5분 이상 차이 나면 새 세션 생성
  -> staging path에 dt 기준 Parquet/Snappy 저장
  -> staging 결과의 row 수와 dt partition 목록 검증
  -> 검증된 결과를 _versions/{run_id} 경로로 이동
  -> Hive partition location을 새 version path로 전환
  -> run manifest 저장
```

생성 세션 ID는 다음 값으로 만듭니다.

```text
sha2(user_id | session_start_at_utc, 256)
```

처음에는 `session_seq`까지 ID 재료로 넣는 방식도 생각할 수 있습니다. 하지만
`--lookback-input`이 추가되면 같은 세션이라도 앞쪽 history에 따라 `session_seq`가
달라질 수 있습니다. 그래서 재처리 안정성을 위해 `generated_session_id`는
`user_id + session_start_at_utc` 기반으로 만들고, `session_seq`는 검증용 보조 컬럼으로
남겼습니다.

기간 경계에서 이어지는 세션은 `--lookback-input`으로 이전 기간 데이터를 함께 읽어
계산할 수 있습니다. 세션화는 `input + lookback-input` 전체에서 수행하고, 마지막 저장
직전에만 `start-date <= dt < end-date` 조건을 적용합니다. 따라서 최종 저장 대상 row라도
`session_start_at_utc`와 `session_start_at_kst`가 저장 범위 이전 시각을 가리킬 수 있습니다.
이는 경계 세션이 이전 기간에서 시작되었음을 나타내기 위한 의도된 동작입니다.

## 주요 설계 보정

구현 후 재처리, 기간 확장, 장애 상황을 다시 검토하면서 몇 가지 리스크를 발견했습니다.
최종 구현은 이 리스크를 다음과 같이 보정한 결과입니다.

| 검토 중 발견한 리스크 | 보정한 내용 |
|---|---|
| 신규 기간만 읽으면 이전 기간 마지막 이벤트와 이어지는 세션을 알 수 없습니다. | `--lookback-input`을 추가해 이전 기간 데이터를 세션 계산의 판단 참고 데이터로 함께 읽도록 변경했습니다. |
| `session_seq`는 입력 범위에 따라 달라질 수 있습니다. | `generated_session_id`에서 `session_seq`를 제외하고 `user_id + session_start_at_utc` 기반으로 변경했습니다. |
| final partition을 먼저 삭제하면 테이블 반영 중 장애가 날 때 해당 날짜 데이터가 비어 보일 수 있습니다. | `_versions/{run_id}`에 결과를 보존하고 Hive partition `LOCATION`을 전환하는 방식으로 변경했습니다. |
| 기본 저장 결과가 작은 parquet 파일을 과도하게 생성했습니다. | `dt` 기준 repartition을 기본 적용해 전체 실행 기준 parquet 파일 수를 6100개에서 61개로 줄였습니다. |
| 세션 경계 로직에 코드 레벨 회귀 테스트가 부족했습니다. | `SessionizationSpec`, `AppConfigSpec`를 추가해 5분 간격, KST 경계, lookback 입력, 인자 파싱을 검증했습니다. |

## 빌드

현재 로컬 개발 환경은 Spark 4.1.2, Scala 2.13.17 기준입니다.

```bash
sbt compile
sbt test
sbt package
```

`spark-sql` dependency는 `Provided`로 설정했습니다. 실제 실행 시 Spark 라이브러리는
`spark-submit` 런타임에서 제공되기 때문입니다.

## 샘플 검증

전체 Kaggle 데이터는 크기가 크기 때문에, 먼저 작은 샘플 데이터로 세션화 규칙과
KST `dt` partition 처리를 검증했습니다.

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

추가 기간 처리, 동일 기간 재처리, lookback 경계 세션은 별도 샘플로 검증했습니다.

| 검증 | 결과 |
|---|---|
| 새 기간 추가 | 기존 `dt` partition은 유지하고 `2019-10-03` partition 추가 |
| 기존 기간 재처리 | `2019-10-02`가 중복 append되지 않고 새 결과로 교체 |
| lookback 경계 세션 | 최종 저장 row는 `dt=2019-11-01` 1건이지만, `session_start_at_kst=2019-10-31 23:58:00`, `session_event_seq=2`로 계산됨 |

자세한 실행 명령과 검증 쿼리는 다음 문서에 정리했습니다.

- [`docs/sample-run-verification.md`](docs/sample-run-verification.md)
- [`docs/period-extension-verification.md`](docs/period-extension-verification.md)

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

원본 10월, 11월 CSV 전체 row 수보다 결과 row 수가 적은 이유는 날짜 필터를 KST
`dt` 기준으로 적용했기 때문입니다. `--end-date 2019-12-01`은 exclusive 조건이므로
KST 기준 `2019-12-01` partition은 제외됩니다.

## Hive External Table

결과 테이블은 Spark가 만든 Parquet 파일 경로를 바라보는 external table로 설계했습니다.
Hive가 데이터를 직접 소유하지 않고, 지정된 `LOCATION`의 파일을 SQL로 읽는 방식입니다.

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

이 테이블에서 주의한 점은 다음과 같습니다.

- 기본 table location은 `${output}`으로 두고, 각 `dt` partition이 실제로 읽는 위치는
  검증된 `_versions/{run_id}/dt=...` 경로로 설정합니다.
- 새 `dt` partition은 `ALTER TABLE ADD IF NOT EXISTS PARTITION ... LOCATION`으로 추가합니다.
- 기존 `dt` partition 재처리는 `ALTER TABLE ... PARTITION ... SET LOCATION`으로 Hive가 읽을
  위치를 새 version path로 바꿉니다.
- `MSCK REPAIR TABLE`은 table root 아래의 `dt=...` partition을 자동 발견하는 방식에 가깝지만,
  이 구조에서는 재처리 안정성을 위해 partition별 `LOCATION`을 명시적으로 관리합니다.
- `dt`는 원본 UTC 날짜가 아니라 KST로 변환한 날짜이며, WAU도 이 `dt`를 기준으로 계산합니다.

필드별 설명과 설계 근거는 [`docs/table-schema.md`](docs/table-schema.md)에 정리했습니다.

## WAU 계산 쿼리

WAU는 Hive external table에 등록된 KST 기준 `dt` partition으로 계산했습니다. 주 시작일은
월요일로 두었습니다. 아래 쿼리의 `1970-01-05`는 월요일이기 때문에, 각 `dt`가 속한 주의
월요일을 `week_start`로 계산합니다.

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

요구사항에는 생성된 세션 ID 기준 WAU도 포함되어 있습니다. 다만 이 값은 `user_id` 기준
활성 사용자 수가 아니라, 한 주 동안 발생한 distinct `generated_session_id` 수에 가깝습니다.
그래서 결과 컬럼은 의미가 드러나도록 `wau_sessions`로 표시했습니다.

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

현재 구현은 “실패를 자동으로 완전히 복구한다”기보다는, 성공/실패/incomplete run을 구분하고
같은 기간을 안전하게 다시 실행할 수 있게 만드는 데 초점을 두었습니다.

구현한 장치는 다음과 같습니다.

- 각 배치 실행에 `run_id` 부여
- 배치 시작 시 `RUNNING` manifest 작성
- 결과를 final path가 아니라 staging path에 먼저 저장
- staging 결과의 row 수와 `dt` partition 목록 검증
- 검증된 결과를 `_versions/{run_id}` 경로로 이동
- Hive sync 실행에서는 이번 실행에 포함된 `dt` partition의 `LOCATION`을 새 version path로 전환
- 모든 전환이 성공하면 `SUCCESS` manifest 작성
- catch 가능한 예외 발생 시 `FAILED` manifest 작성

Manifest는 결과 저장 루트 경로 아래에 저장됩니다.

```text
{output}/_manifests/{run_id}.json
```

복구 판단 기준:

```text
SUCCESS manifest 있음  => 성공한 배치
FAILED manifest 있음   => catch 가능한 실패가 발생한 배치
RUNNING만 남아 있음    => 비정상 종료 가능성이 있는 incomplete run
manifest 없음          => 성공한 배치로 보지 않음
```

임시 저장 중 실패하면 Hive partition location은 변경하지 않습니다. 다만 여러 `dt` partition의
location을 전환하는 중 장애가 나면 일부 partition만 새 version을 바라보는 mixed state가 될 수
있습니다. 여러 partition의 반영을 하나의 atomic commit처럼 보장해야 한다면 Iceberg, Delta,
Hudi 같은 table format이 더 적절합니다.

자세한 내용은 [`docs/run-manifest.md`](docs/run-manifest.md)에 정리했습니다.

## 현재 구현 범위와 운영 확장 방향

이번 구현은 로컬 환경에서 과제 요구사항을 검증할 수 있는 Spark batch application에 집중했습니다.
운영 환경으로 확장한다면 다음 지점을 추가로 보완하는 것이 좋습니다.

| 항목 | 현재 구현 | 운영 확장 방향 |
|---|---|---|
| 추가 기간의 세션 경계 | `--lookback-input`으로 이전 기간 데이터를 명시적으로 함께 읽습니다. 세션화는 `input + lookback-input` 전체에서 수행하고, 최종 저장은 `start-date <= dt < end-date` 범위만 수행합니다. | 완전한 stateful incremental 처리가 필요하면 `user_id`별 마지막 이벤트와 세션 상태를 별도 state table로 관리합니다. |
| partition location 전환 중 장애 | Hive sync 실행에서는 `_versions/{run_id}` 경로를 만들고 Hive partition location을 전환합니다. 기존 데이터를 먼저 삭제하지 않아 특정 날짜 데이터가 비어 보일 위험은 줄였지만, mixed state 가능성은 남습니다. Hive sync를 끈 로컬 샘플 실행은 파일시스템 partition 교체 방식으로 동작합니다. | 여러 partition을 하나의 atomic commit으로 묶어야 한다면 Iceberg/Delta/Hudi 같은 table format을 사용합니다. |
| 작은 파일 과다 생성 | 저장 전에 `dt` 기준 `repartition`을 수행해 작은 파일 수를 줄였습니다. | 날짜별 데이터가 커지는 운영 환경에서는 목표 파일 크기를 기준으로 파일 수를 조절하거나, 후속 compaction batch를 둡니다. |
| 자동화 테스트 | Spark local mode 테스트로 세션화 핵심 경계와 인자 파싱을 검증했습니다. | LakeWriter의 versioned publish까지 자동화하려면 별도 integration test를 추가할 수 있습니다. |

## Scala 선택 이유

Scala를 선택한 이유는 Spark가 Scala 기반으로 만들어진 프레임워크이고, DataFrame API와
window function 기반 변환을 간결하게 표현할 수 있기 때문입니다.

이 프로젝트의 핵심은 `user_id`별 이벤트 순서를 기준으로 이전 이벤트와의 시간 차이를 계산하고,
그 결과를 누적해 세션 번호를 만드는 것입니다. Scala에서는 Spark SQL function과 window
specification을 자연스럽게 조합할 수 있어, 세션화 로직을 Spark 실행 모델에 가깝게 표현할 수
있다고 판단했습니다.

## AI 도구 사용 내역

사용한 도구:

- OpenAI ChatGPT/Codex
- Hermes Agent 기반 리뷰

AI는 정답을 그대로 받는 용도보다는, 요구사항을 쪼개고 제가 헷갈리는 개념을 확인하며 구현과
문서를 검토하는 용도로 사용했습니다. 특히 제출 전에는 비관적인 리뷰를 요청해 재처리, 장애 복구,
기간 경계 세션처럼 놓치기 쉬운 지점을 다시 점검했습니다.

AI를 활용한 부분:

- 요구사항 분해와 구현 범위 체크리스트 정리
- Hive external table, partition, Parquet/Snappy, manifest 개념 학습 보조
- Spark/Scala 코드 초안 작성과 세션화 로직 리뷰
- README와 `docs/` 문서 초안 작성 및 표현 정리
- 실행 명령, 검증 쿼리, 면접 예상 질문 점검

AI 리뷰를 통해 발견하고 보정한 부분:

| 리뷰에서 드러난 리스크 | 보정한 내용 |
|---|---|
| 신규 기간만 읽으면 이전 기간 마지막 이벤트와 이어지는 세션을 알 수 없습니다. | `--lookback-input`을 추가해 이전 기간 데이터를 세션 계산의 판단 참고 데이터로 함께 읽도록 변경했습니다. |
| `session_seq`는 입력 범위에 따라 달라질 수 있습니다. | `generated_session_id`에서 `session_seq`를 제외하고 `user_id + session_start_at_utc` 기반으로 변경했습니다. |
| final partition을 먼저 삭제하면 테이블 반영 중 장애가 날 때 해당 날짜 데이터가 비어 보일 수 있습니다. | `_versions/{run_id}`에 결과를 보존하고 Hive partition `LOCATION`을 전환하는 방식으로 변경했습니다. |
| 기본 저장 결과가 작은 parquet 파일을 과도하게 생성했습니다. | `dt` 기준 repartition을 기본 적용해 전체 실행 기준 parquet 파일 수를 6100개에서 61개로 줄였습니다. |
| 세션 경계 로직에 코드 레벨 회귀 테스트가 부족했습니다. | `SessionizationSpec`, `AppConfigSpec`를 추가해 핵심 경계를 자동화 테스트로 검증했습니다. |

직접 판단하고 검증한 부분:

- `event_time`을 KST로 변환한 뒤 `dt` partition을 만드는 방향
- 세션 경계 조건을 `이전 이벤트와 현재 이벤트의 차이 >= 300초`로 해석
- 원본 `user_session`은 기준을 알 수 없으므로 참고용 `source_user_session`으로 보존하고, 새 `generated_session_id`를 생성하기로 한 결정
- `--lookback-input` 도입 후 `session_seq`가 입력 범위에 따라 바뀔 수 있음을 반영해 `generated_session_id`에서 `session_seq`를 제외한 결정
- 추가 기간 적재와 동일 기간 재처리를 Hive partition location 전환으로 처리한 설계
- 배치 실패를 자동 복구하기보다, incomplete run을 식별하고 같은 기간을 재실행할 수 있게 만든 복구 방향
- 샘플 데이터, lookback 경계 케이스, 전체 데이터 실행 결과, WAU 결과 확인

프롬프트 전략:

- 처음부터 코드 생성을 요청하기보다, 요구사항을 하나씩 나누어 제가 이해한 내용을 설명하고 빠진 부분을 지적하게 했습니다.
- 모르는 개념은 바로 구현으로 넘어가지 않고, Hive external table, partition, Parquet/Snappy, manifest가 각각 어떤 문제를 해결하는지 질문했습니다.
- AI가 제안한 구현은 그대로 확정하지 않고, 재처리, 중복 append, 중간 실패, KST 날짜 경계처럼 실패할 수 있는 상황을 다시 질문했습니다.
- 구현 후에는 비관적 리뷰를 요청해 약한 가정과 면접 질문 후보를 뽑고, 현재 구현 범위에서 보완할 수 있는 항목만 코드와 문서에 반영했습니다.

검증 방식:

- 작은 샘플 CSV로 KST `dt` partition, 5분 간격 세션화, 정확히 5분인 경계 조건 확인
- `SessionizationSpec`에서 4분 59초 간격, 정확히 5분 간격, user별 독립 세션, KST 날짜 경계, 원본 `user_session`과 생성 세션 분리, lookback 경계 세션 확인
- `AppConfigSpec`에서 `--lookback-input`과 `--repartition-by-dt` 인자 해석 확인
- 추가 기간 적재와 동일 기간 재처리 시 partition이 추가되거나 location이 전환되는지 확인
- 전체 10월/11월 데이터를 실행해 row 수, partition 수, WAU 결과 확인
- `sbt compile`, `sbt test`, `sbt package`, `spark-submit`, `spark-sql` 실행 결과를 기준으로 검증
