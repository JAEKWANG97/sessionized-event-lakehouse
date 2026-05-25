# Run Manifest

처음에는 Spark 로그만 확인해도 충분해 보일 수 있습니다. 하지만 재처리 가능한 배치에서는 로그보다
중요한 질문이 있습니다.

```text
이 run은 최종 테이블에 반영됐는가?
같은 기간을 다시 실행해도 되는가?
어떤 dt partition이 이번 실행에서 만들어졌는가?
```

이 질문에 답하기 위해 각 실행의 상태와 처리 결과를 manifest JSON 파일로 남겼습니다.

```text
로그      = 실패 원인 디버깅용
manifest = 성공 여부, 처리 범위, 재실행 범위 판단용
```

## 저장 위치

Manifest는 결과 저장 루트 경로 아래 `_manifests` 디렉터리에 저장합니다.

```text
{output}/_manifests/{run_id}.json
```

예:

```text
data/lake/sessionized_events/_manifests/full-201910-201911-001.json
```

`_manifests`는 `_`로 시작하므로 실제 데이터 partition과 구분됩니다.

## 상태 전이

Manifest는 같은 파일을 상태에 따라 덮어씁니다.

```text
RUNNING -> SUCCESS
RUNNING -> FAILED
```

처리 순서:

```text
1. SparkSession 생성
2. RUNNING manifest 작성
3. CSV 읽기 / KST 기준 dt 생성 / 세션 계산
4. staging path에 Parquet/Snappy 저장
5. staging 결과를 다시 읽어 row_count와 dt partition 목록 검증
6. 검증된 결과를 _versions/{run_id} 경로로 이동
7. Hive partition location을 _versions/{run_id}/dt=... 경로로 전환
8. SUCCESS manifest 작성
```

위 순서의 `_versions/{run_id}` publish와 Hive partition location 전환은 Hive sync를 사용하는 실행 경로에
적용됩니다. Hive sync를 끈 로컬 샘플 실행은 Hive metadata를 변경하지 않고 파일시스템의 `dt` partition을
교체하는 방식으로 동작합니다.

catch 가능한 예외가 발생하면 `FAILED` manifest로 갱신합니다. 프로세스가 `kill -9`, OOM, 서버 장애처럼
강제로 종료되면 `FAILED` manifest를 쓸 기회가 없을 수 있습니다. 이 경우 `RUNNING`만 남고 `SUCCESS`가
없으므로 완료된 배치로 보지 않습니다.

## 필드

| Field | 의미 |
|---|---|
| `application` | 애플리케이션 이름 |
| `run_id` | 배치 실행 식별자 |
| `status` | `RUNNING`, `SUCCESS`, `FAILED` |
| `input` | 입력 경로 |
| `lookback_input` | 세션 경계 판단을 위해 함께 읽은 이전 기간 입력 경로. 없으면 `null` |
| `input_paths` | Spark가 실제로 함께 읽은 전체 CSV 경로 목록 |
| `output` | 결과 파일과 manifest를 저장한 경로 |
| `database` | Hive에서 사용할 database |
| `table` | Hive에서 사용할 table |
| `start_date` | 처리 시작일, inclusive |
| `end_date` | 처리 종료일, exclusive |
| `timezone` | `dt` partition을 나누는 기준 timezone |
| `session_gap_minutes` | 세션을 나누는 기준 시간 |
| `repartition_by_dt` | 저장 전에 `dt` 기준 repartition을 수행했는지 여부 |
| `row_count` | 성공 시 처리 결과 row 수 |
| `partitions` | 성공 시 생성 또는 교체된 `dt` partition 목록 |
| `started_at` | 실행 시작 시각 |
| `completed_at` | 성공 또는 실패 기록 시각 |
| `error_class` | 실패 시 예외 class |
| `error_message` | 실패 시 예외 message |

## 성공 manifest 예시

샘플 실행:

```bash
spark-submit \
  --master 'local[2]' \
  --driver-memory 2g \
  --class com.jaekwang.lakehouse.SessionizedEventLakehouseApp \
  target/scala-2.13/sessionized-event-lakehouse_2.13-0.1.0.jar \
  --input sample/sample_events.csv \
  --output data/lake/manifest_test \
  --start-date 2019-10-01 \
  --end-date 2019-10-03 \
  --run-id manifest-test-success-001 \
  --database default \
  --table sessionized_events_manifest_test
```

생성된 manifest:

```json
{
  "application": "sessionized-event-lakehouse",
  "run_id": "manifest-test-success-001",
  "status": "SUCCESS",
  "input": "sample/sample_events.csv",
  "lookback_input": null,
  "input_paths": ["sample/sample_events.csv"],
  "output": "data/lake/manifest_test",
  "database": "default",
  "table": "sessionized_events_manifest_test",
  "start_date": "2019-10-01",
  "end_date": "2019-10-03",
  "timezone": "Asia/Seoul",
  "session_gap_minutes": 5,
  "repartition_by_dt": true,
  "row_count": 9,
  "partitions": ["2019-10-01", "2019-10-02"],
  "started_at": "2026-05-25T09:09:59.796165Z",
  "completed_at": "2026-05-25T09:10:05.216342Z",
  "error_class": null,
  "error_message": null
}
```

## 실패 manifest 예시

없는 input 경로를 지정해 실패 manifest를 검증했습니다.

```bash
spark-submit \
  --master 'local[2]' \
  --driver-memory 2g \
  --class com.jaekwang.lakehouse.SessionizedEventLakehouseApp \
  target/scala-2.13/sessionized-event-lakehouse_2.13-0.1.0.jar \
  --input sample/missing_manifest_input.csv \
  --output data/lake/manifest_failed_test \
  --start-date 2019-10-01 \
  --end-date 2019-10-03 \
  --run-id manifest-test-failed-001 \
  --database default \
  --table sessionized_events_manifest_failed_test
```

생성된 manifest:

```json
{
  "application": "sessionized-event-lakehouse",
  "run_id": "manifest-test-failed-001",
  "status": "FAILED",
  "input": "sample/missing_manifest_input.csv",
  "lookback_input": null,
  "input_paths": ["sample/missing_manifest_input.csv"],
  "output": "data/lake/manifest_failed_test",
  "database": "default",
  "table": "sessionized_events_manifest_failed_test",
  "start_date": "2019-10-01",
  "end_date": "2019-10-03",
  "timezone": "Asia/Seoul",
  "session_gap_minutes": 5,
  "repartition_by_dt": true,
  "row_count": null,
  "partitions": [],
  "started_at": "2026-05-25T09:10:28.388218Z",
  "completed_at": "2026-05-25T09:10:29.946141Z",
  "error_class": "org.apache.spark.sql.AnalysisException",
  "error_message": "[PATH_NOT_FOUND] Path does not exist: file:/.../sample/missing_manifest_input.csv. SQLSTATE: 42K03"
}
```

## 복구 판단

```text
SUCCESS manifest 있음
=> 성공한 배치로 판단

FAILED manifest 있음
=> catch 가능한 실패가 발생한 배치로 판단

RUNNING manifest만 있음
=> 비정상 종료 가능성이 있으므로 완료된 배치로 보지 않음

manifest 없음
=> 시작 전 또는 기록 전 실패 가능성. 성공한 배치로 보지 않음
```

실패 또는 incomplete run은 같은 `start-date`, `end-date`, `input`, `output` 조건으로 다시 실행합니다.
재실행할 때는 새 `run_id`를 사용하는 것이 안전합니다. 동일 `dt` partition은 새 version path를 바라보게
되므로 중복 append를 피할 수 있습니다.

## run_id별 version path로 publish하는 이유

단순한 방식은 staging 결과를 최종 `dt` partition으로 바로 옮기는 것입니다. 하지만 기존 partition을 먼저
삭제한 뒤 새 결과로 이름을 바꾸는 방식은 publish 중 장애가 나면 기존 날짜 데이터가 비어 보이는 위험이
있습니다.

현재 구현은 이 위험을 줄이기 위해 결과를 staging path에 먼저 만들고, 검증이 끝난 뒤 run_id별 version path로
옮깁니다.

```text
staging path에 먼저 파일 저장
-> row 수 / dt partition 목록 검증
-> 검증된 결과를 _versions/{run_id} 경로로 이동
-> Hive partition location을 _versions/{run_id}/dt=... 로 전환
-> SUCCESS manifest
```

저장 경로는 다음 형태를 사용합니다.

```text
{output}/_staging/{run_id}
{output}/_versions/{run_id}
```

Hive external table의 각 `dt` partition은 `{output}/dt=...`를 직접 읽는 것이 아니라
`_versions/{run_id}/dt=...` 위치를 바라봅니다. `MSCK REPAIR TABLE`은 table root 아래의 `dt=...`
partition을 자동 발견하는 방식에 가깝지만, 이 구조에서는 재처리 안정성을 위해 partition별 `LOCATION`을
명시적으로 관리합니다.

이 구조는 기존 데이터를 먼저 삭제하지 않기 때문에 특정 날짜가 비어 보이는 위험을 줄입니다. 다만 여러
`dt` partition의 location을 전환하는 중 장애가 나면 일부 partition만 새 version path를 바라볼 수 있습니다.
그래서 `SUCCESS` manifest가 없는 실행은 완료된 배치로 보지 않고 재실행 대상으로 판단합니다.
