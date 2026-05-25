# Run Manifest

## 목적

배치 장애 시 복구 판단을 돕기 위해 각 실행의 상태와 처리 결과를 manifest JSON 파일로 남긴다.

Manifest는 일반 실행 로그가 아니라, 배치 실행 결과를 구조화해서 남기는 실행 요약 파일이다.

```text
로그      = 실패 원인 디버깅용
manifest = 성공 여부, 처리 범위, 재실행 범위 판단용
```

## 저장 위치

Manifest는 output root 아래 `_manifests` 디렉터리에 저장한다.

```text
{output}/_manifests/{run_id}.json
```

예:

```text
data/lake/sessionized_events/_manifests/full-201910-201911-001.json
```

`_manifests`는 `_`로 시작하므로 Hive partition repair 대상에서 제외된다.
샘플 검증에서도 `MSCK REPAIR TABLE` 실행 시 `_manifests` 디렉터리가 partition으로 등록되지 않는 것을 확인했다.

## 상태 전이

Manifest는 같은 파일을 상태에 따라 덮어쓴다.

```text
RUNNING -> SUCCESS
RUNNING -> FAILED
```

처리 순서:

```text
1. SparkSession 생성
2. RUNNING manifest 작성
3. CSV read / KST partition / sessionization
4. 최종 경로가 아닌 임시 저장 경로에 Parquet/Snappy write
5. 임시 저장 결과를 다시 읽어 row_count, partition 목록 검증
6. 검증된 partition만 최종 경로로 이동
7. Hive external table sync
8. SUCCESS manifest 작성
```

catch 가능한 예외가 발생하면 `FAILED` manifest로 갱신한다.

프로세스가 `kill -9`, OOM, 서버 장애처럼 강제로 종료되면 `FAILED` manifest를 쓸 기회가 없을 수 있다.
이 경우 `RUNNING`만 남고 `SUCCESS`가 없으므로 완료된 배치로 보지 않는다.

## 필드

| Field | 의미 |
|---|---|
| `application` | 애플리케이션 이름 |
| `run_id` | 배치 실행 식별자 |
| `status` | `RUNNING`, `SUCCESS`, `FAILED` |
| `input` | 입력 경로 |
| `output` | 출력 경로 |
| `database` | Hive database |
| `table` | Hive table |
| `start_date` | 처리 시작일, inclusive |
| `end_date` | 처리 종료일, exclusive |
| `timezone` | partition 기준 timezone |
| `session_gap_minutes` | 세션 분리 기준 |
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
  "output": "data/lake/manifest_test",
  "database": "default",
  "table": "sessionized_events_manifest_test",
  "start_date": "2019-10-01",
  "end_date": "2019-10-03",
  "timezone": "Asia/Seoul",
  "session_gap_minutes": 5,
  "row_count": 9,
  "partitions": ["2019-10-01", "2019-10-02"],
  "started_at": "2026-05-25T09:09:59.796165Z",
  "completed_at": "2026-05-25T09:10:05.216342Z",
  "error_class": null,
  "error_message": null
}
```

## 실패 manifest 예시

없는 input 경로를 지정해 실패 manifest를 검증했다.

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
  "output": "data/lake/manifest_failed_test",
  "database": "default",
  "table": "sessionized_events_manifest_failed_test",
  "start_date": "2019-10-01",
  "end_date": "2019-10-03",
  "timezone": "Asia/Seoul",
  "session_gap_minutes": 5,
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

실패 또는 incomplete run은 같은 `start-date`, `end-date`, `input`, `output` 조건으로 다시 실행한다.
동일 `dt` partition은 검증된 결과로 교체되므로 중복 append를 피할 수 있다.

## 임시 저장 경로 기반 반영

현재 구현은 중간 실패 결과가 최종 테이블에 노출되는 것을 줄이기 위해
최종 경로가 아닌 임시 저장 경로에 먼저 결과를 만든다.

```text
최종 경로가 아닌 임시 저장 경로에 먼저 write
-> row count / partition 검증
-> 검증이 끝난 partition만 최종 경로로 이동
-> Hive sync
-> SUCCESS manifest
```

임시 저장 경로는 다음 형태를 사용한다.

```text
{output}/_staging/{run_id}
```

`_staging`은 `_`로 시작하므로 Hive partition repair 대상에서 제외된다.
실패한 실행의 임시 저장 경로가 남더라도 `SUCCESS` manifest가 없으면
완료된 배치로 보지 않는다.
