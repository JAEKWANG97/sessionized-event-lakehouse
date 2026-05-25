# Spark 애플리케이션 실행 인자 설계

이 애플리케이션은 특정 월이나 특정 파일명에 묶이지 않도록 만들었습니다. 처음에는 10월, 11월 CSV만
처리하면 충분해 보일 수 있지만, 과제 요구사항에는 추가 기간 처리도 포함되어 있습니다. 그래서 처리
대상은 코드가 아니라 실행 인자로 결정하도록 설계했습니다.

## 핵심 아이디어

```text
코드에는 처리 로직을 둔다.
처리 대상 파일, 기간, 테이블 이름은 실행 인자로 바꾼다.
```

10월과 11월을 함께 처리할 때는 다음처럼 실행합니다.

```bash
spark-submit \
  --class com.jaekwang.lakehouse.SessionizedEventLakehouseApp \
  target/scala-2.13/sessionized-event-lakehouse_2.13-0.1.0.jar \
  --input 'data/raw/extracted/2019-*.csv' \
  --output data/lake/sessionized_events \
  --database default \
  --table sessionized_events \
  --start-date 2019-10-01 \
  --end-date 2019-12-01 \
  --run-id full-201910-201911-001
```

나중에 12월 데이터가 추가되면 코드를 수정하지 않고 인자만 바꿉니다.

```bash
spark-submit \
  --class com.jaekwang.lakehouse.SessionizedEventLakehouseApp \
  target/scala-2.13/sessionized-event-lakehouse_2.13-0.1.0.jar \
  --input 'data/raw/extracted/2019-Dec.csv' \
  --output data/lake/sessionized_events \
  --database default \
  --table sessionized_events \
  --start-date 2019-12-01 \
  --end-date 2020-01-01 \
  --run-id full-201912-001
```

## 지원 인자

| 인자 | 필수 여부 | 예시 | 설명 |
|---|---:|---|---|
| `--input` | 필수 | `sample/sample_events.csv` | 입력 CSV 경로 또는 glob 패턴 |
| `--lookback-input` | 선택 | `sample/lookback_previous_period.csv` | 세션 경계 판단에 참고할 이전 기간 CSV 경로 또는 glob 패턴 |
| `--output` | 필수 | `data/lake/sessionized_events` | Parquet/Snappy 결과와 manifest가 저장될 lake root 경로 |
| `--start-date` | 필수 | `2019-10-01` | 처리할 KST `dt` partition 시작일. 포함합니다. |
| `--end-date` | 필수 | `2019-12-01` | 처리할 KST `dt` partition 종료일. 포함하지 않습니다. |
| `--run-id` | 필수 | `sample-001` | 배치 실행 ID |
| `--database` | 선택 | `default` | Hive database 이름. 기본값은 `default` |
| `--table` | 선택 | `sessionized_events` | Hive external table 이름. 기본값은 `sessionized_events` |
| `--enable-hive-sync` | 선택 | `true` | Hive external table과 partition location을 갱신할지 여부. 기본값은 `true` |
| `--repartition-by-dt` | 선택 | `true` | 저장 전에 `dt` 기준 repartition을 수행할지 여부. 기본값은 `true` |

## 날짜 범위는 KST `dt` 기준이다

날짜 필터는 원본 UTC 시간이 아니라 KST로 변환한 `dt` partition에 적용합니다.

```text
start-date <= dt < end-date
```

예를 들어 다음 인자는:

```text
--start-date 2019-10-01
--end-date 2019-12-01
```

KST 기준으로 `2019-10-01`부터 `2019-11-30`까지 저장한다는 뜻입니다.

`end-date`를 포함하지 않는 방식으로 잡은 이유는 기간을 이어 붙이기 쉽기 때문입니다.

```text
10월 처리: --start-date 2019-10-01 --end-date 2019-11-01
11월 처리: --start-date 2019-11-01 --end-date 2019-12-01
12월 처리: --start-date 2019-12-01 --end-date 2020-01-01
```

이렇게 하면 월별 배치를 이어서 실행해도 날짜가 겹치거나 빠지지 않습니다.

## run_id를 두는 이유

배치 작업은 중간에 실패할 수 있습니다. 예를 들어 Spark가 Parquet 파일을 쓰는 도중 실패하거나,
Hive partition location을 갱신하기 전에 종료될 수 있습니다.

그래서 모든 실행에 `run_id`를 부여합니다.

```text
- 결과 테이블의 run_id 컬럼
- staging path
- _versions/{run_id} path
- run manifest 파일명
- 장애 분석과 재처리 추적
```

예:

```text
run_id = sample-001
run_id = full-201910-201911-001
```

## 추가 기간 처리와 세션 경계 문제

추가 기간 처리는 단순히 새 파일을 읽는 문제만은 아닙니다. 세션화 로직은 `user_id`별 직전 이벤트와
현재 이벤트의 시간 차이를 봐야 하기 때문입니다.

예를 들어 어떤 사용자의 이벤트가 이렇게 있다고 하겠습니다.

```text
2019-11-30 23:58:00 UTC
2019-12-01 00:01:00 UTC
```

두 이벤트는 3분 차이이므로 같은 세션이어야 합니다. 그런데 12월 데이터만 따로 처리하면 12월 첫
이벤트는 11월 마지막 이벤트를 알 수 없습니다. 이 경우 잘못해서 새 세션으로 판단할 수 있습니다.

이 문제를 줄이기 위해 `--lookback-input`을 두었습니다.

```text
input과 lookback-input을 함께 읽고 세션을 계산한다.
마지막 저장 직전에 start-date <= dt < end-date 범위만 남긴다.
같은 기간을 재처리하면 해당 dt partition의 location을 새 version path로 전환한다.
```

`--lookback-input`은 이전 처리 상태를 저장해 이어 붙이는 완전한 stateful incremental 처리는 아닙니다.
이전 기간 데이터를 명시적으로 함께 읽는 방식입니다. 따라서 정확성을 위해서는 경계 세션이 포함될 만큼
충분한 이전 데이터를 `--lookback-input`으로 제공해야 합니다.

예를 들어 11월만 최종 저장하되 10월 말 이벤트와 이어지는 세션을 판단하려면 다음처럼 실행할 수 있습니다.

```bash
spark-submit \
  --class com.jaekwang.lakehouse.SessionizedEventLakehouseApp \
  target/scala-2.13/sessionized-event-lakehouse_2.13-0.1.0.jar \
  --input data/raw/2019-Nov.csv \
  --lookback-input data/raw/2019-Oct.csv \
  --output data/lake/sessionized_events \
  --start-date 2019-11-01 \
  --end-date 2019-12-01 \
  --run-id full-201911-001
```

이때 11월 `dt`에 저장되는 row라도 `session_start_at_utc`와 `session_start_at_kst`가 10월 말 시각을
가리킬 수 있습니다. 이는 경계 세션이 이전 기간에서 시작되었음을 나타내는 의도된 동작입니다.

운영 환경으로 확장한다면 `user_id`별 마지막 이벤트 시간과 마지막 세션 상태를 별도 state table로
관리하는 방식도 고려할 수 있습니다. 다만 과제 범위에서는 구현 복잡도를 과하게 키우지 않기 위해
명시적인 lookback 입력 방식으로 경계 세션을 방어했습니다.

## 샘플 실행

```bash
spark-submit \
  --class com.jaekwang.lakehouse.SessionizedEventLakehouseApp \
  target/scala-2.13/sessionized-event-lakehouse_2.13-0.1.0.jar \
  --input sample/sample_events.csv \
  --output data/lake/sessionized_events \
  --database default \
  --table sessionized_events \
  --start-date 2019-10-01 \
  --end-date 2019-10-03 \
  --run-id sample-001 \
  --enable-hive-sync false \
  --repartition-by-dt true
```

Hive sync를 끈 샘플 실행에서는 Hive metadata를 갱신하지 않고 파일시스템의 `dt` partition을 교체합니다.
Hive sync를 사용하는 실행에서는 각 `dt` partition이 `_versions/{run_id}/dt=...` 경로를 바라보도록
partition location을 갱신합니다.

```text
data/lake/sessionized_events/dt=2019-10-01/
data/lake/sessionized_events/dt=2019-10-02/
```

## 정리

| 요구사항 | 설계 반영 |
|---|---|
| 추가 기간 처리 대응 | `--input`, `--lookback-input`, `--start-date`, `--end-date` |
| KST 기준 daily partition | 날짜 필터를 KST `dt` 기준으로 적용 |
| 재처리 가능 | 같은 기간 재실행 시 Hive partition location을 새 version path로 전환 |
| 배치 장애 복구 | `run_id`, run manifest, staging path, version path |
| Hive external table | `--output`, `--database`, `--table`로 위치와 테이블명 분리 |
