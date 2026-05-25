# 추가 기간 처리 검증

## 목적

External Table 방식과 추가 기간 처리를 샘플 데이터로 검증한다. 여기서 external
table은 Spark가 만든 파일 위치를 Hive에 등록해 SQL로 읽는 방식이다.

검증 포인트는 다음 세 가지다.

1. 새로운 기간을 추가 처리할 때 기존 `dt` 날짜 구간이 유지되는가
2. 이미 존재하는 기간을 다시 처리할 때 중복 append가 아니라 대상 날짜 구간만 교체되는가
3. 기간 경계에서 이어지는 세션을 `--lookback-input`으로 끊기지 않게 계산할 수 있는가

## 전제

애플리케이션은 입력 경로, 출력 경로, 처리 기간, Hive database/table 이름을 실행 인자로 받는다.

저장은 `dt` 기준 날짜별 저장을 사용한다.
애플리케이션은 결과를 최종 경로가 아닌 임시 저장 경로에 먼저 쓴 뒤,
임시 저장 경로의 Parquet를 다시 읽어 row 수와 날짜 구간 목록을 확인한다.

검증이 끝나면 결과를 `_versions/{run_id}` 경로로 이동하고,
이번 실행 결과에 포함된 `dt` 날짜 구간을 Hive가 읽는 위치를 새 보관 경로로 전환한다.
이 방식의 의도는 결과 저장 루트 경로 전체를 매번 삭제하는 것이 아니라, 이번 실행 결과에
포함된 `dt` 날짜 구간만 추가 또는 전환하는 것이다.

```text
{output}/_staging/{run_id}/dt=yyyy-MM-dd
-> 검증
-> {output}/_versions/{run_id}/dt=yyyy-MM-dd
-> ALTER TABLE ... PARTITION (dt='yyyy-MM-dd') SET LOCATION ...
```

## 검증 데이터

사용한 샘플 파일:

```text
sample/sample_events.csv
sample/additional_period_2019_10_03.csv
sample/reprocess_2019_10_02.csv
sample/lookback_previous_period.csv
sample/lookback_target_period.csv
```

검증용 output/table:

```text
output: data/lake/period_extension_test
table:  default.sessionized_events_period_test
```

## 1차 실행: 초기 기간 적재

```bash
spark-submit \
  --master 'local[2]' \
  --driver-memory 2g \
  --class com.jaekwang.lakehouse.SessionizedEventLakehouseApp \
  target/scala-2.13/sessionized-event-lakehouse_2.13-0.1.0.jar \
  --input sample/sample_events.csv \
  --output data/lake/period_extension_test \
  --start-date 2019-10-01 \
  --end-date 2019-10-03 \
  --run-id period-test-initial \
  --database default \
  --table sessionized_events_period_test
```

검증 쿼리:

```sql
SELECT dt, count(*) AS rows, collect_set(run_id) AS run_ids
FROM default.sessionized_events_period_test
GROUP BY dt
ORDER BY dt;
```

결과:

| dt | rows | run_ids |
|---|---:|---|
| 2019-10-01 | 6 | `["period-test-initial"]` |
| 2019-10-02 | 3 | `["period-test-initial"]` |

## 2차 실행: 새 기간 추가

```bash
spark-submit \
  --master 'local[2]' \
  --driver-memory 2g \
  --class com.jaekwang.lakehouse.SessionizedEventLakehouseApp \
  target/scala-2.13/sessionized-event-lakehouse_2.13-0.1.0.jar \
  --input sample/additional_period_2019_10_03.csv \
  --output data/lake/period_extension_test \
  --start-date 2019-10-03 \
  --end-date 2019-10-04 \
  --run-id period-test-add-20191003 \
  --database default \
  --table sessionized_events_period_test
```

결과:

| dt | rows | run_ids |
|---|---:|---|
| 2019-10-01 | 6 | `["period-test-initial"]` |
| 2019-10-02 | 3 | `["period-test-initial"]` |
| 2019-10-03 | 3 | `["period-test-add-20191003"]` |

기존 `2019-10-01`, `2019-10-02` 날짜 구간이 유지되고, 새 `2019-10-03` 날짜 구간이 추가되었다.

## 3차 실행: 기존 기간 재처리

```bash
spark-submit \
  --master 'local[2]' \
  --driver-memory 2g \
  --class com.jaekwang.lakehouse.SessionizedEventLakehouseApp \
  target/scala-2.13/sessionized-event-lakehouse_2.13-0.1.0.jar \
  --input sample/reprocess_2019_10_02.csv \
  --output data/lake/period_extension_test \
  --start-date 2019-10-02 \
  --end-date 2019-10-03 \
  --run-id period-test-reprocess-20191002 \
  --database default \
  --table sessionized_events_period_test
```

검증 쿼리:

```sql
SELECT
  dt,
  count(*) AS rows,
  collect_set(run_id) AS run_ids,
  min(price) AS min_price,
  max(price) AS max_price
FROM default.sessionized_events_period_test
GROUP BY dt
ORDER BY dt;
```

결과:

| dt | rows | run_ids | min_price | max_price |
|---|---:|---|---:|---:|
| 2019-10-01 | 6 | `["period-test-initial"]` | 100.00 | 700.00 |
| 2019-10-02 | 2 | `["period-test-reprocess-20191002"]` | 31.00 | 31.00 |
| 2019-10-03 | 3 | `["period-test-add-20191003"]` | 80.00 | 80.00 |

`2019-10-02` 날짜 구간은 기존 3건에 새 2건이 append되어 5건이 된 것이 아니라, 재처리 결과 2건으로 교체되었다.

## 4차 실행: lookback input으로 기간 경계 세션 검증

이 검증은 최종 저장 대상 기간의 row가 이전 기간 이벤트와 같은 세션으로 이어질 수
있는지를 확인한다.

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

검증 쿼리:

```sql
SELECT
  product_id,
  user_id,
  session_seq,
  session_event_seq,
  date_format(session_start_at_kst, 'yyyy-MM-dd HH:mm:ss') AS session_start_at_kst,
  run_id
FROM parquet.`data/lake/lookback_boundary_test/dt=2019-11-01`;
```

결과:

| product_id | user_id | session_seq | session_event_seq | session_start_at_kst | run_id |
|---:|---:|---:|---:|---|---|
| 8002 | 800 | 1 | 2 | 2019-10-31 23:58:00 | `lookback-boundary-001` |

최종 저장된 row는 `dt=2019-11-01`의 1건뿐이다. 하지만 `session_event_seq=2`이고
`session_start_at_kst`가 최종 저장 범위 이전인 `2019-10-31 23:58:00`을 가리킨다.
즉 lookback input의 이전 이벤트를 세션 계산에 사용했고, 최종 저장은 요청한
KST `dt` 범위만 수행했다.

## Hive partition 확인

```sql
SHOW PARTITIONS default.sessionized_events_period_test;
```

결과:

```text
dt=2019-10-01
dt=2019-10-02
dt=2019-10-03
```

파일 시스템에서는 run_id별 보관 경로가 확인된다.

```text
data/lake/period_extension_test
data/lake/period_extension_test/_versions/period-test-initial/dt=2019-10-01
data/lake/period_extension_test/_versions/period-test-reprocess-20191002/dt=2019-10-02
data/lake/period_extension_test/_versions/period-test-add-20191003/dt=2019-10-03
```

## 결론

샘플 검증 결과, 현재 구현은 추가 기간 처리와 동일 기간 재처리 모두에서 의도한 방식으로 동작했다.

- 추가 기간 처리: 기존 날짜 구간 유지 + 새 날짜 구간 추가
- 동일 기간 재처리: 대상 날짜 구간을 Hive가 읽는 위치만 새 보관 경로로 전환, 중복 append 없음
- 기간 경계 세션: `--lookback-input`으로 이전 기간 이벤트를 함께 읽어 세션 단절 방지
- Hive external table: `ALTER TABLE ADD/SET PARTITION LOCATION` 이후 새 날짜 구간 조회 가능
