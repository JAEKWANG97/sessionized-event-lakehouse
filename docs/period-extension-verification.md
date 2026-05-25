# 추가 기간 처리 검증

## 목적

`External Table 방식으로 설계 하고, 추가 기간 처리에 대응가능하도록 구현` 요구사항을
샘플 데이터로 검증한다.

검증 포인트는 다음 두 가지다.

1. 새로운 기간을 추가 처리할 때 기존 `dt` partition이 유지되는가
2. 이미 존재하는 기간을 다시 처리할 때 중복 append가 아니라 대상 partition만 교체되는가

## 전제

애플리케이션은 입력 경로, 출력 경로, 처리 기간, Hive database/table 이름을 실행 인자로 받는다.

저장은 `dt` 기준 partition write를 사용한다.
애플리케이션은 결과를 최종 경로가 아닌 임시 저장 경로에 먼저 쓴 뒤,
임시 저장 경로의 Parquet를 다시 읽어 row count와 partition 목록을 확인한다.

검증이 끝나면 이번 실행 결과에 포함된 `dt` partition만 최종 경로로 이동한다.
이 방식의 의도는 전체 output root를 매번 삭제하는 것이 아니라, 이번 실행 결과에
포함된 `dt` partition만 추가 또는 교체하는 것이다.

```text
{output}/_staging/{run_id}/dt=yyyy-MM-dd
-> 검증
-> {output}/dt=yyyy-MM-dd
```

## 검증 데이터

사용한 샘플 파일:

```text
sample/sample_events.csv
sample/additional_period_2019_10_03.csv
sample/reprocess_2019_10_02.csv
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

기존 `2019-10-01`, `2019-10-02` partition이 유지되고, 새 `2019-10-03` partition이 추가되었다.

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

`2019-10-02` partition은 기존 3건에 새 2건이 append되어 5건이 된 것이 아니라, 재처리 결과 2건으로 교체되었다.

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

파일 시스템에서도 동일한 partition 경로가 확인된다.

```text
data/lake/period_extension_test
data/lake/period_extension_test/dt=2019-10-01
data/lake/period_extension_test/dt=2019-10-02
data/lake/period_extension_test/dt=2019-10-03
```

## 결론

샘플 검증 결과, 현재 구현은 추가 기간 처리와 동일 기간 재처리 모두에서 의도한 방식으로 동작했다.

- 추가 기간 처리: 기존 partition 유지 + 새 partition 추가
- 동일 기간 재처리: 대상 partition만 교체, 중복 append 없음
- Hive external table: `MSCK REPAIR TABLE` 이후 새 partition 조회 가능
