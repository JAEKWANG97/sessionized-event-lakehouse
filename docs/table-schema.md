# Hive 테이블 스키마 설계

이 프로젝트의 핵심 결과물은 Hive external table이다. Spark가 CSV 로그를 읽고, KST 기준 날짜와 새 세션 ID를 붙인 뒤, Parquet/Snappy 파일로 저장한다. Hive table은 이 파일 위치를 바라보면서 SQL로 조회할 수 있게 해준다.

테이블 이름은 다음으로 정한다.

```text
sessionized_events
```

이름 그대로 “세션화된 이벤트 로그”를 담는 테이블이다.

## 왜 원본 컬럼명을 그대로 쓰지 않는가

원본 CSV에는 `event_time`과 `user_session`이라는 컬럼이 있다.

하지만 결과 테이블에서는 이 둘을 그대로 쓰지 않고 이름을 바꾼다.

```text
event_time      -> event_time_utc
user_session    -> source_user_session
```

이유는 명확성을 위해서다.

`event_time`은 실제로 `UTC`가 붙은 문자열이다. 이후 KST로 변환한 시간도 함께 저장할 것이므로, 원본 시간은 `event_time_utc`라고 부르는 게 안전하다.

또한 `user_session`은 Kaggle 데이터에 원래 들어 있는 세션 ID다. 하지만 과제에서는 `event_time` 간격이 5분 이상이면 새 세션 ID를 생성하라고 했다. 따라서 원본 `user_session`과 우리가 생성한 `generated_session_id`를 구분해야 한다.

## 최종 스키마

| 컬럼 | 타입 | 설명 |
|---|---|---|
| `event_time_utc` | timestamp | 원본 `event_time`을 UTC timestamp로 파싱한 값 |
| `event_time_kst` | timestamp | `event_time_utc`를 Asia/Seoul 기준으로 변환한 값 |
| `event_type` | string | `view`, `cart`, `purchase`, `remove_from_cart` 등 이벤트 타입 |
| `product_id` | bigint | 상품 ID |
| `category_id` | bigint | 카테고리 ID |
| `category_code` | string | 카테고리 코드. 빈 값 가능 |
| `brand` | string | 브랜드. 빈 값 가능 |
| `price` | decimal(18,2) | 상품 가격 |
| `user_id` | bigint | 사용자 ID. user 기준 WAU와 세션화 기준 |
| `source_user_session` | string | 원본 CSV의 `user_session`. 추적용으로 보존 |
| `generated_session_id` | string | 5분 inactivity rule에 따라 새로 생성한 세션 ID |
| `session_seq` | bigint | user별 세션 순번 |
| `session_start_at_utc` | timestamp | 생성된 세션의 UTC 기준 시작 시각 |
| `session_start_at_kst` | timestamp | 생성된 세션의 KST 기준 시작 시각 |
| `session_event_seq` | bigint | 생성된 세션 안에서 이벤트 순번 |
| `ingested_at` | timestamp | 배치 처리 시각 |
| `run_id` | string | 배치 실행 ID |
| `dt` | string | KST 기준 날짜 partition |

`dt`는 일반 컬럼이 아니라 partition 컬럼으로 둔다.

## generated_session_id 생성 방식

세션 ID는 사람이 읽기 쉬운 `user_id_sessionSeq` 형태가 아니라 SHA-256 해시값으로 만든다.

개념식은 다음과 같다.

```text
generated_session_id = sha2(concat_ws('|', user_id, session_start_at_utc, session_seq), 256)
```

이 방식을 선택한 이유는 다음과 같다.

```text
1. 같은 데이터를 재처리해도 같은 session_id가 나온다.
2. downstream query에서 안정적으로 사용할 수 있다.
3. user_id와 session_seq를 그대로 붙인 값을 대표 ID로 노출하지 않아도 된다.
4. 세션 시작 시각을 함께 사용해 기간을 나눠 처리할 때 session_seq가 반복되어 생길 수 있는 충돌 가능성을 줄인다.
5. 디버깅은 session_seq와 session_start_at_utc 컬럼으로 할 수 있다.
```

## 세션 경계 조건

동일 `user_id` 안에서 이벤트를 시간순으로 정렬한다. 그리고 직전 이벤트와 현재 이벤트의 차이를 계산한다.

새 세션이 시작되는 조건은 다음과 같다.

```text
previous event is null
OR current event_time_utc - previous event_time_utc >= 300 seconds
```

요구사항이 “5분 이상”이므로 정확히 5분 차이도 새 세션이다.

## Hive DDL 초안

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS sessionized_events (
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
PARTITIONED BY (
  dt string
)
STORED AS PARQUET
LOCATION '${output_path}';
```

## dt를 string으로 둔 이유

`dt`는 KST 기준 날짜다.

예:

```text
dt=2019-10-01
```

Hive external table에서는 partition 컬럼을 `string`으로 두는 방식이 가장 무난하다. 실제 파일 경로도 다음처럼 문자열 기반 partition directory로 만들어진다.

```text
data/lake/sessionized_events/dt=2019-10-01/
data/lake/sessionized_events/dt=2019-10-02/
```

`date` 타입도 가능하지만, Spark/Hive/Trino 호환성과 partition path 관점에서는 `string`이 단순하고 안정적이다.

## 이 테이블로 계산할 지표

user 기준 WAU:

```sql
COUNT(DISTINCT user_id)
```

생성된 세션 기준 WAU:

```sql
COUNT(DISTINCT generated_session_id)
```

두 번째 지표는 엄밀히 말하면 “주간 활성 세션 수”에 가깝다. 다만 과제 요구사항에서 생성된 세션 ID 기준 WAU를 요구하므로, 쿼리와 결과를 함께 제공한다.
