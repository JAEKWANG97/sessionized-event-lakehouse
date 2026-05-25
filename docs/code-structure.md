# Spark 코드 구조

역할이 다른 코드는 파일을 나눠서 관리한다. 단일 파일로도 구현할 수 있지만, 이 프로젝트에서는 읽는 사람이 처리 흐름과 책임을 쉽게 따라갈 수 있도록 최소한의 역할 분리를 둔다.

## 현재 파일 구조

```text
src/main/scala/com/jaekwang/lakehouse/
  SessionizedEventLakehouseApp.scala
  config/AppConfig.scala
  schema/EventSchema.scala
  transform/Sessionization.scala
  io/LakeWriter.scala
```

## 파일별 역할

### SessionizedEventLakehouseApp.scala

애플리케이션의 진입점이다.

실행 인자를 읽고, SparkSession을 만들고, 전체 처리 흐름을 연결한다.

```text
CSV 읽기 → 세션화 → 날짜 필터 → 임시 저장 경로에 Parquet 저장 → 검증 → 최종 partition 반영 → Hive table sync
```

이 파일에는 세부 변환 로직을 많이 넣지 않고, 파이프라인의 큰 흐름만 남긴다.

### config/AppConfig.scala

실행 인자를 파싱한다.

현재 지원하는 주요 인자는 다음과 같다.

```text
--input
--output
--start-date
--end-date
--run-id
--database
--table
```

처리 기간과 입출력 경로를 코드에 박아두지 않기 위해 별도 config로 분리했다.

### schema/EventSchema.scala

원본 CSV 스키마를 정의한다.

Spark가 CSV를 추론하게 두면 실행마다 타입이 흔들릴 수 있으므로, 명시적인 schema를 사용한다.

### transform/Sessionization.scala

프로젝트의 핵심 로직이다.

이 파일에서 다음 작업을 수행한다.

```text
1. event_time을 UTC timestamp로 파싱
2. KST timestamp와 dt partition 생성
3. user_id별 event_time 순서 정렬
4. 직전 이벤트와 5분 이상 차이나면 새 세션으로 판단
5. session_seq와 generated_session_id 생성
```

### io/LakeWriter.scala

결과를 lake path에 저장하고 Hive external table을 동기화한다.
최종 경로를 바로 쓰지 않고, 먼저 임시 저장 경로에 쓴 뒤 row count와
partition 목록을 확인하고 검증된 `dt` partition만 최종 경로로 이동한다.

저장 형식은 다음과 같다.

```text
Parquet + Snappy
partitionBy("dt")
```

## 중요한 설계 보정

처음에는 `generated_session_id = sha2(user_id|session_seq)`로 생각했지만, 추가 기간을 따로 처리하면 같은 user_id의 `session_seq=1`이 여러 기간에서 반복될 수 있다.

그래서 구현에서는 세션 시작 시간을 함께 넣는다.

```text
generated_session_id = sha2(user_id|session_start_at_utc|session_seq)
```

이렇게 하면 세션 ID가 해시값이라는 장점은 유지하면서도, 기간을 나눠 처리할 때 충돌 가능성을 줄일 수 있다.
