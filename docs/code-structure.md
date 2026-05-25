# Spark 코드 구조

이 프로젝트는 한 파일에 모든 로직을 몰아넣기보다, 설명 가능한 단위로만 역할을 나눴습니다.
면접에서 코드를 따라 설명해야 한다면 아래 순서대로 보면 전체 흐름이 가장 자연스럽습니다.

## 현재 파일 구조

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
```

## 파일별 역할

### SessionizedEventLakehouseApp.scala

애플리케이션의 진입점입니다. 실행 인자를 읽고, SparkSession을 만들고, 전체 파이프라인을 연결합니다.

```text
CSV 읽기
-> 세션 계산
-> KST dt 범위 필터
-> Parquet/Snappy 저장
-> run manifest 기록
```

세부 변환 로직은 이 파일에 많이 넣지 않았습니다. 이 파일은 “어떤 순서로 처리하는가”를 보여주는
역할에 가깝습니다.

### config/AppConfig.scala

실행 인자를 `AppConfig`로 바꿉니다. 처리 대상 파일과 기간을 코드에 박아두지 않기 위해 분리했습니다.

주요 인자:

```text
--input
--lookback-input
--output
--start-date
--end-date
--run-id
--database
--table
--enable-hive-sync
--repartition-by-dt
```

`--lookback-input`이 있으면 `input + lookback-input`을 함께 읽습니다. 다만 최종 저장은
`start-date <= dt < end-date` 범위만 수행합니다.

### schema/EventSchema.scala

원본 CSV 스키마를 명시합니다. Spark가 CSV 타입을 자동 추론하게 두면 실행 환경이나 샘플에 따라
타입이 흔들릴 수 있으므로, 과제 데이터의 컬럼 타입을 코드에서 고정했습니다.

### transform/Sessionization.scala

과제의 핵심 로직입니다.

```text
1. event_time을 UTC timestamp로 파싱
2. KST timestamp와 KST 기준 dt partition 생성
3. user_id별 event_time 순서로 정렬
4. lag로 직전 이벤트 시간을 가져옴
5. 직전 이벤트와 5분 이상 차이나면 새 세션으로 판단
6. 누적 sum으로 session_seq 생성
7. user_id + session_start_at_utc 기반으로 generated_session_id 생성
```

여기서 `session_seq`는 사람이 검증하기 위한 보조 컬럼입니다. `--lookback-input`을 사용하면 입력
범위에 따라 `session_seq`가 달라질 수 있으므로, 실제 식별자로 쓰는 `generated_session_id`에는
넣지 않았습니다.

### io/LakeWriter.scala

결과를 lake path에 저장하고, Hive external table이 읽는 partition location을 갱신합니다.

```text
staging path에 Parquet/Snappy 저장
-> row 수와 dt partition 목록 검증
-> Hive sync 실행이면 _versions/{run_id}로 이동
-> Hive partition location을 새 version path로 전환
```

Hive sync를 끈 로컬 샘플 실행에서는 Hive metadata를 갱신하지 않고 파일시스템의 `dt` partition을
교체합니다. 제출 문서에서 설명하는 versioned partition location 전환은 Hive sync 실행 경로 기준입니다.

### io/RunManifestWriter.scala

배치 실행 상태를 manifest JSON으로 남깁니다. 로그는 실패 원인을 보는 데 유용하지만,
“이 run이 최종 테이블에 반영됐는가”, “같은 기간을 다시 실행해도 되는가”를 판단하기에는 부족합니다.
그래서 `RUNNING`, `SUCCESS`, `FAILED` 상태와 처리 범위, row 수, partition 목록을 구조화해서 기록합니다.

### transform/SessionizationSpec.scala

세션화 규칙을 Spark local mode로 검증하는 테스트입니다.

검증하는 핵심 경계:

```text
4분 59초 간격은 같은 세션
정확히 5분 간격은 새 세션
user_id가 다르면 세션 순번은 독립적으로 계산
UTC event_time에서 KST 기준 dt partition 생성
원본 user_session이 달라도 generated_session_id는 5분 간격 기준으로 결정
lookback 이벤트가 최종 저장 범위 이전 session_start를 만들 수 있음
```

### config/AppConfigSpec.scala

실행 인자 파싱을 검증합니다. 특히 `--lookback-input`이 실제 CSV read path에 포함되는지,
`--repartition-by-dt false`가 의도대로 해석되는지 확인합니다.

## 면접에서 설명할 핵심 보정

처음에는 `generated_session_id = sha2(user_id | session_seq)`처럼 단순하게 생각할 수 있습니다.
하지만 추가 기간을 따로 처리하거나 lookback 입력을 붙이면 같은 실제 세션의 `session_seq`가 달라질 수
있습니다.

그래서 최종 구현에서는 다음 값을 사용했습니다.

```text
generated_session_id = sha2(user_id | session_start_at_utc)
```

`session_seq`는 사람이 세션 순서를 확인하기 위한 보조 컬럼으로 남기고, 다음 분석이나 쿼리에서 쓰는
식별자는 `user_id`와 세션 시작 시각 기반으로 만들었습니다.
