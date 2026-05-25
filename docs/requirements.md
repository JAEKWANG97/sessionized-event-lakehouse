# 요구사항 정리

이 문서는 README가 너무 길어지지 않도록, 요구사항 항목과 현재
프로젝트에서의 반영 위치를 따로 정리한 문서다.

## 입력 데이터

- `2019-Oct.csv`
- `2019-Nov.csv`
- Kaggle: Ecommerce behavior data from multi category store

## 요구사항 체크리스트

| 요구사항 | 구현/문서 위치 |
|---|---|
| 사용자 activity 로그를 Hive table로 제공하기 위한 Spark Application 작성 | `src/main/scala/com/jaekwang/lakehouse/SessionizedEventLakehouseApp.scala` |
| KST 기준 daily partition 처리 | `Sessionization.scala`, `dt` partition |
| 동일 `user_id` 내에서 `event_time` 간격이 5분 이상이면 새 세션 ID 생성 | `Sessionization.scala` window function |
| 재처리 후 Parquet/Snappy 처리 | `LakeWriter.scala`, `docs/period-extension-verification.md` |
| External Table 방식 설계 | `sql/create_external_table.sql`, `docs/table-schema.md` |
| 추가 기간 처리에 대응 가능하도록 구현 | 실행 인자 `--input`, `--lookback-input`, `--start-date`, `--end-date`, `docs/application-arguments.md` |
| 배치 장애 시 복구를 위한 장치 구현 | `RunManifestWriter.scala`, staging 후 publish 구조, `docs/run-manifest.md` |
| Hive external table을 이용한 WAU 계산 | `sql/wau_by_user.sql`, `sql/wau_by_session.sql`, README WAU 결과 |
| `user_id` 기준 WAU 계산 | README `WAU 계산 쿼리` |
| 생성된 session ID 기준 WAU 계산 | README `WAU 계산 쿼리` |
| 계산에 사용한 쿼리와 결과값 제출 | README `WAU 계산 쿼리`, `sql/` |
| Scala 또는 Java 사용 | Scala 사용, README `Scala 선택 이유` |
| 사용한 AI 도구, 활용 범위, 프롬프트 전략 작성 | README `AI 도구 사용 내역` |
