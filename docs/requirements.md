# 요구사항 정리

이 문서는 과제 요구사항이 프로젝트 안에서 어디에 반영되어 있는지 빠르게 확인하기 위한 체크리스트입니다.
README에는 핵심 흐름과 결과를 두고, 여기서는 요구사항별 구현 위치를 따로 정리했습니다.

## 입력 데이터

- `2019-Oct.csv`
- `2019-Nov.csv`
- Kaggle: Ecommerce behavior data from multi category store

## 요구사항 체크리스트

| 요구사항 | 구현/문서 위치 |
|---|---|
| 사용자 activity 로그를 Hive table로 제공하기 위한 Spark Application 작성 | `src/main/scala/com/jaekwang/lakehouse/SessionizedEventLakehouseApp.scala` |
| KST 기준 daily partition 처리 | `Sessionization.scala`, KST 기준 `dt` partition 생성 |
| 동일 `user_id` 내에서 `event_time` 간격이 5분 이상이면 새 세션 ID 생성 | `Sessionization.scala`, window function으로 이전 이벤트와 현재 이벤트 비교 |
| 재처리 후 Parquet/Snappy 처리 | `LakeWriter.scala`, `docs/period-extension-verification.md` |
| External Table 방식 설계 | `sql/create_external_table.sql`, `docs/table-schema.md` |
| 추가 기간 처리에 대응 가능하도록 구현 | 실행 인자 `--input`, `--lookback-input`, `--start-date`, `--end-date`, `docs/application-arguments.md` |
| 배치 장애 시 복구를 위한 장치 구현 | `RunManifestWriter.scala`, staging path, run manifest, version path, `docs/run-manifest.md` |
| Hive external table을 이용한 WAU 계산 | `sql/wau_by_user.sql`, `sql/wau_by_session.sql`, README WAU 결과 |
| `user_id` 기준 WAU 계산 | README `WAU 계산 쿼리` |
| 생성된 session ID 기준 WAU 계산 | README `WAU 계산 쿼리` |
| 계산에 사용한 쿼리와 결과값 제출 | README `WAU 계산 쿼리`, `sql/` |
| Scala 또는 Java 사용 | Scala 사용, README `Scala 선택 이유` |
| 사용한 AI 도구, 활용 범위, 프롬프트 전략 작성 | README `AI 도구 사용 내역` |

## 읽는 순서

처음 확인할 때는 README를 먼저 읽고, 세부 근거가 필요한 부분만 아래 문서로 이동하면 됩니다.

```text
요구사항 대응      -> docs/requirements.md
실행 인자와 기간   -> docs/application-arguments.md
Hive table 설계    -> docs/table-schema.md
재처리/장애 복구   -> docs/run-manifest.md
추가 기간 검증     -> docs/period-extension-verification.md
코드 구조          -> docs/code-structure.md
```
