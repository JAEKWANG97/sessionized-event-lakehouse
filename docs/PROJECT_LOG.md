## 2026-05-25 18:48 KST

- 작업: 배치 장애 복구 요구사항의 구현 상태를 재점검하고, 현재 구현과 추가 구현이 필요한 범위를 구분했다.
- 문제·고민: `배치 장애시 복구를 위한 장치 구현`을 자동 복구까지 의미하는지, 또는 안전한 재실행 구조를 의미하는지 해석이 필요했다.
- 해결방안: Spark Application 내부에서는 run manifest, deterministic session id, dynamic partition overwrite로 실패/미완료 배치를 식별하고 재실행 시 중복을 피하는 데 집중한다고 정리했다.
- 결정: 현재 구현은 최소 복구 장치로 볼 수 있으나, 중간 실패 결과가 최종 테이블에 노출되는 것을 더 강하게 막으려면 임시 저장 경로에 먼저 쓰고 검증 후 최종 partition으로 반영하는 구조를 추가하는 것이 좋다고 판단했다.
- 후속작업: `LakeWriter`에 임시 저장 경로 쓰기, 검증, 최종 partition 교체 흐름을 구현하고 샘플 데이터로 성공/재처리 케이스를 검증한다.

## 2026-05-25 18:53 KST

- 작업: `LakeWriter`에 임시 저장 경로 기반 Parquet/Snappy write, 저장 결과 검증, 최종 `dt` partition 교체 흐름을 구현했다.
- 문제·고민: 임시 저장 경로의 Parquet를 다시 읽을 때 partition 컬럼 `dt`가 문자열이 아닌 날짜 타입으로 추론되어 partition 경로 생성 시 캐스팅 오류가 발생했다.
- 해결방안: 검증 집계에서 `dt`를 명시적으로 string으로 변환해 partition 목록을 수집하도록 수정했다.
- 결정: `SUCCESS` manifest는 임시 저장, 검증, 최종 partition 반영, Hive sync가 모두 끝난 뒤에만 기록한다.
- 후속작업: README 최종 정리와 `src/test` 하위 자동화 테스트 추가 여부를 검토한다.
