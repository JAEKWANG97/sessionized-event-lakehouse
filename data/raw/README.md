# Raw Dataset

이 디렉토리는 Spark batch pipeline의 원본 데이터를 로컬에 보관하는 공간이다.

원본 파일은 용량이 크기 때문에 Git에 올리지 않는다. 이 README만 커밋한다.

## 데이터 출처

Kaggle: Ecommerce behavior data from multi category store

URL:
https://www.kaggle.com/mkechinov/ecommerce-behavior-data-from-multi-category-store

로컬에 필요한 파일은 다음과 같다.

```text
data/raw/2019-Oct.csv.zip
data/raw/2019-Nov.csv.zip
```

## 파일 크기와 row 수

다운로드한 zip 파일을 기준으로 확인한 값이다.

| File | Inner CSV | Compressed size | Uncompressed size | Data rows |
|---|---:|---:|---:|---:|
| `2019-Oct.csv.zip` | `2019-Oct.csv` | 1,732,595,746 bytes | 5,668,612,855 bytes | 42,448,764 |
| `2019-Nov.csv.zip` | `2019-Nov.csv` | 2,874,125,107 bytes | 9,006,762,395 bytes | 67,501,979 |

두 파일의 총 row 수는 다음과 같다. header는 제외한 값이다.

```text
109,950,743 rows
```

## 원본 스키마

CSV header는 다음과 같다.

```csv
event_time,event_type,product_id,category_id,category_code,brand,price,user_id,user_session
```

각 컬럼은 Spark에서 다음처럼 해석한다.

| Column | Planned type | 설명 |
|---|---|---|
| `event_time` | string -> timestamp | 원본 값에 `UTC` suffix가 있다. UTC로 파싱한 뒤 KST로 변환한다. |
| `event_type` | string | `view`, `cart`, `purchase`, `remove_from_cart` 등 이벤트 타입 |
| `product_id` | long | 상품 ID |
| `category_id` | long | 카테고리 ID |
| `category_code` | string | 빈 값이 있을 수 있다. |
| `brand` | string | 빈 값이 있을 수 있다. |
| `price` | decimal(18,2) | 상품 가격 |
| `user_id` | long | user 기준 WAU와 세션화 기준 |
| `user_session` | string | 원본 세션 ID. 결과 테이블에서는 `source_user_session`으로 보존한다. |

## 시간 범위

원본 파일의 시작/종료 timestamp는 UTC 기준이다.

| File | First data row timestamp | Last data row timestamp |
|---|---|---|
| `2019-Oct.csv.zip` | `2019-10-01 00:00:00 UTC` | `2019-10-31 23:59:59 UTC` |
| `2019-Nov.csv.zip` | `2019-11-01 00:00:00 UTC` | `2019-11-30 23:59:59 UTC` |

주의할 점은 partition은 UTC가 아니라 KST 기준이라는 것이다.

예를 들어:

```text
2019-10-01 15:30:00 UTC
= 2019-10-02 00:30:00 KST
=> dt=2019-10-02
```

따라서 10월 CSV를 처리해도 KST 기준으로는 11월 1일 partition이 생길 수 있다.

## 샘플 row

October 첫 row:

```csv
2019-10-01 00:00:00 UTC,view,44600062,2103807459595387724,,shiseido,35.79,541312140,72d76fde-8bb3-4e00-8c23-a032dfed738c
```

November 첫 row:

```csv
2019-11-01 00:00:00 UTC,view,1003461,2053013555631882655,electronics.smartphone,xiaomi,489.07,520088904,4d3b30da-a5e4-49df-b1a8-ba5943f1dd33
```

## 처리 가정

1. `event_time`은 UTC로 해석한다.
2. output partition인 `dt`는 KST로 변환한 event timestamp에서 만든다.
3. 원본 `user_session`은 보존하되, 새로 생성하는 세션 ID로 사용하지 않는다.
4. 새 세션 ID는 동일 `user_id` 안에서 직전 이벤트와 현재 이벤트의 간격이 5분 이상일 때 새로 시작한다.
5. raw zip/CSV 파일은 로컬 전용이며 Git에 커밋하지 않는다.

## 압축 해제 관련 메모

두 zip 파일을 모두 압축 해제하면 약 14.7GB의 추가 공간이 필요하다.

```text
2019-Oct.csv  약 5.6GB
2019-Nov.csv  약 9.0GB
```

개발 초기에는 전체 데이터를 바로 처리하지 않고, `sample/` 아래의 작은 CSV로 세션화 로직을 먼저 검증한다.

유용한 확인 명령어:

```bash
# zip 내부 파일 확인
unzip -l data/raw/2019-Oct.csv.zip

# 전체 압축 해제 없이 header와 앞 5줄 확인
unzip -p data/raw/2019-Oct.csv.zip 2019-Oct.csv | head -n 6
```
