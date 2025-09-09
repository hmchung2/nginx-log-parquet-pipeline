# 데이터 엔지니어 기술 과제 : Nginx Access Log → Parquet on MinIO

Nginx Access 로그를 1시간 단위로 수집 → Parquet 변환 → MinIO 적재하고, Spark로 바로 조회 가능한 구조를 제공합니다.
운영 안정성을 위해 Airflow로 스케줄링/알림, Vector로 수집, Hive 스타일 파티셔닝을 사용합니다.

## 아키텍처
```
                         ┌─────────────────────┐
                         │        Nginx        │
                         │   (Access Logs)     │
                         └─────────┬───────────┘
                                   │ 로그
                                   ▼
                         ┌─────────────────────┐
                         │       Vector        │
                         │  (로그 수집 에이전트)   │
                         └───────┬───────┬─────┘
                                 │       │
                  Access Logs ▼  │       │  Error Logs
                                 │       │----------|
                                 ▼                  ▼
                     ┌─────────────────┐   ┌─────────────────┐
                     │   MinIO (raw)   │   │      Slack      │
                     │ raw/YYYY/...    │   │ (실시간 에러 알림)  │
                     └───────┬─────────┘   └─────────────────┘
                             │
                             ▼
                     ┌─────────────────────┐
                     │       Airflow       │
                     │ RawToParquetOperator│
                     │   (매시 10분 실행)     │
                     └───────┬─────────────┘
                             │
                             ▼
                 ┌─────────────────────────┐
                 │ ThreadPoolExecutor      │
                 │ (병렬 파일 파싱/변환)       │
                 └─────────┬───────────────┘
                           │
                           ▼
                     ┌─────────────────────┐
                     │   Parquet 변환       │
                     │    (pyarrow)        │
                     └───────┬───────┬─────┘
                             │       │
                  변환 성공    │       │  변환 에러
                             │       │--------------|
                             ▼                      ▼
                     ┌─────────────────┐   ┌─────────────────┐
                     │  MinIO (parquet)│   │      Slack      │
                     │ parquet/year=.. │   │ (변환 실패 알림)   │
                     └───────┬─────────┘   └─────────────────┘
                             │
                             ▼
                     ┌─────────────────────┐
                     │        Spark        │
                     │ (s3a:// 조회 가능)    │
                     └─────────────────────┘


```
- **수집**: Nginx 로그 파일을 Vector가 tail 하여 MinIO bucket에 raw/ 경로에 적재
- **변환**: Airflow DAG raw_to_parquet_dag가 매시 10분에 이전 1시간 데이터를 읽어 Parquet으로 변환 
- **저장 구조**: parquet/year=YYYY/month=MM/day=DD/hour=HH/part-*.parquet (Hive 파티셔닝)  
- **확장성**: 파일 파싱을 ThreadPoolExecutor로 병렬 처리 (I/O 바운드 최적)
- **운영성**: 실패/경고 Slack 알림, MinIO 버킷 자동 생성


## 빠른 시작 (Quickstart)

### 0) 사전 요구사항

- Docker, Docker Compose
- 포트 사용 가능: 3000(nginx), 9000/9001(MinIO), 8080(Airflow)

### 1) 환경 변수(.env) 생성

루트에 .env 파일 생성
⚠️ 안내: 아래 값들은 편의를 위해 그대로 복붙해서 바로 실행 가능하도록 제공됩니다.

실제 운영 환경에서는 Webhook/비밀번호 등 민감 값은 저장소에 커밋하지 않고 Secret/환경변수 관리 도구로 분리하세요.

프로젝트 루트에 .env 파일을 만들고 다음 내용을 그대로 넣습니다:
```
# MinIO 설정
MINIO_ENDPOINT=minio:9000
MINIO_ROOT_USER=minio_admin
MINIO_ROOT_PASSWORD=minio_password
MINIO_REGION=us-east-1
MINIO_SECURE=false
MINIO_VERIFY_SSL=true
MINIO_CONNECT_TIMEOUT=3.0
MINIO_READ_TIMEOUT=30.0
BUCKET_NAME=nginx-logs
MINIO_HTTP_POOL_MAXSIZE=60
MINIO_HTTP_NUM_POOLS=10

# Airflow 설정
AIRFLOW_UID=50000
AIRFLOW_PROJ_DIR=./airflow
PIPELINE_SLACK_WEBHOOK_URL=https://hooks.slack.com/services/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

# vector 설정
NGINX_SLACK_WEBHOOK_URL=https://hooks.slack.com/services/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

# Airflow 웹 계정 설정
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow
```
이 .env는 docker-compose.yaml 및 docker-compose-airflow.yaml에서 자동으로 참조됩니다.
다음 단계에서 바로 컨테이너를 올리면 즉시 실행됩니다.

### 2) 서비스 실행

Nginx + MinIO + Vector + Airflow 전체 실행:
```
docker-compose -f docker-compose.yaml -f docker-compose-airflow.yaml up -d
```
필요 시 로그 확인:
docker logs -f minio / nginx / vector / airflow-scheduler 등

### 3) UI 접근

- Airflow UI: http://localhost:8080
로그인: ${_AIRFLOW_WWW_USER_USERNAME} / ${_AIRFLOW_WWW_USER_PASSWORD}
raw_to_parquet_dag -> on 으로 세팅 (현재는 pause로 생성)

- Nginx 테스트 페이지: http://localhost:3000

- MinIO 콘솔: http://localhost:9001
로그인: ${MINIO_ROOT_USER} / ${MINIO_ROOT_PASSWORD}
Nginx 접숙 이후 raw/ 경로 확인
매시 10분 이후 parquet/ 경로 확인

### 4) DAG 실행
- 자동 스케줄 (Airflow 접속 후 On 세팅)
주기: 매시 10분
크론: schedule="10 * * * *"
처리 대상: 실행 기준 시각의 직전 1시간 파티션
예) 19:10에 실행되면 raw/YYYY/MM/DD/18/를 처리

- 수동 실행 (Airflow UI)
Airflow UI → DAGs → raw_to_parquet_dag → Trigger DAG w/ config
(옵션) 특정 시각을 명시하고 싶다면 아래 Config JSON을 입력:
```
{ "target_time": "2025090722" }
```
포맷: YYYYMMDDHH (예: 2025년 09월 07일 22시 → "2025090722")
이 값을 주면 해당 시각의 데이터를 처리합니다.
값을 주지 않으면 **기본 동작(직전 1시간)**으로 처리합니다.

수동 실행 (CLI 예시)
```
# 직전 1시간 처리
airflow dags trigger raw_to_parquet_dag

# 특정 시각(UTC 2025-09-07 22시) 처리
airflow dags trigger raw_to_parquet_dag \
  --conf '{"target_time":"2025090722"}'
```

## 처리 경로
### 대상 시각이 2025-09-07 22시일 때:
    입력: raw/2025/09/07/22/
    출력: parquet/year=2025/month=09/day=07/hour=22/part-<uuid>.parquet

### 동작 요약
```
| 설정             | `target_time` 제공 | 처리 대상 시각        | 비고                           |
|-----------------|-------------------|-------------------|-------------------------------|
| 자동/수동 공통     | ❌ 없음            | (기준시각 − 1h)      | 스케줄 기준 1시간 전 파티션         |
| 자동/수동 공통     | ✅ 있음            | `target_time`      | 지정된 시각의 파티션               |

```

## 스키마 정의 & 진화 정책

### Raw (입력)
- JSON
- {"message": "...json string..."} (message 필드안에 원본 access 로그 데이터가 있다)

### Domain (중간)
AccessLog
 - 원본: remote_addr, request, status, time_iso8601, user_agent, body_bytes_sent 등
 - 파생: year, month, day, hour

### Parquet (출력)
 - Hive Partition: year, month, day, hour
 - 경로 예시: s3a://nginx-logs/parquet/year=2025/month=09/day=09/hour=03/part-<uuid>.parquet

### 스키마 정의
| 필드명           | 타입      | Nullable | 기본값 | 설명              |
|------------------|----------|----------|--------|-------------------|
| remote_addr      | string   | false    | ""     | 클라이언트 IP     |
| remote_user      | string   | false    | ""     | 인증 사용자명     |
| http_user_agent  | string   | false    | ""     | 브라우저 UA       |
| host             | string   | false    | ""     | 요청 Host 헤더    |
| hostname         | string   | false    | ""     | 서버 hostname     |
| request          | string   | false    | ""     | 요청 라인         |
| request_method   | string   | false    | ""     | GET/POST 등       |
| request_uri      | string   | false    | ""     | 요청 URI          |
| status           | int      | false    | 0      | HTTP 상태코드     |
| time_iso8601     | string   | false    | ""     | ISO8601 시각      |
| time_local       | string   | false    | ""     | Nginx localtime   |
| uri              | string   | false    | ""     | 요청된 URI 경로   |
| http_referer     | string   | false    | ""     | 리퍼러            |
| body_bytes_sent  | int      | false    | 0      | 응답 바이트 수    |
| ts               | datetime | false    | -      | 파생: 이벤트 시각 |
| year             | int      | false    | -      | ts에서 추출       |
| month            | int      | false    | -      | ts에서 추출       |
| day              | int      | false    | -      | ts에서 추출       |
| hour             | int      | false    | -      | ts에서 추출       |
> 추후 운영 환경에서는 `schema_registry.json` 같은 파일로 스키마를 관리

## Spark로 검증하기
```
pyspark \
  --packages org.apache.hadoop:hadoop-aws:3.4.1,software.amazon.awssdk:bundle:2.24.6 \
  --conf spark.hadoop.fs.s3a.endpoint=http://localhost:9000 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --conf spark.hadoop.fs.s3a.access.key=minio_admin \
  --conf spark.hadoop.fs.s3a.secret.key=minio_password \
  --conf spark.hadoop.fs.s3a.connection.timeout=120000 \
  --conf spark.hadoop.fs.s3a.socket.timeout=120000 \
  --conf spark.hadoop.fs.s3a.threads.keepalivetime=60

df = spark.read.parquet(
    "s3a://nginx-logs/parquet/year=2025/month=09/day=07/hour=22/part-0fc3c3f7fd4c4a61a1dbaf729928c600.parquet"
)
df.printSchema()
df.show(5, truncate=False)
df.createOrReplaceTempView("access_logs")
spark.sql("SELECT status, COUNT(*) AS cnt FROM access_logs GROUP BY status ORDER BY cnt DESC").show()

```


## 운영 플로우
```
 Nginx Access Logs
         │
         ▼  (매분)
      Vector ──────────────▶ MinIO (raw/YYYY/MM/DD/HH/)
         │
         ▼  (매시 10분)
     Airflow DAG: raw_to_parquet_dag
         │
         ├─ 변환 성공 → MinIO (parquet/year=YYYY/month=MM/day=DD/hour=HH/)
         │                  
         │
         └─ 변환 실패 → Slack 알림 (에러 로그)
```
### 운영 및 장애 대응 시나리오
- 실시간 수집: Vector가 매분 Nginx Access Log를 읽어 raw/ 버킷에 저장
- 주기 변환: Airflow DAG raw_to_parquet_dag가 매시 10분 실행, 직전 1시간 파티션을 Parquet으로 변환
- 에러 처리: 모든 에러 메시지 + 경고 메시지는 슬랙 자동 전송. Info 레벨은 로그 전송 하지 않음
- 매뉴얼 리커버리:
        크리티컬 이슈(잘못된 Parquet 생성) → 운영자가 해당 시간대 파티션을 MinIO에서 삭제
        Airflow DAG을 target_time=YYYYMMDDHH 파라미터로 재실행하여 재처리

### 로드맵
- 현재: 매뉴얼/백필 재실행 시 Parquet 중복 생성 가능 → Slack 알림/로그 검토 후 수동 조치
- 향후 개선:
        1) DAG 실행 시 target_time 기반으로 기존 파티션 중복 검사 후 overwrite 처리
        2) 자동 idempotent 보장
        3) Prometheus/Grafana 대시보드로 처리량/지연/실패율 가시화
        4) expired 데이터 자동 삭제 스케줄 추가