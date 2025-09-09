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
- **저장**: parquet/year=YYYY/month=MM/day=DD/hour=HH/part-*.parquet (Hive 파티셔닝)  
- **안정성**: Slack 알림(경고/에러), MinIO 버킷 자동 생성, 원자적 커밋
- **확장성**: 병렬 파일 파싱(ThreadPoolExecutor)


## 빠른 시작 (Quickstart)

### 0) 사전 요구사항

- Docker, Docker Compose
- 포트 사용 가능: 3000(nginx), 9000/9001(MinIO), 8080(Airflow)

### 1) 환경 변수(.env) 생성

루트에 .env 파일 생성
실제 운영에서는 민감정보(.env) 저장소 커밋 금지
프로젝트 루트에 .env 파일을 만들고 세팅합니다.
사용 가능한 슬랙 웹훅 있으면 같이 추가합니다.

```
# MinIO 설정
MINIO_ENDPOINT=minio:9000
MINIO_ROOT_USER=minio_admin
MINIO_ROOT_PASSWORD=minio_password
MINIO_REGION=us-east-1
MINIO_SECURE=false
MINIO_VERIFY_SSL=false
MINIO_CONNECT_TIMEOUT=3.0
MINIO_READ_TIMEOUT=30.0
BUCKET_NAME=nginx-logs
MINIO_HTTP_POOL_MAXSIZE=60
MINIO_HTTP_NUM_POOLS=10


# Airflow 설정
AIRFLOW_UID=50000
AIRFLOW_PROJ_DIR=./airflow
_AIRFLOW_WWW_USER_USERNAME=airflow
_AIRFLOW_WWW_USER_PASSWORD=airflow

# 슬랙 웹훅
PIPELINE_SLACK_WEBHOOK_URL=<your Slack Incoming Webhook URL for pipeline alerts>
NGINX_SLACK_WEBHOOK_URL=<your Slack Incoming Webhook URL for Nginx error/alert messages>
```

### 2) 서비스 실행

Nginx + MinIO + Vector + Airflow 전체 실행:
```
docker-compose -f docker-compose.yaml -f docker-compose-airflow.yaml up -d
```
필요 시 로그 확인:
docker logs -f minio / nginx / vector / airflow-scheduler 등

### 3) UI 접근

- Airflow: http://localhost:8080 (auto start, unpaused)

- Nginx 테스트: http://localhost:3000

- MinIO 콘솔: http://localhost:9001 (로그인: MINIO_ROOT_USER / MINIO_ROOT_PASSWORD)

### 4) 스케줄/실행

- 스케줄: 10 * * * * (매시 10분)

- 처리 대상: (기준시각 − 1h) 파티션 (예: 19:10 실행 → raw/.../18/ 처리)

- 수동 실행(선택):
Airflow UI → Trigger DAG → Config에 {"target_time": "YYYYMMDDHH"} 입력 시 해당 시각 처리 (에러 발생시 대처용)


## 동작 상세
### 시간/타임존

- DAG 내부 변환 타임존: UTC (코드 기준 통일)
- target_time 미지정 시: (스케줄 기준시각 - 1h)을 정시로 처리

### 입력 포맷
**Vector raw: GZIP + JSON***
- 본 구현은 “한 줄 JSON 배열”(list) 또는 단일 JSON을 모두 허용
- recursive_list=True 시 raw/.../HH/ 하위 폴더까지 탐색
- raw_suffix=".gz" 확장자 필터

### 저장 구조
- 최종: parquet/year=YYYY/month=MM/day=DD/hour=HH/part-<uuid>.parquet
- 원자적 커밋(옵션 ON):
        1) parquet/_staging/YYYY/MM/DD/HH/part-*.parquet 업로드
        2) 서버사이드 copy → 최종 경로
        3) 스테이징 즉시 삭제(그래서 콘솔에서 거의 보이지 않음)

### 알림/로깅
- Slack: WARNING 이상 로그/예외 자동 전송
- 실패 원인·대상 시각·태스크 정보가 포함되도록 메시지 구조화


## 현재 기본 설정(샘플 DAG)
```
@dag(
    dag_id="raw_to_parquet_dag",
    start_date=datetime(2025, 1, 1),
    schedule="10 * * * *",
    catchup=False,
    is_paused_upon_creation=False,  # 서버 기동 시 자동 시작
)
def raw_to_parquet_dag():
    RawToParquetOperator(
        task_id="to_parquet",
        hook=MinioHook(),
        tz="UTC",
        base_prefix="raw",
        output_prefix="parquet",

        # 운영 안전 옵션(데모 기본값)
        atomic_commit=True,           # 스테이징 후 copy-commit
        staging_dir="_staging",
        write_success_marker=True,    # _SUCCESS 생성
        skip_if_success_exists=False, # 재실행 시에도 항상 수행(데모 성향)
        enable_manifest_dedup=False,  # 같은 입력이어도 스킵하지 않음(데모 성향)
        manifest_filename="_MANIFEST.json",
        enable_quality_check=True,    # 간단 품질검사 활성화
    )
```
운영 권장값:
skip_if_success_exists=True, enable_manifest_dedup=True → 멱등/중복 방지 강화


## 데이터 스키마

- Raw(Vector): {"message": "...json..."} 형태(또는 JSON 배열)
- Domain(Airflow) → Parquet(MinIO): Hive 파티션(year/month/day/hour)


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

> 스키마 진화는 추후 schema_registry.json 등으로 관리 권장

## Spark 검증
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
    "s3a://nginx-logs/parquet/year=2025/month=09/day=09/hour=07/part-351a711ab6cf4f4fb5b767ef0e20a104.parquet"
)
df.printSchema()
df.show(5, truncate=False)
df.createOrReplaceTempView("access_logs")
spark.sql("SELECT status, COUNT(*) AS cnt FROM access_logs GROUP BY status ORDER BY cnt DESC").show()

```


## 운영 & 장애 대응

### 정상 플로우
- Vector: 매분 raw 적재
- Airflow: 매시 10분 변환(직전 1h)
- 성공 시: 최종 파티션 + _SUCCESS
- 실패/경고: Slack 자동 전송

### 리커버리
- 잘못 생성된 파티션 제거 → 동일 target_time로 재실행
- (운영 권장) _SUCCESS + 매니페스트 dedup 활성화로 중복 적재 방지

### 옵션 설명

- atomic_commit : 스테이징 업로드 후 copy-commit으로 원자적 노출
- staging_dir : 스테이징 prefix명 (기본 _staging)
- write_success_marker : 완료 시 _SUCCESS 파일 생성
- skip_if_success_exists : _SUCCESS 있으면 스킵
- enable_manifest_dedup : 동일 시간대 입력 파일셋 해시가 같으면 스킵
- enable_quality_check : 필수/범위 체크(이상치 시 실패)
- recursive_list : raw/.../HH/ 하위 폴더 포함 탐색
- raw_suffix : 입력 파일 확장자 필터(기본 .gz)

스테이징 객체는 copy 후 즉시 삭제되어 콘솔에서 보이지 않는 것이 정상입니다.

### 트러블슈팅

- No raw logs found: 해당 시간대에 파일 없음 / raw_suffix 불일치 / 권한/경로 확인
- Upload to MinIO failed: 네트워크/인증/버킷 정책 확인 (버킷은 코드에서 자동 생성)
- 중복 파켓 증가: 데모 기본값은 스킵 비활성 → 운영 전환 시
- skip_if_success_exists=True, enable_manifest_dedup=True 권장
- Slack 미알림: PIPELINE_SLACK_WEBHOOK_URL 환경변수 확인, 방화벽/프록시 점검

### 로드맵 (향후)
- 파티션 overwrite 모드(idempotent 보장)
- Prometheus/Grafana 연동 (처리량/지연/실패율)
- 데이터 만료/보존 정책(수명 주기/자동 삭제)