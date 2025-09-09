from datetime import datetime
from airflow.decorators import dag
from minio_hook import MinioHook
from raw_to_parquet_operator import RawToParquetOperator


@dag(
    dag_id="raw_to_parquet_dag",
    start_date=datetime(2025, 1, 1),
    schedule="10 * * * *",          
    catchup=False,             
    is_paused_upon_creation=False,
    tags=["plugin", "svc"],
)
def raw_to_parquet_dag():
    # Nginx raw → Parquet 변환(운영 안전 옵션 일부 ON)
    RawToParquetOperator(
        task_id="to_parquet",
        hook=MinioHook(),
        tz="UTC",                    # 타임존: UTC 고정
        base_prefix="raw",           # 입력 prefix
        output_prefix="parquet",     # 출력 prefix

        # 안정성 옵션
        atomic_commit=True,          # 스테이징 후 copy-commit (중간 노출 방지)
        staging_dir="_staging",      # 스테이징 디렉터리명
        write_success_marker=True,   # 성공 시 _SUCCESS 파일 기록

        # 재실행 시에도 항상 수행하도록(중복 스킵 비활성)
        skip_if_success_exists=False,  # _SUCCESS 있어도 실행
        enable_manifest_dedup=False,   # 같은 입력이어도 스킵하지 않음
        manifest_filename="_MANIFEST.json",

        enable_quality_check=True,   # 기본 품질검사 활성화
        # params={"target_time": "2025090516"},  # 수동 테스트용(YYYYMMDDHH)
    )

dag = raw_to_parquet_dag()