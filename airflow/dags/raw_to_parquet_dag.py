from datetime import datetime
from airflow.decorators import dag
from minio_hook import MinioHook
from raw_to_parquet_operator import RawToParquetOperator

@dag(
    dag_id="raw_to_parquet_dag",
    start_date=datetime(2025, 1, 1),
    schedule="10 * * * *",  # 매시 10분
    catchup=False,
    tags=["plugin", "svc"],
)
def raw_to_parquet_dag():
    RawToParquetOperator(
        task_id="to_parquet",
        hook=MinioHook(),
        tz="UTC",
        base_prefix="raw",
        output_prefix="parquet",
        # -----------------------------------------------------------------
        # params 예시:
        # {"target_time": "2025090516"}
        #
        # - 수동 실행 시 특정 시각(YYYYMMDDHH)을 강제로 처리하고 싶을 때 사용
        # - 일반 스케줄링 실행에서는 지정하지 않음 (자동으로 -1시간 계산)
        # -----------------------------------------------------------------
        # params={"target_time": "2025090516"},
    )

dag = raw_to_parquet_dag()