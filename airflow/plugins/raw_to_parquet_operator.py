import logging
from airflow.models import BaseOperator
from alarms import SlackLogHandler, notify_on_exception
from pipelines.accessLog.convert import  domain_to_df
from pipelines.accessLog.schemas.access_log_schema import map_raw_to_domain, AccessLog
from datetime import datetime, timedelta, timezone
import tempfile
import gzip
import json
import pytz
from typing import Any, Iterable, List
from minio_hook import MinioHook
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

class RawToParquetOperator(BaseOperator):
    def __init__(
        self,
        hook: MinioHook,
        tz: str = "UTC",
        base_prefix: str = "raw",
        output_prefix: str = "parquet",
        recursive_list: bool = True,
        raw_suffix: str = ".gz",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.hook = hook
        self.client = hook.client
        self.slack_handler = SlackLogHandler(level=logging.WARNING)
        if not any(isinstance(h, SlackLogHandler) for h in self.log.handlers):
            self.log.addHandler(self.slack_handler)
        self.base_prefix = base_prefix
        self.output_prefix = output_prefix
        self.recursive_list = recursive_list
        self.raw_suffix = raw_suffix
        self.tz = pytz.timezone(tz)

    def _read_object_json(self, object_name) -> Any:
        data = self.hook.read_file(object_name)
        try:
            text = gzip.decompress(data).decode("utf-8", errors="replace")
        except (OSError, gzip.BadGzipFile):
            text = data.decode("utf-8", errors="replace")
        return json.loads(text)

    def _str_to_access_log(self, log_str: str) -> AccessLog:
        log_js = json.loads(log_str)
        return map_raw_to_domain(log_js)
    
    def _resolve_target_time(self, context) -> datetime:
        target_time_str = context.get("params", {}).get("target_time")
        if target_time_str:
            dt = datetime.strptime(target_time_str, "%Y%m%d%H")
            return self.tz.localize(dt)
        base = (
            context.get("data_interval_end")
            or context.get("logical_date")
            or context.get("execution_date")
            or datetime.now(timezone.utc)
        )
        if base.tzinfo is None:
            base = base.replace(tzinfo=timezone.utc)
        base_local = base.astimezone(self.tz)
        return (base_local - timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)

    def _hive_prefix(self, dt: datetime) -> str:
        return (
            f"{self.output_prefix}"
            f"/year={dt:%Y}"
            f"/month={dt:%m}"
            f"/day={dt:%d}"
            f"/hour={dt:%H}/"
        )

    def _iter_raw_messages(self, loaded_json: dict) -> Iterable[str]:
            if isinstance(loaded_json, list):
                for item in loaded_json:
                    if isinstance(item, dict) and "message" in item:
                        yield item["message"]
                    else:
                        yield json.dumps(item, ensure_ascii=False)
            elif isinstance(loaded_json, dict):
                if "message" in loaded_json:
                    yield loaded_json["message"]
                else:
                    yield json.dumps(loaded_json, ensure_ascii=False)
            elif isinstance(loaded_json, str):
                yield loaded_json
            else:
                yield json.dumps(loaded_json, ensure_ascii=False)

    def _process_file(self, obj) -> List[AccessLog]:
        local_records = []
        try:
            payload = self._read_object_json(obj.object_name)
            for msg in self._iter_raw_messages(payload):
                try:
                    rec_js = json.loads(msg)
                except json.JSONDecodeError:
                    rec_js = {"raw": msg}
                try:
                    local_records.append(map_raw_to_domain(rec_js))
                except Exception as e:
                    self.log.warning(f"Skip malformed record in {obj.object_name}: {e}")
        except Exception as e:
            self.log.error(f"Failed to read {obj.object_name}: {e}", exc_info=True)
        return local_records

    # ---------- 메인 실행 ----------
    @notify_on_exception
    def execute(self, context):
        self.slack_handler.set_context(context)
        # 대상 시간 결정
        target_time = self._resolve_target_time(context)

        # raw 데이터 경로
        raw_prefix_dir = f"{self.base_prefix}/{target_time:%Y/%m/%d/%H}/"

        # 파일 나열 + AccessLog 변환
        files = list(self.hook.list_files(raw_prefix_dir, recursive=self.recursive_list, suffix=self.raw_suffix))

        # 병렬 처리
        records: List[AccessLog] = []
        max_threads = min(60, len(files)) if files else 1
        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            futures = [executor.submit(self._process_file, obj) for obj in files]
            for f in as_completed(futures):
                records.extend(f.result())
        if not records:
            self.log.warning(f"No raw logs found in {raw_prefix_dir}")

        # DataFrame 변환
        try:
            df = domain_to_df(records)
        except Exception:
            self.log.error("DataFrame conversion failed", exc_info=True)
            raise

        # 5) 업로드
        hive_prefix = self._hive_prefix(target_time)
        object_name = hive_prefix + f"part-{uuid.uuid4().hex}.parquet"

        try:
            with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:                
                df.to_parquet(tmp.name, engine="pyarrow", index=False)
                self.hook.upload_file(
                    object_name=object_name,
                    file_path=tmp.name,
                    content_type="application/parquet",
                )
            self.log.info(
                f"Uploaded {len(records)} records → "
                f"s3://{self.hook.config.bucket}/{object_name}"
            )
        except Exception:
            self.log.error("Upload to MinIO failed", exc_info=True)
            raise