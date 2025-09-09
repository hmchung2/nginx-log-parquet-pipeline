import logging
from airflow.models import BaseOperator
from alarms import SlackLogHandler, notify_on_exception
from pipelines.accessLog.convert import domain_to_df
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

import hashlib, io
from minio.commonconfig import CopySource


class RawToParquetOperator(BaseOperator):
    """
    Nginx raw 로그를 읽어 도메인 스키마로 파싱 후 Parquet으로 저장한다.
    - 기본: 최종 경로에 바로 저장
    - 옵션: 스테이징 커밋/성공마커/매니페스트 중복방지/품질검사/성공시 스킵
    """

    def __init__(
        self,
        hook: MinioHook,                           # Minio 훅
        tz: str = "UTC",                           # UTC 시간. (해당 프로젝트 UTC 통일)
        base_prefix: str = "raw",                  # access raw 데이터 베이스 경로
        output_prefix: str = "parquet",            # parquet 저장 베이스 경로
        recursive_list: bool = True,               # 하위 경로 추가 여부
        raw_suffix: str = ".gz",                   # raw 데이터 저장 형태
        atomic_commit: bool = False,               # 스테이징→카피 커밋
        staging_dir: str = "_staging",             # 스테이징 디렉터리명
        write_success_marker: bool = False,        # 완료 마커(_SUCCESS) 생성
        enable_manifest_dedup: bool = False,       # 입력 파일셋 해시로 중복 실행 스킵
        manifest_filename: str = "_MANIFEST.json", # 매니페스트 파일명
        enable_quality_check: bool = False,        # 간단 품질검사 활성화
        skip_if_success_exists: bool = False,      # _SUCCESS 존재 시 스킵
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

        # 옵션 저장
        self.atomic_commit = atomic_commit
        self.staging_dir = staging_dir.strip("/")
        self.write_success_marker = write_success_marker
        self.enable_manifest_dedup = enable_manifest_dedup
        self.manifest_filename = manifest_filename
        self.enable_quality_check = enable_quality_check
        self.skip_if_success_exists = skip_if_success_exists

    def _read_object_json(self, object_name) -> Any:
        data = self.hook.read_file(object_name)
        try:
            text = gzip.decompress(data).decode("utf-8", errors="replace")
        except (OSError, gzip.BadGzipFile):
            text = data.decode("utf-8", errors="replace")
        payload = json.loads(text)
        return payload if isinstance(payload, list) else [payload]

    def _str_to_access_log(self, log_str: str) -> AccessLog:
        """문자열 JSON을 AccessLog로 변환."""
        log_js = json.loads(log_str)
        return map_raw_to_domain(log_js)

    def _resolve_target_time(self, context) -> datetime:
        """처리 대상 시각 결정(없으면 기준시각-1h, 정시)."""
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

    def _staging_prefix(self, dt: datetime) -> str:
        """스테이징 경로(prefix) 생성."""
        return f"{self.output_prefix}/{self.staging_dir}/{dt:%Y/%m/%d/%H}/"

    def _success_key(self, dt: datetime) -> str:
        """성공 마커(_SUCCESS) 경로."""
        return self._hive_prefix(dt) + "_SUCCESS"

    def _manifest_key(self, dt: datetime) -> str:
        """매니페스트 경로."""
        return self._hive_prefix(dt) + self.manifest_filename

    def _load_manifest(self, key: str) -> dict:
        """매니페스트 로드(없으면 기본)."""
        try:
            data = self.hook.read_file(key)
            return json.loads(data.decode("utf-8"))
        except Exception:
            return {"runs": []}

    def _save_manifest(self, key: str, manifest: dict) -> None:
        """매니페스트 저장."""
        b = json.dumps(manifest, ensure_ascii=False, indent=2).encode()
        self.client.put_object(self.hook.config.bucket, key, io.BytesIO(b), len(b))

    def _quality_check(self, recs: List[AccessLog]) -> None:
        """간단 품질검사(필수/범위)."""
        if not self.enable_quality_check:
            return
        bad = [r for r in recs if not r.remote_addr or r.status < 0 or r.status > 599]
        if bad:
            raise ValueError(f"Quality check failed: bad_rows={len(bad)}")

    def _hive_prefix(self, dt: datetime) -> str:
        """Hive 파티션 경로(prefix) 생성."""
        return (
            f"{self.output_prefix}"
            f"/year={dt:%Y}"
            f"/month={dt:%m}"
            f"/day={dt:%d}"
            f"/hour={dt:%H}/"
        )

    def _iter_raw_messages(self, loaded_json: dict) -> Iterable[str]:
        """Vector 포맷(JSON/NDJSON/배열/문자열)에서 메시지 추출."""
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
        """raw 객체 1개를 AccessLog 리스트로 파싱."""
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

    @notify_on_exception
    def execute(self, context):
        """메인: 대상 시각의 raw→Parquet 변환/적재."""
        self.slack_handler.set_context(context)

        # 대상 시각 및 raw 경로
        target_time = self._resolve_target_time(context)
        raw_prefix_dir = f"{self.base_prefix}/{target_time:%Y/%m/%d/%H}/"

        # 성공 마커 스킵(옵션)
        if self.skip_if_success_exists and self.hook.object_exists(self._success_key(target_time)):
            self.log.info(f"Skip (already succeeded): {self._success_key(target_time)}")
            return

        # 입력 파일 나열
        files = list(self.hook.list_files(raw_prefix_dir, recursive=self.recursive_list, suffix=self.raw_suffix))

        # 매니페스트 중복 방지(옵션): 파일셋 해시 비교
        input_hash = None
        if self.enable_manifest_dedup:
            md5 = hashlib.md5()
            for obj in sorted(getattr(f, "object_name", str(f)) for f in files):
                md5.update(obj.encode())
            input_hash = md5.hexdigest()
            manifest_key = self._manifest_key(target_time)
            manifest = self._load_manifest(manifest_key)
            if any(run.get("input_hash") == input_hash for run in manifest.get("runs", [])):
                self.log.info(f"Dedup: identical input already processed for {raw_prefix_dir}")
                return

        # 병렬 파싱
        records: List[AccessLog] = []
        max_threads = min(60, len(files)) if files else 1
        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            futures = [executor.submit(self._process_file, obj) for obj in files]
            for f in as_completed(futures):
                records.extend(f.result())
        if not records:
            self.log.warning(f"No raw logs found in {raw_prefix_dir}")

        # 품질 검사(옵션)
        self._quality_check(records)

        # DataFrame 변환
        try:
            df = domain_to_df(records)
        except Exception:
            self.log.error("DataFrame conversion failed", exc_info=True)
            raise

        # 최종 경로/파일명
        hive_prefix = self._hive_prefix(target_time)
        object_name = hive_prefix + f"part-{uuid.uuid4().hex}.parquet"

        # 업로드(원샷 또는 스테이징 커밋)
        try:
            with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
                df.to_parquet(tmp.name, engine="pyarrow", index=False)

                if not self.atomic_commit:
                    # 즉시 최종 경로 업로드
                    self.hook.upload_file(
                        object_name=object_name,
                        file_path=tmp.name,
                        content_type="application/parquet",
                    )
                else:
                    # 스테이징 업로드 → 최종 카피 → 스테이징 정리 → 성공마커/매니페스트
                    staging_prefix = self._staging_prefix(target_time)
                    staging_key = staging_prefix + object_name.rsplit("/", 1)[-1]

                    self.hook.upload_file(
                        object_name=staging_key,
                        file_path=tmp.name,
                        content_type="application/parquet",
                    )

                    bucket = self.hook.config.bucket
                    self.client.copy_object(bucket, object_name, CopySource(bucket, staging_key))

                    try:
                        self.hook.remove_object(bucket, staging_key)
                    except Exception:
                        self.log.warning(f"Failed to clean staging {staging_key}")

                    if self.write_success_marker:
                        self.client.put_object(bucket, self._success_key(target_time), io.BytesIO(b""), 0)

            if self.enable_manifest_dedup and input_hash:
                manifest_key = self._manifest_key(target_time)
                manifest = self._load_manifest(manifest_key)
                manifest.setdefault("runs", []).append({
                    "run_id": str(uuid.uuid4()),
                    "input_hash": input_hash,
                    "files": len(files),
                    "rows": int(getattr(df, "shape", (0, 0))[0]),
                })
                self._save_manifest(manifest_key, manifest)

            self.log.info(
                f"Uploaded {len(records)} records → "
                f"s3://{self.hook.config.bucket}/{object_name}"
            )
        except Exception:
            self.log.error("Upload to MinIO failed", exc_info=True)
            raise
