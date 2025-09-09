from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Optional, Iterable, Dict, Any
from minio import Minio
from minio.error import S3Error
from minio.deleteobjects import DeleteObject
import urllib3


# -------- Utils --------
def _str_to_bool(v: Optional[str], default: bool = False) -> bool:
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "y", "on"}


def _strip_scheme(endpoint: str) -> str:
    return re.sub(r"^https?://", "", endpoint, flags=re.IGNORECASE)


# -------- Config --------
@dataclass(frozen=True)
class MinioConfig:
    endpoint: str
    access_key: str
    secret_key: str
    secure: bool
    region: Optional[str]
    connect_timeout: float
    read_timeout: float
    verify_ssl: bool
    bucket: str 

    @staticmethod
    def from_env() -> "MinioConfig":
        endpoint = _strip_scheme(os.getenv("MINIO_ENDPOINT", "minio:9000").strip())
        access_key = os.getenv("MINIO_ROOT_USER")
        secret_key = os.getenv("MINIO_ROOT_PASSWORD")

        if not access_key or not secret_key:
            raise RuntimeError(
                "MINIO_ROOT_USER / MINIO_ROOT_PASSWORD 환경변수가 설정되어야 합니다."
            )

        secure = _str_to_bool(os.getenv("MINIO_SECURE"), default=False)
        region = os.getenv("MINIO_REGION") or None
        connect_timeout = float(os.getenv("MINIO_CONNECT_TIMEOUT", "3.0"))
        read_timeout = float(os.getenv("MINIO_READ_TIMEOUT", "30.0"))
        verify_ssl = _str_to_bool(os.getenv("MINIO_VERIFY_SSL"), default=False)
        bucket = os.getenv("BUCKET_NAME", "nginx-logs")
        
        return MinioConfig(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
            region=region,
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
            verify_ssl=verify_ssl,
            bucket=bucket
        )


# -------- Hook --------
class MinioHook:
    def __init__(self, config: Optional[MinioConfig] = None):
        self.config = config or MinioConfig.from_env()
        self._client = self._build_client(self.config)

    @staticmethod
    def _build_http_client(cfg: MinioConfig) -> urllib3.PoolManager:
        timeout = urllib3.util.timeout.Timeout(
            connect=cfg.connect_timeout, read=cfg.read_timeout
        )
        retries = urllib3.Retry(
            total=5,
            connect=3,
            read=3,
            redirect=0,
            backoff_factor=0.4,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=frozenset(["GET", "PUT", "POST", "DELETE", "HEAD"]),
            raise_on_status=False,
        )
        cert_reqs = "CERT_REQUIRED" if cfg.verify_ssl else "CERT_NONE"
        maxsize = int(os.getenv("MINIO_HTTP_POOL_MAXSIZE", "60"))
        num_pools = int(os.getenv("MINIO_HTTP_NUM_POOLS", "10"))
        return urllib3.PoolManager(
            timeout=timeout,
            retries=retries,
            cert_reqs=cert_reqs,
            maxsize=maxsize,
            num_pools=num_pools, 
            block=True,
        )

    def _build_client(self, cfg: MinioConfig) -> Minio:
        http_client = self._build_http_client(cfg)
        return Minio(
            endpoint=cfg.endpoint,
            access_key=cfg.access_key,
            secret_key=cfg.secret_key,
            secure=cfg.secure,
            region=cfg.region,
            http_client=http_client,
        )

    @property
    def client(self) -> Minio:
        return self._client


    def ensure_bucket(self, make_public: bool = False) -> None:
        bucket = self.config.bucket
        if not self.client.bucket_exists(bucket):
            try:
                self.client.make_bucket(bucket)
            except S3Error as e:
                if e.code not in {"BucketAlreadyOwnedByYou", "BucketAlreadyExists"}:
                    raise

        if make_public:
            policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"AWS": ["*"]},
                        "Action": ["s3:GetObject"],
                        "Resource": [f"arn:aws:s3:::{bucket}/*"],
                    }
                ],
            }
            import json
            self.client.set_bucket_policy(bucket, json.dumps(policy))

    def object_exists(self, object_name: str) -> bool:
        try:
            self.client.stat_object(self.config.bucket, object_name)
            return True
        except S3Error as e:
            if e.code in {"NoSuchKey", "NotFound"}:
                return False
            raise

    def upload_file(
        self,
        object_name: str,
        file_path: str,
        content_type: str,
        metadata: Optional[Dict[str, str]] = None,
        make_bucket_public: bool = True,
    ) -> None:
        self.ensure_bucket(make_public=make_bucket_public)
        ctype = content_type,
        self.client.fput_object(
            bucket_name=self.config.bucket,
            object_name=object_name,
            file_path=file_path,
            content_type=ctype,
            metadata=metadata,
        )

    def read_file(self,
                  object_name: str)-> Any:
        resp = self.client.get_object(self.config.bucket , object_name)
        try:
            return resp.read()
        finally:
            resp.close()
            resp.release_conn()

    def list_objects(
        self,
        prefix: str = "",
        recursive: bool = False,
        include_user_meta: bool = False,
        include_version: bool = False,
    ) -> Iterable[Any]:
        return self.client.list_objects(
            self.config.bucket, prefix=prefix, recursive=recursive,
            include_user_meta=include_user_meta, include_version=include_version
        )
    
    def list_files(
        self,
        prefix: str = "",
        recursive: bool = False,
        *,
        names_only: bool = False,
        suffix: str | None = None,
        **kwargs,
    ):
        for obj in self.list_objects(prefix=prefix, recursive=recursive, **kwargs):
            if getattr(obj, "is_dir", False):
                continue
            if suffix and not str(obj.object_name).endswith(suffix):
                continue
            yield (obj.object_name if names_only else obj)

    def remove_object(self, bucket: str, object_name: str) -> None:
        self.client.remove_object(bucket, object_name)

    def remove_objects(self, bucket: str, objects: Iterable[str]) -> None:
        for err in self.client.remove_objects(bucket, [DeleteObject(o) for o in objects]):
            raise RuntimeError(f"Failed to delete {err.object_name}: {err}")
