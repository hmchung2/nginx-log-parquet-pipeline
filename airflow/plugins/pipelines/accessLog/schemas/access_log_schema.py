from __future__ import annotations
from dataclasses import dataclass, asdict
from typing import  Dict, Any
from datetime import datetime

@dataclass
class AccessLog:
    # 원본 필드
    remote_addr: str
    remote_user: str
    http_user_agent: str
    host: str
    hostname: str
    request: str
    request_method: str
    request_uri: str
    status: int
    time_iso8601: str
    time_local: str
    uri: str
    http_referer: str
    body_bytes_sent: int
    # 파생 필드
    ts: datetime
    year: int
    month: int
    day: int
    hour: int

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["ts"] = self.ts.isoformat()
        return d

def _parse_ts(time_iso8601: str) -> datetime:
    if not time_iso8601:
        raise ValueError("time_iso8601 is required but missing")
    return datetime.fromisoformat(time_iso8601.replace("Z", "+00:00"))


def map_raw_to_domain(rec: Dict[str, Any]) -> AccessLog:
    ts = _parse_ts(rec.get("time_iso8601"))
    return AccessLog(
        remote_addr     = rec.get("remote_addr", ""),
        remote_user     = rec.get("remote_user" ,""),
        http_user_agent = rec.get("http_user_agent", ""),
        host            = rec.get("host", ""),
        hostname        = rec.get("hostname", ""),
        request         = rec.get("request", ""),
        request_method  = rec.get("request_method", ""),
        request_uri     = rec.get("request_uri", ""),
        status          = int(rec.get("status") or 0),
        time_iso8601    = rec.get("time_iso8601", ""),
        time_local      = rec.get("time_local", ""),
        uri             = rec.get("uri", ""),
        http_referer    = rec.get("http_referer", ""),
        body_bytes_sent = int(rec.get("body_bytes_sent") or 0),

        ts   = ts,
        year = ts.year,
        month= ts.month,
        day  = ts.day,
        hour = ts.hour,
    )
