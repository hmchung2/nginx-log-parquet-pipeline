import pandas as pd
import json
from typing import List
from pipelines.accessLog.schemas.access_log_schema import AccessLog

# -----------------------------------------------------------------------------
# AccessLog 파이프라인 로직
# -----------------------------------------------------------------------------
# 현재는 access 로그 그 상태 필드로 저장
# -----------------------------------------------------------------------------
def domain_to_df(domain_records: List[AccessLog]) -> pd.DataFrame:
    # AccessLog dataclass에서 필드 이름 추출 (순서 포함)
    columns = list(AccessLog.__annotations__.keys())
    if not domain_records:
        return pd.DataFrame(columns=columns)
    dict_list = [rec.to_dict() for rec in domain_records]
    df = pd.DataFrame(dict_list, columns=columns)
    int_fields = ["status", "body_bytes_sent", "year", "month", "day", "hour"]
    for col in int_fields:
        df[col] = df[col].astype(int)
    return df