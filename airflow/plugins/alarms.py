import os, requests
from functools import wraps

def slack_fail_alert(context):
    """Airflow 컨텍스트를 받아 Slack Webhook으로 에러/경고 메시지 전송."""
    webhook = os.getenv("PIPELINE_SLACK_WEBHOOK_URL")
    if not webhook:
        return
    ti = context.get("ti") or context.get("task_instance")
    dag_id = getattr(ti, "dag_id", "unknown")
    task_id = getattr(ti, "task_id", "unknown")
    run_id = getattr(ti, "run_id", context.get("run_id", "unknown"))
    exc_msg = None
    exc = context.get("exception") or context.get("error") or context.get("reason")
    if exc:
        exc_msg = str(exc)

    if not exc_msg and ti:
        exc_msg = ti.xcom_pull(task_ids=task_id, key="error_message")

    if not exc_msg:
        exc_msg = "N/A"

    msg = (
        f"- DAG: `{dag_id}`\n"
        f"- Task: `{task_id}`\n"
        f"- Run: `{run_id}`\n"
        f"- Message: {exc_msg}\n"
    )
    requests.post(webhook, json={"text": msg}, timeout=5)


def notify_on_exception(fn):
    """오퍼레이터 함수 예외 시: Slack 알림"""
    @wraps(fn)
    def wrapper(self, context, *args, **kwargs):
        try:
            return fn(self, context, *args, **kwargs)
        except Exception as e:

            ti = context.get("ti") or context.get("task_instance")
            if ti:
                ti.xcom_push(key="error_message", value=str(e))
            try:
                slack_fail_alert(context)
            except Exception:
                pass
            raise
    return wrapper


import logging
from airflow.models import BaseOperator

class SlackLogHandler(logging.Handler):
    """로그 레코드(WARNING 이상)를 Slack으로 전송하는 핸들러."""
    def __init__(self, level=logging.WARNING):
        super().__init__(level=level)
        self.context = None
    def set_context(self, context):
        self.context = context
    def emit(self, record):
        try:
            if record.levelno >= self.level:
                ctx = self.context or {}
                slack_fail_alert({
                    **ctx,
                    "exception": f"[{record.levelname}] {record.getMessage()}"
                })
        except Exception:
            pass # 알림 실패는 pass