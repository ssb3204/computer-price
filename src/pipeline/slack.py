"""Slack 알림 전송 헬퍼."""

import json
import logging
import os
import urllib.error
import urllib.request

logger = logging.getLogger(__name__)


def _sanitize_for_slack(text: str) -> str:
    """Slack mrkdwn 인젝션 방지: HTML 엔티티 및 백틱 이스케이프."""
    return (
        text.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace("`", "'")
    )


def _send_slack_message(text: str) -> None:
    """Slack 메시지 전송 내부 헬퍼."""
    webhook_url = os.environ.get("SLACK_WEBHOOK_URL", "")
    if not webhook_url:
        logger.info("[Slack] SLACK_WEBHOOK_URL 미설정 — 건너뜀")
        return
    payload = {"text": text}
    req = urllib.request.Request(
        webhook_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            logger.info("[Slack] 전송 완료 (status=%d)", resp.status)
    except (urllib.error.URLError, urllib.error.HTTPError, OSError):
        logger.exception("[Slack] 전송 실패")


def send_slack_failures(crawl_failures: list[dict]) -> int:
    """크롤링 실패 목록을 Slack으로 전송."""
    if not crawl_failures:
        logger.info("[Slack] 크롤링 실패 없음 — 건너뜀")
        return 0

    lines = [f"*🔴 크롤링 실패 — {len(crawl_failures)}개 사이트*"]
    for failure in crawl_failures:
        safe_error = _sanitize_for_slack(str(failure["error"]))
        lines.append(
            f"• *{failure['site_name']}* — {failure['failed_at']}\n"
            f"    `{safe_error}`"
        )
    _send_slack_message("\n".join(lines))
    return len(crawl_failures)
