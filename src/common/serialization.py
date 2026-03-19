"""JSON serialization for Kafka messages with datetime/Decimal support."""

import json
from dataclasses import asdict
from datetime import datetime
from decimal import Decimal
from typing import Any

from src.common.models import Alert, PriceChange, RawPrice


class _Encoder(json.JSONEncoder):
    def default(self, o: Any) -> Any:
        if isinstance(o, datetime):
            return o.isoformat()
        if isinstance(o, Decimal):
            return str(o)
        return super().default(o)


def serialize(obj: RawPrice | PriceChange | Alert) -> bytes:
    return json.dumps(asdict(obj), cls=_Encoder, ensure_ascii=False).encode("utf-8")


def deserialize_raw_price(data: bytes) -> RawPrice:
    d = json.loads(data)
    return RawPrice(
        product_name=d["product_name"],
        category=d["category"],
        brand=d.get("brand"),
        site=d["site"],
        price=d["price"],
        url=d["url"],
        crawled_at=datetime.fromisoformat(d["crawled_at"]),
    )


def deserialize_price_change(data: bytes) -> PriceChange:
    d = json.loads(data)
    return PriceChange(
        change_id=d["change_id"],
        product_id=d["product_id"],
        product_name=d["product_name"],
        category=d["category"],
        site=d["site"],
        old_price=d.get("old_price"),
        new_price=d["new_price"],
        change_amount=d.get("change_amount"),
        change_pct=Decimal(d["change_pct"]) if d.get("change_pct") else None,
        url=d["url"],
        crawled_at=datetime.fromisoformat(d["crawled_at"]),
    )
