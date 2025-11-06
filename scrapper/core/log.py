# scrapper/core/log.py
from __future__ import annotations

import json
import logging
import sys
from typing import Any, Mapping


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "level": record.levelname,
            "msg": record.getMessage(),
            "logger": record.name,
            "time": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
        }
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)

        # Nasz LoggerAdapter wkłada dodatki do record.extra (jako dict)
        if hasattr(record, "extra") and isinstance(record.extra, dict):
            for k, v in record.extra.items():
                if k not in payload:
                    payload[k] = v

        return json.dumps(payload, ensure_ascii=False)


def setup_json_logger(level: str = "INFO") -> logging.Logger:
    """
    Inicjalizuje bazowy logger 'scrapper' z JSON formatterem (stdout).
    Wywołanie jest idempotentne: czyści poprzednie handlery i zakłada nowy.
    """
    logger = logging.getLogger("scrapper")
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())

    logger.handlers.clear()
    logger.addHandler(handler)

    # Nie propagujemy do root loggera, żeby uniknąć duplikatów
    logger.propagate = False
    return logger


class ExtraAdapter(logging.LoggerAdapter):
    """
    Adapter, który pozwala wołać:
        log.info("event_name", extra={"key": "value"})
    i mieć to dostępne w JsonFormatter jako record.extra (dict).
    """
    def process(self, msg: str, kwargs: dict) -> tuple[str, dict]:
        # Pobierz 'extra' od użytkownika (to nasze pole logiczne)
        user_extra: Mapping[str, Any] | None = kwargs.pop("extra", None)

        # logging.LoggerAdapter też używa 'extra', więc przekażemy mu wewnętrzne
        real_extra: dict[str, Any] = kwargs.get("extra", {}) or {}

        # Umieść dane użytkownika pod kluczem 'extra' oczekiwanym przez JsonFormatter
        if user_extra:
            # scal bez nadpisywania istniejących pól
            merged = dict(getattr(self, "extra", {}))
            merged.update(user_extra)
            real_extra["extra"] = merged
        else:
            # jeśli nic nie dodajemy, upewnij się, że przynajmniej pusta mapa jest obecna
            real_extra.setdefault("extra", {})

        kwargs["extra"] = real_extra
        return msg, kwargs


def get_logger(name: str = "scrapper") -> ExtraAdapter:
    """
    Zwraca LoggerAdapter:
      - bazuje na 'scrapper' z JSON formatterem,
      - wspiera 'extra={...}' w wywołaniach logujących.
    Przykład:
        log = get_logger("scrapper.morizon")
        log.info("morizon_osm_query", extra={"q": q, "url": url})
        log.warning("detail_parse_fail", extra={"url": offer_url, "err": "TypeError"})
    """
    base = logging.getLogger("scrapper")
    # Jeśli ktoś jeszcze nie zainicjalizował loggera – zrób to teraz
    if not base.handlers:
        setup_json_logger()

    logger = base if name == "scrapper" else logging.getLogger(name)
    # Nie dokładaj handlerów do childów – dziedziczą z 'scrapper'
    logger.propagate = True
    return ExtraAdapter(logger, {})
