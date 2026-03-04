import fitz
import pytest
from requests.exceptions import RequestException

from app import invoice_search


class DummyResponse:
    def __init__(self, status_code: int, payload: dict | None = None, content: bytes = b""):
        self.status_code = status_code
        self._payload = payload or {}
        self.content = content

    def json(self) -> dict:
        return self._payload


def test_normalize_removes_accents_spaces_and_url_prefixes() -> None:
    text = "  NÃO   Notificado   https://www.Example.com/Olá   "
    assert invoice_search.normalize(text) == "naonotificadoexample.com/ola"


def test_pdf_to_text_success_with_single_page() -> None:
    doc = fitz.open()
    page = doc.new_page()
    page.insert_text((72, 72), "Fatura Notificada")
    pdf_bytes = doc.tobytes()
    doc.close()

    extracted = invoice_search.pdf_to_text(pdf_bytes, max_pages=1)
    assert "Fatura Notificada" in extracted


def test_pdf_to_text_returns_empty_on_failure() -> None:
    assert invoice_search.pdf_to_text(b"invalid-pdf", max_pages=1) == ""


def test_buscar_fatura_found_phrase(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(invoice_search, "sleep", lambda _: None)
    monkeypatch.setattr(
        invoice_search,
        "request_invoice_api",
        lambda **_: DummyResponse(200, {"url": "https://example.com/invoice.pdf"}),
    )
    monkeypatch.setattr(
        invoice_search.requests,
        "get",
        lambda *_, **__: DummyResponse(200, content=b"%PDF"),
    )
    monkeypatch.setattr(invoice_search, "pdf_to_text", lambda *_args, **_kwargs: "Cliente foi NOTIFICADO.")

    result = invoice_search.buscar_fatura(
        invoice_id="123",
        phrases=["notificado"],
        base_url="https://base/",
        api_key="token",
    )

    assert result["status"] == "finished"
    assert result["found"] is True
    assert result["result_label"] == "notificado"
    assert result["attempts"] == 1
    assert result["matched_phrases"] == ["notificado"]
    assert result["pdf_url"] == "https://example.com/invoice.pdf"
    assert result["last_error"] is None


def test_buscar_fatura_phrase_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(invoice_search, "sleep", lambda _: None)
    monkeypatch.setattr(
        invoice_search,
        "request_invoice_api",
        lambda **_: DummyResponse(200, {"url": "https://example.com/invoice.pdf"}),
    )
    monkeypatch.setattr(
        invoice_search.requests,
        "get",
        lambda *_, **__: DummyResponse(200, content=b"%PDF"),
    )
    monkeypatch.setattr(invoice_search, "pdf_to_text", lambda *_args, **_kwargs: "Texto sem alvo.")

    result = invoice_search.buscar_fatura(
        invoice_id="123",
        phrases=["notificado"],
        base_url="https://base/",
        api_key="token",
    )

    assert result["status"] == "finished"
    assert result["found"] is False
    assert result["result_label"] == "nao_notificado"
    assert result["matched_phrases"] == []
    assert result["pdf_url"] == "https://example.com/invoice.pdf"


def test_buscar_fatura_sem_fatura_when_url_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(invoice_search, "sleep", lambda _: None)
    monkeypatch.setattr(
        invoice_search,
        "request_invoice_api",
        lambda **_: DummyResponse(200, {}),
    )

    result = invoice_search.buscar_fatura(
        invoice_id="123",
        phrases=["abc"],
        base_url="https://base/",
        api_key="token",
    )

    assert result == {
        "status": "finished",
        "found": None,
        "result_label": "sem_fatura",
        "error_code": None,
        "matched_phrases": None,
        "pdf_url": None,
        "last_error": "invoice_api_response_missing_url",
        "attempts": 1,
    }


def test_buscar_fatura_retryable_http_exhausted(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(invoice_search, "sleep", lambda _: None)
    monkeypatch.setattr(
        invoice_search,
        "request_invoice_api",
        lambda **_: DummyResponse(401, {}),
    )

    result = invoice_search.buscar_fatura(
        invoice_id="123",
        phrases=["abc"],
        base_url="https://base/",
        api_key="token",
        max_attempts=3,
    )

    assert result == {
        "status": "error",
        "found": None,
        "result_label": "erro_401",
        "error_code": 401,
        "matched_phrases": None,
        "pdf_url": None,
        "last_error": "invoice_api_http_error_401",
        "attempts": 3,
    }


def test_buscar_fatura_network_then_success(monkeypatch: pytest.MonkeyPatch) -> None:
    state = {"calls": 0}

    def fake_request_invoice_api(**_kwargs):
        state["calls"] += 1
        if state["calls"] == 1:
            raise RequestException("timeout")
        return DummyResponse(200, {"url": "https://example.com/invoice.pdf"})

    monkeypatch.setattr(invoice_search, "sleep", lambda _: None)
    monkeypatch.setattr(invoice_search, "request_invoice_api", fake_request_invoice_api)
    monkeypatch.setattr(
        invoice_search.requests,
        "get",
        lambda *_, **__: DummyResponse(200, content=b"%PDF"),
    )
    monkeypatch.setattr(invoice_search, "pdf_to_text", lambda *_args, **_kwargs: "texto com chave")

    result = invoice_search.buscar_fatura(
        invoice_id="123",
        phrases=["chave"],
        base_url="https://base/",
        api_key="token",
        max_attempts=3,
    )

    assert result["status"] == "finished"
    assert result["found"] is True
    assert result["attempts"] == 2
    assert result["matched_phrases"] == ["chave"]


def test_buscar_fatura_pdf_parse_exhausted(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(invoice_search, "sleep", lambda _: None)
    monkeypatch.setattr(
        invoice_search,
        "request_invoice_api",
        lambda **_: DummyResponse(200, {"url": "https://example.com/invoice.pdf"}),
    )
    monkeypatch.setattr(
        invoice_search.requests,
        "get",
        lambda *_, **__: DummyResponse(200, content=b"%PDF"),
    )
    monkeypatch.setattr(invoice_search, "pdf_to_text", lambda *_args, **_kwargs: "")

    result = invoice_search.buscar_fatura(
        invoice_id="123",
        phrases=["abc"],
        base_url="https://base/",
        api_key="token",
        max_attempts=3,
    )

    assert result == {
        "status": "error",
        "found": None,
        "result_label": "erro_pdf",
        "error_code": None,
        "matched_phrases": None,
        "pdf_url": "https://example.com/invoice.pdf",
        "last_error": "pdf_text_empty_after_retries",
        "attempts": 3,
    }


def test_buscar_fatura_uses_independent_timeouts(monkeypatch: pytest.MonkeyPatch) -> None:
    captured = {"invoice_timeout": None, "pdf_timeout": None}

    def fake_request_invoice_api(**kwargs):
        captured["invoice_timeout"] = kwargs["timeout"]
        return DummyResponse(200, {"url": "https://example.com/invoice.pdf"})

    def fake_pdf_get(*_args, **kwargs):
        captured["pdf_timeout"] = kwargs.get("timeout")
        return DummyResponse(200, content=b"%PDF")

    monkeypatch.setattr(invoice_search, "sleep", lambda _: None)
    monkeypatch.setattr(invoice_search, "request_invoice_api", fake_request_invoice_api)
    monkeypatch.setattr(invoice_search.requests, "get", fake_pdf_get)
    monkeypatch.setattr(invoice_search, "pdf_to_text", lambda *_args, **_kwargs: "ok")

    result = invoice_search.buscar_fatura(
        invoice_id="123",
        phrases=["ok"],
        base_url="https://base/",
        api_key="token",
        invoice_api_timeout=13,
        pdf_download_timeout=17,
    )

    assert result["status"] == "finished"
    assert captured["invoice_timeout"] == 13
    assert captured["pdf_timeout"] == 17
