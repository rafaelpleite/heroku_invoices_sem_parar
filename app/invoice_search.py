import logging
import re
import unicodedata
from time import sleep
from typing import Any

import fitz
import requests
from requests import Response
from requests.exceptions import RequestException
from urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

RETRYABLE_HTTP_CODES = {401, 403, 404, 423}


def normalize(text: str) -> str:
    normalized = text.lower()
    normalized = unicodedata.normalize("NFKD", normalized)
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    normalized = re.sub(r"\s+", " ", normalized).strip()
    normalized = normalized.replace("https://", "").replace("http://", "").replace("www.", "")
    return normalized.replace(" ", "")


def pdf_to_text(pdf_bytes: bytes, max_pages: int | None = 3) -> str:
    try:
        with fitz.open(stream=pdf_bytes, filetype="pdf") as document:
            max_page_count = min(document.page_count, max_pages) if max_pages else document.page_count
            text_parts = []
            for page_number in range(max_page_count):
                text_parts.append(document[page_number].get_text())
            return "".join(text_parts)
    except Exception:
        return ""


def mask_api_key(api_key: str) -> str:
    cleaned = api_key.strip()
    if not cleaned:
        return "missing"
    visible = cleaned[:4]
    return f"prefix={visible}**** len={len(cleaned)}"


def truncate_text(text: str | None, limit: int) -> str:
    if not text:
        return ""
    single_line = " ".join(text.split())
    if len(single_line) <= limit:
        return single_line
    return f"{single_line[:limit]}..."


def _build_invoice_api_error(
    status_code: int,
    endpoint: str,
    www_authenticate: str,
    response_body: str,
    limit: int,
) -> str:
    excerpt = truncate_text(response_body, limit)
    return (
        f"invoice_api_http_error_{status_code}"
        f"|endpoint={endpoint}"
        f"|www_auth={truncate_text(www_authenticate, 120)}"
        f"|body={excerpt}"
    )


def request_invoice_api(
    invoice_id: str,
    base_url: str,
    api_key: str,
    timeout: int,
) -> Response:
    endpoint = f"{base_url}{invoice_id}"
    headers = {"TOKEN": api_key}
    return requests.get(endpoint, headers=headers, timeout=timeout, verify=False)


def buscar_fatura(
    invoice_id: str,
    phrases: list[str],
    base_url: str,
    api_key: str,
    invoice_api_timeout: int = 20,
    pdf_download_timeout: int = 20,
    max_attempts: int = 3,
    max_pages: int | None = 3,
    logger: logging.Logger | None = None,
    log_context: str = "",
    debug_logs: bool = False,
    debug_body_limit: int = 300,
) -> dict[str, Any]:
    logger = logger or logging.getLogger(__name__)
    phrase_pairs: list[tuple[str, str]] = []
    for phrase in phrases:
        normalized = normalize(phrase)
        if normalized:
            phrase_pairs.append((phrase, normalized))

    for attempt in range(1, max_attempts + 1):
        pdf_url: str | None = None
        endpoint = f"{base_url}{invoice_id}"
        token_mask = mask_api_key(api_key)
        try:
            invoice_response = request_invoice_api(
                invoice_id=invoice_id,
                base_url=base_url,
                api_key=api_key,
                timeout=invoice_api_timeout,
            )
        except RequestException as exc:
            logger.warning(
                "%s invoice_id=%s attempt=%s network_error=%s",
                log_context,
                invoice_id,
                attempt,
                exc,
            )
            if attempt == max_attempts:
                return {
                    "status": "error",
                    "found": None,
                    "result_label": "erro_rede",
                    "error_code": None,
                    "matched_phrases": None,
                    "pdf_url": None,
                    "last_error": f"invoice_api_network_error: {exc}",
                    "attempts": attempt,
                }
            sleep(2 * attempt)
            continue

        status_code = invoice_response.status_code
        www_authenticate = invoice_response.headers.get("www-authenticate", "")
        response_text_excerpt = truncate_text(getattr(invoice_response, "text", ""), debug_body_limit)
        if debug_logs:
            logger.info(
                (
                    "%s invoice_id=%s attempt=%s endpoint=%s token_header_present=true token_mask=%s "
                    "response_status=%s www_authenticate=%s x_request_id=%s x_runtime=%s body_excerpt=%s"
                ),
                log_context,
                invoice_id,
                attempt,
                endpoint,
                token_mask,
                status_code,
                truncate_text(www_authenticate, 120),
                invoice_response.headers.get("x-request-id", ""),
                invoice_response.headers.get("x-runtime", ""),
                response_text_excerpt,
            )
        if status_code == 200:
            try:
                invoice_payload = invoice_response.json()
            except ValueError:
                invoice_payload = {}
            pdf_url = invoice_payload.get("url")
            if not pdf_url:
                return {
                    "status": "finished",
                    "found": None,
                    "result_label": "sem_fatura",
                    "error_code": None,
                    "matched_phrases": None,
                    "pdf_url": None,
                    "last_error": "invoice_api_response_missing_url",
                    "attempts": attempt,
                }
            pdf_url = str(pdf_url)
        elif status_code in RETRYABLE_HTTP_CODES:
            # Business rule: these status codes are retried even though they are often non-transient.
            logger.warning(
                "%s invoice_id=%s attempt=%s retryable_http=%s",
                log_context,
                invoice_id,
                attempt,
                status_code,
            )
            if attempt == max_attempts:
                detailed_error = _build_invoice_api_error(
                    status_code=status_code,
                    endpoint=endpoint,
                    www_authenticate=www_authenticate,
                    response_body=getattr(invoice_response, "text", ""),
                    limit=debug_body_limit,
                )
                return {
                    "status": "error",
                    "found": None,
                    "result_label": f"erro_{status_code}",
                    "error_code": status_code,
                    "matched_phrases": None,
                    "pdf_url": None,
                    "last_error": detailed_error,
                    "attempts": attempt,
                }
            sleep(2 * attempt)
            continue
        else:
            return {
                "status": "finished",
                "found": None,
                "result_label": "sem_fatura",
                "error_code": None,
                "matched_phrases": None,
                "pdf_url": None,
                "last_error": f"invoice_api_non_retryable_http_{status_code}",
                "attempts": attempt,
            }

        try:
            pdf_response = requests.get(pdf_url, timeout=pdf_download_timeout, verify=False)
        except RequestException as exc:
            logger.warning(
                "%s invoice_id=%s attempt=%s pdf_download_error=%s",
                log_context,
                invoice_id,
                attempt,
                exc,
            )
            if attempt == max_attempts:
                return {
                    "status": "error",
                    "found": None,
                    "result_label": "erro_rede",
                    "error_code": None,
                    "matched_phrases": None,
                    "pdf_url": pdf_url,
                    "last_error": f"pdf_download_network_error: {exc}",
                    "attempts": attempt,
                }
            sleep(2 * attempt)
            continue

        if pdf_response.status_code >= 400:
            logger.warning(
                "%s invoice_id=%s attempt=%s pdf_http_error=%s",
                log_context,
                invoice_id,
                attempt,
                pdf_response.status_code,
            )
            if attempt == max_attempts:
                return {
                    "status": "error",
                    "found": None,
                    "result_label": "erro_rede",
                    "error_code": pdf_response.status_code,
                    "matched_phrases": None,
                    "pdf_url": pdf_url,
                    "last_error": f"pdf_download_http_error_{pdf_response.status_code}",
                    "attempts": attempt,
                }
            sleep(2 * attempt)
            continue

        extracted_text = pdf_to_text(pdf_response.content, max_pages=max_pages)
        if not extracted_text.strip():
            logger.warning(
                "%s invoice_id=%s attempt=%s pdf_extraction_empty=true",
                log_context,
                invoice_id,
                attempt,
            )
            if attempt == max_attempts:
                return {
                    "status": "error",
                    "found": None,
                    "result_label": "erro_pdf",
                    "error_code": None,
                    "matched_phrases": None,
                    "pdf_url": pdf_url,
                    "last_error": "pdf_text_empty_after_retries",
                    "attempts": attempt,
                }
            sleep(2 * attempt)
            continue

        normalized_pdf = normalize(extracted_text)
        matched_phrases = [
            original_phrase
            for original_phrase, normalized_phrase in phrase_pairs
            if normalized_phrase in normalized_pdf
        ]
        found = bool(matched_phrases)
        return {
            "status": "finished",
            "found": found,
            "result_label": "notificado" if found else "nao_notificado",
            "error_code": None,
            "matched_phrases": matched_phrases,
            "pdf_url": pdf_url,
            "last_error": None,
            "attempts": attempt,
        }

    return {
        "status": "error",
        "found": None,
        "result_label": "erro_rede",
        "error_code": None,
        "matched_phrases": None,
        "pdf_url": None,
        "last_error": "unexpected_retry_exit",
        "attempts": max_attempts,
    }
