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


def request_invoice_api(
    invoice_id: str,
    base_url: str,
    api_key: str,
    timeout: int,
) -> Response:
    endpoint = f"{base_url}{invoice_id}"
    headers = {"authorization": f"Bearer {api_key}"}
    return requests.get(endpoint, headers=headers, timeout=timeout, verify=False)


def buscar_fatura(
    invoice_id: str,
    phrases: list[str],
    base_url: str,
    api_key: str,
    timeout: int = 20,
    max_attempts: int = 3,
    max_pages: int | None = 3,
    logger: logging.Logger | None = None,
    log_context: str = "",
) -> dict[str, Any]:
    logger = logger or logging.getLogger(__name__)
    normalized_phrases = [normalize(phrase) for phrase in phrases if normalize(phrase)]

    for attempt in range(1, max_attempts + 1):
        try:
            invoice_response = request_invoice_api(
                invoice_id=invoice_id,
                base_url=base_url,
                api_key=api_key,
                timeout=timeout,
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
                    "attempts": attempt,
                }
            sleep(2 * attempt)
            continue

        status_code = invoice_response.status_code
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
                    "attempts": attempt,
                }
        elif status_code in RETRYABLE_HTTP_CODES:
            logger.warning(
                "%s invoice_id=%s attempt=%s retryable_http=%s",
                log_context,
                invoice_id,
                attempt,
                status_code,
            )
            if attempt == max_attempts:
                return {
                    "status": "error",
                    "found": None,
                    "result_label": f"erro_{status_code}",
                    "error_code": status_code,
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
                "attempts": attempt,
            }

        try:
            pdf_response = requests.get(pdf_url, timeout=timeout, verify=False)
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
                    "attempts": attempt,
                }
            sleep(2 * attempt)
            continue

        normalized_pdf = normalize(extracted_text)
        found = any(phrase in normalized_pdf for phrase in normalized_phrases)
        return {
            "status": "finished",
            "found": found,
            "result_label": "notificado" if found else "nao_notificado",
            "error_code": None,
            "attempts": attempt,
        }

    return {
        "status": "error",
        "found": None,
        "result_label": "erro_rede",
        "error_code": None,
        "attempts": max_attempts,
    }

