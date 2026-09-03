import json

_TEXT_EXTENSIONS = {"txt", "json", "csv"}


def detect_edge_case(extension: str, data: bytes) -> str | None:
    if len(data) == 0:
        return "empty_content"
    if extension == "json":
        try:
            json.loads(data)
        except Exception:
            return "decode_failed"
    return None


def extract_text(extension: str, data: bytes) -> str | None:
    if extension not in _TEXT_EXTENSIONS:
        return None
    return data.decode("utf-8", errors="replace")
