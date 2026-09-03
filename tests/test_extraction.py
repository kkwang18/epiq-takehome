from content_intake.pipeline.extraction import detect_edge_case, extract_text


def test_empty_bytes_is_empty_content_edge_case():
    assert detect_edge_case("txt", b"") == "empty_content"


def test_malformed_json_is_decode_failed():
    png_bytes = b"\x89PNG\r\n\x1a\nrestofbytes"
    assert detect_edge_case("json", png_bytes) == "decode_failed"


def test_valid_json_is_not_an_edge_case():
    assert detect_edge_case("json", b'{"a": 1}') is None


def test_nonempty_txt_is_not_an_edge_case():
    assert detect_edge_case("txt", b"hello") is None


def test_png_is_never_an_edge_case_even_though_binary():
    png_bytes = b"\x89PNG\r\n\x1a\nrestofbytes"
    assert detect_edge_case("png", png_bytes) is None


def test_extract_text_for_txt():
    assert extract_text("txt", b"hello world") == "hello world"


def test_extract_text_for_valid_json():
    assert extract_text("json", b'{"a": 1}') == '{"a": 1}'


def test_extract_text_for_csv():
    assert extract_text("csv", b"a,b\n1,2") == "a,b\n1,2"


def test_extract_text_returns_none_for_png():
    png_bytes = b"\x89PNG\r\n\x1a\nrestofbytes"
    assert extract_text("png", png_bytes) is None
