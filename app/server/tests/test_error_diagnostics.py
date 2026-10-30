import logging

from src.errors import logger, redacted_exception_info


def test_boundary_diagnostics_do_not_log_original_exception_details(caplog) -> None:
    private_marker = "private-provider-token-marker"
    with caplog.at_level(logging.ERROR, logger="elevenwriter.runtime"):
        try:
            raise RuntimeError(private_marker)
        except RuntimeError:
            logger.error("Provider operation failed", exc_info=redacted_exception_info())

    assert "Provider operation failed" in caplog.text
    assert "Original exception details redacted" in caplog.text
    assert private_marker not in caplog.text
