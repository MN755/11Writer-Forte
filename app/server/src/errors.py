"""Domain validation errors and credential-safe failure diagnostics."""

import logging


class InputValidationError(ValueError):
    """External input violates the API's value-validation contract."""


class UpstreamPayloadError(RuntimeError):
    """A provider returned a payload that violates its expected contract."""


logger = logging.getLogger("elevenwriter.runtime")
logger.addHandler(logging.NullHandler())


def redacted_exception_info() -> tuple[type[RuntimeError], RuntimeError, None]:
    """Log failure context without serializing provider errors or credentials."""
    return RuntimeError, RuntimeError("Original exception details redacted."), None
