"""Error model from API_SPEC.md §1.2."""

from typing import Any


class ApiError(Exception):
    def __init__(
        self,
        status: int,
        code: str,
        message: str,
        *,
        topic: str | None = None,
        details: dict[str, Any] | None = None,
        retryable: bool = False,
        headers: dict[str, str] | None = None,
    ):
        super().__init__(message)
        self.status, self.code, self.message = status, code, message
        self.topic, self.details, self.retryable, self.headers = topic, details or {}, retryable, headers or {}

    def body(self) -> dict:
        err: dict[str, Any] = {"code": self.code, "message": self.message, "details": self.details, "retryable": self.retryable}
        if self.topic:
            err["topic"] = self.topic
        return {"error": err}
