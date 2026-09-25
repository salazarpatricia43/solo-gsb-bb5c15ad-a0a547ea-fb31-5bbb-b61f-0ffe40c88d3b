"""Expected service failures mapped to HTTP status codes."""
from __future__ import annotations


class ServiceError(Exception):
    """Base class for expected service failures mapped to HTTP errors."""


class NotFoundError(ServiceError):
    status_code = 404


class BadRequestError(ServiceError):
    status_code = 400


class ConflictError(ServiceError):
    status_code = 409


class UnprocessableEntityError(ServiceError):
    status_code = 422
