"""The few Schema Registry calls the pipeline makes, over its REST API.

It needs only the standard library, so the Flink jobs can use it too: their
image has no confluent-kafka.
"""

from __future__ import annotations

import json
import re
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any

CONTENT_TYPE = "application/vnd.schemaregistry.v1+json"
# Every version must stay readable by every other, old and new, both ways:
# the events topic keeps events forever and the jobs read it from the start.
COMPATIBILITY = "FULL_TRANSITIVE"
# The registry's error codes for a missing subject or version, and for a
# subject already deleted but not yet for good.
SUBJECT_NOT_FOUND = 40401
VERSION_NOT_FOUND = 40402
SUBJECT_SOFT_DELETED = 40404
# One of the registry's verbose reasons; the rest of its messages are context.
REASON = re.compile(
    r"errorType:'(?P<type>[A-Z_]+)', description:'(?P<description>.*)', additionalInfo"
)


class RegistryError(RuntimeError):
    """The registry refused a request."""

    def __init__(self, status: int, error_code: int | None, message: str) -> None:
        super().__init__(f"HTTP {status}: {message}")
        self.status = status
        self.error_code = error_code
        self.message = message


class RegistryUnavailable(RuntimeError):
    """The registry could not be reached, or failed; trying again may work."""


class UnknownSchema(LookupError):
    """The registry has no schema with this id."""


class IncompatibleSchema(ValueError):
    """The registry would not take the schema: it breaks an earlier version."""


@dataclass(frozen=True)
class SchemaRegistry:
    url: str
    timeout_s: float = 5.0

    def call(self, method: str, path: str, body: object = None) -> Any:
        request = urllib.request.Request(
            self.url.rstrip("/") + path,
            data=None if body is None else json.dumps(body).encode(),
            method=method,
            headers={"Content-Type": CONTENT_TYPE, "Accept": CONTENT_TYPE},
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout_s) as response:
                return json.load(response)
        except urllib.error.HTTPError as exc:
            if exc.code >= 500:
                raise RegistryUnavailable(f"{self.url}: HTTP {exc.code}") from exc
            try:
                detail = json.load(exc)
            except ValueError:
                detail = {}
            raise RegistryError(
                exc.code, detail.get("error_code"), detail.get("message", exc.reason)
            ) from exc
        except (urllib.error.URLError, OSError) as exc:
            raise RegistryUnavailable(f"{self.url}: {exc}") from exc

    def subjects(self) -> list[str]:
        subjects: list[str] = self.call("GET", "/subjects")
        return subjects

    def register(self, subject: str, schema: str) -> int:
        """The schema's id, registering it as the subject's next version if new."""
        try:
            response = self.call(
                "POST", f"/subjects/{quote(subject)}/versions", {"schema": schema}
            )
        except RegistryError as exc:
            if exc.status == 409:
                raise IncompatibleSchema(exc.message) from exc
            raise
        schema_id: int = response["id"]
        return schema_id

    def schema(self, schema_id: int) -> str:
        """The schema registered under this id."""
        try:
            response = self.call("GET", f"/schemas/ids/{schema_id}")
        except RegistryError as exc:
            if exc.status == 404:
                raise UnknownSchema(f"no schema with id {schema_id}") from exc
            raise
        schema: str = response["schema"]
        return schema

    def problems(self, subject: str, schema: str) -> list[str]:
        """Why the schema would break the subject's versions; empty if it would not.

        It is checked against every version the subject's compatibility level
        covers: all of them, for a transitive level. (versions/latest would
        check against the newest only.)
        """
        try:
            response = self.call(
                "POST",
                f"/compatibility/subjects/{quote(subject)}/versions?verbose=true",
                {"schema": schema},
            )
        except RegistryError as exc:
            if exc.error_code in (SUBJECT_NOT_FOUND, VERSION_NOT_FOUND):
                return []
            raise
        if response["is_compatible"]:
            return []
        messages = response.get("messages") or []
        reasons = [
            f"{match['type']}: {match['description']}"
            for message in messages
            if (match := REASON.search(message))
        ]
        return reasons or messages or ["not compatible"]

    def set_compatibility(self, subject: str, level: str = COMPATIBILITY) -> None:
        self.call("PUT", f"/config/{quote(subject)}", {"compatibility": level})

    def delete_subject(self, subject: str) -> None:
        """Delete the subject and its versions for good, if it exists."""
        path = f"/subjects/{quote(subject)}"
        for delete in (path, f"{path}?permanent=true"):
            try:
                self.call("DELETE", delete)
            except RegistryError as exc:
                if exc.error_code not in (SUBJECT_NOT_FOUND, SUBJECT_SOFT_DELETED):
                    raise


def quote(subject: str) -> str:
    return urllib.parse.quote(subject, safe="")
