import json
import socket
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import pytest

from realtime_recommendation_pipeline.common.schema_registry import (
    IncompatibleSchema,
    RegistryError,
    RegistryUnavailable,
    SchemaRegistry,
    UnknownSchema,
)

NOT_FOUND = (404, {"error_code": 40401, "message": "Subject not found."})


class Stub:
    """Canned answers by (method, path); records each request it gets."""

    def __init__(self) -> None:
        self.answers: dict[tuple[str, str], tuple[int, Any]] = {}
        self.requests: list[tuple[str, str, Any, str | None]] = []


@pytest.fixture
def stub() -> Iterator[tuple[Stub, SchemaRegistry]]:
    canned = Stub()

    class Handler(BaseHTTPRequestHandler):
        def answer(self) -> None:
            length = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(length)
            canned.requests.append(
                (
                    self.command,
                    self.path,
                    json.loads(body) if body else None,
                    self.headers.get("Content-Type"),
                )
            )
            status, reply = canned.answers.get((self.command, self.path), NOT_FOUND)
            data = reply if isinstance(reply, bytes) else json.dumps(reply).encode()
            self.send_response(status)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        do_GET = do_POST = do_PUT = do_DELETE = answer

        def log_message(self, format: str, *args: Any) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(
        target=server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True
    ).start()
    try:
        yield canned, SchemaRegistry(f"http://127.0.0.1:{server.server_port}")
    finally:
        server.shutdown()
        server.server_close()


def test_register_sends_the_schema_and_returns_its_id(
    stub: tuple[Stub, SchemaRegistry],
) -> None:
    canned, registry = stub
    canned.answers[("POST", "/subjects/a.Event/versions")] = (200, {"id": 7})

    assert registry.register("a.Event", '{"type": "string"}') == 7
    (request,) = canned.requests
    assert request == (
        "POST",
        "/subjects/a.Event/versions",
        {"schema": '{"type": "string"}'},
        "application/vnd.schemaregistry.v1+json",
    )


def test_a_schema_that_breaks_an_earlier_one_is_refused(
    stub: tuple[Stub, SchemaRegistry],
) -> None:
    canned, registry = stub
    canned.answers[("POST", "/subjects/a.Event/versions")] = (
        409,
        {"error_code": 409, "message": "Schema being registered is incompatible"},
    )

    with pytest.raises(IncompatibleSchema, match="incompatible"):
        registry.register("a.Event", "{}")


def test_an_invalid_schema_is_a_registry_error(
    stub: tuple[Stub, SchemaRegistry],
) -> None:
    canned, registry = stub
    canned.answers[("POST", "/subjects/a.Event/versions")] = (
        422,
        {"error_code": 42201, "message": "Invalid schema"},
    )

    with pytest.raises(RegistryError) as raised:
        registry.register("a.Event", "not a schema")

    assert raised.value.error_code == 42201


def test_schema_by_id(stub: tuple[Stub, SchemaRegistry]) -> None:
    canned, registry = stub
    canned.answers[("GET", "/schemas/ids/3")] = (200, {"schema": '"string"'})

    assert registry.schema(3) == '"string"'
    with pytest.raises(UnknownSchema):
        registry.schema(4)


def test_problems_are_the_registrys_reasons(stub: tuple[Stub, SchemaRegistry]) -> None:
    canned, registry = stub
    path = "/compatibility/subjects/a.Event/versions?verbose=true"
    canned.answers[("POST", path)] = (
        200,
        {
            "is_compatible": False,
            "messages": [
                (
                    "{errorType:'READER_FIELD_MISSING_DEFAULT_VALUE', description:'The "
                    "field 'device' has no default value', additionalInfo:'device'}"
                ),
                "{oldSchemaVersion: 1}",
            ],
        },
    )

    assert registry.problems("a.Event", "{}") == [
        "READER_FIELD_MISSING_DEFAULT_VALUE: The field 'device' has no default value"
    ]

    canned.answers[("POST", path)] = (
        200,
        {"is_compatible": False, "messages": ["something new"]},
    )
    assert registry.problems("a.Event", "{}") == ["something new"]

    canned.answers[("POST", path)] = (200, {"is_compatible": True, "messages": []})
    assert registry.problems("a.Event", "{}") == []


def test_a_new_subject_has_nothing_to_break(stub: tuple[Stub, SchemaRegistry]) -> None:
    _, registry = stub

    assert registry.problems("new.Event", "{}") == []


def test_set_compatibility_and_subjects(stub: tuple[Stub, SchemaRegistry]) -> None:
    canned, registry = stub
    canned.answers[("PUT", "/config/a.Event")] = (200, {})
    canned.answers[("GET", "/subjects")] = (200, ["a.Event"])

    registry.set_compatibility("a.Event")

    assert canned.requests[0][2] == {"compatibility": "FULL_TRANSITIVE"}
    assert registry.subjects() == ["a.Event"]


def test_delete_subject_deletes_it_for_good(stub: tuple[Stub, SchemaRegistry]) -> None:
    canned, registry = stub
    canned.answers[("DELETE", "/subjects/a.Event")] = (200, [1, 2])
    canned.answers[("DELETE", "/subjects/a.Event?permanent=true")] = (200, [1, 2])

    registry.delete_subject("a.Event")

    assert [request[1] for request in canned.requests] == [
        "/subjects/a.Event",
        "/subjects/a.Event?permanent=true",
    ]


def test_deleting_a_missing_or_soft_deleted_subject_is_fine(
    stub: tuple[Stub, SchemaRegistry],
) -> None:
    canned, registry = stub
    canned.answers[("DELETE", "/subjects/a.Event")] = (
        404,
        {"error_code": 40404, "message": "Subject was soft deleted."},
    )

    registry.delete_subject("a.Event")
    registry.delete_subject("never.Registered")


def test_a_server_error_means_try_again(stub: tuple[Stub, SchemaRegistry]) -> None:
    canned, registry = stub
    canned.answers[("GET", "/schemas/ids/1")] = (503, b"busy")

    with pytest.raises(RegistryUnavailable):
        registry.schema(1)


def test_an_error_without_a_json_body_keeps_the_reason(
    stub: tuple[Stub, SchemaRegistry],
) -> None:
    canned, registry = stub
    canned.answers[("PUT", "/config/a.Event")] = (400, b"<html>bad</html>")

    with pytest.raises(RegistryError) as raised:
        registry.set_compatibility("a.Event")

    assert raised.value.error_code is None
    assert raised.value.status == 400


def test_no_registry_listening_means_try_again() -> None:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    # Nothing listens on the port now.

    with pytest.raises(RegistryUnavailable):
        SchemaRegistry(f"http://127.0.0.1:{port}", timeout_s=1).subjects()
