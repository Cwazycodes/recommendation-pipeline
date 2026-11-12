"""Against the Schema Registry of the stack: make up, then make test-registry.

CI starts Kafka and the registry from docker-compose.yml for these tests and
for make schema-check. Each test uses a subject of its own and deletes it.
"""

import json
import uuid
from collections.abc import Callable, Iterator
from typing import Any

import pytest

from realtime_recommendation_pipeline.common.event_schemas import (
    WriterSchemas,
    encode,
    parsed_schema,
    schema_text,
)
from realtime_recommendation_pipeline.common.schema_registry import (
    IncompatibleSchema,
    SchemaRegistry,
)
from realtime_recommendation_pipeline.flink_jobs.validation import validate_event
from realtime_recommendation_pipeline.producer.generate import generate_events
from realtime_recommendation_pipeline.settings import SchemaRegistrySettings, load
from realtime_recommendation_pipeline.tools.schema_check import check, working_history

pytestmark = pytest.mark.registry

Change = Callable[[dict[str, Any]], None]


@pytest.fixture
def registry() -> SchemaRegistry:
    return SchemaRegistry(load(SchemaRegistrySettings).url)


@pytest.fixture
def subject(registry: SchemaRegistry) -> Iterator[str]:
    name = f"test-{uuid.uuid4()}"
    registry.set_compatibility(name)
    yield name
    registry.delete_subject(name)


def changed(version: int, change: Change) -> str:
    schema = json.loads(schema_text(version))
    change(schema)
    return json.dumps(schema)


def field(schema: dict[str, Any], name: str) -> dict[str, Any]:
    found: dict[str, Any] = next(f for f in schema["fields"] if f["name"] == name)
    return found


BREAKING: dict[str, Change] = {
    "a new field with no default": lambda schema: schema["fields"].append(
        {"name": "country", "type": "string"}
    ),
    "a field removed that had no default": lambda schema: schema["fields"].remove(
        field(schema, "device")
    ),
    "a field changed to another type": lambda schema: field(
        schema, "position_s"
    ).update(type="string"),
    "a new event type": lambda schema: field(schema, "event_type")["type"][
        "symbols"
    ].append("rewind"),
    "a field renamed": lambda schema: field(schema, "user_id").update(name="viewer_id"),
}


def test_the_registry_takes_v1_then_v2(registry: SchemaRegistry, subject: str) -> None:
    ids = [registry.register(subject, schema_text(version)) for version in (1, 2)]

    assert ids[0] != ids[1]
    # The same schema again is the same id, not a new version.
    assert registry.register(subject, schema_text(2)) == ids[1]


@pytest.mark.parametrize("change", BREAKING.values(), ids=BREAKING.keys())
def test_the_registry_refuses_a_breaking_v3(
    registry: SchemaRegistry, subject: str, change: Change
) -> None:
    for version in (1, 2):
        registry.register(subject, schema_text(version))
    v3 = changed(2, change)

    assert registry.problems(subject, v3)
    with pytest.raises(IncompatibleSchema):
        registry.register(subject, v3)


def test_another_optional_field_is_compatible(
    registry: SchemaRegistry, subject: str
) -> None:
    for version in (1, 2):
        registry.register(subject, schema_text(version))
    v3 = changed(
        2,
        lambda schema: schema["fields"].append(
            {"name": "bitrate_kbps", "type": ["null", "int"], "default": None}
        ),
    )

    assert registry.problems(subject, v3) == []
    registry.register(subject, v3)


def test_a_version_must_suit_every_older_one_not_just_the_last(
    registry: SchemaRegistry, subject: str
) -> None:
    # device gains a default, then goes: each step suits the one before, but
    # a v1 reader has no default for device, so it could not read v3 events.
    with_default = changed(1, lambda schema: field(schema, "device").update(default=""))
    without = changed(
        1, lambda schema: schema["fields"].remove(field(schema, "device"))
    )
    registry.register(subject, schema_text(1))
    registry.register(subject, with_default)

    with pytest.raises(IncompatibleSchema):
        registry.register(subject, without)


def test_the_check_passes_the_repos_versions(registry: SchemaRegistry) -> None:
    assert check(registry, working_history()) == []


def test_the_check_fails_a_breaking_new_version(registry: SchemaRegistry) -> None:
    v3 = changed(2, BREAKING["a new field with no default"])

    problems = check(registry, [*working_history(), ("v3", v3)])

    assert problems
    assert all(
        problem.startswith("v3 breaks an earlier version") for problem in problems
    )


def test_the_check_fails_a_released_version_edited_in_place(
    registry: SchemaRegistry,
) -> None:
    released = [(f"{label} on main", text) for label, text in working_history()]
    edited = changed(2, BREAKING["a field removed that had no default"])

    problems = check(registry, [*released, ("v1", schema_text(1)), ("v2", edited)])

    assert problems
    assert problems[0].startswith("v2 breaks an earlier version")


def test_v1_consumers_read_events_written_as_v2(
    registry: SchemaRegistry, subject: str
) -> None:
    v2_id = registry.register(subject, schema_text(2))
    schemas = WriterSchemas(registry.schema)

    for event in generate_events(50, seed=9):
        result = validate_event(encode(event, parsed_schema(2), v2_id), schemas)

        assert result.event is not None
        assert str(result.event.event_id) == event["event_id"]
        assert result.event.session_id is None
