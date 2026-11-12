import subprocess
from pathlib import Path

import pytest

from realtime_recommendation_pipeline.common.schema_registry import (
    IncompatibleSchema,
    RegistryError,
    RegistryUnavailable,
)
from realtime_recommendation_pipeline.tools import schema_check
from realtime_recommendation_pipeline.tools.schema_check import (
    SCHEMA_PATH,
    check,
    gaps,
    released_history,
    working_history,
)


class FakeRegistry:
    """Refuses the schemas in breaking; records what the check asks of it."""

    def __init__(self, breaking: frozenset[str] = frozenset()) -> None:
        self.breaking = breaking
        self.calls: list[tuple[str, ...]] = []

    def set_compatibility(self, subject: str, level: str = "FULL_TRANSITIVE") -> None:
        self.calls.append(("config", subject, level))

    def register(self, subject: str, schema: str) -> int:
        self.calls.append(("register", subject, schema))
        if schema == "invalid":
            raise RegistryError(422, 42201, "Invalid schema")
        if schema in self.breaking:
            raise IncompatibleSchema("Schema being registered is incompatible")
        return len(self.calls)

    def problems(self, subject: str, schema: str) -> list[str]:
        return ["READER_FIELD_MISSING_DEFAULT_VALUE"]

    def delete_subject(self, subject: str) -> None:
        self.calls.append(("delete", subject))


def test_check_registers_the_history_in_order_in_a_subject_of_its_own() -> None:
    registry = FakeRegistry()

    problems = check(registry, [("v1", "one"), ("v2", "two")])  # type: ignore[arg-type]

    assert problems == []
    (config, first, second, delete) = registry.calls
    subject = config[1]
    assert subject.startswith("schema-check-")
    assert config == ("config", subject, "FULL_TRANSITIVE")
    assert (first, second) == (
        ("register", subject, "one"),
        ("register", subject, "two"),
    )
    assert delete == ("delete", subject)


def test_a_refused_version_fails_with_the_registrys_reasons() -> None:
    registry = FakeRegistry(breaking=frozenset({"three"}))

    problems = check(
        registry,  # type: ignore[arg-type]
        [("v1", "one"), ("v2", "two"), ("v3", "three"), ("v4", "invalid")],
    )

    assert problems == [
        "v3 breaks an earlier version: READER_FIELD_MISSING_DEFAULT_VALUE",
        "v4 was refused: Invalid schema",
    ]


def test_the_subject_is_deleted_even_when_the_registry_goes_away() -> None:
    class Vanishing(FakeRegistry):
        def register(self, subject: str, schema: str) -> int:
            raise RegistryUnavailable("connection refused")

    registry = Vanishing()

    with pytest.raises(RegistryUnavailable):
        check(registry, [("v1", "one")])  # type: ignore[arg-type]

    assert registry.calls[-1][0] == "delete"


def test_gaps_name_the_missing_versions() -> None:
    assert gaps([1, 2]) == []
    assert gaps([1, 3, 5]) == ["v2.avsc is missing", "v4.avsc is missing"]


def test_the_working_tree_has_every_version_in_order() -> None:
    labels = [label for label, _ in working_history()]

    assert labels[:2] == ["v1", "v2"]
    assert labels == [f"v{n}" for n in range(1, len(labels) + 1)]


def git(repo: Path, *args: str) -> None:
    subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True)


def test_released_history_reads_the_schemas_at_a_ref(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    git(tmp_path, "init", "-q")
    folder = tmp_path / SCHEMA_PATH
    folder.mkdir(parents=True)
    (folder / "v1.avsc").write_text("one")
    (folder / "v2.avsc").write_text("two")
    (folder / "notes.md").write_text("not a version")
    git(tmp_path, "add", ".")
    git(
        tmp_path,
        "-c",
        "user.name=t",
        "-c",
        "user.email=t@example.com",
        "commit",
        "-qm",
        "x",
    )
    monkeypatch.chdir(tmp_path)

    assert released_history("HEAD") == [("v1 on HEAD", "one"), ("v2 on HEAD", "two")]
    # Before the first schema, or a ref that does not exist: nothing released.
    assert released_history("no-such-ref") == []


@pytest.mark.parametrize(
    ("registry", "code", "says"),
    [
        (FakeRegistry(), None, "Every version can read"),
        # The newest version in the working tree breaks one before it.
        (FakeRegistry(breaking=frozenset({working_history()[-1][1]})), 1, "breaks"),
    ],
    ids=["compatible", "breaking"],
)
def test_main_exits_1_when_a_version_breaks(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    registry: FakeRegistry,
    code: int | None,
    says: str,
) -> None:
    monkeypatch.setattr(schema_check, "SchemaRegistry", lambda url: registry)
    monkeypatch.setattr(schema_check, "load", lambda cls: cls(url="http://unused"))

    if code is None:
        schema_check.main(["--base-ref", "no-such-ref"])
    else:
        with pytest.raises(SystemExit) as raised:
            schema_check.main(["--base-ref", "no-such-ref"])
        assert raised.value.code == code

    output = capsys.readouterr()
    assert says in output.out + output.err


def test_main_exits_2_when_the_registry_is_unreachable(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    class Unreachable(FakeRegistry):
        def set_compatibility(self, subject: str, level: str = "") -> None:
            raise RegistryUnavailable("connection refused")

    monkeypatch.setattr(schema_check, "SchemaRegistry", lambda url: Unreachable())
    monkeypatch.setattr(schema_check, "load", lambda cls: cls(url="http://unused"))

    with pytest.raises(SystemExit) as raised:
        schema_check.main(["--base-ref", "no-such-ref"])

    assert raised.value.code == 2
    assert "could not reach the registry" in capsys.readouterr().err
