"""No host, port, table or topic is written into the code: they all live in .env.

This is the ticket's grep, made precise: it reads every string literal in
src/ and jobs/ (but not docstrings, which may describe things) and fails on
anything that looks like an address or equals a configured name.
"""

import ast
import re
from collections.abc import Iterator
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

# localhost, loopback, and the ports or Compose addresses of the local stack.
ADDRESS = re.compile(
    r"localhost|127\.0\.0\.1|\b(kafka|flink-jobmanager|dynamodb-local|prometheus):\d+"
    r"|:(9092|29092|8000|8001|8081|9090|9249|9464)\b"
)
CONFIGURED_NAMES = (
    "DYNAMODB_TABLE",
    "KAFKA_EVENTS_TOPIC",
    "KAFKA_EVENTS_DLQ_TOPIC",
    "KAFKA_TRENDING_TOPIC",
    "KAFKA_WRITER_DLQ_TOPIC",
)


def env_example() -> dict[str, str]:
    values = {}
    for line in (ROOT / ".env.example").read_text().splitlines():
        if "=" in line and not line.lstrip().startswith("#"):
            name, value = line.split("=", 1)
            values[name.strip()] = value.strip().strip('"')
    return values


def string_literals(path: Path) -> Iterator[tuple[int, str]]:
    tree = ast.parse(path.read_text())
    docstrings = {
        id(node.body[0].value)
        for node in ast.walk(tree)
        if isinstance(
            node, (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)
        )
        and node.body
        and isinstance(node.body[0], ast.Expr)
        and isinstance(node.body[0].value, ast.Constant)
    }
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Constant)
            and isinstance(node.value, str)
            and id(node) not in docstrings
        ):
            yield node.lineno, node.value


def test_the_code_has_no_hosts_tables_or_topics_of_its_own() -> None:
    values = env_example()
    names = {values[name] for name in CONFIGURED_NAMES}
    paths = [*(ROOT / "src").rglob("*.py"), *(ROOT / "jobs").glob("*.py")]

    found = [
        f"{path.relative_to(ROOT)}:{line}: {text!r}"
        for path in sorted(paths)
        for line, text in string_literals(path)
        if ADDRESS.search(text) or text in names
    ]

    assert found == []


def test_the_check_would_catch_one() -> None:
    assert ADDRESS.search("http://localhost:8001")
    assert ADDRESS.search("kafka:29092")
    assert ADDRESS.search("host.docker.internal:9464")
    assert not ADDRESS.search("203.0.113.10")
