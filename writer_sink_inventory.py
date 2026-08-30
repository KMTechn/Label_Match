"""Code-derived inventory and static coverage checks for Label writer sinks."""

from __future__ import annotations

import ast
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import re
from typing import Iterable


INVENTORY_SCHEMA = "label-match-writer-sink-inventory-v1"
_EXCLUDED_ROOTS = frozenset(
    {
        ".git",
        ".pytest_cache",
        ".ruff_cache",
        ".venv",
        "build",
        "dist",
        "docs",
        "output",
        "qualification",
        "reports",
        "tests",
        "tmp",
        "venv",
    }
)
_POWERSHELL_DELEGATED_SOURCE = re.compile(
    r"Enter-LabelWriterDelegatedOperation(?:(?!\n\s*\}).){0,1200}?"
    r"-Source\s+['\"](?P<source>[a-z0-9_]+)['\"]",
    re.IGNORECASE | re.DOTALL,
)


class WriterSinkInventoryError(RuntimeError):
    """The executable writer surface is ambiguous or not fenced."""


@dataclass(frozen=True)
class WriterSinkEvidence:
    source: str
    source_path: str
    source_line: int
    qualified_name: str
    guard_kind: str
    source_sha256: str

    def record(self) -> dict[str, object]:
        return {
            "source": self.source,
            "source_path": self.source_path,
            "source_line": self.source_line,
            "qualified_name": self.qualified_name,
            "guard_kind": self.guard_kind,
            "source_sha256": self.source_sha256,
        }


def _runtime_files(root: Path, suffix: str) -> Iterable[Path]:
    for path in root.rglob(f"*{suffix}"):
        relative = path.relative_to(root)
        if not relative.parts or relative.parts[0] in _EXCLUDED_ROOTS:
            continue
        if any(part.startswith(".pytest-") for part in relative.parts):
            continue
        yield path


def _literal_source(call: ast.Call, function_name: str) -> str | None:
    name = ""
    if isinstance(call.func, ast.Name):
        name = call.func.id
    elif isinstance(call.func, ast.Attribute):
        name = call.func.attr
    if name != function_name or not call.args:
        return None
    value = call.args[0]
    if isinstance(value, ast.Constant) and isinstance(value.value, str):
        return value.value
    return None


def _python_inventory(path: Path, root: Path) -> list[WriterSinkEvidence]:
    raw = path.read_bytes()
    try:
        tree = ast.parse(raw.decode("utf-8-sig"), filename=str(path))
    except (SyntaxError, UnicodeDecodeError) as exc:
        raise WriterSinkInventoryError(f"writer inventory parse failed: {path}") from exc
    relative = path.relative_to(root).as_posix()
    source_sha256 = hashlib.sha256(raw).hexdigest()
    rows: list[WriterSinkEvidence] = []

    class Visitor(ast.NodeVisitor):
        def __init__(self) -> None:
            self.scope: list[str] = []

        def _function(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
            qualified = ".".join((*self.scope, node.name))
            for decorator in node.decorator_list:
                if not isinstance(decorator, ast.Call):
                    continue
                source = _literal_source(decorator, "writer_sink")
                if source is not None:
                    rows.append(
                        WriterSinkEvidence(
                            source=source,
                            source_path=relative,
                            source_line=node.lineno,
                            qualified_name=qualified,
                            guard_kind="decorator",
                            source_sha256=source_sha256,
                        )
                    )
            self.scope.append(node.name)
            for child in node.body:
                if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                    self.visit(child)
                    continue
                for nested in ast.walk(child):
                    if not isinstance(nested, (ast.With, ast.AsyncWith)):
                        continue
                    for item in nested.items:
                        if not isinstance(item.context_expr, ast.Call):
                            continue
                        source = _literal_source(item.context_expr, "writer_admission")
                        if source is not None:
                            rows.append(
                                WriterSinkEvidence(
                                    source=source,
                                    source_path=relative,
                                    source_line=nested.lineno,
                                    qualified_name=qualified,
                                    guard_kind="bounded_context",
                                    source_sha256=source_sha256,
                                )
                            )
            self.scope.pop()

        def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
            self._function(node)

        def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
            self._function(node)

        def visit_ClassDef(self, node: ast.ClassDef) -> None:
            self.scope.append(node.name)
            for child in node.body:
                self.visit(child)
            self.scope.pop()

    Visitor().visit(tree)
    return rows


def _powershell_inventory(path: Path, root: Path) -> list[WriterSinkEvidence]:
    raw = path.read_bytes()
    try:
        text = raw.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise WriterSinkInventoryError(f"writer inventory parse failed: {path}") from exc
    source_sha256 = hashlib.sha256(raw).hexdigest()
    relative = path.relative_to(root).as_posix()
    rows: list[WriterSinkEvidence] = []
    for match in _POWERSHELL_DELEGATED_SOURCE.finditer(text):
        rows.append(
            WriterSinkEvidence(
                source=match.group("source"),
                source_path=relative,
                source_line=text.count("\n", 0, match.start()) + 1,
                qualified_name="Enter-LabelWriterDelegatedOperation",
                guard_kind="powershell_delegated_operation",
                source_sha256=source_sha256,
            )
        )
    return rows


def derive_writer_sink_inventory(root: str | Path) -> list[WriterSinkEvidence]:
    selected_root = Path(root).resolve()
    rows: list[WriterSinkEvidence] = []
    for path in sorted(_runtime_files(selected_root, ".py"), key=lambda p: p.as_posix().casefold()):
        rows.extend(_python_inventory(path, selected_root))
    for path in sorted(_runtime_files(selected_root, ".ps1"), key=lambda p: p.as_posix().casefold()):
        rows.extend(_powershell_inventory(path, selected_root))
    rows.sort(
        key=lambda row: (
            row.source,
            row.source_path.casefold(),
            row.source_line,
            row.qualified_name,
        )
    )
    if not rows:
        raise WriterSinkInventoryError("writer sink inventory is empty")
    sources = [row.source for row in rows]
    if len(sources) != len(set(sources)):
        duplicates = sorted({source for source in sources if sources.count(source) > 1})
        raise WriterSinkInventoryError(
            "writer sink source identifiers are duplicated: " + ", ".join(duplicates)
        )
    return rows


def writer_sink_inventory_sha256(rows: Iterable[WriterSinkEvidence]) -> str:
    payload = {
        "schema": INVENTORY_SCHEMA,
        "sinks": [row.record() for row in rows],
    }
    encoded = json.dumps(
        payload,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("ascii")
    return hashlib.sha256(encoded).hexdigest()


__all__ = [
    "INVENTORY_SCHEMA",
    "WriterSinkEvidence",
    "WriterSinkInventoryError",
    "derive_writer_sink_inventory",
    "writer_sink_inventory_sha256",
]
