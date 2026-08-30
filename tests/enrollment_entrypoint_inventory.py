"""Derive Label enrollment entrypoint evidence from executable source code.

The inventory has no expected path or entrypoint-ID list. Python roles come
from AST call/import relationships, mutex ownership, executable module roots,
cross-module dispatch, and thread targets. PowerShell roles come from actual
enrollment argument or product-mode invocation lines.
"""

from __future__ import annotations

import ast
from collections import defaultdict, deque
from dataclasses import dataclass
import hashlib
from pathlib import Path
import re
from typing import Iterable


_EXCLUDED_TOP_LEVEL = frozenset(
    {
        ".git",
        ".github",
        ".mypy_cache",
        ".pytest_cache",
        ".ruff_cache",
        ".venv",
        "build",
        "dist",
        "docs",
        "output",
        "qualification",
        "reports",
        "scripts",
        "tests",
        "tmp",
        "venv",
    }
)
_NON_RUNTIME_TOOL_PREFIXES = (
    "build_",
    "capture_",
    "compose_",
    "measure_",
    "publish_",
    "verify_",
)
_ENROLLMENT_FLAGS = frozenset(
    {
        "--activate-current-user-runtime",
        "--onboard-current-user",
        "--self-enroll",
    }
)
_TOOL_FILENAME = re.compile(r"(?P<stem>[A-Za-z0-9_]+)\.(?:exe|py)$", re.IGNORECASE)
_POWERSHELL_SELF_ENROLL = re.compile(r"['\"]--self-enroll['\"]", re.IGNORECASE)
_POWERSHELL_PRODUCT_MODE = re.compile(
    r"\bProduct\b.*['\"](?:--onboard-current-user|--activate-current-user-runtime)['\"]",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class EntrypointEvidence:
    logical_id: str
    kind: str
    source_path: str
    source_line: int
    source_text: str
    source_sha256: str
    guard_path: tuple[str, ...]


@dataclass
class _Function:
    key: str
    module: str
    name: str
    path: Path
    relative_path: str
    node: ast.FunctionDef | ast.AsyncFunctionDef
    aliases: dict[str, str]


@dataclass(frozen=True)
class _Edge:
    caller: str
    target: str
    line: int
    kind: str


def _product_paths(root: Path, suffix: str) -> Iterable[Path]:
    for path in root.rglob(f"*{suffix}"):
        relative = path.relative_to(root)
        if not relative.parts or relative.parts[0] in _EXCLUDED_TOP_LEVEL:
            continue
        if (
            relative.parts[0] == "tools"
            and path.stem.startswith(_NON_RUNTIME_TOOL_PREFIXES)
        ):
            continue
        if any(part.startswith(".pytest-") for part in relative.parts):
            continue
        yield path


def _module_name(root: Path, path: Path) -> str:
    parts = list(path.relative_to(root).with_suffix("").parts)
    if parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts)


def _attribute_parts(node: ast.AST) -> list[str] | None:
    if isinstance(node, ast.Name):
        return [node.id]
    if isinstance(node, ast.Attribute):
        parent = _attribute_parts(node.value)
        return [*parent, node.attr] if parent else None
    return None


def _imports(nodes: Iterable[ast.AST], module: str) -> dict[str, str]:
    aliases: dict[str, str] = {}
    for root_node in nodes:
        for node in ast.walk(root_node):
            if isinstance(node, ast.Import):
                for row in node.names:
                    aliases[row.asname or row.name.split(".")[0]] = row.name
            elif isinstance(node, ast.ImportFrom):
                base = node.module or ""
                if node.level:
                    parent = module.split(".")[: -node.level]
                    base = ".".join([*parent, *([base] if base else [])])
                for row in node.names:
                    if row.name != "*":
                        aliases[row.asname or row.name] = ".".join(
                            part for part in (base, row.name) if part
                        )
    return aliases


def _resolve(
    expression: ast.AST,
    function: _Function,
    functions: dict[str, _Function],
) -> str | None:
    parts = _attribute_parts(expression)
    if not parts:
        return None
    head, *tail = parts
    if head in function.aliases:
        candidate = ".".join([function.aliases[head], *tail])
    elif len(parts) == 1:
        candidate = f"{function.module}.{head}" if function.module else head
    else:
        candidate = ".".join(parts)
    if candidate in functions:
        return candidate
    suffix_matches = [key for key in functions if key.endswith(f".{candidate}")]
    return suffix_matches[0] if len(suffix_matches) == 1 else None


class _BodyVisitor(ast.NodeVisitor):
    def __init__(self) -> None:
        self.calls: list[ast.Call] = []

    def visit_Call(self, node: ast.Call) -> None:  # noqa: N802
        self.calls.append(node)
        self.generic_visit(node)

    def visit_FunctionDef(self, _node: ast.FunctionDef) -> None:  # noqa: N802
        return

    def visit_AsyncFunctionDef(
        self,
        _node: ast.AsyncFunctionDef,
    ) -> None:  # noqa: N802
        return

    def visit_ClassDef(self, _node: ast.ClassDef) -> None:  # noqa: N802
        return


def _body_calls(function: _Function) -> list[ast.Call]:
    visitor = _BodyVisitor()
    for statement in function.node.body:
        visitor.visit(statement)
    return visitor.calls


def _string_values(
    function: _Function,
    constants: dict[str, dict[str, str]],
) -> set[str]:
    values = {
        node.value
        for node in ast.walk(function.node)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    }
    names = {
        node.id for node in ast.walk(function.node) if isinstance(node, ast.Name)
    }
    values.update(
        value
        for name in names
        if (value := constants.get(function.module, {}).get(name)) is not None
    )
    return values


def _static_string(node: ast.AST, constants: dict[str, str]) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.Name):
        return constants.get(node.id)
    if isinstance(node, ast.Call):
        parts = _attribute_parts(node.func) or []
        if parts and parts[-1] == "Path" and len(node.args) == 1:
            return _static_string(node.args[0], constants)
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Div):
        left = _static_string(node.left, constants)
        right = _static_string(node.right, constants)
        if left is not None and right is not None:
            return left.rstrip("/\\") + "/" + right.lstrip("/\\")
    return None


def _decorator_owner(function: _Function) -> bool:
    return any(
        (parts := _attribute_parts(decorator))
        and parts[-1] == "enrollment_mutex_entrypoint"
        for decorator in function.node.decorator_list
    )


def _direct_owner(function: _Function) -> bool:
    if _decorator_owner(function):
        return True
    calls = _body_calls(function)
    if any(
        (_attribute_parts(call.func) or [""])[-1] == "EnrollmentMutex"
        for call in calls
    ):
        return True
    positional = [*function.node.args.posonlyargs, *function.node.args.args]
    defaults = function.node.args.defaults
    offset = len(positional) - len(defaults)
    factories: set[str] = set()
    for index, default in enumerate(defaults, start=offset):
        parts = _attribute_parts(default)
        if parts and parts[-1] == "EnrollmentMutex":
            factories.add(positional[index].arg)
    for argument, default in zip(
        function.node.args.kwonlyargs,
        function.node.args.kw_defaults,
    ):
        parts = _attribute_parts(default) if default is not None else None
        if parts and parts[-1] == "EnrollmentMutex":
            factories.add(argument.arg)
    return any(
        isinstance(call.func, ast.Name) and call.func.id in factories for call in calls
    )


def _flag_builder(function: _Function) -> bool:
    for call in _body_calls(function):
        parts = _attribute_parts(call.func) or []
        if not parts or parts[-1] not in {"append", "extend", "insert"}:
            continue
        strings = {
            node.value
            for argument in call.args
            for node in ast.walk(argument)
            if isinstance(node, ast.Constant) and isinstance(node.value, str)
        }
        if strings & _ENROLLMENT_FLAGS:
            return True
    return False


def _target_mains(value: str, functions: dict[str, _Function]) -> set[str]:
    normalized = value.replace("\\", "/")
    filename = Path(normalized).name
    match = _TOOL_FILENAME.match(filename)
    stem = match.group("stem") if match else ""
    module_value = (
        normalized[:-3].replace("/", ".") if normalized.endswith(".py") else normalized
    )
    return {
        key
        for key, function in functions.items()
        if function.name == "main"
        and (
            (stem and function.path.stem.casefold() == stem.casefold())
            or function.module == module_value
            or function.module.endswith(f".{module_value}")
        )
    }


def _shortest_path(
    start: str,
    adjacency: dict[str, set[str]],
    owners: set[str],
) -> tuple[str, ...]:
    queue: deque[tuple[str, tuple[str, ...]]] = deque([(start, (start,))])
    visited = {start}
    while queue:
        current, path = queue.popleft()
        if current in owners:
            return path
        for target in sorted(adjacency.get(current, set())):
            if target not in visited:
                visited.add(target)
                queue.append((target, (*path, target)))
    return ()


def derive_enrollment_entrypoint_inventory(
    root: Path,
) -> tuple[EntrypointEvidence, ...]:
    """Return source-derived logical entrypoint evidence with guard paths."""

    root = root.resolve()
    parsed: dict[Path, ast.Module] = {}
    constants: dict[str, dict[str, str]] = defaultdict(dict)
    functions: dict[str, _Function] = {}

    for path in _product_paths(root, ".py"):
        source = path.read_text(encoding="utf-8-sig")
        tree = ast.parse(source, filename=str(path))
        parsed[path] = tree
        module = _module_name(root, path)
        module_aliases = _imports(tree.body, module)
        for node in tree.body:
            if isinstance(node, (ast.Assign, ast.AnnAssign)):
                value = node.value
                targets = node.targets if isinstance(node, ast.Assign) else [node.target]
                static = _static_string(value, constants[module])
                if static is not None:
                    for target in targets:
                        if isinstance(target, ast.Name):
                            constants[module][target.id] = static
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                key = f"{module}.{node.name}" if module else node.name
                functions[key] = _Function(
                    key=key,
                    module=module,
                    name=node.name,
                    path=path,
                    relative_path=path.relative_to(root).as_posix(),
                    node=node,
                    aliases={**module_aliases, **_imports([node], module)},
                )

    owners = {
        key for key, function in functions.items() if _direct_owner(function)
    }
    edges: list[_Edge] = []
    thread_edges: list[_Edge] = []

    loader_targets: dict[str, set[str]] = defaultdict(set)
    for function in functions.values():
        if "load" not in function.name:
            continue
        for value in _string_values(function, constants):
            loader_targets[function.key].update(_target_mains(value, functions))
        for alias in _imports([function.node], function.module).values():
            loader_targets[function.key].update(_target_mains(alias, functions))

    for function in functions.values():
        local_modules: dict[str, set[str]] = defaultdict(set)
        for node in ast.walk(function.node):
            if not isinstance(node, (ast.Assign, ast.AnnAssign)):
                continue
            value = node.value
            if not isinstance(value, ast.Call):
                continue
            loader = _resolve(value.func, function, functions)
            if loader not in loader_targets:
                continue
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            for target in targets:
                if isinstance(target, ast.Name):
                    local_modules[target.id].update(loader_targets[loader])

        for call in _body_calls(function):
            target = _resolve(call.func, function, functions)
            if target is not None:
                edges.append(
                    _Edge(function.key, target, int(call.lineno), "python_call")
                )
            parts = _attribute_parts(call.func) or []
            if parts and parts[-1] == "Thread":
                for keyword in call.keywords:
                    if keyword.arg == "target":
                        thread_target = _resolve(keyword.value, function, functions)
                        if thread_target:
                            edge = _Edge(
                                function.key,
                                thread_target,
                                int(call.lineno),
                                "thread_target",
                            )
                            edges.append(edge)
                            thread_edges.append(edge)
            if parts and parts[-1] == "_run_imported_main" and call.args:
                module_literal = _static_string(
                    call.args[0],
                    constants[function.module],
                )
                if module_literal:
                    for dynamic_target in _target_mains(module_literal, functions):
                        edges.append(
                            _Edge(
                                function.key,
                                dynamic_target,
                                int(call.lineno),
                                "python_call",
                            )
                        )
            if len(parts) == 2 and parts[1] == "main":
                for dynamic_target in local_modules.get(parts[0], set()):
                    edges.append(
                        _Edge(
                            function.key,
                            dynamic_target,
                            int(call.lineno),
                            "python_call",
                        )
                    )

        if "command" in function.name:
            for value in _string_values(function, constants):
                filename = Path(value.replace("\\", "/")).name
                match = _TOOL_FILENAME.match(filename)
                stem = match.group("stem").casefold() if match else ""
                if not (stem.startswith("register_") or "install_pack" in stem):
                    continue
                for external_target in _target_mains(value, functions):
                    if external_target != function.key:
                        edges.append(
                            _Edge(
                                function.key,
                                external_target,
                                int(function.node.lineno),
                                "external_command",
                            )
                        )

    tool_references: dict[str, set[str]] = defaultdict(set)
    for function in functions.values():
        for value in _string_values(function, constants):
            filename = Path(value.replace("\\", "/")).name
            match = _TOOL_FILENAME.match(filename)
            stem = match.group("stem").casefold() if match else ""
            if stem.startswith("register_") or "install_pack" in stem:
                tool_references[function.key].update(_target_mains(value, functions))
    python_adjacency: dict[str, set[str]] = defaultdict(set)
    for edge in edges:
        if edge.kind == "python_call":
            python_adjacency[edge.caller].add(edge.target)
    for function in functions.values():
        if not (
            "registration_command" in function.name
            or function.name.endswith("tool_command")
        ):
            continue
        reachable = {function.key}
        queue = deque([function.key])
        while queue:
            current = queue.popleft()
            for target in python_adjacency.get(current, set()):
                if target not in reachable:
                    reachable.add(target)
                    queue.append(target)
        external_targets = {
            target
            for reachable_function in reachable
            for target in tool_references.get(reachable_function, set())
        }
        for external_target in sorted(external_targets):
            if external_target != function.key:
                edges.append(
                    _Edge(
                        function.key,
                        external_target,
                        int(function.node.lineno),
                        "external_command",
                    )
                )

    adjacency: dict[str, set[str]] = defaultdict(set)
    reverse: dict[str, set[str]] = defaultdict(set)
    for edge in edges:
        adjacency[edge.caller].add(edge.target)
        reverse[edge.target].add(edge.caller)

    related = set(owners)
    queue = deque(
        sorted(
            owners
            | {
                key
                for key, function in functions.items()
                if _flag_builder(function)
            }
        )
    )
    related.update(queue)
    while queue:
        target = queue.popleft()
        for caller in reverse.get(target, set()):
            if caller not in related:
                related.add(caller)
                queue.append(caller)

    module_roots: set[str] = set()
    for path, tree in parsed.items():
        module = _module_name(root, path)
        pseudo = _Function(
            key=f"{module}.<module>",
            module=module,
            name="<module>",
            path=path,
            relative_path=path.relative_to(root).as_posix(),
            node=ast.FunctionDef(
                name="<module>",
                args=ast.arguments(
                    posonlyargs=[],
                    args=[],
                    vararg=None,
                    kwonlyargs=[],
                    kw_defaults=[],
                    kwarg=None,
                    defaults=[],
                ),
                body=[],
                decorator_list=[],
            ),
            aliases=_imports(tree.body, module),
        )
        visitor = _BodyVisitor()
        for statement in tree.body:
            visitor.visit(statement)
        for call in visitor.calls:
            target = _resolve(call.func, pseudo, functions)
            if target in related:
                module_roots.add(target)

    file_hashes = {
        path: hashlib.sha256(path.read_bytes()).hexdigest() for path in parsed
    }
    source_lines = {
        path: path.read_text(encoding="utf-8-sig").splitlines() for path in parsed
    }
    evidence: dict[str, EntrypointEvidence] = {}

    def add(
        logical_id: str,
        kind: str,
        function: _Function,
        line: int,
        guard_start: str,
    ) -> None:
        lines = source_lines[function.path]
        evidence[logical_id] = EntrypointEvidence(
            logical_id=logical_id,
            kind=kind,
            source_path=function.relative_path,
            source_line=line,
            source_text=lines[line - 1].strip(),
            source_sha256=file_hashes[function.path],
            guard_path=_shortest_path(guard_start, adjacency, owners),
        )

    for key in sorted(owners):
        if key not in module_roots:
            function = functions[key]
            add(
                f"mutex-owner:{function.relative_path}:{function.name}:"
                f"{function.node.lineno}",
                "direct_mutex_owner",
                function,
                int(function.node.lineno),
                key,
            )
    for key in sorted(module_roots):
        function = functions[key]
        add(
            f"executable-root:{function.relative_path}:{function.name}:"
            f"{function.node.lineno}",
            "executable_root",
            function,
            int(function.node.lineno),
            key,
        )
    for edge in edges:
        if (
            edge.kind != "python_call"
            or edge.caller not in related
            or edge.target not in related
        ):
            continue
        caller = functions[edge.caller]
        target = functions[edge.target]
        if caller.module == target.module or not (
            edge.target in owners or target.name.endswith("main")
        ):
            continue
        add(
            f"dispatch:{caller.relative_path}:{caller.name}:{edge.line}"
            f"->{edge.target}",
            "cross_module_dispatch",
            caller,
            edge.line,
            edge.target,
        )
    for edge in thread_edges:
        if edge.target in related:
            caller = functions[edge.caller]
            add(
                f"thread:{caller.relative_path}:{caller.name}:{edge.line}"
                f"->{edge.target}",
                "thread_target",
                caller,
                edge.line,
                edge.target,
            )

    mode_candidates = {
        key
        for key, function in functions.items()
        if "--onboard-current-user" in _string_values(function, constants)
        and _shortest_path(key, adjacency, owners)
    }
    self_enroll_modules = {
        function.module
        for function in functions.values()
        if "--self-enroll" in _string_values(function, constants)
    }
    self_enroll_roots = {
        key
        for key, function in functions.items()
        if function.name == "main"
        and function.module in self_enroll_modules
        and _shortest_path(key, adjacency, owners)
    }
    for path in _product_paths(root, ".ps1"):
        relative = path.relative_to(root).as_posix()
        lines = path.read_text(encoding="utf-8-sig").splitlines()
        mentioned_targets = {
            target
            for line in lines
            for target in _target_mains(line.strip().strip("'\""), functions)
        }
        for number, line in enumerate(lines, start=1):
            candidates: set[str] = set()
            if _POWERSHELL_PRODUCT_MODE.search(line):
                candidates = set(mode_candidates)
            elif _POWERSHELL_SELF_ENROLL.search(line):
                candidates = (
                    set(self_enroll_roots & mentioned_targets)
                    or set(self_enroll_roots)
                )
            if not candidates:
                continue
            selected = min(
                candidates,
                key=lambda key: (
                    len(_shortest_path(key, adjacency, owners)),
                    key,
                ),
            )
            evidence[f"powershell:{relative}:{number}"] = EntrypointEvidence(
                logical_id=f"powershell:{relative}:{number}",
                kind="powershell_guarded_delegate",
                source_path=relative,
                source_line=number,
                source_text=line.strip(),
                source_sha256=hashlib.sha256(path.read_bytes()).hexdigest(),
                guard_path=_shortest_path(selected, adjacency, owners),
            )

    return tuple(evidence[key] for key in sorted(evidence))


__all__ = [
    "EntrypointEvidence",
    "derive_enrollment_entrypoint_inventory",
]
