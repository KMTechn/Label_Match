import ast
from pathlib import Path


def test_registration_and_onboarding_defaults_are_nonproduction(monkeypatch):
    from tools import register_label_match_worker_pc as registration
    from failure_redaction import InstallerArgumentParser

    selected = []
    original = InstallerArgumentParser.parse_args

    class Parsed(Exception):
        pass

    def capture(parser, *args, **kwargs):
        selected.append(original(parser, *args, **kwargs))
        raise Parsed()

    monkeypatch.setattr(InstallerArgumentParser, "parse_args", capture)
    try:
        registration.main(["--dry-run"])
    except Parsed:
        pass
    assert len(selected) == 1
    assert selected[0].server_base_url == "https://unreachable.invalid"

    root = Path(__file__).resolve().parents[1]
    tree = ast.parse((root / "Label_Match.py").read_bytes())
    names = {"LABEL_MATCH_DIRECT_SYNC_SERVER_BASE_URL_ENV",
             "LABEL_MATCH_DIRECT_SYNC_DEFAULT_SERVER_BASE_URL"}
    nodes = [node for node in tree.body if
             (isinstance(node, ast.Assign) and any(
                 isinstance(target, ast.Name) and target.id in names
                 for target in node.targets)) or
             (isinstance(node, ast.FunctionDef) and
              node.name == "_first_run_server_base_url")]
    import os
    namespace = {"os": os}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(root / "Label_Match.py"),
                 "exec"), namespace)
    assert namespace["_first_run_server_base_url"](None) == "https://unreachable.invalid"
    assert namespace["_first_run_server_base_url"](
        {"server_base_url": "https://worker.kmtecherp.com"}) == "https://unreachable.invalid"
