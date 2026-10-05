"""Native-free PowerShell driver shared by the R4 regression modules."""
import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = ROOT / 'tests' / 'fixtures' / 'r4'
ENGINES = ('powershell.exe', 'pwsh.exe')


@pytest.fixture(params=ENGINES)
def engine(request):
    executable = shutil.which(request.param)
    assert executable, f'required PowerShell engine unavailable: {request.param}'
    return executable


def run_driver(tmp_path, engine, driver, **arguments):
    output = tmp_path / 'result.json'
    command = [engine, '-NoLogo', '-NoProfile', '-NonInteractive', '-File', str(FIXTURES / driver),
               '-Repo', str(ROOT), '-Output', str(output)]
    for name, value in arguments.items():
        command.extend(['-' + name, str(value)])
    environment = os.environ.copy()
    environment.update({'TEMP': str(tmp_path), 'TMP': str(tmp_path)})
    result = subprocess.run(command, cwd=tmp_path, env=environment, capture_output=True,
                            creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0))
    # Never include source XML, arbitrary native output, or environment in a failure.
    (tmp_path / 'driver.stdout').write_bytes(result.stdout)
    (tmp_path / 'driver.stderr').write_bytes(result.stderr)
    assert result.returncode == 0, f'R4 driver failed ({result.returncode}); isolated root: {tmp_path}'
    assert output.is_file(), f'R4 result missing; isolated root: {tmp_path}'
    return json.loads(output.read_text(encoding='utf-8-sig'))


def fingerprint(identity):
    """Contract §4: sorted names, UTF-8 without BOM, LF including the last line."""
    import hashlib
    lines = {name: f'{name}={value}\n'.encode('utf-8') for name, value in identity.items()}
    fields = {name: hashlib.sha256(value).hexdigest() for name, value in lines.items()}
    full = hashlib.sha256(b''.join(lines[name] for name in sorted(lines))).hexdigest()
    return full, fields
