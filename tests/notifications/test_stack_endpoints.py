"""Endpoint discovery must not select or install another project's Python environment."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from conftest import stack_endpoints


@pytest.mark.parametrize("matrix_python", ["3.10", "3.11", "3.14"])
def test_discovery_uses_only_stdlib_under_service_environment(matrix_python, tmp_path):
    isolated_env = tmp_path / "service-environment"
    isolated_env.mkdir()
    sentinel = isolated_env / "do-not-replace"
    sentinel.write_text("service dependencies")
    result = subprocess.run(
        [sys.executable, "-S", str(Path(__file__).with_name("stack_endpoints.py"))],
        env={
            **os.environ,
            "UV_PYTHON": matrix_python,
            "UV_PROJECT_ENVIRONMENT": str(isolated_env),
        },
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == stack_endpoints()
    assert list(isolated_env.iterdir()) == [sentinel]
    assert sentinel.read_text() == "service dependencies"
