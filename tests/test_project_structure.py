"""
test_project_structure.py
Lightweight CI tests for the dbt project — no Snowflake connection required.

Validates:
- dbt_project.yml is valid YAML with required keys
- All SQL test files are non-empty and start with a SELECT or WITH
- All model directories exist
- packages.yml is valid YAML
"""

from __future__ import annotations

import pathlib

import pytest
import yaml

PROJECT_ROOT = pathlib.Path(__file__).parent.parent


# ── dbt_project.yml ───────────────────────────────────────────────────────────


def test_dbt_project_yml_exists():
    assert (PROJECT_ROOT / "dbt_project.yml").exists()


def test_dbt_project_yml_valid_yaml():
    content = yaml.safe_load((PROJECT_ROOT / "dbt_project.yml").read_text())
    assert isinstance(content, dict)


def test_dbt_project_yml_required_keys():
    content = yaml.safe_load((PROJECT_ROOT / "dbt_project.yml").read_text())
    for key in ("name", "version", "profile", "model-paths"):
        assert key in content, f"dbt_project.yml missing required key: {key}"


# ── packages.yml ─────────────────────────────────────────────────────────────


def test_packages_yml_exists():
    assert (PROJECT_ROOT / "packages.yml").exists()


def test_packages_yml_valid_yaml():
    content = yaml.safe_load((PROJECT_ROOT / "packages.yml").read_text())
    assert isinstance(content, dict)
    assert "packages" in content


# ── model directories ─────────────────────────────────────────────────────────


def test_models_directory_exists():
    assert (PROJECT_ROOT / "models").is_dir()


def test_models_contains_sql_files():
    sql_files = list((PROJECT_ROOT / "models").rglob("*.sql"))
    assert len(sql_files) > 0, "No SQL model files found under models/"


# ── SQL test files ────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "sql_file",
    list(pathlib.Path(__file__).parent.glob("*.sql")),
)
def test_sql_test_file_is_nonempty(sql_file):
    content = sql_file.read_text().strip()
    assert len(content) > 0, f"{sql_file.name} is empty"


@pytest.mark.parametrize(
    "sql_file",
    list(pathlib.Path(__file__).parent.glob("*.sql")),
)
def test_sql_test_file_starts_with_select_or_with(sql_file):
    first_word = sql_file.read_text().strip().upper().split()[0]
    assert first_word in (
        "SELECT",
        "WITH",
        "--",
    ), f"{sql_file.name} should start with SELECT, WITH, or --, got: {first_word}"
