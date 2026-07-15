from __future__ import annotations

import json
import sqlite3

import pytest

from logfusion.cli import main
from logfusion.project import ProjectError, init_project, load_project, project_status


def test_project_init_loads_from_nested_directory_and_reports_states(tmp_path):
    project = init_project(tmp_path, "Security Lab")
    nested = tmp_path / "one" / "two"
    nested.mkdir(parents=True)
    loaded = load_project(start=nested)
    assert loaded.name == "Security Lab"
    assert loaded.path == project.path
    assert loaded.state("feature") == tmp_path / "output/features.db"

    connection = sqlite3.connect(loaded.state("feature"))
    connection.execute("CREATE TABLE feature_meta(key TEXT PRIMARY KEY,value TEXT NOT NULL)")
    connection.execute("INSERT INTO feature_meta VALUES ('feature_schema_version','1')")
    connection.commit()
    connection.close()
    status = project_status(loaded)
    assert status["states"]["feature"]["healthy"] is True
    assert status["states"]["feature"]["meta"]["feature_schema_version"] == "1"


def test_project_init_refuses_overwrite_and_cli_seeds_demo(tmp_path, capsys):
    init_project(tmp_path)
    with pytest.raises(ProjectError, match="already exists"):
        init_project(tmp_path)
    config = tmp_path / ".logfusion/project.yaml"
    assert main(["demo", "seed", "--project", str(config)]) == 0
    report = json.loads(capsys.readouterr().out)
    assert report["demo_data"].endswith(".logfusion/demo.json")
