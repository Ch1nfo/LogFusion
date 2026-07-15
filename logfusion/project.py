from __future__ import annotations

import json
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any


PROJECT_FILE = Path(".logfusion/project.yaml")
STATE_NAMES = ("feature", "baseline", "detection", "incident", "risk", "case", "evaluation")
DEFAULT_DOCUMENT: dict[str, dict[str, Any]] = {
    "project": {"name": "LogFusion"},
    "states": {
        "feature": "output/features.db",
        "baseline": "output/baseline.db",
        "detection": "output/detection.db",
        "incident": "output/incidents.db",
        "risk": "output/risk.db",
        "case": "output/cases.db",
        "evaluation": "output/evaluation.db",
    },
    "files": {
        "sources": "config/sources.yaml",
        "kafka": "config/kafka.local.yaml",
        "rules": "config/rules.local.json",
        "model_detection": "config/model-detection.local.yaml",
        "readiness_report": "output/readiness-report.json",
    },
    "runtime": {"host": "127.0.0.1", "port": 8765},
}


class ProjectError(ValueError):
    """Raised when a LogFusion project configuration is missing or invalid."""


@dataclass(frozen=True)
class ProjectConfig:
    root: Path
    path: Path
    document: dict[str, dict[str, Any]]

    @property
    def name(self) -> str:
        return str(self.document["project"]["name"])

    def value(self, section: str, key: str) -> Any:
        try:
            return self.document[section][key]
        except KeyError as exc:
            raise ProjectError(f"project configuration is missing {section}.{key}") from exc

    def resolve(self, section: str, key: str) -> Path:
        value = self.value(section, key)
        if not isinstance(value, str) or not value.strip():
            raise ProjectError(f"project configuration {section}.{key} must be a path")
        path = Path(value).expanduser()
        return path if path.is_absolute() else self.root / path

    def state(self, name: str) -> Path:
        if name not in STATE_NAMES:
            raise ProjectError(f"unsupported project state: {name}")
        return self.resolve("states", name)

    @property
    def jobs_state(self) -> Path:
        return self.root / ".logfusion/jobs.db"

    @property
    def demo_state(self) -> Path:
        return self.root / ".logfusion/demo.json"


def init_project(root: Path | str, name: str = "LogFusion", force: bool = False) -> ProjectConfig:
    root_path = Path(root).expanduser().resolve()
    path = root_path / PROJECT_FILE
    if path.exists() and not force:
        raise ProjectError(f"project configuration already exists: {path}")
    if not name.strip():
        raise ProjectError("project name is required")
    document = {section: dict(values) for section, values in DEFAULT_DOCUMENT.items()}
    document["project"]["name"] = name.strip()
    path.parent.mkdir(parents=True, exist_ok=True)
    (root_path / "output").mkdir(parents=True, exist_ok=True)
    path.write_text(_dump_yaml(document), encoding="utf-8")
    return ProjectConfig(root_path, path, document)


def load_project(path: Path | str | None = None, start: Path | str | None = None) -> ProjectConfig:
    if path is not None:
        config_path = Path(path).expanduser().resolve()
    else:
        config_path = find_project_file(Path(start).resolve() if start is not None else Path.cwd().resolve())
    if not config_path.exists():
        raise ProjectError(f"project configuration does not exist: {config_path}")
    try:
        document = _load_yaml(config_path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ProjectError(f"cannot read project configuration: {exc}") from exc
    _validate(document)
    root = config_path.parent.parent if config_path.parent.name == ".logfusion" else config_path.parent
    return ProjectConfig(root.resolve(), config_path, document)


def find_project_file(start: Path) -> Path:
    current = start if start.is_dir() else start.parent
    while True:
        candidate = current / PROJECT_FILE
        if candidate.exists():
            return candidate
        if current.parent == current:
            return start / PROJECT_FILE
        current = current.parent


def project_status(project: ProjectConfig) -> dict[str, Any]:
    states = {}
    for name in STATE_NAMES:
        path = project.state(name)
        item: dict[str, Any] = {"path": str(path), "exists": path.exists(), "size_bytes": path.stat().st_size if path.exists() else 0}
        if path.exists():
            item.update(_sqlite_summary(path))
        states[name] = item
    files = {}
    for name in project.document.get("files", {}):
        path = project.resolve("files", name)
        files[name] = {"path": str(path), "exists": path.exists()}
    return {
        "project": project.name,
        "root": str(project.root),
        "config": str(project.path),
        "python": sys.version.split()[0],
        "states": states,
        "files": files,
    }


def project_doctor(project: ProjectConfig) -> dict[str, Any]:
    status = project_status(project)
    checks = [
        {"name": "project_config", "status": "ok", "message": str(project.path)},
        {"name": "python", "status": "ok" if sys.version_info >= (3, 10) else "error", "message": sys.version.split()[0]},
    ]
    for name, item in status["files"].items():
        checks.append({"name": f"file:{name}", "status": "ok" if item["exists"] else "warning", "message": item["path"]})
    existing_states = sum(item["exists"] for item in status["states"].values())
    checks.append({
        "name": "state_store",
        "status": "ok" if existing_states else "warning",
        "message": f"{existing_states}/{len(STATE_NAMES)} state databases exist",
    })
    return {"ok": not any(item["status"] == "error" for item in checks), "checks": checks}


def _sqlite_summary(path: Path) -> dict[str, Any]:
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        tables = [row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
        meta: dict[str, str] = {}
        for table in tables:
            if table.endswith("_meta"):
                try:
                    meta.update({row[0]: row[1] for row in connection.execute(f'SELECT key,value FROM "{table}"')})
                except sqlite3.Error:
                    continue
        return {"healthy": True, "meta": meta}
    except sqlite3.Error as exc:
        return {"healthy": False, "error": str(exc)}
    finally:
        if connection is not None:
            connection.close()


def _load_yaml(text: str) -> dict[str, dict[str, Any]]:
    document: dict[str, dict[str, Any]] = {}
    section: str | None = None
    for line_number, raw_line in enumerate(text.splitlines(), 1):
        if not raw_line.strip() or raw_line.lstrip().startswith("#"):
            continue
        if not raw_line[0].isspace() and raw_line.rstrip().endswith(":"):
            section = raw_line.strip()[:-1]
            document[section] = {}
            continue
        if section is None or not raw_line.startswith("  ") or raw_line.startswith("    ") or ":" not in raw_line:
            raise ProjectError(f"unsupported project YAML at line {line_number}")
        key, raw_value = raw_line.strip().split(":", 1)
        if not key or not raw_value.strip():
            raise ProjectError(f"invalid project YAML at line {line_number}")
        document[section][key] = _scalar(raw_value.strip())
    return document


def _scalar(value: str) -> Any:
    if value in {"true", "false"}:
        return value == "true"
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def _dump_yaml(document: dict[str, dict[str, Any]]) -> str:
    lines = ["# LogFusion local project configuration."]
    for section, values in document.items():
        lines.append(f"{section}:")
        for key, value in values.items():
            encoded = json.dumps(value, ensure_ascii=False) if isinstance(value, str) else str(value).lower()
            lines.append(f"  {key}: {encoded}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _validate(document: dict[str, dict[str, Any]]) -> None:
    required = {"project": {"name"}, "states": set(STATE_NAMES), "files": {"sources", "model_detection", "readiness_report"}, "runtime": {"host", "port"}}
    for section, keys in required.items():
        if section not in document:
            raise ProjectError(f"project configuration is missing section or keys {section}")
        missing = keys.difference(document[section])
        if missing:
            suffix = f": {', '.join(sorted(missing))}"
            raise ProjectError(f"project configuration is missing section or keys {section}{suffix}")
    if not isinstance(document["runtime"]["port"], int) or not 1 <= document["runtime"]["port"] <= 65535:
        raise ProjectError("project runtime.port must be between 1 and 65535")
