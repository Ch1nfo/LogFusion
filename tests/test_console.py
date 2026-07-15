from __future__ import annotations

from io import BytesIO
import json
from urllib.parse import urlencode

from logfusion.console import ConsoleApp, JobStore, build_job_command, seed_demo
from logfusion.project import init_project


SSO_SAMPLE = json.dumps({
    "ctime": "2026-07-15 10:20:30", "eventType": "authenticateSuccess",
    "status": "0", "shortName": "alice", "userIp": "10.1.2.3",
    "casServer": "10.2.3.4", "appUrl": "https://example/app",
})


def _request(app, path: str, method: str = "GET", form: dict[str, str] | None = None):
    body = urlencode(form or {}).encode()
    environ = {
        "REQUEST_METHOD": method,
        "PATH_INFO": path,
        "CONTENT_LENGTH": str(len(body)),
        "CONTENT_TYPE": "application/x-www-form-urlencoded",
        "wsgi.input": BytesIO(body),
    }
    response = {}

    def start_response(status, headers):
        response["status"] = status
        response["headers"] = dict(headers)

    response["body"] = b"".join(app(environ, start_response)).decode()
    return response


def _multipart_request(app, path: str, fields: dict[str, str], file_name: str, file_content: str):
    boundary = "----logfusion-test-boundary"
    parts = []
    for name, value in fields.items():
        parts.append(f"--{boundary}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n{value}\r\n".encode())
    parts.append(
        f"--{boundary}\r\nContent-Disposition: form-data; name=\"sample_file\"; filename=\"{file_name}\"\r\nContent-Type: application/json\r\n\r\n".encode()
        + file_content.encode()
        + b"\r\n"
    )
    body = b"".join(parts) + f"--{boundary}--\r\n".encode()
    environ = {
        "REQUEST_METHOD": "POST", "PATH_INFO": path,
        "CONTENT_LENGTH": str(len(body)), "CONTENT_TYPE": f"multipart/form-data; boundary={boundary}",
        "wsgi.input": BytesIO(body),
    }
    response = {}

    def start_response(status, headers):
        response["status"] = status
        response["headers"] = dict(headers)

    response["body"] = b"".join(app(environ, start_response)).decode()
    return response


def test_job_store_persists_lifecycle_and_builds_allowlisted_command(tmp_path):
    project = init_project(tmp_path)
    store = JobStore(project.jobs_state)
    job_id = store.submit("readiness")
    claimed = store.claim_next()
    assert claimed["job_id"] == job_id
    assert claimed["status"] == "running"
    store.finish(job_id, 0, "done")
    assert store.get(job_id)["status"] == "succeeded"
    command = build_job_command(project, "readiness", {})
    assert command[-2:] == ["--report-output", str(tmp_path / "output/readiness-report.json")]


def test_demo_console_renders_core_pages_and_is_read_only(tmp_path):
    project = init_project(tmp_path)
    seed_demo(project)
    app = ConsoleApp(project, demo=True)
    for path, text in (("/", "LogFusion 控制台"), ("/sources", "数据源"), ("/rules", "检测规则"), ("/jobs", "任务中心"), ("/readiness", "数据准备度"), ("/experiments", "检测实验"), ("/cases", "Cases")):
        response = _request(app, path)
        assert response["status"] == "200 OK"
        assert text in response["body"]
        assert "DEMO DATA" in response["body"]
    response = _request(app, "/jobs/readiness", "POST", {"csrf": app.csrf_token})
    assert response["status"] == "400 Bad Request"
    assert "demo mode is read-only" in response["body"]


def _rule_form(token: str) -> dict[str, str]:
    return {
        "csrf": token, "rule_id": "failed-login", "version": "1", "name": "Failed login", "description": "",
        "status": "draft", "scope": "event", "window_size": "300", "score": "90", "match": "all",
        "source_types": "sso", "tags": "auth", "condition_field_1": "event.outcome",
        "condition_operator_1": "eq", "condition_value_1": "failure",
        "positive_json": json.dumps({"event.outcome": "failure", "raw.source_type": "sso"}),
        "negative_json": json.dumps({"event.outcome": "success", "raw.source_type": "sso"}),
    }


def test_console_creates_tests_and_transitions_rule_with_refresh_job(tmp_path):
    project = init_project(tmp_path)
    app = ConsoleApp(project)
    tested = _request(app, "/rules/test", "POST", _rule_form(app.csrf_token))
    assert tested["status"] == "200 OK" and "测试结果" in tested["body"] and "通过" in tested["body"]

    saved = _request(app, "/rules/save", "POST", _rule_form(app.csrf_token))
    assert saved["status"] == "303 See Other"
    assert (tmp_path / "config/rules.local.json").exists()
    listed = _request(app, "/rules")
    assert "failed-login" in listed["body"] and "draft" in listed["body"]

    shadow = _request(app, "/rules/status", "POST", {
        "csrf": app.csrf_token, "rule_id": "failed-login", "version": "1", "status": "shadow",
    })
    assert shadow["status"] == "303 See Other"
    active = _request(app, "/rules/status", "POST", {
        "csrf": app.csrf_token, "rule_id": "failed-login", "version": "1", "status": "active",
    })
    assert active["status"] == "303 See Other"
    assert app.store.list()[0]["kind"] == "rules_refresh"
    command = build_job_command(project, "rules_refresh", {})
    assert command[-2:] == ["--rules", str(tmp_path / "config/rules.local.json")] or command[-1] == "--refresh-rules"


def test_console_rule_import_rejects_unknown_and_demo_writes(tmp_path):
    project = init_project(tmp_path)
    app = ConsoleApp(project)
    imported = _request(app, "/rules/import", "POST", {
        "csrf": app.csrf_token, "rule_json": json.dumps({"rule_id": "x", "version": 1, "unknown": True}),
    })
    assert imported["status"] == "400 Bad Request" and "unknown fields" in imported["body"]
    demo = ConsoleApp(project, demo=True)
    rejected = _request(demo, "/rules/save", "POST", _rule_form(demo.csrf_token))
    assert rejected["status"] == "400 Bad Request" and "read-only" in rejected["body"]


def test_console_submits_validated_background_job(tmp_path):
    project = init_project(tmp_path)
    app = ConsoleApp(project)
    response = _request(app, "/jobs/readiness", "POST", {"csrf": app.csrf_token})
    assert response["status"] == "303 See Other"
    assert response["headers"]["Location"].startswith("/jobs/")
    assert app.store.list()[0]["kind"] == "readiness"

    rejected = _request(app, "/jobs/evaluate", "POST", {
        "csrf": app.csrf_token, "name": "bad", "detector": "hbos", "from": "2026-01-02", "to": "2026-01-01",
    })
    assert rejected["status"] == "400 Bad Request"
    assert "end must be after start" in rejected["body"]


def test_source_wizard_previews_uploaded_sample_and_saves_local_source(tmp_path):
    project = init_project(tmp_path)
    app = ConsoleApp(project)
    page = _request(app, "/sources/new")
    assert page["status"] == "200 OK"
    assert "接入新数据源" in page["body"]

    fields = {
        "csrf": app.csrf_token, "kind": "local", "source_id": "sso-main",
        "source_type": "auto", "record_mode": "line", "path": "data/sso/*.jsonl",
    }
    preview = _multipart_request(app, "/sources/preview", fields, "sample.json", SSO_SAMPLE)
    assert preview["status"] == "200 OK"
    assert "Canonical Event" in preview["body"]
    assert "sso_json" in preview["body"]
    assert "确认并保存配置" in preview["body"]

    saved = _request(app, "/sources/save", "POST", fields)
    assert saved["status"] == "200 OK"
    assert "数据源已保存" in saved["body"]
    assert (tmp_path / "config/sources.yaml").exists()
    listed = _request(app, "/sources")
    assert "sso-main" in listed["body"]


def test_failed_source_preview_does_not_offer_save(tmp_path):
    app = ConsoleApp(init_project(tmp_path))
    response = _request(app, "/sources/preview", "POST", {
        "csrf": app.csrf_token, "kind": "local", "source_id": "bad-parser",
        "source_type": "gitlab", "record_mode": "line", "path": "data/*.json",
        "sample": SSO_SAMPLE,
    })
    assert response["status"] == "200 OK"
    assert "不能保存" in response["body"]
    assert "确认并保存配置" not in response["body"]


def test_console_parse_job_is_allowlisted(tmp_path):
    project = init_project(tmp_path)
    app = ConsoleApp(project)
    response = _request(app, "/jobs/parse", "POST", {"csrf": app.csrf_token})
    assert response["status"] == "303 See Other"
    assert app.store.list()[0]["kind"] == "parse"
    command = build_job_command(project, "parse", {})
    assert command[2:5] == ["logfusion", "parse", "--config"]
    assert command[-2:] == ["--checkpoint", str(tmp_path / "output/parse.checkpoint.json")]
