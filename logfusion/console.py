from __future__ import annotations

import html
import json
import re
import secrets
import sqlite3
import subprocess
import sys
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from email import policy
from email.parser import BytesParser
from http.cookies import CookieError, SimpleCookie
from pathlib import Path
from socketserver import ThreadingMixIn
from typing import Any, Callable, Iterable, Iterator
from urllib.parse import parse_qs, quote, unquote, urlencode
from wsgiref.simple_server import WSGIServer, make_server

from logfusion.cases import CASE_STATUSES, CaseEngine, CaseError, search_cases
from logfusion.config import load_kafka_config, load_sources_config
from logfusion.project import ProjectConfig, project_status
from logfusion.investigation import get_case_investigation
from logfusion.source_management import (
    SourceManagementError,
    add_kafka_source,
    add_local_source,
    preview_source,
    validate_source_settings,
)
from logfusion.rules import NUMERIC_FIELDS, RuleError, add_rule_version, load_rules, set_rule_status, test_rule, validate_rule


MAX_REQUEST_BYTES = 512 * 1024
MAX_JOB_OUTPUT = 100_000
JOB_KINDS = ("parse", "readiness", "evaluate", "rules_refresh")


class ConsoleError(ValueError):
    """Raised when the local console receives an invalid operation."""


class JobStore:
    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self._connection() as connection:
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS jobs (
                    job_id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    status TEXT NOT NULL,
                    arguments_json TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    started_at INTEGER,
                    finished_at INTEGER,
                    exit_code INTEGER,
                    output TEXT NOT NULL DEFAULT '',
                    error TEXT NOT NULL DEFAULT ''
                );
                CREATE INDEX IF NOT EXISTS idx_jobs_status_created ON jobs(status, created_at);
            """)
            connection.execute(
                "UPDATE jobs SET status='failed',finished_at=?,error='Console restarted while this job was running.' WHERE status='running'",
                (_now_ms(),),
            )

    def submit(self, kind: str, arguments: dict[str, Any] | None = None) -> str:
        if kind not in JOB_KINDS:
            raise ConsoleError(f"unsupported console job: {kind}")
        job_id = uuid.uuid4().hex
        with self._connection() as connection:
            connection.execute(
                "INSERT INTO jobs(job_id,kind,status,arguments_json,created_at) VALUES (?,?, 'pending', ?, ?)",
                (job_id, kind, json.dumps(arguments or {}, ensure_ascii=False, sort_keys=True), _now_ms()),
            )
        return job_id

    def list(self, limit: int = 100) -> list[dict[str, Any]]:
        if limit <= 0:
            return []
        with self._connection() as connection:
            rows = connection.execute("SELECT * FROM jobs ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
        return [_job(row) for row in rows]

    def get(self, job_id: str) -> dict[str, Any] | None:
        with self._connection() as connection:
            row = connection.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()
        return _job(row) if row else None

    def claim_next(self) -> dict[str, Any] | None:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute("SELECT * FROM jobs WHERE status='pending' ORDER BY created_at LIMIT 1").fetchone()
            if row is None:
                connection.commit()
                return None
            connection.execute("UPDATE jobs SET status='running',started_at=? WHERE job_id=?", (_now_ms(), row["job_id"]))
            connection.commit()
            claimed = dict(row)
            claimed["status"] = "running"
            claimed["started_at"] = _now_ms()
            return _job(claimed)
        finally:
            connection.close()

    def finish(self, job_id: str, exit_code: int, output: str = "", error: str = "") -> None:
        status = "succeeded" if exit_code == 0 else "failed"
        with self._connection() as connection:
            connection.execute(
                "UPDATE jobs SET status=?,finished_at=?,exit_code=?,output=?,error=? WHERE job_id=? AND status='running'",
                (status, _now_ms(), exit_code, output[-MAX_JOB_OUTPUT:], error[-MAX_JOB_OUTPUT:], job_id),
            )

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=5)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA busy_timeout=5000")
        return connection

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            with connection:
                yield connection
        finally:
            connection.close()


class JobRunner:
    def __init__(self, project: ProjectConfig, store: JobStore, poll_seconds: float = 0.25) -> None:
        self.project = project
        self.store = store
        self.poll_seconds = poll_seconds
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._run, name="logfusion-console-jobs", daemon=True)
        self.process: subprocess.Popen[str] | None = None
        self.process_lock = threading.Lock()

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        with self.process_lock:
            process = self.process
        if process is not None and process.poll() is None:
            process.terminate()
        self.thread.join(timeout=5)
        with self.process_lock:
            remaining = self.process
        if self.thread.is_alive() and remaining is not None and remaining.poll() is None:
            remaining.kill()
            self.thread.join(timeout=2)

    def _run(self) -> None:
        while not self.stop_event.is_set():
            job = self.store.claim_next()
            if job is None:
                self.stop_event.wait(self.poll_seconds)
                continue
            if self.stop_event.is_set():
                self.store.finish(job["job_id"], 1, error="Console stopped before the job started.")
                return
            try:
                command = build_job_command(self.project, job["kind"], job["arguments"])
                process = subprocess.Popen(command, cwd=self.project.root, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                with self.process_lock:
                    self.process = process
                output, error = process.communicate()
                self.store.finish(job["job_id"], process.returncode, output, error)
            except Exception as exc:
                self.store.finish(job["job_id"], 1, error=f"{type(exc).__name__}: {exc}")
            finally:
                with self.process_lock:
                    self.process = None


class ConsoleApp:
    def __init__(self, project: ProjectConfig, store: JobStore | None = None, demo: bool = False) -> None:
        self.project = project
        self.store = store or JobStore(project.jobs_state)
        self.demo = demo
        self.csrf_token = secrets.token_urlsafe(24)
        self.demo_data = _load_demo(project) if demo else None

    def __call__(self, environ: dict[str, Any], start_response: Callable[..., Any]) -> Iterable[bytes]:
        try:
            method = environ.get("REQUEST_METHOD", "GET").upper()
            path = environ.get("PATH_INFO", "/")
            if method == "GET" and path == "/api/status":
                return self._json(start_response, "200 OK", project_status(self.project))
            if method == "GET" and path == "/":
                return self._html(start_response, self._overview())
            if method == "GET" and path == "/sources":
                return self._html(start_response, self._sources())
            if method == "GET" and path == "/sources/new":
                return self._html(start_response, self._source_wizard())
            if method == "GET" and path == "/jobs":
                return self._html(start_response, self._jobs())
            if method == "GET" and path.startswith("/jobs/"):
                return self._html(start_response, self._job_detail(path.rsplit("/", 1)[-1]))
            if method == "GET" and path == "/readiness":
                return self._html(start_response, self._readiness())
            if method == "GET" and path == "/experiments":
                return self._html(start_response, self._experiments())
            if method == "GET" and path == "/cases":
                return self._html(start_response, self._cases(environ))
            if method == "GET" and path.startswith("/cases/"):
                return self._html(start_response, self._case_detail(environ, self._case_id(path)))
            if method == "GET" and path == "/rules":
                return self._html(start_response, self._rules())
            if method == "GET" and path == "/rules/new":
                return self._html(start_response, self._rule_editor())
            if method == "GET" and path.startswith("/rules/edit/"):
                return self._html(start_response, self._edit_rule(path))
            if method == "POST" and path == "/jobs/readiness":
                return self._submit_readiness(environ, start_response)
            if method == "POST" and path == "/jobs/parse":
                return self._submit_parse(environ, start_response)
            if method == "POST" and path == "/jobs/evaluate":
                return self._submit_evaluation(environ, start_response)
            if method == "POST" and path == "/sources/preview":
                return self._preview_source(environ, start_response)
            if method == "POST" and path == "/sources/save":
                return self._save_source(environ, start_response)
            if method == "POST" and path == "/rules/test":
                return self._test_rule(environ, start_response)
            if method == "POST" and path == "/rules/save":
                return self._save_rule(environ, start_response)
            if method == "POST" and path == "/rules/import":
                return self._import_rule(environ, start_response)
            if method == "POST" and path == "/rules/status":
                return self._set_rule_status(environ, start_response)
            if method == "POST" and path.startswith("/cases/"):
                return self._case_action(environ, start_response, path)
            return self._html(start_response, _layout("未找到", "", "<section class='panel'><h1>页面不存在</h1></section>", self.demo), "404 Not Found")
        except (ConsoleError, SourceManagementError, RuleError) as exc:
            return self._html(start_response, _layout("请求失败", "", f"<section class='panel'><h1>请求失败</h1><p>{_e(exc)}</p></section>", self.demo), "400 Bad Request")
        except Exception as exc:
            return self._html(start_response, _layout("控制台错误", "", f"<section class='panel'><h1>控制台错误</h1><p>{_e(exc)}</p></section>", self.demo), "500 Internal Server Error")

    def _overview(self) -> str:
        status = project_status(self.project)
        readiness = self._readiness_data()
        states_ready = sum(item["exists"] for item in status["states"].values())
        jobs = self.store.list(10)
        running = sum(job["status"] in {"pending", "running"} for job in jobs)
        if self.demo_data:
            overview = self.demo_data["overview"]
            cards = [
                ("日志源", overview["source_count"], "已配置来源"),
                ("采集历史", f"{overview['history_days']} 天", "UTC 自然日"),
                ("解析成功率", f"{overview['parse_success_rate']:.1%}", "示例指标"),
                ("待处理 Case", overview["open_cases"], "需要分析"),
            ]
        else:
            cards = [
                ("状态库", f"{states_ready}/{len(status['states'])}", "已创建"),
                ("后台任务", running, "等待或运行中"),
                ("历史准备度", f"{readiness.get('history', {}).get('observed_days', 0)}/30", "UTC 天"),
                ("Case", len(self._case_data()), "当前记录"),
            ]
        body = "<header class='hero'><div><p class='eyebrow'>SECURITY OPERATIONS</p><h1>LogFusion 控制台</h1><p>数据接入、准备度、检测实验与分析处置集中在一个工作区。</p></div></header>"
        body += "<section class='cards'>" + "".join(_card(*card) for card in cards) + "</section>"
        body += "<section class='grid two'><div class='panel'><h2>数据准备度</h2>" + _readiness_summary(readiness) + "<a class='button secondary' href='/readiness'>查看详情</a></div>"
        body += "<div class='panel'><h2>最近任务</h2>" + _job_table(jobs[:5]) + "<a class='button secondary' href='/jobs'>全部任务</a></div></section>"
        return _layout("总览", "overview", body, self.demo)

    def _sources(self) -> str:
        sources = self.demo_data["sources"] if self.demo_data else _source_data(self.project)
        rows = [[item.get("source_id", "—"), item.get("kind", "local"), item.get("source_type", "auto"), item.get("record_mode", "—"), ", ".join(item.get("paths", [])) or ", ".join(item.get("topics", [])) or "—"] for item in sources]
        body = "<header class='page-head'><div><p class='eyebrow'>INGESTION</p><h1>数据源</h1><p>查看当前配置的数据来源和解析方式。</p></div>"
        if not self.demo:
            body += "<a class='button' href='/sources/new'>接入新数据源</a>"
        body += "</header>"
        body += "<section class='panel'>" + _table(["来源 ID", "接入方式", "日志类型", "记录模式", "路径 / Topic"], rows, "尚未配置数据源") + "</section>"
        body += "<section class='notice'><strong>安全约束</strong><span>样例只用于当前请求的解析预览，不写入配置；Kafka 密码通过环境变量提供。</span></section>"
        return _layout("数据源", "sources", body, self.demo)

    def _source_wizard(self, values: dict[str, str] | None = None, result: dict[str, Any] | None = None) -> str:
        values = values or {"kind": "local", "source_type": "auto", "record_mode": "line"}
        body = "<header class='page-head'><div><p class='eyebrow'>SOURCE ONBOARDING</p><h1>接入新数据源</h1><p>配置来源，提交一条脱敏样例，并在保存前验证 Canonical 映射。</p></div><a class='button secondary' href='/sources'>返回列表</a></header>"
        body += _source_form(self.csrf_token, values)
        if result is not None:
            body += _source_preview(self.csrf_token, values, result)
        return _layout("接入数据源", "sources", body, self.demo)

    def _jobs(self) -> str:
        body = "<header class='page-head'><div><p class='eyebrow'>OPERATIONS</p><h1>任务中心</h1><p>页面关闭后后台任务仍会继续执行。</p></div></header>"
        body += "<section class='panel'>" + _job_table(self.store.list()) + "</section>"
        return _layout("任务中心", "jobs", body, self.demo)

    def _rules(self) -> str:
        document = load_rules(_optional_project_file(self.project, "rules", "config/rules.local.json"))
        rows = []
        for rule in sorted(document["rules"], key=lambda item: (item["rule_id"], -item["version"])):
            report = test_rule(rule)
            actions = [] if self.demo else [f"<a href='/rules/edit/{_e(rule['rule_id'])}/{rule['version']}'>新版本</a>"]
            if not self.demo:
                for status in _rule_next_statuses(rule["status"]):
                    actions.append(_rule_status_button(self.csrf_token, rule, status))
            rows.append([
                _e(rule["rule_id"]), _e(f"v{rule['version']}"), _e(rule["scope"]), _e(rule.get("window_size", "—")),
                f"<span class='status {rule['status']}'>{_e(rule['status'])}</span>",
                _e("通过" if report["passed"] else f"{report['test_count']} 条 / 未通过"), " ".join(actions) or "—",
            ])
        body = "<header class='page-head'><div><p class='eyebrow'>DETECTION RULES</p><h1>检测规则</h1><p>规则与统计模型输出同一种 Candidate；shadow 命中不会进入关联和告警。</p></div>"
        if not self.demo:
            body += "<a class='button' href='/rules/new'>新建规则</a>"
        body += "</header><section class='panel'>" + _raw_table(["规则", "版本", "范围", "窗口", "状态", "测试", "操作"], rows, "尚无检测规则") + "</section>"
        if not self.demo:
            body += _rule_import_form(self.csrf_token)
        return _layout("检测规则", "rules", body, self.demo)

    def _rule_editor(self, values: dict[str, Any] | None = None, report: dict[str, Any] | None = None) -> str:
        body = "<header class='page-head'><div><p class='eyebrow'>RULE EDITOR</p><h1>规则编辑器</h1><p>v1 支持平铺 all/any 和最多五个条件，不执行任意代码或正则。</p></div><a class='button secondary' href='/rules'>返回规则</a></header>"
        body += _rule_form(self.csrf_token, values or {})
        if report is not None:
            rows = [[item["name"], item["expected_match"], item["actual_match"], "通过" if item["passed"] else "失败"] for item in report["results"]]
            body += "<section class='panel'><h2>测试结果</h2>" + _table(["样例", "期望", "实际", "结果"], rows, "没有测试样例") + "</section>"
        return _layout("规则编辑器", "rules", body, self.demo)

    def _edit_rule(self, path: str) -> str:
        parts = path.split("/")
        if len(parts) != 5 or not parts[4].isdigit():
            raise ConsoleError("invalid rule edit path")
        rule_id, version = parts[3], int(parts[4])
        document = load_rules(_optional_project_file(self.project, "rules", "config/rules.local.json"))
        rule = next((item for item in document["rules"] if item["rule_id"] == rule_id and item["version"] == version), None)
        if rule is None:
            raise ConsoleError("rule does not exist")
        values = _rule_to_form(rule)
        values["version"] = str(max(item["version"] for item in document["rules"] if item["rule_id"] == rule_id) + 1)
        values["status"] = "draft"
        return self._rule_editor(values)

    def _job_detail(self, job_id: str) -> str:
        if not job_id.isalnum():
            raise ConsoleError("invalid job id")
        job = self.store.get(job_id)
        if job is None:
            return _layout("任务不存在", "jobs", "<section class='panel'><h1>任务不存在</h1></section>", self.demo)
        body = f"<header class='page-head'><div><p class='eyebrow'>JOB</p><h1>{_e(job['kind'])}</h1><p>{_e(job['job_id'])}</p></div><span class='status {job['status']}'>{_e(job['status'])}</span></header>"
        body += f"<section class='grid two'><div class='panel'><h2>参数</h2><pre>{_e(json.dumps(job['arguments'], ensure_ascii=False, indent=2))}</pre></div><div class='panel'><h2>输出</h2><pre>{_e(job['output'] or job['error'] or '暂无输出')}</pre></div></section>"
        return _layout("任务详情", "jobs", body, self.demo)

    def _readiness(self) -> str:
        readiness = self._readiness_data()
        body = "<header class='page-head'><div><p class='eyebrow'>DATA READINESS</p><h1>数据准备度</h1><p>仅检查无监督检测所需的历史跨度、窗口样本和群体覆盖。</p></div>"
        if not self.demo:
            body += _post_button("/jobs/readiness", "重新检查", self.csrf_token)
        body += "</header><section class='panel'>" + _readiness_summary(readiness, detailed=True) + "</section>"
        actions = readiness.get("next_actions", [])
        body += "<section class='panel'><h2>下一步动作</h2>" + ("<ol class='actions'>" + "".join(f"<li>{_e(item.get('message', item))}</li>" for item in actions) + "</ol>" if actions else "<p class='muted'>暂无建议，或尚未生成报告。</p>") + "</section>"
        return _layout("数据准备度", "readiness", body, self.demo)

    def _experiments(self) -> str:
        experiments = self.demo_data["experiments"] if self.demo_data else _experiment_data(self.project)
        rows = [[item.get("name", "—"), item.get("detector_id", "—"), _metric(item, "candidate_count"), _metric(item, "unique_predicted_users"), _metric(item, "average_daily_predictions"), _metric(item, "daily_prediction_cv"), item.get("from", "—")] for item in experiments]
        body = "<header class='page-head'><div><p class='eyebrow'>EVALUATION</p><h1>检测实验</h1><p>在相同 UTC 范围内比较无监督检测量、覆盖面和稳定性。</p></div></header>"
        if not self.demo:
            body += _experiment_form(self.csrf_token)
        body += "<section class='panel'>" + _table(["实验", "检测器", "Candidate", "异常用户", "每日预测", "日波动 CV", "起始时间"], rows, "尚无实验") + "</section>"
        return _layout("检测实验", "experiments", body, self.demo)

    def _cases(self, environ: dict[str, Any]) -> str:
        filters = self._case_filters(environ)
        page = int(filters.pop("page"))
        if self.demo_data:
            document = _search_demo_cases(self.demo_data["cases"], filters, page)
        else:
            case_path = self.project.state("case")
            document = search_cases(case_path, **filters, page=page) if case_path.exists() else _empty_case_search(page)
        summary = document["summary"]
        cards = "<section class='cards'>" + "".join((
            _card("全部 Case", summary["total"], "本地审计状态"),
            _card("待复核", summary["requires_review"], "告警版本已更新"),
            _card("未分配", summary["unassigned"], "尚无负责人"),
            _card("筛选结果", document["total"], f"第 {document['page']} 页"),
        )) + "</section>"
        filter_form = _case_filter_form(filters)
        rows = []
        for item in document["items"]:
            review = "<span class='review-flag'>待复核</span>" if item.get("requires_review") else "—"
            rows.append([
                f"<a href='/cases/{_e(item['case_id'])}'>{_e(item.get('user_name', '—'))}</a>",
                f"<span class='status {_e(item.get('status', ''))}'>{_e(item.get('status', '—'))}</span>",
                _e(item.get("severity", "—")), _e(item.get("risk_score", "—")), _e(item.get("owner") or "未分配"),
                _e(", ".join(item.get("tags", [])) or "—"), review, _e(item.get("last_seen", "—")),
            ])
        body = "<header class='page-head'><div><p class='eyebrow'>INVESTIGATION</p><h1>Cases</h1><p>Case 只服务调查、处置和审计，不产生模型训练标签。</p></div></header>"
        body += cards + filter_form
        body += "<section class='panel'>" + _raw_table(["用户", "状态", "严重度", "风险分", "负责人", "标签", "复核", "最后出现"], rows, "尚无 Case") + _case_pagination(document, filters) + "</section>"
        return _layout("Cases", "cases", body, self.demo)

    def _case_detail(self, environ: dict[str, Any], case_id: str, error: str | None = None) -> str:
        actor = self._actor(environ)
        if self.demo_data:
            investigation = _demo_investigation(self.demo_data, case_id)
        else:
            investigation = get_case_investigation(
                self.project.state("case"), self.project.state("risk"), self.project.state("incident"),
                self.project.state("detection"), case_id,
            )
        return _case_detail_page(investigation, self.csrf_token, actor, self.demo, error)

    def _case_action(
        self, environ: dict[str, Any], start_response: Callable[..., Any], path: str,
    ) -> Iterable[bytes]:
        parts = path.strip("/").split("/")
        if len(parts) != 3 or parts[0] != "cases" or parts[2] not in {"transition", "assign", "comment", "tags"}:
            raise ConsoleError("invalid Case action")
        case_id, action = self._validated_case_id(parts[1]), parts[2]
        form = self._form(environ)
        actor = _one(form, "actor").strip()
        try:
            self._authorize(form)
            self._ensure_real()
            if not actor or len(actor) > 80:
                raise ConsoleError("分析员名称必须为 1-80 个字符")
            with CaseEngine.for_operations(self.project.state("case")) as engine:
                if action == "transition":
                    status = _one(form, "status")
                    suppression_until = _suppression_until(status, _one(form, "suppression_duration"), _one(form, "suppression_custom"))
                    engine.transition(case_id, status, actor, _one(form, "note").strip() or None, suppression_until)
                elif action == "assign":
                    engine.assign(case_id, _one(form, "owner"), actor, _one(form, "note").strip() or None)
                elif action == "comment":
                    note = _one(form, "note").strip()
                    if not note or len(note) > 4000:
                        raise ConsoleError("评论必须为 1-4000 个字符")
                    engine.comment(case_id, actor, note)
                else:
                    tags = [item.strip() for item in _one(form, "tags").split(",") if item.strip()]
                    engine.set_tags(case_id, tags, actor)
            cookie = f"logfusion_actor={quote(actor, safe='')}; Max-Age=2592000; Path=/; HttpOnly; SameSite=Lax"
            return self._redirect(start_response, f"/cases/{case_id}?updated={action}", [("Set-Cookie", cookie)])
        except (CaseError, ConsoleError) as exc:
            retry_environ = dict(environ)
            if actor:
                retry_environ["HTTP_COOKIE"] = f"logfusion_actor={quote(actor, safe='')}"
            return self._html(start_response, self._case_detail(retry_environ, case_id, str(exc)), "400 Bad Request")

    def _case_filters(self, environ: dict[str, Any]) -> dict[str, Any]:
        query = parse_qs(environ.get("QUERY_STRING", ""), keep_blank_values=False)
        page_value = _query_one(query, "page") or "1"
        if not page_value.isdigit() or int(page_value) < 1:
            raise ConsoleError("invalid Case page")
        status = _query_one(query, "status") or None
        severity = _query_one(query, "severity") or None
        review_value = _query_one(query, "review")
        if review_value not in {"", "yes", "no"}:
            raise ConsoleError("invalid Case review filter")
        return {
            "user_name": (_query_one(query, "user") or "").strip() or None,
            "status": status,
            "severity": severity,
            "owner": (_query_one(query, "owner") or "").strip() or None,
            "requires_review": True if review_value == "yes" else (False if review_value == "no" else None),
            "page": int(page_value),
        }

    def _case_id(self, path: str) -> str:
        parts = path.strip("/").split("/")
        if len(parts) != 2 or parts[0] != "cases":
            raise ConsoleError("invalid Case path")
        return self._validated_case_id(parts[1])

    @staticmethod
    def _validated_case_id(value: str) -> str:
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", value):
            raise ConsoleError("invalid Case ID")
        return value

    @staticmethod
    def _actor(environ: dict[str, Any]) -> str:
        cookie = SimpleCookie()
        try:
            cookie.load(environ.get("HTTP_COOKIE", ""))
            return unquote(cookie["logfusion_actor"].value)[:80] if "logfusion_actor" in cookie else ""
        except (CookieError, UnicodeDecodeError):
            return ""

    def _submit_readiness(self, environ: dict[str, Any], start_response: Callable[..., Any]) -> Iterable[bytes]:
        form = self._form(environ)
        self._authorize(form)
        self._ensure_real()
        job_id = self.store.submit("readiness")
        return self._redirect(start_response, f"/jobs/{job_id}")

    def _submit_parse(self, environ: dict[str, Any], start_response: Callable[..., Any]) -> Iterable[bytes]:
        form = self._form(environ)
        self._authorize(form)
        self._ensure_real()
        job_id = self.store.submit("parse")
        return self._redirect(start_response, f"/jobs/{job_id}")

    def _test_rule(self, environ: dict[str, Any], start_response: Callable[..., Any]) -> Iterable[bytes]:
        form = self._form(environ)
        self._authorize(form)
        values = {key: _one(form, key) for key in form if key != "csrf"}
        rule = validate_rule(_rule_from_form(form))
        return self._html(start_response, self._rule_editor(values, test_rule(rule)))

    def _save_rule(self, environ: dict[str, Any], start_response: Callable[..., Any]) -> Iterable[bytes]:
        form = self._form(environ)
        self._authorize(form)
        self._ensure_real()
        rule = add_rule_version(_optional_project_file(self.project, "rules", "config/rules.local.json"), _rule_from_form(form))
        if rule["status"] in {"shadow", "active"}:
            job_id = self.store.submit("rules_refresh")
            return self._redirect(start_response, f"/jobs/{job_id}")
        return self._redirect(start_response, f"/rules?created={rule['rule_id']}")

    def _import_rule(self, environ: dict[str, Any], start_response: Callable[..., Any]) -> Iterable[bytes]:
        form = self._form(environ)
        self._authorize(form)
        self._ensure_real()
        try:
            item = json.loads(_one(form, "rule_json"))
        except json.JSONDecodeError as exc:
            raise ConsoleError(f"invalid rule JSON: {exc}") from exc
        if not isinstance(item, dict):
            raise ConsoleError("imported rule must be a JSON object")
        rule = add_rule_version(_optional_project_file(self.project, "rules", "config/rules.local.json"), item)
        if rule["status"] in {"shadow", "active"}:
            job_id = self.store.submit("rules_refresh")
            return self._redirect(start_response, f"/jobs/{job_id}")
        return self._redirect(start_response, "/rules")

    def _set_rule_status(self, environ: dict[str, Any], start_response: Callable[..., Any]) -> Iterable[bytes]:
        form = self._form(environ)
        self._authorize(form)
        self._ensure_real()
        version = _one(form, "version")
        if not version.isdigit():
            raise ConsoleError("invalid rule version")
        set_rule_status(
            _optional_project_file(self.project, "rules", "config/rules.local.json"),
            _one(form, "rule_id"), int(version), _one(form, "status"),
        )
        job_id = self.store.submit("rules_refresh")
        return self._redirect(start_response, f"/jobs/{job_id}")

    def _preview_source(self, environ: dict[str, Any], start_response: Callable[..., Any]) -> Iterable[bytes]:
        form = self._form(environ)
        self._authorize(form)
        values = _source_values(form)
        settings = validate_source_settings(values)
        sample = _one(form, "sample").strip() or _one(form, "sample_file")
        result = preview_source(settings, sample)
        return self._html(start_response, self._source_wizard(values, result))

    def _save_source(self, environ: dict[str, Any], start_response: Callable[..., Any]) -> Iterable[bytes]:
        form = self._form(environ)
        self._authorize(form)
        self._ensure_real()
        values = _source_values(form)
        settings = validate_source_settings(values)
        if settings["kind"] == "local":
            add_local_source(self.project.resolve("files", "sources"), settings)
            action = _post_button("/jobs/parse", "立即解析本地日志", self.csrf_token)
            detail = "配置已原子写入。你可以立即提交一次批量解析任务。"
        else:
            kafka_path = _optional_project_file(self.project, "kafka", "config/kafka.local.yaml")
            add_kafka_source(kafka_path, settings)
            action = "<a class='button secondary' href='/sources'>返回数据源</a>"
            detail = "Kafka 配置已原子写入。连接凭据仍需通过环境变量配置，持续消费任务暂未自动启动。"
        body = f"<header class='page-head'><div><p class='eyebrow'>SOURCE SAVED</p><h1>数据源已保存</h1><p>{_e(settings['source_id'])}</p></div></header><section class='panel success-panel'><h2>接入配置已就绪</h2><p>{_e(detail)}</p>{action}</section>"
        return self._html(start_response, _layout("数据源已保存", "sources", body, self.demo))

    def _submit_evaluation(self, environ: dict[str, Any], start_response: Callable[..., Any]) -> Iterable[bytes]:
        form = self._form(environ)
        self._authorize(form)
        self._ensure_real()
        detector = _one(form, "detector")
        name = _one(form, "name").strip()
        start, end = _one(form, "from"), _one(form, "to")
        if detector not in {"statistical", "hbos", "isolation_forest"}:
            raise ConsoleError("invalid detector")
        if not name or len(name) > 80:
            raise ConsoleError("experiment name must be 1-80 characters")
        _validate_utc_day(start)
        _validate_utc_day(end)
        if start >= end:
            raise ConsoleError("experiment end must be after start")
        job_id = self.store.submit("evaluate", {"detector": detector, "name": name, "from": start, "to": end})
        return self._redirect(start_response, f"/jobs/{job_id}")

    def _form(self, environ: dict[str, Any]) -> dict[str, list[str]]:
        try:
            length = int(environ.get("CONTENT_LENGTH") or 0)
        except ValueError as exc:
            raise ConsoleError("invalid request length") from exc
        if length < 0 or length > MAX_REQUEST_BYTES:
            raise ConsoleError("request body is too large")
        body = environ["wsgi.input"].read(length)
        content_type = environ.get("CONTENT_TYPE", "application/x-www-form-urlencoded")
        if content_type.startswith("application/x-www-form-urlencoded"):
            return parse_qs(body.decode("utf-8"), keep_blank_values=True)
        if content_type.startswith("multipart/form-data"):
            message = BytesParser(policy=policy.default).parsebytes(
                f"Content-Type: {content_type}\r\nMIME-Version: 1.0\r\n\r\n".encode("ascii") + body
            )
            if not message.is_multipart():
                raise ConsoleError("invalid multipart request")
            form: dict[str, list[str]] = {}
            for part in message.iter_parts():
                name = part.get_param("name", header="content-disposition")
                if not name:
                    continue
                payload = part.get_payload(decode=True) or b""
                form.setdefault(name, []).append(payload.decode("utf-8", errors="replace"))
            return form
        raise ConsoleError("unsupported form content type")

    def _authorize(self, form: dict[str, list[str]]) -> None:
        if not secrets.compare_digest(_one(form, "csrf"), self.csrf_token):
            raise ConsoleError("invalid CSRF token")

    def _ensure_real(self) -> None:
        if self.demo:
            raise ConsoleError("demo mode is read-only")

    def _readiness_data(self) -> dict[str, Any]:
        if self.demo_data:
            return self.demo_data["readiness"]
        path = self.project.resolve("files", "readiness_report")
        if not path.exists():
            return {}
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
            return value if isinstance(value, dict) else {}
        except (OSError, json.JSONDecodeError):
            return {}

    @staticmethod
    def _html(start_response: Callable[..., Any], body: str, status: str = "200 OK") -> Iterable[bytes]:
        encoded = body.encode("utf-8")
        start_response(status, _headers("text/html; charset=utf-8", len(encoded)))
        return [encoded]

    @staticmethod
    def _json(start_response: Callable[..., Any], status: str, document: dict[str, Any]) -> Iterable[bytes]:
        encoded = (json.dumps(document, ensure_ascii=False, sort_keys=True) + "\n").encode("utf-8")
        start_response(status, _headers("application/json; charset=utf-8", len(encoded)))
        return [encoded]

    @staticmethod
    def _redirect(start_response: Callable[..., Any], location: str, extra_headers: list[tuple[str, str]] | None = None) -> Iterable[bytes]:
        start_response("303 See Other", [("Location", location), *(extra_headers or []), *(_headers("text/plain; charset=utf-8", 0))])
        return [b""]


class ThreadingWSGIServer(ThreadingMixIn, WSGIServer):
    daemon_threads = True


def serve_console(project: ProjectConfig, host: str | None = None, port: int | None = None, demo: bool = False) -> None:
    bind_host = host or str(project.value("runtime", "host"))
    bind_port = port or int(project.value("runtime", "port"))
    store = JobStore(project.jobs_state)
    runner = JobRunner(project, store)
    app = ConsoleApp(project, store, demo)
    server = make_server(bind_host, bind_port, app, server_class=ThreadingWSGIServer)
    runner.start()
    print(f"LogFusion Console: http://{bind_host}:{bind_port}")
    if demo:
        print("DEMO DATA — not real detection results")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.shutdown()
        runner.stop()


def build_job_command(project: ProjectConfig, kind: str, arguments: dict[str, Any]) -> list[str]:
    common = [sys.executable, "-m", "logfusion"]
    if kind == "parse":
        output = project.root / "output"
        return common + [
            "parse",
            "--config", str(project.resolve("files", "sources")),
            "--output", str(output / "normalized.jsonl"),
            "--unknown-output", str(output / "unknown.jsonl"),
            "--summary-output", str(output / "summary.json"),
            "--checkpoint", str(output / "parse.checkpoint.json"),
        ]
    if kind == "readiness":
        command = common + [
            "evaluate", "readiness",
            "--feature-state", str(project.state("feature")),
            "--baseline-state", str(project.state("baseline")),
            "--report-output", str(project.resolve("files", "readiness_report")),
        ]
        model_config = project.resolve("files", "model_detection")
        if model_config.exists():
            command += ["--model-config", str(model_config)]
        return command
    if kind == "rules_refresh":
        return common + [
            "detect", "run",
            "--feature-state", str(project.state("feature")),
            "--baseline-state", str(project.state("baseline")),
            "--state", str(project.state("detection")),
            "--rules", str(_optional_project_file(project, "rules", "config/rules.local.json")),
            "--refresh-rules",
        ]
    if kind == "evaluate":
        detector = str(arguments.get("detector", ""))
        if detector not in {"statistical", "hbos", "isolation_forest"}:
            raise ConsoleError("invalid evaluation detector")
        command = common + [
            "evaluate", "run",
            "--feature-state", str(project.state("feature")),
            "--baseline-state", str(project.state("baseline")),
            "--state", str(project.state("evaluation")),
            "--detector", detector,
            "--name", str(arguments["name"]),
            "--from", str(arguments["from"]),
            "--to", str(arguments["to"]),
        ]
        model_config = project.resolve("files", "model_detection")
        if detector != "statistical" and model_config.exists():
            command += ["--model-config", str(model_config)]
        return command
    raise ConsoleError(f"unsupported console job: {kind}")


def seed_demo(project: ProjectConfig, force: bool = False) -> Path:
    path = project.demo_state
    if path.exists() and not force:
        raise ConsoleError(f"demo data already exists: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_demo_document(), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _load_demo(project: ProjectConfig) -> dict[str, Any]:
    if project.demo_state.exists():
        try:
            value = json.loads(project.demo_state.read_text(encoding="utf-8"))
            if isinstance(value, dict):
                return value
        except (OSError, json.JSONDecodeError):
            pass
    return _demo_document()


def _demo_document() -> dict[str, Any]:
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "overview": {"source_count": 4, "history_days": 21, "parse_success_rate": 0.987, "open_cases": 4},
        "sources": [
            {"source_id": "svn-main", "source_type": "svn", "record_mode": "line", "paths": ["data/svn/*.log"]},
            {"source_id": "sso-main", "source_type": "sso", "record_mode": "object", "paths": ["data/sso/*.txt"]},
            {"source_id": "gitlab-main", "source_type": "gitlab", "record_mode": "line", "paths": ["data/gitlab/*.jsonl"]},
            {"source_id": "hiklink-main", "source_type": "hiklink", "record_mode": "object", "paths": ["data/hiklink/*.txt"]},
        ],
        "readiness": {
            "history": {"observed_days": 21, "target_days": 30, "missing_days": 9, "ready": False},
            "detectors": {
                "statistical": {"training_status": "not_ready", "evaluation_ready": False, "evaluation_mode": "unsupervised"},
                "hbos": {"training_status": "not_ready", "evaluation_ready": False, "evaluation_mode": "unsupervised"},
                "isolation_forest": {"training_status": "not_ready", "evaluation_ready": False, "evaluation_mode": "unsupervised"},
            },
            "next_actions": [
                {"message": "继续采集 9 个 UTC 自然日。"},
                {"message": "继续积累各窗口的真实行为样本。"},
            ],
        },
        "experiments": [
            {"name": "statistical-demo", "detector_id": "statistical", "from": "2026-06-01", "metrics": {"candidate_count": 162, "unique_predicted_users": 34, "average_daily_predictions": 18.4, "daily_prediction_cv": 0.31}},
            {"name": "hbos-demo", "detector_id": "hbos", "from": "2026-06-01", "metrics": {"candidate_count": 104, "unique_predicted_users": 27, "average_daily_predictions": 12.1, "daily_prediction_cv": 0.24}},
            {"name": "iforest-demo", "detector_id": "isolation_forest", "from": "2026-06-01", "metrics": {"candidate_count": 131, "unique_predicted_users": 31, "average_daily_predictions": 15.3, "daily_prediction_cv": 0.28}},
        ],
        "cases": [
            {"case_id": "demo-case-1", "current_alert_id": "alert-demo-1", "incident_id": "incident-demo-1", "user_name": "wangkun78", "status": "investigating", "severity": "critical", "risk_score": 94, "owner": "alice", "tags": ["privileged", "cross-system"], "requires_review": True, "suppression_until": None, "first_seen": "2026-07-13T10:55:00Z", "last_seen": "2026-07-13T11:35:00Z"},
            {"case_id": "demo-case-2", "current_alert_id": "alert-demo-2", "incident_id": "incident-demo-2", "user_name": "ops-admin", "status": "new", "severity": "high", "risk_score": 86, "owner": None, "tags": ["authentication"], "requires_review": False, "suppression_until": None, "first_seen": "2026-07-13T08:40:00Z", "last_seen": "2026-07-13T09:10:00Z"},
            {"case_id": "demo-case-3", "current_alert_id": "alert-demo-3", "incident_id": "incident-demo-3", "user_name": "lihua", "status": "resolved", "severity": "high", "risk_score": 82, "owner": "bob", "tags": ["repository"], "requires_review": False, "suppression_until": None, "first_seen": "2026-07-12T18:05:00Z", "last_seen": "2026-07-12T18:45:00Z"},
            {"case_id": "demo-case-4", "current_alert_id": "alert-demo-4", "incident_id": "incident-demo-4", "user_name": "zhangsan", "status": "false_positive", "severity": "high", "risk_score": 80, "owner": "alice", "tags": ["automation"], "requires_review": False, "suppression_until": None, "first_seen": "2026-07-12T02:58:00Z", "last_seen": "2026-07-12T03:22:00Z"},
        ],
    }


def _source_data(project: ProjectConfig) -> list[dict[str, Any]]:
    path = project.resolve("files", "sources")
    result: list[dict[str, Any]] = []
    try:
        if path.exists():
            result.extend({**item, "kind": "local"} for item in load_sources_config(path))
        kafka_path = _optional_project_file(project, "kafka", "config/kafka.local.yaml")
        if kafka_path.exists():
            kafka = load_kafka_config(kafka_path)
            if kafka:
                result.append({**kafka, "kind": "kafka", "record_mode": "stream"})
        return result
    except (OSError, ValueError):
        return []


def _experiment_data(project: ProjectConfig) -> list[dict[str, Any]]:
    path = project.state("evaluation")
    if not path.exists():
        return []
    try:
        connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        connection.row_factory = sqlite3.Row
        rows = connection.execute("""
            SELECT e.*,m.candidate_count,m.unique_predicted_users,m.average_daily_predictions,m.daily_prediction_cv
            FROM experiments e LEFT JOIN experiment_metrics m USING(experiment_id)
            ORDER BY e.created_at DESC
        """).fetchall()
        connection.close()
        result = []
        for row in rows:
            item = dict(row)
            item["from"] = _iso_ms(item.pop("start_ms"))
            item["to"] = _iso_ms(item.pop("end_ms"))
            item["metrics"] = {key: item.pop(key) for key in ("candidate_count", "unique_predicted_users", "average_daily_predictions", "daily_prediction_cv")}
            result.append(item)
        return result
    except sqlite3.Error:
        return []


def _empty_case_search(page: int = 1) -> dict[str, Any]:
    return {
        "items": [], "total": 0, "page": page, "page_size": 50,
        "summary": {"total": 0, "unassigned": 0, "requires_review": 0, "by_status": {item: 0 for item in CASE_STATUSES}},
    }


def _search_demo_cases(items: list[dict[str, Any]], filters: dict[str, Any], page: int) -> dict[str, Any]:
    selected = list(items)
    if filters.get("user_name"):
        needle = filters["user_name"].lower()
        selected = [item for item in selected if needle in item.get("user_name", "").lower()]
    for field in ("status", "severity"):
        if filters.get(field):
            selected = [item for item in selected if item.get(field) == filters[field]]
    if filters.get("owner") == "__unassigned__":
        selected = [item for item in selected if not item.get("owner")]
    elif filters.get("owner"):
        selected = [item for item in selected if item.get("owner") == filters["owner"]]
    if filters.get("requires_review") is not None:
        selected = [item for item in selected if bool(item.get("requires_review")) is filters["requires_review"]]
    selected.sort(key=lambda item: (not bool(item.get("requires_review")), -int(item.get("risk_score", 0)), item.get("last_seen", "")), reverse=False)
    by_status = {status: sum(item.get("status") == status for item in items) for status in CASE_STATUSES}
    return {
        "items": selected[(page - 1) * 50:page * 50], "total": len(selected), "page": page, "page_size": 50,
        "summary": {
            "total": len(items), "unassigned": sum(not item.get("owner") for item in items),
            "requires_review": sum(bool(item.get("requires_review")) for item in items), "by_status": by_status,
        },
    }


def _case_filter_form(filters: dict[str, Any]) -> str:
    status = filters.get("status") or ""
    severity = filters.get("severity") or ""
    owner = filters.get("owner") or ""
    review = "yes" if filters.get("requires_review") is True else ("no" if filters.get("requires_review") is False else "")
    return f"""<section class='panel'><form class='case-filters' method='get' action='/cases'>
<label>用户<input name='user' value='{_e(filters.get('user_name') or '')}' placeholder='用户名包含'></label>
<label>状态<select name='status'>{_option('', '全部', status)}{''.join(_option(item,item,status) for item in CASE_STATUSES)}</select></label>
<label>严重度<select name='severity'>{_option('', '全部', severity)}{''.join(_option(item,item,severity) for item in ('low','medium','high','critical'))}</select></label>
<label>负责人<input name='owner' value='{_e(owner)}' placeholder='姓名或 __unassigned__'></label>
<label>待复核<select name='review'>{_option('', '全部', review)}{_option('yes','是',review)}{_option('no','否',review)}</select></label>
<button class='button' type='submit'>筛选</button><a class='button secondary compact' href='/cases'>清除</a>
</form></section>"""


def _case_pagination(document: dict[str, Any], filters: dict[str, Any]) -> str:
    page, page_size, total = int(document["page"]), int(document["page_size"]), int(document["total"])
    pages = max(1, (total + page_size - 1) // page_size)
    if pages <= 1:
        return ""
    query = {
        "user": filters.get("user_name") or "", "status": filters.get("status") or "",
        "severity": filters.get("severity") or "", "owner": filters.get("owner") or "",
        "review": "yes" if filters.get("requires_review") is True else ("no" if filters.get("requires_review") is False else ""),
    }
    links = []
    if page > 1:
        links.append(f"<a href='/cases?{_e(urlencode({**query, 'page': page - 1}))}'>上一页</a>")
    links.append(f"<span>第 {page}/{pages} 页</span>")
    if page < pages:
        links.append(f"<a href='/cases?{_e(urlencode({**query, 'page': page + 1}))}'>下一页</a>")
    return "<nav class='pagination' aria-label='Case 分页'>" + "".join(links) + "</nav>"


def _demo_investigation(document: dict[str, Any], case_id: str) -> dict[str, Any]:
    case = next((item for item in document["cases"] if item["case_id"] == case_id), None)
    if case is None:
        raise CaseError(f"case does not exist: {case_id}")
    case = {**case, "events": [
        {"event_id": 1, "event_type": "created", "actor": "system", "created_at": case["first_seen"], "details": {"alert_id": case["current_alert_id"]}},
        {"event_id": 2, "event_type": "owner_changed", "actor": "alice", "created_at": case["last_seen"], "details": {"from": None, "to": case.get("owner")}},
    ]}
    candidate_id = f"candidate-{case_id}"
    evidence = [{
        "candidate_id": candidate_id, "evidence_order": 1, "event_id": "event-demo-001",
        "event_time": case["first_seen"], "record_id": "record-demo-001", "source_id": "sso-main",
        "source_type": "sso", "storage_ref": "kafka://security-log/2/9182", "checksum": "sha256:demo",
        "reason": "authentication failure burst",
    }]
    return {
        "case": case,
        "alert": {"alert_id": case["current_alert_id"], "assessment_id": "assessment-demo-1", "risk_score": case["risk_score"], "severity": case["severity"], "policy_reason": "threshold_and_budget", "first_seen": case["first_seen"], "last_seen": case["last_seen"]},
        "assessment": {"assessment_id": "assessment-demo-1", "base_score": 88, "risk_score": case["risk_score"], "policy_action": "alert", "risk_breakdown": {"base": 88, "cross_system_bonus": 6}},
        "incident": {"incident_id": case["incident_id"], "incident_type": "compound_behavior_anomaly", "aggregation_score": 91, "severity": "high", "detector_families": ["rule", "statistical"], "source_types": ["sso", "gitlab"], "source_ips": ["10.20.4.17"], "resources": ["repository:finance"], "candidate_count": 1},
        "candidates": [{
            "candidate_id": candidate_id, "detector_family": "rule", "detector_id": "sso-brute-force", "detector_version": "1",
            "detector_mode": "active", "score": 91, "severity": "high", "window_start": case["first_seen"], "window_end": case["last_seen"],
            "feature_revision": 42, "baseline_revision": 12, "explanation": {"condition": "failure_count >= 10", "observed": 17},
            "evidence_query": {"event_count": 17, "evidence_truncated": True}, "evidence": evidence,
        }],
        "evidence_status": {"risk": "available", "incident": "available", "detection": "available"},
    }


def _case_detail_page(
    investigation: dict[str, Any], token: str, actor: str, demo: bool, error: str | None,
) -> str:
    case = investigation["case"]
    review = "<div class='notice error'><strong>需要复核</strong><span>Risk Alert 已产生新版本，请重新检查当前证据链。</span></div>" if case.get("requires_review") else ""
    error_html = f"<div class='notice error'><strong>操作失败</strong><span>{_e(error)}</span></div>" if error else ""
    cards = "<section class='cards'>" + "".join((
        _card("风险分", case.get("risk_score", "—"), case.get("severity", "—")),
        _card("状态", case.get("status", "—"), "运营状态"),
        _card("负责人", case.get("owner") or "未分配", "本地审计身份"),
        _card("检测时间", case.get("last_seen", "—"), f"首次 {case.get('first_seen', '—')}"),
    )) + "</section>"
    status_rows = [[key, value] for key, value in investigation.get("evidence_status", {}).items()]
    alert = investigation.get("alert") or {}
    assessment = investigation.get("assessment") or {}
    incident = investigation.get("incident") or {}
    chain = _raw_table(["层级", "ID / 类型", "关键结果"], [
        ["Risk Alert", _e(alert.get("alert_id", "不可用")), _e(f"{alert.get('severity', '—')} / {alert.get('risk_score', '—')} / {alert.get('policy_reason', '—')}")],
        ["Assessment", _e(assessment.get("assessment_id", "不可用")), _e(assessment.get("policy_action", "—"))],
        ["Incident", _e(incident.get("incident_id", "不可用")), _e(incident.get("incident_type", "—"))],
    ], "检测链不可用")
    risk_breakdown = f"<h3>风险分解</h3><pre>{_e(json.dumps(assessment.get('risk_breakdown', {}), ensure_ascii=False, indent=2, sort_keys=True))}</pre>" if assessment else ""
    candidates_html = "".join(_candidate_panel(item) for item in investigation.get("candidates", [])) or "<p class='empty'>暂无可用 Candidate。</p>"
    evidence_rows = []
    for candidate in investigation.get("candidates", []):
        for item in candidate.get("evidence", []):
            evidence_rows.append([
                _e(item.get("event_time", "—")), _e(item.get("event_id", "—")), _e(item.get("source_type", "—")),
                _e(item.get("source_id", "—")), _e(item.get("record_id", "—")),
                f"<code class='copy-ref'>{_e(item.get('storage_ref', '—'))}</code>", _e(item.get("checksum", "—")), _e(item.get("reason", "—")),
            ])
    audit_rows = [[_e(item.get("created_at", "—")), _e(item.get("event_type", "—")), _e(item.get("actor", "—")), f"<pre class='inline-json'>{_e(json.dumps(item.get('details', {}), ensure_ascii=False, sort_keys=True))}</pre>"] for item in case.get("events", [])]
    forms = "<p class='muted'>Demo 模式只读，处置表单已禁用。</p>" if demo else _case_action_forms(case, token, actor)
    body = f"""<header class='page-head'><div><p class='eyebrow'>CASE INVESTIGATION</p><h1>{_e(case.get('user_name','Case'))}</h1><p>{_e(case.get('case_id'))}</p></div><a class='button secondary compact' href='/cases'>返回列表</a></header>
{review}{error_html}{cards}
<div class='investigation-layout'><div>
<section class='panel'><h2>当前检测链</h2>{chain}{risk_breakdown}<h3>状态</h3>{_table(['状态库','可用性'],status_rows,'暂无状态')}</section>
<section class='panel'><h2>Incident 上下文</h2>{_incident_summary(incident)}</section>
<section class='panel'><h2>Candidate 时间线</h2>{candidates_html}</section>
<section class='panel'><h2>Evidence 引用</h2><p class='muted'>仅展示不可变引用元数据，不读取或缓存原始日志。</p>{_raw_table(['时间','Event','类型','Source','Record','Storage Ref','Checksum','命中原因'],evidence_rows,'暂无 Evidence 引用')}</section>
<section class='panel'><h2>Case 审计时间线</h2>{_raw_table(['时间','事件','Actor','详情'],audit_rows,'暂无审计事件')}</section>
</div><div class='case-actions'><section class='panel'><h2>调查与处置</h2>{forms}</section></div></div>"""
    return _layout("Case 调查", "cases", body, demo)


def _candidate_panel(candidate: dict[str, Any]) -> str:
    query = candidate.get("evidence_query", {})
    truncated = " · Evidence 已截断" if query.get("evidence_truncated") else ""
    return f"""<article class='candidate-panel'><header><div><strong>{_e(candidate.get('detector_family','—'))} / {_e(candidate.get('detector_id','—'))}</strong><span>{_e(candidate.get('window_start','—'))} → {_e(candidate.get('window_end','—'))}</span></div><b>{_e(candidate.get('score','—'))}</b></header>
<p class='muted'>version {_e(candidate.get('detector_version','—'))} · mode {_e(candidate.get('detector_mode','—'))} · scope {_e(candidate.get('baseline_scope') or '—')} · group {_e(candidate.get('baseline_group_id') or '—')} · Feature rev {_e(candidate.get('feature_revision','—'))} · Baseline rev {_e(candidate.get('baseline_revision','—'))}{truncated}</p>
<details><summary>查看 explanation</summary><pre>{_e(json.dumps(candidate.get('explanation', {}), ensure_ascii=False, indent=2, sort_keys=True))}</pre></details></article>"""


def _incident_summary(incident: dict[str, Any]) -> str:
    if not incident:
        return "<p class='empty'>Incident 数据不可用。</p>"
    items = (("检测层", incident.get("detector_families", [])), ("来源类型", incident.get("source_types", [])), ("来源 IP", incident.get("source_ips", [])), ("资源", incident.get("resources", [])))
    return "<dl class='summary-list'>" + "".join(f"<div><dt>{_e(label)}</dt><dd>{_e(', '.join(map(str,value)) if isinstance(value,list) else value or '—')}</dd></div>" for label, value in items) + "</dl>"


def _case_action_forms(case: dict[str, Any], token: str, actor: str) -> str:
    base = f"<input type='hidden' name='csrf' value='{_e(token)}'><label>分析员<input name='actor' required maxlength='80' value='{_e(actor)}' placeholder='首次操作时填写'></label>"
    case_id = _e(case["case_id"])
    status_options = "".join(_option(item, item, case.get("status")) for item in CASE_STATUSES)
    return f"""<div class='case-form-stack'>
<form method='post' action='/cases/{case_id}/transition'>{base}<h3>状态</h3><label>目标状态<select name='status'>{status_options}</select></label><label>抑制期限<select name='suppression_duration'><option value=''>不设置</option><option value='1h'>1 小时</option><option value='24h'>24 小时</option><option value='7d'>7 天</option><option value='custom'>自定义</option></select></label><label>自定义期限<input name='suppression_custom' placeholder='2030-01-01T00:00:00+08:00'></label><label>备注<textarea name='note' rows='2'></textarea></label><button class='button' type='submit'>更新状态</button></form>
<form method='post' action='/cases/{case_id}/assign'>{base}<h3>负责人</h3><label>负责人<input name='owner' value='{_e(case.get('owner') or '')}' placeholder='留空以清除'></label><label>备注<input name='note'></label><button class='button' type='submit'>保存负责人</button></form>
<form method='post' action='/cases/{case_id}/tags'>{base}<h3>标签</h3><label>逗号分隔<input name='tags' value='{_e(', '.join(case.get('tags', [])))}'></label><button class='button' type='submit'>保存标签</button></form>
<form method='post' action='/cases/{case_id}/comment'>{base}<h3>评论</h3><label>内容<textarea name='note' required maxlength='4000' rows='5'></textarea></label><button class='button' type='submit'>添加评论</button></form>
</div>"""


def _suppression_until(status: str, duration: str, custom: str) -> str | None:
    if status not in CASE_STATUSES:
        raise ConsoleError(f"unsupported case status: {status}")
    if status != "suppressed":
        if duration or custom.strip():
            raise ConsoleError("仅 suppressed 状态可以设置抑制期限")
        return None
    if duration in {"1h", "24h", "7d"}:
        delta = {"1h": timedelta(hours=1), "24h": timedelta(hours=24), "7d": timedelta(days=7)}[duration]
        return (datetime.now(timezone.utc) + delta).isoformat()
    if duration != "custom" or not custom.strip():
        raise ConsoleError("suppressed 状态必须选择抑制期限")
    try:
        parsed = datetime.fromisoformat(custom.strip().replace("Z", "+00:00"))
    except ValueError as exc:
        raise ConsoleError("自定义抑制期限必须是 ISO-8601 时间") from exc
    if parsed.tzinfo is None:
        raise ConsoleError("自定义抑制期限必须包含时区")
    return custom.strip()


def _query_one(query: dict[str, list[str]], name: str) -> str:
    values = query.get(name)
    return values[0] if values else ""


def _layout(title: str, active: str, body: str, demo: bool) -> str:
    links = [("overview", "/", "总览"), ("sources", "/sources", "数据源"), ("rules", "/rules", "检测规则"), ("jobs", "/jobs", "任务"), ("readiness", "/readiness", "准备度"), ("experiments", "/experiments", "实验"), ("cases", "/cases", "Cases")]
    nav = "".join(f"<a href='{href}' class='{'active' if key == active else ''}'>{label}</a>" for key, href, label in links)
    banner = "<div class='demo-banner'>DEMO DATA — 不是真实检测结果</div>" if demo else ""
    return f"""<!doctype html><html lang='zh-CN'><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'><title>{_e(title)} · LogFusion</title><style>{_CSS}</style></head><body>{banner}<div class='shell'><aside><a class='brand' href='/'><span>LF</span><strong>LogFusion</strong></a><nav aria-label='主导航'>{nav}</nav><div class='aside-foot'>LOCAL CONTROL PLANE</div></aside><main>{body}</main></div></body></html>"""


def _card(label: str, value: Any, note: str) -> str:
    return f"<article class='card'><span>{_e(label)}</span><strong>{_e(value)}</strong><small>{_e(note)}</small></article>"


def _table(headers: list[str], rows: list[list[Any]], empty: str) -> str:
    if not rows:
        return f"<p class='empty'>{_e(empty)}</p>"
    head = "".join(f"<th scope='col'>{_e(value)}</th>" for value in headers)
    body = "".join("<tr>" + "".join(f"<td>{_e(value)}</td>" for value in row) + "</tr>" for row in rows)
    return f"<div class='table-wrap'><table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table></div>"


def _raw_table(headers: list[str], rows: list[list[Any]], empty: str) -> str:
    if not rows:
        return f"<p class='empty'>{_e(empty)}</p>"
    head = "".join(f"<th scope='col'>{_e(value)}</th>" for value in headers)
    body = "".join("<tr>" + "".join(f"<td>{value}</td>" for value in row) + "</tr>" for row in rows)
    return f"<div class='table-wrap'><table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table></div>"


def _job_table(jobs: list[dict[str, Any]]) -> str:
    if not jobs:
        return "<p class='empty'>尚无任务</p>"
    rows = []
    for job in jobs:
        rows.append([f"<a href='/jobs/{job['job_id']}'>{_e(job['kind'])}</a>", f"<span class='status {job['status']}'>{_e(job['status'])}</span>", _format_ms(job["created_at"]), job["exit_code"] if job["exit_code"] is not None else "—"])
    head = "".join(f"<th>{value}</th>" for value in ("任务", "状态", "创建时间", "退出码"))
    body = "".join("<tr>" + "".join(f"<td>{value}</td>" for value in row) + "</tr>" for row in rows)
    return f"<div class='table-wrap'><table><thead><tr>{head}</tr></thead><tbody>{body}</tbody></table></div>"


def _readiness_summary(report: dict[str, Any], detailed: bool = False) -> str:
    if not report:
        return "<p class='empty'>尚未生成准备度报告。点击“重新检查”后查看。</p>"
    history = report.get("history", {})
    items = [
        ("历史数据", history.get("observed_days", 0), history.get("target_days", 30)),
    ]
    result = "<div class='progress-list'>"
    for label, current, target in items:
        percent = min(100, round(current / target * 100)) if target else 0
        result += f"<div><header><span>{_e(label)}</span><strong>{current}/{target}</strong></header><div class='track'><span style='width:{percent}%'></span></div></div>"
    result += "</div>"
    if detailed:
        detectors = report.get("detectors", {})
        result += "<div class='detectors'>" + "".join(f"<article><strong>{_e(name)}</strong><span>{_e(item.get('training_status', 'unknown'))}</span><small>{'可运行无监督评估' if item.get('evaluation_ready') else '训练数据未就绪'}</small></article>" for name, item in detectors.items()) + "</div>"
    return result


def _post_button(action: str, label: str, token: str) -> str:
    return f"<form method='post' action='{action}'><input type='hidden' name='csrf' value='{_e(token)}'><button class='button' type='submit'>{_e(label)}</button></form>"


def _experiment_form(token: str) -> str:
    return f"""<section class='panel'><h2>新建实验</h2><form class='form-grid' method='post' action='/jobs/evaluate'><input type='hidden' name='csrf' value='{_e(token)}'><label>实验名称<input name='name' required maxlength='80' placeholder='hbos-july'></label><label>检测器<select name='detector'><option value='statistical'>Statistical</option><option value='hbos'>HBOS</option><option value='isolation_forest'>Isolation Forest</option></select></label><label>开始（UTC）<input type='date' name='from' required></label><label>结束（UTC，排他）<input type='date' name='to' required></label><div><button class='button' type='submit'>提交实验</button></div></form></section>"""


def _source_form(token: str, values: dict[str, str]) -> str:
    return f"""<form method='post' action='/sources/preview' enctype='multipart/form-data'>
<input type='hidden' name='csrf' value='{_e(token)}'>
<section class='panel wizard'><div class='step-title'><span>1</span><div><h2>连接与格式</h2><p>选择接入方式。暂时不在页面中收集任何密码。</p></div></div>
<div class='source-grid'>
<label>接入方式<select name='kind'>{_option('local', '本地文件 / Glob', values.get('kind'))}{_option('kafka', 'Kafka', values.get('kind'))}</select></label>
<label>来源 ID<input name='source_id' required maxlength='64' value='{_e(values.get('source_id', ''))}' placeholder='sso-main'></label>
<label>日志类型<select name='source_type'>{''.join(_option(item, item, values.get('source_type', 'auto')) for item in ('auto','svn','sso','gitlab','hiklink'))}</select></label>
<label>产品（可选）<input name='product' maxlength='255' value='{_e(values.get('product', ''))}' placeholder='security-audit'></label>
<label>格式版本（可选）<input name='format_version' maxlength='255' value='{_e(values.get('format_version', ''))}' placeholder='v1'></label>
</div></section>
<section class='grid two'><div class='panel wizard'><div class='step-title'><span>2A</span><div><h2>本地文件</h2><p>支持普通文本及 gzip、bz2、xz、zip。</p></div></div>
<div class='stack'><label>路径或 Glob<input name='path' value='{_e(values.get('path', ''))}' placeholder='data/sso/*.jsonl'></label><label>记录模式<select name='record_mode'>{_option('line','每个非空行一条记录',values.get('record_mode','line'))}{_option('object','跨行对象',values.get('record_mode','line'))}</select></label></div></div>
<div class='panel wizard'><div class='step-title'><span>2B</span><div><h2>Kafka</h2><p>多个 Topic 使用英文逗号分隔。</p></div></div>
<div class='stack'><label>Bootstrap servers<input name='bootstrap_servers' value='{_e(values.get('bootstrap_servers', ''))}' placeholder='kafka-1:9092,kafka-2:9092'></label><label>Topics<input name='topics' value='{_e(values.get('topics', ''))}' placeholder='raw.security.audit'></label><label>Consumer group<input name='group_id' value='{_e(values.get('group_id', ''))}' placeholder='logfusion-security'></label></div></div></section>
<section class='panel wizard'><div class='step-title'><span>3</span><div><h2>样例预览</h2><p>粘贴内容优先于上传文件；最多 512 KB。请先脱敏。</p></div></div>
<div class='sample-grid'><label>粘贴一条原始日志<textarea name='sample' rows='9' placeholder='在这里粘贴一条有代表性的原始日志'>{_e(values.get('sample', ''))}</textarea></label><label class='upload'>或上传 UTF-8 样例文件<input type='file' name='sample_file' accept='.txt,.log,.json,.jsonl,text/plain,application/json'></label></div>
<label class='check'><input type='checkbox' name='include_raw_text' value='true' {'checked' if values.get('include_raw_text') == 'true' else ''}> 在正式输出事件中保留原文（默认关闭，可能包含敏感信息）</label>
<button class='button' type='submit'>检测并预览映射</button></section></form>"""


def _source_preview(token: str, values: dict[str, str], result: dict[str, Any]) -> str:
    coverage = f"{result['field_coverage']:.0%}"
    confidence = f"{result['confidence']:.0%}"
    fields = "".join(f"<li class='{'present' if item['present'] else 'missing'}'><span>{_e(item['path'])}</span><strong>{'已映射' if item['present'] else '缺失'}</strong></li>" for item in result["fields"])
    canonical = result["normalized"] if result["normalized"] is not None else result["unknown"]
    status = "解析成功" if result["ok"] else "未能解析"
    body = f"<section class='panel preview'><div class='step-title'><span>4</span><div><h2>Raw → Canonical 预览</h2><p>这是当前项目解析器和 Schema 校验的真实结果。</p></div></div><div class='preview-stats'>{_card('结果', status, result['parser_id'])}{_card('自动识别', result['detected_source_type'], '实际检测')}{_card('Parser confidence', confidence, '解析器声明值')}{_card('关键字段覆盖', coverage, '6 个核心字段')}</div><div class='grid two'><div><h3>Raw</h3><pre>{_e(result['sample'])}</pre></div><div><h3>{'Canonical Event' if result['ok'] else 'Unknown Record'}</h3><pre>{_e(json.dumps(canonical, ensure_ascii=False, indent=2))}</pre></div></div><h3>关键字段</h3><ul class='field-checks'>{fields}</ul>"
    if result["ok"]:
        hidden = "".join(f"<input type='hidden' name='{_e(key)}' value='{_e(value)}'>" for key, value in values.items() if key != "sample")
        body += f"<form method='post' action='/sources/save'><input type='hidden' name='csrf' value='{_e(token)}'>{hidden}<button class='button' type='submit'>确认并保存配置</button></form>"
    else:
        reason = (result.get("unknown") or {}).get("reason", "unknown")
        body += f"<div class='notice error'><strong>不能保存</strong><span>{_e(reason)}。请调整日志类型、记录模式或样例后重新预览。</span></div>"
    return body + "</section>"


def _option(value: str, label: str, selected: str | None) -> str:
    return f"<option value='{_e(value)}' {'selected' if value == selected else ''}>{_e(label)}</option>"


def _rule_form(token: str, values: dict[str, Any]) -> str:
    defaults = {
        "version": "1", "status": "draft", "scope": "event", "window_size": "300",
        "score": "80", "match": "all",
    }
    data = {**defaults, **{key: str(value) for key, value in values.items()}}
    field_options = sorted((field, field) for field in (
        "event.category", "event.type", "event.action", "event.outcome", "user.name", "source.ip", "destination.ip",
        "host.name", "resource.type", "resource.name", "http.request.method", "http.response.status_code", "parser.id", "raw.source_type",
        "event_count", "success_count", "failure_count", "other_outcome_count", "source_type_distinct_count",
        "source_ip_distinct_count", "resource_distinct_count", "action_distinct_count", "read_action_count",
        "write_action_count", "network_bytes_total", "first_seen_ip_count", "first_seen_resource_count", "first_seen_action_count",
    ))
    operators = ("eq", "neq", "in", "not_in", "gt", "gte", "lt", "lte", "exists", "contains", "starts_with", "ends_with", "cidr_contains")
    conditions = ""
    for index in range(1, 6):
        field = data.get(f"condition_field_{index}", "")
        conditions += f"<div class='condition-row'><label>字段<select name='condition_field_{index}'><option value=''>—</option>{''.join(_option(value,label,field) for value,label in field_options)}</select></label><label>运算符<select name='condition_operator_{index}'>{''.join(_option(value,value,data.get(f'condition_operator_{index}','eq')) for value in operators)}</select></label><label>值<input name='condition_value_{index}' value='{_e(data.get(f'condition_value_{index}', ''))}' placeholder='in/not_in 使用英文逗号'></label></div>"
    return f"""<form method='post' class='rule-form'><input type='hidden' name='csrf' value='{_e(token)}'><input type='hidden' name='status' value='{_e(data['status'])}'>
<section class='panel wizard'><div class='step-title'><span>1</span><div><h2>规则信息</h2><p>保存新版本时必须使用当前最大版本 + 1。</p></div></div><div class='source-grid'>
<label>Rule ID<input name='rule_id' required maxlength='80' value='{_e(data.get('rule_id',''))}'></label><label>版本<input name='version' type='number' min='1' required value='{_e(data['version'])}'></label><label>名称<input name='name' required maxlength='120' value='{_e(data.get('name',''))}'></label><label>范围<select name='scope'>{_option('event','Canonical 单事件',data['scope'])}{_option('window','聚合窗口',data['scope'])}</select></label><label>窗口<select name='window_size'>{''.join(_option(str(value),str(value),data['window_size']) for value in (60,300,3600,86400))}</select></label><label>分数<input name='score' type='number' min='0' max='100' required value='{_e(data['score'])}'></label><label>条件关系<select name='match'>{_option('all','全部满足',data['match'])}{_option('any','任一满足',data['match'])}</select></label><label>日志类型<input name='source_types' value='{_e(data.get('source_types',''))}' placeholder='sso,gitlab'></label><label>Tags<input name='tags' value='{_e(data.get('tags',''))}' placeholder='authentication,credential'></label></div><label>说明<textarea name='description' rows='3'>{_e(data.get('description',''))}</textarea></label></section>
<section class='panel wizard'><div class='step-title'><span>2</span><div><h2>匹配条件</h2><p>最多五项；字段必须与规则范围一致。</p></div></div><div class='conditions'>{conditions}</div></section>
<section class='panel wizard'><div class='step-title'><span>3</span><div><h2>正反例测试</h2><p>填写两个 JSON 对象后才能激活；event 可使用点路径键，window 使用指标键。</p></div></div><div class='grid two'><label>正例 JSON<textarea name='positive_json' rows='8' placeholder='{{"event.outcome":"failure","raw.source_type":"sso"}}'>{_e(data.get('positive_json',''))}</textarea></label><label>反例 JSON<textarea name='negative_json' rows='8' placeholder='{{"event.outcome":"success","raw.source_type":"sso"}}'>{_e(data.get('negative_json',''))}</textarea></label></div><div class='button-row'><button class='button secondary' type='submit' formaction='/rules/test'>运行测试</button><button class='button' type='submit' formaction='/rules/save'>保存 Draft</button></div></section></form>"""


def _rule_import_form(token: str) -> str:
    return f"""<section class='panel'><h2>导入单条规则 JSON</h2><form method='post' action='/rules/import'><input type='hidden' name='csrf' value='{_e(token)}'><label class='stack'>规则对象<textarea name='rule_json' rows='10' required placeholder='{{"rule_id":"...","version":1,...}}'></textarea></label><button class='button' type='submit'>验证并导入</button></form></section>"""


def _rule_from_form(form: dict[str, list[str]]) -> dict[str, Any]:
    version, score = _one(form, "version"), _one(form, "score")
    if not version.isdigit() or not score.isdigit():
        raise ConsoleError("rule version and score must be integers")
    scope = _one(form, "scope")
    conditions = []
    for index in range(1, 6):
        field = _one(form, f"condition_field_{index}")
        if not field:
            continue
        operator = _one(form, f"condition_operator_{index}")
        raw = _one(form, f"condition_value_{index}")
        value: Any = raw
        if operator in {"gt", "gte", "lt", "lte"} or (field in NUMERIC_FIELDS and operator in {"eq", "neq"}):
            try:
                number = float(raw)
                value = int(number) if number.is_integer() else number
            except ValueError as exc:
                raise ConsoleError(f"condition {index} requires a number") from exc
        elif operator in {"in", "not_in"}:
            values = [item.strip() for item in raw.split(",") if item.strip()]
            if field in NUMERIC_FIELDS:
                try:
                    numbers = [float(item) for item in values]
                    value = [int(item) if item.is_integer() else item for item in numbers]
                except ValueError as exc:
                    raise ConsoleError(f"condition {index} requires numeric list values") from exc
            else:
                value = values
        elif operator == "exists":
            value = raw.lower() != "false"
        conditions.append({"field": field, "operator": operator, "value": value})
    tests = []
    for name, expected in (("positive", True), ("negative", False)):
        raw = _one(form, f"{name}_json").strip()
        if raw:
            try:
                value = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise ConsoleError(f"invalid {name} test JSON: {exc}") from exc
            if not isinstance(value, dict):
                raise ConsoleError(f"{name} test must be a JSON object")
            tests.append({"name": name, "input": value, "expected_match": expected})
    result: dict[str, Any] = {
        "rule_id": _one(form, "rule_id").strip(), "version": int(version), "name": _one(form, "name").strip(),
        "description": _one(form, "description"), "status": _one(form, "status") or "draft", "scope": scope,
        "source_types": [item.strip() for item in _one(form, "source_types").split(",") if item.strip()],
        "score": int(score), "match": _one(form, "match"), "conditions": conditions,
        "tags": [item.strip() for item in _one(form, "tags").split(",") if item.strip()], "tests": tests,
    }
    if scope == "window":
        window_size = _one(form, "window_size")
        result["window_size"] = int(window_size) if window_size.isdigit() else 0
    return result


def _rule_to_form(rule: dict[str, Any]) -> dict[str, str]:
    values = {
        "rule_id": rule["rule_id"], "version": str(rule["version"]), "name": rule["name"],
        "description": rule.get("description", ""), "status": rule["status"], "scope": rule["scope"],
        "window_size": str(rule.get("window_size", 300)), "score": str(rule["score"]), "match": rule["match"],
        "source_types": ",".join(rule.get("source_types", [])), "tags": ",".join(rule.get("tags", [])),
    }
    for index, condition in enumerate(rule["conditions"][:5], 1):
        values[f"condition_field_{index}"] = condition["field"]
        values[f"condition_operator_{index}"] = condition["operator"]
        value = condition.get("value")
        values[f"condition_value_{index}"] = ",".join(map(str, value)) if isinstance(value, list) else str(value).lower() if isinstance(value, bool) else str(value or "")
    for test in rule.get("tests", []):
        name = "positive" if test["expected_match"] else "negative"
        values[f"{name}_json"] = json.dumps(test["input"], ensure_ascii=False, indent=2)
    return values


def _rule_next_statuses(status: str) -> tuple[str, ...]:
    return {
        "draft": ("shadow", "disabled", "deprecated"), "shadow": ("active", "disabled", "deprecated"),
        "active": ("disabled", "deprecated"), "disabled": ("shadow", "active", "deprecated"), "deprecated": (),
    }[status]


def _rule_status_button(token: str, rule: dict[str, Any], status: str) -> str:
    return f"<form class='inline-form' method='post' action='/rules/status'><input type='hidden' name='csrf' value='{_e(token)}'><input type='hidden' name='rule_id' value='{_e(rule['rule_id'])}'><input type='hidden' name='version' value='{rule['version']}'><input type='hidden' name='status' value='{_e(status)}'><button class='link-button' type='submit'>{_e(status)}</button></form>"


def _source_values(form: dict[str, list[str]]) -> dict[str, str]:
    names = (
        "kind", "source_id", "source_type", "product", "format_version",
        "record_mode", "path", "bootstrap_servers", "topics", "group_id",
        "include_raw_text", "sample",
    )
    return {name: _one(form, name) for name in names}


def _optional_project_file(project: ProjectConfig, key: str, default: str) -> Path:
    value = project.document.get("files", {}).get(key, default)
    path = Path(str(value)).expanduser()
    return path if path.is_absolute() else project.root / path


def _metric(item: dict[str, Any], name: str) -> str:
    value = item.get("metrics", {}).get(name)
    if value is None:
        return "—"
    return f"{value:.3f}" if name != "average_daily_predictions" else f"{value:.1f}"


def _job(row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
    item = dict(row)
    item["arguments"] = json.loads(item.pop("arguments_json"))
    return item


def _one(form: dict[str, list[str]], name: str) -> str:
    values = form.get(name)
    return values[0] if values else ""


def _validate_utc_day(value: str) -> None:
    if len(value) != 10:
        raise ConsoleError("evaluation dates must be YYYY-MM-DD")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ConsoleError("evaluation dates must be YYYY-MM-DD") from exc
    if parsed.strftime("%Y-%m-%d") != value or parsed.time() != datetime.min.time():
        raise ConsoleError("evaluation dates must align to UTC days")


def _headers(content_type: str, length: int) -> list[tuple[str, str]]:
    return [
        ("Content-Type", content_type),
        ("Content-Length", str(length)),
        ("Content-Security-Policy", "default-src 'self'; style-src 'unsafe-inline'; form-action 'self'; frame-ancestors 'none'"),
        ("X-Content-Type-Options", "nosniff"),
        ("X-Frame-Options", "DENY"),
        ("Referrer-Policy", "no-referrer"),
    ]


def _e(value: Any) -> str:
    return html.escape(str(value), quote=True)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _format_ms(value: int | None) -> str:
    return _iso_ms(value) if value else "—"


def _iso_ms(value: int) -> str:
    return datetime.fromtimestamp(value / 1000, timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


_CSS = """
:root{
  --bg:#f1f8ff;
  --ink:#193754;
  --muted:#607d98;
  --line:#d6e8f7;
  --panel:#ffffff;
  --accent:#1976d2;
  --accent-soft:#e5f2ff;
  --nav:#e8f4ff;
  --cyan:#67c7f2;
}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--ink);font-family:Inter,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}
.shell{display:grid;grid-template-columns:220px 1fr;min-height:100vh}
aside{position:sticky;top:0;height:100vh;background:linear-gradient(180deg,#e8f4ff 0%,#dceeff 100%);color:var(--ink);padding:28px 20px;display:flex;flex-direction:column;border-right:1px solid #c9e0f3}
.brand{display:flex;gap:11px;align-items:center;color:#193754;text-decoration:none;font-size:18px}
.brand span{display:grid;place-items:center;width:34px;height:34px;background:linear-gradient(135deg,#58b7ee,#1976d2);color:#fff;font-weight:900;border-radius:9px;box-shadow:0 8px 22px #1976d233}
nav{margin-top:38px;display:grid;gap:7px}
nav a{color:#557591;text-decoration:none;padding:11px 13px;border-radius:8px;font-size:14px;border:1px solid transparent}
nav a:hover{background:#ffffff99;color:#195a95}
nav a.active{background:#fff;border-color:#bfdcf3;color:#1769aa;box-shadow:0 4px 12px #4d91c418}
.aside-foot{margin-top:auto;color:#7c9ab4;font-size:10px;letter-spacing:.16em}
main{padding:48px;max-width:1500px;width:100%;margin:0 auto}
.hero,.page-head{display:flex;align-items:flex-end;justify-content:space-between;margin-bottom:28px}
.hero{position:relative;overflow:hidden;padding:34px;background:radial-gradient(circle at 90% 0%,#8ed6ff99 0%,transparent 36%),linear-gradient(135deg,#dff1ff,#b9dcfa);color:#183b5b;border:1px solid #b4d8f3;border-radius:18px;box-shadow:0 18px 45px #4c8fbd18}
.hero h1,.page-head h1{font-size:34px;margin:4px 0 8px;letter-spacing:-.035em}
.hero p,.page-head p{margin:0;max-width:720px}
.hero p{color:#52738f}
.eyebrow{font-size:11px!important;letter-spacing:.18em;font-weight:800;color:#1976d2}
.cards{display:grid;grid-template-columns:repeat(4,1fr);gap:15px;margin-bottom:20px}
.card,.panel,.notice{background:var(--panel);border:1px solid var(--line);border-radius:14px;box-shadow:0 3px 14px #4b91bd0b}
.card{padding:21px;border-top:3px solid #8fc9ef}
.card span,.card small{display:block;color:var(--muted)}
.card strong{display:block;font-size:30px;margin:9px 0 5px;color:#193754}
.grid{display:grid;gap:20px}
.grid.two{grid-template-columns:1fr 1fr}
.panel{padding:24px;margin-bottom:20px}
.panel h2{font-size:17px;margin:0 0 18px}
.button{display:inline-flex;border:0;border-radius:8px;background:var(--accent);color:#fff;padding:10px 15px;font:inherit;font-weight:700;text-decoration:none;cursor:pointer;box-shadow:0 5px 14px #1976d22b}
.button:hover{background:#1267b8}
.button.secondary{background:var(--accent-soft);color:#1769aa;margin-top:17px;box-shadow:none}
.notice{padding:17px 20px;display:flex;gap:16px;background:#f5fbff;border-color:#cbe5f8}
.notice strong{color:#1769aa}
.notice span,.muted,.empty{color:var(--muted)}
.table-wrap{overflow:auto}
table{width:100%;border-collapse:collapse;font-size:14px}
th{text-align:left;color:#718ba2;font-size:11px;letter-spacing:.08em;text-transform:uppercase;background:#f5faff}
th,td{padding:13px 11px;border-bottom:1px solid #e7f0f8}
tbody tr:hover{background:#f2f9ff}
td a{color:var(--accent);font-weight:700;text-decoration:none}
.status{display:inline-block;padding:4px 8px;border-radius:999px;background:#eef1f5;color:#596579;font-size:11px}
.status.succeeded{background:#dcfce7;color:#166534}
.status.failed{background:#fee2e2;color:#991b1b}
.status.running{background:#fef3c7;color:#92400e}
.status.pending{background:#dbeafe;color:#1e40af}
.status.active{background:#dcfce7;color:#166534}.status.shadow{background:#e0f2fe;color:#075985}.status.draft{background:#fef3c7;color:#92400e}.status.disabled,.status.deprecated{background:#eef1f5;color:#596579}
.status.new{background:#dbeafe;color:#1e40af}.status.acknowledged{background:#e0f2fe;color:#075985}.status.investigating{background:#fef3c7;color:#92400e}.status.resolved{background:#dcfce7;color:#166534}.status.false_positive{background:#f1f5f9;color:#475569}.status.suppressed{background:#ede9fe;color:#6d28d9}
.review-flag{display:inline-block;padding:4px 8px;border-radius:99px;background:#fff1f2;color:#be123c;font-size:11px;font-weight:800}
.progress-list{display:grid;gap:17px;margin:5px 0 20px}
.progress-list header{display:flex;justify-content:space-between;font-size:13px}
.track{height:8px;background:#e0edf8;border-radius:99px;margin-top:7px;overflow:hidden}
.track span{display:block;height:100%;background:linear-gradient(90deg,#2563eb,var(--cyan));border-radius:99px}
.detectors{display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-top:25px}
.detectors article{padding:14px;background:#f3f9ff;border:1px solid #dcecf8;border-radius:9px}
.detectors span,.detectors small{display:block;color:var(--muted);margin-top:5px}
.actions{padding-left:20px;color:var(--muted)}
.actions li{margin:10px 0}
.form-grid{display:grid;grid-template-columns:2fr 1fr 1fr 1fr auto;gap:12px;align-items:end}
.form-grid label{display:grid;gap:6px;color:var(--muted);font-size:12px}
.form-grid input,.form-grid select{width:100%;border:1px solid var(--line);border-radius:8px;padding:10px;background:#fff;color:var(--ink)}
.form-grid input:focus,.form-grid select:focus{outline:3px solid #bfdbfe;border-color:#60a5fa}
.case-filters{display:grid;grid-template-columns:2fr repeat(4,1fr) auto auto;gap:12px;align-items:end}.case-filters label,.case-form-stack label{display:grid;gap:6px;color:var(--muted);font-size:12px}.case-filters input,.case-filters select,.case-form-stack input,.case-form-stack select,.case-form-stack textarea{width:100%;border:1px solid var(--line);border-radius:8px;padding:10px;background:#fff;color:var(--ink);font:inherit}.button.compact{margin-top:0;padding:10px 13px}.pagination{display:flex;justify-content:flex-end;align-items:center;gap:14px;margin-top:18px}.pagination a{color:var(--accent);font-weight:700;text-decoration:none}.pagination span{color:var(--muted);font-size:13px}
.investigation-layout{display:grid;grid-template-columns:minmax(0,1fr) 360px;gap:20px;align-items:start}.case-actions{position:sticky;top:24px}.case-form-stack{display:grid;gap:22px}.case-form-stack form{display:grid;gap:10px;padding-bottom:20px;border-bottom:1px solid var(--line)}.case-form-stack form:last-child{border-bottom:0}.case-form-stack h3{margin:4px 0 0;font-size:14px}.candidate-panel{border:1px solid var(--line);border-radius:11px;padding:17px;margin:12px 0;background:#fafdff}.candidate-panel header{display:flex;justify-content:space-between;gap:15px}.candidate-panel header span{display:block;color:var(--muted);font-size:12px;margin-top:5px}.candidate-panel header b{font-size:27px;color:var(--accent)}.candidate-panel summary{cursor:pointer;color:var(--accent);font-weight:700}.summary-list{display:grid;grid-template-columns:1fr 1fr;gap:12px}.summary-list div{padding:13px;background:#f5faff;border-radius:9px}.summary-list dt{font-size:11px;color:var(--muted);margin-bottom:5px}.summary-list dd{margin:0;word-break:break-word}.copy-ref{display:block;max-width:300px;word-break:break-all;color:#175d95}.inline-json{margin:0;max-height:130px;min-width:220px;font-size:11px;padding:9px}
.wizard label,.stack label,.source-grid label,.sample-grid label{display:grid;gap:7px;color:var(--muted);font-size:12px}
.wizard input,.wizard select,.wizard textarea{width:100%;border:1px solid var(--line);border-radius:8px;padding:11px;background:#fff;color:var(--ink);font:inherit}
.wizard input:focus,.wizard select:focus,.wizard textarea:focus{outline:3px solid #bfdbfe;border-color:#60a5fa}
.rule-form label,.stack{display:grid;gap:7px;color:var(--muted);font-size:12px}.rule-form textarea,.stack textarea{width:100%;border:1px solid var(--line);border-radius:8px;padding:11px;background:#fff;color:var(--ink);font:inherit}.conditions{display:grid;gap:12px}.condition-row{display:grid;grid-template-columns:2fr 1fr 2fr;gap:12px}.button-row{display:flex;gap:10px}.button-row .button.secondary{margin-top:0}.inline-form{display:inline}.link-button{border:0;background:transparent;color:var(--accent);font:inherit;font-weight:700;cursor:pointer;padding:3px}
.step-title{display:flex;align-items:flex-start;gap:12px;margin-bottom:18px}
.step-title>span{display:grid;place-items:center;min-width:31px;height:31px;border-radius:9px;background:var(--accent-soft);color:var(--accent);font-weight:800;font-size:12px}
.step-title h2{margin:2px 0 4px}.step-title p{margin:0;color:var(--muted);font-size:13px}
.source-grid{display:grid;grid-template-columns:repeat(5,1fr);gap:13px}.stack{display:grid;gap:13px}
.sample-grid{display:grid;grid-template-columns:3fr 1fr;gap:15px;margin-bottom:15px}.upload{align-content:center;padding:20px;border:1px dashed #9bc8e9;border-radius:10px;background:#f6fbff}
.check{display:flex!important;align-items:center!important;grid-template-columns:auto 1fr!important;margin:0 0 16px}.check input{width:auto}
.preview-stats{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:20px}.preview-stats .card{box-shadow:none}
.preview h3{font-size:13px;color:var(--muted);margin:18px 0 9px}.field-checks{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;list-style:none;padding:0;margin:0 0 20px}.field-checks li{display:flex;justify-content:space-between;padding:10px 12px;border-radius:8px;background:#f5faff;font-size:12px}.field-checks .present strong{color:#15803d}.field-checks .missing strong{color:#b45309}
.notice.error{margin-top:16px;background:#fff7ed;border-color:#fed7aa}.notice.error strong{color:#c2410c}.success-panel{max-width:760px}
pre{white-space:pre-wrap;word-break:break-word;background:#153754;color:#dff3ff;padding:16px;border:1px solid #285879;border-radius:9px;max-height:520px;overflow:auto}
.demo-banner{position:fixed;z-index:3;top:0;left:220px;right:0;background:#dceeff;color:#185d96;border-bottom:1px solid #b9d9f2;text-align:center;font-size:11px;font-weight:800;letter-spacing:.08em;padding:6px}
.demo-banner+.shell main{padding-top:70px}
@media(max-width:1100px){.source-grid{grid-template-columns:1fr 1fr}.preview-stats{grid-template-columns:1fr 1fr}.case-filters{grid-template-columns:1fr 1fr 1fr}.investigation-layout{grid-template-columns:1fr}.case-actions{position:static}}
@media(max-width:950px){.shell{grid-template-columns:1fr}aside{position:static;height:auto}nav{display:flex;overflow:auto;margin-top:20px}.aside-foot{display:none}main{padding:28px}.cards{grid-template-columns:1fr 1fr}.grid.two,.sample-grid,.condition-row{grid-template-columns:1fr}.form-grid{grid-template-columns:1fr 1fr}.demo-banner{left:0}}
@media(max-width:560px){.cards,.form-grid,.detectors,.source-grid,.preview-stats,.field-checks{grid-template-columns:1fr}.hero,.page-head{align-items:flex-start;gap:20px;flex-direction:column}main{padding:18px}}
"""
