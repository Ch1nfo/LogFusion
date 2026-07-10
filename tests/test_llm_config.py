import json

from logfusion.cli import main
from logfusion.config import load_llm_config


class FakeProvider:
    def __init__(self, response):
        self.response = response

    def generate_parser(self, request):
        return self.response


def _proposal():
    return {
        "parser_type": "key_value",
        "field_extractors": {},
        "field_candidates": {"user": "alice", "action": "process_start"},
        "schema_mapping": {"user": "user.name", "action": "event.action"},
        "confidence": 0.9,
    }


def _unknown():
    return {
        "unknown_id": "u1",
        "detected_format": "key_value",
        "suggested_source_type": "edr",
        "template_hint": "user=<VAR> action=<VAR>",
        "raw": {
            "record_id": "r1",
            "source_id": "edr_acme",
            "source_type": "edr",
            "llm_enabled": True,
            "text": "user=alice action=process_start",
        },
    }


def test_load_llm_config_reads_local_provider_settings(tmp_path):
    config_path = tmp_path / "llm.local.yaml"
    config_path.write_text(
        "llm:\n"
        "  enabled: true\n"
        "  provider: openai_compatible\n"
        "  base_url: https://api.deepseek.com\n"
        "  model: deepseek-v4-flash\n"
        "  api_key_env: DEEPSEEK_API_KEY\n"
        "  timeout_seconds: 45\n"
        "  sample_limit: 3\n",
        encoding="utf-8",
    )

    settings = load_llm_config(config_path)

    assert settings == {
        "enabled": True,
        "provider": "openai_compatible",
        "base_url": "https://api.deepseek.com",
        "model": "deepseek-v4-flash",
        "api_key_env": "DEEPSEEK_API_KEY",
        "timeout_seconds": 45,
        "sample_limit": 3,
    }


def test_cli_uses_llm_config_without_repeating_provider_flags(tmp_path, monkeypatch):
    unknown_path = tmp_path / "unknown.jsonl"
    output_path = tmp_path / "candidates.jsonl"
    config_path = tmp_path / "llm.local.yaml"
    unknown_path.write_text(json.dumps(_unknown()) + "\n", encoding="utf-8")
    config_path.write_text(
        "llm:\n"
        "  enabled: true\n"
        "  provider: openai_compatible\n"
        "  base_url: https://api.deepseek.com\n"
        "  model: deepseek-v4-flash\n"
        "  api_key_env: DEEPSEEK_API_KEY\n"
        "  timeout_seconds: 45\n"
        "  sample_limit: 3\n",
        encoding="utf-8",
    )
    captured = {}

    def provider_factory(base_url, model, api_key_env, timeout_seconds):
        captured.update({
            "base_url": base_url,
            "model": model,
            "api_key_env": api_key_env,
            "timeout_seconds": timeout_seconds,
        })
        return FakeProvider(_proposal())

    monkeypatch.setattr("logfusion.cli.OpenAICompatibleProvider", provider_factory)

    assert main([
        "propose-parsers",
        "--unknown-input", str(unknown_path),
        "--output", str(output_path),
        "--llm-config", str(config_path),
    ]) == 0
    assert captured == {
        "base_url": "https://api.deepseek.com",
        "model": "deepseek-v4-flash",
        "api_key_env": "DEEPSEEK_API_KEY",
        "timeout_seconds": 45,
    }
    assert len(output_path.read_text(encoding="utf-8").splitlines()) == 1
