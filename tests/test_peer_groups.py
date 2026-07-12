from __future__ import annotations

import pytest

from logfusion.baseline import BaselineEngine, BaselineError
from logfusion.features import FeatureEngine
from tests.test_features import _event


def test_peer_group_baseline_builds_observed_windows_and_exposes_group(tmp_path):
    feature_db = tmp_path / "features.db"
    mapping = tmp_path / "groups.csv"
    mapping.write_text("user_name,group_id\n" + "\n".join(f"user-{index},engineering" for index in range(5)) + "\n", encoding="utf-8")
    with FeatureEngine(feature_db, mode="replay") as features:
        for user_index in range(5):
            for day in range(20):
                features.process_event(_event(f"{user_index}-{day}", f"2026-01-{day + 1:02d}T12:00:00Z", user=f"user-{user_index}"))
        features.flush_sessions()
    with BaselineEngine(feature_db, tmp_path / "baseline.db", peer_groups_path=mapping) as baseline:
        report = baseline.update("2026-01-20T12:00:00Z")
        result = baseline.query_user("user-0", 3600)
    assert report["recomputed_peer_groups"] == 1
    assert result["group_id"] == "engineering"
    assert any(row["status"] == "ready" for row in result["peer_group_metrics"])


def test_peer_group_mapping_change_requires_rebuild(tmp_path):
    feature_db = tmp_path / "features.db"
    mapping = tmp_path / "groups.csv"
    mapping.write_text("user_name,group_id\nwangkun78,engineering\n", encoding="utf-8")
    with FeatureEngine(feature_db, mode="replay") as features:
        features.process_event(_event("one", "2026-01-01T00:00:00Z"))
        features.flush_sessions()
    with BaselineEngine(feature_db, tmp_path / "baseline.db", peer_groups_path=mapping):
        pass
    mapping.write_text("user_name,group_id\nwangkun78,operations\n", encoding="utf-8")
    with pytest.raises(BaselineError, match="peer group mapping changed"):
        BaselineEngine(feature_db, tmp_path / "baseline.db", peer_groups_path=mapping)
