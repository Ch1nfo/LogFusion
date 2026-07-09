from __future__ import annotations

import json
import re

from logfusion.models import RawRecord


SVN_RE = re.compile(r'^\d+\.\d+\.\d+\.\d+ - \S+ \[[^\]]+\] "\S+ ')


def detect_source_type(record: RawRecord) -> str:
    text = record.raw_text.strip()
    if SVN_RE.match(text):
        return "svn"
    if text.startswith("{"):
        try:
            obj = json.loads(text)
        except json.JSONDecodeError:
            if "uid:" in text and "attribute2:" in text:
                return "hiklink"
            return "unknown"
        if "shortName" in obj and "eventType" in obj:
            return "sso"
        if "username" in obj and "route" in obj:
            return "gitlab"
    return "unknown"

