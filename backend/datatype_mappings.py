"""Central datatype mapping definitions and override helpers."""

from __future__ import annotations

import re
from typing import Dict, List, Any

DEFAULT_DATATYPE_MAPPINGS: List[Dict[str, Any]] = [
    {
        "source": "NVARCHAR2",
        "target": "VARCHAR",
        "description": "Oracle NVARCHAR2 should become Databricks VARCHAR (length preserved unless overridden).",
    },
    {
        "source": "VARCHAR2",
        "target": "VARCHAR",
        "description": "Oracle VARCHAR2 becomes native VARCHAR in Databricks.",
    },
    {
        "source": "NCHAR",
        "target": "CHAR",
        "description": "Oracle NCHAR -> Databricks CHAR to keep fixed-width semantics.",
    },
    {
        "source": "CLOB",
        "target": "STRING",
        "description": "Large character objects map to STRING in Databricks.",
    },
    {
        "source": "NCLOB",
        "target": "STRING",
        "description": "NCLOB -> STRING for Unicode large text data.",
    },
    {
        "source": "TEXT",
        "target": "STRING",
        "description": "Free-form TEXT columns become STRING.",
    },
    {
        "source": "BLOB",
        "target": "BINARY",
        "description": "Binary large objects should be stored as BINARY.",
    },
    {
        "source": "RAW",
        "target": "BINARY",
        "description": "RAW -> BINARY for fixed-length byte data.",
    },
    {
        "source": "BINARY_FLOAT",
        "target": "FLOAT",
        "description": "Databricks FLOAT is the closest match for Oracle BINARY_FLOAT.",
    },
    {
        "source": "BINARY_DOUBLE",
        "target": "DOUBLE",
        "description": "BINARY_DOUBLE should become DOUBLE.",
    },
    {
        "source": "FLOAT",
        "target": "DOUBLE",
        "description": "Normalize FLOAT columns to DOUBLE for compatibility.",
    },
    {
        "source": "STRING",
        "target": "STRING",
        "description": "Strip explicit lengths from STRING to match Databricks behavior.",
    },
    {
        "source": "BINARY",
        "target": "BINARY",
        "description": "Normalize BINARY lengths to the native type.",
    },
    {
        "source": "NUMBER",
        "target": "INT",
        "description": "NUMBER with no precision becomes INT by default (adjust with overrides).",
    },
    {
        "source": "NUMBER(p)",
        "target": "DECIMAL(p)",
        "description": "NUMBER with precision only is mapped to DECIMAL(p).",
    },
    {
        "source": "NUMBER(p,s)",
        "target": "DECIMAL(p,s)",
        "description": "NUMBER(p,s) becomes DECIMAL(p,s) in Databricks.",
    },
]


def build_mapping_rows(overrides: Dict[str, str]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    normalized_overrides = {str(k or "").strip(): str(v or "").strip() for k, v in overrides.items() if k and v}
    for entry in DEFAULT_DATATYPE_MAPPINGS:
        source = entry["source"]
        override_value = normalized_overrides.get(source)
        active_target = override_value or entry["target"]
        rows.append({
            "source": source,
            "defaultTarget": entry["target"],
            "activeTarget": active_target,
            "description": entry.get("description") or "",
            "override": override_value,
        })
    return rows


def apply_datatype_overrides(raw_sql: str, overrides: Dict[str, str]) -> str:
    if not raw_sql or not overrides:
        return raw_sql

    ddl = str(raw_sql)
    normalized_overrides = {str(k or "").strip(): str(v or "").strip() for k, v in overrides.items() if k and v}
    for source, target in normalized_overrides.items():
        pattern = re.compile(fr"\b{re.escape(source)}(\s*\([^\)]*\))?", re.IGNORECASE)

        def replace(match: re.Match) -> str:
            length_token = match.group(1) or ""
            if "{{length}}" in target:
                return target.replace("{{length}}", length_token)
            return f"{target}{length_token}"

        ddl = pattern.sub(replace, ddl)
    return ddl
