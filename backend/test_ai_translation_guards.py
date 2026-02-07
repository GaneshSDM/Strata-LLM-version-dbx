from backend import ai


def test_normalize_translation_result_accepts_valid_shape():
    result = {
        "objects": [{"name": "t1", "kind": "table", "target_sql": "CREATE TABLE t1(id INT);", "notes": ["ok"]}],
        "warnings": [],
    }
    normalized = ai._normalize_translation_result(result, {"name": "fallback", "kind": "table"})
    assert len(normalized["objects"]) == 1
    assert normalized["objects"][0]["target_sql"].startswith("CREATE TABLE")


def test_normalize_translation_result_rejects_invalid_shape():
    bad = {"objects": [{"name": "t1"}]}
    try:
        ai._normalize_translation_result(bad, {"name": "fallback", "kind": "table"})
        raised = False
    except ValueError:
        raised = True
    assert raised


def test_databricks_circuit_helpers_open_after_threshold():
    original_failures = ai._databricks_failure_count
    original_open_until = ai._databricks_circuit_open_until
    try:
        ai._databricks_failure_count = ai.DATABRICKS_CIRCUIT_FAILURE_THRESHOLD - 1
        ai._databricks_circuit_open_until = 0.0
        ai._record_databricks_failure()
        assert ai._is_databricks_circuit_open()
    finally:
        ai._databricks_failure_count = original_failures
        ai._databricks_circuit_open_until = original_open_until
