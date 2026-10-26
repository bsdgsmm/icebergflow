import json

import pytest

from icebergflow.ingestion.sources import ENTITIES, discover_batches


def _batch(root, name, row_counts=None):
    path = root / name
    path.mkdir()
    if row_counts is not None:
        (path / "_manifest.json").write_text(json.dumps({"row_counts": row_counts}))
    return path


def test_batches_are_returned_in_load_order(tmp_path):
    for name in ("2024-01-03", "2024-01-01", "2024-01-02-r1", "notes", "2024-1-4"):
        _batch(tmp_path, name)
    (tmp_path / "2024-01-05").write_text("not a directory")

    assert [b.batch_id for b in discover_batches(tmp_path)] == [
        "2024-01-01",
        "2024-01-02-r1",
        "2024-01-03",
    ]


def test_batch_selection_and_unknown_ids(tmp_path):
    _batch(tmp_path, "2024-01-01")
    _batch(tmp_path, "2024-01-02")

    assert [b.batch_id for b in discover_batches(tmp_path, ["2024-01-02"])] == ["2024-01-02"]
    with pytest.raises(FileNotFoundError, match="2024-01-09"):
        discover_batches(tmp_path, ["2024-01-09"])


def test_missing_raw_directory(tmp_path):
    with pytest.raises(FileNotFoundError):
        discover_batches(tmp_path / "absent")


def test_expected_rows_come_from_manifest(tmp_path):
    _batch(tmp_path, "2024-01-01", {"orders": 12})
    _batch(tmp_path, "2024-01-02")
    with_manifest, without = discover_batches(tmp_path)

    assert with_manifest.expected_rows(ENTITIES["orders"]) == 12
    assert with_manifest.expected_rows(ENTITIES["customers"]) is None
    assert without.expected_rows(ENTITIES["orders"]) is None
