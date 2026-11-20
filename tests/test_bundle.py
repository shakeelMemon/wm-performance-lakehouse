"""Static contract checks on the bundle: every referenced file exists, every notebook
compiles, and the objects each layer depends on are actually produced upstream."""
import py_compile
import re
from pathlib import Path

import yaml

from wm_common.config import FEEDS

ROOT = Path(__file__).resolve().parents[1]
RES = ROOT / "resources"


def _resources():
    out = {}
    for p in RES.glob("*.yml"):
        out[p.name] = yaml.safe_load(p.read_text())
    return out


def _walk(obj, key):
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k == key and isinstance(v, str):
                yield v
            yield from _walk(v, key)
    elif isinstance(obj, list):
        for x in obj:
            yield from _walk(x, key)


def test_bundle_root_config():
    cfg = yaml.safe_load((ROOT / "databricks.yml").read_text())
    assert {"dev", "test", "prod"} <= set(cfg["targets"])
    assert cfg["targets"]["prod"]["mode"] == "production"


def test_referenced_files_exist():
    for name, doc in _resources().items():
        for key in ("notebook_path", "path", "file_path", "source_code_path"):
            for rel in _walk(doc, key):
                assert (RES / rel).resolve().exists(), f"{name}: {key} -> {rel}"


def test_python_sources_compile():
    for p in list((ROOT / "src").rglob("*.py")) + list((ROOT / "tools").rglob("*.py")):
        py_compile.compile(str(p), doraise=True)


def test_every_feed_has_bronze_and_silver_consumer():
    silver = "".join(p.read_text() for p in (ROOT / "src" / "pipelines" / "silver").glob("*.py"))
    for f in FEEDS:
        assert re.search(rf"""["'{{.]{f.name}["'}})]""", silver) or f'_stream("{f.name}")' in silver \
            or f'{{B}}.{f.name}' in silver, f"no silver consumer for bronze feed {f.name}"


def test_gold_objects_used_downstream_are_defined():
    gold_sql = "".join(p.read_text() for p in (ROOT / "src" / "pipelines" / "gold").glob("*.sql"))
    defined = set(re.findall(r"wm_gold\.(\w+)\s*\n", gold_sql)) | set(
        re.findall(r"MATERIALIZED VIEW \$\{wm\.catalog\}\.wm_gold\.(\w+)", gold_sql))
    defined.add("campaign_impact_results")  # written by the impact notebook
    consumers = [ROOT / "src" / "setup" / "apply_governance.py", *(ROOT / "src" / "semantic" / "metric_views").glob("*.yml")]
    for p in consumers:
        for t in re.findall(r"(?:GOLD\}|wm_gold)\.(\w+)", p.read_text()):
            assert t in defined, f"{p.name} uses undefined gold object {t}"


def test_pipeline_libraries_cover_all_pipeline_sources():
    pipe = _resources()["wm_pipeline.yml"]["resources"]["pipelines"]["wm_reporting_pipeline"]
    listed = {Path(lib["file"]["path"]).name for lib in pipe["libraries"]}
    on_disk = {p.name for p in (ROOT / "src" / "pipelines").rglob("*") if p.suffix in (".py", ".sql")}
    assert listed == on_disk
