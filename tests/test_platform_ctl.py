"""scripts/platform_ctl.py against stand-ins for the cde CLI and the CAI API: it acts only on this
project's prefixes, kills Spark children before orchestration runs, and changes nothing on a dry run."""

import re
import sys

import pytest

from ci.cai_jobs import APP, CAI_PROJECT_NAME, JOBS, MODEL
from coll.config import ROOT
from tests.conftest import _load

DAG = (ROOT / "cde" / "dags" / "collections_dag.py").read_text()
DEPLOY_DAG = (ROOT / "cde" / "scripts" / "deploy_dag.sh").read_text()


@pytest.fixture
def ctl(monkeypatch):
    mod = _load(ROOT / "scripts" / "platform_ctl.py")
    monkeypatch.setattr(mod, "DRY", False)
    monkeypatch.setattr(mod.time, "sleep", lambda s: None)
    return mod


def test_no_script_shadows_the_standard_library():
    stems = {p.stem for p in (ROOT / "scripts").glob("*.py")}
    assert not stems & set(sys.stdlib_module_names)


def test_config_names_match_the_deployed_resources(ctl):
    prj = ctl.PRJ
    assert prj["cai_project"] == CAI_PROJECT_NAME
    assert prj["cai_apps"] == [APP["name"]] and prj["cai_models"] == [MODEL["name"]]
    assert all(j["name"].startswith(prj["cai_job_prefix"]) for j in JOBS)
    job_prefix = re.search(r'^JOB_PREFIX = "([\w-]+)"', DAG, re.M).group(1)
    assert prj["cde_job_prefix"] == f"{job_prefix}-"
    assert prj["cde_dag_jobs"] == [re.search(r'DAG_JOB_NAME:-([\w-]+)', DEPLOY_DAG).group(1)]
    assert ctl.CFG["start_order"] == list(reversed(ctl.CFG["stop_order"]))
    assert set(ctl.LAYERS) == set(ctl.PROJECT) | set(ctl.SHARED) == {"cai", "cde", "cdw"}


def _fake_cde(calls, runs, jobs):
    def cde(*args, read=False):
        calls.append(args)
        if args[:2] == ("run", "list"):
            return runs
        if args[:2] == ("job", "list"):
            return jobs
        return ""
    return cde


RUNS = [
    {"id": 1, "job": "rsingh-coll-dlq-orchestration", "type": "airflow", "status": "running"},
    {"id": 2, "job": "rsingh-coll-dlq-build-silver", "type": "spark", "status": "starting"},
    {"id": 3, "job": "rsingh-spend-orchestration", "type": "airflow", "status": "running"},
    {"id": 4, "job": "rsingh-coll-dlq-dq-check", "type": "spark", "status": "succeeded"},
]
JOBS_LIST = [
    {"name": "rsingh-coll-dlq-orchestration", "schedule": {"enabled": True, "paused": False}},
    {"name": "rsingh-coll-dlq-build-silver", "schedule": {"enabled": False}},
    {"name": "rsingh-mule-orchestration", "schedule": {"enabled": True, "paused": False}},
]


def test_cde_stop_pauses_and_kills_only_this_project_children_first(ctl, monkeypatch):
    calls = []
    monkeypatch.setattr(ctl, "cde", _fake_cde(calls, RUNS, JOBS_LIST))
    ctl.cde_project("stop")
    acted = [c for c in calls if c[:2] not in (("run", "list"), ("job", "list"))]
    assert acted == [("job", "schedule", "pause", "--name", "rsingh-coll-dlq-orchestration"),
                     ("run", "kill", "--id", "2"), ("run", "kill", "--id", "1")]


def test_cde_start_unpauses_with_the_cli_verb(ctl, monkeypatch):
    calls = []
    paused = [{**JOBS_LIST[0], "schedule": {"enabled": True, "paused": True}}, JOBS_LIST[2]]
    monkeypatch.setattr(ctl, "cde", _fake_cde(calls, [], paused))
    ctl.cde_project("start")
    assert [c for c in calls if c[0] == "job" and c[1] == "schedule"] == [
        ("job", "schedule", "unpause", "--name", "rsingh-coll-dlq-orchestration")]


def test_dry_run_changes_nothing(ctl, monkeypatch, capsys):
    calls = []
    monkeypatch.setattr(ctl, "cde", _fake_cde(calls, RUNS, JOBS_LIST))
    monkeypatch.setattr(ctl, "DRY", True)
    ctl.cde_project("stop")
    assert {c[:2] for c in calls} == {("run", "list"), ("job", "list")}
    out = capsys.readouterr().out
    assert "would kill CDE run rsingh-coll-dlq-build-silver #2" in out and "rsingh-spend" not in out


class FakeCai:
    def __init__(self):
        self.calls = []

    def __call__(self, method, path, body=None, params=None):
        self.calls.append((method, path))
        return {}

    def apps(self):
        return [{"id": "a1", "name": APP["name"], "status": "APPLICATION_RUNNING", "cpu": 2, "memory": 4}]

    def jobs(self):
        return [{"id": "j1", "name": "rsingh-coll-dlq-daily-score", "schedule": "", "paused": False}]

    def active_runs(self, job):
        return [{"id": "r1", "status": "ENGINE_RUNNING"}]

    def deployments(self):
        m, b = {"id": "m", "name": MODEL["name"]}, {"id": "b"}
        return [(m, b, {"id": "d2", "status": "deployed", "created_at": "2"}),
                (m, b, {"id": "d1", "status": "stopped", "created_at": "1"})]


def test_cai_stop_and_start(ctl, monkeypatch):
    fake = FakeCai()
    monkeypatch.setattr(ctl, "Cai", lambda: fake)
    ctl.cai_project("stop")
    assert fake.calls == [("POST", "/applications/a1:stop"), ("POST", "/models/m/builds/b/deployments/d2:stop"),
                          ("POST", "/jobs/j1/runs/r1:stop")]
    fake.calls.clear()
    fake.deployments = lambda: [({"id": "m", "name": MODEL["name"]}, {"id": "b"},
                                 {"id": "d2", "status": "stopped", "created_at": "2"})]
    fake.apps = lambda: [{"id": "a1", "name": APP["name"], "status": "APPLICATION_STOPPED"}]
    ctl.cai_project("start")
    # the newest deployment before the app, which calls it; manual jobs get no schedule change
    assert fake.calls == [("POST", "/models/m/builds/b/deployments/d2:restart"), ("POST", "/applications/a1:restart")]


def test_platform_scope_needs_consent(ctl):
    with pytest.raises(SystemExit, match="--yes-shared"):
        ctl.main(["stop", "--scope", "platform"])
    with pytest.raises(SystemExit, match="unknown layers"):
        ctl.main(["status", "--only", "nifi"])


def test_dag_runs_are_bounded():
    assert "max_active_runs=1" in DAG and "dagrun_timeout=DAGRUN_TIMEOUT" in DAG
    assert '"execution_timeout": CDE_TASK_TIMEOUT' in DAG
    assert "execution_timeout=CAI_TASK_TIMEOUT" in DAG
    assert "status[in]running,starting" in DEPLOY_DAG and "exit 1" in DEPLOY_DAG
