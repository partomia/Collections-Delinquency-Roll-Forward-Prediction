"""GitHub -> CAI: the resource definitions, the trigger against a fake CAI API v2,
the sync-code job on a throwaway git repo, setup_cai and the Airflow Variables
against fakes, and the cai-pipeline workflow job."""

import io
import json
import subprocess
import sys
import urllib.error

import pytest

from ci import cai_jobs, trigger_cai_pipeline as trig
from tests.conftest import ROOT, _load

SHA = "0123456789abcdef0123"
SYNC, DAILY = cai_jobs.SYNC_JOB, cai_jobs.DAILY_JOB


class FakeApi:
    """Job list plus runs that go running -> the final status given per job name."""

    def __init__(self, final: dict | None = None, names=cai_jobs.GITHUB_CHAIN):
        self.jobs = {n: f"id-{i}" for i, n in enumerate(names)}
        self.final = final or {}
        self.started, self.polls = [], {}

    def __call__(self, method, path, body=None, params=None):
        if method == "GET" and path == "/jobs":
            return {"jobs": [{"name": n, "id": i} for n, i in self.jobs.items()]}
        name = next(n for n, i in self.jobs.items() if f"/jobs/{i}/" in path)
        if method == "POST":
            self.started.append((name, body["environment"]))
            return {"id": f"run-{name}"}
        self.polls[name] = self.polls.get(name, 0) + 1
        done = self.polls[name] > 1
        return {"status": self.final.get(name, "ENGINE_SUCCEEDED") if done else "ENGINE_RUNNING"}


def test_names_carry_the_project_prefix():
    names = [cai_jobs.CAI_PROJECT_NAME, *cai_jobs.BY_NAME, cai_jobs.MODEL["name"], cai_jobs.APP["name"],
             cai_jobs.APP["subdomain"]]
    assert all(n.startswith("rsingh-coll-dlq") for n in names), names


def test_cai_runs_on_cpu_with_the_federal_runtime():
    # federal: GPU runs, and 8 vCPU / 32 GB, never left ENGINE_SCHEDULING
    for spec in (*cai_jobs.JOBS, cai_jobs.MODEL):
        assert spec["gpu"] == 0 and spec["cpu"] <= 4 and spec["memory"] <= 16, spec
    assert cai_jobs.RUNTIME.endswith("ml-runtime-pbj-jupyterlab-python3.11-standard:2026.08.1-b5")


def test_chain_syncs_then_scores_as_a_dry_run(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(tmp_path / "summary.md"))
    api = FakeApi()
    assert trig.run_chain(api, SHA, poll_s=0) == 0
    assert [n for n, _ in api.started] == [SYNC, DAILY]
    env = dict(api.started)
    assert env[SYNC] == {"EXPECTED_GIT_SHA": SHA[:12]}
    assert env[DAILY] == {"COLL_TRIGGERED_BY": "github", "COLL_DRY_RUN": "1"}
    assert "Dry-run scoring passed" in capsys.readouterr().out
    assert (tmp_path / "summary.md").read_text().count("succeeded") == 2


def test_a_failed_scoring_run_fails_the_check_by_name(capsys):
    api = FakeApi({DAILY: "ENGINE_FAILED"})
    assert trig.run_chain(api, SHA, poll_s=0) == 1
    assert "::error::DRY-RUN SCORING FAILED" in capsys.readouterr().out


def test_a_failed_sync_stops_the_chain(capsys):
    api = FakeApi({SYNC: "ENGINE_FAILED"})
    assert trig.run_chain(api, SHA, poll_s=0) == 1
    assert [n for n, _ in api.started] == [SYNC]
    out = capsys.readouterr().out
    assert f"::error::{SYNC} failed" in out and "SCORING" not in out


def test_a_run_that_never_ends_times_out():
    api = FakeApi({SYNC: "ENGINE_RUNNING"})
    assert trig.run_job(api, SYNC, "id-0", {}, poll_s=0, deadline_s=0.05) == "timedout"


def test_missing_jobs_are_named():
    with pytest.raises(SystemExit, match=DAILY):
        trig.job_ids(FakeApi(names=cai_jobs.GITHUB_CHAIN[:-1]))


def test_no_cai_url_is_a_notice_not_a_failure(monkeypatch, capsys):
    monkeypatch.delenv("CAI_URL", raising=False)
    assert trig.main() == 0 and "::notice::" in capsys.readouterr().out


def test_the_dry_run_flag_comes_from_the_environment(monkeypatch):
    job = _load(ROOT / "cai" / "jobs" / "daily_score.py")
    seen = {}

    class Called(Exception):
        pass

    def run_daily(storage, **kwargs):
        seen.update(kwargs)
        raise Called

    monkeypatch.setattr(job, "run_daily", run_daily)
    monkeypatch.setattr(job, "get_storage", lambda backend: None)
    monkeypatch.setattr(sys, "argv", ["daily_score.py"])
    monkeypatch.setenv("COLL_DRY_RUN", "1")
    with pytest.raises(Called):
        job.main()
    assert seen["write"] is False
    monkeypatch.delenv("COLL_DRY_RUN")
    with pytest.raises(Called):
        job.main()
    assert seen["write"] is True


def test_every_job_is_in_the_runbook_and_its_script_exists():
    runbook = (ROOT / "docs" / "DEMO_RUNBOOK.md").read_text()
    for job in cai_jobs.JOBS:
        assert (ROOT / job["script"]).exists(), job["script"]
        row = (f"| `{job['name']}` | `{job['script']}` | {job['cpu']} vCPU / {job['memory']} GB / "
               f"{job['gpu']} GPU | {job['timeout']} min |")
        assert row in runbook, row
    assert (ROOT / cai_jobs.MODEL["file"]).exists() and (ROOT / cai_jobs.APP["script"]).exists()


def test_the_dag_waits_as_long_as_the_trigger():
    dag = (ROOT / "cde" / "dags" / "collections_dag.py").read_text()
    assert f"CAI_DEADLINE_MIN = {cai_jobs.DEADLINE_MIN}" in dag
    assert cai_jobs.DEADLINE_MIN >= cai_jobs.BY_NAME[DAILY]["timeout"]


def _git(cwd, *args):
    return subprocess.check_output(["git", "-c", "user.name=t", "-c", "user.email=t@t", *args], cwd=cwd,
                                   text=True).strip()


def test_sync_code_resets_to_the_pushed_commit(tmp_path, monkeypatch, capsys):
    origin, project = tmp_path / "origin", tmp_path / "project"
    origin.mkdir()
    _git(origin, "init", "-q", "-b", "main")
    (origin / "a.txt").write_text("v1")
    _git(origin, "add", "a.txt")
    _git(origin, "commit", "-qm", "v1")
    _git(tmp_path, "clone", "-q", str(origin), str(project))
    (origin / "a.txt").write_text("v2")
    _git(origin, "commit", "-qam", "v2")
    pushed = _git(origin, "rev-parse", "HEAD")
    (project / "a.txt").write_text("edited in the project")
    (project / "models").mkdir()
    (project / "models" / "coll_context.parquet").write_text("untracked")

    job = _load(ROOT / "cai" / "jobs" / "sync_code.py")
    installs = []
    monkeypatch.setattr(job, "_repo_root", lambda: project)
    monkeypatch.setattr(job, "install_requirements", lambda root: installs.append(root))
    monkeypatch.setenv("EXPECTED_GIT_SHA", pushed[:12])
    assert job.main() == 0 and installs == [project]
    assert (project / "a.txt").read_text() == "v2" and (project / "models" / "coll_context.parquet").exists()

    monkeypatch.setenv("EXPECTED_GIT_SHA", "deadbeef")                  # a newer push moved main on
    assert job.main() == 1 and "stopping the chain" in capsys.readouterr().out
    assert installs == [project]                                        # a stopped chain installs nothing


def test_sync_code_installs_requirements_only_when_they_change(tmp_path):
    job = _load(ROOT / "cai" / "jobs" / "sync_code.py")
    (tmp_path / "requirements.txt").write_text("pyyaml\n")
    calls = []
    assert job.install_requirements(tmp_path, pip=lambda: calls.append(1)) and calls == [1]
    assert not job.install_requirements(tmp_path, pip=lambda: calls.append(1)) and calls == [1]
    (tmp_path / "requirements.txt").write_text("pyyaml\nimpyla\n")
    assert job.install_requirements(tmp_path, pip=lambda: calls.append(1)) and calls == [1, 1]


def test_the_sync_marker_is_gitignored():
    assert "artifacts/" in (ROOT / ".gitignore").read_text().splitlines()


def test_workflow_runs_the_cai_chain_after_test():
    ci = (ROOT / ".github" / "workflows" / "ci.yml").read_text()
    job = ci.split("\n  cai-pipeline:")[1]
    assert "needs: test" in job and "github.event_name != 'pull_request'" in job
    assert "group: rsingh-coll-dlq-cai" in job and "cancel-in-progress: false" in job
    assert "python ci/trigger_cai_pipeline.py" in job
    for secret in ("CAI_URL", "CAI_API_KEY", "CAI_PROJECT_ID"):
        assert f"{secret}: ${{{{ secrets.{secret} }}}}" in job


class FakeWorkbench:
    def __init__(self, env="{}"):
        self.calls, self.env, self.jobs, self.project = [], env, [], None
        self.models, self.apps, self.base = [], [], "https://ml-x.example/api/v2"

    def __call__(self, method, path, body=None, params=None):
        self.calls.append((method, path, body))
        for kind, items in (("models", self.models), ("applications", self.apps)):
            if path == f"/projects/p1/{kind}" and method == "GET":
                return {kind: items}
            if path == f"/projects/p1/{kind}" and method == "POST":
                items.append({"id": f"{kind[0]}{len(items)}", "access_key": "mk-123", **body})
                return items[-1]
            if path.startswith(f"/projects/p1/{kind}/") and method == "GET":
                return next(i for i in items if i["id"] == path.rsplit("/", 1)[1])
        if path.startswith("/projects/p1/models/") and path.endswith("/builds") and method == "POST":
            return {"id": "b0", **body}
        if path == "/projects" and method == "GET":
            return {"projects": [self.project] if self.project else []}
        if path == "/projects" and method == "POST":
            self.project = {"id": "p1", "name": body["name"]}
            return self.project
        if path == "/projects/p1" and method == "GET":
            return {"creation_status": "success", "environment": self.env}
        if path == "/projects/p1" and method == "PATCH":
            self.env = body["environment"]
            return {}
        if path == "/projects/p1/jobs" and method == "GET":
            return {"jobs": self.jobs}
        if path == "/projects/p1/jobs" and method == "POST":
            self.jobs.append({"name": body["name"], "id": f"j{len(self.jobs)}", **body})
            return self.jobs[-1]
        if path.startswith("/projects/p1/jobs/") and method == "PATCH":
            next(j for j in self.jobs if j["id"] == path.rsplit("/", 1)[1]).update(body)
            return {}
        raise AssertionError((method, path))


def test_setup_cai_creates_once_and_never_prints_secrets(monkeypatch, capsys):
    from ci import setup_cai
    monkeypatch.setenv("COLL_IMPALA_USER", "u")
    monkeypatch.setenv("COLL_IMPALA_PASSWORD", "s3cret-value")
    wb = FakeWorkbench(env=json.dumps({"HF_TOKEN": "hf-kept"}))
    project = setup_cai.ensure_project(wb, dry_run=False)
    setup_cai.ensure_env(wb, project, dry_run=False)
    ids = setup_cai.ensure_jobs(wb, project, dry_run=False)
    assert project["name"] == "rsingh-coll-dlq" and set(ids) == set(cai_jobs.BY_NAME)
    assert json.loads(wb.env) == {"HF_TOKEN": "hf-kept", "HF_HOME": "/home/cdsw/.hf_cache",
                                  "COLL_IMPALA_USER": "u", "COLL_IMPALA_PASSWORD": "s3cret-value"}
    daily = next(j for j in wb.jobs if j["name"] == DAILY)
    spec = cai_jobs.BY_NAME[DAILY]
    assert (daily["cpu"], daily["memory"], daily["nvidia_gpu"]) == (spec["cpu"], spec["memory"], spec["gpu"])
    assert (daily["timeout"], daily["kill_on_timeout"]) == (spec["timeout"], True)
    assert all(j["runtime_identifier"] == cai_jobs.RUNTIME and j["arguments"] == "" for j in wb.jobs)
    n = len(wb.calls)
    setup_cai.ensure_project(wb, dry_run=False)
    setup_cai.ensure_env(wb, project, dry_run=False)
    setup_cai.ensure_jobs(wb, project, dry_run=False)
    assert not [c for c in wb.calls[n:] if c[0] in ("POST", "PATCH")]       # idempotent
    assert "s3cret-value" not in capsys.readouterr().out


def test_setup_cai_adopts_a_ui_made_job_and_fixes_only_what_drifted(capsys):
    from ci import setup_cai
    wb = FakeWorkbench()
    wb.project = {"id": "p1", "name": cai_jobs.CAI_PROJECT_NAME}
    # as the job is made in the UI: timeout 15 (a string in the API), otherwise right
    spec = cai_jobs.BY_NAME[DAILY]
    wb.jobs = [{"name": DAILY, "id": "zx81", "cpu": spec["cpu"], "memory": spec["memory"],
                "nvidia_gpu": spec["gpu"], "timeout": "15", "kill_on_timeout": True}]
    project = setup_cai.ensure_project(wb, dry_run=False)
    setup_cai.ensure_jobs(wb, project, dry_run=True)
    assert not [c for c in wb.calls if c[0] in ("POST", "PATCH")]
    assert f"would update {{'timeout': {spec['timeout']}}}" in capsys.readouterr().out
    ids = setup_cai.ensure_jobs(wb, project, dry_run=False)
    assert ("PATCH", "/projects/p1/jobs/zx81", {"timeout": spec["timeout"]}) in wb.calls
    assert ids[DAILY] == "zx81" and SYNC in ids                                # adopted + created


class PredatingWorkbench(FakeWorkbench):
    """A project cloned before this commit: job scripts are missing until uploaded."""

    def __init__(self):
        super().__init__()
        self.project, self.uploaded = {"id": "p1", "name": cai_jobs.CAI_PROJECT_NAME}, []

    def upload(self, project_id, rel_path, data):
        self.uploaded.append(rel_path)

    def __call__(self, method, path, body=None, params=None):
        if method == "POST" and path == "/projects/p1/jobs" and body["script"] not in self.uploaded:
            raise urllib.error.HTTPError(path, 400, "Bad Request", {}, io.BytesIO(
                f"script '{body['script']}' not found in project directory".encode()))
        return super().__call__(method, path, body, params)


def test_setup_cai_uploads_only_sync_code_into_a_project_that_predates_it():
    from ci import setup_cai
    wb = PredatingWorkbench()
    project = setup_cai.ensure_project(wb, dry_run=False)
    with pytest.raises(SystemExit, match=f"run {SYNC}"):
        setup_cai.ensure_jobs(wb, project, dry_run=False)
    assert wb.uploaded == ["cai/jobs/sync_code.py"] and [j["name"] for j in wb.jobs] == [SYNC]


def test_setup_cai_deploys_the_model_then_the_app_once_the_model_key_is_set(monkeypatch, capsys):
    from ci import setup_cai
    monkeypatch.setenv("COLL_IMPALA_USER", "u")
    monkeypatch.setenv("COLL_IMPALA_PASSWORD", "p")
    monkeypatch.delenv("COLL_ENDPOINT_API_KEY", raising=False)
    wb = FakeWorkbench()
    project = setup_cai.ensure_project(wb, dry_run=False)
    model = setup_cai.ensure_model(wb, project, dry_run=False)
    build = next(b for m, p, b in wb.calls if p.endswith("/builds"))
    assert build["file_path"] == "cai/model/predict.py" and build["function_name"] == "predict"
    assert build["auto_deploy_model"] and build["auto_deployment_config"]["nvidia_gpus"] == 0
    assert wb.models[0]["disable_authentication"] is False
    endpoint = setup_cai.endpoint_env(wb, model)
    assert endpoint == {"COLL_ENDPOINT_URL": "https://modelservice.ml-x.example/model",
                        "COLL_ENDPOINT_ACCESS_KEY": "mk-123"}
    setup_cai.ensure_app(wb, project, "COLL_ENDPOINT_API_KEY" in endpoint, dry_run=False)
    assert wb.apps == []                                                     # no Model API key yet
    monkeypatch.setenv("COLL_ENDPOINT_API_KEY", "model-key-value")
    model = setup_cai.ensure_model(wb, project, dry_run=False)
    assert len(wb.models) == 1
    endpoint = setup_cai.endpoint_env(wb, model)
    setup_cai.ensure_env(wb, project, dry_run=False, extra=endpoint)
    assert json.loads(wb.env)["COLL_ENDPOINT_API_KEY"] == "model-key-value"
    setup_cai.ensure_app(wb, project, True, dry_run=False)
    setup_cai.ensure_app(wb, project, True, dry_run=False)
    assert [(a["script"], a["subdomain"]) for a in wb.apps] == [("app/run.py", cai_jobs.APP["subdomain"])]
    out = capsys.readouterr().out
    assert "model-key-value" not in out and "mk-123" not in out


def test_setup_cai_no_serving_makes_jobs_but_no_model_or_app(monkeypatch):
    from ci import setup_cai
    wb = FakeWorkbench()
    monkeypatch.setattr(setup_cai, "Workbench", lambda *a: wb)
    monkeypatch.setattr(sys, "argv", ["setup_cai.py", "--no-serving"])
    for k, v in {"COLL_CAI_HOST": "ml-x.example", "COLL_CAI_API_KEY": "k",
                 "COLL_IMPALA_USER": "u", "COLL_IMPALA_PASSWORD": "p"}.items():
        monkeypatch.setenv(k, v)
    assert setup_cai.main() == 0
    assert {j["name"] for j in wb.jobs} == set(cai_jobs.BY_NAME) and wb.models == wb.apps == []
    assert not [p for _, p, _ in wb.calls if "/models" in p or "/applications" in p]


def test_airflow_variables_upsert_only_the_coll_keys_and_never_print_values(capsys):
    av = _load(ROOT / "cde" / "scripts" / "set_airflow_variables.py")
    store = {"COLL_CAI_HOST": "https://old", "COLL_CAI_PROJECT_ID": "p1", "CHURN_CAI_HOST": "https://churn"}
    calls = []

    def airflow(method, path, body=None):
        calls.append((method, path))
        key = path.rsplit("/", 1)[1]
        if method == "GET":
            if key not in store:
                raise urllib.error.HTTPError(path, 404, "not found", {}, None)
            return {"key": key, "value": store[key]}
        store[body["key"]] = body["value"]
        return {}

    wb = FakeWorkbench()
    wb.project = {"id": "p1", "name": cai_jobs.CAI_PROJECT_NAME}
    wb.jobs = [{"name": j["name"], "id": f"id-{j['name']}"} for j in cai_jobs.JOBS]
    wanted = av.wanted_variables(wb, "ml-x.example", "api-key-value")
    assert set(wanted) == {"COLL_CAI_HOST", "COLL_CAI_PROJECT_ID", "COLL_CAI_API_KEY", "COLL_CAI_JOB_ID"}
    assert wanted["COLL_CAI_HOST"] == "https://ml-x.example"
    assert wanted["COLL_CAI_JOB_ID"] == f"id-{DAILY}"
    dag = (ROOT / "cde" / "dags" / "collections_dag.py").read_text()
    assert all(f'Variable.get("{k}"' in dag or f"Variable.get('{k}'" in dag for k in wanted)
    av.apply(airflow, wanted, dry_run=True)
    assert all(m == "GET" for m, _ in calls) and store["COLL_CAI_HOST"] == "https://old"
    av.apply(airflow, wanted, dry_run=False)
    assert {k: store[k] for k in wanted} == wanted and store["CHURN_CAI_HOST"] == "https://churn"
    assert ("PATCH", "/variables/COLL_CAI_HOST") in calls
    assert not [p for m, p in calls if m != "GET" and "COLL_CAI_PROJECT_ID" in p]   # unchanged
    assert "api-key-value" not in capsys.readouterr().out
