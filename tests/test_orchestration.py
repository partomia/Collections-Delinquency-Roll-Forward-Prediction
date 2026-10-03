"""The DAG and the drill script name CDE jobs by string; a mismatch with
deploy_jobs.sh only shows up on the cluster as a 404 "job not found". Airflow is
not installed locally or in CI, so these checks read the files as text, and the
CAI trigger is run against stand-ins for the Airflow imports."""

import re
import sys
import types

import pytest

from coll.config import ROOT, settings
from tests.conftest import _load

DAG = (ROOT / "cde" / "dags" / "collections_dag.py").read_text()
DEPLOY = (ROOT / "cde" / "scripts" / "deploy_jobs.sh").read_text()
DRILL = (ROOT / "cde" / "scripts" / "backfill_drill.sh").read_text()
DEPLOY_DAG = (ROOT / "cde" / "scripts" / "deploy_dag.sh").read_text()


def _const(text: str, name: str) -> str:
    return re.search(rf'^{name}="?\$?{{?(?:{name}:-)?([\w-]+)', text, re.M).group(1)


def _deployed_suffixes() -> set[str]:
    return set(re.findall(r'^create_job "\$\{JOB_PREFIX\}-([\w-]+)"', DEPLOY, re.M))


def test_prefixes_agree():
    prefix = settings()["databases"]["prefix"]
    assert re.search(r'^DB_PREFIX = "(\w+)"', DAG, re.M).group(1) == prefix
    assert _const(DEPLOY, "DB_PREFIX") == _const(DRILL, "DB_PREFIX") == prefix
    job_prefix = re.search(r'^JOB_PREFIX = "([\w-]+)"', DAG, re.M).group(1)
    assert job_prefix == "rsingh-coll-dlq"
    assert _const(DEPLOY, "JOB_PREFIX") == _const(DRILL, "JOB_PREFIX") == job_prefix


def test_dag_and_drill_only_run_deployed_jobs():
    deployed = _deployed_suffixes()
    assert deployed == {"generate-loan-bronze", "dq-check", "build-silver", "build-gold-features"}
    assert set(re.findall(r'job_name=f"\{JOB_PREFIX\}-([\w-]+)"', DAG)) == deployed
    assert set(re.findall(r'"\$\{JOB_PREFIX\}-([\w-]+)"', DRILL)) == deployed


def test_dag_has_seven_tasks_in_pipeline_order():
    chain = re.search(r"^\s+(generate >> .+)$", DAG, re.M).group(1).split(" >> ")
    assert chain == ["generate", "dq_bronze", "silver", "dq_silver", "gold", "dq_gold", "score"]
    assert re.findall(r'dq_task\("(\w+)", "(\w+)"\)', DAG) == [
        ("dq_bronze", "bronze"), ("dq_silver", "silver"), ("dq_gold", "gold")]


def test_every_overridden_run_repeats_the_db_prefix():
    # run-time args replace the job's own args
    for args in re.findall(r'"args": \[(.+?)\]', DAG, re.S):
        assert args.lstrip().startswith('"--db-prefix", DB_PREFIX')
    assert 'run() { cde job run --name "$1" --arg=--db-prefix --arg="${DB_PREFIX}"' in DRILL


def test_schedule_and_dag_registration():
    assert 'DAILY = "30 0 * * *"' in DAG
    assert 'dag_id="collections_roll_forward_pipeline"' in DAG
    # unpaused registration runs the latest closed interval at once
    assert "is_paused_upon_creation=True" in DAG
    assert 'DAG_PATH="cde/dags/collections_dag.py"' in DEPLOY_DAG
    assert "rsingh-coll-dlq-orchestration" in DEPLOY_DAG and "rsingh-coll-dlq-pipeline" in DEPLOY_DAG
    for var in ("COLL_CAI_HOST", "COLL_CAI_PROJECT_ID", "COLL_CAI_JOB_ID", "COLL_CAI_API_KEY"):
        assert f'"{var}"' in DAG or f"'{var}'" in DAG
    assert '"COLL_RUN_DATE": as_of' in DAG and '"COLL_TRIGGERED_BY": "airflow"' in DAG


def test_cde_resources_fit_the_federal_queue_and_stay_overridable():
    # go01 sizing (4-core driver, 4 initial executors) was rejected on federal before it started
    defaults = dict(re.findall(r'"\$\{([A-Z_]+):-([\w]+)\}"', DEPLOY.split("RESOURCES=(")[1].split(")")[0]))
    assert defaults == {"DRIVER_CORES": "2", "DRIVER_MEMORY": "4g", "EXECUTOR_CORES": "4",
                        "EXECUTOR_MEMORY": "8g", "MIN_EXECUTORS": "1", "INITIAL_EXECUTORS": "2",
                        "MAX_EXECUTORS": "4"}


def test_impala_points_at_federal():
    imp = settings()["impala"]
    assert imp["host"] == "coordinator-federal-impala-1.dw-federal-cdp-env.dp5i-5vkq.cloudera.site"
    assert (imp["port"], imp["http_path"], imp["auth_mechanism"]) == (443, "cliservice", "LDAP")


def _load_dag(monkeypatch):
    """The DAG module with stand-ins for the Airflow and Cloudera imports."""

    class Op:
        def __init__(self, *a, **k):
            pass

        def __rshift__(self, other):
            return other

    class Dag(Op):
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

    class Skip(Exception):
        pass

    airflow = types.ModuleType("airflow")
    airflow.DAG = Dag
    mods = {"airflow": airflow,
            "airflow.exceptions": types.SimpleNamespace(AirflowException=RuntimeError, AirflowSkipException=Skip),
            "airflow.models": types.SimpleNamespace(Variable=types.SimpleNamespace(
                get=lambda k, default_var=None: {"COLL_CAI_HOST": "https://cai"}.get(k, "x"))),
            "airflow.operators.python": types.SimpleNamespace(PythonOperator=Op),
            "cloudera.cdp.airflow.operators.cde_operator": types.SimpleNamespace(CDEJobRunOperator=Op)}
    for name, mod in mods.items():
        monkeypatch.setitem(sys.modules, name, mod)
    return _load(ROOT / "cde" / "dags" / "collections_dag.py")


def test_a_dropped_cai_poll_does_not_fail_the_task(monkeypatch):
    requests = pytest.importorskip("requests")
    dag = _load_dag(monkeypatch)
    monkeypatch.setattr(dag.time, "sleep", lambda s: None)

    class Resp:
        def __init__(self, code, body):
            self.status_code, self.body = code, body

        def json(self):
            return self.body

        def raise_for_status(self):
            if self.status_code >= 400:
                raise requests.HTTPError(f"HTTP {self.status_code}", response=self)

    polls = iter([requests.ConnectionError("Connection reset by peer"), Resp(503, {}),
                  Resp(200, {"status": "ENGINE_RUNNING"}), Resp(200, {"status": "ENGINE_SUCCEEDED"})])

    def get(*a, **k):
        r = next(polls)
        if isinstance(r, Exception):
            raise r
        return r

    posts = []
    monkeypatch.setattr(dag.requests, "post", lambda *a, **k: posts.append(k) or Resp(200, {"id": "r1"}))
    monkeypatch.setattr(dag.requests, "get", get)
    assert dag.trigger_cai_job("2026-10-02") == "r1" and len(posts) == 1
    assert posts[0]["json"] == {"environment": {"COLL_TRIGGERED_BY": "airflow", "COLL_RUN_DATE": "2026-10-02"}}

    monkeypatch.setattr(dag.requests, "get", lambda *a, **k: Resp(403, {}))
    with pytest.raises(requests.HTTPError):
        dag.trigger_cai_job("2026-10-02")

    def reset(*a, **k):
        raise requests.ConnectionError("reset")

    monkeypatch.setattr(dag.requests, "get", reset)
    with pytest.raises(RuntimeError, match="failed polls in a row"):
        dag.trigger_cai_job("2026-10-02")
