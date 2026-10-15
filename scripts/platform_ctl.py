#!/usr/bin/env python3
"""
Stop, start or show this demo's live Cloudera resources, layer by layer (config/platform.json).

Two scopes:
  project   (default) this demo's workloads only: the CAI application, model deployment and any
            active rsingh-coll-dlq-* job runs; the CDE Airflow schedule and running or starting
            rsingh-coll-dlq-* runs. Iceberg tables, CDE jobs/resources and Data Visualization
            dashboards stay as they are.
  platform  the project scope plus the environment's shared services: the CDW Impala virtual
            warehouse, the CDE virtual cluster and the CAI workspace. Sibling demos use the same
            services, so stop/start needs --yes-shared.

Stop runs in stop_order (CAI, CDE, CDW: consumers first), start in start_order (the reverse),
each layer's shared service before its workloads on start and after them on stop. --only limits
the layers. This project has no NiFi, Kafka, Flink or streaming jobs. Data Visualization has no
suspend in the CDP CLI and is left running.

CAI credentials come from .env (COLL_CAI_HOST, COLL_CAI_API_KEY), read here and never printed;
the cde CLI uses its profile (~/.cde/config.yaml), the cdp CLI its own (~/.cdp/credentials).

  python scripts/platform_ctl.py status [--scope platform]
  python scripts/platform_ctl.py stop  [--only cai,cde] [--dry-run]
  python scripts/platform_ctl.py start [--dry-run]
  python scripts/platform_ctl.py stop --scope platform --yes-shared

Not named platform.py: a scripts/platform.py would shadow the standard library's `platform`
for every script run from scripts/ (CI runs scripts/run_cde_local.py on Linux).
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
CFG = json.loads((ROOT / "config" / "platform.json").read_text())
PRJ, PLT = CFG["project"], CFG["platform"]
LAYERS = tuple(CFG["stop_order"])
CAI_ACTIVE = ("ENGINE_SCHEDULING", "ENGINE_STARTING", "ENGINE_RUNNING")
DRY = False
ACTED = 0


def say(msg: str) -> None:
    print(msg, flush=True)


def act(what: str, fn, *a, **kw):
    global ACTED
    ACTED += 1
    if DRY:
        say(f"  would {what}")
        return None
    say(f"  {what}")
    return fn(*a, **kw)


def wait_for(what: str, fn, ok: set, timeout: int = 2400, poll: int = 20) -> str:
    if DRY:
        return ""
    t0, last = time.time(), None
    while time.time() - t0 < timeout:
        st = str(fn())
        if st != last:
            say(f"    {what}: {st} ({time.time() - t0:.0f} s)")
            last = st
        if st.lower() in {s.lower() for s in ok}:
            return st
        time.sleep(poll)
    raise SystemExit(f"{what}: still {last} after {timeout} s")


def load_env(path: Path = ROOT / ".env") -> None:
    """KEY=VALUE lines of the git-ignored .env into os.environ (already-set variables win)."""
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.removeprefix("export ").split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("'\""))


def mine(name: str, prefix: str) -> bool:
    return bool(prefix) and name.startswith(prefix)


# ------------------------------------------------------------------ CLIs
def cdp(*args: str) -> dict:
    exe = os.environ.get("COLL_CDP_CLI") or shutil.which("cdp") or next(iter(glob.glob(
        os.path.expanduser("~/Library/Python/*/bin/cdp"))), None)
    if not exe:
        raise SystemExit("cdp CLI not found: pip install cdpcli, or set COLL_CDP_CLI")
    out = subprocess.run([exe, *args], capture_output=True, text=True)
    if out.returncode:
        raise RuntimeError(f"cdp {' '.join(args[:2])}: {out.stderr.strip()[:300]}")
    return json.loads(out.stdout) if out.stdout.strip() else {}


def cde(*args: str, read: bool = False):
    """The cde CLI. Read-only calls (list, describe) are retried: the CDE API drops connections
    now and then (EOF). Kill, pause and unpause are never retried."""
    for attempt in range(1, (4 if read else 1) + 1):
        out = subprocess.run(["cde", *args], capture_output=True, text=True)
        if not out.returncode:
            text = out.stdout.strip()
            return json.loads(text) if text.startswith(("{", "[")) else text
        if attempt < (4 if read else 1):
            time.sleep(5 * attempt)
    raise RuntimeError(f"cde {' '.join(args[:3])}: {out.stderr.strip()[:300]}")


class Cai:
    """The CAI API v2 for this project. Prints names and states only: job, run and project objects
    carry environment values in clear."""

    def __init__(self):
        from ci.setup_cai import Workbench, cai_host, find_project
        from ci.trigger_cai_pipeline import Api

        missing = [k for k in ("COLL_CAI_HOST", "COLL_CAI_API_KEY") if not os.environ.get(k)]
        if missing:
            raise SystemExit(f"{missing} not set in .env")
        host, key = cai_host(), os.environ["COLL_CAI_API_KEY"]
        project = find_project(Workbench(host, key), PRJ["cai_project"])
        if project is None:
            raise RuntimeError(f"CAI project {PRJ['cai_project']} not found")
        self.api = Api(host, key, project["id"])

    def __call__(self, method: str, path: str, body: dict | None = None, params: dict | None = None) -> dict:
        return self.api(method, path, body=body, params=params)

    def apps(self) -> list[dict]:
        return [a for a in self("GET", "/applications", params={"page_size": 100}).get("applications", [])
                if a["name"] in PRJ["cai_apps"]]

    def jobs(self) -> list[dict]:
        return [j for j in self("GET", "/jobs", params={"page_size": 200}).get("jobs", [])
                if mine(j["name"], PRJ["cai_job_prefix"])]

    def active_runs(self, job: dict) -> list[dict]:
        runs = self("GET", f"/jobs/{job['id']}/runs",
                    params={"page_size": 20, "sort": "-created_at"}).get("job_runs", [])
        return [r for r in runs if r.get("status") in CAI_ACTIVE]

    def deployments(self) -> list[tuple[dict, dict, dict]]:
        """(model, build, deployment) for every deployment of the project's models, newest first."""
        out = []
        for m in self("GET", "/models", params={"page_size": 100}).get("models", []):
            if m["name"] not in PRJ["cai_models"]:
                continue
            for b in self("GET", f"/models/{m['id']}/builds", params={"page_size": 100}).get("model_builds", []):
                for d in self("GET", f"/models/{m['id']}/builds/{b['id']}/deployments").get("model_deployments", []):
                    out.append((m, b, d))
        return sorted(out, key=lambda t: t[2]["created_at"], reverse=True)


# ------------------------------------------------------------------ project workloads
def cai_project(action: str) -> None:
    c = Cai()
    deps = c.deployments()
    if action == "status":
        for a in c.apps():
            say(f"  app {a['name']}: {a['status']} ({a['cpu']} vCPU / {a['memory']} GB)")
        for name in PRJ["cai_models"]:
            mine_ = [(b, d) for m, b, d in deps if m["name"] == name]
            say(f"  model {name}: " + (f"{mine_[0][1]['status']} (newest deployment {mine_[0][1]['id'][:8]}, "
                                       f"build {mine_[0][0]['id'][:8]})" if mine_ else "no deployment"))
        for j in c.jobs():
            runs = c.active_runs(j)
            sched = j.get("schedule") or "manual"
            say(f"  job {j['name']}: schedule {sched}{' PAUSED' if j.get('schedule') and j.get('paused') else ''}"
                f", {len(runs)} active run(s)")
        return
    if action == "stop":
        for a in c.apps():
            if a["status"] != "APPLICATION_STOPPED":
                act(f"stop application {a['name']}", c, "POST", f"/applications/{a['id']}:stop", body={})
        for m, b, d in deps:
            if d["status"] in ("deployed", "deploying", "pending"):
                act(f"stop model {m['name']} deployment {d['id'][:8]}", c, "POST",
                    f"/models/{m['id']}/builds/{b['id']}/deployments/{d['id']}:stop", body={})
        for j in c.jobs():
            if j["name"] in PRJ["cai_scheduled_jobs"] and j.get("schedule") and not j.get("paused"):
                act(f"pause schedule of {j['name']}", c, "PATCH", f"/jobs/{j['id']}", body={"paused": True})
            for r in c.active_runs(j):
                act(f"stop job run {j['name']} {r['id']}", c, "POST", f"/jobs/{j['id']}/runs/{r['id']}:stop", body={})
        return
    # start: schedules, the newest deployment of each model, then the applications (the app calls the model)
    for j in c.jobs():
        if j["name"] in PRJ["cai_scheduled_jobs"] and j.get("schedule") and j.get("paused"):
            act(f"resume schedule of {j['name']}", c, "PATCH", f"/jobs/{j['id']}", body={"paused": False})
    for name in PRJ["cai_models"]:
        newest = [(m, b, d) for m, b, d in deps if m["name"] == name]
        if newest and newest[0][2]["status"] not in ("deployed", "deploying", "pending"):
            m, b, d = newest[0]
            act(f"restart model {name} deployment {d['id'][:8]}", c, "POST",
                f"/models/{m['id']}/builds/{b['id']}/deployments/{d['id']}:restart", body={})
        elif not newest:
            say(f"  model {name}: no deployment to restart (ci/setup_cai.py builds and deploys it)")
    for a in c.apps():
        if a["status"] not in ("APPLICATION_RUNNING", "APPLICATION_STARTING"):
            act(f"start application {a['name']}", c, "POST", f"/applications/{a['id']}:restart", body={})


def cde_project(action: str) -> None:
    prefix = PRJ["cde_job_prefix"]

    def active() -> list[dict]:
        return [r for r in cde("run", "list", "--filter", "status[in]running,starting",
                               "--filter", f"job[rlike]^{prefix}", read=True) or []
                if mine(r["job"], prefix) and r["status"] in ("running", "starting")]

    runs = active()
    scheduled = [j for j in cde("job", "list", "--filter", f"name[rlike]^{prefix}", read=True) or []
                 if mine(j["name"], prefix) and (j.get("schedule") or {}).get("enabled")]
    if action == "status":
        say("  active runs: " + (", ".join(f"{r['job']} #{r['id']} {r['status']}" for r in runs) or "none"))
        for j in scheduled:
            s = j["schedule"]
            say(f"  schedule {j['name']} ({s.get('cronExpression')}): {'paused' if s.get('paused') else 'active'}")
        missing = [j for j in PRJ["cde_streaming_jobs"] if j not in {r["job"] for r in runs}]
        if missing:
            say(f"  streaming not running: {', '.join(missing)}")
        return
    if action == "stop":
        for j in scheduled:
            if not j["schedule"].get("paused"):
                act(f"pause CDE schedule {j['name']}", cde, "job", "schedule", "pause", "--name", j["name"])
        # Spark children before orchestration runs, so a DAG run cannot start a task again
        for r in sorted(runs, key=lambda r: r.get("type") == "airflow"):
            act(f"kill CDE run {r['job']} #{r['id']} ({r['status']})", cde, "run", "kill", "--id", str(r["id"]))
        if runs and not DRY:
            time.sleep(10)
            left = active()
            say("  still active: " + (", ".join(f"{r['job']} #{r['id']} {r['status']}" for r in left) or "none"))
        return
    for j in scheduled:
        if j["schedule"].get("paused"):
            # CLI 1.23 has pause/unpause, no resume
            act(f"unpause CDE schedule {j['name']} (Airflow runs the latest closed interval at once)",
                cde, "job", "schedule", "unpause", "--name", j["name"])
    running = {r["job"] for r in runs}
    for j in PRJ["cde_streaming_jobs"]:
        if j not in running:
            act(f"start streaming job {j} (resumes from its checkpoint)", cde, "job", "run", "--name", j)


PROJECT = {"cai": cai_project, "cde": cde_project}


# ------------------------------------------------------------------ shared services
def cdw(action: str) -> None:
    env_crn = cdp("environments", "describe-environment", "--environment-name", CFG["environment"])["environment"]["crn"]
    cid = next(c["id"] for c in cdp("dw", "list-clusters")["clusters"] if c["environmentCrn"] == env_crn)
    vws = {v["name"]: v for v in cdp("dw", "list-vws", "--cluster-id", cid)["vws"]}
    for name in PLT["cdw_vws"]:
        vw = vws[name]
        status = lambda v=vw: cdp("dw", "describe-vw", "--cluster-id", cid, "--vw-id", v["id"])["vw"]["status"]  # noqa: E731
        if action == "status":
            say(f"  CDW virtual warehouse {name}: {vw['status']}")
        elif action == "stop" and vw["status"].lower() not in ("stopped", "suspended"):
            act(f"suspend CDW virtual warehouse {name}", cdp, "dw", "suspend-vw", "--cluster-id", cid, "--vw-id", vw["id"])
            wait_for(name, status, {"Stopped", "Suspended"})
        elif action == "start" and vw["status"].lower() != "running":
            act(f"start CDW virtual warehouse {name}", cdp, "dw", "start-vw", "--cluster-id", cid, "--vw-id", vw["id"])
            wait_for(name, status, {"Running"})
    if action == "status":
        viz = {v.get("name"): v for v in cdp("dw", "list-data-visualizations", "--cluster-id", cid)
               .get("dataVisualizations", [])}
        for name in PLT.get("cdw_dataviz", []):
            say(f"  Data Visualization {name}: {(viz.get(name) or {}).get('status', 'not found')} "
                "(no suspend in the CDP CLI, left as is)")


def cde_vc(action: str) -> None:
    svc = next(s for s in cdp("de", "list-services")["services"]
               if s["name"] == PLT["cde_service"] and s["status"] == "ClusterCreationCompleted")
    vc = next(v for v in cdp("de", "list-vcs", "--cluster-id", svc["clusterId"])["vcs"] if v["vcName"] == PLT["cde_vc"])
    ids = ("--cluster-id", svc["clusterId"], "--vc-id", vc["vcId"])
    status = lambda: cdp("de", "describe-vc", *ids)["vc"]["status"]  # noqa: E731
    st = vc["status"]
    if action == "status":
        say(f"  CDE virtual cluster {PLT['cde_vc']}: {st}")
    elif action == "stop" and "suspend" not in st.lower():
        act(f"suspend CDE virtual cluster {PLT['cde_vc']}", cdp, "de", "suspend-vc", *ids)
        wait_for(PLT["cde_vc"], status, {"AppSuspended", "Suspended"})
    elif action == "start" and "suspend" in st.lower():
        act(f"resume CDE virtual cluster {PLT['cde_vc']}", cdp, "de", "resume-vc", *ids)
        wait_for(PLT["cde_vc"], status, {"AppInstalled", "Running"})


def cai_workspace(action: str) -> None:
    def ws():
        return next(w for w in cdp("ml", "list-workspaces")["workspaces"]
                    if w["instanceName"] == PLT["cai_workspace"] and w["environmentName"] == CFG["environment"])
    w = ws()
    st = w["instanceStatus"]
    if action == "status":
        say(f"  CAI workspace {PLT['cai_workspace']}: {st}")
    elif action == "stop" and "suspend" not in st:
        act(f"suspend CAI workspace {PLT['cai_workspace']}", cdp, "ml", "suspend-workspace", "--workspace-crn", w["crn"])
        wait_for(PLT["cai_workspace"], lambda: ws()["instanceStatus"], {"suspend:finished"}, timeout=3600)
    elif action == "start" and "suspend" in st:
        act(f"resume CAI workspace {PLT['cai_workspace']}", cdp, "ml", "resume-workspace", "--workspace-crn", w["crn"])
        wait_for(PLT["cai_workspace"], lambda: ws()["instanceStatus"], {"resume:finished", "installation:finished"},
                 timeout=3600)


SHARED = {"cdw": cdw, "cde": cde_vc, "cai": cai_workspace}


def layer(name: str, action: str, scope: str) -> None:
    say(f"[{name}]")
    steps = []
    if action == "start" and scope == "platform":
        steps.append(SHARED[name])
    if name in PROJECT:
        steps.append(PROJECT[name])
    if action != "start" and scope == "platform":
        steps.append(SHARED[name])
    if not steps:
        say("  shared service only: --scope platform")
        return
    before = ACTED
    for step in steps:
        try:
            step(action)
        except Exception as e:  # noqa: BLE001  one layer's failure is reported; the others still run
            say(f"  ERROR {type(e).__name__}: {e}")
            if action != "status":
                raise
    if action != "status" and ACTED == before:
        say(f"  nothing to {action}: already {'stopped' if action == 'stop' else 'running'}")


def main(argv: list[str] | None = None) -> int:
    global DRY
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("action", choices=["status", "stop", "start"])
    ap.add_argument("--scope", choices=["project", "platform"], default="project")
    ap.add_argument("--only", default="", help=f"comma-separated layers from {', '.join(LAYERS)}")
    ap.add_argument("--yes-shared", action="store_true", help="required to stop or start the shared services")
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args(argv)
    DRY = a.dry_run
    if a.scope == "platform" and a.action != "status" and not (a.yes_shared or a.dry_run):
        raise SystemExit("--scope platform stops/starts services other demos share: add --yes-shared")
    only = {x.strip() for x in a.only.split(",") if x.strip()}
    if only - set(LAYERS):
        raise SystemExit(f"unknown layers {sorted(only - set(LAYERS))}; this project has {', '.join(LAYERS)}")
    load_env()
    if DRY:
        say("dry run: nothing is changed")
    order = CFG["start_order"] if a.action == "start" else CFG["stop_order"]
    for name in order:
        if not only or name in only:
            layer(name, a.action, a.scope)
    return 0


if __name__ == "__main__":
    sys.exit(main())
