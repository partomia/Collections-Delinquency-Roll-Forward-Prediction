#!/usr/bin/env python3
"""
CAI setup over the API v2, from a laptop or a CAI session. Idempotent: each step
finds by name first, creates only what is missing and corrects what drifted, so it
also adopts resources first made in the UI.

  1. Project CAI_PROJECT_NAME from this GitHub repo, if absent; waits for the clone.
  2. Project environment variables: HF_HOME, COLL_IMPALA_USER and
     COLL_IMPALA_PASSWORD (from the caller's environment, never printed). Other
     variables already in the project are kept.
  3. The jobs in ci/cai_jobs.py (script, vCPU, memory, GPU, timeout, manual
     schedule, no arguments); an existing job is updated to match.
  4. The model (authentication on, CPU), built and deployed once; its URL and
     access key become COLL_ENDPOINT_URL / _ACCESS_KEY in the project environment,
     with COLL_ENDPOINT_API_KEY when the caller sets it.
  5. The application, once COLL_ENDPOINT_API_KEY is in the project.

Prints the project and job IDs for the Airflow Variables. Standard library only.

  set -a; source .env; set +a
  python ci/setup_cai.py --dry-run                  # what would change
  python ci/setup_cai.py --no-serving --sync        # a new environment: project, env, jobs; then sync
  python ci/setup_cai.py --sync                     # + model and app (needs a published run)

A project cloned before cai/jobs/sync_code.py existed cannot create that job (the
API checks the script is there): the script is uploaded once, and the sync job's
git reset brings the rest.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from ci.cai_jobs import APP, CAI_PROJECT_NAME, DAILY_JOB, GIT_URL, JOBS, MODEL, RUNTIME, SYNC_JOB  # noqa: E402
from ci.trigger_cai_pipeline import Api, run_job  # noqa: E402

PROJECT_ENV_FROM_CALLER = ("COLL_IMPALA_USER", "COLL_IMPALA_PASSWORD")
PROJECT_ENV = {"HF_HOME": "/home/cdsw/.hf_cache"}


def cai_host() -> str:
    host = os.environ["COLL_CAI_HOST"].rstrip("/")
    return host if host.startswith("http") else f"https://{host}"


class Workbench(Api):
    """Api is rooted at /api/v2/projects/<id>; the workbench-level calls need the bare /api/v2."""

    def __init__(self, url: str, key: str):
        super().__init__(url, key, "")
        self.base = f"{url.rstrip('/')}/api/v2"

    def upload(self, project_id: str, rel_path: str, data: bytes) -> None:
        """PUT multipart with the form field named by the target path: a POST, or the path only in the
        filename, lands the file in the project root."""
        boundary = uuid.uuid4().hex
        body = (f'--{boundary}\r\nContent-Disposition: form-data; name="{rel_path}"; '
                f'filename="{Path(rel_path).name}"\r\nContent-Type: application/octet-stream\r\n\r\n').encode()
        body += data + f"\r\n--{boundary}--\r\n".encode()
        req = urllib.request.Request(f"{self.base}/projects/{project_id}/files", data=body, method="PUT",
                                     headers={**self.headers, "Content-Type": f"multipart/form-data; boundary={boundary}"})
        with urllib.request.urlopen(req, timeout=60, context=self.ctx):
            pass


def script_missing(err: urllib.error.HTTPError) -> bool:
    return err.code == 400 and "not found in project directory" in err.read().decode(errors="replace")


def find_project(wb: Workbench, name: str) -> dict | None:
    found = wb("GET", "/projects", params={"search_filter": json.dumps({"name": name}), "page_size": 100})
    return next((p for p in found.get("projects", []) if p["name"] == name), None)


def ensure_project(wb: Workbench, dry_run: bool) -> dict | None:
    project = find_project(wb, CAI_PROJECT_NAME)
    if project:
        print(f"project {CAI_PROJECT_NAME}: exists ({project['id']})")
        return project
    if dry_run:
        print(f"project {CAI_PROJECT_NAME}: would create from {GIT_URL}")
        return None
    project = wb("POST", "/projects", body={
        "name": CAI_PROJECT_NAME, "template": "git", "git_url": GIT_URL, "visibility": "private",
        "default_project_engine_type": "ml_runtime",
        "description": f"Collections delinquency roll-forward prediction: TabICL call list ({GIT_URL})"})
    print(f"project {CAI_PROJECT_NAME}: created ({project['id']}), cloning", end="", flush=True)
    for _ in range(60):
        status = str(wb("GET", f"/projects/{project['id']}").get("creation_status", "")).lower()
        if status in ("success", "succeeded", ""):
            break
        if "fail" in status or "error" in status:
            raise SystemExit(f"\nproject creation {status}")
        print(".", end="", flush=True)
        time.sleep(5)
    print(" done")
    return project


def project_env(wb: Workbench, project: dict) -> dict:
    return json.loads(wb("GET", f"/projects/{project['id']}").get("environment") or "{}")


def ensure_env(wb: Workbench, project: dict, dry_run: bool, extra: dict | None = None) -> None:
    missing = [k for k in PROJECT_ENV_FROM_CALLER if not os.environ.get(k)]
    if missing:
        raise SystemExit(f"set {missing} in the environment (source .env) first")
    current = project_env(wb, project)
    wanted = {**PROJECT_ENV, **{k: os.environ[k] for k in PROJECT_ENV_FROM_CALLER}, **(extra or {})}
    changed = sorted(k for k, v in wanted.items() if current.get(k) != v)
    if not changed:
        print("project environment: up to date")
        return
    if dry_run:
        print(f"project environment: would set {changed}")
        return
    wb("PATCH", f"/projects/{project['id']}", body={"environment": json.dumps({**current, **wanted})})
    print(f"project environment: set {changed}")


def job_settings(job: dict) -> dict:
    return {"cpu": job["cpu"], "memory": job["memory"], "nvidia_gpu": job["gpu"],
            "timeout": job["timeout"], "kill_on_timeout": True}


def drift(have: dict, want: dict) -> dict:
    """The settings that differ; the API returns some numbers as strings (timeout "15")."""
    return {k: v for k, v in want.items() if str(have.get(k)).lower() != str(v).lower()}


def ensure_jobs(wb: Workbench, project: dict, dry_run: bool) -> dict:
    existing = {j["name"]: j for j in wb("GET", f"/projects/{project['id']}/jobs",
                                         params={"page_size": 200}).get("jobs", [])}
    ids = {}
    for job in JOBS:
        want = job_settings(job)
        if job["name"] in existing:
            have = existing[job["name"]]
            ids[job["name"]] = have["id"]
            diff = drift(have, want)
            if not diff:
                print(f"job {job['name']}: exists ({have['id']})")
            elif dry_run:
                print(f"job {job['name']}: would update {diff} (now {({k: have.get(k) for k in diff})})")
            else:
                wb("PATCH", f"/projects/{project['id']}/jobs/{have['id']}", body=diff)
                print(f"job {job['name']}: updated {diff} ({have['id']})")
            continue
        if dry_run:
            print(f"job {job['name']}: would create ({job['script']}, {want})")
            continue
        body = {"name": job["name"], "script": job["script"], "runtime_identifier": RUNTIME, "arguments": "", **want}
        try:
            created = wb("POST", f"/projects/{project['id']}/jobs", body=body)
        except urllib.error.HTTPError as e:
            if not script_missing(e):
                raise
            if job["name"] != SYNC_JOB:
                raise SystemExit(f"job {job['name']}: {job['script']} is not in the project yet; "
                                 f"run {SYNC_JOB} (or git pull in a session), then rerun setup_cai.py")
            # a project cloned before sync_code.py existed: upload it once; the job's
            # git reset then brings the whole project to origin
            wb.upload(project["id"], job["script"], (ROOT / job["script"]).read_bytes())
            print(f"job {job['name']}: uploaded {job['script']} (the project predates it)")
            created = wb("POST", f"/projects/{project['id']}/jobs", body=body)
        ids[job["name"]] = created["id"]
        print(f"job {job['name']}: created ({created['id']})")
    return ids


def ensure_model(wb: Workbench, project: dict, dry_run: bool) -> dict | None:
    """The model, with a first build that deploys itself once built (cdsw-build.sh installs requirements.txt)."""
    pid = project["id"]
    model = next((m for m in wb("GET", f"/projects/{pid}/models", params={"page_size": 100}).get("models", [])
                  if m["name"] == MODEL["name"]), None)
    if model:
        print(f"model {MODEL['name']}: exists ({model['id']})")
        return model
    if dry_run:
        print(f"model {MODEL['name']}: would create, build and deploy "
              f"({MODEL['cpu']} vCPU / {MODEL['memory']} GB / {MODEL['gpu']} GPU)")
        return None
    model = wb("POST", f"/projects/{pid}/models", body={
        "project_id": pid, "name": MODEL["name"], "description": MODEL["description"],
        "disable_authentication": False})
    build = wb("POST", f"/projects/{pid}/models/{model['id']}/builds", body={
        "project_id": pid, "model_id": model["id"], "file_path": MODEL["file"], "function_name": "predict",
        "kernel": "python3", "runtime_identifier": RUNTIME, "auto_deploy_model": True,
        "auto_deployment_config": {"cpu": MODEL["cpu"], "memory": MODEL["memory"],
                                   "nvidia_gpus": MODEL["gpu"], "replicas": 1}})
    print(f"model {MODEL['name']}: created ({model['id']}), build {build['id']} deploys when built")
    return wb("GET", f"/projects/{pid}/models/{model['id']}")


def endpoint_env(wb: Workbench, model: dict | None) -> dict:
    """COLL_ENDPOINT_URL and _ACCESS_KEY from the model; _API_KEY (a Model API key) only from the caller."""
    if not model:
        return {}
    host = wb.base.split("://", 1)[1].split("/", 1)[0]
    env = {"COLL_ENDPOINT_URL": f"https://modelservice.{host}/model",
           "COLL_ENDPOINT_ACCESS_KEY": model["access_key"]}
    if os.environ.get("COLL_ENDPOINT_API_KEY"):
        env["COLL_ENDPOINT_API_KEY"] = os.environ["COLL_ENDPOINT_API_KEY"]
    return env


def ensure_app(wb: Workbench, project: dict, ready: bool, dry_run: bool) -> None:
    pid = project["id"]
    app = next((a for a in wb("GET", f"/projects/{pid}/applications", params={"page_size": 100})
                .get("applications", []) if a["name"] == APP["name"]), None)
    if app:
        print(f"application {APP['name']}: exists ({app['id']}, {app.get('status', '').lower()})")
    elif not ready:
        print(f"application {APP['name']}: waiting for COLL_ENDPOINT_API_KEY (the what-if tab calls the model)")
    elif dry_run:
        print(f"application {APP['name']}: would create ({APP['script']}, {APP['cpu']} vCPU / {APP['memory']} GB)")
    else:
        app = wb("POST", f"/projects/{pid}/applications", body={
            "project_id": pid, "name": APP["name"], "subdomain": APP["subdomain"], "script": APP["script"],
            "cpu": APP["cpu"], "memory": APP["memory"], "kernel": "python3", "runtime_identifier": RUNTIME,
            "description": APP["description"]})
        print(f"application {APP['name']}: created ({app['id']}), subdomain {APP['subdomain']}")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--no-serving", action="store_true",
                   help="project, environment and jobs only: the model and app need a published run first")
    p.add_argument("--sync", action="store_true",
                   help=f"run {SYNC_JOB} and wait: the project to origin/main and requirements installed")
    args, _ = p.parse_known_args()
    host, key = cai_host(), os.environ["COLL_CAI_API_KEY"]
    wb = Workbench(host, key)
    project = ensure_project(wb, args.dry_run)
    if project is None:
        return 0
    ids = ensure_jobs(wb, project, args.dry_run)
    if args.no_serving:
        ensure_env(wb, project, args.dry_run)
        print(f"model {MODEL['name']} and application {APP['name']}: skipped (--no-serving)")
    else:
        model = ensure_model(wb, project, args.dry_run)
        endpoint = endpoint_env(wb, model)
        ensure_env(wb, project, args.dry_run, endpoint)
        ready = "COLL_ENDPOINT_API_KEY" in endpoint or "COLL_ENDPOINT_API_KEY" in project_env(wb, project)
        ensure_app(wb, project, ready, args.dry_run)
    if args.sync and not args.dry_run:
        status = run_job(Api(host, key, project["id"]), SYNC_JOB, ids[SYNC_JOB], {})
        if status != "succeeded":
            raise SystemExit(f"{SYNC_JOB} {status}: see its run in the CAI project's Jobs page")
    print(f"\nCOLL_CAI_PROJECT_ID = {project['id']}")
    print(f"COLL_CAI_JOB_ID     = {ids.get(DAILY_JOB, '(not created)')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
