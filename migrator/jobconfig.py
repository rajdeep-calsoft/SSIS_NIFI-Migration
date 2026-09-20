"""Loads jobs/<name>/job.yml -- the ONLY place a job's domain-specific facts
live. Everything under ssis2nifi/ and catalogue/ is schema-agnostic; this
module is what lets that generic core target a concrete deployment without
a single table or column name ever appearing in Python code.

job.yml shape (see jobs/telecom_cdr/job.yml for a worked example):

    name: telecom_cdr
    package: pkg_telecom_cdr.dtsx        # relative to the job's own directory

    nifi:
      url: http://localhost:8085

    destination_db:                       # what the migrated flow writes to
      db_type: PostgreSQL
      host: localhost                     # HOST-side address (published port) --
      port: 5437                          # used by report/ and other host tools
      dbname: telecom
      user: telecom_etl
      password_ref: TELECOM_DB_PASSWORD   # env var name; value never stored here
      driver_class: org.postgresql.Driver
      driver_path: /opt/nifi/drivers/postgresql.jar
      identifier_case: lower
      landing_dir: /opt/nifi/data/landing # path inside the NiFi container
      # Optional: address NiFi's OWN DBCP pool must use, reached from INSIDE
      # its container -- "localhost" means something different there than on
      # the host, so when NiFi and this DB are both in Docker, this must be
      # the DB service's internal Compose name + container port, NOT the
      # host/port above. Falls back to host/port if omitted (only correct
      # when NiFi itself isn't in Docker).
      nifi_internal_host: postgres
      nifi_internal_port: 5432

    source_db:                            # the SSIS-side warehouse, reached
      host: localhost                     # directly over its published port
      port: 5436                          # for the comparison report -- no FDW
      dbname: telecom
      user: telecom_etl
      password_ref: TELECOM_DB_PASSWORD

    ledger:                               # optional; omit for no job-run bookkeeping
      table: control.job_run_log
      reject_tables: [quarantine_cdr]

    compare:                              # what the report compares
      fact_table: fact_calls
      reject_table: quarantine_cdr
      source_table: raw_cdr
      reason_column: reason
"""
from __future__ import annotations

import dataclasses
import pathlib

import yaml


@dataclasses.dataclass
class JobConfig:
    path: pathlib.Path          # jobs/<name>/job.yml
    name: str
    raw: dict                   # full parsed document, for anything not modeled below

    @property
    def job_dir(self) -> pathlib.Path:
        return self.path.parent

    @property
    def package_path(self) -> pathlib.Path:
        return self.job_dir / self.raw["package"]

    @property
    def nifi_url(self) -> str:
        return self.raw["nifi"]["url"]

    @property
    def destination_db(self) -> dict:
        return self.raw["destination_db"]

    @property
    def source_db(self) -> dict:
        return self.raw.get("source_db")

    @property
    def ledger(self) -> dict | None:
        return self.raw.get("ledger")

    @property
    def compare(self) -> dict | None:
        return self.raw.get("compare")


def load(job_yml: str | pathlib.Path) -> JobConfig:
    path = pathlib.Path(job_yml).resolve()
    if not path.is_file():
        raise FileNotFoundError(f"no job.yml at {path}")
    raw = yaml.safe_load(path.read_text())

    missing = [k for k in ("name", "package", "nifi", "destination_db") if k not in raw]
    if missing:
        raise ValueError(f"{path}: missing required key(s): {', '.join(missing)}")

    return JobConfig(path=path, name=raw["name"], raw=raw)


def find(jobs_root: str | pathlib.Path, job_name: str) -> JobConfig:
    """Resolve a bare job name (e.g. "telecom_cdr") to jobs/<name>/job.yml."""
    return load(pathlib.Path(jobs_root) / job_name / "job.yml")


def resolve(job_arg: str, jobs_root: str | pathlib.Path = "jobs") -> JobConfig:
    """Accepts a job.yml path, a job directory, or a bare name under
    `jobs_root` -- the one job-lookup rule every entry point (migrator/cli.py,
    report/cli.py) shares."""
    p = pathlib.Path(job_arg)
    if p.suffix in (".yml", ".yaml") and p.is_file():
        return load(p)
    if (p / "job.yml").is_file():
        return load(p / "job.yml")
    return find(jobs_root, job_arg)
