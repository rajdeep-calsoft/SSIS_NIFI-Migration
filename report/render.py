"""Renders a comparison result (diff.ComparisonResult) + pipeline state into
one self-contained HTML file, no external CSS/JS, no template engine
dependency -- plain string building with html.escape at every insertion
point. Nothing here is job-specific: every table/column name that appears
came from the job.yml the report was run against.
"""
from __future__ import annotations

import html
import json

from . import diff as diff_mod

CSS = """
body { font: 14px/1.5 -apple-system, Segoe UI, sans-serif; margin: 2rem; color: #1a1a1a; background: #fff; }
h1 { font-size: 1.4rem; margin-bottom: 0; }
h2 { font-size: 1.1rem; margin-top: 2rem; border-bottom: 1px solid #ddd; padding-bottom: .25rem; }
.sub { color: #666; margin-top: .25rem; }
table { border-collapse: collapse; width: 100%; margin: .5rem 0 1rem; }
th, td { text-align: left; padding: .35rem .6rem; border-bottom: 1px solid #eee; font-variant-numeric: tabular-nums; }
th { color: #666; font-weight: 600; font-size: .85rem; text-transform: uppercase; }
.ok { color: #0a7d33; }
.bad { color: #c62828; font-weight: 600; }
.badge { display: inline-block; padding: .15rem .6rem; border-radius: 1rem; font-size: .8rem; font-weight: 600; }
.badge.ok { background: #e6f4ea; color: #0a7d33; }
.badge.bad { background: #fce8e6; color: #c62828; }
.badge.state-RUNNING { background: #e6f4ea; color: #0a7d33; }
.badge.state-STOPPED { background: #f1f1f1; color: #555; }
.badge.state-MIXED { background: #fff4e5; color: #b76e00; }
.mono { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: .85rem; }
.dim { color: #888; }
.sample { color: #888; font-size: .85rem; }
"""


def _badge(text: str, ok: bool) -> str:
    return f'<span class="badge {"ok" if ok else "bad"}">{html.escape(text)}</span>'


def _scalars_table(scalars: list[diff_mod.ScalarDiff]) -> str:
    rows = "".join(
        f"<tr><td class='mono'>{html.escape(s.key)}</td>"
        f"<td>{s.source if s.source is not None else '-'}</td>"
        f"<td>{s.destination if s.destination is not None else '-'}</td>"
        f"<td>{'<span class=ok>==</span>' if s.agrees else '<span class=bad>' + str((s.destination or 0) - (s.source or 0)) + '</span>'}</td></tr>"
        for s in scalars
    )
    return f"<table><tr><th>metric</th><th>source (SSIS)</th><th>destination (NiFi)</th><th>delta</th></tr>{rows}</table>"


def _histograms(histograms: list[diff_mod.HistogramDiff]) -> str:
    out = []
    for h in histograms:
        rows = "".join(
            f"<tr><td class='mono'>{html.escape(str(bucket))}</td>"
            f"<td>{s if s is not None else '-'}</td><td>{d if d is not None else '-'}</td>"
            f"<td>{'<span class=ok>==</span>' if s == d else '<span class=bad>differs</span>'}</td></tr>"
            for bucket, s, d in h.rows
        )
        out.append(f"<h3>{html.escape(h.name.replace('_', ' '))}</h3>"
                    f"<table><tr><th>reason</th><th>source</th><th>destination</th><th></th></tr>{rows}</table>")
    return "".join(out) or "<p class='dim'>no histograms captured</p>"


def _sets(sets: list[diff_mod.SetDiff]) -> str:
    out = []
    for s in sets:
        out.append(f"<h3>{html.escape(s.name.replace('_', ' '))}</h3>")
        out.append(f"<p>in both: <b>{s.in_both}</b> &nbsp; "
                    f"only source: <b class='{'ok' if not s.only_source else 'bad'}'>{len(s.only_source)}</b> &nbsp; "
                    f"only destination: <b class='{'ok' if not s.only_destination else 'bad'}'>{len(s.only_destination)}</b></p>")
        for label, vals in (("only source", s.only_source), ("only destination", s.only_destination)):
            if vals:
                sample = ", ".join(html.escape(v) for v in vals[:10])
                more = f" &hellip; and {len(vals) - 10} more" if len(vals) > 10 else ""
                out.append(f"<p class='sample'>{label}: {sample}{more}</p>")
    return "".join(out)


def _pipeline_state(nifi_state: dict, source_runs: list[dict]) -> str:
    if not nifi_state.get("found"):
        nifi_html = f"<p class='bad'>process group {html.escape(nifi_state['group_name'])!r} not found on this NiFi canvas</p>"
    else:
        badge = f'<span class="badge state-{nifi_state["state"]}">{nifi_state["state"]}</span>'
        proc_rows = "".join(
            f"<tr><td>{html.escape(p['name'])}</td><td class='mono'>{html.escape(p['type'])}</td>"
            f"<td>{html.escape(p['state'])}</td><td>{html.escape(str(p.get('run_status') or '-'))}</td>"
            f"<td>{p.get('queued', 0)}</td></tr>"
            for p in nifi_state.get("processors", [])
        )
        bulletins = "".join(f"<li class='bad'>{html.escape(b)}</li>" for b in nifi_state.get("bulletins", []))
        nifi_html = (
            f"<p>group <span class='mono'>{html.escape(nifi_state['group_name'])}</span> {badge} "
            f"&nbsp; running={nifi_state['running']} stopped={nifi_state['stopped']} invalid={nifi_state['invalid']}</p>"
            f"<table><tr><th>processor</th><th>type</th><th>state</th><th>run status</th><th>queued</th></tr>{proc_rows}</table>"
            + (f"<p class='bad'>bulletins:</p><ul>{bulletins}</ul>" if bulletins else "")
        )

    run_rows = "".join(
        f"<tr><td class='mono'>{html.escape(r['batch_id'])}</td><td>{html.escape(r['source_file'])}</td>"
        f"<td>{html.escape(str(r['status']))}</td><td>{r.get('records_loaded', 0)}</td>"
        f"<td>{r.get('records_rejected', 0)}</td><td>{r.get('duration_ms') or '-'}</td></tr>"
        for r in source_runs
    )
    source_html = (f"<table><tr><th>batch</th><th>file</th><th>status</th><th>loaded</th>"
                    f"<th>rejected</th><th>duration (ms)</th></tr>{run_rows}</table>"
                    if source_runs else "<p class='dim'>no runs recorded on the source ledger</p>")

    return (f"<h2>Pipeline state</h2>"
            f"<h3>Destination (NiFi)</h3>{nifi_html}"
            f"<h3>Source (SSIS-equivalent run log)</h3>{source_html}")


def render_html(job_name: str, result: diff_mod.ComparisonResult,
                 nifi_state: dict, source_runs: list[dict]) -> str:
    verdict = _badge("AGREES", True) if result.agrees else _badge("DIFFERENCES FOUND", False)
    return f"""<!doctype html>
<html><head><meta charset="utf-8"><title>{html.escape(job_name)} -- comparison report</title>
<style>{CSS}</style></head>
<body>
<h1>{html.escape(job_name)} -- SSIS vs NiFi comparison report</h1>
<p class="sub">source captured {html.escape(result.source['captured_at'])} &middot;
destination captured {html.escape(result.destination['captured_at'])} &middot; {verdict}</p>

<h2>Row agreement</h2>
{_scalars_table(result.scalars)}

<h2>Reject reason breakdown</h2>
{_histograms(result.histograms)}

<h2>Key-set agreement</h2>
{_sets(result.sets)}

{_pipeline_state(nifi_state, source_runs)}

</body></html>
"""


def to_json(job_name: str, result: diff_mod.ComparisonResult,
            nifi_state: dict, source_runs: list[dict]) -> str:
    return json.dumps({
        "job": job_name,
        "agrees": result.agrees,
        "source_capture": result.source,
        "destination_capture": result.destination,
        "scalars": [vars(s) for s in result.scalars],
        "histograms": [vars(h) for h in result.histograms],
        "sets": [vars(s) for s in result.sets],
        "nifi_state": nifi_state,
        "source_job_runs": source_runs,
    }, indent=2, default=str)
