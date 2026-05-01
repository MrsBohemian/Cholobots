import json
import os
import random
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib import request

import discord
from discord.ext import commands

try:
    from config import client
except Exception:
    client = None

# ---------- CRUDOBOT DATA FILES ----------
# You will reformat historic data into these files.
# Crudobot assumes these exist, but handles missing/empty files safely.

JOB_COSTING_FILE = Path("crudo_job_costing.json")
NARRATIVE_FILE = Path("crudo_narrative_data.json")
ESTIMATE_HISTORY_FILE = Path("crudo_estimate_history.json")

# Optional refreshed data from other bots.
# Metichebot pushes raw time to DATA_SERVICE_URL/tasks.
DATA_SERVICE_URL = os.getenv("DATA_SERVICE_URL", "").rstrip("/")

CRUDOBOT_PHRASES = [
    "the invoice wore sunglasses indoors",
    "a raccoon with a clipboard would like a word",
    "sometimes the drywall knows too much",
    "the spreadsheet sneezed and called it overhead",
    "a tiny accountant lives in the caulk gun",
    "measure twice, panic once",
    "the lumber is gossiping again",
    "the jobsite has entered its cryptid era",
]


# ---------- BASIC JSON HELPERS ----------

def now_iso() -> str:
    return datetime.now().isoformat()


def load_json(path: Path, default):
    if path.exists():
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return default


def save_json(path: Path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def money(value: Any) -> float:
    try:
        return float(str(value).replace("$", "").replace(",", "").strip())
    except Exception:
        return 0.0


def number(value: Any) -> float:
    try:
        return float(str(value).replace(",", "").strip())
    except Exception:
        return 0.0


def short(text: str, limit: int = 220) -> str:
    text = (text or "").replace("\n", " · ").strip()
    return text[:limit] + ("..." if len(text) > limit else "")


def crudo_phrase() -> str:
    return random.choice(CRUDOBOT_PHRASES)


def fetch_json_url(url: str, default):
    try:
        with request.urlopen(url, timeout=8) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception:
        return default


# ---------- DATA ACCESS ----------

def load_job_costing_reports() -> List[Dict[str, Any]]:
    return load_json(JOB_COSTING_FILE, [])


def load_narratives() -> List[Dict[str, Any]]:
    return load_json(NARRATIVE_FILE, [])


def save_narratives(items: List[Dict[str, Any]]):
    save_json(NARRATIVE_FILE, items)


def load_estimate_history() -> List[Dict[str, Any]]:
    return load_json(ESTIMATE_HISTORY_FILE, [])


def save_estimate_history(items: List[Dict[str, Any]]):
    save_json(ESTIMATE_HISTORY_FILE, items)


def fetch_metiche_raw_time() -> Dict[str, Any]:
    if not DATA_SERVICE_URL:
        return {}
    return fetch_json_url(f"{DATA_SERVICE_URL}/tasks", {})


# ---------- REPORT NORMALIZATION ----------

def report_title(report: Dict[str, Any]) -> str:
    return (
        report.get("job_name")
        or report.get("title")
        or report.get("job_id")
        or "Untitled job"
    )


def report_type(report: Dict[str, Any]) -> str:
    return (
        report.get("job_type")
        or report.get("scope_type")
        or report.get("category")
        or "uncategorized"
    )


def report_quantity(report: Dict[str, Any]) -> float:
    return number(report.get("quantity") or report.get("sqft") or report.get("units") or 0)


def report_unit(report: Dict[str, Any]) -> str:
    return str(report.get("unit") or ("sqft" if report.get("sqft") else "unit")).lower()


def report_revenue(report: Dict[str, Any]) -> float:
    return money(report.get("revenue") or report.get("price") or report.get("amount_charged") or 0)


def report_labor_hours(report: Dict[str, Any]) -> float:
    return number(report.get("labor_hours") or report.get("hours") or 0)


def report_materials_cost(report: Dict[str, Any]) -> float:
    return money(report.get("materials_cost") or report.get("material_cost") or report.get("materials") or 0)


def report_total_cost(report: Dict[str, Any]) -> float:
    explicit = money(report.get("total_cost") or report.get("cost") or 0)
    if explicit:
        return explicit
    labor_cost = money(report.get("labor_cost") or 0)
    return labor_cost + report_materials_cost(report)


def report_profit(report: Dict[str, Any]) -> float:
    explicit = report.get("profit")
    if explicit is not None:
        return money(explicit)
    return report_revenue(report) - report_total_cost(report)


def report_search_text(report: Dict[str, Any]) -> str:
    fields = [
        report.get("job_id", ""),
        report.get("job_name", ""),
        report.get("job_type", ""),
        report.get("scope", ""),
        report.get("description", ""),
        report.get("notes", ""),
        " ".join(report.get("tags", []) or []),
    ]
    return " ".join(str(x) for x in fields).lower()


def find_reports(query: str, reports: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    q = query.strip().lower()

    if not q:
        return reports

    if q.isdigit():
        idx = int(q) - 1
        if 0 <= idx < len(reports):
            return [reports[idx]]

    words = [w for w in re.findall(r"[a-zA-Z0-9]+", q) if len(w) > 1]

    scored = []
    for report in reports:
        haystack = report_search_text(report)
        score = sum(1 for w in words if w in haystack)
        if score:
            scored.append((score, report))

    scored.sort(key=lambda x: x[0], reverse=True)
    return [r for _, r in scored]


def comparable_reports(job_type_query: str, unit: str, reports: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    q_words = {w for w in re.findall(r"[a-zA-Z0-9]+", job_type_query.lower()) if len(w) > 2}
    unit = unit.lower().strip()

    matches = []
    for report in reports:
        haystack = report_search_text(report)
        type_words = {w for w in re.findall(r"[a-zA-Z0-9]+", haystack) if len(w) > 2}
        word_overlap = len(q_words & type_words)
        unit_match = report_unit(report) == unit
        qty = report_quantity(report)

        if qty > 0 and (word_overlap or unit_match):
            score = word_overlap + (2 if unit_match else 0)
            matches.append((score, report))

    matches.sort(key=lambda x: x[0], reverse=True)
    return [r for _, r in matches]


# ---------- FORMATTERS ----------

def format_report(report: Dict[str, Any]) -> str:
    title = report_title(report)
    job_type = report_type(report)
    qty = report_quantity(report)
    unit = report_unit(report)
    revenue = report_revenue(report)
    materials = report_materials_cost(report)
    labor_hours = report_labor_hours(report)
    total_cost = report_total_cost(report)
    profit = report_profit(report)

    lines = [
        f"🧾 {title}",
        f"{crudo_phrase()}",
        "",
        f"Job type: {job_type}",
    ]

    if qty:
        lines.append(f"Quantity: {qty:g} {unit}")

    if revenue:
        lines.append(f"Revenue: ${revenue:,.2f}")

    if total_cost:
        lines.append(f"Total cost: ${total_cost:,.2f}")

    if materials:
        lines.append(f"Materials: ${materials:,.2f}")

    if labor_hours:
        lines.append(f"Labor hours: {labor_hours:g}")

    if revenue or total_cost:
        lines.append(f"Profit: ${profit:,.2f}")

    notes = report.get("notes") or report.get("narrative") or report.get("description")
    if notes:
        lines.extend(["", "Notes:", short(str(notes), 500)])

    return "\n".join(lines)


def average(values: List[float]) -> float:
    values = [v for v in values if v is not None]
    return sum(values) / len(values) if values else 0.0


def range_label(values: List[float], suffix: str = "") -> str:
    values = [v for v in values if v is not None]
    if not values:
        return "not enough data"
    return f"{min(values):.2f}{suffix}–{max(values):.2f}{suffix}"


def format_estimate_basis(job_type: str, quantity: float, unit: str, matches: List[Dict[str, Any]]) -> str:
    usable = [r for r in matches if report_quantity(r) > 0]

    if not usable:
        return (
            f"🧮 Crudobot estimate support\n"
            f"{crudo_phrase()}\n\n"
            f"I do not have enough formatted actuals for `{job_type}` by `{unit}` yet.\n"
            f"Add job costing reports with quantity/unit/labor/materials/revenue, then I can extrapolate."
        )

    labor_per_unit = [
        report_labor_hours(r) / report_quantity(r)
        for r in usable
        if report_labor_hours(r) and report_quantity(r)
    ]

    materials_per_unit = [
        report_materials_cost(r) / report_quantity(r)
        for r in usable
        if report_materials_cost(r) and report_quantity(r)
    ]

    cost_per_unit = [
        report_total_cost(r) / report_quantity(r)
        for r in usable
        if report_total_cost(r) and report_quantity(r)
    ]

    revenue_per_unit = [
        report_revenue(r) / report_quantity(r)
        for r in usable
        if report_revenue(r) and report_quantity(r)
    ]

    avg_labor = average(labor_per_unit)
    avg_materials = average(materials_per_unit)
    avg_cost = average(cost_per_unit)
    avg_revenue = average(revenue_per_unit)

    lines = [
        f"🧮 Crudobot estimate support",
        f"{crudo_phrase()}",
        "",
        f"Estimate type: {job_type}",
        f"Quantity: {quantity:g} {unit}",
        f"Comparable reports found: {len(usable)}",
        "",
        "Actuals basis:",
        f"- Labor per {unit}: {range_label(labor_per_unit, 'h')}",
        f"- Materials per {unit}: ${range_label(materials_per_unit)}",
        f"- Total cost per {unit}: ${range_label(cost_per_unit)}",
        f"- Revenue per {unit}: ${range_label(revenue_per_unit)}",
        "",
        "Extrapolated from actuals:",
    ]

    if avg_labor:
        lines.append(f"- Labor: ~{avg_labor * quantity:.2f} hours")
    if avg_materials:
        lines.append(f"- Materials: ~${avg_materials * quantity:,.2f}")
    if avg_cost:
        lines.append(f"- Total cost basis: ~${avg_cost * quantity:,.2f}")
    if avg_revenue:
        lines.append(f"- Past charged basis: ~${avg_revenue * quantity:,.2f}")

    lines.extend(["", "Closest reports:"])
    for r in usable[:5]:
        lines.append(f"- {report_title(r)} ({report_type(r)})")

    return "\n".join(lines)


def grounded_theory_summary(reports: List[Dict[str, Any]], narratives: List[Dict[str, Any]]) -> str:
    # Simple non-LLM fallback that still returns useful grounded data.
    job_types: Dict[str, int] = {}
    repeated_words: Dict[str, int] = {}

    for r in reports:
        jt = report_type(r)
        job_types[jt] = job_types.get(jt, 0) + 1

    text_blob = " ".join([
        str(r.get("notes", "")) + " " + str(r.get("description", ""))
        for r in reports
    ] + [
        str(n.get("narrative", "")) + " " + str(n.get("notes", ""))
        for n in narratives
    ]).lower()

    stop = {"the", "and", "for", "with", "that", "this", "was", "were", "job", "work", "had", "but", "not", "you", "all", "out", "too"}
    for word in re.findall(r"[a-zA-Z]{4,}", text_blob):
        if word not in stop:
            repeated_words[word] = repeated_words.get(word, 0) + 1

    top_types = sorted(job_types.items(), key=lambda x: x[1], reverse=True)[:8]
    top_words = sorted(repeated_words.items(), key=lambda x: x[1], reverse=True)[:12]

    lines = [
        "📊 Crudobot grounded business report",
        crudo_phrase(),
        "",
        f"Job costing reports: {len(reports)}",
        f"Narrative entries: {len(narratives)}",
        "",
        "Most common job types:",
    ]

    if top_types:
        for jt, count in top_types:
            lines.append(f"- {jt}: {count}")
    else:
        lines.append("- not enough formatted job type data yet")

    lines.extend(["", "Repeated language / potential themes:"])
    if top_words:
        for word, count in top_words:
            lines.append(f"- {word}: {count}")
    else:
        lines.append("- not enough narrative text yet")

    return "\n".join(lines)


async def ask(ctx: commands.Context, bot: commands.Bot, prompt: str) -> str:
    await ctx.send(prompt)

    def check(m: discord.Message):
        return (m.author.id == ctx.author.id) and (m.channel.id == ctx.channel.id)

    msg = await bot.wait_for("message", check=check)
    return msg.content.strip()


# ---------- CRUDOBOT COMMANDS ----------

def register_crudo(bot: commands.Bot):

    @bot.group(name="crudo", invoke_without_command=True)
    async def crudo_group(ctx: commands.Context):
        await ctx.send(
            "Crudobot commands:\n"
            "- `!crudojc` — list and retrieve job costing reports\n"
            "- `!crudoestimate` — estimate support from historical actuals\n"
            "- `!crudoreport` — grounded business report from job costing + narrative data\n"
            "- `!crudo phrase` — receive nonsense from the jobsite cryptid"
        )

    @bot.command(name="crudojc")
    async def crudojc(ctx: commands.Context):
        reports = load_job_costing_reports()

        if not reports:
            await ctx.send("No job costing reports found in crudo_job_costing.json yet.")
            return

        lines = ["🧾 Available job costing reports:\n"]
        for idx, report in enumerate(reports[:25], start=1):
            lines.append(f"{idx}. {report_title(report)} — {report_type(report)}")

        lines.append("\nReply with a number or search term.")
        await ctx.send("\n".join(lines))

        def check(m: discord.Message):
            return (m.author.id == ctx.author.id) and (m.channel.id == ctx.channel.id)

        msg = await bot.wait_for("message", check=check)
        matches = find_reports(msg.content, reports)

        if not matches:
            await ctx.send("I could not find a matching job costing report.")
            return

        await ctx.send(format_report(matches[0]))

        narratives = load_narratives()
        narratives.append({
            "timestamp": now_iso(),
            "type": "job_costing_report_retrieved",
            "job_id": matches[0].get("job_id"),
            "job_name": report_title(matches[0]),
            "query": msg.content,
        })
        save_narratives(narratives)

    @bot.command(name="crudoestimate")
    async def crudoestimate(ctx: commands.Context):
        reports = load_job_costing_reports()

        if not reports:
            await ctx.send("No job costing reports found yet. I need formatted actuals before I can support estimating.")
            return

        job_type = await ask(ctx, bot, "What are you estimating? Example: `drywall repair`")
        quantity_text = await ask(ctx, bot, "How much? Example: `120 sqft`")

        qty_match = re.search(r"([\d,.]+)", quantity_text)
        quantity = number(qty_match.group(1)) if qty_match else 0
        unit_match = re.search(r"[a-zA-Z]+", quantity_text.replace(qty_match.group(1), "") if qty_match else quantity_text)
        unit = unit_match.group(0).lower() if unit_match else "unit"

        if not quantity:
            await ctx.send("I need a quantity to extrapolate from actuals.")
            return

        matches = comparable_reports(job_type, unit, reports)
        output = format_estimate_basis(job_type, quantity, unit, matches)
        await ctx.send(output[:1900])

        history = load_estimate_history()
        history.append({
            "timestamp": now_iso(),
            "job_type_query": job_type,
            "quantity": quantity,
            "unit": unit,
            "comparable_count": len(matches),
            "output": output,
        })
        save_estimate_history(history)

    @bot.command(name="crudoreport")
    async def crudoreport(ctx: commands.Context):
        reports = load_job_costing_reports()
        narratives = load_narratives()

        # Keep this grounded. Use local fallback unless/until the schema is rich enough
        # to safely send summarized data to the model.
        report = grounded_theory_summary(reports, narratives)
        await ctx.send(report[:1900])

    @crudo_group.command(name="phrase")
    async def crudo_phrase_cmd(ctx: commands.Context):
        await ctx.send(crudo_phrase())

    # Backward-compatible old commands.
    @crudo_group.command(name="report")
    async def crudo_report_old(ctx: commands.Context):
        await crudoreport(ctx)

    @crudo_group.command(name="close")
    async def crudo_close_old(ctx: commands.Context):
        await ctx.send(
            "Crudobot is now report/retrieval first.\n"
            "Use `!crudojc` for job costing reports or `!crudoreport` for business analysis."
        )
