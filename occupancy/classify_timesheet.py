#!/usr/bin/env python3
"""Parse Telegram timesheet export and estimate occupancy % by project."""

import json
from collections import defaultdict
from pathlib import Path

SRC = Path("/home/ubuntu/.cursor/projects/workspace/uploads/result__6__ef1d.json")
OUT_DIR = Path("/workspace/occupancy")

# Canonical people
PEOPLE = ["Александр", "Даниела", "Николай", "Виталий", "Миша"]

# Canonical projects (order for tables)
PROJECTS = ["Тануки", "Московский", "ТМЦ", "Агент", "ГСП", "Сапсан", "Прочее"]

# Manual classification: date -> person -> list of (project, weight)
# Each reporting day is later normalized to 1.0 FTE.
# Weight = hours if known, else 1 per task. A task mentioning two projects is split.
DAYS = {
    "2026-08-17": {
        "Александр": [("Тануки", 1), ("Тануки", 1)],
        "Даниела": [("ТМЦ", 1), ("Тануки", 1)],
        "Николай": [("Московский", 1), ("Прочее", 1)],
        "Виталий": [("ТМЦ", 1), ("ТМЦ", 1)],
        "Миша": [("ГСП", 1)],
    },
    "2026-08-18": {
        "Александр": [("Тануки", 1)],
        "Даниела": [("ТМЦ", 1), ("ТМЦ", 1)],
        "Николай": [("Сапсан", 1)],
        "Виталий": [("ТМЦ", 1)],
        "Миша": [("ТМЦ", 1), ("Тануки", 1.5)],
    },
    "2026-08-19": {
        "Александр": [("Тануки", 1), ("Тануки", 1), ("Московский", 1)],
        "Даниела": [("Московский", 1), ("Тануки", 1), ("ТМЦ", 1), ("ТМЦ", 0.5), ("ГСП", 0.5)],
        "Николай": [("Сапсан", 1), ("Сапсан", 1)],
        "Виталий": [("ТМЦ", 1), ("ТМЦ", 1)],
        "Миша": [("Тануки", 2), ("ТМЦ", 1), ("ГСП", 1), ("Прочее", 1)],
    },
    "2026-08-20": {
        "Александр": [("Тануки", 1), ("Тануки", 1)],
        "Даниела": [("ГСП", 1), ("ТМЦ", 1)],
        "Николай": [("Сапсан", 1), ("Московский", 0.5), ("ТМЦ", 0.5)],
        "Виталий": [("ТМЦ", 1)],
        "Миша": [("Тануки", 0.5), ("ТМЦ", 4)],
    },
    "2026-08-21": {
        "Александр": [("Московский", 1), ("Тануки", 1)],
        "Даниела": [("ТМЦ", 1), ("Тануки", 1)],
        "Николай": [("Сапсан", 1), ("Московский", 1)],
        "Виталий": [("Агент", 1)],
        "Миша": [
            ("Тануки", 1.5),
            ("Тануки", 0.5),
            ("Прочее", 1),
            ("Прочее", 1.5),
            ("Тануки", 0.5),
            ("Московский", 1),
        ],
    },
    "2026-08-24": {
        "Александр": [("Тануки", 1), ("Тануки", 1), ("Тануки", 1), ("Тануки", 1)],
        "Даниела": [("Тануки", 1), ("Тануки", 1), ("Московский", 1), ("Московский", 1)],
        "Николай": [("Московский", 1), ("Сапсан", 1), ("Тануки", 1)],
        "Виталий": [("Агент", 1)],
        "Миша": [("ТМЦ", 2), ("Сапсан", 2)],
    },
    "2026-08-25": {
        "Александр": [("Московский", 1), ("Тануки", 1), ("Тануки", 1)],
        "Даниела": [("Тануки", 1), ("Тануки", 1), ("Московский", 1), ("Тануки", 1)],
        "Николай": [("Московский", 1), ("Тануки", 1), ("ТМЦ", 1)],
        "Виталий": [("Агент", 1)],
        "Миша": [("Тануки", 0.5), ("Сапсан", 2), ("Прочее", 2), ("ТМЦ", 1)],
    },
    "2026-08-26": {
        "Даниела": [("Московский", 1), ("ТМЦ", 1), ("Тануки", 1)],
        "Николай": [("ТМЦ", 1), ("ТМЦ", 1)],
        "Виталий": [("Агент", 1), ("ТМЦ", 1)],
        "Миша": [("Тануки", 1), ("Тануки", 1), ("Московский", 1), ("Прочее", 1)],
    },
    "2026-08-27": {
        "Даниела": [("ГСП", 1), ("Тануки", 1), ("Тануки", 1)],
        "Николай": [("ГСП", 1), ("Московский", 1), ("ТМЦ", 1)],
        "Виталий": [("ТМЦ", 1)],
        "Миша": [("Прочее", 1), ("Московский", 1), ("Тануки", 0.5), ("Тануки", 1)],
    },
    "2026-08-28": {
        "Даниела": [("ГСП", 1), ("Тануки", 1)],
        "Николай": [("Московский", 1), ("Сапсан", 1)],
        "Виталий": [("ТМЦ", 1)],
        "Миша": [("Тануки", 1.5), ("Прочее", 1), ("Московский", 2)],
    },
    "2026-08-31": {
        "Даниела": [("Тануки", 1)],
        "Николай": [("Московский", 1), ("Московский", 1), ("ГСП", 1)],
        "Виталий": [("ТМЦ", 1), ("ТМЦ", 1)],
        "Миша": [
            ("Московский", 1),
            ("ГСП", 1),
            ("Тануки", 1),
            ("Прочее", 1),
            ("Тануки", 1),
            ("Прочее", 1),
        ],
    },
    "2026-09-01": {
        "Даниела": [("Тануки", 1)],
        "Николай": [("ГСП", 1), ("Московский", 1)],
        "Виталий": [("ТМЦ", 1)],
        "Миша": [("ТМЦ", 0.75), ("Московский", 4), ("Тануки", 1.5)],
    },
    "2026-09-02": {
        "Даниела": [("Тануки", 1), ("Московский", 1), ("Тануки", 1)],
        "Николай": [("ГСП", 1), ("Московский", 1), ("Московский", 1), ("ТМЦ", 1)],
        "Виталий": [("ТМЦ", 1), ("ТМЦ", 1)],
        "Миша": [
            ("Тануки", 1),
            ("ТМЦ", 1),
            ("ТМЦ", 1),
            ("Прочее", 1.5),
            ("Тануки", 1),
            ("Московский", 1),
        ],
    },
    "2026-09-03": {
        "Даниела": [("Тануки", 1), ("Тануки", 1), ("Московский", 1)],
        "Николай": [("Московский", 1), ("ТМЦ", 1)],
        "Виталий": [("ТМЦ", 1), ("Агент", 1)],
        "Миша": [("Тануки", 1), ("Тануки", 1.5), ("Сапсан", 1), ("Московский", 0.5)],
    },
    "2026-09-04": {
        "Даниела": [("Тануки", 1)],
        "Николай": [("Московский", 1), ("ТМЦ", 1)],
        "Виталий": [("ТМЦ", 1), ("Агент", 1)],
        "Миша": [
            ("Прочее", 0.5),
            ("Тануки", 0.5),
            ("Тануки", 1.5),
            ("Сапсан", 1),
            ("Тануки", 0.5),
        ],
    },
    "2026-09-07": {
        "Даниела": [("Тануки", 1)],
        "Николай": [("ТМЦ", 1), ("Московский", 1), ("Московский", 1)],
        "Виталий": [("Агент", 1)],
        "Миша": [("Московский", 1), ("Московский", 1), ("Московский", 1), ("Тануки", 1)],
    },
    "2026-09-08": {
        "Даниела": [("Тануки", 1), ("Московский", 1)],
        "Николай": [("Московский", 1), ("Московский", 1), ("ТМЦ", 1)],
        "Виталий": [("Агент", 1)],
        "Миша": [("Тануки", 2), ("Тануки", 1), ("Прочее", 1)],
    },
    "2026-09-09": {
        "Александр": [("Московский", 1), ("Тануки", 1)],
        "Даниела": [("Московский", 1), ("Тануки", 1), ("Тануки", 1)],
        "Николай": [("ТМЦ", 1), ("ТМЦ", 1)],
        "Виталий": [("ТМЦ", 1), ("Агент", 1)],
        "Миша": [("Тануки", 0.5), ("Тануки", 1), ("Московский", 0.5), ("Тануки", 1)],
    },
    "2026-09-10": {
        "Александр": [("Тануки", 1), ("Тануки", 1), ("Тануки", 1)],
        "Даниела": [("Московский", 1), ("Московский", 1), ("Московский", 1)],
        "Николай": [("ТМЦ", 1), ("ТМЦ", 1), ("Московский", 1), ("Московский", 1)],
        "Виталий": [("ТМЦ", 1), ("Агент", 1)],
        "Миша": [("Тануки", 3), ("ТМЦ", 3.5)],
    },
    "2026-09-14": {
        "Александр": [("Тануки", 1), ("Тануки", 1)],
        "Даниела": [("Московский", 1), ("Московский", 1)],
        "Николай": [("ТМЦ", 1)],
        "Виталий": [("ТМЦ", 1), ("Агент", 1)],
        "Миша": [("Тануки", 1), ("Московский", 1), ("Тануки", 1)],
    },
    "2026-09-15": {
        "Александр": [("Тануки", 1), ("Московский", 1), ("Тануки", 1)],
        "Даниела": [("ТМЦ", 1), ("Московский", 1)],
        "Виталий": [("Агент", 1)],
        "Миша": [("ТМЦ", 0.5), ("Тануки", 0.5), ("Московский", 1), ("ТМЦ", 0.5)],
    },
    "2026-09-16": {
        "Александр": [("Московский", 1), ("Тануки", 1), ("Тануки", 1), ("Тануки", 1)],
        "Даниела": [("Московский", 1), ("Московский", 1), ("ТМЦ", 1)],
        "Виталий": [("Агент", 1)],
        "Миша": [("Тануки", 1.5), ("Московский", 6)],
    },
    "2026-09-17": {
        "Александр": [("Тануки", 1), ("Тануки", 1), ("Тануки", 1)],
        "Даниела": [("ТМЦ", 1), ("Тануки", 1), ("Московский", 1)],
        "Виталий": [("Агент", 1)],
        "Миша": [("Московский", 2), ("Сапсан", 1), ("Тануки", 1), ("Прочее", 1)],
    },
    "2026-09-18": {
        "Александр": [("Тануки", 1), ("Московский", 1), ("Тануки", 1)],
        "Даниела": [("Московский", 1), ("Тануки", 1), ("Тануки", 1), ("Тануки", 1)],
        "Виталий": [("Агент", 1)],
        "Миша": [("ТМЦ", 1), ("Тануки", 2), ("Сапсан", 1), ("Прочее", 1)],
    },
    "2026-09-21": {
        "Александр": [("Тануки", 1), ("Тануки", 1), ("Тануки", 1), ("Московский", 1)],
        "Даниела": [("Тануки", 1), ("Тануки", 1), ("Тануки", 1)],
        "Николай": [("ТМЦ", 1), ("ТМЦ", 1)],
        "Виталий": [("Агент", 1), ("Агент", 1)],
    },
}


def pct(part, total):
    if total <= 0:
        return 0.0
    return 100.0 * part / total


def round_pcts(values, digits=0):
    """Round percentages so they sum to 100."""
    if not values:
        return values
    raw = [round(v, digits) for v in values]
    # fix remainder on the largest item
    target = 100.0 if digits == 0 else round(100.0, digits)
    diff = round(target - sum(raw), digits)
    if diff and raw:
        idx = max(range(len(raw)), key=lambda i: values[i])
        raw[idx] = round(raw[idx] + diff, digits)
    return raw


def compute():
    # person -> project -> FTE-days
    fte = {p: defaultdict(float) for p in PEOPLE}
    days_count = {p: 0 for p in PEOPLE}

    for date, people in DAYS.items():
        for person, tasks in people.items():
            wsum = sum(w for _, w in tasks)
            if wsum <= 0:
                continue
            days_count[person] += 1
            for project, w in tasks:
                fte[person][project] += w / wsum

    # team: equal person-weight (average of personal %), and FTE-weighted
    team_fte = defaultdict(float)
    total_fte_days = 0.0
    for person in PEOPLE:
        for project, v in fte[person].items():
            team_fte[project] += v
        total_fte_days += days_count[person]

    return fte, days_count, team_fte, total_fte_days


def fmt(n, digits=0):
    if digits == 0:
        return f"{int(round(n))}%"
    return f"{n:.{digits}f}%".replace(".", ",")


def build_rows(fte, days_count, team_fte, total_fte_days):
    rows = []
    for person in PEOPLE:
        total = days_count[person]
        values = [pct(fte[person].get(pr, 0.0), total) for pr in PROJECTS]
        rounded = round_pcts(values, 0)
        rows.append(
            {
                "name": person,
                "days": total,
                "pcts": rounded,
                "raw": values,
            }
        )

    team_values = [pct(team_fte.get(pr, 0.0), total_fte_days) for pr in PROJECTS]
    team_rounded = round_pcts(team_values, 0)
    rows.append(
        {
            "name": "Команда",
            "days": round(total_fte_days, 1),
            "pcts": team_rounded,
            "raw": team_values,
        }
    )
    return rows


def to_md(rows):
    header = "| Сотрудник | Дней | " + " | ".join(PROJECTS) + " |"
    sep = "|---|---:|" + "|".join(["---:" for _ in PROJECTS]) + "|"
    lines = [
        "# Занятость по проектам",
        "",
        "Период: **17.08.2026 — 21.09.2026** (стендапы из группы «Таймшит»).",
        "",
        "Каждый день сотрудника = 100%. Задачи внутри дня делятся поровну, "
        "если в тексте указаны часы — пропорционально часам.",
        "",
        header,
        sep,
    ]
    for row in rows:
        days = row["days"]
        days_s = str(int(days)) if float(days).is_integer() else str(days)
        cells = " | ".join(fmt(v) if v else "—" for v in row["pcts"])
        strong = "**" if row["name"] == "Команда" else ""
        lines.append(f"| {strong}{row['name']}{strong} | {days_s} | {cells} |")
    lines += [
        "",
        "## Как читается",
        "",
        "Пример: у Александра 81% Тануки и 19% Московский — это доля его "
        "рабочих дней за период, а не доля команды.",
        "",
        "## Что куда относил",
        "",
        "- **Тануки** — синки, сегменты, отзывы, избранное/Mindbox, сториз, "
        "лояльность iOS, статические страницы, рубрикатор Android, поиск Android.",
        "- **Московский** — релизы, аппроутер, Strapi, фильтры, чеки, декларации, видео.",
        "- **ТМЦ** — очередь, реестр, загрузка файлов, справочники, аудиторы, Ollama, DOTS.",
        "- **Агент** — UI-агент, драйверы, мультимодальность, сценарии, Qwen/GUI.",
        "- **ГСП** — поиск/очередь/релиз/макет ГСП, счётчик документов ГСП.",
        "- **Сапсан** — лендинг, статьи, SEO, заявки.",
        "- **Прочее** — налоги, счета, Кардекс, КТС, Мозен, внутренние разборки, "
        "инфра без явного проекта.",
        "",
        "Оценка приблизительная: в таймшите нет полных часов у всех, часть задач "
        "без названия проекта размечена по контексту соседних дней.",
        "",
    ]
    return "\n".join(lines) + "\n"


def to_html(rows):
    thead = "".join(f"<th>{p}</th>" for p in PROJECTS)
    body = []
    for row in rows:
        cls = ' class="team"' if row["name"] == "Команда" else ""
        days = row["days"]
        days_s = str(int(days)) if float(days).is_integer() else str(days)
        tds = []
        for v in row["pcts"]:
            if not v:
                tds.append('<td class="empty">—</td>')
            else:
                bar = min(100, v)
                tds.append(
                    f'<td><div class="cell"><span class="bar" style="width:{bar}%"></span>'
                    f'<span class="n">{fmt(v)}</span></div></td>'
                )
        body.append(
            f"<tr{cls}><th>{row['name']}</th><td class='days'>{days_s}</td>{''.join(tds)}</tr>"
        )

    return f"""<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Занятость по проектам · 17.08–21.09.2026</title>
  <style>
    :root {{
      --bg: #0f1419;
      --card: #171e27;
      --line: #2a3542;
      --text: #e8eef5;
      --muted: #8b9aab;
      --accent: #3d9cf0;
      --team: #1d2a22;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0; font-family: "Segoe UI", system-ui, sans-serif;
      background: var(--bg); color: var(--text); padding: 32px 20px 64px;
    }}
    main {{ max-width: 1100px; margin: 0 auto; }}
    h1 {{ font-size: 28px; font-weight: 650; margin: 0 0 8px; }}
    .sub {{ color: var(--muted); margin: 0 0 24px; line-height: 1.45; }}
    .card {{
      background: var(--card); border: 1px solid var(--line);
      border-radius: 12px; overflow: auto;
    }}
    table {{ border-collapse: collapse; width: 100%; min-width: 860px; }}
    th, td {{ padding: 10px 12px; border-bottom: 1px solid var(--line); }}
    thead th {{
      text-align: left; font-size: 12px; letter-spacing: .04em;
      text-transform: uppercase; color: var(--muted); font-weight: 600;
      background: #121820; position: sticky; top: 0;
    }}
    tbody th {{ text-align: left; font-weight: 600; white-space: nowrap; }}
    td {{ text-align: right; }}
    .days {{ color: var(--muted); width: 64px; }}
    tr.team {{ background: var(--team); font-weight: 650; }}
    .cell {{ position: relative; min-width: 72px; }}
    .bar {{
      position: absolute; left: 0; top: 50%; transform: translateY(-50%);
      height: 22px; background: rgba(61,156,240,.22); border-radius: 4px;
    }}
    .n {{ position: relative; }}
    .empty {{ color: #5c6b7a; }}
    .note {{
      margin-top: 20px; color: var(--muted); font-size: 14px; line-height: 1.55;
    }}
    ul {{ margin: 8px 0 0; padding-left: 18px; }}
  </style>
</head>
<body>
<main>
  <h1>Занятость по проектам</h1>
  <p class="sub">Таймшит 17.08.2026 — 21.09.2026. Каждый день сотрудника = 100%,
  задачи внутри дня делятся поровну (или по часам, если они указаны).</p>
  <div class="card">
    <table>
      <thead>
        <tr><th>Сотрудник</th><th>Дней</th>{thead}</tr>
      </thead>
      <tbody>
        {''.join(body)}
      </tbody>
    </table>
  </div>
  <div class="note">
    <strong>Легенда проектов.</strong>
    <ul>
      <li><b>Тануки</b> — синки, сегменты, отзывы, избранное/Mindbox, сториз, лояльность iOS, статические страницы, рубрикатор Android.</li>
      <li><b>Московский</b> — релизы, аппроутер, Strapi, фильтры, чеки, декларации, видео.</li>
      <li><b>ТМЦ</b> — очередь, реестр, загрузка файлов, справочники, аудиторы, Ollama.</li>
      <li><b>Агент</b> — UI-агент, драйверы, мультимодальность, сценарии.</li>
      <li><b>ГСП</b> — поиск/очередь/релиз/макет ГСП.</li>
      <li><b>Сапсан</b> — лендинг, статьи, SEO.</li>
      <li><b>Прочее</b> — налоги, счета, Кардекс, КТС, Мозен, внутренние созвоны без проекта.</li>
    </ul>
    Цифры оценочные: не у всех есть часы, часть задач без названия проекта размечена по контексту.
  </div>
</main>
</body>
</html>
"""


def to_csv(rows):
    cols = ["Сотрудник", "Дней"] + PROJECTS
    lines = [";".join(cols)]
    for row in rows:
        days = row["days"]
        days_s = str(int(days)) if float(days).is_integer() else str(days)
        pcts = [str(int(v)) if v else "0" for v in row["pcts"]]
        lines.append(";".join([row["name"], days_s] + pcts))
    return "\n".join(lines) + "\n"


def main():
    data = json.loads(SRC.read_text(encoding="utf-8"))
    n_msg = sum(1 for m in data.get("messages", []) if m.get("type") == "message")
    fte, days_count, team_fte, total_fte_days = compute()
    rows = build_rows(fte, days_count, team_fte, total_fte_days)

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "occupancy.md").write_text(to_md(rows), encoding="utf-8")
    (OUT_DIR / "occupancy.html").write_text(to_html(rows), encoding="utf-8")
    (OUT_DIR / "occupancy.csv").write_text(to_csv(rows), encoding="utf-8")

    print(f"messages: {n_msg}")
    print(to_md(rows))


if __name__ == "__main__":
    main()
