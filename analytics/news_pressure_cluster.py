"""Cluster News Pressure Radar headlines from SQLite.

This module is separate from the collector on purpose:

    collector -> raw SQLite
    cluster   -> derived topic runs

The first implementation is a local similarity graph over normalized tokens.
It is not a final semantic model, but it is deterministic, cheap, inspectable,
and good enough to validate the downstream pressure/reporting pipeline before
adding real embedding models.

Usage:
    python analytics/news_pressure_cluster.py --from 2026-06-22 --to 2026-07-21
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import requests


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = ROOT / "data" / "news_pressure" / "news_pressure.db"
OUT_DIR = ROOT / "data" / "news_pressure"
CLAIM_NUMBER_RE = re.compile(r"(?<![\wа-яА-ЯёЁ])(?:\d+[.,]?\d*|[IVX]{2,})(?![\wа-яА-ЯёЁ])", re.IGNORECASE)
NUMBER_WORDS = {
    "ноль": "0",
    "два": "2",
    "две": "2",
    "три": "3",
    "четыре": "4",
    "пять": "5",
    "шесть": "6",
    "семь": "7",
    "восемь": "8",
    "девять": "9",
    "десять": "10",
}
ROUTINE_TOKENS = {
    "праздник", "праздник", "отмеча", "календар",
    "гороскоп", "прогноз", "погод",
}


@dataclass(frozen=True)
class Article:
    article_id: str
    source: str
    published_date: str
    title: str
    url: str
    tokens: tuple[str, ...]


class UnionFind:
    def __init__(self, size: int) -> None:
        self.parent = list(range(size))
        self.rank = [0] * size

    def find(self, item: int) -> int:
        while self.parent[item] != item:
            self.parent[item] = self.parent[self.parent[item]]
            item = self.parent[item]
        return item

    def union(self, left: int, right: int) -> None:
        root_left = self.find(left)
        root_right = self.find(right)
        if root_left == root_right:
            return
        if self.rank[root_left] < self.rank[root_right]:
            self.parent[root_left] = root_right
        elif self.rank[root_left] > self.rank[root_right]:
            self.parent[root_right] = root_left
        else:
            self.parent[root_right] = root_left
            self.rank[root_left] += 1


def clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build semantic-like headline clusters from news_pressure.db")
    parser.add_argument("--db", default=str(DEFAULT_DB_PATH))
    parser.add_argument("--from", dest="from_date", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--to", dest="to_date", required=True, help="End date YYYY-MM-DD")
    parser.add_argument("--min-cluster-size", type=int, default=5)
    parser.add_argument("--cluster-limit", type=int, default=20)
    parser.add_argument("--min-shared-tokens", type=int, default=2)
    parser.add_argument("--jaccard-threshold", type=float, default=0.24)
    parser.add_argument("--max-token-df", type=int, default=320)
    parser.add_argument("--label-mode", choices=["heuristic", "deepseek"], default="heuristic")
    parser.add_argument("--llm-model", default="deepseek-chat")
    parser.add_argument("--report-mode", choices=["auto", "daily", "weekly", "monthly", "history"], default="auto")
    return parser.parse_args()


def load_articles(db_path: Path, start: date, end: date) -> list[Article]:
    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        """
        SELECT h.id, h.source, h.published_date, h.title, h.url, n.token_json
        FROM headline_snapshots h
        JOIN normalized_titles n ON n.article_id = h.id
        WHERE h.published_date BETWEEN ? AND ?
        ORDER BY h.published_date, h.source, h.id
        """,
        (start.isoformat(), end.isoformat()),
    ).fetchall()
    conn.close()

    articles = []
    for article_id, source, published_date, title, url, token_json in rows:
        tokens = tuple(dict.fromkeys(json.loads(token_json)))
        if tokens:
            articles.append(Article(article_id, source, published_date, title, url, tokens))
    return articles


def build_similarity_components(
    articles: list[Article],
    min_shared_tokens: int,
    jaccard_threshold: float,
    max_token_df: int,
) -> list[list[Article]]:
    postings: dict[str, list[int]] = defaultdict(list)
    for idx, article in enumerate(articles):
        for token in set(article.tokens):
            postings[token].append(idx)

    usable_postings = {
        token: ids
        for token, ids in postings.items()
        if 2 <= len(ids) <= max_token_df
    }

    pair_counts: Counter[tuple[int, int]] = Counter()
    for ids in usable_postings.values():
        for pos, left in enumerate(ids):
            for right in ids[pos + 1:]:
                pair_counts[(left, right)] += 1

    uf = UnionFind(len(articles))
    token_sets = [set(article.tokens) for article in articles]
    for (left, right), shared in pair_counts.items():
        if shared < min_shared_tokens:
            continue
        union_size = len(token_sets[left] | token_sets[right])
        jaccard = shared / max(1, union_size)
        if jaccard >= jaccard_threshold or shared >= min_shared_tokens + 2:
            uf.union(left, right)

    components: dict[int, list[Article]] = defaultdict(list)
    for idx, article in enumerate(articles):
        components[uf.find(idx)].append(article)
    return list(components.values())


def pressure_metrics(cluster: list[Article], all_articles: list[Article], start: date, end: date) -> dict[str, float | int | str]:
    full_days = max(1, (end - start).days + 1)
    all_sources = {article.source for article in all_articles}
    last_week_start = max(start, end - timedelta(days=6))
    prev_week_start = max(start, last_week_start - timedelta(days=7))
    previous_history_end = last_week_start - timedelta(days=1)

    last_total = sum(1 for article in all_articles if date.fromisoformat(article.published_date) >= last_week_start)
    prev_total = sum(
        1 for article in all_articles
        if prev_week_start <= date.fromisoformat(article.published_date) < last_week_start
    )
    previous_history_total = sum(
        1 for article in all_articles
        if start <= date.fromisoformat(article.published_date) <= previous_history_end
    )
    last_count = sum(1 for article in cluster if date.fromisoformat(article.published_date) >= last_week_start)
    prev_count = sum(
        1 for article in cluster
        if prev_week_start <= date.fromisoformat(article.published_date) < last_week_start
    )
    previous_history_count = sum(
        1 for article in cluster
        if start <= date.fromisoformat(article.published_date) <= previous_history_end
    )

    velocity = (last_count / max(1, last_total)) - (prev_count / max(1, prev_total))
    volume = len(cluster)
    active_dates = {article.published_date for article in cluster}
    persistence = len(active_dates)
    source_spread = len({article.source for article in cluster})
    last_rate = last_count / 7
    prev_rate = prev_count / 7
    velocity_ratio = (last_rate - prev_rate) / max(1.0, prev_rate)
    recent_share = last_count / max(1, volume)
    historical_share = previous_history_count / max(1, previous_history_total)
    current_share = last_count / max(1, last_total)
    novelty = clamp((current_share - historical_share) / max(0.001, historical_share)) if historical_share else min(1.0, recent_share)
    silence_gap = 1.0 - (source_spread / max(1, len(all_sources)))
    volume_score = clamp(math.log1p(volume) / math.log1p(max(10, len(all_articles) * 0.015)))
    velocity_score = clamp(velocity_ratio)
    persistence_score = clamp(persistence / full_days)
    source_spread_score = clamp(source_spread / max(1, len(all_sources)))
    novelty_score = clamp(novelty)
    token_set = {token for article in cluster for token in article.tokens}
    routine_hit = bool(token_set & ROUTINE_TOKENS)
    noise_penalty = 0.0
    if source_spread == 1 and persistence >= 14:
        noise_penalty += 0.30
    if source_spread == 1 and volume >= 50:
        noise_penalty += 0.20
    if routine_hit and source_spread == 1:
        noise_penalty += 0.20
    noise_penalty = clamp(noise_penalty, 0.0, 0.75)
    anomaly_score = clamp(silence_gap * max(velocity_score, 0.15) * (1.0 - min(0.6, noise_penalty)))
    pressure_score = (
        0.25 * volume_score
        + 0.25 * velocity_score
        + 0.20 * source_spread_score
        + 0.15 * persistence_score
        + 0.15 * novelty_score
    ) * (1.0 - noise_penalty)
    return {
        "volume": volume,
        "velocity": velocity,
        "velocity_ratio": velocity_ratio,
        "persistence": persistence,
        "source_spread": source_spread,
        "novelty": novelty,
        "silence_gap": silence_gap,
        "volume_score": volume_score,
        "velocity_score": velocity_score,
        "persistence_score": persistence_score,
        "source_spread_score": source_spread_score,
        "novelty_score": novelty_score,
        "noise_penalty": noise_penalty,
        "anomaly_score": anomaly_score,
        "pressure_score": pressure_score,
        "first_seen": min(active_dates),
        "last_seen": max(active_dates),
    }


def cluster_label(cluster: list[Article], global_df: Counter[str]) -> str:
    cluster_df: Counter[str] = Counter()
    for article in cluster:
        cluster_df.update(set(article.tokens))

    scored = []
    total_docs = max(1, sum(global_df.values()))
    for token, count in cluster_df.items():
        idf = math.log(1 + total_docs / max(1, global_df[token]))
        scored.append((count * idf, token))
    return " / ".join(token for _, token in sorted(scored, reverse=True)[:4])


def build_clusters(
    articles: list[Article],
    start: date,
    end: date,
    min_cluster_size: int,
    cluster_limit: int,
    min_shared_tokens: int,
    jaccard_threshold: float,
    max_token_df: int,
) -> list[dict[str, object]]:
    components = build_similarity_components(articles, min_shared_tokens, jaccard_threshold, max_token_df)
    global_df: Counter[str] = Counter()
    for article in articles:
        global_df.update(set(article.tokens))

    clusters = []
    for component in components:
        if len(component) < min_cluster_size:
            continue
        metrics = pressure_metrics(component, articles, start, end)
        representatives = sorted(component, key=lambda article: article.published_date, reverse=True)[:8]
        clusters.append({
            **metrics,
            "label": cluster_label(component, global_df),
            "articles": component,
            "representatives": representatives,
        })

    return sorted(clusters, key=lambda item: float(item["pressure_score"]), reverse=True)[:cluster_limit]


def save_topic_run(
    db_path: Path,
    clusters: list[dict[str, object]],
    start: date,
    end: date,
    params: dict[str, object],
) -> str:
    conn = sqlite3.connect(db_path)
    run_id = "topic_graph_" + datetime.now(UTC).strftime("%Y%m%dT%H%M%S%fZ")
    now = datetime.now(UTC).isoformat(timespec="seconds")
    conn.execute(
        """
        INSERT INTO topic_runs(run_id, period_from, period_to, method, params_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (run_id, start.isoformat(), end.isoformat(), "local_similarity_graph_v1", json.dumps(params), now),
    )
    for idx, cluster in enumerate(clusters, start=1):
        topic_id = f"{run_id}_{idx:03d}"
        representative_rows = [
            {
                "date": article.published_date,
                "source": article.source,
                "title": article.title,
                "url": article.url,
            }
            for article in cluster["representatives"]
        ]
        conn.execute(
            """
            INSERT INTO topics(
                topic_id, run_id, label, summary, pressure_score, volume, velocity,
                persistence, source_spread, novelty, silence_gap, representative_json
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                topic_id,
                run_id,
                cluster["label"],
                "Local similarity graph over normalized headline tokens.",
                cluster["pressure_score"],
                cluster["volume"],
                cluster["velocity"],
                cluster["persistence"],
                cluster["source_spread"],
                cluster["novelty"],
                cluster["silence_gap"],
                json.dumps(representative_rows, ensure_ascii=False),
            ),
        )
        for article in cluster["articles"]:
            conn.execute(
                "INSERT OR IGNORE INTO article_topics(article_id, topic_id, confidence) VALUES (?, ?, ?)",
                (article.article_id, topic_id, 0.65),
            )
    conn.commit()
    conn.close()
    return run_id


def heuristic_interpretation(cluster: dict[str, object]) -> dict[str, str]:
    label = str(cluster["label"])
    volume = int(cluster["volume"])
    source_spread = int(cluster["source_spread"])
    persistence = int(cluster["persistence"])
    velocity_score = float(cluster.get("velocity_score", 0.0))

    if velocity_score >= 0.35:
        pressure = "растущее"
    elif persistence >= 14 and source_spread >= 3:
        pressure = "устойчивое"
    else:
        pressure = "фоновое"

    return {
        "human_label": label.replace(" / ", " + "),
        "pressure_type": pressure,
        "why_it_matters": (
            f"Сюжет держится {persistence} дн., встречается в {source_spread} источн. "
            f"и включает {volume} заголовков за период."
        ),
        "watch_next": "Следить за повторением темы в нескольких источниках и изменением недельной скорости.",
    }


def deepseek_chat(messages: list[dict[str, str]], model: str) -> str:
    api_key = os.environ.get("DEEPSEEK_API_KEY") or os.environ.get("deep_seek_token")
    if not api_key:
        raise RuntimeError("DEEPSEEK_API_KEY/deep_seek_token is not set")

    response = requests.post(
        "https://api.deepseek.com/chat/completions",
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={"model": model, "messages": messages, "temperature": 0.2},
        timeout=60,
    )
    response.raise_for_status()
    data = response.json()
    return data["choices"][0]["message"]["content"].strip()


def parse_json_object(text: str) -> dict[str, str]:
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise ValueError("No JSON object in LLM response")
    data = json.loads(text[start:end + 1])
    return {str(key): str(value) for key, value in data.items()}


def claim_numbers(text: str) -> set[str]:
    normalized = text.lower().replace(",", ".")
    numbers = set(CLAIM_NUMBER_RE.findall(normalized))
    words = re.findall(r"[а-яё]+", normalized)
    for word in words:
        if word in NUMBER_WORDS:
            numbers.add(NUMBER_WORDS[word])
    return numbers


def number_supported(number: str, evidence_numbers: set[str]) -> bool:
    if number in evidence_numbers:
        return True
    try:
        value = float(number)
    except ValueError:
        return False
    for evidence in evidence_numbers:
        try:
            evidence_value = float(evidence)
        except ValueError:
            continue
        if value == evidence_value:
            return True
        if value.is_integer() and int(value) == int(evidence_value):
            return True
        if value.is_integer() and abs(value - evidence_value) <= 1.5:
            return True
        if value.is_integer() and int(value) % 10 == 0 and value <= evidence_value < value + 10:
            return True
    return False


def llm_interpretation(cluster: dict[str, object], model: str) -> dict[str, str]:
    representatives = cluster["representatives"][:10]
    headline_lines = [
        f"- {article.published_date} / {article.source}: {article.title}"
        for article in representatives
    ]
    prompt = "\n".join([
        "Ты аналитик новостной повестки. По группе заголовков дай краткую интерпретацию.",
        "Не выдумывай факты вне заголовков. Пиши по-русски.",
        "Не добавляй числа, даты, причинно-следственные связи и итоги, которых нет в заголовках.",
        "Если заголовки не доказывают вывод, формулируй осторожно: 'заголовки указывают', 'виден поток сообщений'.",
        "Верни только JSON с ключами:",
        "human_label, pressure_type, why_it_matters, watch_next.",
        "",
        f"Техническая метка: {cluster['label']}",
        f"volume={cluster['volume']}, velocity={float(cluster['velocity']):+.2%}, "
        f"persistence={cluster['persistence']}, source_spread={cluster['source_spread']}",
        "",
        "Заголовки:",
        *headline_lines,
    ])
    text = deepseek_chat(
        [
            {"role": "system", "content": "Ты помогаешь строить краткий отчёт News Pressure Radar."},
            {"role": "user", "content": prompt},
        ],
        model=model,
    )
    parsed = parse_json_object(text)
    fallback = heuristic_interpretation(cluster)
    return {**fallback, **parsed}


def quality_flags(cluster: dict[str, object], report_mode: str) -> list[str]:
    interp = cluster.get("interpretation", {})
    evidence_text = " ".join(
        f"{article.published_date} {article.title}"
        for article in cluster["articles"]
    ).lower()
    interp_text = " ".join(
        str(interp.get(key, ""))
        for key in ("human_label", "why_it_matters", "watch_next")
    ).lower()
    flags = []

    evidence_numbers = claim_numbers(evidence_text)
    for number in sorted(claim_numbers(interp_text)):
        if not number_supported(number, evidence_numbers):
            flags.append(f"unsupported_number:{number}")

    if int(cluster["source_spread"]) == 1 and int(cluster["persistence"]) >= 14:
        flags.append("single_source_persistent")

    if int(cluster["volume"]) >= 75 and int(cluster["source_spread"]) == 1:
        flags.append("single_source_high_volume")

    if report_mode != "daily" and int(cluster["volume"]) <= 10 and int(cluster["source_spread"]) <= 2:
        flags.append("thin_cluster")

    return flags


def apply_quality_gate(clusters: list[dict[str, object]], report_mode: str) -> None:
    for cluster in clusters:
        flags = quality_flags(cluster, report_mode)
        cluster["quality_flags"] = flags
        if flags:
            interp = dict(cluster.get("interpretation", {}))
            interp["quality_status"] = "needs_review"
            interp["quality_flags"] = ", ".join(flags)
            cluster["interpretation"] = interp


def interpret_clusters(clusters: list[dict[str, object]], mode: str, model: str) -> None:
    for cluster in clusters:
        if mode == "deepseek":
            try:
                cluster["interpretation"] = llm_interpretation(cluster, model)
                continue
            except Exception as exc:
                cluster["interpretation"] = {
                    **heuristic_interpretation(cluster),
                    "llm_error": f"{type(exc).__name__}: {exc}",
                }
                continue
        cluster["interpretation"] = heuristic_interpretation(cluster)


def update_topic_interpretations(db_path: Path, topic_run_id: str, clusters: list[dict[str, object]]) -> None:
    conn = sqlite3.connect(db_path)
    for idx, cluster in enumerate(clusters, start=1):
        topic_id = f"{topic_run_id}_{idx:03d}"
        interp = cluster.get("interpretation", {})
        summary = json.dumps(
            {
                **interp,
                "score_components": {
                    "volume": cluster.get("volume_score"),
                    "velocity": cluster.get("velocity_score"),
                    "source_spread": cluster.get("source_spread_score"),
                    "persistence": cluster.get("persistence_score"),
                    "novelty": cluster.get("novelty_score"),
                    "noise_penalty": cluster.get("noise_penalty"),
                    "anomaly": cluster.get("anomaly_score"),
                },
            },
            ensure_ascii=False,
        )
        conn.execute(
            "UPDATE topics SET label=?, summary=? WHERE topic_id=?",
            (interp.get("human_label", cluster["label"]), summary, topic_id),
        )
    conn.commit()
    conn.close()


def write_report(path: Path, clusters: list[dict[str, object]], start: date, end: date, topic_run_id: str) -> None:
    lines = [
        "# News Pressure Similarity Cluster Report",
        "",
        f"Period: `{start.isoformat()}` to `{end.isoformat()}`",
        f"Topic run: `{topic_run_id}`",
        "Method: `local_similarity_graph_v1`",
        "",
    ]
    for idx, cluster in enumerate(clusters, start=1):
        interp = cluster.get("interpretation", {})
        human_label = interp.get("human_label", cluster["label"])
        lines += [
            f"## {idx}. {human_label}",
            "",
            f"- technical_label: `{cluster['label']}`",
            f"- pressure_type: `{interp.get('pressure_type', 'n/a')}`",
            f"- pressure_score: `{float(cluster['pressure_score']):.4f}`",
            f"- volume: `{cluster['volume']}`",
            f"- velocity: `{float(cluster['velocity']):+.2%}`",
            f"- velocity_ratio: `{float(cluster.get('velocity_ratio', 0.0)):+.2f}`",
            f"- persistence: `{cluster['persistence']}` days",
            f"- source_spread: `{cluster['source_spread']}`",
            f"- silence_gap: `{float(cluster['silence_gap']):.2f}`",
            f"- score_components: volume `{float(cluster.get('volume_score', 0.0)):.2f}`, "
            f"velocity `{float(cluster.get('velocity_score', 0.0)):.2f}`, "
            f"persistence `{float(cluster.get('persistence_score', 0.0)):.2f}`, "
            f"sources `{float(cluster.get('source_spread_score', 0.0)):.2f}`, "
            f"novelty `{float(cluster.get('novelty_score', 0.0)):.2f}`, "
            f"noise_penalty `{float(cluster.get('noise_penalty', 0.0)):.2f}`, "
            f"anomaly `{float(cluster.get('anomaly_score', 0.0)):.2f}`",
            f"- quality_status: `{interp.get('quality_status', 'ok')}`",
            f"- why_it_matters: {interp.get('why_it_matters', 'n/a')}",
            f"- watch_next: {interp.get('watch_next', 'n/a')}",
            "- representatives:",
        ]
        if interp.get("quality_flags"):
            lines.append(f"- quality_flags: `{interp['quality_flags']}`")
        if interp.get("llm_error"):
            lines.append(f"- llm_error: `{interp['llm_error']}`")
        for article in cluster["representatives"]:
            lines.append(f"  - `{article.published_date}` `{article.source}`: {article.title} ({article.url})")
        lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def write_digest(path: Path, clusters: list[dict[str, object]], start: date, end: date, topic_run_id: str) -> None:
    synchronized = [
        cluster for cluster in clusters
        if int(cluster["source_spread"]) >= 4
    ]
    rising = sorted(
        [cluster for cluster in clusters if float(cluster["velocity"]) > 0],
        key=lambda cluster: float(cluster["velocity"]),
        reverse=True,
    )
    persistent = sorted(
        [cluster for cluster in clusters if int(cluster["persistence"]) >= 14],
        key=lambda cluster: int(cluster["persistence"]),
        reverse=True,
    )
    anomalies = [
        cluster for cluster in clusters
        if float(cluster["silence_gap"]) >= 0.4 or str(cluster.get("interpretation", {}).get("pressure_type", "")).lower() in {
            "информационный фон", "routine_event"
        }
    ]

    def title(cluster: dict[str, object]) -> str:
        return str(cluster.get("interpretation", {}).get("human_label", cluster["label"]))

    def one_line(cluster: dict[str, object]) -> str:
        interp = cluster.get("interpretation", {})
        return (
            f"- **{title(cluster)}**: score `{float(cluster['pressure_score']):.3f}`, "
            f"{cluster['volume']} заголовков, {cluster['source_spread']} источн., "
            f"{cluster['persistence']} дн. {interp.get('why_it_matters', '')}"
        ).strip()

    lines = [
        "# Пульс Повестки",
        "",
        f"Период: `{start.isoformat()}` — `{end.isoformat()}`",
        f"Topic run: `{topic_run_id}`",
        "Метод: `local_similarity_graph_v1 + LLM labels`",
        "",
        "## Главный Сигнал",
        one_line(clusters[0]) if clusters else "- Нет кластеров.",
        "",
        "## Что Усилилось",
    ]
    lines.extend(one_line(cluster) for cluster in rising[:5])
    if not rising:
        lines.append("- Явных растущих кластеров нет.")

    lines += ["", "## Синхронные Сюжеты"]
    lines.extend(one_line(cluster) for cluster in synchronized[:5])
    if not synchronized:
        lines.append("- Нет кластеров, подхваченных 4+ источниками.")

    lines += ["", "## Устойчивый Фон"]
    lines.extend(one_line(cluster) for cluster in persistent[:5])
    if not persistent:
        lines.append("- Нет долгоживущих кластеров.")

    lines += ["", "## Аномалии И Шум"]
    lines.extend(one_line(cluster) for cluster in anomalies[:5])
    if not anomalies:
        lines.append("- Явных одноисточниковых или шумовых кластеров нет.")

    lines += ["", "## Что Смотреть Дальше"]
    for cluster in clusters[:8]:
        watch_next = cluster.get("interpretation", {}).get("watch_next")
        if watch_next:
            lines.append(f"- **{title(cluster)}**: {watch_next}")

    lines += ["", "## Аудит"]
    for idx, cluster in enumerate(clusters[:10], start=1):
        lines.append(f"- `{idx}` {title(cluster)} -> technical `{cluster['label']}`")

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def infer_report_mode(start: date, end: date, requested: str) -> str:
    if requested != "auto":
        return requested
    days = (end - start).days + 1
    if days <= 1:
        return "daily"
    if days <= 7:
        return "weekly"
    if days <= 31:
        return "monthly"
    return "history"


def write_digest_v2(
    path: Path,
    clusters: list[dict[str, object]],
    start: date,
    end: date,
    topic_run_id: str,
    report_mode: str,
) -> None:
    def title(cluster: dict[str, object]) -> str:
        return str(cluster.get("interpretation", {}).get("human_label", cluster["label"]))

    def cluster_key(cluster: dict[str, object]) -> str:
        return str(cluster["label"])

    def describe(cluster: dict[str, object]) -> str:
        interp = cluster.get("interpretation", {})
        quality = ""
        if interp.get("quality_status") == "needs_review":
            quality = f"\n  Проверить: {interp.get('quality_flags', 'needs_review')}."
        metrics = (
            f"volume {float(cluster.get('volume_score', 0.0)):.2f}, "
            f"velocity {float(cluster.get('velocity_score', 0.0)):.2f}, "
            f"sources {float(cluster.get('source_spread_score', 0.0)):.2f}, "
            f"persistence {float(cluster.get('persistence_score', 0.0)):.2f}, "
            f"novelty {float(cluster.get('novelty_score', 0.0)):.2f}, "
            f"noise {float(cluster.get('noise_penalty', 0.0)):.2f}"
        )
        return (
            f"- **{title(cluster)}**: score `{float(cluster['pressure_score']):.3f}`, "
            f"{cluster['volume']} заголовков, {cluster['source_spread']} источн., "
            f"{cluster['persistence']} дн.\n"
            f"  Компоненты: {metrics}.\n"
            f"  {interp.get('why_it_matters', '')}"
            f"{quality}"
        ).rstrip()

    def take(candidates: list[dict[str, object]], used: set[str], limit: int) -> list[dict[str, object]]:
        out = []
        for cluster in candidates:
            key = cluster_key(cluster)
            if key in used:
                continue
            used.add(key)
            out.append(cluster)
            if len(out) >= limit:
                break
        return out

    by_score = sorted(clusters, key=lambda cluster: float(cluster["pressure_score"]), reverse=True)
    used: set[str] = set()
    if report_mode == "daily":
        main_candidates = sorted(
            clusters,
            key=lambda cluster: (
                float(cluster.get("velocity_score", 0.0)),
                float(cluster.get("novelty_score", 0.0)),
                float(cluster["pressure_score"]),
            ),
            reverse=True,
        )
    else:
        main_candidates = by_score
    main = take(main_candidates, used, 1)
    rising = take(
        sorted(
            [cluster for cluster in clusters if float(cluster.get("velocity_score", 0.0)) > 0],
            key=lambda cluster: float(cluster.get("velocity_score", 0.0)),
            reverse=True,
        ),
        used,
        4,
    )
    sync_threshold = 3 if report_mode in {"daily", "weekly"} else 4
    synchronized = take(
        sorted(
            [cluster for cluster in clusters if int(cluster["source_spread"]) >= sync_threshold],
            key=lambda cluster: (int(cluster["source_spread"]), float(cluster["pressure_score"])),
            reverse=True,
        ),
        used,
        4,
    )
    persistent = take(
        sorted(
            [
                cluster for cluster in clusters
                if int(cluster["persistence"]) >= 14 and not cluster.get("quality_flags")
            ],
            key=lambda cluster: (int(cluster["persistence"]), float(cluster["pressure_score"])),
            reverse=True,
        ),
        used,
        4,
    )
    anomaly = take(
        sorted(
            [
                cluster for cluster in clusters
                if float(cluster["silence_gap"]) >= 0.4
                or cluster.get("quality_flags")
                or float(cluster.get("anomaly_score", 0.0)) >= 0.25
                or str(cluster.get("interpretation", {}).get("pressure_type", "")).lower()
                in {"информационный фон", "routine_event"}
            ],
            key=lambda cluster: (float(cluster.get("anomaly_score", 0.0)), float(cluster["silence_gap"])),
            reverse=True,
        ),
        used,
        4,
    )

    if report_mode == "daily":
        report_title = "Пульс Дня"
        method_note = "daily impulse mode"
        sections = [
            ("Главный Импульс", main),
            ("Что Вспыхнуло", rising),
            ("Синхронно Подхватили", synchronized),
            ("Аномалии И Шум", anomaly),
        ]
    elif report_mode == "weekly":
        report_title = "Пульс Недели"
        method_note = "weekly development mode"
        sections = [
            ("Главный Сюжет Недели", main),
            ("Что Ускорилось", rising),
            ("Синхронные Сюжеты", synchronized),
            ("Устойчивый Фон Недели", persistent),
            ("Аномалии И Шум", anomaly),
        ]
    elif report_mode == "monthly":
        report_title = "Пульс Месяца"
        method_note = "monthly background mode"
        sections = [
            ("Главный Сигнал", main),
            ("Что Усилилось", rising),
            ("Синхронные Сюжеты", synchronized),
            ("Устойчивый Фон", persistent),
            ("Аномалии И Шум", anomaly),
        ]
    else:
        report_title = "Пульс Повестки: 90 Дней"
        method_note = "90-day history mode"
        sections = [
            ("Главное Давление Периода", main),
            ("Новые Или Усилившиеся Сюжеты", rising),
            ("Синхронные Сюжеты", synchronized),
            ("Долгий Фон", persistent),
            ("Аномалии И Шум", anomaly),
        ]

    summary_lines = []
    if main:
        if report_mode == "daily":
            summary_lines.append(f"Главный импульс дня: {title(main[0])}.")
        elif report_mode == "weekly":
            summary_lines.append(f"Главный сюжет недели: {title(main[0])}.")
        elif report_mode == "monthly":
            summary_lines.append(f"Главное давление месяца: {title(main[0])}.")
        else:
            summary_lines.append(f"Главное давление 90-дневного окна: {title(main[0])}.")
    if rising:
        label = "Свежие всплески" if report_mode == "daily" else "Самое заметное усиление"
        summary_lines.append(label + ": " + "; ".join(title(cluster) for cluster in rising[:2]) + ".")
    if synchronized:
        summary_lines.append("Широко синхронизированы между источниками: " + "; ".join(title(cluster) for cluster in synchronized[:2]) + ".")
    if persistent:
        if report_mode != "daily":
            summary_lines.append("Дольше всего держатся в фоне: " + "; ".join(title(cluster) for cluster in persistent[:2]) + ".")
    if anomaly:
        summary_lines.append("Шум/аномалии для контроля: " + "; ".join(title(cluster) for cluster in anomaly[:2]) + ".")

    lines = [
        f"# {report_title}",
        "",
        f"Период: `{start.isoformat()}` — `{end.isoformat()}`",
        f"Topic run: `{topic_run_id}`",
        f"Режим: `{report_mode}`",
        f"Метод: `local_similarity_graph_v1 + LLM labels + section-dedup + pressure_score_v2 + {method_note}`",
        "",
        "## Executive Summary",
    ]
    lines.extend(f"- {line}" for line in summary_lines[:7])
    if not summary_lines:
        lines.append("- Недостаточно кластеров для summary.")

    for section_name, section_clusters in sections:
        if report_mode == "daily" and not section_clusters:
            continue
        lines += ["", f"## {section_name}"]
        if section_clusters:
            lines.extend(describe(cluster) for cluster in section_clusters)
        else:
            lines.append("- Нет кластеров для этого раздела.")

    lines += ["", "## Что Смотреть Дальше"]
    watch_candidates = main + rising + synchronized + persistent
    watch_used = set()
    for cluster in watch_candidates:
        key = cluster_key(cluster)
        if key in watch_used:
            continue
        watch_used.add(key)
        watch_next = cluster.get("interpretation", {}).get("watch_next")
        if watch_next:
            lines.append(f"- **{title(cluster)}**: {watch_next}")

    lines += ["", "## Аудит"]
    for idx, cluster in enumerate(clusters[:15], start=1):
        flags = []
        if cluster in main:
            flags.append("main")
        if cluster in rising:
            flags.append("rising")
        if cluster in synchronized:
            flags.append("synchronized")
        if cluster in persistent:
            flags.append("persistent")
        if cluster in anomaly:
            flags.append("anomaly/noise")
        if cluster.get("quality_flags"):
            flags.append("needs_review")
        flag_text = ", ".join(flags) if flags else "unassigned"
        lines.append(f"- `{idx}` {title(cluster)} -> `{cluster['label']}` [{flag_text}]")

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    args = parse_args()
    db_path = Path(args.db)
    start = date.fromisoformat(args.from_date)
    end = date.fromisoformat(args.to_date)
    report_mode = infer_report_mode(start, end, args.report_mode)
    articles = load_articles(db_path, start, end)
    clusters = build_clusters(
        articles,
        start,
        end,
        min_cluster_size=args.min_cluster_size,
        cluster_limit=args.cluster_limit,
        min_shared_tokens=args.min_shared_tokens,
        jaccard_threshold=args.jaccard_threshold,
        max_token_df=args.max_token_df,
    )
    params = {
        "min_cluster_size": args.min_cluster_size,
        "cluster_limit": args.cluster_limit,
        "min_shared_tokens": args.min_shared_tokens,
        "jaccard_threshold": args.jaccard_threshold,
        "max_token_df": args.max_token_df,
        "article_count": len(articles),
        "label_mode": args.label_mode,
        "llm_model": args.llm_model if args.label_mode == "deepseek" else None,
        "score_version": "pressure_score_v2",
        "report_mode": report_mode,
    }
    interpret_clusters(clusters, args.label_mode, args.llm_model)
    apply_quality_gate(clusters, report_mode)
    topic_run_id = save_topic_run(db_path, clusters, start, end, params)
    update_topic_interpretations(db_path, topic_run_id, clusters)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    report_path = OUT_DIR / f"semantic_cluster_pulse_{start.isoformat()}_{end.isoformat()}.md"
    digest_path = OUT_DIR / f"agenda_pulse_{start.isoformat()}_{end.isoformat()}.md"
    digest_v2_path = OUT_DIR / f"agenda_pulse_v2_{start.isoformat()}_{end.isoformat()}.md"
    write_report(report_path, clusters, start, end, topic_run_id)
    write_digest(digest_path, clusters, start, end, topic_run_id)
    write_digest_v2(digest_v2_path, clusters, start, end, topic_run_id, report_mode)
    print(f"Articles: {len(articles)}")
    print(f"Clusters: {len(clusters)}")
    print(f"Topic run: {topic_run_id}")
    print(f"Report: {report_path}")
    print(f"Digest: {digest_path}")
    print(f"Digest v2: {digest_v2_path}")
    for cluster in clusters[:10]:
        print(f"- {cluster['label']}: volume={cluster['volume']} score={float(cluster['pressure_score']):.4f}")


if __name__ == "__main__":
    main()
