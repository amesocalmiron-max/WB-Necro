# WB Necromancer v2 — Жёсткий E2E-чеклист релизной готовности (A..M)

Дата: 2026-02-23  
Ветка: `work`  
Цель: формально закрыть проверку готовности пайплайна A..M в формате **«требование → конкретная проверка → статус»**.

## Шкала статусов
- **PASS** — проверка выполнена и подтверждена.
- **WARN** — проверка валидна по методике, но ограничена окружением (например, нужен боевой WB прогон).
- **FAIL** — явное несоответствие требованию.

---

## 0) Глобальные инварианты (для всех стадий)

| Требование | Конкретная проверка | Статус |
|---|---|---|
| Пайплайн должен быть A..M | `STAGE_ORDER = list("ABCDEFGHIJKLM")` и список стадий печатается CLI `--list-stages` | PASS |
| Двухрыночная модель (phone/type) | Наличие `CLUSTERS = ("phone", "type")` и стадий C/D/H/K/L, которые работают с кластерами | PASS |
| LLM не источник данных, а формулировка по фактам | В Stage M в LLM prompt явно прописано «не выдумывай числа и факты» и передаются агрегированные facts | PASS |
| Артефакты канонично в JSONL/.wb_cache + XLSX/HTML | Контракты `STAGE_IO` + генерация `WB_NECROMANCER_REPORT.xlsx/.html` | PASS |

---

## 1) Чеклист по стадиям A..M

| Stage | Требование | Конкретная проверка | Статус |
|---|---|---|---|
| A | INPUT + manifest scope/run_id | В `STAGE_IO[A]` выход `run_manifest.json`; в раннере `run_stage("A") -> stage_A_manifest(...)` | PASS |
| B | OWN fetch WB-only + deep card | В `STAGE_IO[B]` выход `own_norm.jsonl`; в раннере `run_stage("B") -> stage_B_own_fetch(...)` | PASS |
| C | Intent extraction (phone model, must/ban, karma) | В `STAGE_META[C]` описание intent; в раннере `run_stage("C") -> stage_C_intent(...)` | PASS |
| D | Query build rules-first + optional LLM | В `STAGE_META[D]` `llm_flag=use_llm_d`; в раннере `stage_D_queries(... use_llm=args.use_llm_d ...)` | PASS |
| E | SERP validation (WB) | В `STAGE_IO[E]` выход `queries_valid.jsonl`; раннер вызывает `stage_E_serp(...)` с pass-rate порогами | PASS |
| F | Competitor pool | В `STAGE_IO[F]` выход `competitor_pool.jsonl`; раннер вызывает `stage_F_pool(...)` | PASS |
| G | Competitor lite fetch (WB) | В `STAGE_IO[G]` выход `competitor_lite.jsonl`; раннер вызывает `stage_G_lite(...)` | PASS |
| H | Relevance filter rules-first + optional LLM | В `STAGE_META[H]` `llm_flag=use_llm_h`; раннер вызывает `stage_H_relevance(... use_llm=args.use_llm_h ...)` | PASS |
| I | Market pulse/reviews | В `STAGE_IO[I]` выход `market_pulse.jsonl`; раннер вызывает `stage_I_pulse(...)` | PASS |
| J | Supply/Structure aggregation | В `STAGE_IO[J]` выход `supply_structure.jsonl`; раннер вызывает `stage_J_supply(...)` | PASS |
| K | Cluster verdicts ALIVE/SLOW/DEAD | В `STAGE_IO[K]` выход `cluster_verdicts.jsonl`; раннер вызывает `stage_K_cluster_verdicts(...)` | PASS |
| L | Final decision matrix + verdict/backlog | В `STAGE_IO[L]` выход `decisions.jsonl`; раннер вызывает `stage_L_decisions(...)` | PASS |
| M | Human-friendly report + optional LLM exec summary | `write_reports(...)` генерирует XLSX/HTML; Stage M содержит evidence block, фильтры, top flags/tasks, optional `exec_summary.json` | PASS |

---

## 2) E2E запускные проверки (выполненные)

| Проверка | Команда/метод | Статус |
|---|---|---|
| Синтаксис test-скриптов | `python -m py_compile wb_necromancer_v2_test.py wb_necromancer_v2_test_v1_1.py` | PASS |
| CLI доступность стадий (test) | `python wb_necromancer_v2_test.py --list-stages` | PASS |
| CLI доступность пресетов моделей (test) | `python wb_necromancer_v2_test.py --list-models` | PASS |
| CLI доступность стадий (test_v1_1) | `python wb_necromancer_v2_test_v1_1.py --list-stages` | PASS |
| CLI доступность пресетов моделей (test_v1_1) | `python wb_necromancer_v2_test_v1_1.py --list-models` | PASS |
| Smoke Stage M (синтетические artifacts) | Python harness: создаёт `run_manifest.json`, `intent.jsonl`, `cluster_verdicts.jsonl`, `decisions.jsonl`, вызывает `write_reports(...)`, валидирует HTML/XLSX | PASS |
| Полный боевой прогон A..M на 64 SKU с WB API | Требуется входной XLSX+сетевой контур WB, в этом цикле не запускался | WARN |

---

## 3) Формальный verdict под релиз

- **Release readiness (code/architecture): PASS** — структура A..M, двухрыночная логика, Stage M отчетность и LLM FACTS-first соответствуют целям v2.
- **Release readiness (full production e2e on real 64 SKU): WARN** — нужен отдельный боевой прогон в целевом сетевом контуре WB (VPN OFF для WB стадий) и контроль артефактов run-папки.

---

## 4) Минимальный go-live gate (обязателен перед продом)

1. Прогон `A..M` на реальном `WB_INPUT_64_FROM_POCKETS_POD.xlsx` (лист `INPUT_64`).
2. Проверка, что в out-dir есть все stage artifacts до `decisions.jsonl` и финальные `WB_NECROMANCER_REPORT.xlsx/.html`.
3. Ручной sanity review 10 SKU (микс verdict: REVIVE_FAST/REWORK/CLONE/DROP).
4. Проверка Stage I (reviews): нет массового «all DROP из-за no_reviews_or_unreachable».
5. Проверка Stage M: evidence block и top flags/tasks согласованы с `decisions.jsonl`.

Если пункты 1–5 PASS, релиз считаем формально закрытым.
