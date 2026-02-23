# WB Revival v2 «Некромант» — файл для переноса в новый чат
Дата: 2026-02-21  
Таймзона: Europe/Oslo

> Это “один файл, чтобы не потерять мозги”. Его кидаешь в новый чат вместе со скриптом — и продолжаем.

---

## 0) TL;DR
Мы делаем пайплайн для **реанимации мёртвых SKU** на WB (у нас чехлы), но **не по “нашей цене/позициям”**, а по **фактам рынка**:
- **Рынок 1 (phone-market):** жив ли спрос на чехлы для конкретной модели телефона.
- **Рынок 2 (type-market):** жив ли спрос на **TPU силиконовый чехол с карманом под карту** для этой модели.

Код собирает факты (SERP, конкуренты, отзывы), считает агрегаты (pulse/supply/structure), дальше выдаёт **вердикт**:
- `REVIVE_FAST` / `REVIVE_REWORK` / `CLONE_NEW_CARD` / `DROP`
и список задач/флагов.

LLM — **не источник данных**. LLM только:
- (опционально) обогащает поисковые запросы
- (опционально) помогает на пограничной релевантности
- формулирует итог (rationale/backlog/exec summary) **строго по FACTS**.

---

## 1) Контекст проекта и “границы мира”
### 1.1 Scope
- Работаем **строго по 64 SKU** из файла:
  - `WB_INPUT_64_FROM_POCKETS_POD.xlsx`, лист `INPUT_64`
- Один запуск = один `run_id`, один `out_dir` (например `RUN_TEST2`)

### 1.2 Архитектура “WB vs LLM” и VPN
- **WB стадии** (сетевые запросы к WB) должны работать **без VPN** (или с РФ-сетью), чтобы не ловить блокировки.
- **LLM стадии** можно делать под VPN (например Норвегия) — это не бьёт WB, потому что LLM **не ходит в WB**.

Скрипт печатает подсказки:
- `Network: WB/LOCAL`
- `VPN hint: OFF (WB stage)` или `VPN hint: ON (LLM stage)`
Плюс есть `--pause_before_llm` и/или `--pause_between_stages`.

---

## 2) Почему “v1 revive” был концептуально неверный и зачем v2
### 2.1 Инсайт
Сравнивать “нашу мёртвую карточку” с “живыми конкурентами” по цене/позициям — **хуёвая метрика**, потому что:
- цена на мёртвом SKU часто мусор (распродажа/заглушка/устаревшее)
- позиции плавают из‑за рекламы/ставок и зависят от ключа
- “50k отзывов” может быть кладбищем (последний отзыв год назад)

### 2.2 Поэтому v2 “Некромант”
Мы оцениваем:
- **пульс спроса** через свежесть отзывов (review velocity)
- **давление предложения** и структуру рынка (цены/стоки/концентрация продавцов)
- и отдельно решаем **phone-market** и **type-market**

---

## 3) Что берём из own-card и что игнорируем
Own-card нужна **не для сравнения с рынком по цене**, а чтобы:
- извлечь модель телефона (phone model)
- определить тип/фичи (TPU, карман/карты и т.п.)
- собрать “карму карточки” (rating, feedback count)
- получить контент для запросов/фильтров

Правило: **own_price в решениях игнорируем**.

---

## 4) Главная цель v2: “два рынка на один SKU”
### 4.1 Рынок 1: Чехлы для модели телефона (phone-market)
Вопрос: **вообще продаются ли чехлы** на эту модель (телефон жив или умер).

### 4.2 Рынок 2: TPU + карман под карту (type-market)
Вопрос: даже если модель жива, **продаётся ли именно этот тип** (TPU + карман/карта).

---

## 5) Матрица решений v2 (Final Decision)
1) Если **phone-market DEAD** → `DROP`
2) Если **phone-market ALIVE**, но **type-market DEAD** → обычно `DROP`
   - или `alt_strategy`: “модель жива, но карман не востребован, рассмотреть другой тип”
3) Если **type-market ALIVE**:
   - если **карма токсична** (низкий рейтинг при достаточном числе отзывов) → `CLONE_NEW_CARD`
   - иначе → `REVIVE_FAST` или `REVIVE_REWORK` (по контент‑долгу)

Контент‑анализ вторичен: нужен, чтобы отличить FAST vs REWORK и сформировать backlog, но не чтобы решать “жив ли рынок”.

---

## 6) Структура пайплайна (A–M)
Каноничная схема данных: JSONL + `.wb_cache` + отчёты.

### A) INPUT
- читает xlsx (`nm_id`, `vendor_code`, `name`, `potential_qty` и т.п.)
- пишет `run_manifest.json` (фиксирует scope, run_id, параметры)

### B) OWN FETCH (WB)
- тянет карточку товара (v4/v1 endpoints)
- опционально `deep-card` (basket) — **best-effort**, не должен ломать стадию
- пишет `own_norm.jsonl`
- пишет `own_errors.jsonl` для ошибок

### C) INTENT EXTRACT (LOCAL)
- из `own_norm.jsonl` и/или из имени из Excel (fallback) вытаскивает:
  - phone_model
  - feature flags (TPU, pocket/card)
  - “карму” (rating/feedbacks)
- пишет `intent_norm.jsonl`

### D) QUERY BUILD (LOCAL + optional LLM)
Строит запросы **для двух кластеров**:
- D1: phone-market queries
- D2: type-market queries (TPU+карман)
Правило: **rules-first** + optional LLM enrichment  
Валидация/дедуп обязательны. Пишет `queries_valid.jsonl`.

### E) SERP SNAPSHOT + VALIDATION (WB)
- снимает SERP по запросам
- валидирует запросы по pass_rate и минимальным критериям
- выбирает 2–5 запросов на кластер
- пишет `serp_snapshots.jsonl`, `queries_selected.jsonl`

### F) COMPETITOR POOL (LOCAL/WB-derived)
- формирует пул конкурентов по каждому кластеру:
  - leaders + closest match
  - диверсификация по seller
  - дедуп по imt_id/root
- пишет `competitor_pool.jsonl`

### G) LITE FETCH (WB)
- минимальный fetch по конкурентам (rating/feedbacks, prices, stocks proxy)
- кэш по imt_id
- пишет `competitor_lite.jsonl`

### H) RELEVANCE FILTER (LOCAL + optional LLM)
- rules-first фильтр KEEP/DROP
  - phone-market: must phone_model + case intent
  - type-market: must phone_model + TPU + pocket/card
- optional LLM только на пограничных
- пишет `competitors_selected.jsonl` и/или `llm_relevance.jsonl`

### I) MARKET PULSE (WB)
- review velocity по imt_id: recent_30/90, days_since_last
- кэш + early-stop
- пишет `market_pulse.jsonl`

### J) SUPPLY/STRUCTURE (LOCAL)
- робастная цена (median/trimmed + outlier flags)
- stock proxy (median + концентрация)
- seller concentration (unique sellers, top1 share, HHI) если считаем
- пишет `market_structure.jsonl`

### K) CLUSTER VERDICTS (LOCAL rules)
- отдельно phone-market и type-market:
  - `ALIVE` / `SLOW` / `DEAD` + confidence
- пишет `cluster_verdicts.jsonl`

### L) FINAL DECISION (LOCAL rules + optional LLM wording)
- применяет матрицу решений v2
- LLM (если включено) только формулирует rationale/backlog по FACTS
- пишет `decisions.jsonl`

### M) REPORTS
- XLSX + HTML (самодостаточный дашборд: поиск/фильтры/детали)
- опционально LLM для exec summary (тоже по FACTS)
- пишет `WB_REVIVE_REPORT.xlsx` и `WB_REVIVE_REPORT.html`

---

## 7) Каноничные форматы данных и требования
- Все JSONL записи содержат:
  - `meta: {schema_version, run_id, nm_id (строка), vendor_code, ts, stage}`
- `nm_id` **всегда строкой** (никакой scientific notation)
- Кэш WB: `.wb_cache/` (own, serp, competitors, reviews и т.п.)
- Отчёты: `XLSX + HTML`

---

## 8) LLM: политика, промптинг, JSON-only
### 8.1 Политика
LLM не имеет права:
- ходить в интернет/WB
- выдумывать числа
- менять решение матрицы (если стадия L с rules-first)

LLM делает:
- нормализацию/обогащение текстовых задач
- классификацию на границе (если включено)
- русское объяснение по фактам

### 8.2 Формат ответа
- `rationale` — RU
- `risk_flags` — EN `UPPER_SNAKE_CASE`
- JSON-only output (внутри скрипта)

---

## 9) Словари, склонения и внешний lexicon
### 9.1 Проблема
Русская морфология ломает “keyword split”: чехол/чехла/чехлу…
Нужен минимальный stem-based матчинг или аккуратные regex/токенизация.

### 9.2 Решение “на сейчас”
- используем **основы (stems)** типа `чехл`, `кошел`, `кармашк`, `карточк`
- избегаем слишком общих `карт` (ложные совпадения)

### 9.3 Внешний словарь (план и реализация)
План: внешний файл, чтобы руками дописывать термы без правки кода.

Формат (пример):
```json
{
  "case_terms": ["чехл", "обложк"],
  "pocket_terms": ["кошел", "кармашк", "карточк", "картхолдер"],
  "tpu_terms": ["тпу", "термопластич", "silicone", "soft tpu"]
}
```

Скрипт должен:
- грузить `necromancer_lexicon.json` из cwd или рядом со скриптом
- не мутировать глобальные списки “магически”
- логировать, что загрузил

---

## 10) Реальные проблемы, на которые уже наступили, и как лечили
### 10.1 В старом revive: rel50≈0
Причина: фильтры must/ban были слишком жёсткие → `serp_relevance_pass` обнулял релевантность.  
Лечение: ослабление/перепроектирование фильтров + выбор нескольких ключей.

### 10.2 В некроманте: Stage B падал, own_norm.jsonl не создавался
Симптом: Stage C кричит “own_norm.jsonl not found”.  
Факт из `own_errors.jsonl`: `TypeError wb_get_json() got unexpected keyword argument 'backoff'`.  
Причина: **переопределение `wb_get_json`** и несовместимые сигнатуры.  
Лечение:
- привести к единой сигнатуре `wb_get_json`
- сделать deep fetch best-effort
- добавить degraded-mode Stage C (создавать stub из manifest, если own_norm отсутствует)

### 10.3 Тихие конфликты имён функций (критично для качества данных)
- `parse_search_items` была определена дважды (Stage E/F) → пропадал `raw` → цена считалась эвристикой.  
Лечение: уникальные имена / единая каноничная реализация с `raw`.

- `wb_get_json` определялась в нескольких стадиях → тихая смена поведения.  
Лечение: одна каноничная функция, “legacy” переименовать.

### 10.4 Внешний lexicon мутировал глобальные списки
Риск: непредсказуемое поведение стадий после загрузки словаря.  
Лечение: функция должна возвращать новые списки (pure), а присваивание делать явно.

---

## 11) Что уже сделано в текущем “финальном” скрипте
Текущая рабочая версия для продолжения:
- `wb_necromancer_v2_FINAL_FIXED_REFACTORED.py`

В ней:
- меню (как в wb_revive) + `--menu`
- явный `.env loader`
- паузы между стадиями (VPN подсказки)
- pretty HTML отчёт (по аналогии с revive)
- best-effort deep fetch (не ломает Stage B)
- degraded-mode Stage C (если own_norm нет)
- устранены конфликты имён (`parse_search_items`, `wb_get_json`, и т.п.)
- внешний lexicon поддерживается без “магической” мутации

---

## 12) Как запускать (типовые сценарии)
### 12.1 Rules-only (без LLM, VPN не нужен)
```powershell
python wb_necromancer_v2_FINAL_FIXED_REFACTORED.py --out RUN_001 --start_stage A --end_stage M
```

### 12.2 LLM только формулировки (L+M), с паузой перед LLM
```powershell
python wb_necromancer_v2_FINAL_FIXED_REFACTORED.py --out RUN_002 --start_stage A --end_stage M ^
  --use_llm_l --use_llm_m --pause_before_llm
```

### 12.3 LLM D+H+L+M и паузы между стадиями (ручное включение VPN)
```powershell
python wb_necromancer_v2_FINAL_FIXED_REFACTORED.py --out RUN_003 --start_stage A --end_stage M ^
  --use_llm_d --use_llm_h --use_llm_l --use_llm_m --pause_between_stages
```

---

## 13) Что делать дальше (следующий чат)
Приоритеты “чтобы было production-ish”:
1) Вынести утилиты в один блок/модуль: http client, backoff, parsing, jsonl IO
2) Конфиги в отдельный файл: thresholds, lexicon, pass_rate
3) Тестовый режим:
   - прогон на 1–3 SKU
   - фиксация снимков SERP/competitors
4) Улучшить русскую нормализацию (минимальный стемминг/токенизация)
5) Усилить контроль качества данных:
   - если `queries_selected` пуст → явный флаг и деградация, а не молчаливый мусор

---

## 14) Файлы, которые обычно кидаем в новый чат
- План/конспект v2: `WB_Revival_v2_Necromancer_plan_transfer.txt`
- Скрипт: `wb_necromancer_v2_FINAL_FIXED_REFACTORED.py`
- (если нужно) пример логов: `own_errors.jsonl` (как “вот так ломалось”)
- входной xlsx: `WB_INPUT_64_FROM_POCKETS_POD.xlsx`

---
Конец файла.
